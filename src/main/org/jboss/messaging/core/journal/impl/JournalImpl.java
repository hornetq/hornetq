/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
  * by the @authors tag. See the copyright.txt in the distribution for a
  * full listing of individual contributors.
  *
  * This is free software; you can redistribute it and/or modify it
  * under the terms of the GNU Lesser General Public License as
  * published by the Free Software Foundation; either version 2.1 of
  * the License, or (at your option) any later version.
  *
  * This software is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  * Lesser General Public License for more details.
  *
  * You should have received a copy of the GNU Lesser General Public
  * License along with this software; if not, write to the Free
  * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
  */
package org.jboss.messaging.core.journal.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import org.jboss.messaging.core.journal.Journal;
import org.jboss.messaging.core.journal.PreparedTransactionInfo;
import org.jboss.messaging.core.journal.RecordInfo;
import org.jboss.messaging.core.journal.SequentialFile;
import org.jboss.messaging.core.journal.SequentialFileFactory;
import org.jboss.messaging.core.logging.Logger;

/**
 * 
 * A JournalImpl
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class JournalImpl implements Journal
{
	private static final Logger log = Logger.getLogger(JournalImpl.class);
	
	private static final int STATE_STOPPED = 0;
	
	private static final int STATE_STARTED = 1;
	
	private static final int STATE_LOADED = 2;
	
	// The sizes of primitive types
	
	private static final int SIZE_LONG = 8;
   
   private static final int SIZE_INT = 4;
   
   private static final int SIZE_BYTE = 1;
   
   public static final int MIN_FILE_SIZE = 1024;
   
   public static final int MIN_TASK_PERIOD = 5000;
   
   //Record markers - they must be all unique
   
	public static final byte ADD_RECORD = 11;
	
	public static final byte UPDATE_RECORD = 12;
	
	public static final byte DELETE_RECORD = 13;
			
	public static final byte ADD_RECORD_TX = 14;
	
	public static final byte UPDATE_RECORD_TX = 15;
	
	public static final byte DELETE_RECORD_TX = 16;
	
	public static final byte PREPARE_RECORD = 17;
		
	public static final byte COMMIT_RECORD = 18;
	
	public static final byte ROLLBACK_RECORD = 19;
	
	public static final byte DONE = 20;
	
	public static final byte FILL_CHARACTER = 74; // Letter 'J' 
		

	private final int fileSize;
	
	private final int minFiles;
	
	private final int minAvailableFiles;
	
	private final boolean sync;
	
	private final SequentialFileFactory fileFactory;
	
	private final long taskPeriod;
	
	public final String filePrefix;
	
	public final String fileExtension;
	 
	
	private final Queue<JournalFile> files = new ConcurrentLinkedQueue<JournalFile>();
	
	private final Queue<JournalFile> availableFiles = new ConcurrentLinkedQueue<JournalFile>();
	
	/*
	 * We use a semaphore rather than synchronized since it performs better when contended
	 */
	
	//TODO - improve concurrency by allowing concurrent accesses if doesn't change current file
	private final Semaphore lock = new Semaphore(1, true);
		
	private volatile JournalFile currentFile ;
		
	private volatile int state;
	
	private volatile long lastOrderingID;
	
	private final Timer timer = new Timer(true);
	
	private TimerTask reclaimerTask;
	
	private TimerTask availableFilesTask;
	
	private final AtomicLong transactionIDSequence = new AtomicLong(0);
	
	public JournalImpl(final int fileSize, final int minFiles, final int minAvailableFiles,
			             final boolean sync, final SequentialFileFactory fileFactory, final long taskPeriod,
			             final String filePrefix, final String fileExtension)
	{
		if (fileSize < MIN_FILE_SIZE)
		{
			throw new IllegalArgumentException("File size cannot be less than " + MIN_FILE_SIZE + " bytes");
		}
		if (minFiles < 2)
		{
			throw new IllegalArgumentException("minFiles cannot be less than 2");
		}
		if (minAvailableFiles < 2)
		{
			throw new IllegalArgumentException("minAvailableFiles cannot be less than 2");
		}
		if (fileFactory == null)
		{
			throw new NullPointerException("fileFactory is null");
		}
		if (taskPeriod < MIN_TASK_PERIOD)
		{
			throw new IllegalArgumentException("taskPeriod cannot be less than " + MIN_TASK_PERIOD);
		}
		if (filePrefix == null)
		{
			throw new NullPointerException("filePrefix is null");
		}
		if (fileExtension == null)
		{
			throw new NullPointerException("fileExtension is null");
		}
		
		this.fileSize = fileSize;
		
		this.minFiles = minFiles;
		
		this.minAvailableFiles = minAvailableFiles;
		
		this.sync = sync;
		
		this.fileFactory = fileFactory;
		
		this.taskPeriod = taskPeriod;
		
		this.filePrefix = filePrefix;
		
		this.fileExtension = fileExtension;
	}
	
	// Journal implementation ----------------------------------------------------------------
	
	public ByteBuffer allocateBuffer(final int size) throws Exception
	{
		return ByteBuffer.allocateDirect(size);
	}
	
	public void appendAddRecord(final long id, final byte[] record) throws Exception
	{
		if (state != STATE_LOADED)
		{
			throw new IllegalStateException("Journal must be loaded first");
		}
		
		int size = SIZE_BYTE + SIZE_LONG + SIZE_INT + record.length + SIZE_BYTE;
		
		ByteBuffer bb = ByteBuffer.wrap(new byte[size]);
		
		bb.put(ADD_RECORD);		
		bb.putLong(id);		
		bb.putInt(record.length);		
		bb.put(record);		
		bb.put(DONE);			
		bb.flip();
		
		appendRecord(bb, true);
	}
			
	public void appendUpdateRecord(final long id, final byte[] record) throws Exception
	{
		if (state != STATE_LOADED)
		{
			throw new IllegalStateException("Journal must be loaded first");
		}
			
		int size = SIZE_BYTE + SIZE_LONG + SIZE_INT + record.length + SIZE_BYTE;
		
		ByteBuffer bb = ByteBuffer.wrap(new byte[size]);
		
		bb.put(UPDATE_RECORD);		
		bb.putLong(id);		
		bb.putInt(record.length);		
		bb.put(record);		
		bb.put(DONE);		
		bb.flip();
		
		appendRecord(bb, true);			
	}
			
	public void appendDeleteRecord(long id) throws Exception
	{
		if (state != STATE_LOADED)
		{
			throw new IllegalStateException("Journal must be loaded first");
		}
		
		int size = SIZE_BYTE + SIZE_LONG + SIZE_BYTE;
		
		ByteBuffer bb = ByteBuffer.wrap(new byte[size]);
		
		bb.put(DELETE_RECORD);		
		bb.putLong(id);		
		bb.put(DONE);		
		bb.flip();
								
		appendRecord(bb, true);			
	}		
	
	public long getTransactionID()
	{
		return transactionIDSequence.getAndIncrement();
	}

	public void appendAddRecordTransactional(final long txID, final long id,
			                                   final byte[] record) throws Exception
   {
		if (state != STATE_LOADED)
		{
			throw new IllegalStateException("Journal must be loaded first");
		}

		int size = SIZE_BYTE + SIZE_LONG + SIZE_LONG + SIZE_INT + record.length + SIZE_BYTE;

		ByteBuffer bb = ByteBuffer.wrap(new byte[size]);

		bb.put(ADD_RECORD_TX);
		bb.putLong(txID);
		bb.putLong(id);
		bb.putInt(record.length);
		bb.put(record);
		bb.put(DONE);		
		bb.flip();
		
		appendRecord(bb, false);
	}
			
	public void appendUpdateRecordTransactional(final long txID, final long id,
			final byte[] record) throws Exception
	{
		if (state != STATE_LOADED)
		{
			throw new IllegalStateException("Journal must be loaded first");
		}

		int size = SIZE_BYTE + SIZE_LONG + SIZE_LONG + SIZE_INT + record.length + SIZE_BYTE;
		
		ByteBuffer bb = ByteBuffer.wrap(new byte[size]);
		
		bb.put(UPDATE_RECORD_TX);		
		bb.putLong(txID);		
		bb.putLong(id);		
		bb.putInt(record.length);		
		bb.put(record);		
		bb.put(DONE);		
		bb.flip();
		
		appendRecord(bb, false);			
	}
	
	public void appendDeleteRecordTransactional(final long txID, final long id) throws Exception
	{
		if (state != STATE_LOADED)
		{
			throw new IllegalStateException("Journal must be loaded first");
		}
		
		int size = SIZE_BYTE + SIZE_LONG + SIZE_LONG + SIZE_BYTE;
		
		ByteBuffer bb = ByteBuffer.wrap(new byte[size]);
		
		bb.put(DELETE_RECORD_TX);		
		bb.putLong(txID);		
		bb.putLong(id);		
		bb.put(DONE);			
		bb.flip();
								
		appendRecord(bb, false);				
	}	
	
		
	public void appendPrepareRecord(final long txID) throws Exception
	{
		if (state != STATE_LOADED)
		{
			throw new IllegalStateException("Journal must be loaded first");
		}
		
		int size = SIZE_BYTE + SIZE_LONG + SIZE_BYTE;
		
		ByteBuffer bb = ByteBuffer.wrap(new byte[size]);
		
		bb.put(PREPARE_RECORD);		
		bb.putLong(txID);		
		bb.put(DONE);				
		bb.flip();
		
		appendRecord(bb, true);		
	}
	
	public void appendCommitRecord(final long txID) throws Exception
	{
		if (state != STATE_LOADED)
		{
			throw new IllegalStateException("Journal must be loaded first");
		}
		
		int size = SIZE_BYTE + SIZE_LONG + SIZE_BYTE;
		
		ByteBuffer bb = ByteBuffer.wrap(new byte[size]);
		
		bb.put(COMMIT_RECORD);		
		bb.putLong(txID);		
		bb.put(DONE);				
		bb.flip();
		
		appendRecord(bb, true);	
	}
	
	public void appendRollbackRecord(final long txID) throws Exception
	{
		if (state != STATE_LOADED)
		{
			throw new IllegalStateException("Journal must be loaded first");
		}
		
		int size = SIZE_BYTE + SIZE_LONG + SIZE_BYTE;
		
		ByteBuffer bb = ByteBuffer.wrap(new byte[size]);
		
		bb.put(ROLLBACK_RECORD);		
		bb.putLong(txID);		
		bb.put(DONE);			
		bb.flip();
								
		appendRecord(bb, true);			
	}
		
	public void load(final List<RecordInfo> committedRecords,
		              final List<PreparedTransactionInfo> preparedTransactions) throws Exception
	{
		if (state != STATE_STARTED)
		{
			throw new IllegalStateException("Journal must be in started state");
		}
		
		Set<Long> recordsToDelete = new HashSet<Long>();
		
		Map<Long, TransactionHolder> transactions = new LinkedHashMap<Long, TransactionHolder>();
		
		List<RecordInfo> records = new ArrayList<RecordInfo>();
		
		List<String> fileNames = fileFactory.listFiles(fileExtension);
		
		List<JournalFile> orderedFiles = new ArrayList<JournalFile>(fileNames.size());
				
		for (String fileName: fileNames)
		{
			SequentialFile file = fileFactory.createSequentialFile(fileName, sync);
			
			file.open();
			
			ByteBuffer bb = ByteBuffer.wrap(new byte[SIZE_LONG]);
			
			file.read(bb);
			
			bb.flip();
			
			long orderingID = bb.getLong();
						
			orderedFiles.add(new JournalFile(file, orderingID));
			
			file.close();
		}
		
		int createNum = minFiles - orderedFiles.size();
		
		//Preallocate some more if necessary
		for (int i = 0; i < createNum; i++)
		{
			JournalFile file = createFile();
			
			orderedFiles.add(file);
			
			file.getFile().close();
		}
			
		//Now order them by ordering id - we can't use the file name for ordering since we can re-use files
		
		class JournalFileComparator implements Comparator<JournalFile>
		{
			public int compare(JournalFile f1, JournalFile f2)
	      {
	         long id1 = f1.getOrderingID();
	         long id2 = f2.getOrderingID();

	         return (id1 < id2 ? -1 : (id1 == id2 ? 0 : 1));
	      }
		}

		Collections.sort(orderedFiles, new JournalFileComparator());
		
		int lastDataPos = -1;
		
		long maxTransactionID = -1;
		
		for (JournalFile file: orderedFiles)
		{	
			ByteBuffer bb = ByteBuffer.wrap(new byte[fileSize]);
			
			file.getFile().open();
			
			int bytesRead = file.getFile().read(bb);
			
			if (bytesRead != fileSize)
			{
				//deal with this better
				
				throw new IllegalStateException("File is wrong size " + bytesRead +
						                          " expected " + fileSize + " : " + file.getFile().getFileName());
			}
			
			bb.flip();
			
			//First long is the ordering timestamp
			bb.getLong();
			
			boolean hasData = false;
			
			while (bb.hasRemaining())
			{
				int pos = bb.position();
				
				byte recordType = bb.get();
				
				switch(recordType)
				{
					case ADD_RECORD:
					{									
						long id = bb.getLong();				
						int size = bb.getInt();						
						byte[] record = new byte[size];						
						bb.get(record);						
						byte end = bb.get();
						
						if (end != DONE)
						{
							repairFrom(pos, file);
						}
						else
						{																				
							records.add(new RecordInfo(id, record, false));
							hasData = true;							
						}
												
						break;
					}										
					case UPDATE_RECORD:						
					{
						long id = bb.getLong();		
						int size = bb.getInt();						
						byte[] record = new byte[size];						
						bb.get(record);						
						byte end = bb.get();
						
						if (end != DONE)
						{
							repairFrom(pos, file);
						}
						else
						{					
							records.add(new RecordInfo(id, record, true));							
							hasData = true;							
						}
												
						break;
					}					
					case DELETE_RECORD:						
					{
						long id = bb.getLong();	
						byte end = bb.get();
						
						if (end != DONE)
						{
							repairFrom(pos, file);
						}
						else
						{						
							recordsToDelete.add(id);							
							hasData = true;							
						}
						
						break;
					}					
					case ADD_RECORD_TX:
					{					
						long txID = bb.getLong();							
						maxTransactionID = Math.max(maxTransactionID, txID);						
						long id = bb.getLong();				
						int size = bb.getInt();						
						byte[] record = new byte[size];						
						bb.get(record);						
						byte end = bb.get();
						
						if (end != DONE)
						{
							repairFrom(pos, file);
						}
						else
						{						
							TransactionHolder tx = transactions.get(txID);
							
							if (tx == null)
							{
								tx = new TransactionHolder(txID);								
								transactions.put(txID, tx);
							}
							
							tx.recordInfos.add(new RecordInfo(id, record, false));							
							hasData = true;							
						}
					
						break;
					}		
					case UPDATE_RECORD_TX:
					{					
						long txID = bb.getLong();	
						maxTransactionID = Math.max(maxTransactionID, txID);						
						long id = bb.getLong();					
						int size = bb.getInt();						
						byte[] record = new byte[size];						
						bb.get(record);						
						byte end = bb.get();
						
						if (end != DONE)
						{
							repairFrom(pos, file);
						}
						else
						{					
							TransactionHolder tx = transactions.get(txID);
							
							if (tx == null)
							{
								tx = new TransactionHolder(txID);								
								transactions.put(txID, tx);
							}
							
							tx.recordInfos.add(new RecordInfo(id, record, true));

							hasData = true;							
						}
											
						break;
					}	
					case DELETE_RECORD_TX:
					{					
						long txID = bb.getLong();	
						maxTransactionID = Math.max(maxTransactionID, txID);						
						long id = bb.getLong();			
						byte end = bb.get();
						
						if (end != DONE)
						{
							repairFrom(pos, file);
						}
						else
						{					
							TransactionHolder tx = transactions.get(txID);
							
							if (tx == null)
							{
								tx = new TransactionHolder(txID);								
								transactions.put(txID, tx);
							}
							
							tx.recordsToDelete.add(id);							
							hasData = true;							
						}
											
						break;
					}	
					case PREPARE_RECORD:
					{
						long txID = bb.getLong();				
						maxTransactionID = Math.max(maxTransactionID, txID);						
						byte end = bb.get();
						
						if (end != DONE)
						{
							repairFrom(pos, file);
						}
						else
						{
							TransactionHolder tx = transactions.get(txID);
							
							if (tx == null)
							{
								throw new IllegalStateException("Cannot find tx with id " + txID);
							}
														
							tx.prepared = true;
						}
						
						break;
					}
					case COMMIT_RECORD:
					{
						long txID = bb.getLong();	
						maxTransactionID = Math.max(maxTransactionID, txID);
						byte end = bb.get();
						
						if (end != DONE)
						{
							repairFrom(pos, file);
						}
						else
						{
							TransactionHolder tx = transactions.remove(txID);
							
							if (tx == null)
							{
								throw new IllegalStateException("Cannot find tx with id " + txID);
							}
							
							records.addAll(tx.recordInfos);							
							recordsToDelete.addAll(tx.recordsToDelete);														
						}
						
						break;
					}
					case ROLLBACK_RECORD:
					{
						long txID = bb.getLong();		
						maxTransactionID = Math.max(maxTransactionID, txID);						
						byte end = bb.get();
						
						if (end != DONE)
						{
							repairFrom(pos, file);
						}
						else
						{
							TransactionHolder tx = transactions.remove(txID);
							
							if (tx == null)
							{
								throw new IllegalStateException("Cannot find tx with id " + txID);
							}						
						}
						
						break;
					}
					case FILL_CHARACTER:						
					{	
						//End of records in file - we check the file only contains fill characters from this point
						while (bb.hasRemaining())
						{
							byte b = bb.get();
							
							if (b != FILL_CHARACTER)
							{
								throw new IllegalStateException("Corrupt file " + file.getFile().getFileName() +
										" contains non fill character at position " + pos);
							}
						}
						
						break;						
					}					
					default:						
					{
						throw new IllegalStateException("Journal " + file.getFile().getFileName() +
								                         " is corrupt, invalid record type " + recordType);
					}
				}
				
				if (recordType != FILL_CHARACTER)
				{
					lastDataPos = bb.position();
				}
			}
						
			if (hasData)
			{			
				files.add(file);
				
				//Files are always maintained closed - there may be a lot of them and we don't want to run out
				//of file handles
				file.getFile().close();				
			}
			else
			{				
				//Empty files with no data
				availableFiles.add(file);
				
				//Position it ready for writing
				file.getFile().position(SIZE_LONG);
			}								
		}			
		
		transactionIDSequence.set(maxTransactionID + 1);
									
		//Now it's possible that some of the files are no longer needed
		
		checkFilesForReclamation();
				
		//Check we have enough available files
		
		checkAndCreateAvailableFiles();
						
		for (JournalFile file: files)
		{
			currentFile = file;						
		}
		
		if (currentFile != null)
		{		
			currentFile.getFile().open();
		
			currentFile.getFile().position(lastDataPos);
			
			currentFile.setOffset(lastDataPos);
		}
		else
		{
			currentFile = availableFiles.remove();
			
			files.add(currentFile);
		}				
		
		startTasks();
				
		for (RecordInfo record: records)
		{
			if (!recordsToDelete.contains(record.id))
			{
				committedRecords.add(record);
			}
		}
		
		for (TransactionHolder transaction: transactions.values())
		{
			if (!transaction.prepared)
			{
				log.warn("Uncommitted transaction with id " + transaction.transactionID + " found and discarded");
			}
			else
			{
				PreparedTransactionInfo info = new PreparedTransactionInfo(transaction.transactionID);
				
				info.records.addAll(transaction.recordInfos);
				
				info.recordsToDelete.addAll(transaction.recordsToDelete);
				
				preparedTransactions.add(info);
			}
		}
				
		state = STATE_LOADED;
	}
			
	public void checkAndCreateAvailableFiles() throws Exception
	{		
		int filesToCreate = minAvailableFiles - availableFiles.size();
		
		for (int i = 0; i < filesToCreate; i++)
		{
			JournalFile file = createFile();

			availableFiles.add(file);
		}
	}
	
	// MessagingComponent implementation ---------------------------------------------------
	
	public synchronized void start()
	{
		if (state != STATE_STOPPED)
		{
			throw new IllegalStateException("Journal is not stopped");
		}
		
		state = STATE_STARTED;
	}
	
	public synchronized void stop() throws Exception
	{
		if (state == STATE_STOPPED)
		{
			throw new IllegalStateException("Journal is already stopped");
		}
		
		if (reclaimerTask != null)
		{
			reclaimerTask.cancel();
		}
		
		if (availableFilesTask != null)
		{
			availableFilesTask.cancel();
		}
		
		if (currentFile != null)
		{
			currentFile.getFile().close();
		}
		
		for (JournalFile file: availableFiles)
		{
			file.getFile().close();
		}

		currentFile = null;
		
		files.clear();
		
		availableFiles.clear();		
		
		state = STATE_STOPPED;
	}
	
	public void startTasks()
	{
//		reclaimerTask = new ReclaimerTask();
//		timer.schedule(reclaimerTask, taskPeriod, taskPeriod);
//		
//		availableFilesTask = new AvailableFilesTask();
//		timer.schedule(availableFilesTask, taskPeriod, taskPeriod);
	}
	
	// Public -----------------------------------------------------------------------------
	
	public Queue<JournalFile> getFiles()
	{
		return files;
	}
	
	public Queue<JournalFile> getAvailableFiles()
	{
		return availableFiles;
	}
	
	public void checkFilesForReclamation() throws Exception
	{		
		for (JournalFile file: files)
		{		
			//TODO reclamation
   		if (false && file != currentFile)
   		{
   			//File can be reclaimed
   			
   			files.remove(file);
   			
   			//Re-initialise it
   			
   			long newOrderingID = generateOrderingID();
   			
   			ByteBuffer bb = ByteBuffer.wrap(new byte[SIZE_LONG]);
   			
   			bb.putLong(newOrderingID);
   			
   			SequentialFile sf = file.getFile();
   			
   			//Note we MUST re-fill it - otherwise we won't be able to detect corrupt records
   			sf.fill(0, fileSize, FILL_CHARACTER);
   			
   			sf.write(bb, true);
   			
   			JournalFile jf = new JournalFile(sf, newOrderingID);
   			
   			sf.position(SIZE_LONG);
   			
   			jf.setOffset(SIZE_LONG);
   			
   			availableFiles.add(jf);   		
   		}
		}
	}
		
	// Private -----------------------------------------------------------------------------
		
	private void appendRecord(ByteBuffer bb, boolean sync) throws Exception
	{
		lock.acquire();
		
		int size = bb.capacity();
				
		try
		{   					
			checkFile(size);
			currentFile.getFile().write(bb, false);			
			currentFile.extendOffset(size);
		}
		finally
		{
			lock.release();
		}
	}
	
	private void repairFrom(int pos, JournalFile file) throws Exception
	{
		log.warn("Corruption has been detected in file: " + file.getFile().getFileName() +
				   " in the record that starts at position " + pos + ". " + 
				   "The most likely cause is that a crash occurred in the previous run. The corrupt record will be discarded.");
		
		file.getFile().fill(pos, fileSize - pos, FILL_CHARACTER);
		
		file.getFile().position(pos);
	}
			
	private JournalFile createFile() throws Exception
	{
		long orderingID = generateOrderingID();
		
		String fileName = filePrefix + "-" + orderingID + "." + fileExtension;
						
		SequentialFile sequentialFile = fileFactory.createSequentialFile(fileName, sync);
		
		sequentialFile.open();
						
		sequentialFile.fill(0, fileSize, FILL_CHARACTER);
		
		ByteBuffer bb = ByteBuffer.wrap(new byte[SIZE_LONG]);
		
		bb.putLong(orderingID);
		
		bb.flip();
		
		sequentialFile.write(bb, true);
		
		sequentialFile.position(SIZE_LONG);
		
		JournalFile info = new JournalFile(sequentialFile, orderingID);
		
		info.extendOffset(SIZE_LONG);
		
		return info;
	}
	
	private long generateOrderingID()
	{
		long orderingID = System.currentTimeMillis();
		
		while (orderingID == lastOrderingID)
		{
			//Ensure it's unique
			try
			{				
				Thread.sleep(1);
			}
			catch (InterruptedException ignore)
			{				
			}
			orderingID = System.currentTimeMillis();
		}
		lastOrderingID = orderingID;	
		
		return orderingID;
	}
	
	private void checkFile(final int size) throws Exception
	{
		//We take into account the first timestamp long
		if (size > fileSize - SIZE_LONG)
		{
			throw new IllegalArgumentException("Record is too large to store " + size);
		}

		if (currentFile == null || fileSize - currentFile.getOffset() < size)
		{
			checkAndCreateAvailableFiles();
			
			currentFile.getFile().close();
			
		   currentFile = availableFiles.remove();			

			files.add(currentFile);
		}
	}
	
	private class ReclaimerTask extends TimerTask
	{
		public boolean cancel()
		{
			timer.cancel();
			
			return super.cancel();
		}

		public void run()
		{
			try
			{
				//checkFilesForReclamation();
			}
			catch (Exception e)
			{
				log.error("Failure in running reclaimer", e);
				
				cancel();
			}
		}		
	}
	
	private class AvailableFilesTask extends TimerTask
	{
		public boolean cancel()
		{
			timer.cancel();
			
			return super.cancel();
		}

		public void run()
		{
			try
			{
				//checkAndCreateAvailableFiles();
			}
			catch (Exception e)
			{
				log.error("Failure in running availableFileChecker", e);
				
				cancel();
			}
		}		
	}	
}
