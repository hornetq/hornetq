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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;

import org.jboss.messaging.core.journal.Journal;
import org.jboss.messaging.core.journal.RecordHandle;
import org.jboss.messaging.core.journal.RecordHistory;
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
	
	// The sizes of primitive types
	
	private static final int SIZE_LONG = 8;
   
   private static final int SIZE_INT = 4;
   
   private static final int SIZE_BYTE = 1;
   
   //Record markers - they must be all unique
   
	public static final byte ADD_RECORD = 11;
	
	public static final byte UPDATE_RECORD = 12;
	
	public static final byte DELETE_RECORD = 13;
	
	public static final byte ADD_RECORD_TX = 14;
	
	public static final byte UPDATE_RECORD_TX = 15;
	
	public static final byte DELETE_RECORD_TX = 16;
	
	//End markers - they must all be unique
	
	public static final byte DONE = 21;
	
	public static final byte TX_CONTINUE = 22;
	
	public static final byte TX_DONE = 23;
	
	public static final byte FILL_CHARACTER = 74; // Letter 'J' 
		
	
	
	  	
	private final String journalDir;
	
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
	
	private final Map<Long, TransactionInfo> transactions = new ConcurrentHashMap<Long, TransactionInfo>();
	
	private final Map<Long, List<RecordHandle>> transactionalDeletes =
		new ConcurrentHashMap<Long, List<RecordHandle>>();
			
	/*
	 * We use a semaphore rather than synchronized since it performs better when contended
	 */
	
	//TODO - improve concurrency by allowing concurrent accesses if doesn't change current file
	private final Semaphore lock = new Semaphore(1, true);
		
	private volatile JournalFile currentFile ;
		
	private volatile boolean loaded;
	
	private volatile long lastOrderingID;
	
	private final Timer timer = new Timer(true);
	
	private final TimerTask reclaimerTask = new ReclaimerTask();
	
	private final TimerTask availableFilesTask = new AvailableFilesTask();
	

	
	public JournalImpl(final String journalDir, final int fileSize, final int minFiles, final int minAvailableFiles,
			             final boolean sync, final SequentialFileFactory fileFactory, final long taskPeriod,
			             final String filePrefix, final String fileExtension)
	{
		this.journalDir = journalDir;
		
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
	
	public RecordHandle appendAddRecord(final long id, final byte[] record) throws Exception
	{
		if (!loaded)
		{
			throw new IllegalStateException("Journal must be loaded first");
		}
		
		//TODO optimise to avoid creating a new byte buffer
		
		int size = SIZE_BYTE + SIZE_LONG + SIZE_INT + record.length + SIZE_BYTE;
		
		ByteBuffer bb = ByteBuffer.wrap(new byte[size]);
		
		bb.put(ADD_RECORD);
		
		bb.putLong(id);
		
		bb.putInt(record.length);
		
		bb.put(record);
		
		bb.put(DONE);
		
		bb.flip();
			             				
		lock.acquire();
		
		try
		{   					
   		checkFile(size);
   		   		
   		currentFile.getFile().write(bb);		
   		
   		currentFile.extendOffset(size);
   		
   		RecordHandleImpl rh = new RecordHandleImpl(id);
   		
   		rh.addFile(currentFile);
   		
   		return rh;
		}
		finally
		{
			lock.release();
		}
	}
	
	public RecordHandle appendAddRecordTransactional(final long txID, final long id,
			                                           final byte[] record, final boolean done) throws Exception
	{
		if (!loaded)
		{
			throw new IllegalStateException("Journal must be loaded first");
		}
		
		//TODO optimise to avoid creating a new byte buffer
		
		int size = SIZE_BYTE + SIZE_LONG + SIZE_LONG + SIZE_INT + record.length + SIZE_BYTE;
		
		ByteBuffer bb = ByteBuffer.wrap(new byte[size]);
		
		bb.put(ADD_RECORD);
		
		bb.putLong(txID);
		
		bb.putLong(id);
		
		bb.putInt(record.length);
		
		bb.put(record);
		
		if (done)
		{
			bb.put(TX_DONE);
		}
		else
		{
			bb.put(TX_CONTINUE);
		}
		
		bb.flip();
			             				
		lock.acquire();
		
		try
		{   					
   		checkFile(size);
   		   		
   		currentFile.getFile().write(bb);		
   		
   		currentFile.extendOffset(size);
   		
   		RecordHandleImpl rh = new RecordHandleImpl(id);
   		
   		rh.addFile(currentFile);
   		
   		if (done)
   		{   		
      		List<RecordHandle> list = transactionalDeletes.remove(txID);
      		
      		if (list != null)
   			{
   				releaseDeletes(list);
   			}      		
   		} 
   		
   		return rh;
		}
		finally
		{
			lock.release();
		}
	}
	
	public void appendUpdateRecord(final RecordHandle handle, final byte[] record) throws Exception
	{
		if (!loaded)
		{
			throw new IllegalStateException("Journal must be loaded first");
		}
		
		RecordHandleImpl rh = (RecordHandleImpl)handle;
		
		//TODO optimise to avoid creating a new byte buffer
		
		int size = SIZE_BYTE + SIZE_LONG + SIZE_INT + record.length + SIZE_BYTE;
		
		ByteBuffer bb = ByteBuffer.wrap(new byte[size]);
		
		bb.put(UPDATE_RECORD);
		
		bb.putLong(rh.getID());
		
		bb.putInt(record.length);
		
		bb.put(record);
		
		bb.put(DONE);
		
		bb.flip();
		
		lock.acquire();
		
		try
		{   		
   		checkFile(size);
   		   		
   		currentFile.getFile().write(bb);		
   		
   		currentFile.extendOffset(size);
   		
   		rh.addFile(currentFile);
		}
		finally
		{
			lock.release();
		}				
	}
	
	public void appendUpdateRecordTransactional(final long txID, final RecordHandle handle,
			final byte[] record, final boolean done) throws Exception
	{
		if (!loaded)
		{
			throw new IllegalStateException("Journal must be loaded first");
		}
		
		RecordHandleImpl rh = (RecordHandleImpl)handle;
		
		//TODO optimise to avoid creating a new byte buffer
		
		int size = SIZE_BYTE + SIZE_LONG + SIZE_LONG + SIZE_INT + record.length + SIZE_BYTE;
		
		ByteBuffer bb = ByteBuffer.wrap(new byte[size]);
		
		bb.put(UPDATE_RECORD);
		
		bb.putLong(txID);
		
		bb.putLong(rh.getID());
		
		bb.putInt(record.length);
		
		bb.put(record);
		
	   if (done)
	   {
	   	bb.put(TX_DONE);
	   }
	   else
	   {
	   	bb.put(TX_CONTINUE);
	   }
		
		bb.flip();
		
		lock.acquire();
		
		try
		{   		
   		checkFile(size);
   		   		
   		currentFile.getFile().write(bb);		
   		
   		currentFile.extendOffset(size);
   		   		
   		rh.addFile(currentFile);
   		
   		if (done)
   		{   		
      		List<RecordHandle> list = transactionalDeletes.remove(txID);
      		
      		if (list != null)
   			{
   				releaseDeletes(list);
   			}      		
   		}   		
		}
		finally
		{
			lock.release();
		}				
	}
	
	private void releaseDeletes(final List<RecordHandle> deletes)
	{
		for (RecordHandle handle: deletes)
		{
			RecordHandleImpl rh = (RecordHandleImpl)handle;
			
			rh.recordDeleted();
		}
	}
	
	public void appendDeleteRecord(final RecordHandle handle) throws Exception
	{
		if (!loaded)
		{
			throw new IllegalStateException("Journal must be loaded first");
		}
		
		RecordHandleImpl rh = (RecordHandleImpl)handle;
		
		int size = SIZE_BYTE + SIZE_LONG + SIZE_BYTE;
		
		ByteBuffer buffer = ByteBuffer.wrap(new byte[size]);
		
		buffer.put(DELETE_RECORD);
		
		buffer.putLong(rh.getID());
		
		buffer.put(DONE);
				
		buffer.flip();
								
		lock.acquire();
		
		try
		{   		
   		checkFile(size);
   		   		
   		currentFile.getFile().write(buffer);		
   		
   		currentFile.extendOffset(size);
   		
   		rh.recordDeleted();
		}
		finally
		{
			lock.release();
		}				
	}
	
	public void appendDeleteRecordTransactional(final long txID, final RecordHandle handle, final boolean done) throws Exception
	{
		if (!loaded)
		{
			throw new IllegalStateException("Journal must be loaded first");
		}
		
		RecordHandleImpl rh = (RecordHandleImpl)handle;
		
		int size = SIZE_BYTE + SIZE_LONG + SIZE_LONG + SIZE_BYTE;
		
		ByteBuffer buffer = ByteBuffer.wrap(new byte[size]);
		
		buffer.put(DELETE_RECORD);
		
		buffer.putLong(txID);
		
		buffer.putLong(rh.getID());
		
		if (done)
		{
			buffer.put(TX_DONE);
		}
		else
		{
			buffer.put(TX_CONTINUE);
		}
				
		buffer.flip();
								
		lock.acquire();
		
		try
		{   		
   		checkFile(size);
   		   		
   		currentFile.getFile().write(buffer);		
   		
   		currentFile.extendOffset(size);
   		
   		//It's a transactional delete so we need to make sure file doesn't get deleted
   		rh.addFile(currentFile);
   		
   		List<RecordHandle> list = transactionalDeletes.get(txID);
   		
   		if (list == null)
   		{
   			list = new ArrayList<RecordHandle>();
   			
   			transactionalDeletes.put(txID, list);
   		}
   		
   		list.add(handle);
   		
   		if (done)
   		{
   			transactionalDeletes.remove(txID);
   			
   			releaseDeletes(list);
   		}
		}
		finally
		{
			lock.release();
		}				
	}	
		
	public List<RecordHistory> load() throws Exception
	{
		if (loaded)
		{
			throw new IllegalStateException("Journal is already loaded");
		}
		
		log.info("Loading...");
		
		List<String> fileNames = fileFactory.listFiles(journalDir, fileExtension);
		
		log.info("There are " + fileNames.size() + " files in directory");
		
		List<JournalFile> orderedFiles = new ArrayList<JournalFile>(fileNames.size());
				
		for (String fileName: fileNames)
		{
			SequentialFile file = fileFactory.createSequentialFile(fileName, sync);
			
			file.open();
			
			ByteBuffer bb = ByteBuffer.wrap(new byte[SIZE_LONG]);
			
			file.read(bb);
			
			bb.flip();
			
			long orderingID = bb.getLong();
			
			file.reset();
							
			orderedFiles.add(new JournalFile(file, orderingID));
		}
		
		log.info("minFiles is " + minFiles);
		
		int createNum = minFiles - orderedFiles.size();
		
		//Preallocate some more if necessary
		for (int i = 0; i < createNum; i++)
		{
			JournalFile file = createFile();
			
			orderedFiles.add(file);
			
			log.info("Created new file");
		}
		
		log.info("Done creating new ones");
			
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
		
		Map<Long, RecordHistory> histories = new LinkedHashMap<Long, RecordHistory>();
				
		for (JournalFile file: orderedFiles)
		{
			log.info("Loading file, ordering id is " + file.getOrderingID());
			
			ByteBuffer bb = ByteBuffer.wrap(new byte[fileSize]);
			
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
				byte recordType = bb.get();
				
				log.info("recordtype is " + recordType);
				
				int pos = bb.position();
				
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
							handleAddRecord(id, file, record, histories);
							
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
							handleUpdateRecord(id, file, record, histories);
							
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
							handleDeleteRecord(id, histories);
							
							hasData = true;							
						}
						
						break;
					}					
					case ADD_RECORD_TX:
					{					
						long txID = bb.getLong();
						
						long id = bb.getLong();
						
						int size = bb.getInt();
						
						byte[] record = new byte[size];
						
						bb.get(record);
						
						byte end = bb.get();
						
						if (end != TX_CONTINUE && end != TX_DONE)
						{
							repairFrom(pos, file);
						}
						else
						{						
							handleTransactionalRecord(ADD_RECORD, txID, id, file, record, end, histories);
							
							hasData = true;							
						}
					
						break;
					}		
					case UPDATE_RECORD_TX:
					{					
						long txID = bb.getLong();
						
						long id = bb.getLong();
						
						int size = bb.getInt();
						
						byte[] record = new byte[size];
						
						bb.get(record);
						
						byte end = bb.get();
						
						if (end != TX_CONTINUE && end != TX_DONE)
						{
							repairFrom(pos, file);
						}
						else
						{					
							handleTransactionalRecord(UPDATE_RECORD, txID, id, file, record, end, histories);
							
							hasData = true;							
						}
											
						break;
					}	
					case DELETE_RECORD_TX:
					{					
						long txID = bb.getLong();
						
						long id = bb.getLong();
						
						byte end = bb.get();
						
						if (end != TX_CONTINUE && end != TX_DONE)
						{
							repairFrom(pos, file);
						}
						else
						{					
							handleTransactionalRecord(DELETE_RECORD, txID, id, file, null, end, histories);
							
							hasData = true;							
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
			}
				
			if (hasData)
			{
				log.info("Adding to files");
				
				files.add(file);
			}
			else
			{
				log.info("Adding to available files");
				
				//Empty files with no data
				availableFiles.add(file);
			}								
		}				
						
		//Now it's possible that some of the files are no longer needed
		
		checkFilesForReclamation();
		
		for (JournalFile file: files)
		{
			currentFile = file;
		}
		
		//Check we have enough available files
		
		checkAndCreateAvailableFiles();
				
		if (currentFile == null)
		{
			currentFile = availableFiles.remove();
			
			files.add(currentFile);
		}				
		
		//Close all files apart from the current one
		
		for (JournalFile file: files)
		{
			if (file != currentFile)
			{
				file.getFile().close();
			}
		}
						
		startTasks();
		
		loaded = true;
		
		return new ArrayList<RecordHistory>(histories.values());
	}
	
	private void repairFrom(int pos, JournalFile file) throws Exception
	{
		log.warn("Corruption has been detected in file: " + file.getFile().getFileName() +
				   " in the record that starts at position " + pos + ". " + 
				   "The most likely cause is that a crash occurred in the previous run. The corrupt record will be discarded.");
		
		file.getFile().fill(pos, fileSize - pos, FILL_CHARACTER);
	}
	
	public void checkAndCreateAvailableFiles() throws Exception
	{
		log.info("Checking if we need to create more files");
		
		int filesToCreate = minAvailableFiles - availableFiles.size();
		
		for (int i = 0; i < filesToCreate; i++)
		{
			JournalFile file = createFile();
			
			availableFiles.add(file);
		}
	}
	
	public void stop() throws Exception
	{
		reclaimerTask.cancel();
		
		availableFilesTask.cancel();
		
		for (JournalFile file: files)
		{
			file.getFile().close();
		}
		
		for (JournalFile file: availableFiles)
		{
			file.getFile().close();
		}

		this.currentFile = null;
		
		files.clear();
		
		availableFiles.clear();			
	}
	
	public void startTasks()
	{
		timer.schedule(reclaimerTask, taskPeriod, taskPeriod);
		
		timer.schedule(availableFilesTask, taskPeriod, taskPeriod);
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
		log.info("checking files for reclamation");
		
		for (JournalFile file: files)
		{		
   		if (file.isEmpty() && file != currentFile)
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
   			
   			sf.write(bb);
   			
   			JournalFile jf = new JournalFile(sf, newOrderingID);
   			
   			availableFiles.add(jf);   		
   		}
		}
	}
		
	// Private -----------------------------------------------------------------------------
	
	private void playTransaction(final TransactionInfo tx, final Map<Long, RecordHistory> histories)
	{
		for (TransactionEntry entry: tx.entries)
		{
			switch (entry.type)
			{
				case ADD_RECORD:
				{
					handleAddRecord(entry.id, entry.file, entry.record, histories);
					
					break;
				}
				case UPDATE_RECORD:
				{
					handleUpdateRecord(entry.id, entry.file, entry.record, histories);
					
					break;
				}
				case DELETE_RECORD:
				{
					handleDeleteRecord(entry.id, histories);
					
					break;
				}
				default:
				{
					throw new IllegalStateException("Invalid record type " + entry.type);
				}
			}
		}
	}
	
	private void handleAddRecord(final long id, final JournalFile file,
			                       final byte[] record, final Map<Long, RecordHistory> histories)
	{
		RecordHandleImpl handle = new RecordHandleImpl(id);
		
		handle.addFile(file);
		
		RecordHistoryImpl history = new RecordHistoryImpl(handle);
		
		history.addRecord(record);
		
		histories.put(id, history);
	}
	
	private void handleUpdateRecord(final long id, final JournalFile file,
         final byte[] record, final Map<Long, RecordHistory> histories)
   {
		RecordHistoryImpl history = (RecordHistoryImpl)histories.get(id);
		
		if (history == null)
		{
			throw new IllegalStateException("Cannot find record (update) " + id);
		}
		
		RecordHandleImpl handle = (RecordHandleImpl)history.getHandle();
		
		handle.addFile(file);
		
		history.addRecord(record);	
   }
	
	private void handleDeleteRecord(final long id, final Map<Long, RecordHistory> histories)
   {
		RecordHistoryImpl history = (RecordHistoryImpl)histories.remove(id);
		
		if (history == null)
		{
			throw new IllegalStateException("Cannot find record (delete) " + id);
		}
		
		RecordHandleImpl handle = (RecordHandleImpl)history.getHandle();
		
		handle.recordDeleted();
   }
	
	private void handleTransactionalRecord(final byte type, final long txID, final long id,
			                                 final JournalFile file, final byte[] record,
			                                 final byte end,
			                                 final Map<Long, RecordHistory> histories)
	{
		TransactionInfo tx = transactions.get(txID);
		
		if (tx == null)
		{
			tx = new TransactionInfo();
			
			transactions.put(txID, tx);
		}
		
		TransactionEntry entry = new TransactionEntry(type, id, file, record);
		
		tx.entries.add(entry);
		
		if (end == TX_DONE)
		{
			transactions.remove(txID);
			
			playTransaction(tx, histories);
		}
		else if (end == TX_CONTINUE)
		{
			//
		}
		else
		{
			throw new IllegalStateException("Invalid transaction marker " + end);
		}
	}
			
	private JournalFile createFile() throws Exception
	{
		log.info("Creating a new file");
		
		long orderingID = generateOrderingID();
		
		String fileName = journalDir + "/" + filePrefix + "-" + orderingID + "." + fileExtension;
						
		SequentialFile sequentialFile = fileFactory.createSequentialFile(fileName, sync);
		
		sequentialFile.open();
						
		sequentialFile.fill(0, fileSize, FILL_CHARACTER);
		
		ByteBuffer bb = ByteBuffer.wrap(new byte[SIZE_LONG]);
		
		bb.putLong(orderingID);
		
		bb.flip();
		
		sequentialFile.write(bb);
		
		sequentialFile.reset();
		
		JournalFile info = new JournalFile(sequentialFile, orderingID);
		
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
			log.info("Getting new file");
			
			if (currentFile != null)
			{
				currentFile.getFile().close();								
			}
			
			log.info("Getting new file");
			
			checkAndCreateAvailableFiles();
			
		   currentFile = availableFiles.remove();			

			files.add(currentFile);
		}
	}
	
	private static class TransactionInfo
	{
		final List<TransactionEntry> entries = new ArrayList<TransactionEntry>();
	}
	
	private static class TransactionEntry
	{
		TransactionEntry(final byte type, final long id, final JournalFile file, final byte[] record)
		{
			this.type = type;
			this.id = id;
			this.file = file;
			this.record = record;
		}
		final byte type;
		final long id;
		final JournalFile file;
		final byte[] record;
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
				checkFilesForReclamation();
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
				checkAndCreateAvailableFiles();
			}
			catch (Exception e)
			{
				log.error("Failure in running availableFileChecker", e);
				
				cancel();
			}
		}
		
	}
	
}
