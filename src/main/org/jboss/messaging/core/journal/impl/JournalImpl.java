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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.jboss.messaging.core.journal.Journal;
import org.jboss.messaging.core.journal.RecordHandle;
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
	
	private static final int INT_LENGTH = 4;
	
	private static final int LONG_LENGTH = 8;
	
	public static final byte ADD_RECORD = 1;
	
	public static final byte DELETE_RECORD = 2;
	
	public static final byte FILL_CHARACTER = (byte)'J';
	
	public static final String JOURNAL_FILE_PREFIX = "jbm";
	
	public static final String JOURNAL_FILE_EXTENSION = "jbm";
	
   
	
	private final String journalDir;
	
	private final int fileSize;
	
	private final int numFiles;
	
	private final boolean sync;
	
	private final SequentialFileFactory fileFactory;
	
	private final LinkedList<JournalFile> files = new LinkedList<JournalFile>();
	
	private final LinkedList<JournalFile> availableFiles = new LinkedList<JournalFile>();
	
	private final LinkedList<JournalFile> filesToDelete = new LinkedList<JournalFile>();
		
	/*
	 * We use a semaphore rather than synchronized since it performs better when contended
	 */
	
	//TODO - improve concurrency by allowing concurrent accesses if doesn't change current file
	private final Semaphore lock = new Semaphore(1, true);
		
	private volatile JournalFile currentFile ;
		
	private volatile boolean loaded;
	
	private volatile long lastOrderingID;
					
	public JournalImpl(final String journalDir, final int fileSize, final int numFiles, final boolean sync,
			             final SequentialFileFactory fileFactory)
	{
		this.journalDir = journalDir;
		
		this.fileSize = fileSize;
		
		this.numFiles = numFiles;
		
		this.sync = sync;
		
		this.fileFactory = fileFactory;
	}
	
	// Journal implementation ----------------------------------------------------------------
	
	public RecordHandle add(final long id, final byte[] bytes) throws Exception
	{
		if (!loaded)
		{
			throw new IllegalStateException("Journal must be loaded first");
		}
		
		int size = 1 + INT_LENGTH + LONG_LENGTH + bytes.length;
		
		lock.acquire();
		
		try
		{   		
   		checkFile(size);
   		
   		byte[] toWrite = new byte[size];
   		ByteBuffer bb = ByteBuffer.wrap(toWrite);
   		bb.put(ADD_RECORD);		
   		bb.putLong(id);
   		bb.putInt(bytes.length);
   		bb.put(bytes);
   		
   		bb.flip();
   		
   		currentFile.getFile().write(bb);		
   		
   		currentFile.extendOffset(size);
   		
   		currentFile.addID(id);
   		
   		return new RecordHandleImpl(id, currentFile);
		}
		finally
		{
			lock.release();
		}
	}
	
	public void delete(RecordHandle handle) throws Exception
	{
		if (!loaded)
		{
			throw new IllegalStateException("Journal must be loaded first");
		}
		
		RecordHandleImpl rh = (RecordHandleImpl)handle;
		
		int size = 1 + LONG_LENGTH;
		
		lock.acquire();
		
		try
		{		
   		checkFile(size);
   		
   		long id = rh.getID();
   		
   		byte[] toWrite = new byte[size];
   		ByteBuffer bb = ByteBuffer.wrap(toWrite);
   		bb.put(DELETE_RECORD);
   		bb.putLong(id);
   		
   		bb.flip();
   		
   		currentFile.getFile().write(bb);	
   		
   		JournalFile addedFile = rh.getFile();
   		   		
   		addedFile.removeID(id);
   		
   		checkAndReclaimFile(addedFile);
		}
		finally
		{
			lock.release();
		}
	}
		
	public Map<Long, byte[]> load() throws Exception
	{
		if (loaded)
		{
			throw new IllegalStateException("Journal is already loaded");
		}
		
		log.info("Loading...");
		
		List<String> fileNames = fileFactory.listFiles(journalDir, JOURNAL_FILE_EXTENSION);
		
		log.info("There are " + fileNames.size() + " files in directory");
		
		List<JournalFile> orderedFiles = new ArrayList<JournalFile>(fileNames.size());
				
		for (String fileName: fileNames)
		{
			SequentialFile file = fileFactory.createSequentialFile(fileName, sync);
			
			file.open();
			
			ByteBuffer bb = ByteBuffer.wrap(new byte[LONG_LENGTH]);
			
			file.read(bb);
			
			bb.flip();
			
			long orderingID = bb.getLong();
			
			file.reset();
							
			orderedFiles.add(new JournalFile(file, orderingID));
		}
		
		log.info("numFiles is " + numFiles);
		
		int createNum = numFiles - orderedFiles.size();
		
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
		
		Map<Long, byte[]> records = new HashMap<Long, byte[]>();
		
		boolean filesWithData = true;
		
		outer: for (JournalFile file: orderedFiles)
		{
			log.info("Loading file, ordering id is " + file.getOrderingID());
			
			byte[] bytes = new byte[fileSize];
			
			ByteBuffer bb = ByteBuffer.wrap(bytes);
			
			file.getFile().read(bb);
			
			bb.flip();
			
			bb.getLong();
			
			while (filesWithData && bb.hasRemaining())
			{
				byte recordType = bb.get();
				
				log.info("recordtype is " + recordType);
				
				if (recordType == ADD_RECORD)
				{
					long id = bb.getLong();
					
					int length = bb.getInt();
					
					//TODO - optimise this - no need to copy
					
					byte[] record = new byte[length];
					
					bb.get(record);
					
					records.put(id, record);
				}
				else if (recordType == DELETE_RECORD)
				{
					long id = bb.getLong();
					
					records.remove(id);
				}
				else if (recordType == FILL_CHARACTER)
				{										
					//Implies end of records in the file
					
					files.add(file);
					
					currentFile = file;
					
					filesWithData = false;
															
					continue outer;
				}		
				else
				{
					throw new IllegalStateException("Journal " + file.getFile().getFileName() +
							                         " is corrupt, invalid record type " + recordType);
				}
			}
				
			if (filesWithData)
			{
				log.info("Adding to files");
				files.add(file);
			}
			else
			{
				log.info("Adding to available files");
				//Empty files with no data of importance
				availableFiles.add(file);
			}
		}				
		
		loaded = true;
		
		return records;
	}
	
	public void stop() throws Exception
	{
		log.info("files size " + files.size());
		log.info("available files size " + availableFiles.size());
		log.info("files top delete size " + filesToDelete.size());
		
		for (JournalFile file: files)
		{
			file.getFile().close();
		}
		
		for (JournalFile file: availableFiles)
		{
			file.getFile().close();
		}
		
		for (JournalFile file: filesToDelete)
		{
			file.getFile().close();
		}
		
		this.currentFile = null;
		
		files.clear();
		
		availableFiles.clear();
		
		filesToDelete.clear();
	}
	
	// Public -----------------------------------------------------------------------------
	
	public LinkedList<JournalFile> getFiles()
	{
		return files;
	}
	
	public LinkedList<JournalFile> getAvailableFiles()
	{
		return availableFiles;
	}
	
	public LinkedList<JournalFile> getFilesToDelete()
	{
		return filesToDelete;
	}
	
	// Private -----------------------------------------------------------------------------
	
	private void checkAndReclaimFile(JournalFile file) throws Exception
	{		
		if (file.isEmpty() && file != currentFile)
		{
			//File can be reclaimed
			
			files.remove(file);
			
			//TODO - add to delete file list if there are a lot of available files
			
			//Re-initialise it
			
			long newOrderingID = generateOrderingID();
			
			ByteBuffer bb = ByteBuffer.wrap(new byte[LONG_LENGTH]);
			
			bb.putLong(newOrderingID);
			
			SequentialFile sf = file.getFile();
			
			sf.reset();
			
			sf.write(bb);
			
			JournalFile jf = new JournalFile(sf, newOrderingID);
			
			availableFiles.add(jf);   		
		}
	}
	
	private JournalFile createFile() throws Exception
	{
		log.info("Creating a new file");
		
		long orderingID = generateOrderingID();
		
		String fileName = journalDir + "/" + JOURNAL_FILE_PREFIX + "-" + orderingID + "." + JOURNAL_FILE_EXTENSION;
						
		SequentialFile sequentialFile = fileFactory.createSequentialFile(fileName, sync);
		
		sequentialFile.open();
						
		sequentialFile.preAllocate(fileSize, FILL_CHARACTER);
		
		ByteBuffer bb = ByteBuffer.wrap(new byte[LONG_LENGTH]);
		
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
		if (size > fileSize - LONG_LENGTH)
		{
			throw new IllegalArgumentException("Record is too large to store " + size);
		}
		
		if (currentFile == null || fileSize - currentFile.getOffset() < size)
		{
			log.info("Getting new file");
			
			if (currentFile != null)
			{
				currentFile.getFile().close();
				
				checkAndReclaimFile(currentFile);
			}
			
			log.info("Getting new file");
			
			if (!availableFiles.isEmpty())
			{
				currentFile = availableFiles.remove();			
			}
			else
			{
				currentFile = createFile();			
			}
			
			files.add(currentFile);
		}
	}
}
