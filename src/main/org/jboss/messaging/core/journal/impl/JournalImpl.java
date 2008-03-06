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

import java.io.File;
import java.io.FilenameFilter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.jboss.messaging.core.journal.Journal;
import org.jboss.messaging.core.journal.SequentialFile;
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
	
	private static final byte ADD_RECORD = 1;
	
	private static final byte DELETE_RECORD = 2;
	
	private static final String JOURNAL_FILE_PREFIX = "journal-";
	
	private static final String JOURNAL_FILE_EXTENSION = "jbm";
	
   	
	private final String journalDir;
	
	private final int fileSize;
	
	private JournalFile currentFile ;
	
	private List<JournalFile> files = new ArrayList<JournalFile>();
	
	private LinkedList<JournalFile> availableFiles = new LinkedList<JournalFile>();
	
	private int fileSequence;
	
	
	public JournalImpl(final String journalDir, final int fileSize)
	{
		this.journalDir = journalDir;
		
		this.fileSize = fileSize;
	}
	
	public void preAllocateFiles(int numFiles) throws Exception
	{
		log.info("Pre-allocating " + numFiles + " files");
		
		for (int i = 0; i < numFiles; i++)
		{
			JournalFile info = createFile();
			
			availableFiles.add(info);
		}
		
		log.info("Done pre-allocate");
	}
		
	public void add(long id, byte[] bytes) throws Exception
	{
		int size = 1 + INT_LENGTH + LONG_LENGTH + bytes.length;
		
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
	}
	
	public void delete(long id) throws Exception
	{
		int size = 1 + LONG_LENGTH;
		
		checkFile(size);
		
		byte[] toWrite = new byte[size];
		ByteBuffer bb = ByteBuffer.wrap(toWrite);
		bb.put(DELETE_RECORD);
		bb.putLong(id);
		
		bb.flip();
		
		currentFile.getFile().write(bb);			
	}
	
	private boolean loaded;
	
	public void load() throws Exception
	{
		if (loaded)
		{
			throw new IllegalStateException("Journal is already loaded");
		}
		
		loadFiles();
		
		Map<Long, byte[]> records = new HashMap<Long, byte[]>();
				
		for (JournalFile file: this.files)
		{
			byte[] bytes = new byte[fileSize];
			
			ByteBuffer bb = ByteBuffer.wrap(bytes);
			
			file.getFile().read(bb);
			
			while (true)
			{
				byte recordType = bb.get();
				
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
				else
				{
					//Implies end of records in the file
					break;
					
					//TODO set currentFile and offset
				}
			}
			
		}
		
		
	}
	
	// Private -----------------------------------------------------------------------------
	
	private void loadFiles() throws Exception
	{
		File dir = new File(journalDir);
		
		FilenameFilter fnf = new FilenameFilter()
		{
			public boolean accept(File file, String name)
			{
				return name.endsWith(".jbm");
			}
		};
		
		String[] fileNames = dir.list(fnf);
		
		List<JournalFile> files = new ArrayList<JournalFile>(fileNames.length);
				
		for (String fileName: fileNames)
		{
			SequentialFile file = new NIOSequentialFile(fileName, true);
			
			file.load();
			
			files.add(new JournalFile(file));
		}
		
		//Now order them by ordering id - we can't use the file name for ordering since we can re-use files
		
		class JournalFileComparator implements Comparator<JournalFile>
		{
			public int compare(JournalFile f1, JournalFile f2)
	      {
	         long id1 = f1.getFile().getOrderingID();
	         long id2 = f2.getFile().getOrderingID();

	         return (id1 < id2 ? -1 : (id1 == id2 ? 0 : 1));
	      }
		}

		Collections.sort(files, new JournalFileComparator());
		
		for (JournalFile file: files)
		{
			
		}
	}
	
	private JournalFile createFile() throws Exception
	{
		int fileNo = fileSequence++;
		
		String fileName = journalDir + "/" + JOURNAL_FILE_PREFIX + fileNo + "." + JOURNAL_FILE_EXTENSION;
		
		SequentialFile sequentialFile = new NIOSequentialFile(fileName, true);
		
		sequentialFile.create();
		
		JournalFile info = new JournalFile(sequentialFile);
		
		return info;
	}
	
	private void checkFile(int size) throws Exception
	{
		if (currentFile == null || fileSize - currentFile.getOffset() < size)
		{
			if (currentFile != null)
			{
				currentFile.getFile().close();
			}
			
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
