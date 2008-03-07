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
package org.jboss.messaging.core.journal.impl.test.unit;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Map;

import org.jboss.messaging.core.journal.SequentialFile;
import org.jboss.messaging.core.journal.impl.JournalImpl;
import org.jboss.messaging.core.journal.impl.test.unit.fakes.FakeSequentialFileFactory;
import org.jboss.messaging.core.journal.impl.test.unit.fakes.FakeSequentialFileFactory.FakeSequentialFile;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.test.unit.UnitTestCase;

/**
 * 
 * A JournalTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class JournalTest extends UnitTestCase
{
	private static final Logger log = Logger.getLogger(JournalTest.class);
	
	private String journalDir = System.getProperty("user.home") + "/journal-test";
	
	private FakeSequentialFileFactory factory = new FakeSequentialFileFactory();
	
	protected void setUp() throws Exception
	{
		super.setUp();
		
		File file = new File(journalDir);
		
		deleteDirectory(file);
		
		file.mkdir();		
	}
	
	public void testLoad() throws Exception
	{
		final int numFiles = 10;
		
		final int fileSize = 10 * 1024;
		
		final boolean sync = true;
		
		long timeStart = System.currentTimeMillis();
		
		JournalImpl journal = new JournalImpl(journalDir, fileSize, numFiles, sync, factory);
		
		journal.load();
		
		long timeEnd = System.currentTimeMillis();
		
		assertEquals(1, journal.getFiles().size());
		assertEquals(numFiles - 1, journal.getAvailableFiles().size());
		assertEquals(0, journal.getFilesToDelete().size());
		
		assertEquals(numFiles, factory.getFileMap().size());
		
		for (Map.Entry<String, FakeSequentialFile> entry: factory.getFileMap().entrySet())
		{
			FakeSequentialFile file = (FakeSequentialFile)entry.getValue();
			
			assertEquals(sync, file.isSync());
			
			assertTrue(file.isOpen());
			
			byte[] bytes = file.getData().array();
			
			assertEquals(fileSize, bytes.length);
									
			//First four bytes should be ordering id timestamp
			
			ByteBuffer bb = ByteBuffer.wrap(bytes, 0, 8);
			long orderingID = bb.getLong();
			
			String expectedFilename =
				journalDir + "/" + JournalImpl.JOURNAL_FILE_PREFIX + "-" + orderingID + "." + JournalImpl.JOURNAL_FILE_EXTENSION;
			
			assertEquals(expectedFilename, file.getFileName());
			
			log.info("Ordering id is " + orderingID);
			
			assertTrue(orderingID >= timeStart);
			
			assertTrue(orderingID <= timeEnd);
			
			for (int i = 8; i < bytes.length; i++)
			{
				if (bytes[i] != JournalImpl.FILL_CHARACTER)
				{
					fail("Not filled correctly");
				}
			}
		}
		
		journal.stop();
		
		for (Map.Entry<String, FakeSequentialFile> entry: factory.getFileMap().entrySet())
		{
			FakeSequentialFile file = (FakeSequentialFile)entry.getValue();
			
			assertFalse(file.isOpen());
		}
		
		assertEquals(0, journal.getFiles().size());
		assertEquals(0, journal.getAvailableFiles().size());
		assertEquals(0, journal.getFilesToDelete().size());
		
		//Now reload
		
		journal = new JournalImpl(journalDir, fileSize, numFiles, sync, factory);
		
		log.info("******** reloading");
		
		journal.load();
		
		assertEquals(1, journal.getFiles().size());
		assertEquals(numFiles - 1, journal.getAvailableFiles().size());
		assertEquals(0, journal.getFilesToDelete().size());
		
		assertEquals(numFiles, factory.getFileMap().size());
		
		for (Map.Entry<String, FakeSequentialFile> entry: factory.getFileMap().entrySet())
		{
			FakeSequentialFile file = (FakeSequentialFile)entry.getValue();
			
			assertEquals(sync, file.isSync());
			
			assertTrue(file.isOpen());
			
			byte[] bytes = file.getData().array();
			
			assertEquals(fileSize, bytes.length);
									
			//First four bytes should be ordering id timestamp
			
			ByteBuffer bb = ByteBuffer.wrap(bytes, 0, 8);
			long orderingID = bb.getLong();
			
			String expectedFilename =
				journalDir + "/" + JournalImpl.JOURNAL_FILE_PREFIX + "-" + orderingID + "." + JournalImpl.JOURNAL_FILE_EXTENSION;
			
			assertEquals(expectedFilename, file.getFileName());
			
			log.info("Ordering id is " + orderingID);
			
			assertTrue(orderingID >= timeStart);
			
			assertTrue(orderingID <= timeEnd);
			
			for (int i = 8; i < bytes.length; i++)
			{
				if (bytes[i] != JournalImpl.FILL_CHARACTER)
				{
					fail("Not filled correctly");
				}
			}
		}
		
		
	}

//	public void test1() throws Exception
//	{		
//		File file = new File(journalDir);
//		
//		JournalImpl journal = new JournalImpl(journalDir, 10 * 1024 * 1024, 10, true, factory);
//		
//		journal.load();
//		
//		long start = System.currentTimeMillis();
//		
//		byte[] bytes = new byte[1024];
//
//		for (int i = 0; i < bytes.length; i++)
//		{
//			if (i % 100 == 0)
//			{
//				bytes[i] = '\n';
//			}
//			else
//			{
//				bytes[i] = 'T';
//			}
//		}
//		
//		final int numIts = 50000;
//		
//		for (int i = 0; i < numIts; i++)
//		{
//			journal.add(1, bytes);
//		}
//				
//		long end = System.currentTimeMillis();
//		
//		long numbytes = numIts * 1024;
//		
//		double actualRate = 1000 * (double)numbytes / ( end - start);
//      
//      log.info("Rate: (bytes/sec) " + actualRate);
//      
//      double recordRate = 1000 * (double)numIts / ( end - start);
//      
//      log.info("Rate: (records/sec) " + recordRate);
//		
//	}

}
