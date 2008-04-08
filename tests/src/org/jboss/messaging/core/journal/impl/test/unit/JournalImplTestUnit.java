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

import java.util.List;

import org.jboss.messaging.core.journal.RecordInfo;
import org.jboss.messaging.core.journal.impl.JournalImpl;
import org.jboss.messaging.core.logging.Logger;

/**
 * 
 * A JournalImplTestBase
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public abstract class JournalImplTestUnit extends JournalImplTestBase
{
	private static final Logger log = Logger.getLogger(JournalImplTestUnit.class);
	
	// General tests
	// =============
	
	public void testState() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		try
		{
			load();
			fail("Should throw exception");
		}
		catch (IllegalStateException e)
		{
			//OK
		}
		try
		{
			stopJournal();
			fail("Should throw exception");
		}
		catch (IllegalStateException e)
		{
			//OK
		}
		startJournal();
		try
		{
			startJournal();
			fail("Should throw exception");
		}
		catch (IllegalStateException e)
		{
			//OK
		}
		stopJournal();
		startJournal();
		load();
		try
		{
			load();
			fail("Should throw exception");
		}
		catch (IllegalStateException e)
		{
			//OK
		}
		try
		{
			startJournal();
			fail("Should throw exception");
		}
		catch (IllegalStateException e)
		{
			//OK
		}
		stopJournal();		
	}
	
	public void testParams() throws Exception
	{
		try
		{
			new JournalImpl(JournalImpl.MIN_FILE_SIZE - 1, 10, true, fileFactory, 5000, filePrefix, fileExtension);
			
			fail("Should throw exception");
		}
		catch (IllegalArgumentException e)
		{
			//Ok
		}
		
		try
		{
			new JournalImpl(10 * 1024, 1, true, fileFactory, 5000, filePrefix, fileExtension);
			
			fail("Should throw exception");
		}
		catch (IllegalArgumentException e)
		{
			//Ok
		}
				
		try
		{
			new JournalImpl(10 * 1024, 10, true, null, 5000, filePrefix, fileExtension);
			
			fail("Should throw exception");
		}
		catch (NullPointerException e)
		{
			//Ok
		}
		
		try
		{
			new JournalImpl(10 * 1024, 10, true, fileFactory, JournalImpl.MIN_TASK_PERIOD - 1, filePrefix, fileExtension);
			
			fail("Should throw exception");
		}
		catch (IllegalArgumentException e)
		{
			//Ok
		}
		
		try
		{
			new JournalImpl(10 * 1024, 10, true, fileFactory, 5000, null, fileExtension);
			
			fail("Should throw exception");
		}
		catch (NullPointerException e)
		{
			//Ok
		}
		
		try
		{
			new JournalImpl(10 * 1024, 10, true, fileFactory, 5000, filePrefix, null);
			
			fail("Should throw exception");
		}
		catch (NullPointerException e)
		{
			//Ok
		}
		
	}
	
	public void testFilesImmediatelyAfterload() throws Exception
	{
		try
		{
   		setup(10, 10 * 1024, true);
   		createJournal();
   		startJournal();
   		load();
   		
   		List<String> files = fileFactory.listFiles(fileExtension);
   		
   		assertEquals(10, files.size());
   		
   		for (String file: files)
   		{
   			assertTrue(file.startsWith(filePrefix));
   		}
   		
   		stopJournal();
   		
   		resetFileFactory();
   		
   		setup(20, 10 * 1024, true);
   		createJournal();
   		startJournal();
   		load();
   		
   		files = fileFactory.listFiles(fileExtension);
   		
   		assertEquals(20, files.size());
   		
   		for (String file: files)
   		{
   			assertTrue(file.startsWith(filePrefix));
   		}
   						
   		stopJournal();	
   		
   		fileExtension = "tim";
   		
   		resetFileFactory();
   		
   		setup(17, 10 * 1024, true);
   		createJournal();
   		startJournal();
   		load();
   		
   		files = fileFactory.listFiles(fileExtension);
   		
   		assertEquals(17, files.size());
   		
   		for (String file: files)
   		{
   			assertTrue(file.startsWith(filePrefix));
   		}
   		
   		stopJournal();	
   		
   		filePrefix = "echidna";
   		
   		resetFileFactory();
   		
   		setup(11, 10 * 1024, true);
   		createJournal();
   		startJournal();
   		load();
   		
   		files = fileFactory.listFiles(fileExtension);
   		
   		assertEquals(11, files.size());
   		
   		for (String file: files)
   		{
   			assertTrue(file.startsWith(filePrefix));
   		}
   		
   		stopJournal();	
		}
		finally
		{
			filePrefix = "jbm";
			
			fileExtension = "jbm";
		}
	}
	
	public void testCreateFilesOnLoad() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		
		List<String> files1 = fileFactory.listFiles(fileExtension);
		
		assertEquals(10, files1.size());
		
		stopJournal();
		
		//Now restart with different number of minFiles - should create 10 more
		
		setup(20, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		
		List<String> files2 = fileFactory.listFiles(fileExtension);
		
		assertEquals(20, files2.size());
		
		for (String file: files1)
		{
			assertTrue(files2.contains(file));
		}
				
		stopJournal();	
	}
	
	public void testReduceFreeFiles() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		
		List<String> files1 = fileFactory.listFiles(fileExtension);
		
		assertEquals(10, files1.size());
		
		stopJournal();
		
		setup(5, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		
		List<String> files2 = fileFactory.listFiles(fileExtension);
		
		assertEquals(10, files2.size());
		
		for (String file: files1)
		{
			assertTrue(files2.contains(file));
		}
				
		stopJournal();	
	}
			
	public void testCheckCreateMoreFiles() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		
		List<String> files1 = fileFactory.listFiles(fileExtension);
		
		assertEquals(10, files1.size());
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(9, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
				
		//Fill all the files
		
		for (int i = 0; i < 90; i++)
		{
			add(i);
		}
		
		assertEquals(9, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(90, journal.getIDMapSize());
				
		List<String> files2 = fileFactory.listFiles(fileExtension);
		
		assertEquals(10, files2.size());
				
		for (String file: files1)
		{
			assertTrue(files2.contains(file));
		}
		
		//Now add some more
		
		for (int i = 90; i < 95; i++)
		{
			add(i);
		}
		
		assertEquals(10, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(95, journal.getIDMapSize());
		
		List<String> files3 = fileFactory.listFiles(fileExtension);
		
		assertEquals(11, files3.size());
				
		for (String file: files1)
		{
			assertTrue(files3.contains(file));
		}
		
		//And a load more
		
		for (int i = 95; i < 200; i++)
		{
			add(i);
		}
		
		assertEquals(22, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(200, journal.getIDMapSize());
		
		List<String> files4 = fileFactory.listFiles(fileExtension);
		
		assertEquals(23, files4.size());
				
		for (String file: files1)
		{
			assertTrue(files4.contains(file));
		}
						
		stopJournal();	
	}
	
	public void testReclaim() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		
		List<String> files1 = fileFactory.listFiles(fileExtension);
		
		assertEquals(10, files1.size());
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(9, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
				
		for (int i = 0; i < 100; i++)
		{
			add(i);
		}
		
		assertEquals(11, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(100, journal.getIDMapSize());
		
		List<String> files4 = fileFactory.listFiles(fileExtension);
		
		assertEquals(12, files4.size());
				
		for (String file: files1)
		{
			assertTrue(files4.contains(file));
		}
		
		//Now delete half of them
		
		for (int i = 0; i < 50; i++)
		{
			delete(i);
		}
		
		assertEquals(11, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(50, journal.getIDMapSize());
		
		//Make sure the deletes aren't in the current file
		
		for (int i = 100; i < 110; i++)
		{
			add(i);
		}
		
		assertEquals(12, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(60, journal.getIDMapSize());
		
		journal.checkAndReclaimFiles();
		
		//Several of them should be reclaimed - and others deleted - the total number of files should not drop below
		//10
		
		assertEquals(7, journal.getDataFilesCount());
		assertEquals(2, journal.getFreeFilesCount());
		assertEquals(60, journal.getIDMapSize());
		
		List<String> files5 = fileFactory.listFiles(fileExtension);
		
		assertEquals(10, files5.size());
		
		//Now delete the rest
		
		for (int i = 50; i < 110; i++)
		{
			delete(i);
		}
		
		//And fill the current file
		
		for (int i = 110; i < 120; i++)
		{
			add(i);
			delete(i);
		}
		
		journal.checkAndReclaimFiles();
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(9, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
		
		List<String> files6 = fileFactory.listFiles(fileExtension);
		
		assertEquals(10, files6.size());
										
		stopJournal();	
	}
			
	public void testReclaimAddUpdateDeleteDifferentFiles1() throws Exception
	{
		setup(2, 1046, true); //Make sure there is one record per file
		createJournal();
		startJournal();
		load();
		
		add(1);
		update(1);
		delete(1);
		
		List<String> files1 = fileFactory.listFiles(fileExtension);
		
		assertEquals(3, files1.size());
		
		assertEquals(2, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
		
		journal.checkAndReclaimFiles();
		
		List<String> files2 = fileFactory.listFiles(fileExtension);
		
		assertEquals(2, files2.size());
		
		//1 gets deleted and 1 gets reclaimed
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(1, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
		
		stopJournal();
	}
	
	public void testReclaimAddUpdateDeleteDifferentFiles2() throws Exception
	{
		setup(2, 1046, true); //Make sure there is one record per file
		createJournal();
		startJournal();
		load();
		
		add(1);
		update(1);
		add(2);
		
		List<String> files1 = fileFactory.listFiles(fileExtension);
		
		assertEquals(3, files1.size());
		
		assertEquals(2, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(2, journal.getIDMapSize());
		
		journal.checkAndReclaimFiles();
		
		List<String> files2 = fileFactory.listFiles(fileExtension);
		
		assertEquals(3, files2.size());
		
		assertEquals(2, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(2, journal.getIDMapSize());
		
		
		stopJournal();
	}
	
	public void testReclaimTransactionalAddCommit() throws Exception
	{
		testReclaimTransactionalAdd(true);
	}
	
	public void testReclaimTransactionalAddRollback() throws Exception
	{
		testReclaimTransactionalAdd(false);
	}
	
	//TODO commit and rollback, also transactional deletes
	
	private void testReclaimTransactionalAdd(boolean commit) throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		
		List<String> files1 = fileFactory.listFiles(fileExtension);
		
		assertEquals(10, files1.size());
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(9, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
				
		for (int i = 0; i < 100; i++)
		{
			addTx(1, i);
		}
		
		assertEquals(11, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
		
		List<String> files2 = fileFactory.listFiles(fileExtension);
		
		assertEquals(12, files2.size());
				
		for (String file: files1)
		{
			assertTrue(files2.contains(file));
		}
		
		journal.checkAndReclaimFiles();
		
		//Make sure nothing reclaimed
		
		assertEquals(11, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
		
		List<String> files3 = fileFactory.listFiles(fileExtension);
		
		assertEquals(12, files3.size());
				
		for (String file: files1)
		{
			assertTrue(files3.contains(file));
		}
		
		//Add a load more updates
		
		for (int i = 100; i < 200; i++)
		{
			updateTx(1, i);
		}
		
		assertEquals(22, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
		
		List<String> files4 = fileFactory.listFiles(fileExtension);
		
		assertEquals(23, files4.size());
				
		for (String file: files1)
		{
			assertTrue(files4.contains(file));
		}
		
		journal.checkAndReclaimFiles();
		
		//Make sure nothing reclaimed
		
		assertEquals(22, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
		
		List<String> files5 = fileFactory.listFiles(fileExtension);
		
		assertEquals(23, files5.size());
				
		for (String file: files1)
		{
			assertTrue(files5.contains(file));
		}
				
		//Now delete them
											
		for (int i = 0; i < 200; i++)
		{
			deleteTx(1, i);
		}
		
		assertEquals(22, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
		
		List<String> files7 = fileFactory.listFiles(fileExtension);
		
		assertEquals(23, files7.size());
				
		for (String file: files1)
		{
			assertTrue(files7.contains(file));
		}
		
		journal.checkAndReclaimFiles();
		
		assertEquals(22, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
		
		List<String> files8 = fileFactory.listFiles(fileExtension);
		
		assertEquals(23, files8.size());
				
		for (String file: files1)
		{
			assertTrue(files8.contains(file));
		}
		
		//Commit
		
		if (commit)
		{
			commit(1);
		}
		else
		{
			rollback(1);
		}
		
		//Add more records to make sure we get to the next file
		
		for (int i = 200; i < 210; i++)
		{
			add(i);
		}
		
		assertEquals(23, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(10, journal.getIDMapSize());
		
		List<String> files9 = fileFactory.listFiles(fileExtension);
		
		assertEquals(24, files9.size());
				
		for (String file: files1)
		{
			assertTrue(files9.contains(file));
		}
		
		journal.checkAndReclaimFiles();
		
		//Most Should now be reclaimed - leaving 10 left in total

		assertEquals(1, journal.getDataFilesCount());
		assertEquals(8, journal.getFreeFilesCount());
		assertEquals(10, journal.getIDMapSize());
		
		List<String> files10 = fileFactory.listFiles(fileExtension);
		
		assertEquals(10, files10.size());	
	}
	
	public void testReclaimTransactionalSimple() throws Exception
	{
		setup(2, 1054, true);
		createJournal();
		startJournal();
		load();
		
		List<String> files1 = fileFactory.listFiles(fileExtension);
		
		assertEquals(2, files1.size());
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(1, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
					
		addTx(1, 1);           // in file 0
		
		deleteTx(1, 1);        // in file 1
		
		List<String> files2 = fileFactory.listFiles(fileExtension);
		
		assertEquals(2, files2.size());
		
		assertEquals(1, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
		
		//Make sure we move on to the next file
		
		add(2);                // in file 2
		
		List<String> files3 = fileFactory.listFiles(fileExtension);
		
		assertEquals(3, files3.size());
		
		assertEquals(2, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(1, journal.getIDMapSize());
		
		commit(1);            // in file 3
		
		List<String> files4 = fileFactory.listFiles(fileExtension);
		
		assertEquals(4, files4.size());
		
		assertEquals(3, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(1, journal.getIDMapSize());
		
		//Make sure we move on to the next file
		
		add(3);                // in file 4
		
		List<String> files5 = fileFactory.listFiles(fileExtension);
		
		assertEquals(5, files5.size());
		
		assertEquals(4, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(2, journal.getIDMapSize());
		
		journal.checkAndReclaimFiles();
		
		List<String> files6 = fileFactory.listFiles(fileExtension);
		
		//Three should get deleted (files 0, 1, 3)
		
		assertEquals(2, files6.size());
		
		assertEquals(1, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(2, journal.getIDMapSize());		
		
		//Now restart
		
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
		
		assertEquals(2, files6.size());
		
		assertEquals(1, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(2, journal.getIDMapSize());		
	}
	
	public void testAddDeleteCommitTXIDMap1() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		
		List<String> files1 = fileFactory.listFiles(fileExtension);
		
		assertEquals(10, files1.size());
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(9, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
		
		addTx(1, 1);
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(9, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
		
		deleteTx(1, 1);
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(9, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
		
		commit(1);
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(9, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());				
	}
	
	public void testAddCommitTXIDMap1() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		
		List<String> files1 = fileFactory.listFiles(fileExtension);
		
		assertEquals(10, files1.size());
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(9, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
		
		addTx(1, 1);
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(9, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
						
		commit(1);
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(9, journal.getFreeFilesCount());
		assertEquals(1, journal.getIDMapSize());				
	}
	
	public void testAddDeleteCommitTXIDMap2() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		
		List<String> files1 = fileFactory.listFiles(fileExtension);
		
		assertEquals(10, files1.size());
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(9, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
		
		add(1, 1);
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(9, journal.getFreeFilesCount());
		assertEquals(1, journal.getIDMapSize());
		
		deleteTx(1, 1);
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(9, journal.getFreeFilesCount());
		assertEquals(1, journal.getIDMapSize());
		
		commit(1);
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(9, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());				
	}
	
				
	public void testAddDeleteRollbackTXIDMap1() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		
		List<String> files1 = fileFactory.listFiles(fileExtension);
		
		assertEquals(10, files1.size());
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(9, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
		
		addTx(1, 1);
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(9, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
		
		deleteTx(1, 1);
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(9, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
		
		rollback(1);
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(9, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());				
	}
	
	public void testAddRollbackTXIDMap1() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		
		List<String> files1 = fileFactory.listFiles(fileExtension);
		
		assertEquals(10, files1.size());
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(9, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
		
		addTx(1, 1);
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(9, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
					
		rollback(1);
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(9, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());				
	}
	
	public void testAddDeleteRollbackTXIDMap2() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		
		List<String> files1 = fileFactory.listFiles(fileExtension);
		
		assertEquals(10, files1.size());
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(9, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
		
		add(1, 1);
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(9, journal.getFreeFilesCount());
		assertEquals(1, journal.getIDMapSize());
		
		deleteTx(1, 1);
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(9, journal.getFreeFilesCount());
		assertEquals(1, journal.getIDMapSize());
		
		rollback(1);
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(9, journal.getFreeFilesCount());
		assertEquals(1, journal.getIDMapSize());				
	}
	
	public void testAddDeleteIDMap() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		
		List<String> files1 = fileFactory.listFiles(fileExtension);
		
		assertEquals(10, files1.size());
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(9, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
		
		add(1);
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(9, journal.getFreeFilesCount());
		assertEquals(1, journal.getIDMapSize());
		
		delete(1);
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(9, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
		
	}
	
	public void testCommitRecordsInFileReclaim() throws Exception
	{
		setup(2, 1054, true);
		createJournal();
		startJournal();
		load();
		
		List<String> files1 = fileFactory.listFiles(fileExtension);
		
		assertEquals(2, files1.size());
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(1, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
					
		addTx(1, 1);
		
		List<String> files2 = fileFactory.listFiles(fileExtension);
		
		assertEquals(2, files2.size());
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(1, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
						
		//Make sure we move on to the next file
		
		commit(1);
		
		List<String> files3 = fileFactory.listFiles(fileExtension);
		
		assertEquals(2, files3.size());
		
		assertEquals(1, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(1, journal.getIDMapSize());
		
		add(2);
		
		//Move on to another file
		
		List<String> files4 = fileFactory.listFiles(fileExtension);
		
		assertEquals(3, files4.size());
		
		assertEquals(2, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(2, journal.getIDMapSize());
		
		journal.checkAndReclaimFiles();
		
		//Nothing should be reclaimed
		
		List<String> files5 = fileFactory.listFiles(fileExtension);
		
		assertEquals(3, files5.size());
		
		assertEquals(2, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(2, journal.getIDMapSize());		
	}
	
	
	// file 1: add 1 tx,
	// file 2: commit 1, add 2, delete 2
	// file 3: add 3
		
	public void testCommitRecordsInFileNoReclaim() throws Exception
	{
		setup(2, 1300, true);
		createJournal();
		startJournal();
		load();
		
		List<String> files1 = fileFactory.listFiles(fileExtension);
		
		assertEquals(2, files1.size());
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(1, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
					
		addTx(1, 1);           // in file 0
						
		//Make sure we move on to the next file
		
		add(2);               // in file 1
		
    	List<String> files2 = fileFactory.listFiles(fileExtension);
		
		assertEquals(2, files2.size());
		
		assertEquals(1, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(1, journal.getIDMapSize());
		
		commit(1);            // in file 1
		
		List<String> files3 = fileFactory.listFiles(fileExtension);
		
		assertEquals(2, files3.size());
		
		assertEquals(1, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(2, journal.getIDMapSize());
		
		delete(2);            // in file 1
		
		List<String> files4 = fileFactory.listFiles(fileExtension);
		
		assertEquals(2, files4.size());
		
		assertEquals(1, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(1, journal.getIDMapSize());
		
		//Move on to another file
		
		add(3);               // in file 2
		
		List<String> files5 = fileFactory.listFiles(fileExtension);
		
		assertEquals(3, files5.size());
		
		assertEquals(2, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(2, journal.getIDMapSize());
				
		journal.checkAndReclaimFiles();
		
		List<String> files6 = fileFactory.listFiles(fileExtension);
		
		assertEquals(3, files6.size());
		
		assertEquals(2, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(2, journal.getIDMapSize());	
		
		//Restart
		
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
		
		List<String> files7 = fileFactory.listFiles(fileExtension);
		
		assertEquals(3, files7.size());
		
		assertEquals(2, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(2, journal.getIDMapSize());
	}
	
	public void testRollbackRecordsInFileNoReclaim() throws Exception
	{
		setup(2, 1300, true);
		createJournal();
		startJournal();
		load();
		
		List<String> files1 = fileFactory.listFiles(fileExtension);
		
		assertEquals(2, files1.size());
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(1, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
					
		addTx(1, 1);          // in file 0
						
		//Make sure we move on to the next file
		
		add(2);               // in file 1
		
    	List<String> files2 = fileFactory.listFiles(fileExtension);
		
		assertEquals(2, files2.size());
		
		assertEquals(1, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(1, journal.getIDMapSize());
		
		rollback(1);          // in file 1
		
		List<String> files3 = fileFactory.listFiles(fileExtension);
		
		assertEquals(2, files3.size());
		
		assertEquals(1, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(1, journal.getIDMapSize());
		
		delete(2);            // in file 1
		
		List<String> files4 = fileFactory.listFiles(fileExtension);
		
		assertEquals(2, files4.size());
		
		assertEquals(1, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
		
		//Move on to another file
		
		add(3);                // in file 2 (current file)
		
		List<String> files5 = fileFactory.listFiles(fileExtension);
		
		assertEquals(3, files5.size());
		
		assertEquals(2, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(1, journal.getIDMapSize());
				
		journal.checkAndReclaimFiles();
		
		List<String> files6 = fileFactory.listFiles(fileExtension);
		
		// files 0 and 1 should be deleted
		
		assertEquals(2, files6.size());
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(1, journal.getFreeFilesCount());
		assertEquals(1, journal.getIDMapSize());	
		
		//Restart
		
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
		
		List<String> files7 = fileFactory.listFiles(fileExtension);
		
		assertEquals(2, files7.size());
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(1, journal.getFreeFilesCount());
		assertEquals(1, journal.getIDMapSize());
	}
	
	public void testPrepareNoReclaim() throws Exception
	{
		setup(2, 1300, true);
		createJournal();
		startJournal();
		load();
		
		List<String> files1 = fileFactory.listFiles(fileExtension);
		
		assertEquals(2, files1.size());
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(1, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
					
		addTx(1, 1);          // in file 0
						
		//Make sure we move on to the next file
		
		add(2);               // in file 1
		
    	List<String> files2 = fileFactory.listFiles(fileExtension);
		
		assertEquals(2, files2.size());
		
		assertEquals(1, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(1, journal.getIDMapSize());
		
		prepare(1);          // in file 1
		
		List<String> files3 = fileFactory.listFiles(fileExtension);
		
		assertEquals(2, files3.size());
		
		assertEquals(1, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(1, journal.getIDMapSize());
		
		delete(2);            // in file 1
		
		List<String> files4 = fileFactory.listFiles(fileExtension);
		
		assertEquals(2, files4.size());
		
		assertEquals(1, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
		
		//Move on to another file
		
		add(3);                // in file 2
		
		List<String> files5 = fileFactory.listFiles(fileExtension);
		
		assertEquals(3, files5.size());
		
		assertEquals(2, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(1, journal.getIDMapSize());
				
		journal.checkAndReclaimFiles();
		
		List<String> files6 = fileFactory.listFiles(fileExtension);
		
		assertEquals(3, files6.size());
		
		assertEquals(2, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(1, journal.getIDMapSize());
		
		add(4);		// in file 3
		
		List<String> files7 = fileFactory.listFiles(fileExtension);
		
		assertEquals(4, files7.size());
		
		assertEquals(3, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(2, journal.getIDMapSize());
		
		commit(1);   // in file 4
		
		List<String> files8 = fileFactory.listFiles(fileExtension);
		
		assertEquals(4, files8.size());
		
		assertEquals(3, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(3, journal.getIDMapSize());
		
		//Restart
		
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
	
	}
	
	public void testPrepareReclaim() throws Exception
	{
		setup(2, 1300, true);
		createJournal();
		startJournal();
		load();
		
		List<String> files1 = fileFactory.listFiles(fileExtension);
		
		assertEquals(2, files1.size());
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(1, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
					
		addTx(1, 1);          // in file 0
		
	   files1 = fileFactory.listFiles(fileExtension);
		
		assertEquals(2, files1.size());
		
		assertEquals(0, journal.getDataFilesCount());
		assertEquals(1, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
						
		//Make sure we move on to the next file
		
		add(2);               // in file 1
		
    	List<String> files2 = fileFactory.listFiles(fileExtension);
		
		assertEquals(2, files2.size());
		
		assertEquals(1, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(1, journal.getIDMapSize());
		
		prepare(1);          // in file 1
		
		List<String> files3 = fileFactory.listFiles(fileExtension);
		
		assertEquals(2, files3.size());
		
		assertEquals(1, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(1, journal.getIDMapSize());
		
		delete(2);            // in file 1
		
		List<String> files4 = fileFactory.listFiles(fileExtension);
		
		assertEquals(2, files4.size());
		
		assertEquals(1, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(0, journal.getIDMapSize());
		
		//Move on to another file
		
		add(3);                // in file 2
		
		journal.checkAndReclaimFiles();
				
		List<String> files5 = fileFactory.listFiles(fileExtension);
		
		assertEquals(3, files5.size());
		
		assertEquals(2, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(1, journal.getIDMapSize());
				
		journal.checkAndReclaimFiles();
		
		List<String> files6 = fileFactory.listFiles(fileExtension);
		
		assertEquals(3, files6.size());
		
		assertEquals(2, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(1, journal.getIDMapSize());
		
		add(4);		// in file 3
		
		List<String> files7 = fileFactory.listFiles(fileExtension);
		
		assertEquals(4, files7.size());
		
		assertEquals(3, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(2, journal.getIDMapSize());
		
		commit(1);   // in file 3
		
		List<String> files8 = fileFactory.listFiles(fileExtension);
		
		assertEquals(4, files8.size());
		
		assertEquals(3, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(3, journal.getIDMapSize());
		
		delete(1);   // in file 3
		
		List<String> files9 = fileFactory.listFiles(fileExtension);
		
		assertEquals(4, files9.size());
		
		assertEquals(3, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(2, journal.getIDMapSize());
		
		journal.checkAndReclaimFiles();
		
		List<String> files10 = fileFactory.listFiles(fileExtension);
		
		assertEquals(4, files10.size());
		
		assertEquals(3, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(2, journal.getIDMapSize());
		
		add(5);       // in file 4
		
		List<String> files11 = fileFactory.listFiles(fileExtension);
		
		assertEquals(5, files11.size());
		
		assertEquals(4, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(3, journal.getIDMapSize());
		
		journal.checkAndReclaimFiles();
		
		List<String> files12 = fileFactory.listFiles(fileExtension);
		
		//File 0, and File 1  should be deleted
		
		assertEquals(3, files12.size());
		
		assertEquals(2, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(3, journal.getIDMapSize());
		
		
		//Restart
		
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
		
		delete(4);
		
		List<String> files13 = fileFactory.listFiles(fileExtension);
		
		assertEquals(3, files13.size());
		
		assertEquals(2, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(2, journal.getIDMapSize());
		
		add(6);
		
		List<String> files14 = fileFactory.listFiles(fileExtension);
		
		assertEquals(4, files14.size());
		
		assertEquals(3, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(3, journal.getIDMapSize());
		
		journal.checkAndReclaimFiles();
		
		//file 3 should now be deleted
		
		List<String> files15 = fileFactory.listFiles(fileExtension);
		
		assertEquals(3, files15.size());
		
		assertEquals(2, journal.getDataFilesCount());
		assertEquals(0, journal.getFreeFilesCount());
		assertEquals(3, journal.getIDMapSize());
		
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();	
	}
	
	// Non transactional tests
	// =======================
	
	public void testSimpleAdd() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		add(1);	
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
	}
	
	public void testMultipleAdd() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		add(1,2,3,4,5,6,7,8,9,10);		
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
	}
	
	public void testMultipleAddNonContiguous() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		add(1,3,5,7,10,13,56,100,102,200,201,202,203);		
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
	}
	
	public void testSimpleAddUpdate() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		add(1);		
		update(1);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
	}
	
	public void testMultipleAddUpdate() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		add(1,2,3,4,5,6,7,8,9,10);		
		update(1,2,4,7,9,10);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
	}
	
	public void testMultipleAddUpdateAll() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		add(1,2,3,4,5,6,7,8,9,10);		
		update(1,2,3,4,5,6,7,8,9,10);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
	}
	
	public void testMultipleAddUpdateNonContiguous() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		add(1,3,5,7,10,13,56,100,102,200,201,202,203);	
		add(3,7,10,13,56,100,200,202,203);	
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
	}
	
	public void testMultipleAddUpdateAllNonContiguous() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		add(1,3,5,7,10,13,56,100,102,200,201,202,203);
		update(1,3,5,7,10,13,56,100,102,200,201,202,203);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
	}
		
	public void testSimpleAddUpdateDelete() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		add(1);		
		update(1);
		delete(1);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
	}
	
	public void testMultipleAddUpdateDelete() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		add(1,2,3,4,5,6,7,8,9,10);		
		update(1,2,4,7,9,10);
		delete(1,4,7,9,10);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
	}
	
	public void testMultipleAddUpdateDeleteAll() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		add(1,2,3,4,5,6,7,8,9,10);		
		update(1,2,3,4,5,6,7,8,9,10);
		update(1,2,3,4,5,6,7,8,9,10);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
	}
	
	public void testMultipleAddUpdateDeleteNonContiguous() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		add(1,3,5,7,10,13,56,100,102,200,201,202,203);	
		add(3,7,10,13,56,100,200,202,203);	
		delete(3,10,56,100,200,203);	
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
	}
	
	public void testMultipleAddUpdateDeleteAllNonContiguous() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		add(1,3,5,7,10,13,56,100,102,200,201,202,203);
		update(1,3,5,7,10,13,56,100,102,200,201,202,203);
		delete(1,3,5,7,10,13,56,100,102,200,201,202,203);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();		
	}
	
	public void testMultipleAddUpdateDeleteDifferentOrder() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		add(1,3,5,7,10,13,56,100,102,200,201,202,203);
		update(203, 202, 201, 200, 102, 100, 1, 3, 5, 7, 10, 13, 56);
		delete(56, 13, 10, 7, 5, 3, 1, 203, 202, 201, 200, 102, 100);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();		
	}
		
	public void testMultipleAddUpdateDeleteDifferentRecordLengths() throws Exception
	{
		setup(10, 2048, true);
		createJournal();
		startJournal();
		load();
		
		for (int i = 0; i < 1000; i++)
		{
			byte[] record = generateRecord(10 + (int)(1500 * Math.random()));
			
			journal.appendAddRecord(i, record);
			
			records.add(new RecordInfo(i, record, false));
		}
		
		for (int i = 0; i < 1000; i++)
		{
			byte[] record = generateRecord(10 + (int)(1024 * Math.random()));
			
			journal.appendUpdateRecord(i, record);
			
			records.add(new RecordInfo(i, record, true));
		}
		
		for (int i = 0; i < 1000; i++)
		{
			journal.appendDeleteRecord(i);
			
			removeRecordsForID(i);
		}
		
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
		stopJournal();			
	}
	
	
	public void testAddUpdateDeleteRestartAndContinue() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		add(1, 2, 3);
		update(1, 2);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
		add(4, 5, 6);
		update(5);
		delete(3);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
		add(7, 8);
		delete(1, 2);
		delete(4, 5, 6);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
	}
	
	public void testAddUpdateDeleteTransactionalRestartAndContinue() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		add(1, 2, 3);
		updateTx(1, 1, 2);
		commit(1);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
		addTx(2, 4, 5, 6);
		update(2, 2);
		delete(2, 3);
		commit(2);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
		addTx(3, 7, 8);
		deleteTx(3, 1);
		deleteTx(3, 4, 5, 6);
		commit(3);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
	}
	
	public void testFillFileExactly() throws Exception
	{		
		this.recordLength = 500;
		
		int numRecords = 2;
		
		//The real appended record size in the journal file = SIZE_BYTE + SIZE_LONG + SIZE_INT + recordLength + SIZE_BYTE
		
		int realLength = 1 + 8 + 4 + this.recordLength + 1;
		
		int fileSize = numRecords * realLength + 8; //8 for timestamp
						
		setup(10, fileSize, true);
		
		createJournal();
		startJournal();
		load();
		
		add(1, 2);
		
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
		
		add(3, 4);
		
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
		
		add(4, 5, 6, 7, 8, 9, 10);
		
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();		
	}
	
	// Transactional tests
	// ===================
	
	public void testSimpleTransaction() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		addTx(1, 1);
		updateTx(1, 1);		
		deleteTx(1, 1);	
		commit(1);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();		
	}
	
	public void testTransactionDontDeleteAll() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		addTx(1, 1, 2, 3);
		updateTx(1, 1, 2);		
		deleteTx(1, 1);	
		commit(1);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();		
	}
	
	public void testTransactionDeleteAll() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		addTx(1, 1, 2, 3);
		updateTx(1, 1, 2);		
		deleteTx(1, 1, 2, 3);
		commit(1);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();		
	}
	
	public void testTransactionUpdateFromBeforeTx() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		add(1, 2, 3);
		addTx(1, 4, 5, 6);
		updateTx(1, 1, 5);
		commit(1);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();		
	}
	
	public void testTransactionDeleteFromBeforeTx() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		add(1, 2, 3);
		addTx(1, 4, 5, 6);
		deleteTx(1, 1, 2, 3, 4, 5, 6);
		commit(1);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();		
	}
	
	public void testTransactionChangesNotVisibleOutsideTX() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		add(1, 2, 3);
		addTx(1, 4, 5, 6);
		updateTx(1, 1, 2, 4, 5);
		deleteTx(1, 1, 2, 3, 4, 5, 6);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();		
	}
	
	public void testMultipleTransactionsDifferentIDs() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		
		addTx(1, 1, 2, 3, 4, 5, 6);
		updateTx(1, 1, 3, 5);
		deleteTx(1, 1, 2, 3, 4, 5, 6);
		commit(1);
		
		addTx(2, 11, 12, 13, 14, 15, 16);
		updateTx(2, 11, 13, 15);
		deleteTx(2, 11, 12, 13, 14, 15, 16);
		commit(2);
		
		addTx(3, 21, 22, 23, 24, 25, 26);
		updateTx(3, 21, 23, 25);
		deleteTx(3, 21, 22, 23, 24, 25, 26);
		commit(3);
		
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
	}
	
	public void testMultipleInterleavedTransactionsDifferentIDs() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		
		addTx(1, 1, 2, 3, 4, 5, 6);		
		addTx(3, 21, 22, 23, 24, 25, 26);				
		updateTx(1, 1, 3, 5);		
		addTx(2, 11, 12, 13, 14, 15, 16);				
		deleteTx(1, 1, 2, 3, 4, 5, 6);						
		updateTx(2, 11, 13, 15);		
		updateTx(3, 21, 23, 25);			
		deleteTx(2, 11, 12, 13, 14, 15, 16);		
		deleteTx(3, 21, 22, 23, 24, 25, 26);
		
		commit(1);
		commit(2);
		commit(3);
		
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
	}
	
	public void testMultipleInterleavedTransactionsSameIDs() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
				
		add(1, 2, 3, 4, 5, 6, 7, 8);		
		addTx(1, 9, 10, 11, 12);		
		addTx(2, 13, 14, 15, 16, 17);		
		addTx(3, 18, 19, 20, 21, 22);		
		updateTx(1, 1, 2, 3);		
		updateTx(2, 4, 5, 6);		
		commit(2);		
		updateTx(3, 7, 8);		
		deleteTx(1, 1, 2);		
		commit(1);		
		deleteTx(3, 7, 8);		
		commit(3);
		
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
	}
	
	public void testTransactionMixed() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		add(1,3,5,7,10,13,56,100,102,200,201,202,203);		
		addTx(1, 675, 676, 677, 700, 703);
		update(1,3,5,7,10,13,56,100,102,200,201,202,203);		
		updateTx(1, 677, 700);		
		delete(1,3,5,7,10,13,56,100,102,200,201,202,203);		
		deleteTx(1, 703, 675);		
		commit(1);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();		
	}
	
	public void testTransactionAddDeleteDifferentOrder() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();		
		addTx(1, 1, 2, 3, 4, 5, 6, 7, 8, 9);					
		deleteTx(1, 9, 8, 5, 3, 7, 6, 2, 1, 4);	
		commit(1);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();		
	}
	
	public void testAddOutsideTXThenUpdateInsideTX() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();		
		add(1, 2, 3);
		updateTx(1, 1, 2, 3);
		commit(1);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
	}
	
	public void testAddOutsideTXThenDeleteInsideTX() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();		
		add(1, 2, 3);
		deleteTx(1, 1, 2, 3);
		commit(1);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
	}
	
	public void testRollback() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();		
		add(1, 2, 3);
		deleteTx(1, 1, 2, 3);
		rollback(1);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
	}
	
	public void testRollbackMultiple() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();		
		add(1, 2, 3);
		deleteTx(1, 1, 2, 3);
		addTx(2, 4, 5, 6);
		rollback(1);
		rollback(2);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
	}
	
	public void testIsolation1() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();			
		addTx(1, 1, 2, 3);		
		deleteTx(1, 1, 2, 3);		
		commit(1);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
	}
	
	public void testIsolation2() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();			
		addTx(1, 1, 2, 3);		
		try
		{
			update(1);
			fail("Should throw exception");
		}
		catch (IllegalStateException e)
		{
			//Ok
		}
		
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
	}
	
	public void testIsolation3() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();			
		addTx(1, 1, 2, 3);		
		try
		{
			delete(1);
			fail("Should throw exception");
		}
		catch (IllegalStateException e)
		{
			//Ok
		}
		
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
	}
	
	
	// XA tests
	// ========
	
	public void testXASimpleNotPrepared() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();		
		addTx(1, 1, 2, 3, 4, 5, 6, 7, 8, 9);					
		updateTx(1, 1, 2, 3, 4, 7, 8);
		deleteTx(1, 1, 2, 3, 4, 5);		
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();		
	}
	
	public void testXASimplePrepared() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();		
		addTx(1, 1, 2, 3, 4, 5, 6, 7, 8, 9);					
		updateTx(1, 1, 2, 3, 4, 7, 8);
		deleteTx(1, 1, 2, 3, 4, 5);	
		prepare(1);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();		
	}
	
	public void testXASimpleCommit() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();		
		addTx(1, 1, 2, 3, 4, 5, 6, 7, 8, 9);					
		updateTx(1, 1, 2,3, 4, 7, 8);
		deleteTx(1, 1, 2, 3, 4, 5);	
		prepare(1);
		commit(1);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();		
	}
	
	public void testXASimpleRollback() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();		
		addTx(1, 1, 2, 3, 4, 5, 6, 7, 8, 9);					
		updateTx(1, 1, 2,3, 4, 7, 8);
		deleteTx(1, 1, 2, 3, 4, 5);	
		prepare(1);
		rollback(1);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();		
	}
	
	public void testXAChangesNotVisibleNotPrepared() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();		
		add(1, 2, 3, 4, 5, 6);
		addTx(1, 7, 8, 9, 10);					
		updateTx(1, 1, 2, 3, 7, 8, 9);
		deleteTx(1, 1, 2, 3, 4, 5);	
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();		
	}
	
	public void testXAChangesNotVisiblePrepared() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();		
		add(1, 2, 3, 4, 5, 6);
		addTx(1, 7, 8, 9, 10);					
		updateTx(1, 1, 2, 3, 7, 8, 9);
		deleteTx(1, 1, 2, 3, 4, 5);	
		prepare(1);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();		
	}
	
	public void testXAChangesNotVisibleRollback() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();		
		add(1, 2, 3, 4, 5, 6);
		addTx(1, 7, 8, 9, 10);					
		updateTx(1, 1, 2, 3, 7, 8, 9);
		deleteTx(1, 1, 2, 3, 4, 5);	
		prepare(1);
		rollback(1);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();		
	}
	
	public void testXAChangesisibleCommit() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();		
		add(1, 2, 3, 4, 5, 6);
		addTx(1, 7, 8, 9, 10);					
		updateTx(1, 1, 2, 3, 7, 8, 9);
		deleteTx(1, 1, 2, 3, 4, 5);	
		prepare(1);
		commit(1);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();		
	}
	
	public void testXAMultiple() throws Exception
	{
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();		
		add(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		addTx(1, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20);
		addTx(2, 21, 22, 23, 24, 25, 26, 27);
		updateTx(1, 1, 3, 6, 11, 14, 17);
		addTx(3, 28, 29, 30, 31, 32, 33, 34, 35);
		updateTx(3, 7, 8, 9, 10);
		deleteTx(2, 4, 5, 6, 23, 25, 27);
		prepare(2);
		deleteTx(1, 1, 2, 11, 14, 15);
		prepare(1);
		deleteTx(3, 28, 31, 32, 9);
		prepare(3);
		
		commit(1);
		rollback(2);
		commit(3);
	}

}
