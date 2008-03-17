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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.jboss.messaging.core.journal.Journal;
import org.jboss.messaging.core.journal.PreparedTransactionInfo;
import org.jboss.messaging.core.journal.RecordInfo;
import org.jboss.messaging.core.journal.SequentialFileFactory;
import org.jboss.messaging.core.journal.impl.JournalImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.test.unit.RandomUtil;
import org.jboss.messaging.test.unit.UnitTestCase;

/**
 * 
 * A JournalImplTestBase
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public abstract class JournalImplTestBase extends UnitTestCase
{
	private static final Logger log = Logger.getLogger(JournalImplTestBase.class);
	
	private List<RecordInfo> records = new LinkedList<RecordInfo>();
	
	private Journal journal;
	
	private int recordLength = 1024;
	
	private Map<Long, TransactionHolder> transactions = new LinkedHashMap<Long, TransactionHolder>();
	
	private int minFiles;
	
	private int minAvailableFiles;
	
	private int fileSize;
	
	private boolean sync;
	
	private String filePrefix = "jbm";
	
	private String fileExtension = "jbm";
	
	private SequentialFileFactory fileFactory;
						
	protected void setUp() throws Exception
	{
		super.setUp();
		
		prepareDirectory();
		
		fileFactory = getFileFactory();
	}
	
	protected void tearDown() throws Exception
	{
		super.tearDown();
		
		if (journal != null)
		{
			try
			{
				journal.stop();
			}
			catch (Exception ignore)
			{				
			}
		}
	}
	
	protected abstract void prepareDirectory() throws Exception;
	
	protected abstract SequentialFileFactory getFileFactory() throws Exception;
	
	// General tests
	// =============
	
	public void testState() throws Exception
	{
		setup(10, 10, 10 * 1024, true);
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
			new JournalImpl(JournalImpl.MIN_FILE_SIZE - 1, 10, 10, true, fileFactory, 5000, filePrefix, fileExtension);
			
			fail("Should throw exception");
		}
		catch (IllegalArgumentException e)
		{
			//Ok
		}
		
		try
		{
			new JournalImpl(10 * 1024, 1, 10, true, fileFactory, 5000, filePrefix, fileExtension);
			
			fail("Should throw exception");
		}
		catch (IllegalArgumentException e)
		{
			//Ok
		}
		
		try
		{
			new JournalImpl(10 * 1024, 10, 1, true, fileFactory, 5000, filePrefix, fileExtension);
			
			fail("Should throw exception");
		}
		catch (IllegalArgumentException e)
		{
			//Ok
		}
		
		try
		{
			new JournalImpl(10 * 1024, 10, 10, true, null, 5000, filePrefix, fileExtension);
			
			fail("Should throw exception");
		}
		catch (NullPointerException e)
		{
			//Ok
		}
		
		try
		{
			new JournalImpl(10 * 1024, 10, 10, true, fileFactory, JournalImpl.MIN_TASK_PERIOD - 1, filePrefix, fileExtension);
			
			fail("Should throw exception");
		}
		catch (IllegalArgumentException e)
		{
			//Ok
		}
		
		try
		{
			new JournalImpl(10 * 1024, 10, 10, true, fileFactory, 5000, null, fileExtension);
			
			fail("Should throw exception");
		}
		catch (NullPointerException e)
		{
			//Ok
		}
		
		try
		{
			new JournalImpl(10 * 1024, 10, 10, true, fileFactory, 5000, filePrefix, null);
			
			fail("Should throw exception");
		}
		catch (NullPointerException e)
		{
			//Ok
		}
		
	}

	// Non transactional tests
	// =======================
	
	public void testSimpleAdd() throws Exception
	{
		setup(10, 10, 10 * 1024, true);
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
		setup(10, 10, 10 * 1024, true);
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
		setup(10, 10, 10 * 1024, true);
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
		setup(10, 10, 10 * 1024, true);
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
		setup(10, 10, 10 * 1024, true);
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
		setup(10, 10, 10 * 1024, true);
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
		setup(10, 10, 10 * 1024, true);
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
		setup(10, 10, 10 * 1024, true);
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
		setup(10, 10, 10 * 1024, true);
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
		setup(10, 10, 10 * 1024, true);
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
		setup(10, 10, 10 * 1024, true);
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
		setup(10, 10, 10 * 1024, true);
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
		setup(10, 10, 10 * 1024, true);
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
		setup(10, 10, 10 * 1024, true);
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
		setup(10, 10, 2048, true);
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
	
	public void testAddUpdateDeleteManySmallFileSize() throws Exception
	{
		final int numberAdds = 10000;
		
		final int numberUpdates = 5000;
		
		final int numberDeletes = 3000;
						
		long[] adds = new long[numberAdds];
		
		for (int i = 0; i < numberAdds; i++)
		{
			adds[i] = i;
		}
		
		long[] updates = new long[numberUpdates];
		
		for (int i = 0; i < numberUpdates; i++)
		{
			updates[i] = i;
		}
		
		long[] deletes = new long[numberDeletes];
		
		for (int i = 0; i < numberDeletes; i++)
		{
			deletes[i] = i;
		}
		
		setup(10, 10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		add(adds);
		update(updates);
		delete(deletes);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
		
	}
	
	public void testAddUpdateDeleteManyLargeFileSize() throws Exception
	{
		final int numberAdds = 10000;
		
		final int numberUpdates = 5000;
		
		final int numberDeletes = 3000;
						
		long[] adds = new long[numberAdds];
		
		for (int i = 0; i < numberAdds; i++)
		{
			adds[i] = i;
		}
		
		long[] updates = new long[numberUpdates];
		
		for (int i = 0; i < numberUpdates; i++)
		{
			updates[i] = i;
		}
		
		long[] deletes = new long[numberDeletes];
		
		for (int i = 0; i < numberDeletes; i++)
		{
			deletes[i] = i;
		}
		
		setup(10, 10, 10 * 1024 * 1024, true);
		createJournal();
		startJournal();
		load();
		add(adds);
		update(updates);
		delete(deletes);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
		
	}
	
	// Transactional tests
	// ===================
	
	public void testSimpleTransaction() throws Exception
	{
		setup(10, 10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		addTx(1, false, 1);
		updateTx(1, false, 1);		
		deleteTx(1, true, 1);		
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();		
	}
	
	public void testTransactionDontDeleteAll() throws Exception
	{
		setup(10, 10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		addTx(1, false, 1, 2, 3);
		updateTx(1, false, 1, 2);		
		deleteTx(1, true, 1);		
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();		
	}
	
	public void testTransactionDeleteAll() throws Exception
	{
		setup(10, 10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		addTx(1, false, 1, 2, 3);
		updateTx(1, false, 1, 2);		
		deleteTx(1, true, 1, 2, 3);		
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();		
	}
	
	public void testTransactionUpdateFromBeforeTx() throws Exception
	{
		setup(10, 10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		add(1, 2, 3);
		addTx(1, false, 4, 5, 6);
		updateTx(1, true, 1, 5);	
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();		
	}
	
	public void testTransactionDeleteFromBeforeTx() throws Exception
	{
		setup(10, 10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		add(1, 2, 3);
		addTx(1, false, 4, 5, 6);
		deleteTx(1, true, 1, 2, 3, 4, 5, 6);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();		
	}
	
	public void testTransactionChangesNotVisibleOutsideTX() throws Exception
	{
		setup(10, 10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		add(1, 2, 3);
		addTx(1, false, 4, 5, 6);
		updateTx(1, false, 1, 2, 4, 5);
		deleteTx(1, false, 1, 2, 3, 4, 5, 6);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();		
	}
	
	public void testMultipleTransactionsDifferentIDs() throws Exception
	{
		setup(10, 10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		
		addTx(1, false, 1, 2, 3, 4, 5, 6);
		updateTx(1, false, 1, 3, 5);
		deleteTx(1, false, 1, 2, 3, 4, 5, 6);
		
		addTx(2, false, 11, 12, 13, 14, 15, 16);
		updateTx(2, false, 11, 13, 15);
		deleteTx(2, false, 11, 12, 13, 14, 15, 16);
		
		addTx(3, false, 21, 22, 23, 24, 25, 26);
		updateTx(3, false, 21, 23, 25);
		deleteTx(3, false, 21, 22, 23, 24, 25, 26);
		
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
	}
	
	public void testMultipleInterleavedTransactionsDifferentIDs() throws Exception
	{
		setup(10, 10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		
		addTx(1, false, 1, 2, 3, 4, 5, 6);
		
		addTx(3, false, 21, 22, 23, 24, 25, 26);
				
		updateTx(1, false, 1, 3, 5);
		
		addTx(2, false, 11, 12, 13, 14, 15, 16);
				
		deleteTx(1, false, 1, 2, 3, 4, 5, 6);
						
		updateTx(2, false, 11, 13, 15);
		
		updateTx(3, false, 21, 23, 25);
			
		deleteTx(2, false, 11, 12, 13, 14, 15, 16);
		
		deleteTx(3, false, 21, 22, 23, 24, 25, 26);
		
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
	}
	
	public void testMultipleInterleavedTransactionsSameIDs() throws Exception
	{
		setup(10, 10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
				
		add(1, 2, 3, 4, 5, 6, 7, 8);
		
		addTx(1, false, 9, 10, 11, 12);
		
		addTx(2, false, 13, 14, 15, 16, 17);
		
		addTx(3, false, 18, 19, 20, 21, 22);
		
		updateTx(1, false, 1, 2, 3);
		
		updateTx(2, true, 4, 5, 6);
		
		updateTx(3, false, 7, 8);
		
		deleteTx(1, true, 1, 2);
		
		deleteTx(3, true, 7, 8);
		
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
	}
	
	public void testTransactionMixed() throws Exception
	{
		setup(10, 10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		add(1,3,5,7,10,13,56,100,102,200,201,202,203);		
		addTx(1, false, 675, 676, 677, 700, 703);
		update(1,3,5,7,10,13,56,100,102,200,201,202,203);		
		updateTx(1, false, 677, 700);		
		delete(1,3,5,7,10,13,56,100,102,200,201,202,203);		
		deleteTx(1, true, 703, 675, 1,3,5,7,10);		
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();		
	}
	
	public void testTransactionAddDeleteDifferentOrder() throws Exception
	{
		setup(10, 10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();		
		addTx(1, false, 1, 2, 3, 4, 5, 6, 7, 8, 9);					
		deleteTx(1, true, 9, 8, 5, 3, 7, 6, 2, 1, 4);		
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();		
	}
	
	
	// XA tests
	// ========
	
	public void testXASimpleNotPrepared() throws Exception
	{
		setup(10, 10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();		
		this.addPrepare(1, false, 1, 2, 3, 4, 5, 6, 7, 8, 9);					
		this.updatePrepare(1, false, 1, 2, 3, 4, 7, 8);
		this.deletePrepare(1, false, 1, 2, 3, 4, 5);		
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();		
	}
	
	public void testXASimplePrepared() throws Exception
	{
		setup(10, 10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();		
		this.addPrepare(1, false, 1, 2, 3, 4, 5, 6, 7, 8, 9);					
		this.updatePrepare(1, false, 1, 2, 3, 4, 7, 8);
		this.deletePrepare(1, true, 1, 2, 3, 4, 5);		
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();		
	}
	
	public void testXASimpleCommit() throws Exception
	{
		setup(10, 10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();		
		this.addPrepare(1, false, 1, 2, 3, 4, 5, 6, 7, 8, 9);					
		this.updatePrepare(1, false, 1, 2,3, 4, 7, 8);
		this.deletePrepare(1, true, 1, 2, 3, 4, 5);	
		this.xaCommit(1);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();		
	}
	
	public void testXASimpleRollback() throws Exception
	{
		setup(10, 10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();		
		this.addPrepare(1, false, 1, 2, 3, 4, 5, 6, 7, 8, 9);					
		this.updatePrepare(1, false, 1, 2,3, 4, 7, 8);
		this.deletePrepare(1, true, 1, 2, 3, 4, 5);	
		this.xaRollback(1);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();		
	}
	
	public void testXAChangesNotVisibleNotPrepared() throws Exception
	{
		setup(10, 10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();		
		this.add(1, 2, 3, 4, 5, 6);
		this.addPrepare(1, false, 7, 8, 9, 10);					
		this.updatePrepare(1, false, 1, 2, 3, 7, 8, 9);
		this.deletePrepare(1, false, 1, 2, 3, 4, 5);	
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();		
	}
	
	public void testXAChangesNotVisiblePrepared() throws Exception
	{
		setup(10, 10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();		
		this.add(1, 2, 3, 4, 5, 6);
		this.addPrepare(1, false, 7, 8, 9, 10);					
		this.updatePrepare(1, false, 1, 2, 3, 7, 8, 9);
		this.deletePrepare(1, true, 1, 2, 3, 4, 5);	
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();		
	}
	
	public void testXAChangesNotVisibleRollback() throws Exception
	{
		setup(10, 10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();		
		this.add(1, 2, 3, 4, 5, 6);
		this.addPrepare(1, false, 7, 8, 9, 10);					
		this.updatePrepare(1, false, 1, 2, 3, 7, 8, 9);
		this.deletePrepare(1, true, 1, 2, 3, 4, 5);	
		this.xaRollback(1);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();		
	}
	
	public void testXAChangesisibleCommit() throws Exception
	{
		setup(10, 10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();		
		this.add(1, 2, 3, 4, 5, 6);
		this.addPrepare(1, false, 7, 8, 9, 10);					
		this.updatePrepare(1, false, 1, 2, 3, 7, 8, 9);
		this.deletePrepare(1, true, 1, 2, 3, 4, 5);	
		this.xaCommit(1);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();		
	}
	
	public void testXAMultiple() throws Exception
	{
		setup(10, 10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();		
		this.add(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		this.addPrepare(1, false, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20);
		this.addPrepare(2, false, 21, 22, 23, 24, 25, 26, 27);
		this.updatePrepare(1, false, 1, 3, 6, 11, 14, 17);
		this.addPrepare(3, false, 28, 29, 30, 31, 32, 33, 34, 35);
		this.updatePrepare(3, false, 7, 8, 9, 10);
		this.deletePrepare(2, true, 4, 5, 6, 23, 25, 27);
		this.deletePrepare(1, true, 1, 2, 11, 14, 15);
		this.deletePrepare(3, true, 28, 31, 32, 9);
		
		this.xaCommit(1);
		this.xaRollback(2);
		this.xaCommit(3);
	}
	
	// Private ---------------------------------------------------------------------------------
	
	private void setup(int minFiles, int minAvailableFiles, int fileSize, boolean sync)
	{		
		this.minFiles = minFiles;
		this.minAvailableFiles = minAvailableFiles;
		this.fileSize = fileSize;
		this.sync = sync;
	}
	
	public void createJournal() throws Exception
	{		
		journal =
			new JournalImpl(fileSize, minFiles, minAvailableFiles, sync, fileFactory, 5000, filePrefix, fileExtension);
	}
		
	private void startJournal() throws Exception
	{
		journal.start();
	}
	
	private void stopJournal() throws Exception
	{
		journal.stop();		
	}
	
	private void loadAndCheck() throws Exception
	{
		List<RecordInfo> committedRecords = new ArrayList<RecordInfo>();
		
		List<PreparedTransactionInfo> preparedTransactions = new ArrayList<PreparedTransactionInfo>();
		
		journal.load(committedRecords, preparedTransactions);
		
		checkRecordsEquivalent(records, committedRecords);
		
		//check prepared transactions
		
		List<PreparedTransactionInfo> prepared = new ArrayList<PreparedTransactionInfo>();
		
		for (Map.Entry<Long, TransactionHolder> entry : transactions.entrySet())
		{
			if (entry.getValue().prepared)
			{
				PreparedTransactionInfo info = new PreparedTransactionInfo(entry.getKey());
				
				info.records.addAll(entry.getValue().records);
				
				info.recordsToDelete.addAll(entry.getValue().deletes);
				
				prepared.add(info);
			}
		}
		
		checkTransactionsEquivalent(prepared, preparedTransactions);
	}
	
	
	
	private void load() throws Exception
	{
		journal.load(null, null);
	}
	
	private void add(long... arguments) throws Exception
	{
		for (int i = 0; i < arguments.length; i++)
		{		
			byte[] record = generateRecord(recordLength);
			
			journal.appendAddRecord(arguments[i], record);
			
			records.add(new RecordInfo(arguments[i], record, false));			
		}
	}
	
	private void update(long... arguments) throws Exception
	{
		for (int i = 0; i < arguments.length; i++)
		{		
			byte[] updateRecord = generateRecord(recordLength);
			
			journal.appendUpdateRecord(arguments[i], updateRecord);
			
			records.add(new RecordInfo(arguments[i], updateRecord, true));	
		}
	}
	
	private void delete(long... arguments) throws Exception
	{
		for (int i = 0; i < arguments.length; i++)
		{		
			journal.appendDeleteRecord(arguments[i]);
			
			removeRecordsForID(arguments[i]);
		}
	}
			
	private void addTx(long txID, boolean done, long... arguments) throws Exception
	{
		TransactionHolder tx = transactions.get(txID);
		
		if (tx == null)
		{
			tx = new TransactionHolder();
			
			transactions.put(txID, tx);
		}
		
		for (int i = 0; i < arguments.length; i++)
		{		
			byte[] record = generateRecord(recordLength);
			
			boolean useDone = done ? i == arguments.length - 1 : false;
			
			journal.appendAddRecordTransactional(txID, arguments[i], record, useDone);
			
			tx.records.add(new RecordInfo(arguments[i], record, false));
			
		}
		
		if (done)
		{
			commitTx(txID);
		}
	}
	
	private void addPrepare(long txID, boolean done, long... arguments) throws Exception
	{
		TransactionHolder tx = transactions.get(txID);
		
		if (tx == null)
		{
			tx = new TransactionHolder();
			
			transactions.put(txID, tx);
		}
		
		for (int i = 0; i < arguments.length; i++)
		{		
			byte[] record = generateRecord(recordLength);
			
			boolean useDone = done ? i == arguments.length - 1 : false;
			
			journal.appendAddRecordPrepare(txID, arguments[i], record, useDone);
			
			tx.records.add(new RecordInfo(arguments[i], record, false));
			
		}
		
		if (done)
		{
			tx.prepared = true;
		}
	}
	
	private void updateTx(long txID, boolean done, long... arguments) throws Exception
	{
		TransactionHolder tx = transactions.get(txID);
		
		if (tx == null)
		{
			throw new IllegalStateException("Cannot find tx " + txID);
		}
		
		for (int i = 0; i < arguments.length; i++)
		{		
			byte[] updateRecord = generateRecord(recordLength);
			
			boolean useDone = done ? i == arguments.length - 1 : false;
						
			journal.appendUpdateRecordTransactional(txID, arguments[i], updateRecord, useDone);
			
			tx.records.add(new RecordInfo(arguments[i], updateRecord, true));
		}		
		
		if (done)
		{
			commitTx(txID);
		}
	}
	
	private void updatePrepare(long txID, boolean done, long... arguments) throws Exception
	{
		TransactionHolder tx = transactions.get(txID);
		
		if (tx == null)
		{
			throw new IllegalStateException("Cannot find tx " + txID);
		}
		
		if (tx.prepared)
		{
			throw new IllegalStateException("Transaction is already prepared");
		}
		
		for (int i = 0; i < arguments.length; i++)
		{		
			byte[] updateRecord = generateRecord(recordLength);
			
			boolean useDone = done ? i == arguments.length - 1 : false;
						
			journal.appendUpdateRecordPrepare(txID, arguments[i], updateRecord, useDone);
			
			tx.records.add(new RecordInfo(arguments[i], updateRecord, true));
		}		
		
		if (done)
		{
			tx.prepared = true;
		}
	}
	
	private void deleteTx(long txID, boolean done, long... arguments) throws Exception
	{
		TransactionHolder tx = transactions.get(txID);
		
		if (tx == null)
		{
			throw new IllegalStateException("Cannot find tx " + txID);
		}
		
		for (int i = 0; i < arguments.length; i++)
		{		
			boolean useDone = done ? i == arguments.length - 1 : false;
						
			journal.appendDeleteRecordTransactional(txID, arguments[i], useDone);
			
			tx.deletes.add(arguments[i]);			
		}
		
		if (done)
		{
			commitTx(txID);
		}
	}
	
	private void deletePrepare(long txID, boolean done, long... arguments) throws Exception
	{
		TransactionHolder tx = transactions.get(txID);
		
		if (tx == null)
		{
			throw new IllegalStateException("Cannot find tx " + txID);
		}
		
		if (tx.prepared)
		{
			throw new IllegalStateException("Transaction is already prepared");
		}
		
		for (int i = 0; i < arguments.length; i++)
		{		
			boolean useDone = done ? i == arguments.length - 1 : false;
						
			journal.appendDeleteRecordPrepare(txID, arguments[i], useDone);
			
			tx.deletes.add(arguments[i]);			
		}
		
		if (done)
		{
			tx.prepared = true;
		}
	}
	
	private void xaCommit(long txID) throws Exception
	{
		TransactionHolder tx = transactions.get(txID);
		
		if (tx == null)
		{
			throw new IllegalStateException("Cannot find tx " + txID);
		}
		
		if (!tx.prepared)
		{
			throw new IllegalStateException("Transaction is not prepared");
		}
		
		journal.appendXACommitRecord(txID);
		
		this.commitTx(txID);
	}
	
	private void xaRollback(long txID) throws Exception
	{
		TransactionHolder tx = transactions.remove(txID);
		
		if (tx == null)
		{
			throw new IllegalStateException("Cannot find tx " + txID);
		}
		
		if (!tx.prepared)
		{
			throw new IllegalStateException("Transaction is not prepared");
		}
		
		journal.appendXARollbackRecord(txID);
	}
	
	private void commitTx(long txID)
	{
		TransactionHolder tx = transactions.remove(txID);
		
		if (tx == null)
		{
			throw new IllegalStateException("Cannot find tx " + txID);
		}
		
		records.addAll(tx.records);
		
		for (Long l: tx.deletes)
		{
			removeRecordsForID(l);
		}
	}
	
	private void removeRecordsForID(long id)
	{
		for (ListIterator<RecordInfo> iter = records.listIterator(); iter.hasNext();)
		{
			RecordInfo info = iter.next();
			
			if (info.id == id)
			{
				iter.remove();
			}
		}
	}
	
	
	private void checkTransactionsEquivalent(List<PreparedTransactionInfo> expected, List<PreparedTransactionInfo> actual)
	{
		assertEquals("Lists not same length", expected.size(), actual.size());
		
		Iterator<PreparedTransactionInfo> iterExpected = expected.iterator();
		
		Iterator<PreparedTransactionInfo> iterActual = actual.iterator();
		
		while (iterExpected.hasNext())
		{
			PreparedTransactionInfo rexpected = iterExpected.next();
			
			PreparedTransactionInfo ractual = iterActual.next();
			
			assertEquals("ids not same", rexpected.id, ractual.id);
			
			checkRecordsEquivalent(rexpected.records, ractual.records);
			
			assertEquals("deletes size not same", rexpected.recordsToDelete.size(), ractual.recordsToDelete.size());
			
			Iterator<Long> iterDeletesExpected = rexpected.recordsToDelete.iterator();
			
			Iterator<Long> iterDeletesActual = ractual.recordsToDelete.iterator();
			
			while (iterDeletesExpected.hasNext())
			{
				long lexpected = iterDeletesExpected.next();
				
				long lactual = iterDeletesActual.next();
				
				assertEquals("Delete ids not same", lexpected, lactual);
			}
		}
	}
	
	private void checkRecordsEquivalent(List<RecordInfo> expected, List<RecordInfo> actual)
	{
		assertEquals("Lists not same length", expected.size(), actual.size());
		
		Iterator<RecordInfo> iterExpected = expected.iterator();
		
		Iterator<RecordInfo> iterActual = actual.iterator();
		
		while (iterExpected.hasNext())
		{
			RecordInfo rexpected = iterExpected.next();
			
			RecordInfo ractual = iterActual.next();
			
			assertEquals("ids not same", rexpected.id, ractual.id);
			
			assertEquals("type not same", rexpected.isUpdate, ractual.isUpdate);
			
			assertByteArraysEquivalent(rexpected.data, ractual.data);
		}		
	}
	
	private byte[] generateRecord(int length)
	{
		byte[] record = new byte[length];
		for (int i = 0; i < length; i++)
		{
			record[i] = RandomUtil.randomByte();
		}
		return record;
	}
	
	class TransactionHolder
	{
		List<RecordInfo> records = new ArrayList<RecordInfo>();
		
		List<Long> deletes = new ArrayList<Long>();
		
		boolean prepared;
	}
	
}
