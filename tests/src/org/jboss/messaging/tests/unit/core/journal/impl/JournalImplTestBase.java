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
package org.jboss.messaging.tests.unit.core.journal.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.jboss.messaging.core.asyncio.impl.AsynchronousFileImpl;
import org.jboss.messaging.core.journal.PreparedTransactionInfo;
import org.jboss.messaging.core.journal.RecordInfo;
import org.jboss.messaging.core.journal.SequentialFileFactory;
import org.jboss.messaging.core.journal.TestableJournal;
import org.jboss.messaging.core.journal.impl.JournalImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.tests.util.RandomUtil;
import org.jboss.messaging.tests.util.UnitTestCase;

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
	
	protected List<RecordInfo> records = new LinkedList<RecordInfo>();
	
	protected TestableJournal journal;
	
	protected int recordLength = 1024;
	
	protected Map<Long, TransactionHolder> transactions = new LinkedHashMap<Long, TransactionHolder>();
	
	protected int maxAIO;
	
	protected int minFiles;
	
	protected int fileSize;
	
	protected boolean sync;
	
	protected String filePrefix = "jbm";
	
	protected String fileExtension = "jbm";
	
	protected SequentialFileFactory fileFactory;
	
	protected void setUp() throws Exception
	{
		super.setUp();
		
		resetFileFactory();
		
		transactions.clear();
		
		records.clear();
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
		
		fileFactory = null;
		
		journal = null;
		
		assertEquals(0, AsynchronousFileImpl.getTotalMaxIO());
	}
	
	protected void resetFileFactory() throws Exception
	{
		fileFactory = getFileFactory();
	}
	
	protected void checkAndReclaimFiles() throws Exception
	{
	   journal.debugWait();
	   journal.checkAndReclaimFiles();
	}
	
	protected abstract SequentialFileFactory getFileFactory() throws Exception;
	
	// Private ---------------------------------------------------------------------------------
	
   protected void setup(int minFreeFiles, int fileSize, boolean sync, int maxAIO)
   {     
      this.minFiles = minFreeFiles;
      this.fileSize = fileSize;
      this.sync = sync;
      this.maxAIO = maxAIO;
   }
   
   protected void setup(int minFreeFiles, int fileSize, boolean sync)
   {     
      this.minFiles = minFreeFiles;
      this.fileSize = fileSize;
      this.sync = sync;
      this.maxAIO = 50;
   }
   
	public void createJournal() throws Exception
	{     
		journal =
			new JournalImpl(fileSize, minFiles, sync, fileFactory, 1000, filePrefix, fileExtension, maxAIO, 120000);
	}
	
	protected void startJournal() throws Exception
	{
		journal.start();
	}
	
	protected void stopJournal() throws Exception
	{
		stopJournal(true);
	}
	
	protected void stopJournal(boolean reclaim) throws Exception
	{
		//We do a reclaim in here
		if (reclaim)
		{
			checkAndReclaimFiles();
		}
		
		journal.stop();      
	}
	
	protected void loadAndCheck() throws Exception
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
	
	protected void load() throws Exception
	{
		journal.load(null, null);
	}
	
	protected void add(long... arguments) throws Exception
	{
		addWithSize(recordLength, arguments);
	}
	
	protected void addWithSize(int size, long... arguments) throws Exception
	{
		for (int i = 0; i < arguments.length; i++)
		{     
			byte[] record = generateRecord(size);
			
			journal.appendAddRecord(arguments[i], (byte)0, record);
			
			records.add(new RecordInfo(arguments[i], (byte)0, record, false));         
		}
		
		journal.debugWait();
	}
	
	protected void update(long... arguments) throws Exception
	{
		for (int i = 0; i < arguments.length; i++)
		{     
			byte[] updateRecord = generateRecord(recordLength);
			
			journal.appendUpdateRecord(arguments[i], (byte)0, updateRecord);
			
			records.add(new RecordInfo(arguments[i], (byte)0, updateRecord, true)); 
		}
		
		journal.debugWait();
	}
	
	protected void delete(long... arguments) throws Exception
	{
		for (int i = 0; i < arguments.length; i++)
		{     
			journal.appendDeleteRecord(arguments[i]);
			
			removeRecordsForID(arguments[i]);
		}

		journal.debugWait();
	}
	
	protected void addTx(long txID, long... arguments) throws Exception
	{
		TransactionHolder tx = getTransaction(txID);
		
		for (int i = 0; i < arguments.length; i++)
		{  
			// SIZE_BYTE + SIZE_LONG + SIZE_LONG + SIZE_INT + record.length + SIZE_BYTE
			byte[] record = generateRecord(recordLength - JournalImpl.SIZE_ADD_RECORD_TX );
			
			journal.appendAddRecordTransactional(txID, (byte)0, arguments[i], record);
			
			tx.records.add(new RecordInfo(arguments[i], (byte)0, record, false));
			
		}
		
		journal.debugWait();
	}
	
	protected void updateTx(long txID, long... arguments) throws Exception
	{
		TransactionHolder tx = getTransaction(txID);
		
		for (int i = 0; i < arguments.length; i++)
		{     
			byte[] updateRecord = generateRecord(recordLength - JournalImpl.SIZE_UPDATE_RECORD_TX );
			
			journal.appendUpdateRecordTransactional(txID, (byte)0, arguments[i], updateRecord);
			
			tx.records.add(new RecordInfo(arguments[i], (byte)0, updateRecord, true));
		}     
      journal.debugWait();
	}
	
	protected void deleteTx(long txID, long... arguments) throws Exception
	{
		TransactionHolder tx = getTransaction(txID);
		
		for (int i = 0; i < arguments.length; i++)
		{                 
			journal.appendDeleteRecordTransactional(txID, arguments[i]);
			
			tx.deletes.add(arguments[i]);       
		}
		
      journal.debugWait();
	}
	
	protected void prepare(long txID) throws Exception
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
		
		journal.appendPrepareRecord(txID);
		
		tx.prepared = true;

		journal.debugWait();
	}
	
	protected void commit(long txID) throws Exception
	{
		TransactionHolder tx = transactions.get(txID);
		
		if (tx == null)
		{
			throw new IllegalStateException("Cannot find tx " + txID);
		}
		
		journal.appendCommitRecord(txID);
		
		this.commitTx(txID);
		
		journal.debugWait();
	}
	
	protected void rollback(long txID) throws Exception
	{
		TransactionHolder tx = transactions.remove(txID);
		
		if (tx == null)
		{
			throw new IllegalStateException("Cannot find tx " + txID);
		}
		
		journal.appendRollbackRecord(txID);

		journal.debugWait();
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
	
	protected void removeRecordsForID(long id)
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
	
	protected TransactionHolder getTransaction(long txID)
	{
		TransactionHolder tx = transactions.get(txID);
		
		if (tx == null)
		{
			tx = new TransactionHolder();
			
			transactions.put(txID, tx);
		}
		
		return tx;
	}
	
	protected void checkTransactionsEquivalent(List<PreparedTransactionInfo> expected, List<PreparedTransactionInfo> actual)
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
	
	protected void checkRecordsEquivalent(List<RecordInfo> expected, List<RecordInfo> actual)
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
	
	protected byte[] generateRecord(int length)
	{
		byte[] record = new byte[length];
		for (int i = 0; i < length; i++)
		{
			record[i] = RandomUtil.randomByte();
		}
		return record;
	}
	
	protected String debugJournal() throws Exception
	{
		return "***************************************************\n" + ((JournalImpl)journal).debug() + "***************************************************\n" ;
	}
	
	class TransactionHolder
	{
		List<RecordInfo> records = new ArrayList<RecordInfo>();
		
		List<Long> deletes = new ArrayList<Long>();
		
		boolean prepared;
	}
	
}
