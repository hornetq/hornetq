/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.tests.unit.core.journal.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.core.journal.PreparedTransactionInfo;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.journal.TestableJournal;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.tests.util.UnitTestCase;

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

   protected String filePrefix = "hq";

   protected String fileExtension = "hq";

   protected SequentialFileFactory fileFactory;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      resetFileFactory();
      
      fileFactory.start();

      transactions.clear();

      records.clear();
   }

   @Override
   protected void tearDown() throws Exception
   {
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
      
      if (fileFactory != null)
      {
         fileFactory.stop();
      }

      fileFactory = null;

      journal = null;

      super.tearDown();
   }

   protected void resetFileFactory() throws Exception
   {
      fileFactory = getFileFactory();
   }

   protected void checkAndReclaimFiles() throws Exception
   {
      journal.debugWait();
      journal.checkReclaimStatus();
      journal.debugWait();
   }

   protected abstract SequentialFileFactory getFileFactory() throws Exception;

   // Private
   // ---------------------------------------------------------------------------------

   protected void setup(final int minFreeFiles, final int fileSize, final boolean sync, final int maxAIO)
   {
      minFiles = minFreeFiles;
      this.fileSize = fileSize;
      this.sync = sync;
      this.maxAIO = maxAIO;
   }

   protected void setup(final int minFreeFiles, final int fileSize, final boolean sync)
   {
      minFiles = minFreeFiles;
      this.fileSize = fileSize;
      this.sync = sync;
      maxAIO = 50;
   }

   public void createJournal() throws Exception
   {
      journal = new JournalImpl(fileSize, minFiles, 0, 0, fileFactory, filePrefix, fileExtension, maxAIO);
      journal.setAutoReclaim(false);
   }

   protected void startJournal() throws Exception
   {
      journal.start();
   }

   protected void stopJournal() throws Exception
   {
      stopJournal(true);
   }

   protected void stopJournal(final boolean reclaim) throws Exception
   {
      // We do a reclaim in here
      if (reclaim)
      {
         checkAndReclaimFiles();
      }

      journal.stop();
   }

   protected void loadAndCheck() throws Exception
   {
      loadAndCheck(false);
   }

   protected void loadAndCheck(boolean printDebugJournal) throws Exception
   {
      List<RecordInfo> committedRecords = new ArrayList<RecordInfo>();

      List<PreparedTransactionInfo> preparedTransactions = new ArrayList<PreparedTransactionInfo>();

      journal.load(committedRecords, preparedTransactions, null);

      checkRecordsEquivalent(records, committedRecords);

      if (printDebugJournal)
      {
         printJournalLists(records, committedRecords);
      }

      // check prepared transactions

      List<PreparedTransactionInfo> prepared = new ArrayList<PreparedTransactionInfo>();

      for (Map.Entry<Long, TransactionHolder> entry : transactions.entrySet())
      {
         if (entry.getValue().prepared)
         {
            PreparedTransactionInfo info = new PreparedTransactionInfo(entry.getKey(), null);

            info.records.addAll(entry.getValue().records);

            info.recordsToDelete.addAll(entry.getValue().deletes);

            prepared.add(info);
         }
      }

      checkTransactionsEquivalent(prepared, preparedTransactions);
   }

   protected void load() throws Exception
   {
      journal.load(null, null, null);
   }

   protected void add(final long... arguments) throws Exception
   {
      addWithSize(recordLength, arguments);
   }

   protected void addWithSize(final int size, final long... arguments) throws Exception
   {
      for (long element : arguments)
      {
         byte[] record = generateRecord(size);

         journal.appendAddRecord(element, (byte)0, record, sync);

         records.add(new RecordInfo(element, (byte)0, record, false));
      }

      journal.debugWait();
   }

   protected void update(final long... arguments) throws Exception
   {
      for (long element : arguments)
      {
         byte[] updateRecord = generateRecord(recordLength);

         journal.appendUpdateRecord(element, (byte)0, updateRecord, sync);

         records.add(new RecordInfo(element, (byte)0, updateRecord, true));
      }

      journal.debugWait();
   }

   protected void delete(final long... arguments) throws Exception
   {
      for (long element : arguments)
      {
         journal.appendDeleteRecord(element, sync);

         removeRecordsForID(element);
      }

      journal.debugWait();
   }

   protected void addTx(final long txID, final long... arguments) throws Exception
   {
      TransactionHolder tx = getTransaction(txID);

      for (long element : arguments)
      {
         // SIZE_BYTE + SIZE_LONG + SIZE_LONG + SIZE_INT + record.length +
         // SIZE_BYTE
         byte[] record = generateRecord(recordLength - JournalImpl.SIZE_ADD_RECORD_TX);

         journal.appendAddRecordTransactional(txID, element, (byte)0, record);

         tx.records.add(new RecordInfo(element, (byte)0, record, false));

      }

      journal.debugWait();
   }

   protected void updateTx(final long txID, final long... arguments) throws Exception
   {
      TransactionHolder tx = getTransaction(txID);

      for (long element : arguments)
      {
         byte[] updateRecord = generateRecord(recordLength - JournalImpl.SIZE_UPDATE_RECORD_TX);

         journal.appendUpdateRecordTransactional(txID, element, (byte)0, updateRecord);

         tx.records.add(new RecordInfo(element, (byte)0, updateRecord, true));
      }
      journal.debugWait();
   }

   protected void deleteTx(final long txID, final long... arguments) throws Exception
   {
      TransactionHolder tx = getTransaction(txID);

      for (long element : arguments)
      {
         journal.appendDeleteRecordTransactional(txID, element);

         tx.deletes.add(new RecordInfo(element, (byte)0, null, true));
      }

      journal.debugWait();
   }

   protected void prepare(final long txID, final EncodingSupport xid) throws Exception
   {
      TransactionHolder tx = transactions.get(txID);

      if (tx == null)
      {
         tx = new TransactionHolder();
         transactions.put(txID, tx);
      }

      if (tx.prepared)
      {
         throw new IllegalStateException("Transaction is already prepared");
      }
      journal.appendPrepareRecord(txID, xid, sync);

      tx.prepared = true;

      journal.debugWait();
   }

   protected void commit(final long txID) throws Exception
   {
      TransactionHolder tx = transactions.get(txID);

      if (tx == null)
      {
         throw new IllegalStateException("Cannot find tx " + txID);
      }

      journal.appendCommitRecord(txID, sync);

      commitTx(txID);

      journal.debugWait();
   }

   protected void rollback(final long txID) throws Exception
   {
      TransactionHolder tx = transactions.remove(txID);

      if (tx == null)
      {
         throw new IllegalStateException("Cannot find tx " + txID);
      }

      journal.appendRollbackRecord(txID, sync);

      journal.debugWait();
   }

   private void commitTx(final long txID)
   {
      TransactionHolder tx = transactions.remove(txID);

      if (tx == null)
      {
         throw new IllegalStateException("Cannot find tx " + txID);
      }

      records.addAll(tx.records);

      for (RecordInfo l : tx.deletes)
      {
         removeRecordsForID(l.id);
      }
   }

   protected void removeRecordsForID(final long id)
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

   protected TransactionHolder getTransaction(final long txID)
   {
      TransactionHolder tx = transactions.get(txID);

      if (tx == null)
      {
         tx = new TransactionHolder();

         transactions.put(txID, tx);
      }

      return tx;
   }

   protected void checkTransactionsEquivalent(final List<PreparedTransactionInfo> expected,
                                              final List<PreparedTransactionInfo> actual)
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

         Iterator<RecordInfo> iterDeletesExpected = rexpected.recordsToDelete.iterator();

         Iterator<RecordInfo> iterDeletesActual = ractual.recordsToDelete.iterator();

         while (iterDeletesExpected.hasNext())
         {
            long lexpected = iterDeletesExpected.next().id;

            long lactual = iterDeletesActual.next().id;

            assertEquals("Delete ids not same", lexpected, lactual);
         }
      }
   }

   protected void checkRecordsEquivalent(final List<RecordInfo> expected, final List<RecordInfo> actual)
   {
      if (expected.size() != actual.size())
      {
         printJournalLists(expected, actual);
      }

      assertEquals("Lists not same length", expected.size(), actual.size());

      Iterator<RecordInfo> iterExpected = expected.iterator();

      Iterator<RecordInfo> iterActual = actual.iterator();

      while (iterExpected.hasNext())
      {
         RecordInfo rexpected = iterExpected.next();

         RecordInfo ractual = iterActual.next();

         if (rexpected.id != ractual.id || rexpected.isUpdate != ractual.isUpdate)
         {
            printJournalLists(expected, actual);
         }

         assertEquals("ids not same", rexpected.id, ractual.id);

         assertEquals("type not same", rexpected.isUpdate, ractual.isUpdate);

         assertEqualsByteArrays(rexpected.data, ractual.data);
      }
   }

   /**
    * @param expected
    * @param actual
    */
   protected void printJournalLists(final List<RecordInfo> expected, final List<RecordInfo> actual)
   {
      System.out.println("***********************************************");
      System.out.println("Expected list:");
      for (RecordInfo info : expected)
      {
         System.out.println("Record " + info.id + " isUpdate = " + info.isUpdate);
      }
      if (actual != null)
      {
         System.out.println("***********************************************");
         System.out.println("Actual list:");
         for (RecordInfo info : actual)
         {
            System.out.println("Record " + info.id + " isUpdate = " + info.isUpdate);
         }
      }
      System.out.println("***********************************************");
   }

   protected byte[] generateRecord(final int length)
   {
      byte[] record = new byte[length];
      for (int i = 0; i < length; i++)
      {
         // record[i] = RandomUtil.randomByte();
         record[i] = getSamplebyte(i);
      }
      return record;
   }

   protected String debugJournal() throws Exception
   {
      return "***************************************************\n" + ((JournalImpl)journal).debug() +
             "***************************************************\n";
   }

   class TransactionHolder
   {
      List<RecordInfo> records = new ArrayList<RecordInfo>();

      List<RecordInfo> deletes = new ArrayList<RecordInfo>();

      boolean prepared;
   }

}
