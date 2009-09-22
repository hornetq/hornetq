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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.core.journal.LoaderCallback;
import org.hornetq.core.journal.PreparedTransactionInfo;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.journal.TransactionFailureCallback;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.tests.unit.core.journal.impl.fakes.FakeSequentialFileFactory;
import org.hornetq.tests.unit.core.journal.impl.fakes.SimpleEncoding;
import org.hornetq.tests.util.UnitTestCase;

/**
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class AlignedJournalImplTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   private static final LoaderCallback dummyLoader = new LoaderCallback()
   {

      public void addPreparedTransaction(PreparedTransactionInfo preparedTransaction)
      {
      }

      public void addRecord(RecordInfo info)
      {
      }

      public void deleteRecord(long id)
      {
      }

      public void updateRecord(RecordInfo info)
      {
      }

      public void failedTransaction(long transactionID, List<RecordInfo> records, List<RecordInfo> recordsToDelete)
      {
      }
   };

   // Attributes ----------------------------------------------------

   private SequentialFileFactory factory;

   JournalImpl journalImpl = null;

   private ArrayList<RecordInfo> records = null;
   
   private ArrayList<Long> incompleteTransactions = null;

   private ArrayList<PreparedTransactionInfo> transactions = null;

   // Static --------------------------------------------------------

   private static final Logger log = Logger.getLogger(AlignedJournalImplTest.class);

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // This test just validates basic alignment on the FakeSequentialFile itself
   public void testBasicAlignment() throws Exception
   {

      FakeSequentialFileFactory factory = new FakeSequentialFileFactory(200, true);

      SequentialFile file = factory.createSequentialFile("test1", 1);

      file.open();

      try
      {
         ByteBuffer buffer = ByteBuffer.allocateDirect(200);
         for (int i = 0; i < 200; i++)
         {
            buffer.put(i, (byte)1);
         }

         file.write(buffer, true);

         buffer = ByteBuffer.allocate(400);
         for (int i = 0; i < 400; i++)
         {
            buffer.put(i, (byte)2);
         }

         file.write(buffer, true);

         buffer = ByteBuffer.allocate(600);

         file.position(0);

         file.read(buffer);

         for (int i = 0; i < 200; i++)
         {
            assertEquals((byte)1, buffer.get(i));
         }

         for (int i = 201; i < 600; i++)
         {
            assertEquals("Position " + i, (byte)2, buffer.get(i));
         }

      }
      catch (Exception ignored)
      {
      }
   }

   public void testInconsistentAlignment() throws Exception
   {
      factory = new FakeSequentialFileFactory(512, true);

      try
      {
         journalImpl = new JournalImpl(2000, 2, 0, 0, factory, "tt", "tt", 1000);
         fail("Supposed to throw an exception");
      }
      catch (Exception ignored)
      {
      }

   }

   public void testSimpleAdd() throws Exception
   {
      final int JOURNAL_SIZE = 1060;

      setupAndLoadJournal(JOURNAL_SIZE, 10);

      journalImpl.appendAddRecord(13, (byte)14, new SimpleEncoding(1, (byte)15), false);

      journalImpl.forceMoveNextFile();

      journalImpl.checkReclaimStatus();

      setupAndLoadJournal(JOURNAL_SIZE, 10);

      assertEquals(1, records.size());

      assertEquals(13, records.get(0).id);

      assertEquals(14, records.get(0).userRecordType);

      assertEquals(1, records.get(0).data.length);

      assertEquals(15, records.get(0).data[0]);

   }

   public void testAppendAndUpdateRecords() throws Exception
   {

      final int JOURNAL_SIZE = 1060;

      setupAndLoadJournal(JOURNAL_SIZE, 10);

      assertEquals(0, records.size());
      assertEquals(0, transactions.size());

      for (int i = 0; i < 25; i++)
      {
         byte[] bytes = new byte[5];
         for (int j = 0; j < bytes.length; j++)
         {
            bytes[j] = (byte)i;
         }
         journalImpl.appendAddRecord(i * 100l, (byte)i, bytes, false);
      }

      for (int i = 25; i < 50; i++)
      {
         EncodingSupport support = new SimpleEncoding(5, (byte)i);
         journalImpl.appendAddRecord(i * 100l, (byte)i, support, false);
      }

      setupAndLoadJournal(JOURNAL_SIZE, 1024);

      assertEquals(50, records.size());

      int i = 0;
      for (RecordInfo recordItem : records)
      {
         assertEquals(i * 100l, recordItem.id);
         assertEquals(i, recordItem.getUserRecordType());
         assertEquals(5, recordItem.data.length);
         for (int j = 0; j < 5; j++)
         {
            assertEquals((byte)i, recordItem.data[j]);
         }

         i++;
      }

      for (i = 40; i < 50; i++)
      {
         byte[] bytes = new byte[10];
         for (int j = 0; j < 10; j++)
         {
            bytes[j] = (byte)'x';
         }

         journalImpl.appendUpdateRecord(i * 100l, (byte)i, bytes, false);
      }

      setupAndLoadJournal(JOURNAL_SIZE, 1024);

      i = 0;
      for (RecordInfo recordItem : records)
      {

         if (i < 50)
         {
            assertEquals(i * 100l, recordItem.id);
            assertEquals(i, recordItem.getUserRecordType());
            assertEquals(5, recordItem.data.length);
            for (int j = 0; j < 5; j++)
            {
               assertEquals((byte)i, recordItem.data[j]);
            }
         }
         else
         {
            assertEquals((i - 10) * 100l, recordItem.id);
            assertEquals(i - 10, recordItem.getUserRecordType());
            assertTrue(recordItem.isUpdate);
            assertEquals(10, recordItem.data.length);
            for (int j = 0; j < 10; j++)
            {
               assertEquals((byte)'x', recordItem.data[j]);
            }
         }

         i++;
      }

      journalImpl.stop();

   }

   public void testPartialDelete() throws Exception
   {
      final int JOURNAL_SIZE = 10000;

      setupAndLoadJournal(JOURNAL_SIZE, 100);

      journalImpl.setAutoReclaim(false);

      journalImpl.checkReclaimStatus();

      journalImpl.debugWait();

      assertEquals(2, factory.listFiles("tt").size());

      log.debug("Initial:--> " + journalImpl.debug());

      log.debug("_______________________________");

      for (int i = 0; i < 50; i++)
      {
         journalImpl.appendAddRecord(i, (byte)1, new SimpleEncoding(1, (byte)'x'), false);
      }

      journalImpl.forceMoveNextFile();

      // as the request to a new file is asynchronous, we need to make sure the
      // async requests are done
      journalImpl.debugWait();

      assertEquals(3, factory.listFiles("tt").size());

      for (int i = 10; i < 50; i++)
      {
         journalImpl.appendDeleteRecord(i, false);
      }

      journalImpl.debugWait();

      setupAndLoadJournal(JOURNAL_SIZE, 100);

      assertEquals(10, records.size());

      assertEquals(3, factory.listFiles("tt").size());

   }

   public void testAddAndDeleteReclaimWithoutTransactions() throws Exception
   {
      final int JOURNAL_SIZE = 10000;

      setupAndLoadJournal(JOURNAL_SIZE, 1);

      journalImpl.setAutoReclaim(false);

      journalImpl.checkReclaimStatus();

      journalImpl.debugWait();

      assertEquals(2, factory.listFiles("tt").size());

      log.debug("Initial:--> " + journalImpl.debug());

      log.debug("_______________________________");

      for (int i = 0; i < 50; i++)
      {
         journalImpl.appendAddRecord(i, (byte)1, new SimpleEncoding(1, (byte)'x'), false);
      }

      // as the request to a new file is asynchronous, we need to make sure the
      // async requests are done
      journalImpl.debugWait();

      assertEquals(2, factory.listFiles("tt").size());

      for (int i = 0; i < 50; i++)
      {
         journalImpl.appendDeleteRecord(i, false);
      }

      journalImpl.forceMoveNextFile();

      journalImpl.appendAddRecord(1000, (byte)1, new SimpleEncoding(1, (byte)'x'), false);

      journalImpl.debugWait();

      assertEquals(3, factory.listFiles("tt").size());

      setupAndLoadJournal(JOURNAL_SIZE, 1);

      assertEquals(1, records.size());

      assertEquals(1000, records.get(0).id);

      journalImpl.checkReclaimStatus();

      log.debug(journalImpl.debug());

      journalImpl.debugWait();

      log.debug("Final:--> " + journalImpl.debug());

      log.debug("_______________________________");

      log.debug("Files bufferSize:" + factory.listFiles("tt").size());

      assertEquals(2, factory.listFiles("tt").size());

   }

   public void testReloadWithTransaction() throws Exception
   {
      final int JOURNAL_SIZE = 2000;

      setupAndLoadJournal(JOURNAL_SIZE, 100);

      assertEquals(0, records.size());
      assertEquals(0, transactions.size());

      journalImpl.appendAddRecordTransactional(1, 1, (byte)1, new SimpleEncoding(1, (byte)1));

      setupAndLoadJournal(JOURNAL_SIZE, 100);

      assertEquals(0, records.size());
      assertEquals(0, transactions.size());

      try
      {
         journalImpl.appendCommitRecord(1l, false);
         // This was supposed to throw an exception, as the transaction was
         // forgotten (interrupted by a reload).
         fail("Supposed to throw exception");
      }
      catch (Exception e)
      {
         log.warn(e);
      }

      setupAndLoadJournal(JOURNAL_SIZE, 100);

      assertEquals(0, records.size());
      assertEquals(0, transactions.size());

   }

   public void testReloadWithInterruptedTransaction() throws Exception
   {
      final int JOURNAL_SIZE = 1100;

      setupAndLoadJournal(JOURNAL_SIZE, 100);

      journalImpl.setAutoReclaim(false);

      assertEquals(0, records.size());
      assertEquals(0, transactions.size());

      for (int i = 0; i < 10; i++)
      {
         journalImpl.appendAddRecordTransactional(77l, 1, (byte)1, new SimpleEncoding(1, (byte)1));
         journalImpl.forceMoveNextFile();
      }

      journalImpl.debugWait();

      assertEquals(12, factory.listFiles("tt").size());

      journalImpl.appendAddRecordTransactional(78l, 1, (byte)1, new SimpleEncoding(1, (byte)1));

      assertEquals(12, factory.listFiles("tt").size());

      setupAndLoadJournal(JOURNAL_SIZE, 100);

      assertEquals(0, records.size());
      assertEquals(0, transactions.size());
      assertEquals(2, incompleteTransactions.size());
      assertEquals((Long)77l, incompleteTransactions.get(0));
      assertEquals((Long)78l, incompleteTransactions.get(1));

      try
      {
         journalImpl.appendCommitRecord(77l, false);
         // This was supposed to throw an exception, as the transaction was
         // forgotten (interrupted by a reload).
         fail("Supposed to throw exception");
      }
      catch (Exception e)
      {
         log.debug("Expected exception " + e, e);
      }

      setupAndLoadJournal(JOURNAL_SIZE, 100);

      assertEquals(0, records.size());
      assertEquals(0, transactions.size());

      assertEquals(2, factory.listFiles("tt").size());

   }

   public void testReloadWithCompletedTransaction() throws Exception
   {
      final int JOURNAL_SIZE = 2000;

      setupAndLoadJournal(JOURNAL_SIZE, 100);

      assertEquals(0, records.size());
      assertEquals(0, transactions.size());

      for (int i = 0; i < 10; i++)
      {
         journalImpl.appendAddRecordTransactional(1, i, (byte)1, new SimpleEncoding(1, (byte)1));
         journalImpl.forceMoveNextFile();
      }

      journalImpl.appendCommitRecord(1l, false);

      journalImpl.debugWait();

      assertEquals(12, factory.listFiles("tt").size());

      setupAndLoadJournal(JOURNAL_SIZE, 100);

      assertEquals(10, records.size());
      assertEquals(0, transactions.size());

      journalImpl.checkReclaimStatus();

      assertEquals(10, journalImpl.getDataFilesCount());

      assertEquals(12, factory.listFiles("tt").size());

      for (int i = 0; i < 10; i++)
      {
         journalImpl.appendDeleteRecordTransactional(2l, i);
         journalImpl.forceMoveNextFile();
      }

      journalImpl.appendCommitRecord(2l, false);

      journalImpl.appendAddRecord(100, (byte)1, new SimpleEncoding(5, (byte)1), false);

      journalImpl.forceMoveNextFile();

      journalImpl.appendAddRecord(101, (byte)1, new SimpleEncoding(5, (byte)1), false);

      journalImpl.checkReclaimStatus();

      assertEquals(1, journalImpl.getDataFilesCount());

      setupAndLoadJournal(JOURNAL_SIZE, 100);

      assertEquals(1, journalImpl.getDataFilesCount());

      assertEquals(3, factory.listFiles("tt").size());
   }

   public void testTotalSize() throws Exception
   {
      final int JOURNAL_SIZE = 2000;

      setupAndLoadJournal(JOURNAL_SIZE, 100);

      assertEquals(0, records.size());
      assertEquals(0, transactions.size());

      journalImpl.appendAddRecordTransactional(1l,
                                               2l,
                                               (byte)3,
                                               new SimpleEncoding(1900 - JournalImpl.SIZE_ADD_RECORD_TX, (byte)4));

      journalImpl.appendCommitRecord(1l, false);

      journalImpl.debugWait();

      setupAndLoadJournal(JOURNAL_SIZE, 100);

      assertEquals(1, records.size());

   }

   public void testReloadInvalidCheckSizeOnTransaction() throws Exception
   {
      final int JOURNAL_SIZE = 2000;

      setupAndLoadJournal(JOURNAL_SIZE, 100);

      assertEquals(2, factory.listFiles("tt").size());

      assertEquals(0, records.size());
      assertEquals(0, transactions.size());

      for (int i = 0; i < 2; i++)
      {
         journalImpl.appendAddRecordTransactional(1l, i, (byte)0, new SimpleEncoding(1, (byte)15));
      }

      journalImpl.appendCommitRecord(1l, false);

      System.out.println("Files = " + factory.listFiles("tt"));

      SequentialFile file = factory.createSequentialFile("tt-1.tt", 1);

      file.open();

      ByteBuffer buffer = ByteBuffer.allocate(100);

      // Messing up with the first record (removing the position)
      file.position(100);

      file.read(buffer);

      // jumping RecordType, FileId, TransactionID, RecordID, VariableSize,
      // RecordType, RecordBody (that we know it is 1 )
      buffer.position(1 + 4 + 8 + 8 + 4 + 1 + 1);

      int posCheckSize = buffer.position();

      assertEquals(JournalImpl.SIZE_ADD_RECORD_TX + 1, buffer.getInt());

      buffer.position(posCheckSize);

      buffer.putInt(-1);

      buffer.rewind();

      // Changing the check bufferSize, so reload will ignore this record
      file.position(100);

      file.write(buffer, true);

      file.close();

      setupAndLoadJournal(JOURNAL_SIZE, 100);

      assertEquals(0, records.size());

      journalImpl.checkReclaimStatus();

      assertEquals(0, journalImpl.getDataFilesCount());

      assertEquals(2, factory.listFiles("tt").size());

   }

   public void testPartiallyBrokenFile() throws Exception
   {
      final int JOURNAL_SIZE = 20000;

      setupAndLoadJournal(JOURNAL_SIZE, 100);

      assertEquals(2, factory.listFiles("tt").size());

      assertEquals(0, records.size());
      assertEquals(0, transactions.size());

      for (int i = 0; i < 20; i++)
      {
         journalImpl.appendAddRecordTransactional(1l, i, (byte)0, new SimpleEncoding(1, (byte)15));
         journalImpl.appendAddRecordTransactional(2l, i + 20l, (byte)0, new SimpleEncoding(1, (byte)15));
      }

      journalImpl.appendCommitRecord(1l, false);

      journalImpl.appendCommitRecord(2l, false);

      SequentialFile file = factory.createSequentialFile("tt-1.tt", 1);

      file.open();

      ByteBuffer buffer = ByteBuffer.allocate(100);

      // Messing up with the first record (removing the position)
      file.position(100);

      file.read(buffer);

      // jumping RecordType, FileId, TransactionID, RecordID, VariableSize,
      // RecordType, RecordBody (that we know it is 1 )
      buffer.position(1 + 4 + 8 + 8 + 4 + 1 + 1);

      int posCheckSize = buffer.position();

      assertEquals(JournalImpl.SIZE_ADD_RECORD_TX + 1, buffer.getInt());

      buffer.position(posCheckSize);

      buffer.putInt(-1);

      buffer.rewind();

      // Changing the check bufferSize, so reload will ignore this record
      file.position(100);

      file.write(buffer, true);

      file.close();

      setupAndLoadJournal(JOURNAL_SIZE, 100);

      assertEquals(20, records.size());

      journalImpl.checkReclaimStatus();

   }

   public void testReduceFreeFiles() throws Exception
   {
      final int JOURNAL_SIZE = 2000;

      setupAndLoadJournal(JOURNAL_SIZE, 100, 10);

      assertEquals(10, factory.listFiles("tt").size());

      setupAndLoadJournal(JOURNAL_SIZE, 100, 2);

      assertEquals(10, factory.listFiles("tt").size());

      for (int i = 0; i < 10; i++)
      {
         journalImpl.appendAddRecord(i, (byte)0, new SimpleEncoding(1, (byte)0), false);
         journalImpl.forceMoveNextFile();
      }

      setupAndLoadJournal(JOURNAL_SIZE, 100, 2);

      assertEquals(10, records.size());

      assertEquals(12, factory.listFiles("tt").size());

      for (int i = 0; i < 10; i++)
      {
         journalImpl.appendDeleteRecord(i, false);
      }

      journalImpl.forceMoveNextFile();

      journalImpl.checkReclaimStatus();

      setupAndLoadJournal(JOURNAL_SIZE, 100, 2);

      assertEquals(0, records.size());

      assertEquals(2, factory.listFiles("tt").size());
   }

   public void testReloadIncompleteTransaction() throws Exception
   {
      final int JOURNAL_SIZE = 2000;

      setupAndLoadJournal(JOURNAL_SIZE, 100);

      assertEquals(2, factory.listFiles("tt").size());

      assertEquals(0, records.size());
      assertEquals(0, transactions.size());

      for (int i = 0; i < 10; i++)
      {
         journalImpl.appendAddRecordTransactional(1l, i, (byte)0, new SimpleEncoding(1, (byte)15));
      }

      for (int i = 10; i < 20; i++)
      {
         journalImpl.appendAddRecordTransactional(1l, i, (byte)0, new SimpleEncoding(1, (byte)15));
      }

      journalImpl.appendCommitRecord(1l, false);

      SequentialFile file = factory.createSequentialFile("tt-1.tt", 1);

      file.open();

      ByteBuffer buffer = ByteBuffer.allocate(100);

      // Messing up with the first record (removing the position)
      file.position(100);

      file.read(buffer);

      buffer.position(1);

      buffer.putInt(-1);

      buffer.rewind();

      // Messing up with the first record (changing the fileID, so Journal
      // reload will think the record came from a different journal usage)
      file.position(100);

      file.write(buffer, true);

      file.close();

      setupAndLoadJournal(JOURNAL_SIZE, 100);

      assertEquals(0, records.size());

      journalImpl.checkReclaimStatus();

      assertEquals(0, journalImpl.getDataFilesCount());

      assertEquals(2, factory.listFiles("tt").size());

   }

   public void testPrepareAloneOnSeparatedFile() throws Exception
   {
      final int JOURNAL_SIZE = 20000;

      setupAndLoadJournal(JOURNAL_SIZE, 100);

      assertEquals(0, records.size());
      assertEquals(0, transactions.size());

      for (int i = 0; i < 10; i++)
      {
         journalImpl.appendAddRecordTransactional(1l, i, (byte)0, new SimpleEncoding(1, (byte)15));
      }

      journalImpl.forceMoveNextFile();
      SimpleEncoding xidEncoding = new SimpleEncoding(10, (byte)'a');

      journalImpl.appendPrepareRecord(1l, xidEncoding, false);
      journalImpl.appendCommitRecord(1l, false);

      for (int i = 0; i < 10; i++)
      {
         journalImpl.appendDeleteRecordTransactional(2l, i);
      }

      journalImpl.appendCommitRecord(2l, false);
      journalImpl.appendAddRecord(100l, (byte)0, new SimpleEncoding(1, (byte)10), false); // Add
      // anything
      // to
      // keep
      // holding
      // the
      // file
      journalImpl.forceMoveNextFile();
      journalImpl.checkReclaimStatus();

      setupAndLoadJournal(JOURNAL_SIZE, 100);

      assertEquals(1, records.size());
   }

   public void testCommitWithMultipleFiles() throws Exception
   {
      final int JOURNAL_SIZE = 20000;

      setupAndLoadJournal(JOURNAL_SIZE, 100);

      assertEquals(0, records.size());
      assertEquals(0, transactions.size());

      for (int i = 0; i < 50; i++)
      {
         if (i == 10)
         {
            journalImpl.forceMoveNextFile();
         }
         journalImpl.appendAddRecordTransactional(1l, i, (byte)0, new SimpleEncoding(1, (byte)15));
      }

      journalImpl.appendCommitRecord(1l, false);

      for (int i = 0; i < 10; i++)
      {
         if (i == 5)
         {
            journalImpl.forceMoveNextFile();
         }
         journalImpl.appendDeleteRecordTransactional(2l, i);
      }

      journalImpl.appendCommitRecord(2l, false);
      journalImpl.forceMoveNextFile();
      journalImpl.checkReclaimStatus();

      setupAndLoadJournal(JOURNAL_SIZE, 100);

      assertEquals(40, records.size());

   }

   public void testSimplePrepare() throws Exception
   {
      final int JOURNAL_SIZE = 3 * 1024;

      setupAndLoadJournal(JOURNAL_SIZE, 1);

      assertEquals(0, records.size());
      assertEquals(0, transactions.size());

      SimpleEncoding xid = new SimpleEncoding(10, (byte)1);

      journalImpl.appendAddRecord(10l, (byte)0, new SimpleEncoding(10, (byte)0), false);

      journalImpl.appendDeleteRecordTransactional(1l, 10l, new SimpleEncoding(100, (byte)'j'));

      journalImpl.appendPrepareRecord(1, xid, false);

      journalImpl.debugWait();

      setupAndLoadJournal(JOURNAL_SIZE, 1);

      assertEquals(1, transactions.size());
      assertEquals(1, transactions.get(0).recordsToDelete.size());
      assertEquals(1, records.size());

      for (RecordInfo record : transactions.get(0).recordsToDelete)
      {
         byte[] data = record.data;
         assertEquals(100, data.length);
         for (byte element : data)
         {
            assertEquals((byte)'j', element);
         }
      }

      assertEquals(10, transactions.get(0).extraData.length);

      for (int i = 0; i < 10; i++)
      {
         assertEquals((byte)1, transactions.get(0).extraData[i]);
      }

      journalImpl.appendCommitRecord(1l, false);

      journalImpl.debugWait();

      setupAndLoadJournal(JOURNAL_SIZE, 1);

      assertEquals(0, transactions.size());
      assertEquals(0, records.size());

   }

   public void testReloadWithPreparedTransaction() throws Exception
   {
      final int JOURNAL_SIZE = 3 * 1024;

      setupAndLoadJournal(JOURNAL_SIZE, 1);

      assertEquals(0, records.size());
      assertEquals(0, transactions.size());

      for (int i = 0; i < 10; i++)
      {
         journalImpl.appendAddRecordTransactional(1, i, (byte)1, new SimpleEncoding(50, (byte)1));
         journalImpl.forceMoveNextFile();
      }

      journalImpl.debugWait();

      SimpleEncoding xid1 = new SimpleEncoding(10, (byte)1);

      journalImpl.appendPrepareRecord(1l, xid1, false);

      assertEquals(12, factory.listFiles("tt").size());

      setupAndLoadJournal(JOURNAL_SIZE, 1024);

      assertEquals(0, records.size());
      assertEquals(1, transactions.size());

      assertEquals(10, transactions.get(0).extraData.length);
      for (int i = 0; i < 10; i++)
      {
         assertEquals((byte)1, transactions.get(0).extraData[i]);
      }

      journalImpl.checkReclaimStatus();

      assertEquals(10, journalImpl.getDataFilesCount());

      assertEquals(12, factory.listFiles("tt").size());

      journalImpl.appendCommitRecord(1l, false);

      setupAndLoadJournal(JOURNAL_SIZE, 1024);

      assertEquals(10, records.size());

      journalImpl.checkReclaimStatus();

      for (int i = 0; i < 10; i++)
      {
         journalImpl.appendDeleteRecordTransactional(2l, i);
      }

      SimpleEncoding xid2 = new SimpleEncoding(15, (byte)2);

      journalImpl.appendPrepareRecord(2l, xid2, false);

      setupAndLoadJournal(JOURNAL_SIZE, 1);

      assertEquals(1, transactions.size());

      assertEquals(15, transactions.get(0).extraData.length);

      for (byte element : transactions.get(0).extraData)
      {
         assertEquals(2, element);
      }

      assertEquals(10, journalImpl.getDataFilesCount());

      assertEquals(12, factory.listFiles("tt").size());

      journalImpl.appendCommitRecord(2l, false);

      setupAndLoadJournal(JOURNAL_SIZE, 1);

      assertEquals(0, records.size());
      assertEquals(0, transactions.size());

      journalImpl.forceMoveNextFile();

      // Reclaiming should still be able to reclaim a file if a transaction was
      // ignored
      journalImpl.checkReclaimStatus();

      assertEquals(2, factory.listFiles("tt").size());

   }

   public void testReloadInvalidPrepared() throws Exception
   {
      final int JOURNAL_SIZE = 3000;

      setupAndLoadJournal(JOURNAL_SIZE, 100);

      assertEquals(0, records.size());
      assertEquals(0, transactions.size());

      for (int i = 0; i < 10; i++)
      {
         journalImpl.appendAddRecordTransactional(1, i, (byte)1, new SimpleEncoding(50, (byte)1));
      }

      journalImpl.appendPrepareRecord(1l, new SimpleEncoding(13, (byte)0), false);

      setupAndLoadJournal(JOURNAL_SIZE, 100);
      assertEquals(0, records.size());
      assertEquals(1, transactions.size());

      SequentialFile file = factory.createSequentialFile("tt-1.tt", 1);

      file.open();

      ByteBuffer buffer = ByteBuffer.allocate(100);

      // Messing up with the first record (removing the position)
      file.position(100);

      file.read(buffer);

      buffer.position(1);

      buffer.putInt(-1);

      buffer.rewind();

      // Messing up with the first record (changing the fileID, so Journal
      // reload will think the record came from a different journal usage)
      file.position(100);

      file.write(buffer, true);

      file.close();

      setupAndLoadJournal(JOURNAL_SIZE, 100);

      assertEquals(0, records.size());
      assertEquals(0, transactions.size());
   }

   public void testReclaimAfterRollabck() throws Exception
   {
      final int JOURNAL_SIZE = 2000;

      setupAndLoadJournal(JOURNAL_SIZE, 1);

      for (int i = 0; i < 10; i++)
      {
         journalImpl.appendAddRecordTransactional(1l, i, (byte)0, new SimpleEncoding(1, (byte)0));
         journalImpl.forceMoveNextFile();
      }

      journalImpl.appendRollbackRecord(1l, false);

      journalImpl.forceMoveNextFile();

      journalImpl.checkReclaimStatus();

      assertEquals(0, journalImpl.getDataFilesCount());

      setupAndLoadJournal(JOURNAL_SIZE, 1);

      assertEquals(0, journalImpl.getDataFilesCount());

      assertEquals(2, factory.listFiles("tt").size());

   }

   // It should be ok to write records on AIO, and later read then on NIO
   public void testDecreaseAlignment() throws Exception
   {
      final int JOURNAL_SIZE = 512 * 4;

      setupAndLoadJournal(JOURNAL_SIZE, 512);

      for (int i = 0; i < 10; i++)
      {
         journalImpl.appendAddRecordTransactional(1l, i, (byte)0, new SimpleEncoding(1, (byte)0));
      }

      journalImpl.appendCommitRecord(1l, false);

      setupAndLoadJournal(JOURNAL_SIZE, 100);

      assertEquals(10, records.size());

      setupAndLoadJournal(JOURNAL_SIZE, 1);

      assertEquals(10, records.size());
   }

   // It should be ok to write records on NIO, and later read then on AIO
   public void testIncreaseAlignment() throws Exception
   {
      final int JOURNAL_SIZE = 512 * 4;

      setupAndLoadJournal(JOURNAL_SIZE, 1);

      for (int i = 0; i < 10; i++)
      {
         journalImpl.appendAddRecordTransactional(1l, i, (byte)0, new SimpleEncoding(1, (byte)0));
      }

      journalImpl.appendCommitRecord(1l, false);

      setupAndLoadJournal(JOURNAL_SIZE, 100);

      assertEquals(10, records.size());

      setupAndLoadJournal(JOURNAL_SIZE, 512);

      assertEquals(10, records.size());
   }

   public void testEmptyPrepare() throws Exception
   {
      final int JOURNAL_SIZE = 512 * 4;

      setupAndLoadJournal(JOURNAL_SIZE, 1);

      journalImpl.appendPrepareRecord(2l, new SimpleEncoding(10, (byte)'j'), false);

      journalImpl.forceMoveNextFile();

      journalImpl.appendAddRecord(1l, (byte)0, new SimpleEncoding(10, (byte)'k'), false);

      setupAndLoadJournal(JOURNAL_SIZE, 1);

      assertEquals(1, journalImpl.getDataFilesCount());

      assertEquals(1, transactions.size());

      journalImpl.forceMoveNextFile();

      setupAndLoadJournal(JOURNAL_SIZE, 1);

      assertEquals(1, journalImpl.getDataFilesCount());

      assertEquals(1, transactions.size());

      journalImpl.appendCommitRecord(2l, false);

      journalImpl.appendDeleteRecord(1l, false);

      journalImpl.forceMoveNextFile();

      setupAndLoadJournal(JOURNAL_SIZE, 0);

      journalImpl.forceMoveNextFile();
      journalImpl.debugWait();
      journalImpl.checkReclaimStatus();

      assertEquals(0, transactions.size());
      assertEquals(0, journalImpl.getDataFilesCount());

   }

   public void testReclaimingAfterConcurrentAddsAndDeletes() throws Exception
   {
      final int JOURNAL_SIZE = 10 * 1024;

      setupAndLoadJournal(JOURNAL_SIZE, 1);

      assertEquals(0, records.size());
      assertEquals(0, transactions.size());

      final CountDownLatch latchReady = new CountDownLatch(2);
      final CountDownLatch latchStart = new CountDownLatch(1);
      final AtomicInteger finishedOK = new AtomicInteger(0);
      final BlockingQueue<Integer> queueDelete = new LinkedBlockingQueue<Integer>();

      final int NUMBER_OF_ELEMENTS = 500;

      Thread t1 = new Thread()
      {
         @Override
         public void run()
         {
            try
            {
               latchReady.countDown();
               latchStart.await();
               for (int i = 0; i < NUMBER_OF_ELEMENTS; i++)
               {
                  journalImpl.appendAddRecordTransactional(i, i, (byte)1, new SimpleEncoding(50, (byte)1));
                  journalImpl.appendCommitRecord(i, false);
                  queueDelete.offer(i);
               }
               finishedOK.incrementAndGet();
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }
      };

      Thread t2 = new Thread()
      {
         @Override
         public void run()
         {
            try
            {
               latchReady.countDown();
               latchStart.await();
               for (int i = 0; i < NUMBER_OF_ELEMENTS; i++)
               {
                  Integer toDelete = queueDelete.poll(10, TimeUnit.SECONDS);
                  if (toDelete == null)
                  {
                     break;
                  }
                  journalImpl.appendDeleteRecord(toDelete, false);
               }
               finishedOK.incrementAndGet();
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }
      };

      t1.start();
      t2.start();

      latchReady.await();
      latchStart.countDown();

      t1.join();
      t2.join();

      assertEquals(2, finishedOK.intValue());

      journalImpl.debugWait();

      journalImpl.forceMoveNextFile();

      journalImpl.debugWait();

      journalImpl.checkReclaimStatus();

      assertEquals(0, journalImpl.getDataFilesCount());

      assertEquals(2, factory.listFiles("tt").size());

   }

   public void testAlignmentOverReload() throws Exception
   {

      SequentialFileFactory factory = new FakeSequentialFileFactory(512, false);
      JournalImpl impl = new JournalImpl(512 + 512 * 3, 20, 0, 0, factory, "hq", "hq", 1000);

      impl.start();

      impl.load(dummyLoader);

      impl.appendAddRecord(1l, (byte)0, new SimpleEncoding(100, (byte)'a'), false);
      impl.appendAddRecord(2l, (byte)0, new SimpleEncoding(100, (byte)'b'), false);
      impl.appendAddRecord(3l, (byte)0, new SimpleEncoding(100, (byte)'b'), false);
      impl.appendAddRecord(4l, (byte)0, new SimpleEncoding(100, (byte)'b'), false);

      impl.stop();

      impl = new JournalImpl(512 + 1024 + 512, 20, 0, 0, factory, "hq", "hq", 1000);
      impl.start();
      impl.load(dummyLoader);

      // It looks silly, but this forceMoveNextFile is in place to replicate one
      // specific bug caught during development
      impl.forceMoveNextFile();

      impl.appendDeleteRecord(1l, false);
      impl.appendDeleteRecord(2l, false);
      impl.appendDeleteRecord(3l, false);
      impl.appendDeleteRecord(4l, false);

      impl.stop();

      impl = new JournalImpl(512 + 1024 + 512, 20, 0, 0, factory, "hq", "hq", 1000);
      impl.start();

      ArrayList<RecordInfo> info = new ArrayList<RecordInfo>();
      ArrayList<PreparedTransactionInfo> trans = new ArrayList<PreparedTransactionInfo>();

      impl.load(info, trans, null);

      assertEquals(0, info.size());
      assertEquals(0, trans.size());

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      records = new ArrayList<RecordInfo>();

      transactions = new ArrayList<PreparedTransactionInfo>();
      
      incompleteTransactions = new ArrayList<Long>();

      factory = null;

      journalImpl = null;

   }

   @Override
   protected void tearDown() throws Exception
   {
      if (journalImpl != null)
      {
         try
         {
            journalImpl.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

      records = null;

      transactions = null;
      
      incompleteTransactions = null;

      factory = null;

      journalImpl = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   private void setupAndLoadJournal(final int journalSize, final int alignment) throws Exception
   {
      setupAndLoadJournal(journalSize, alignment, 2);
   }

   private void setupAndLoadJournal(final int journalSize, final int alignment, final int numberOfMinimalFiles) throws Exception
   {
      if (factory == null)
      {
         factory = new FakeSequentialFileFactory(alignment, true);
      }

      if (journalImpl != null)
      {
         journalImpl.stop();
      }

      journalImpl = new JournalImpl(journalSize, numberOfMinimalFiles, 0, 0, factory, "tt", "tt", 1000);

      journalImpl.start();

      records.clear();
      transactions.clear();
      incompleteTransactions.clear();

      journalImpl.load(records, transactions, new TransactionFailureCallback()
      {
         public void failedTransaction(long transactionID, List<RecordInfo> records, List<RecordInfo> recordsToDelete)
         {
            System.out.println("records.length = " + records.size());
            incompleteTransactions.add(transactionID);
         }
         
      });
   }

   // Inner classes -------------------------------------------------

}
