/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.messaging.core.journal.EncodingSupport;
import org.jboss.messaging.core.journal.LoadManager;
import org.jboss.messaging.core.journal.PreparedTransactionInfo;
import org.jboss.messaging.core.journal.RecordInfo;
import org.jboss.messaging.core.journal.SequentialFile;
import org.jboss.messaging.core.journal.SequentialFileFactory;
import org.jboss.messaging.core.journal.impl.JournalImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.tests.unit.core.journal.impl.fakes.ByteArrayEncoding;
import org.jboss.messaging.tests.unit.core.journal.impl.fakes.FakeSequentialFileFactory;
import org.jboss.messaging.tests.unit.core.journal.impl.fakes.SimpleEncoding;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class AlignedJournalImplTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   private static final LoadManager dummyLoader = new LoadManager(){

      public void addPreparedTransaction(
            PreparedTransactionInfo preparedTransaction)
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
      }};
   

   
   // Attributes ----------------------------------------------------
   
   private SequentialFileFactory factory;

   JournalImpl journalImpl = null;
   
   private ArrayList<RecordInfo> records = null;

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
         ByteBuffer buffer = ByteBuffer.allocateDirect(57);
         file.write(buffer, true);
         fail("Exception expected");
      }
      catch (Exception ignored)
      {
      }
      
      try
      {
         ByteBuffer buffer = ByteBuffer.allocateDirect(200);
         for (int i = 0; i < 200; i++)
         {
            buffer.put(i, (byte) 1);
         }
         
         file.write(buffer, true);
         
         buffer = ByteBuffer.allocate(400);
         for (int i = 0; i < 400; i++)
         {
            buffer.put(i, (byte) 2);
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
         journalImpl = new JournalImpl(2000, 2,
            true, true,
            factory, 
            "tt", "tt", 1000, 0);
         fail ("Supposed to throw an exception");
      }
      catch (Exception ignored)
      {
      }

   }
      
   public void testSimpleAdd() throws Exception
   {
      final int JOURNAL_SIZE = 1060;
      
      setupJournal(JOURNAL_SIZE, 10);
      
      journalImpl.appendAddRecord(13, (byte)14, new SimpleEncoding(1, (byte)15));
      
      journalImpl.forceMoveNextFile();
      
      journalImpl.checkAndReclaimFiles();
      
      setupJournal(JOURNAL_SIZE, 10);
      
      assertEquals(1, records.size());
      
      assertEquals(13, records.get(0).id);
      
      assertEquals(14, records.get(0).userRecordType);
      
      assertEquals(1, records.get(0).data.length);
      
      assertEquals(15, records.get(0).data[0]);
      
   }
   
   public void testAppendAndUpdateRecords() throws Exception
   {
      
      final int JOURNAL_SIZE = 1060;
      
      setupJournal(JOURNAL_SIZE, 10);
      
      assertEquals(0, records.size());
      assertEquals(0, transactions.size());

      for (int i = 0; i < 25; i++)
      {
         byte[] bytes = new byte[5];
         for (int j=0; j<bytes.length; j++)
         {
            bytes[j] = (byte)i;
         }
         journalImpl.appendAddRecord(i * 100l, (byte)i, new ByteArrayEncoding(bytes));
      }
      
      for (int i = 25; i < 50; i++)
      {
         EncodingSupport support = new SimpleEncoding(5, (byte) i);
         journalImpl.appendAddRecord(i * 100l, (byte)i, support);
      }
      
      setupJournal(JOURNAL_SIZE, 1024);
      
      assertEquals(50, records.size());
      
      int i=0;
      for (RecordInfo recordItem: records)
      {
         assertEquals(i * 100l, recordItem.id);
         assertEquals(i, recordItem.getUserRecordType());
         assertEquals(5, recordItem.data.length);
         for (int j=0;j<5;j++)
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
         
         journalImpl.appendUpdateRecord(i * 100l, (byte)i, new ByteArrayEncoding(bytes));
      }
      
      setupJournal(JOURNAL_SIZE, 1024);
      
      i=0;
      for (RecordInfo recordItem: records)
      {
         
         if (i < 50)
         {
            assertEquals(i * 100l, recordItem.id);
            assertEquals(i, recordItem.getUserRecordType());
            assertEquals(5, recordItem.data.length);
            for (int j=0;j<5;j++)
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
            for (int j=0;j<10;j++)
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
      
      setupJournal(JOURNAL_SIZE, 100);
      
      journalImpl.setAutoReclaim(false);
      
      journalImpl.checkAndReclaimFiles();
      
      journalImpl.debugWait();
      
      assertEquals(2, factory.listFiles("tt").size());
      
      log.debug("Initial:--> " + journalImpl.debug());
      
      log.debug("_______________________________");
      
      for (int i = 0; i < 50; i++)
      {
         journalImpl.appendAddRecord((long)i, (byte)1, new SimpleEncoding(1, (byte) 'x'));
      }
      
      journalImpl.forceMoveNextFile();
   
      // as the request to a new file is asynchronous, we need to make sure the async requests are done
      journalImpl.debugWait();
      
      assertEquals(3, factory.listFiles("tt").size());
      
      for (int i = 10; i < 50; i++)
      {
         journalImpl.appendDeleteRecord((long)i);
      }
      
      journalImpl.debugWait();
      
      setupJournal(JOURNAL_SIZE, 100);
      
      assertEquals(10, this.records.size());
      
      assertEquals(3, factory.listFiles("tt").size());

   }

   public void testAddAndDeleteReclaimWithoutTransactions() throws Exception
   {
      final int JOURNAL_SIZE = 10000;
      
      setupJournal(JOURNAL_SIZE, 100);
      
      journalImpl.setAutoReclaim(false);
      
      journalImpl.checkAndReclaimFiles();
      
      journalImpl.debugWait();
      
      assertEquals(2, factory.listFiles("tt").size());
      
      log.debug("Initial:--> " + journalImpl.debug());
      
      log.debug("_______________________________");
      
      for (int i = 0; i < 50; i++)
      {
         journalImpl.appendAddRecord((long)i, (byte)1, new SimpleEncoding(1, (byte) 'x'));
      }
   
      // as the request to a new file is asynchronous, we need to make sure the async requests are done
      journalImpl.debugWait();
      
      assertEquals(2, factory.listFiles("tt").size());
      
      for (int i = 0; i < 50; i++)
      {
         journalImpl.appendDeleteRecord((long)i);
      }
      
      journalImpl.forceMoveNextFile();
      
      journalImpl.appendAddRecord((long)1000, (byte)1, new SimpleEncoding(1, (byte) 'x'));
      
      journalImpl.debugWait();
      
      assertEquals(4, factory.listFiles("tt").size());


      setupJournal(JOURNAL_SIZE, 100);
      
      assertEquals(1, records.size());
      
      assertEquals(1000, records.get(0).id);
      
      journalImpl.checkAndReclaimFiles();
      
      log.debug(journalImpl.debug());
      
      journalImpl.debugWait();
      
      log.debug("Final:--> " + journalImpl.debug());
      
      log.debug("_______________________________");

      log.debug("Files size:" + factory.listFiles("tt").size());
      
      assertEquals(2, factory.listFiles("tt").size());

   }

   public void testReloadWithTransaction() throws Exception
   {
      final int JOURNAL_SIZE = 2000;
      
      setupJournal(JOURNAL_SIZE, 100);
      
      assertEquals(0, records.size());
      assertEquals(0, transactions.size());
      
      journalImpl.appendAddRecordTransactional(1, 1, (byte) 1, new SimpleEncoding(1,(byte) 1));
      
      setupJournal(JOURNAL_SIZE, 100);
      
      assertEquals(0, records.size());
      assertEquals(0, transactions.size());

      try
      {
         journalImpl.appendCommitRecord(1l);
         // This was supposed to throw an exception, as the transaction was forgotten (interrupted by a reload).
         fail("Supposed to throw exception");
      }
      catch (Exception e)
      {
         log.warn(e);
      }

      setupJournal(JOURNAL_SIZE, 100);
      
      assertEquals(0, records.size());
      assertEquals(0, transactions.size());
      
   }
   
   public void testReloadWithInterruptedTransaction() throws Exception
   {
      final int JOURNAL_SIZE = 1100;
      
      setupJournal(JOURNAL_SIZE, 100);
      
      journalImpl.setAutoReclaim(false);
      
      assertEquals(0, records.size());
      assertEquals(0, transactions.size());
      
      for (int i = 0; i < 10; i++)
      {
         journalImpl.appendAddRecordTransactional(77l, 1, (byte) 1, new SimpleEncoding(1,(byte) 1));
         journalImpl.forceMoveNextFile();
      }
      
      journalImpl.debugWait();
      
      assertEquals(12, factory.listFiles("tt").size());
      
      journalImpl.appendAddRecordTransactional(78l, 1, (byte) 1, new SimpleEncoding(1,(byte) 1));

      assertEquals(12, factory.listFiles("tt").size());
      
      setupJournal(JOURNAL_SIZE, 100);
      
      assertEquals(0, records.size());
      assertEquals(0, transactions.size());

      try
      {
         journalImpl.appendCommitRecord(77l);
         // This was supposed to throw an exception, as the transaction was forgotten (interrupted by a reload).
         fail("Supposed to throw exception");
      }
      catch (Exception e)
      {
         log.debug("Expected exception " + e, e);
      }

      setupJournal(JOURNAL_SIZE, 100);
      
      assertEquals(0, records.size());
      assertEquals(0, transactions.size());
      
      assertEquals(2, factory.listFiles("tt").size());
      
   }
   
   public void testReloadWithCompletedTransaction() throws Exception
   {
      final int JOURNAL_SIZE = 2000;
      
      setupJournal(JOURNAL_SIZE, 100);
      
      assertEquals(0, records.size());
      assertEquals(0, transactions.size());
      
      for (int i = 0; i < 10; i++)
      {
         journalImpl.appendAddRecordTransactional(1, i, (byte) 1, new SimpleEncoding(1,(byte) 1));
         journalImpl.forceMoveNextFile();
      }
      
      journalImpl.appendCommitRecord(1l);

      journalImpl.debugWait();

      assertEquals(12, factory.listFiles("tt").size());

      setupJournal(JOURNAL_SIZE, 100);

      assertEquals(10, records.size());
      assertEquals(0, transactions.size());
      
      journalImpl.checkAndReclaimFiles();
      
      assertEquals(10, journalImpl.getDataFilesCount());
      
      assertEquals(12, factory.listFiles("tt").size());
      
      for (int i = 0; i < 10; i++)
      {
         journalImpl.appendDeleteRecordTransactional(2l, (long)i);
         journalImpl.forceMoveNextFile();
      }
      
      journalImpl.appendCommitRecord(2l);
      
      journalImpl.appendAddRecord(100, (byte)1, new SimpleEncoding(5, (byte)1));
      
      journalImpl.forceMoveNextFile();
      
      journalImpl.appendAddRecord(101, (byte)1, new SimpleEncoding(5, (byte)1));
      
      journalImpl.checkAndReclaimFiles();
      
      assertEquals(1, journalImpl.getDataFilesCount());
      
      setupJournal(JOURNAL_SIZE, 100);
      
      assertEquals(1, journalImpl.getDataFilesCount());
      
      assertEquals(3, factory.listFiles("tt").size());
   }
   
   
   public void testTotalSize() throws Exception
   {
      final int JOURNAL_SIZE = 2000;
      
      setupJournal(JOURNAL_SIZE, 100);
      
      assertEquals(0, records.size());
      assertEquals(0, transactions.size());
      
      journalImpl.appendAddRecordTransactional(1l, 2l, (byte)3, new SimpleEncoding(1900 - JournalImpl.SIZE_ADD_RECORD_TX, (byte)4));
      
      journalImpl.appendCommitRecord(1l);
      
      journalImpl.debugWait();
      
      setupJournal(JOURNAL_SIZE, 100);
      
      assertEquals(1, records.size());
      
   }
   
   
   public void testReloadInvalidCheckSizeOnTransaction() throws Exception
   {
      final int JOURNAL_SIZE = 2000;
      
      setupJournal(JOURNAL_SIZE, 100);
      
      assertEquals(2, factory.listFiles("tt").size());
      
      
      assertEquals(0, records.size());
      assertEquals(0, transactions.size());
      
      for (int i = 0; i < 20 ; i++)
      {
         journalImpl.appendAddRecordTransactional(1l, (long)i, (byte)0, new SimpleEncoding(1, (byte)15));
         journalImpl.forceMoveNextFile();
      }
      
      journalImpl.forceMoveNextFile();
      
      journalImpl.appendCommitRecord(1l);
      
      SequentialFile file = factory.createSequentialFile("tt-1.tt", 1);
      
      file.open();
      
      ByteBuffer buffer = ByteBuffer.allocate(100);
      
      // Messing up with the first record (removing the position)
      file.position(100);
      
      file.read(buffer);

      // jumping RecordType, FileId, TransactionID, RecordID, VariableSize, RecordType, RecordBody (that we know it is 1 )
      buffer.position(1 + 4 + 8 + 8 + 4 + 1 + 1); 
      
      int posCheckSize = buffer.position();
      
      assertEquals(JournalImpl.SIZE_ADD_RECORD_TX + 1, buffer.getInt());
      
      buffer.position(posCheckSize);
      
      buffer.putInt(-1);
      
      buffer.rewind();
      
      // Changing the check size, so reload will ignore this record
      file.position(100);

      file.write(buffer, true);
      
      file.close();

      setupJournal(JOURNAL_SIZE, 100);
      
      assertEquals(0, records.size());
      
      journalImpl.checkAndReclaimFiles();
      
      assertEquals(0, journalImpl.getDataFilesCount());
      
      assertEquals(2, factory.listFiles("tt").size());
      
   }

   public void testPartiallyBrokenFile() throws Exception
   {
      final int JOURNAL_SIZE = 2000;
      
      setupJournal(JOURNAL_SIZE, 100);
      
      assertEquals(2, factory.listFiles("tt").size());
      
      
      assertEquals(0, records.size());
      assertEquals(0, transactions.size());
      
      for (int i = 0; i < 20 ; i++)
      {
         journalImpl.appendAddRecordTransactional(1l, (long)i, (byte)0, new SimpleEncoding(1, (byte)15));
         journalImpl.appendAddRecordTransactional(2l, (long)i + 20l, (byte)0, new SimpleEncoding(1, (byte)15));
         journalImpl.forceMoveNextFile();
      }
      
      
      journalImpl.forceMoveNextFile();
      
      journalImpl.appendCommitRecord(1l);
      
      journalImpl.appendCommitRecord(2l);
      
      SequentialFile file = factory.createSequentialFile("tt-1.tt", 1);
      
      file.open();
      
      ByteBuffer buffer = ByteBuffer.allocate(100);
      
      // Messing up with the first record (removing the position)
      file.position(100);
      
      file.read(buffer);

      // jumping RecordType, FileId, TransactionID, RecordID, VariableSize, RecordType, RecordBody (that we know it is 1 )
      buffer.position(1 + 4 + 8 + 8 + 4 + 1 + 1); 
      
      int posCheckSize = buffer.position();
      
      assertEquals(JournalImpl.SIZE_ADD_RECORD_TX + 1, buffer.getInt());
      
      buffer.position(posCheckSize);
      
      buffer.putInt(-1);
      
      buffer.rewind();
      
      // Changing the check size, so reload will ignore this record
      file.position(100);

      file.write(buffer, true);
      
      file.close();

      setupJournal(JOURNAL_SIZE, 100);
      
      assertEquals(20, records.size());
      
      journalImpl.checkAndReclaimFiles();
      
      assertEquals(20, journalImpl.getDataFilesCount());
      
      assertEquals(22, factory.listFiles("tt").size());
      
   }

   public void testReduceFreeFiles() throws Exception
   {
      final int JOURNAL_SIZE = 2000;
      
      setupJournal(JOURNAL_SIZE, 100, 10);
      
      assertEquals(10, factory.listFiles("tt").size());
      
      setupJournal(JOURNAL_SIZE, 100, 2);
      
      assertEquals(10, factory.listFiles("tt").size());
      
      for (int i = 0; i < 10; i++)
      {
         journalImpl.appendAddRecord(i, (byte)0, new SimpleEncoding(1,(byte)0));
         journalImpl.forceMoveNextFile();
      }
      
      setupJournal(JOURNAL_SIZE, 100, 2);
      
      assertEquals(10, records.size());
      
      assertEquals(12, factory.listFiles("tt").size());
      
      for (int i = 0; i < 10; i++)
      {
         journalImpl.appendDeleteRecord(i);
      }
      
      journalImpl.forceMoveNextFile();
      
      journalImpl.checkAndReclaimFiles();
      
      setupJournal(JOURNAL_SIZE, 100, 2);

      assertEquals(0, records.size());
      
      assertEquals(2, factory.listFiles("tt").size());
   }

   
   public void testReloadIncompleteTransaction() throws Exception
   {
      final int JOURNAL_SIZE = 2000;
      
      setupJournal(JOURNAL_SIZE, 100);
      
      assertEquals(2, factory.listFiles("tt").size());
      
      
      assertEquals(0, records.size());
      assertEquals(0, transactions.size());
      
      for (int i = 0; i < 10 ; i++)
      {
         journalImpl.appendAddRecordTransactional(1l, (long)i, (byte)0, new SimpleEncoding(1, (byte)15));
         journalImpl.forceMoveNextFile();
      }
      
      
      for (int i = 10; i < 20 ; i++)
      {
         journalImpl.appendAddRecordTransactional(1l, (long)i, (byte)0, new SimpleEncoding(1, (byte)15));
         journalImpl.forceMoveNextFile();
      }
      
      journalImpl.forceMoveNextFile();
      
      journalImpl.appendCommitRecord(1l);
      
      SequentialFile file = factory.createSequentialFile("tt-1.tt", 1);
      
      file.open();
      
      ByteBuffer buffer = ByteBuffer.allocate(100);
      
      // Messing up with the first record (removing the position)
      file.position(100);
      
      file.read(buffer);

      buffer.position(1);
      
      buffer.putInt(-1);
      
      buffer.rewind();
      
      // Messing up with the first record (changing the fileID, so Journal reload will think the record came from a different journal usage)
      file.position(100);

      file.write(buffer, true);
      
      file.close();

      setupJournal(JOURNAL_SIZE, 100);
      
      assertEquals(0, records.size());
      
      journalImpl.checkAndReclaimFiles();
      
      assertEquals(0, journalImpl.getDataFilesCount());
      
      assertEquals(2, factory.listFiles("tt").size());
      
   }
   
   public void testPrepareAloneOnSeparatedFile() throws Exception
   {
      final int JOURNAL_SIZE = 20000;
      
      setupJournal(JOURNAL_SIZE, 100);
      
      assertEquals(0, records.size());
      assertEquals(0, transactions.size());
      
      for (int i=0;i<10;i++)
      {
         journalImpl.appendAddRecordTransactional(1l, (long)i, (byte)0, new SimpleEncoding(1, (byte)15));
      }
      
      journalImpl.forceMoveNextFile();
		SimpleEncoding xidEncoding = new SimpleEncoding(10, (byte)'a');
		
      journalImpl.appendPrepareRecord(1l, xidEncoding);
      journalImpl.appendCommitRecord(1l);
      
      for (int i=0;i<10;i++)
      {
         journalImpl.appendDeleteRecordTransactional(2l, (long)i);
      }
      
      journalImpl.appendCommitRecord(2l);
      journalImpl.appendAddRecord(100l, (byte)0, new SimpleEncoding(1, (byte)10)); // Add anything to keep holding the file
      journalImpl.forceMoveNextFile();
      journalImpl.checkAndReclaimFiles();

      setupJournal(JOURNAL_SIZE, 100);
      
      assertEquals(1, records.size());
   }
   
   public void testCommitWithMultipleFiles() throws Exception
   {
      final int JOURNAL_SIZE = 20000;
      
      setupJournal(JOURNAL_SIZE, 100);
      
      assertEquals(0, records.size());
      assertEquals(0, transactions.size());
      
      for (int i=0;i<50;i++)
      {
         if (i==10)
         {
            journalImpl.forceMoveNextFile();
         }
         journalImpl.appendAddRecordTransactional(1l, (long)i, (byte)0, new SimpleEncoding(1, (byte)15));
      }
      
      journalImpl.appendCommitRecord(1l);
      
      for (int i=0;i<10;i++)
      {
         if (i==5)
         {
            journalImpl.forceMoveNextFile();
         }
         journalImpl.appendDeleteRecordTransactional(2l, (long)i);
      }
      
      journalImpl.appendCommitRecord(2l);
      journalImpl.forceMoveNextFile();
      journalImpl.checkAndReclaimFiles();

      setupJournal(JOURNAL_SIZE, 100);
      
      assertEquals(40, records.size());
      
   }
   
   
   public void testSimplePrepare() throws Exception
   {
      final int JOURNAL_SIZE = 3 * 1024;
      
      setupJournal(JOURNAL_SIZE, 1);
      
      assertEquals(0, records.size());
      assertEquals(0, transactions.size());
      
      journalImpl.appendAddRecordTransactional(1, 1, (byte) 1, new SimpleEncoding(50,(byte) 1));

      SimpleEncoding xid = new SimpleEncoding(10, (byte)1);
      
      journalImpl.appendPrepareRecord(1, xid);
      
      journalImpl.appendAddRecord(2l, (byte)1, new SimpleEncoding(10, (byte)1));
      
      journalImpl.debugWait();

      setupJournal(JOURNAL_SIZE, 1);
      
      assertEquals(1, transactions.size());
      assertEquals(1, records.size());
      
      assertEquals(10, transactions.get(0).extraData.length);
      
      for (int i = 0; i < 10; i++)
      {
         assertEquals((byte)1, transactions.get(0).extraData[i]);
      }
      
      
   }

   
   public void testReloadWithPreparedTransaction() throws Exception
   {
      final int JOURNAL_SIZE = 3 * 1024;
      
      setupJournal(JOURNAL_SIZE, 1);
      
      assertEquals(0, records.size());
      assertEquals(0, transactions.size());
      
      for (int i = 0; i < 10; i++)
      {
         journalImpl.appendAddRecordTransactional(1, i, (byte) 1, new SimpleEncoding(50,(byte) 1));
         journalImpl.forceMoveNextFile();
      }
      
      journalImpl.debugWait();

      SimpleEncoding xid1 = new SimpleEncoding(10, (byte)1);

      journalImpl.appendPrepareRecord(1l, xid1);

      assertEquals(12, factory.listFiles("tt").size());

      setupJournal(JOURNAL_SIZE, 1024);

      assertEquals(0, records.size());
      assertEquals(1, transactions.size());
      
      assertEquals(10, transactions.get(0).extraData.length);
      for (int i = 0; i < 10; i++)
      {
         assertEquals((byte)1, transactions.get(0).extraData[i]);
      }
      
      journalImpl.checkAndReclaimFiles();
      
      assertEquals(10, journalImpl.getDataFilesCount());
      
      assertEquals(12, factory.listFiles("tt").size());
      
      journalImpl.appendCommitRecord(1l);
      
      setupJournal(JOURNAL_SIZE, 1024);

      assertEquals(10, records.size());
      
      journalImpl.checkAndReclaimFiles();
      
      for (int i = 0; i < 10; i++)
      {
         journalImpl.appendDeleteRecordTransactional(2l, (long)i);
      }
      
      SimpleEncoding xid2 = new SimpleEncoding(15, (byte)2);

      journalImpl.appendPrepareRecord(2l, xid2);
      
      setupJournal(JOURNAL_SIZE, 1);
      
      assertEquals(1, transactions.size());
      
      assertEquals(15, transactions.get(0).extraData.length);
      
      for (int i = 0; i < transactions.get(0).extraData.length; i++)
      {
         assertEquals(2, transactions.get(0).extraData[i]);
      }

      assertEquals(10, journalImpl.getDataFilesCount());

      assertEquals(12, factory.listFiles("tt").size());
      
      journalImpl.appendCommitRecord(2l);
      
      setupJournal(JOURNAL_SIZE, 1);
      
      assertEquals(0, records.size());
      assertEquals(0, transactions.size());

      journalImpl.forceMoveNextFile();

      // Reclaiming should still be able to reclaim a file if a transaction was ignored
      journalImpl.checkAndReclaimFiles();
      
      assertEquals(2, factory.listFiles("tt").size());

      
   }
   
   public void testReloadInvalidPrepared() throws Exception
   {
      final int JOURNAL_SIZE = 3000;
      
      setupJournal(JOURNAL_SIZE, 100);
      
      assertEquals(0, records.size());
      assertEquals(0, transactions.size());
      
      for (int i = 0; i < 10; i++)
      {
         journalImpl.appendAddRecordTransactional(1, i, (byte) 1, new SimpleEncoding(50,(byte) 1));
         journalImpl.forceMoveNextFile();
      }

      journalImpl.appendPrepareRecord(1l, new SimpleEncoding(13, (byte)0));

      setupJournal(JOURNAL_SIZE, 100);
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
      
      // Messing up with the first record (changing the fileID, so Journal reload will think the record came from a different journal usage)
      file.position(100);

      file.write(buffer, true);
      
      file.close();
      
      setupJournal(JOURNAL_SIZE, 100);

      assertEquals(0, records.size());
      assertEquals(0, transactions.size());
   }
   
   public void testReclaimAfterRollabck() throws Exception
   {
      final int JOURNAL_SIZE = 2000;
      
      setupJournal(JOURNAL_SIZE, 1);
      
      
      for (int i = 0; i < 10; i++)
      {
         journalImpl.appendAddRecordTransactional(1l, i, (byte)0, new SimpleEncoding(1, (byte)0) );
         journalImpl.forceMoveNextFile();
      }
      
      journalImpl.appendRollbackRecord(1l);
      
      journalImpl.forceMoveNextFile();
      
      journalImpl.checkAndReclaimFiles();
      
      assertEquals(0, journalImpl.getDataFilesCount());
      
      setupJournal(JOURNAL_SIZE, 1);
      
      assertEquals(0, journalImpl.getDataFilesCount());
      
      assertEquals(2, factory.listFiles("tt").size());
      
   }
   
   // It should be ok to write records on AIO, and later read then on NIO
   public void testDecreaseAlignment() throws Exception
   {
      final int JOURNAL_SIZE = 512 * 4;
      
      setupJournal(JOURNAL_SIZE, 512);
      
      for (int i = 0; i < 10; i++)
      {
         journalImpl.appendAddRecordTransactional(1l, i, (byte)0, new SimpleEncoding(1, (byte)0) );
      }
      
      journalImpl.appendCommitRecord(1l);
      
      setupJournal(JOURNAL_SIZE, 100);
      
      assertEquals(10, records.size());
      
      setupJournal(JOURNAL_SIZE, 1);
      
      assertEquals(10, records.size());
   }
   
   // It should be ok to write records on NIO, and later read then on AIO
   public void testIncreaseAlignment() throws Exception
   {
      final int JOURNAL_SIZE = 512 * 4;
      
      setupJournal(JOURNAL_SIZE, 1);
      
      for (int i = 0; i < 10; i++)
      {
         journalImpl.appendAddRecordTransactional(1l, i, (byte)0, new SimpleEncoding(1, (byte)0) );
      }
      
      journalImpl.appendCommitRecord(1l);
      
      setupJournal(JOURNAL_SIZE, 100);
      
      assertEquals(10, records.size());
      
      setupJournal(JOURNAL_SIZE, 512);
      
      assertEquals(10, records.size());
   }
   
   // It should be ok to write records on NIO, and later read then on AIO
   public void testEmptyPrepare() throws Exception
   {
      final int JOURNAL_SIZE = 512 * 4;
      
      setupJournal(JOURNAL_SIZE, 1);

      journalImpl.appendPrepareRecord(2l, new SimpleEncoding(10, (byte)'j'));
      
      journalImpl.forceMoveNextFile();
      
      journalImpl.appendAddRecord(1l, (byte)0, new SimpleEncoding(10, (byte)'k'));
      
      setupJournal(JOURNAL_SIZE, 1);
      
      assertEquals(1, journalImpl.getDataFilesCount());

      assertEquals(1, transactions.size());
      
      journalImpl.forceMoveNextFile();

      setupJournal(JOURNAL_SIZE, 1);
   
      assertEquals(1, journalImpl.getDataFilesCount());

      assertEquals(1, transactions.size());
      
      journalImpl.appendCommitRecord(2l);
      
      journalImpl.appendDeleteRecord(1l);

      journalImpl.forceMoveNextFile();

      setupJournal(JOURNAL_SIZE, 0);
      
      journalImpl.forceMoveNextFile();
      journalImpl.debugWait();
      journalImpl.checkAndReclaimFiles();
      
      assertEquals(0, transactions.size());
      assertEquals(0, journalImpl.getDataFilesCount());
      
   }
   
   
   
   public void testReclaimingAfterConcurrentAddsAndDeletes() throws Exception
   {
      final int JOURNAL_SIZE = 10 * 1024;
      
      setupJournal(JOURNAL_SIZE, 1);
      
      assertEquals(0, records.size());
      assertEquals(0, transactions.size());
      
      final CountDownLatch latchReady = new CountDownLatch(2);
      final CountDownLatch latchStart = new CountDownLatch(1);
      final AtomicInteger finishedOK = new AtomicInteger(0);
      final BlockingQueue<Integer> queueDelete = new LinkedBlockingQueue<Integer>();
      
      final int NUMBER_OF_ELEMENTS = 500;
      
      
      Thread t1 = new Thread()
      {
         public void run()
         {
            try
            {
               latchReady.countDown();
               latchStart.await();
               for (int i = 0; i < NUMBER_OF_ELEMENTS; i++)
               {
                  journalImpl.appendAddRecordTransactional((long)i, i, (byte) 1, new SimpleEncoding(50,(byte) 1));
                  journalImpl.appendCommitRecord((long)i);
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
                  journalImpl.appendDeleteRecord(toDelete);
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

      journalImpl.forceMoveNextFile();
      
      journalImpl.checkAndReclaimFiles();
      
      assertEquals(0, journalImpl.getDataFilesCount());

      assertEquals(2, factory.listFiles("tt").size());

      
   }
   
   public void testAlignmentOverReload() throws Exception
   {

      SequentialFileFactory factory = new FakeSequentialFileFactory(512, false);
      JournalImpl impl = new JournalImpl(512 + 512 * 3, 20, true, false, factory, "jbm", "jbm", 1000, 0);
      
      impl.start();
      
      impl.load(dummyLoader);
      
      impl.appendAddRecord(1l, (byte)0, new SimpleEncoding(100, (byte)'a'));
      impl.appendAddRecord(2l, (byte)0, new SimpleEncoding(100, (byte)'b'));
      impl.appendAddRecord(3l, (byte)0, new SimpleEncoding(100, (byte)'b'));
      impl.appendAddRecord(4l, (byte)0, new SimpleEncoding(100, (byte)'b'));

      impl.stop();

      impl = new JournalImpl(512 + 1024 + 512, 20, true, false, factory, "jbm", "jbm", 1000, 0);
      impl.start();
      impl.load(dummyLoader);
      
      
      // It looks silly, but this forceMoveNextFile is in place to replicate one specific bug caught during development
      impl.forceMoveNextFile();

      impl.appendDeleteRecord(1l);
      impl.appendDeleteRecord(2l);
      impl.appendDeleteRecord(3l);
      impl.appendDeleteRecord(4l);
      
      impl.stop();
      
      
      impl = new JournalImpl(512 + 1024 + 512, 20, true, false, factory, "jbm", "jbm", 1000, 0);
      impl.start();
      
      ArrayList<RecordInfo> info = new ArrayList<RecordInfo>();
      ArrayList<PreparedTransactionInfo> trans  = new ArrayList<PreparedTransactionInfo>();
      
      impl.load(info, trans);

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
      
      factory = null;
      
      journalImpl = null;
      
   }

   @Override
   protected void tearDown() throws Exception
   {
      super.tearDown();
      
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
   }
   
  
   // Private -------------------------------------------------------

   private void setupJournal(final int journalSize, final int alignment) throws Exception
   {
      setupJournal(journalSize, alignment, 2);
   }

   private void setupJournal(final int journalSize, final int alignment, final int numberOfMinimalFiles) throws Exception
   {
      if (factory == null)
      {
         factory = new FakeSequentialFileFactory(alignment,
               true);
      }
      
      if (journalImpl != null)
      {
         journalImpl.stop();
      }
      
      journalImpl = new JournalImpl(journalSize, numberOfMinimalFiles,
            true, true,
            factory, 
            "tt", "tt", 1000, 0);
      
      journalImpl.start();
      
      records.clear();
      transactions.clear();
      
      journalImpl.load(records, transactions);
   }
   

   // Inner classes -------------------------------------------------
   
   
}
