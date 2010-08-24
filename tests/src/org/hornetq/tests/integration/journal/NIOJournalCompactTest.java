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

package org.hornetq.tests.integration.journal;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import junit.framework.Assert;

import org.hornetq.api.core.Pair;
import org.hornetq.core.journal.PreparedTransactionInfo;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.journal.impl.AbstractJournalUpdateTask;
import org.hornetq.core.journal.impl.JournalCompactor;
import org.hornetq.core.journal.impl.JournalFile;
import org.hornetq.core.journal.impl.JournalFileImpl;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.core.journal.impl.NIOSequentialFileFactory;
import org.hornetq.core.logging.Logger;
import org.hornetq.tests.unit.core.journal.impl.JournalImplTestBase;
import org.hornetq.tests.unit.core.journal.impl.fakes.SimpleEncoding;
import org.hornetq.utils.IDGenerator;
import org.hornetq.utils.SimpleIDGenerator;
import org.hornetq.utils.TimeAndCounterIDGenerator;

/**
 * 
 * A JournalImplTestBase
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class NIOJournalCompactTest extends JournalImplTestBase
{
   private final Logger log = Logger.getLogger(this.getClass());

   private static final int NUMBER_OF_RECORDS = 1000;

   IDGenerator idGenerator = new TimeAndCounterIDGenerator();

   // General tests
   // =============

   public void testControlFile() throws Exception
   {
      ArrayList<JournalFile> dataFiles = new ArrayList<JournalFile>();

      for (int i = 0; i < 5; i++)
      {
         SequentialFile file = fileFactory.createSequentialFile("file-" + i + ".tst", 1);
         dataFiles.add(new JournalFileImpl(file, 0, JournalImpl.FORMAT_VERSION));
      }

      ArrayList<JournalFile> newFiles = new ArrayList<JournalFile>();

      for (int i = 0; i < 3; i++)
      {
         SequentialFile file = fileFactory.createSequentialFile("file-" + i + ".tst.new", 1);
         newFiles.add(new JournalFileImpl(file, 0, JournalImpl.FORMAT_VERSION));
      }

      ArrayList<Pair<String, String>> renames = new ArrayList<Pair<String, String>>();
      renames.add(new Pair<String, String>("a", "b"));
      renames.add(new Pair<String, String>("c", "d"));

      AbstractJournalUpdateTask.writeControlFile(fileFactory, dataFiles, newFiles, renames);

      ArrayList<String> strDataFiles = new ArrayList<String>();

      ArrayList<String> strNewFiles = new ArrayList<String>();

      ArrayList<Pair<String, String>> renamesRead = new ArrayList<Pair<String, String>>();

      Assert.assertNotNull(JournalCompactor.readControlFile(fileFactory, strDataFiles, strNewFiles, renamesRead));

      Assert.assertEquals(dataFiles.size(), strDataFiles.size());
      Assert.assertEquals(newFiles.size(), strNewFiles.size());
      Assert.assertEquals(renames.size(), renamesRead.size());

      Iterator<String> iterDataFiles = strDataFiles.iterator();
      for (JournalFile file : dataFiles)
      {
         Assert.assertEquals(file.getFile().getFileName(), iterDataFiles.next());
      }
      Assert.assertFalse(iterDataFiles.hasNext());

      Iterator<String> iterNewFiles = strNewFiles.iterator();
      for (JournalFile file : newFiles)
      {
         Assert.assertEquals(file.getFile().getFileName(), iterNewFiles.next());
      }
      Assert.assertFalse(iterNewFiles.hasNext());

      Iterator<Pair<String, String>> iterRename = renames.iterator();
      for (Pair<String, String> rename : renamesRead)
      {
         Pair<String, String> original = iterRename.next();
         Assert.assertEquals(original.a, rename.a);
         Assert.assertEquals(original.b, rename.b);
      }
      Assert.assertFalse(iterNewFiles.hasNext());

   }

   public void testCrashRenamingFiles() throws Exception
   {
      internalCompactTest(false, false, true, false, false, false, false, false, false, false, true, false, false);
   }

   public void testCrashDuringCompacting() throws Exception
   {
      internalCompactTest(false, false, true, false, false, false, false, false, false, false, false, false, false);
   }

   public void testCompactwithPendingXACommit() throws Exception
   {
      internalCompactTest(true, false, false, false, false, false, false, true, false, false, true, true, true);
   }

   public void testCompactwithPendingXAPrepareAndCommit() throws Exception
   {
      internalCompactTest(false, true, false, false, false, false, false, true, false, false, true, true, true);
   }

   public void testCompactwithPendingXAPrepareAndDelayedCommit() throws Exception
   {
      internalCompactTest(false, true, false, false, false, false, false, true, false, true, true, true, true);
   }

   public void testCompactwithPendingCommit() throws Exception
   {
      internalCompactTest(true, false, false, false, false, false, false, true, false, false, true, true, true);
   }

   public void testCompactwithDelayedCommit() throws Exception
   {
      internalCompactTest(false, true, false, false, false, false, false, true, false, true, true, true, true);
   }

   public void testCompactwithPendingCommitFollowedByDelete() throws Exception
   {
      internalCompactTest(false, false, false, false, false, false, false, true, true, false, true, true, true);
   }

   public void testCompactwithConcurrentUpdateAndDeletes() throws Exception
   {
      internalCompactTest(false, false, true, false, true, true, false, false, false, false, true, true, true);
      tearDown();
      setUp();
      internalCompactTest(false, false, true, false, true, false, true, false, false, false, true, true, true);
   }

   public void testCompactwithConcurrentDeletes() throws Exception
   {
      internalCompactTest(false, false, true, false, false, true, false, false, false, false, true, true, true);
      tearDown();
      setUp();
      internalCompactTest(false, false, true, false, false, false, true, false, false, false, true, true, true);
   }

   public void testCompactwithConcurrentUpdates() throws Exception
   {
      internalCompactTest(false, false, true, false, true, false, false, false, false, false, true, true, true);
   }

   public void testCompactWithConcurrentAppend() throws Exception
   {
      internalCompactTest(false, false, true, true, false, false, false, false, false, false, true, true, true);
   }

   public void testCompactFirstFileReclaimed() throws Exception
   {

      setup(2, 60 * 1024, false);

      final byte recordType = (byte)0;

      journal = new JournalImpl(fileSize, minFiles, 0, 0, fileFactory, filePrefix, fileExtension, maxAIO);

      journal.start();

      journal.loadInternalOnly();

      journal.appendAddRecord(1, recordType, "test".getBytes(), true);

      journal.forceMoveNextFile();

      journal.appendUpdateRecord(1, recordType, "update".getBytes(), true);

      journal.appendDeleteRecord(1, true);

      journal.appendAddRecord(2, recordType, "finalRecord".getBytes(), true);

      for (int i = 10; i < 100; i++)
      {
         journal.appendAddRecord(i, recordType, ("tst" + i).getBytes(), true);
         journal.forceMoveNextFile();
         journal.appendUpdateRecord(i, recordType, ("uptst" + i).getBytes(), true);
         journal.appendDeleteRecord(i, true);
      }

      journal.compact();

      journal.stop();

      List<RecordInfo> records = new ArrayList<RecordInfo>();

      List<PreparedTransactionInfo> preparedRecords = new ArrayList<PreparedTransactionInfo>();

      journal.start();

      journal.load(records, preparedRecords, null);

      assertEquals(1, records.size());

   }

   public void testOnRollback() throws Exception
   {

      setup(2, 60 * 1024, false);

      createJournal();

      startJournal();

      load();

      add(1);

      updateTx(2, 1);

      rollback(2);

      journal.compact();

      stopJournal();

      startJournal();

      loadAndCheck();

      stopJournal();

   }

   public void testCompactSecondFileReclaimed() throws Exception
   {

      setup(2, 60 * 1024, false);

      createJournal();

      startJournal();

      load();

      addTx(1, 1, 2, 3, 4);

      journal.forceMoveNextFile();

      addTx(1, 5, 6, 7, 8);
      
      commit(1);
      
      journal.forceMoveNextFile();
      
      journal.compact();
      
      add(10);

      stopJournal();

      startJournal();

      loadAndCheck();

      stopJournal();

   }

   public void testIncompleteTXDuringcompact() throws Exception
   {

      setup(2, 60 * 1024, false);

      createJournal();

      startJournal();

      load();

      add(1);

      updateTx(2, 1);

      journal.compact();

      journal.compact();

      commit(2);

      stopJournal();

      startJournal();

      loadAndCheck();

      stopJournal();

   }

   private void internalCompactTest(final boolean preXA, // prepare before compact
                                    final boolean postXA, // prepare after compact
                                    final boolean regularAdd,
                                    final boolean performAppend,
                                    final boolean performUpdate,
                                    boolean performDelete,
                                    boolean performNonTransactionalDelete,
                                    final boolean pendingTransactions,
                                    final boolean deleteTransactRecords,
                                    final boolean delayCommit,
                                    final boolean createControlFile,
                                    final boolean deleteControlFile,
                                    final boolean renameFilesAfterCompacting) throws Exception
   {
      if (performNonTransactionalDelete)
      {
         performDelete = false;
      }
      if (performDelete)
      {
         performNonTransactionalDelete = false;
      }

      setup(2, 60 * 1024, false);

      ArrayList<Long> liveIDs = new ArrayList<Long>();

      ArrayList<Pair<Long, Long>> transactedRecords = new ArrayList<Pair<Long, Long>>();

      final CountDownLatch latchDone = new CountDownLatch(1);
      final CountDownLatch latchWait = new CountDownLatch(1);
      journal = new JournalImpl(fileSize, minFiles, 0, 0, fileFactory, filePrefix, fileExtension, maxAIO)
      {

         @Override
         protected SequentialFile createControlFile(final List<JournalFile> files,
                                                    final List<JournalFile> newFiles,
                                                    final Pair<String, String> pair) throws Exception
         {
            if (createControlFile)
            {
               return super.createControlFile(files, newFiles, pair);
            }
            else
            {
               throw new IllegalStateException("Simulating a crash during compact creation");
            }
         }

         @Override
         protected void deleteControlFile(final SequentialFile controlFile) throws Exception
         {
            if (deleteControlFile)
            {
               super.deleteControlFile(controlFile);
            }
         }

         @Override
         protected void renameFiles(final List<JournalFile> oldFiles, final List<JournalFile> newFiles) throws Exception
         {
            if (renameFilesAfterCompacting)
            {
               super.renameFiles(oldFiles, newFiles);
            }
         }

         @Override
         public void onCompactDone()
         {
            latchDone.countDown();
            System.out.println("Waiting on Compact");
            try
            {
               latchWait.await();
            }
            catch (InterruptedException e)
            {
               e.printStackTrace();
            }
            System.out.println("Done");
         }
      };

      journal.setAutoReclaim(false);

      startJournal();
      load();

      long transactionID = 0;

      if (regularAdd)
      {

         for (int i = 0; i < NIOJournalCompactTest.NUMBER_OF_RECORDS / 2; i++)
         {
            add(i);
            if (i % 10 == 0 && i > 0)
            {
               journal.forceMoveNextFile();
            }
            update(i);
         }

         for (int i = NIOJournalCompactTest.NUMBER_OF_RECORDS / 2; i < NIOJournalCompactTest.NUMBER_OF_RECORDS; i++)
         {

            addTx(transactionID, i);
            updateTx(transactionID, i);
            if (i % 10 == 0)
            {
               journal.forceMoveNextFile();
            }
            commit(transactionID++);
            update(i);
         }
      }

      if (pendingTransactions)
      {
         for (long i = 0; i < 100; i++)
         {
            long recordID = idGenerator.generateID();
            addTx(transactionID, recordID);
            updateTx(transactionID, recordID);
            if (preXA)
            {
               prepare(transactionID, new SimpleEncoding(10, (byte)0));
            }
            transactedRecords.add(new Pair<Long, Long>(transactionID++, recordID));
         }
      }

      if (regularAdd)
      {
         for (int i = 0; i < NIOJournalCompactTest.NUMBER_OF_RECORDS; i++)
         {
            if (!(i % 10 == 0))
            {
               delete(i);
            }
            else
            {
               liveIDs.add((long)i);
            }
         }
      }

      journal.forceMoveNextFile();

      Thread t = new Thread()
      {
         @Override
         public void run()
         {
            try
            {
               journal.compact();
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }
      };

      t.start();

      latchDone.await();

      int nextID = NIOJournalCompactTest.NUMBER_OF_RECORDS;

      if (performAppend)
      {
         for (int i = 0; i < 50; i++)
         {
            add(nextID++);
            if (i % 10 == 0)
            {
               journal.forceMoveNextFile();
            }
         }

         for (int i = 0; i < 50; i++)
         {
            // A Total new transaction (that was created after the compact started) to add new record while compacting
            // is still working
            addTx(transactionID, nextID++);
            commit(transactionID++);
            if (i % 10 == 0)
            {
               journal.forceMoveNextFile();
            }
         }
      }

      if (performUpdate)
      {
         int count = 0;
         for (Long liveID : liveIDs)
         {
            if (count++ % 2 == 0)
            {
               update(liveID);
            }
            else
            {
               // A Total new transaction (that was created after the compact started) to update a record that is being
               // compacted
               updateTx(transactionID, liveID);
               commit(transactionID++);
            }
         }
      }

      if (performDelete)
      {
         int count = 0;
         for (long liveID : liveIDs)
         {
            if (count++ % 2 == 0)
            {
               System.out.println("Deleting no trans " + liveID);
               delete(liveID);
            }
            else
            {
               System.out.println("Deleting TX " + liveID);
               // A Total new transaction (that was created after the compact started) to delete a record that is being
               // compacted
               deleteTx(transactionID, liveID);
               commit(transactionID++);
            }

            System.out.println("Deletes are going into " + ((JournalImpl)journal).getCurrentFile());
         }
      }

      if (performNonTransactionalDelete)
      {
         for (long liveID : liveIDs)
         {
            delete(liveID);
         }
      }

      if (pendingTransactions && !delayCommit)
      {
         for (Pair<Long, Long> tx : transactedRecords)
         {
            if (postXA)
            {
               prepare(tx.a, new SimpleEncoding(10, (byte)0));
            }
            if (tx.a % 2 == 0)
            {
               commit(tx.a);

               if (deleteTransactRecords)
               {
                  delete(tx.b);
               }
            }
            else
            {
               rollback(tx.a);
            }
         }
      }

      /** Some independent adds and updates */
      for (int i = 0; i < 1000; i++)
      {
         long id = idGenerator.generateID();
         add(id);
         delete(id);

         if (i % 100 == 0)
         {
            journal.forceMoveNextFile();
         }
      }
      journal.forceMoveNextFile();

      latchWait.countDown();

      t.join();

      if (pendingTransactions && delayCommit)
      {
         for (Pair<Long, Long> tx : transactedRecords)
         {
            if (postXA)
            {
               prepare(tx.a, new SimpleEncoding(10, (byte)0));
            }
            if (tx.a % 2 == 0)
            {
               commit(tx.a);

               if (deleteTransactRecords)
               {
                  delete(tx.b);
               }
            }
            else
            {
               rollback(tx.a);
            }
         }
      }

      add(idGenerator.generateID());

      if (createControlFile && deleteControlFile && renameFilesAfterCompacting)
      {
         journal.compact();
      }

      stopJournal();
      createJournal();
      startJournal();
      loadAndCheck();

   }

   public void testCompactAddAndUpdateFollowedByADelete() throws Exception
   {

      setup(2, 60 * 1024, false);

      SimpleIDGenerator idGen = new SimpleIDGenerator(1000);

      createJournal();
      journal.setAutoReclaim(false);

      startJournal();
      load();

      long consumerTX = idGen.generateID();

      long firstID = idGen.generateID();

      long appendTX = idGen.generateID();

      long addedRecord = idGen.generateID();

      addTx(consumerTX, firstID);

      startCompact();

      addTx(appendTX, addedRecord);

      commit(appendTX);

      updateTx(consumerTX, addedRecord);

      commit(consumerTX);

      delete(addedRecord);

      finishCompact();

      journal.forceMoveNextFile();

      long newRecord = idGen.generateID();
      add(newRecord);
      update(newRecord);

      journal.compact();

      System.out.println("Debug after compact\n" + journal.debug());

      stopJournal();
      createJournal();
      startJournal();
      loadAndCheck();

   }

   public void testCompactAddAndUpdateFollowedByADelete2() throws Exception
   {

      setup(2, 60 * 1024, false);

      SimpleIDGenerator idGen = new SimpleIDGenerator(1000);

      createJournal();

      journal.setAutoReclaim(false);

      startJournal();
      load();

      long firstID = idGen.generateID();

      long consumerTX = idGen.generateID();

      long appendTX = idGen.generateID();

      long addedRecord = idGen.generateID();

      addTx(consumerTX, firstID);

      startCompact();

      addTx(appendTX, addedRecord);
      commit(appendTX);
      updateTx(consumerTX, addedRecord);
      commit(consumerTX);

      long deleteTXID = idGen.generateID();

      deleteTx(deleteTXID, addedRecord);

      commit(deleteTXID);

      finishCompact();

      journal.forceMoveNextFile();

      journal.compact();

      stopJournal();
      createJournal();
      startJournal();
      loadAndCheck();

   }

   public void testCompactAddAndUpdateFollowedByADelete3() throws Exception
   {

      setup(2, 60 * 1024, false);

      SimpleIDGenerator idGen = new SimpleIDGenerator(1000);

      createJournal();

      journal.setAutoReclaim(false);

      startJournal();
      load();

      long firstID = idGen.generateID();

      long consumerTX = idGen.generateID();

      long addedRecord = idGen.generateID();

      add(firstID);

      updateTx(consumerTX, firstID);

      startCompact();

      addTx(consumerTX, addedRecord);
      commit(consumerTX);
      delete(addedRecord);

      finishCompact();

      journal.compact();

      stopJournal();
      createJournal();
      startJournal();
      loadAndCheck();

   }

   public void testCompactAddAndUpdateFollowedByADelete4() throws Exception
   {

      setup(2, 60 * 1024, false);

      SimpleIDGenerator idGen = new SimpleIDGenerator(1000);

      createJournal();
      
      startJournal();
      load();

      long consumerTX = idGen.generateID();

      long firstID = idGen.generateID();

      long appendTX = idGen.generateID();

      long addedRecord = idGen.generateID();

      startCompact();

      addTx(consumerTX, firstID);

      addTx(appendTX, addedRecord);

      commit(appendTX);

      updateTx(consumerTX, addedRecord);

      commit(consumerTX);

      delete(addedRecord);

      finishCompact();

      journal.forceMoveNextFile();

      long newRecord = idGen.generateID();
      add(newRecord);
      update(newRecord);

      journal.compact();

      System.out.println("Debug after compact\n" + journal.debug());

      stopJournal();
      createJournal();
      startJournal();
      loadAndCheck();

   }

   public void testCompactAddAndUpdateFollowedByADelete6() throws Exception
   {

      setup(2, 60 * 1024, false);

      SimpleIDGenerator idGen = new SimpleIDGenerator(1000);

      createJournal();

      journal.setAutoReclaim(false);

      startJournal();
      load();

      long tx0 = idGen.generateID();

      long tx1 = idGen.generateID();

      long add1 = idGen.generateID();

      long add2 = idGen.generateID();

      startCompact();

      addTx(tx0, add1);

      rollback(tx0);

      addTx(tx1, add1, add2);
      commit(tx1);

      finishCompact();

      long tx2 = idGen.generateID();

      updateTx(tx2, add1, add2);
      commit(tx2);

      delete(add1);

      startCompact();

      delete(add2);

      finishCompact();

      journal.forceMoveNextFile();

      journal.compact();

      stopJournal();
      createJournal();
      startJournal();
      loadAndCheck();

   }

   public void testDeleteWhileCleanup() throws Exception
   {

      setup(2, 60 * 1024, false);

      createJournal();

      startJournal();
      load();

      for (int i = 0; i < 100; i++)
      {
         add(i);
      }

      journal.forceMoveNextFile();

      for (int i = 10; i < 90; i++)
      {
         delete(i);
      }

      startCompact();
 
      // Delete part of the live records while cleanup still working
      for (int i = 1; i < 5; i++)
      {
         delete(i);
      }

      finishCompact();
 
      // Delete part of the live records after cleanup is done
      for (int i = 5; i < 10; i++)
      {
         delete(i);
      }

      assertEquals(9, journal.getCurrentFile().getNegCount(journal.getDataFiles()[0]));

      journal.forceMoveNextFile();

      stopJournal();
      createJournal();
      startJournal();
      loadAndCheck();

   }

   public void testCompactAddAndUpdateFollowedByADelete5() throws Exception
   {

      setup(2, 60 * 1024, false);

      SimpleIDGenerator idGen = new SimpleIDGenerator(1000);
      
      createJournal();

      startJournal();
      load();

      long appendTX = idGen.generateID();
      long appendOne = idGen.generateID();
      long appendTwo = idGen.generateID();

      long updateTX = idGen.generateID();

      addTx(appendTX, appendOne);

      startCompact();
      
      addTx(appendTX, appendTwo);

      commit(appendTX);

      updateTx(updateTX, appendOne);
      updateTx(updateTX, appendTwo);

      commit(updateTX);
      // delete(appendTwo);

      finishCompact();

      journal.compact();

      stopJournal();
      createJournal();
      startJournal();
      loadAndCheck();

   }

   public void testSimpleCompacting() throws Exception
   {
      setup(2, 60 * 1024, false);

      createJournal();
      startJournal();
      load();

      int NUMBER_OF_RECORDS = 1000;

      // add and remove some data to force reclaiming
      {
         ArrayList<Long> ids = new ArrayList<Long>();
         for (int i = 0; i < NUMBER_OF_RECORDS; i++)
         {
            long id = idGenerator.generateID();
            ids.add(id);
            add(id);
            if (i > 0 && i % 100 == 0)
            {
               journal.forceMoveNextFile();
            }
         }

         for (Long id : ids)
         {
            delete(id);
         }

         journal.forceMoveNextFile();

         journal.checkReclaimStatus();
      }

      long transactionID = 0;

      for (int i = 0; i < NUMBER_OF_RECORDS / 2; i++)
      {
         add(i);
         if (i % 10 == 0 && i > 0)
         {
            journal.forceMoveNextFile();
         }
         update(i);
      }

      for (int i = NUMBER_OF_RECORDS / 2; i < NUMBER_OF_RECORDS; i++)
      {

         addTx(transactionID, i);
         updateTx(transactionID, i);
         if (i % 10 == 0)
         {
            journal.forceMoveNextFile();
         }
         commit(transactionID++);
         update(i);
      }

      for (int i = 0; i < NUMBER_OF_RECORDS; i++)
      {
         if (!(i % 10 == 0))
         {
            delete(i);
         }
      }

      journal.forceMoveNextFile();

      System.out.println("Number of Files: " + journal.getDataFilesCount());

      System.out.println("Before compact ****************************");
      System.out.println(journal.debug());
      System.out.println("*****************************************");

      journal.compact();

      add(idGenerator.generateID());

      journal.compact();

      stopJournal();
      createJournal();
      startJournal();
      loadAndCheck();

   }

   public void testLiveSize() throws Exception
   {
      setup(2, 60 * 1024, true);

      createJournal();
      startJournal();
      loadAndCheck();

      ArrayList<Long> listToDelete = new ArrayList<Long>();

      ArrayList<Integer> expectedSizes = new ArrayList<Integer>();

      for (int i = 0; i < 10; i++)
      {
         long id = idGenerator.generateID();
         listToDelete.add(id);

         expectedSizes.add(recordLength + JournalImpl.SIZE_ADD_RECORD + 1);

         add(id);
         journal.forceMoveNextFile();
         update(id);

         expectedSizes.add(recordLength + JournalImpl.SIZE_ADD_RECORD + 1);
         journal.forceMoveNextFile();
      }

      JournalFile files[] = journal.getDataFiles();

      stopJournal();
      createJournal();
      startJournal();
      loadAndCheck();

      journal.forceMoveNextFile();

      JournalFile files2[] = journal.getDataFiles();

      Assert.assertEquals(files.length, files2.length);

      for (int i = 0; i < files.length; i++)
      {
         Assert.assertEquals(expectedSizes.get(i).intValue(), files[i].getLiveSize());
         Assert.assertEquals(expectedSizes.get(i).intValue(), files2[i].getLiveSize());
      }

      for (long id : listToDelete)
      {
         delete(id);
      }

      journal.forceMoveNextFile();

      JournalFile files3[] = journal.getDataFiles();

      for (JournalFile file : files3)
      {
         Assert.assertEquals(0, file.getLiveSize());
      }

      stopJournal();
      createJournal();
      startJournal();
      loadAndCheck();

      files3 = journal.getDataFiles();

      for (JournalFile file : files3)
      {
         Assert.assertEquals(0, file.getLiveSize());
      }

   }
   
   public void testCompactFirstFileWithPendingCommits() throws Exception
   {
      setup(2, 60 * 1024, true);

      createJournal();
      startJournal();
      loadAndCheck();

      long tx = idGenerator.generateID();
      for (int i = 0; i < 10; i++)
      {
         addTx(tx, idGenerator.generateID());
      }
      
      journal.forceMoveNextFile();
      commit(tx);
      

      ArrayList<Long> listToDelete = new ArrayList<Long>();
      for (int i = 0; i < 10; i++)
      {
         long id = idGenerator.generateID();
         listToDelete.add(id);
         add(id);
      }
      
      journal.forceMoveNextFile();

      for (Long id : listToDelete)
      {
         delete(id);
      }
      
      journal.forceMoveNextFile();
      
      // This operation used to be journal.cleanup(journal.getDataFiles()[0]); when cleanup was still in place
      journal.compact();

      journal.checkReclaimStatus();
      
      journal.compact();

      stopJournal();
      createJournal();
      startJournal();
      loadAndCheck();
   }

   public void testLiveSizeTransactional() throws Exception
   {
      setup(2, 60 * 1024, true);

      createJournal();
      startJournal();
      loadAndCheck();

      ArrayList<Long> listToDelete = new ArrayList<Long>();

      ArrayList<Integer> expectedSizes = new ArrayList<Integer>();

      for (int i = 0; i < 10; i++)
      {
         long tx = idGenerator.generateID();
         long id = idGenerator.generateID();
         listToDelete.add(id);

         // Append Record Transaction will make the recordSize as exactly recordLength (discounting SIZE_ADD_RECORD_TX)
         addTx(tx, id);

         expectedSizes.add(recordLength);
         journal.forceMoveNextFile();

         updateTx(tx, id);
         // uPDATE Record Transaction will make the recordSize as exactly recordLength (discounting SIZE_ADD_RECORD_TX)
         expectedSizes.add(recordLength);

         journal.forceMoveNextFile();
         expectedSizes.add(0);

         commit(tx);

         journal.forceMoveNextFile();
      }

      JournalFile files[] = journal.getDataFiles();

      stopJournal();
      createJournal();
      startJournal();
      loadAndCheck();

      journal.forceMoveNextFile();

      JournalFile files2[] = journal.getDataFiles();

      Assert.assertEquals(files.length, files2.length);

      for (int i = 0; i < files.length; i++)
      {
         Assert.assertEquals(expectedSizes.get(i).intValue(), files[i].getLiveSize());
         Assert.assertEquals(expectedSizes.get(i).intValue(), files2[i].getLiveSize());
      }

      long tx = idGenerator.generateID();
      for (long id : listToDelete)
      {
         deleteTx(tx, id);
      }
      commit(tx);

      journal.forceMoveNextFile();

      JournalFile files3[] = journal.getDataFiles();

      for (JournalFile file : files3)
      {
         Assert.assertEquals(0, file.getLiveSize());
      }

      stopJournal();
      createJournal();
      startJournal();
      loadAndCheck();

      files3 = journal.getDataFiles();

      for (JournalFile file : files3)
      {
         Assert.assertEquals(0, file.getLiveSize());
      }

   }

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      File file = new File(getTestDir());

      deleteDirectory(file);

      file.mkdir();
   }

   protected void tearDown() throws Exception
   {

      File testDir = new File(getTestDir());

      File files[] = testDir.listFiles(new FilenameFilter()
      {

         public boolean accept(File dir, String name)
         {
            return name.startsWith(filePrefix) && name.endsWith(fileExtension);
         }
      });

      for (File file : files)
      {
         assertEquals("File " + file + " doesn't have the expected number of bytes", fileSize, file.length());
      }

      super.tearDown();
   }

   /* (non-Javadoc)
    * @see org.hornetq.tests.unit.core.journal.impl.JournalImplTestBase#getFileFactory()
    */
   @Override
   protected SequentialFileFactory getFileFactory() throws Exception
   {
      return new NIOSequentialFileFactory(getTestDir());
   }

}
