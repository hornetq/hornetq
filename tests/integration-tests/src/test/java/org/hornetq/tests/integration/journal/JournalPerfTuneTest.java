/*
 * Copyright 2010 Red Hat, Inc.
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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.core.journal.IOCompletion;
import org.hornetq.core.journal.Journal;
import org.hornetq.core.journal.LoaderCallback;
import org.hornetq.core.journal.PreparedTransactionInfo;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.journal.impl.AIOSequentialFileFactory;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.tests.integration.IntegrationTestLogger;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.DataConstants;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * A JournalPerfTuneTest
 * @author tim
 */
@Ignore
public class JournalPerfTuneTest extends UnitTestCase
{
   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   private Journal journal;

   static final class LoaderCB implements LoaderCallback
   {

      public void addPreparedTransaction(PreparedTransactionInfo preparedTransaction)
      {
         // no-op
      }

      public void addRecord(RecordInfo info)
      {
         // no-op
      }

      public void deleteRecord(long id)
      {
         // no-op
      }

      public void updateRecord(RecordInfo info)
      {
         // no-op
      }

      public void failedTransaction(long transactionID, List<RecordInfo> records, List<RecordInfo> recordsToDelete)
      {
         // no-op
      }

   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      final int fileSize = 1024 * 1024 * 10;
      final int minFiles = 10;
      final int compactMinFiles = 20;
      final int compactPercentage = 30;
      final String filePrefix = "data";
      final String extension = "hq";
      final int maxIO = 500;

      final String journalDir = getTestDir();
      final int bufferSize = 490 * 1024;
      final int bufferTimeout = (int)(1000000000d / 2000);
      final boolean logRates = true;

      super.recreateDirectory(journalDir);

      SequentialFileFactory fileFactory = new AIOSequentialFileFactory(journalDir, bufferSize, bufferTimeout, logRates);

      journal = new JournalImpl(fileSize,
                                minFiles,
                                compactMinFiles,
                                compactPercentage,
                                fileFactory,
                                filePrefix,
                                extension,
                                maxIO);
      addHornetQComponent(journal);
      journal.start();

      journal.load(new LoaderCB());
   }

   static final class TestCallback implements IOCompletion
   {
      private final CountDownLatch latch;

      TestCallback(final int counts)
      {
         this.latch = new CountDownLatch(counts);
      }

      public void await() throws Exception
      {
         waitForLatch(latch);
      }

      public void storeLineUp()
      {
      }

      public void done()
      {
         latch.countDown();

         log.info(latch.getCount());
      }

      public void onError(int errorCode, String errorMessage)
      {
      }

   }

   @Test
   public void test1() throws Exception
   {
      final int itersPerThread = 10000000;

      final int numThreads = 1;

      this.callback = new TestCallback(2 * itersPerThread * numThreads);

      Worker[] workers = new Worker[numThreads];

      for (int i = 0; i < numThreads; i++)
      {
         workers[i] = new Worker(itersPerThread);

         workers[i].start();
      }

      for (int i = 0; i < numThreads; i++)
      {
         workers[i].join();
      }

      callback.await();
   }

   private final AtomicLong idGen = new AtomicLong(0);

   private TestCallback callback;

   class Worker extends Thread
   {
      final int iters;

      Worker(final int iters)
      {
         this.iters = iters;
      }

      @Override
      public void run()
      {
         try
         {
            Record record = new Record(new byte[1024]);

            for (int i = 0; i < iters; i++)
            {
               long id = idGen.getAndIncrement();

               journal.appendAddRecord(id, (byte)0, record, true, callback);

               journal.appendDeleteRecord(id, true, callback);

               // log.info("did " + i);
            }
         }
         catch (Exception e)
         {
            log.error("Failed", e);
         }
      }
   }

   static class Record implements EncodingSupport
   {
      private byte[] bytes;

      Record(byte[] bytes)
      {
         this.bytes = bytes;
      }

      public void decode(HornetQBuffer buffer)
      {
         int length = buffer.readInt();

         bytes = new byte[length];

         buffer.readBytes(bytes);
      }

      public void encode(HornetQBuffer buffer)
      {
         buffer.writeInt(bytes.length);

         buffer.writeBytes(bytes);
      }

      public int getEncodeSize()
      {
         return DataConstants.SIZE_INT + bytes.length;
      }
   }
}
