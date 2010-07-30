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

package org.hornetq.tests.stress.journal;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.core.asyncio.impl.AsynchronousFileImpl;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.journal.PreparedTransactionInfo;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.journal.TransactionFailureCallback;
import org.hornetq.core.journal.impl.AIOSequentialFileFactory;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.core.journal.impl.NIOSequentialFileFactory;
import org.hornetq.core.persistence.impl.journal.OperationContextImpl;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.HornetQThreadFactory;
import org.hornetq.utils.OrderedExecutorFactory;
import org.hornetq.utils.SimpleIDGenerator;
import org.hornetq.utils.concurrent.LinkedBlockingDeque;

/**
 * A SoakJournal
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class JournalCleanupCompactStressTest extends ServiceTestBase
{

   public static SimpleIDGenerator idGen = new SimpleIDGenerator(1);

   private volatile boolean running;

   private AtomicInteger errors = new AtomicInteger(0);

   private AtomicInteger numberOfRecords = new AtomicInteger(0);

   private AtomicInteger numberOfUpdates = new AtomicInteger(0);

   private AtomicInteger numberOfDeletes = new AtomicInteger(0);

   private JournalImpl journal;

   ThreadFactory tFactory = new HornetQThreadFactory("SoakTest" + System.identityHashCode(this),
                                                     false,
                                                     JournalCleanupCompactStressTest.class.getClassLoader());

   private final ExecutorService threadPool = Executors.newFixedThreadPool(20, tFactory);

   OrderedExecutorFactory executorFactory = new OrderedExecutorFactory(threadPool);

   @Override
   public void setUp() throws Exception
   {
      super.setUp();

      errors.set(0);

      File dir = new File(getTemporaryDir());
      dir.mkdirs();

      SequentialFileFactory factory;

      int maxAIO;
      if (AsynchronousFileImpl.isLoaded())
      {
         factory = new AIOSequentialFileFactory(dir.getPath());
         maxAIO = ConfigurationImpl.DEFAULT_JOURNAL_MAX_IO_AIO;
      }
      else
      {
         factory = new NIOSequentialFileFactory(dir.getPath());
         maxAIO = ConfigurationImpl.DEFAULT_JOURNAL_MAX_IO_NIO;
      }

      journal = new JournalImpl(ConfigurationImpl.DEFAULT_JOURNAL_FILE_SIZE,
                                10,
                                15,
                                ConfigurationImpl.DEFAULT_JOURNAL_COMPACT_PERCENTAGE,
                                factory,
                                "hornetq-data",
                                "hq",
                                maxAIO);

      journal.start();
      journal.loadInternalOnly();

   }

   @Override
   public void tearDown() throws Exception
   {
      try
      {
         if (journal.isStarted())
         {
            journal.stop();
         }
      }
      catch (Exception e)
      {
         // don't care :-)
      }
   }
   
   protected long getTotalTimeMilliseconds()
   {
      return TimeUnit.MINUTES.toMillis(10);
   }

   public void testAppend() throws Exception
   {

      running = true;
      SlowAppenderNoTX t1 = new SlowAppenderNoTX();

      int NTHREADS = 5;

      FastAppenderTx appenders[] = new FastAppenderTx[NTHREADS];
      FastUpdateTx updaters[] = new FastUpdateTx[NTHREADS];

      for (int i = 0; i < NTHREADS; i++)
      {
         appenders[i] = new FastAppenderTx();
         updaters[i] = new FastUpdateTx(appenders[i].queue);
      }

      t1.start();

      Thread.sleep(1000);

      for (int i = 0; i < NTHREADS; i++)
      {
         appenders[i].start();
         updaters[i].start();
      }

      long timeToEnd = System.currentTimeMillis() + getTotalTimeMilliseconds();

      while (System.currentTimeMillis() < timeToEnd)
      {
         System.out.println("Append = " + numberOfRecords +
                            ", Update = " +
                            numberOfUpdates +
                            ", Delete = " +
                            numberOfDeletes +
                            ", liveRecords = " +
                            (numberOfRecords.get() - numberOfDeletes.get()));
         Thread.sleep(TimeUnit.SECONDS.toMillis(10));
      }

      running = false;

      for (Thread t : appenders)
      {
         t.join();
      }

      for (Thread t : updaters)
      {
         t.join();
      }

      t1.join();

      assertEquals(0, errors.get());

      journal.stop();

      journal.start();

      ArrayList<RecordInfo> committedRecords = new ArrayList<RecordInfo>();
      ArrayList<PreparedTransactionInfo> preparedTransactions = new ArrayList<PreparedTransactionInfo>();
      journal.load(committedRecords, preparedTransactions, new TransactionFailureCallback()
      {
         public void failedTransaction(long transactionID, List<RecordInfo> records, List<RecordInfo> recordsToDelete)
         {
         }
      });

      long appends = 0, updates = 0;

      for (RecordInfo record : committedRecords)
      {
         if (record.isUpdate)
         {
            updates++;
         }
         else
         {
            appends++;
         }
      }

      assertEquals(numberOfRecords.get() - numberOfDeletes.get(), appends);

      journal.stop();
   }

   private byte[] generateRecord()
   {
      int size = RandomUtil.randomPositiveInt() % 10000;
      if (size == 0)
      {
         size = 10000;
      }
      return RandomUtil.randomBytes(size);
   }

   class FastAppenderTx extends Thread
   {
      LinkedBlockingDeque<Long> queue = new LinkedBlockingDeque<Long>();

      OperationContextImpl ctx = new OperationContextImpl(executorFactory.getExecutor());

      @Override
      public void run()
      {
         try
         {

            while (running)
            {
               final int txSize = RandomUtil.randomMax(100);

               long txID = JournalCleanupCompactStressTest.idGen.generateID();

               final long ids[] = new long[txSize];

               for (int i = 0; i < txSize; i++)
               {
                  long id = JournalCleanupCompactStressTest.idGen.generateID();
                  ids[i] = id;
                  journal.appendAddRecordTransactional(txID, id, (byte)0, generateRecord());
                  Thread.sleep(1);
               }
               journal.appendCommitRecord(txID, true, ctx);
               ctx.executeOnCompletion(new IOAsyncTask()
               {

                  public void onError(final int errorCode, final String errorMessage)
                  {
                  }

                  public void done()
                  {
                     numberOfRecords.addAndGet(txSize);
                     for (Long id : ids)
                     {
                        queue.add(id);
                     }
                  }
               });
            }
         }
         catch (Exception e)
         {
            e.printStackTrace();
            running = false;
            errors.incrementAndGet();
         }
      }
   }

   class FastUpdateTx extends Thread
   {
      final LinkedBlockingDeque<Long> queue;

      OperationContextImpl ctx = new OperationContextImpl(executorFactory.getExecutor());

      public FastUpdateTx(final LinkedBlockingDeque<Long> queue)
      {
         this.queue = queue;
      }

      @Override
      public void run()
      {
         try
         {
            int txSize = RandomUtil.randomMax(100);
            int txCount = 0;
            long ids[] = new long[txSize];

            long txID = JournalCleanupCompactStressTest.idGen.generateID();

            while (running)
            {

               long id = queue.poll(60, TimeUnit.MINUTES);
               ids[txCount] = id;
               journal.appendUpdateRecordTransactional(txID, id, (byte)0, generateRecord());
               if (++txCount == txSize)
               {
                  journal.appendCommitRecord(txID, true, ctx);
                  ctx.executeOnCompletion(new DeleteTask(ids));
                  txCount = 0;
                  txSize = RandomUtil.randomMax(100);
                  txID = JournalCleanupCompactStressTest.idGen.generateID();
                  ids = new long[txSize];
               }
            }
         }
         catch (Exception e)
         {
            e.printStackTrace();
            running = false;
            errors.incrementAndGet();
         }
      }
   }

   class DeleteTask implements IOAsyncTask
   {
      final long ids[];

      DeleteTask(final long ids[])
      {
         this.ids = ids;
      }

      public void done()
      {
         numberOfUpdates.addAndGet(ids.length);
         try
         {
            for (long id : ids)
            {
               journal.appendDeleteRecord(id, false);
               numberOfDeletes.incrementAndGet();
            }
         }
         catch (Exception e)
         {
            System.err.println("Can't delete id");
            e.printStackTrace();
            running = false;
            errors.incrementAndGet();
         }
      }

      public void onError(final int errorCode, final String errorMessage)
      {
      }

   }

   /** Adds stuff to the journal, but it will take a long time to remove them.
    *  This will cause cleanup and compacting to happen more often
    */
   class SlowAppenderNoTX extends Thread
   {
      @Override
      public void run()
      {
         try
         {
            while (running)
            {
               long ids[] = new long[5];
               // Append
               for (int i = 0; running & i < ids.length; i++)
               {
                  System.out.println("append slow");
                  ids[i] = JournalCleanupCompactStressTest.idGen.generateID();
                  journal.appendAddRecord(ids[i], (byte)1, generateRecord(), true);
                  numberOfRecords.incrementAndGet();

                  Thread.sleep(TimeUnit.SECONDS.toMillis(50));
               }
               // Delete
               for (int i = 0; running & i < ids.length; i++)
               {
                  System.out.println("Deleting");
                  journal.appendDeleteRecord(ids[i], false);
                  numberOfDeletes.incrementAndGet();
               }
            }
         }
         catch (Exception e)
         {
            e.printStackTrace();
            System.exit(-1);
         }
      }
   }

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
