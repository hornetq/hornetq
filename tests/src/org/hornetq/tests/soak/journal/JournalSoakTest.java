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

package org.hornetq.tests.soak.journal;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.hornetq.core.asyncio.impl.AsynchronousFileImpl;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.config.impl.FileConfiguration;
import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.journal.SequentialFileFactory;
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
public class JournalSoakTest extends ServiceTestBase
{

   public static SimpleIDGenerator idGen = new SimpleIDGenerator(1);

   private volatile boolean running;

   private JournalImpl journal;

   ThreadFactory tFactory = new HornetQThreadFactory("SoakTest" + System.identityHashCode(this),
                                                     false,
                                                     JournalSoakTest.class.getClassLoader());

   private final ExecutorService threadPool = Executors.newFixedThreadPool(20, tFactory);

   OrderedExecutorFactory executorFactory = new OrderedExecutorFactory(threadPool);

   @Override
   public void setUp() throws Exception
   {
      super.setUp();

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
                                ConfigurationImpl.DEFAULT_JOURNAL_COMPACT_MIN_FILES,
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
      journal.stop();
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

      for (int i = 0; i < NTHREADS; i++)
      {
         appenders[i].start();
         updaters[i].start();
      }

      Thread.sleep(TimeUnit.HOURS.toMillis(24));

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
               int txSize = RandomUtil.randomMax(1000);

               long txID = JournalSoakTest.idGen.generateID();

               final ArrayList<Long> ids = new ArrayList<Long>();

               for (int i = 0; i < txSize; i++)
               {
                  long id = JournalSoakTest.idGen.generateID();
                  ids.add(id);
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
            System.exit(-1);
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
            int txSize = RandomUtil.randomMax(1000);
            int txCount = 0;
            long ids[] = new long[txSize];

            long txID = JournalSoakTest.idGen.generateID();

            while (running)
            {

               long id = queue.poll(60, TimeUnit.MINUTES);
               ids[txCount] = id;
               journal.appendUpdateRecordTransactional(txID, id, (byte)0, generateRecord());
               Thread.sleep(1);
               if (++txCount == txSize)
               {
                  journal.appendCommitRecord(txID, true, ctx);
                  ctx.executeOnCompletion(new DeleteTask(ids));
                  txCount = 0;
                  txSize = RandomUtil.randomMax(1000);
                  txID = JournalSoakTest.idGen.generateID();
                  ids = new long[txSize];
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

   class DeleteTask implements IOAsyncTask
   {
      final long ids[];

      DeleteTask(final long ids[])
      {
         this.ids = ids;
      }

      public void done()
      {
         try
         {
            for (long id : ids)
            {
               journal.appendDeleteRecord(id, false);
            }
         }
         catch (Exception e)
         {
            System.err.println("Can't delete id");
            e.printStackTrace();
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
               long ids[] = new long[1000];
               // Append
               for (int i = 0; running & i < 1000; i++)
               {
                  ids[i] = JournalSoakTest.idGen.generateID();
                  journal.appendAddRecord(ids[i], (byte)1, generateRecord(), true);
                  Thread.sleep(300);
               }
               // Update
               for (int i = 0; running & i < 1000; i++)
               {
                  ids[i] = JournalSoakTest.idGen.generateID();
                  journal.appendUpdateRecord(ids[i], (byte)1, generateRecord(), true);
                  Thread.sleep(300);
               }
               // Delete
               for (int i = 0; running & i < 1000; i++)
               {
                  ids[i] = JournalSoakTest.idGen.generateID();
                  journal.appendUpdateRecord(ids[i], (byte)1, generateRecord(), true);
                  Thread.sleep(300);
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
