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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.hornetq.core.journal.PreparedTransactionInfo;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.tests.unit.core.journal.impl.fakes.FakeSequentialFileFactory;
import org.hornetq.tests.unit.core.journal.impl.fakes.SimpleEncoding;
import org.hornetq.tests.util.UnitTestCase;

/**
 * 
 * A JournalAsyncTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 *
 *
 */
public class JournalAsyncTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private FakeSequentialFileFactory factory;

   private JournalImpl journalImpl = null;

   private ArrayList<RecordInfo> records = null;

   private ArrayList<PreparedTransactionInfo> transactions = null;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testAsynchronousCommit() throws Exception
   {
      final int JOURNAL_SIZE = 20000;

      setupJournal(JOURNAL_SIZE, 100, 5);

      factory.setHoldCallbacks(true, null);

      final CountDownLatch latch = new CountDownLatch(1);

      class LocalThread extends Thread
      {
         Exception e;

         @Override
         public void run()
         {
            try
            {
               for (int i = 0; i < 10; i++)
               {
                  journalImpl.appendAddRecordTransactional(1l, i, (byte)1, new SimpleEncoding(1, (byte)0));
               }

               latch.countDown();
               factory.setHoldCallbacks(false, null);
               journalImpl.appendCommitRecord(1l, true);
            }
            catch (Exception e)
            {
               e.printStackTrace();
               this.e = e;
            }
         }
      };

      LocalThread t = new LocalThread();
      t.start();

      assertTrue(latch.await(5, TimeUnit.SECONDS));

      Thread.yield();

      assertTrue(t.isAlive());

      factory.flushAllCallbacks();

      t.join();

      if (t.e != null)
      {
         throw t.e;
      }
   }

   public void testAsynchronousRollbackWithError() throws Exception
   {
      final int JOURNAL_SIZE = 20000;

      setupJournal(JOURNAL_SIZE, 100, 5);

      final CountDownLatch latch = new CountDownLatch(11);

      factory.setHoldCallbacks(true, new FakeSequentialFileFactory.ListenerHoldCallback()
      {

         public void callbackAdded(final ByteBuffer bytes)
         {
            latch.countDown();
         }
      });

      class LocalThread extends Thread
      {
         Exception e;

         @Override
         public void run()
         {
            try
            {
               for (int i = 0; i < 10; i++)
               {
                  journalImpl.appendAddRecordTransactional(1l, i, (byte)1, new SimpleEncoding(1, (byte)0));
               }

               journalImpl.appendRollbackRecord(1l, true);
            }
            catch (Exception e)
            {
               this.e = e;
            }
         }
      };

      LocalThread t = new LocalThread();
      t.start();

      latch.await();

      Thread.yield();

      assertTrue(t.isAlive());

      factory.setCallbackAsError(0);

      factory.flushCallback(0);

      Thread.yield();

      assertTrue(t.isAlive());

      factory.flushAllCallbacks();

      t.join();

      assertNotNull(t.e);
   }

   public void testAsynchronousCommitWithError() throws Exception
   {
      final int JOURNAL_SIZE = 20000;

      setupJournal(JOURNAL_SIZE, 100, 5);

      final CountDownLatch latch = new CountDownLatch(11);

      factory.setHoldCallbacks(true, new FakeSequentialFileFactory.ListenerHoldCallback()
      {

         public void callbackAdded(final ByteBuffer bytes)
         {
            latch.countDown();
         }
      });

      class LocalThread extends Thread
      {
         Exception e;

         @Override
         public void run()
         {
            try
            {
               for (int i = 0; i < 10; i++)
               {
                  journalImpl.appendAddRecordTransactional(1l, i, (byte)1, new SimpleEncoding(1, (byte)0));
               }

               journalImpl.appendCommitRecord(1l, true);
            }
            catch (Exception e)
            {
               this.e = e;
            }
         }
      };

      LocalThread t = new LocalThread();
      t.start();

      latch.await();

      Thread.yield();

      assertTrue(t.isAlive());

      factory.setCallbackAsError(0);

      factory.flushCallback(0);

      Thread.yield();

      assertTrue(t.isAlive());

      factory.flushAllCallbacks();

      t.join();

      assertNotNull(t.e);

      try
      {
         journalImpl.appendRollbackRecord(1l, false);
         fail("Supposed to throw an exception");
      }
      catch (Exception e)
      {

      }
   }

   // If a callback error already arrived, we should just throw the exception
   // right away
   public void testPreviousError() throws Exception
   {
      final int JOURNAL_SIZE = 20000;

      setupJournal(JOURNAL_SIZE, 100, 5);

      factory.setHoldCallbacks(true, null);
      factory.setGenerateErrors(true);

      journalImpl.appendAddRecordTransactional(1l, 1, (byte)1, new SimpleEncoding(1, (byte)0));

      factory.flushAllCallbacks();

      factory.setGenerateErrors(false);
      factory.setHoldCallbacks(false, null);

      try
      {
         journalImpl.appendAddRecordTransactional(1l, 2, (byte)1, new SimpleEncoding(1, (byte)0));
         fail("Exception expected"); // An exception already happened in one
         // of the elements on this transaction.
         // We can't accept any more elements on
         // the transaction
      }
      catch (Exception ignored)
      {
      }
   }

   public void testSyncNonTransaction() throws Exception
   {
      final int JOURNAL_SIZE = 20000;

      setupJournal(JOURNAL_SIZE, 100, 5);

      factory.setGenerateErrors(true);

      try
      {
         journalImpl.appendAddRecord(1l, (byte)0, new SimpleEncoding(1, (byte)0), true);
         fail("Exception expected");
      }
      catch (Exception ignored)
      {

      }

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

      super.tearDown();
   }

   // Private -------------------------------------------------------
   private void setupJournal(final int journalSize, final int alignment, final int numberOfMinimalFiles) throws Exception
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

      journalImpl.load(records, transactions, null);
   }

   // Inner classes -------------------------------------------------

}
