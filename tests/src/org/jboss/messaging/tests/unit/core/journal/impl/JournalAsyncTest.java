/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat
 * Middleware LLC, and individual contributors by the @authors tag. See the
 * copyright.txt in the distribution for a full listing of individual
 * contributors.
 * 
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 * 
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.tests.unit.core.journal.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.jboss.messaging.core.journal.PreparedTransactionInfo;
import org.jboss.messaging.core.journal.RecordInfo;
import org.jboss.messaging.core.journal.impl.JournalImpl;
import org.jboss.messaging.tests.unit.core.journal.impl.fakes.FakeSequentialFileFactory;
import org.jboss.messaging.tests.unit.core.journal.impl.fakes.SimpleEncoding;
import org.jboss.messaging.tests.util.UnitTestCase;

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

   JournalImpl journalImpl = null;

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
               journalImpl.appendCommitRecord(1l);
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

               journalImpl.appendRollbackRecord(1l);
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

               journalImpl.appendCommitRecord(1l);
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
         journalImpl.appendRollbackRecord(1l);
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
         journalImpl.appendAddRecord(1l, (byte)0, new SimpleEncoding(1, (byte)0));
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

      journalImpl = new JournalImpl(journalSize, numberOfMinimalFiles, true, true, false, factory, "tt", "tt", 1000);

      journalImpl.start();

      records.clear();
      transactions.clear();

      journalImpl.load(records, transactions);
   }

   // Inner classes -------------------------------------------------

}
