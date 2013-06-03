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

package org.hornetq.tests.unit.core.persistence.impl;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;

import org.hornetq.api.core.HornetQExceptionType;
import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.persistence.impl.journal.OperationContextImpl;
import org.hornetq.tests.util.UnitTestCase;

/**
 * A OperationContextUnitTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class OperationContextUnitTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testCompleteTaskAfterPaging() throws Exception
   {
      ExecutorService executor = Executors.newSingleThreadExecutor();
      try
      {
         OperationContextImpl impl = new OperationContextImpl(executor);
         final CountDownLatch latch1 = new CountDownLatch(1);
         final CountDownLatch latch2 = new CountDownLatch(1);

         impl.executeOnCompletion(new IOAsyncTask()
         {

            public void onError(int errorCode, String errorMessage)
            {
            }

            public void done()
            {
               latch1.countDown();
            }
         });

         assertTrue(latch1.await(10, TimeUnit.SECONDS));

         for (int i = 0 ; i < 10; i++) impl.storeLineUp();
         for (int i = 0 ; i < 3; i++) impl.pageSyncLineUp();

         impl.executeOnCompletion(new IOAsyncTask()
         {

            public void onError(int errorCode, String errorMessage)
            {
            }

            public void done()
            {
               latch2.countDown();
            }
         });


         assertFalse(latch2.await(1, TimeUnit.MILLISECONDS));

         for (int i = 0 ; i < 9; i++) impl.done();
         for (int i = 0 ; i < 2; i++) impl.pageSyncDone();


         assertFalse(latch2.await(1, TimeUnit.MILLISECONDS));

         impl.done();
         impl.pageSyncDone();

         assertTrue(latch2.await(10, TimeUnit.SECONDS));

      }
      finally
      {
         executor.shutdown();
      }
   }

   @Test
   public void testCaptureExceptionOnExecutor() throws Exception
   {
      ExecutorService executor = Executors.newSingleThreadExecutor();
      executor.shutdown();

      final CountDownLatch latch = new CountDownLatch(1);

      final OperationContextImpl impl = new OperationContextImpl(executor)
      {
         @Override
         public void complete()
         {
            super.complete();
            latch.countDown();
         }

      };

      impl.storeLineUp();

      final AtomicInteger numberOfFailures = new AtomicInteger(0);

      Thread t = new Thread()
      {
         @Override
         public void run()
         {
            try
            {
               impl.waitCompletion(5000);
            }
            catch (Throwable e)
            {
               e.printStackTrace();
               numberOfFailures.incrementAndGet();
            }
         }
      };

      t.start();

      // Need to wait complete to be called first or the test would be invalid.
      // We use a latch instead of forcing a sleep here
      Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));

      impl.done();

      t.join();

      Assert.assertEquals(1, numberOfFailures.get());
   }

   @Test
   public void testCaptureExceptionOnFailure() throws Exception
   {
      ExecutorService executor = Executors.newSingleThreadExecutor();

      final CountDownLatch latch = new CountDownLatch(1);

      final OperationContextImpl context = new OperationContextImpl(executor)
      {
         @Override
         public void complete()
         {
            super.complete();
            latch.countDown();
         }

      };

      context.storeLineUp();

      final AtomicInteger failures = new AtomicInteger(0);

      Thread t = new Thread()
      {
         @Override
         public void run()
         {
            try
            {
               context.waitCompletion(5000);
            }
            catch (Throwable e)
            {
               e.printStackTrace();
               failures.incrementAndGet();
            }
         }
      };

      t.start();

      // Need to wait complete to be called first or the test would be invalid.
      // We use a latch instead of forcing a sleep here
      Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));

      context.onError(HornetQExceptionType.UNSUPPORTED_PACKET.getCode(), "Poop happens!");

      t.join();

      Assert.assertEquals(1, failures.get());

      failures.set(0);

      final AtomicInteger operations = new AtomicInteger(0);

      // We should be up to date with lineUps and executions. this should now just finish processing
      context.executeOnCompletion(new IOAsyncTask()
      {

         public void done()
         {
            operations.incrementAndGet();
         }

         public void onError(final int errorCode, final String errorMessage)
         {
            failures.incrementAndGet();
         }

      });

      Assert.assertEquals(1, failures.get());
      Assert.assertEquals(0, operations.get());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
