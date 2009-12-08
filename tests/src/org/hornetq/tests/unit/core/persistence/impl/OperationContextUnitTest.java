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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

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

      context.onError(1, "Poop happens!");

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
