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

package org.hornetq.tests.unit.util;

import java.util.concurrent.CountDownLatch;

import junit.framework.Assert;

import org.hornetq.core.logging.Logger;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.VariableLatch;

/**
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * 
 */
public class VariableLatchTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(VariableLatchTest.class);

   public void testLatchOnSingleThread() throws Exception
   {
      VariableLatch latch = new VariableLatch();

      for (int i = 1; i <= 100; i++)
      {
         latch.up();
         Assert.assertEquals(i, latch.getCount());
      }

      for (int i = 100; i > 0; i--)
      {
         Assert.assertEquals(i, latch.getCount());
         latch.down();
         Assert.assertEquals(i - 1, latch.getCount());
      }

      latch.waitCompletion();
   }

   /**
    * 
    * This test will open numberOfThreads threads, and add numberOfAdds on the
    * VariableLatch After those addthreads are finished, the latch count should
    * be numberOfThreads * numberOfAdds Then it will open numberOfThreads
    * threads again releasing numberOfAdds on the VariableLatch After those
    * releaseThreads are finished, the latch count should be 0 And all the
    * waiting threads should be finished also
    * 
    * @throws Exception
    */
   public void testLatchOnMultiThread() throws Exception
   {
      final VariableLatch latch = new VariableLatch();

      latch.up(); // We hold at least one, so ThreadWaits won't go away

      final int numberOfThreads = 100;
      final int numberOfAdds = 100;

      class ThreadWait extends Thread
      {
         private volatile boolean waiting = true;

         @Override
         public void run()
         {
            try
            {
               if (!latch.waitCompletion(5000))
               {
                  VariableLatchTest.log.error("Latch timed out");
               }
            }
            catch (Exception e)
            {
               VariableLatchTest.log.error(e);
            }
            waiting = false;
         }
      }

      class ThreadAdd extends Thread
      {
         private final CountDownLatch latchReady;

         private final CountDownLatch latchStart;

         ThreadAdd(final CountDownLatch latchReady, final CountDownLatch latchStart)
         {
            this.latchReady = latchReady;
            this.latchStart = latchStart;
         }

         @Override
         public void run()
         {
            try
            {
               latchReady.countDown();
               // Everybody should start at the same time, to worse concurrency
               // effects
               latchStart.await();
               for (int i = 0; i < numberOfAdds; i++)
               {
                  latch.up();
               }
            }
            catch (Exception e)
            {
               VariableLatchTest.log.error(e.getMessage(), e);
            }
         }
      }

      CountDownLatch latchReady = new CountDownLatch(numberOfThreads);
      CountDownLatch latchStart = new CountDownLatch(1);

      ThreadAdd[] threadAdds = new ThreadAdd[numberOfThreads];
      ThreadWait waits[] = new ThreadWait[numberOfThreads];

      for (int i = 0; i < numberOfThreads; i++)
      {
         threadAdds[i] = new ThreadAdd(latchReady, latchStart);
         threadAdds[i].start();
         waits[i] = new ThreadWait();
         waits[i].start();
      }

      latchReady.await();
      latchStart.countDown();

      for (int i = 0; i < numberOfThreads; i++)
      {
         threadAdds[i].join();
      }

      for (int i = 0; i < numberOfThreads; i++)
      {
         Assert.assertTrue(waits[i].waiting);
      }

      Assert.assertEquals(numberOfThreads * numberOfAdds + 1, latch.getCount());

      class ThreadDown extends Thread
      {
         private final CountDownLatch latchReady;

         private final CountDownLatch latchStart;

         ThreadDown(final CountDownLatch latchReady, final CountDownLatch latchStart)
         {
            this.latchReady = latchReady;
            this.latchStart = latchStart;
         }

         @Override
         public void run()
         {
            try
            {
               latchReady.countDown();
               // Everybody should start at the same time, to worse concurrency
               // effects
               latchStart.await();
               for (int i = 0; i < numberOfAdds; i++)
               {
                  latch.down();
               }
            }
            catch (Exception e)
            {
               VariableLatchTest.log.error(e.getMessage(), e);
            }
         }
      }

      latchReady = new CountDownLatch(numberOfThreads);
      latchStart = new CountDownLatch(1);

      ThreadDown down[] = new ThreadDown[numberOfThreads];

      for (int i = 0; i < numberOfThreads; i++)
      {
         down[i] = new ThreadDown(latchReady, latchStart);
         down[i].start();
      }

      latchReady.await();
      latchStart.countDown();

      for (int i = 0; i < numberOfThreads; i++)
      {
         down[i].join();
      }

      Assert.assertEquals(1, latch.getCount());

      for (int i = 0; i < numberOfThreads; i++)
      {
         Assert.assertTrue(waits[i].waiting);
      }

      latch.down();

      for (int i = 0; i < numberOfThreads; i++)
      {
         waits[i].join();
      }

      Assert.assertEquals(0, latch.getCount());

      for (int i = 0; i < numberOfThreads; i++)
      {
         Assert.assertFalse(waits[i].waiting);
      }
   }

   public void testReuseLatch() throws Exception
   {
      final VariableLatch latch = new VariableLatch();
      latch.up();

      class ThreadWait extends Thread
      {
         private volatile boolean waiting = false;

         private volatile Exception e;

         private final CountDownLatch readyLatch = new CountDownLatch(1);

         @Override
         public void run()
         {
            waiting = true;
            readyLatch.countDown();
            try
            {
               if (!latch.waitCompletion(1000))
               {
                  VariableLatchTest.log.error("Latch timed out!", new Exception("trace"));
               }
            }
            catch (Exception e)
            {
               VariableLatchTest.log.error(e);
               this.e = e;
            }
            waiting = false;
         }
      }

      ThreadWait t = new ThreadWait();
      t.start();

      t.readyLatch.await();

      Assert.assertEquals(true, t.waiting);

      latch.down();

      t.join();

      Assert.assertEquals(false, t.waiting);

      Assert.assertNull(t.e);

      latch.up();

      t = new ThreadWait();
      t.start();

      t.readyLatch.await();

      Assert.assertEquals(true, t.waiting);

      latch.down();

      t.join();

      Assert.assertEquals(false, t.waiting);

      Assert.assertNull(t.e);

      Assert.assertTrue(latch.waitCompletion(1000));

      Assert.assertEquals(0, latch.getCount());

      latch.down();

      Assert.assertEquals(0, latch.getCount());

   }

}
