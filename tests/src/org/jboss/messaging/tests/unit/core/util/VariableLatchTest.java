/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
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

package org.jboss.messaging.tests.unit.core.util;

import java.util.concurrent.CountDownLatch;

import junit.framework.TestCase;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.util.VariableLatch;


/**
 * 
 * @author clebert
 *
 */
public class VariableLatchTest extends TestCase
{
   private static final Logger log = Logger.getLogger(VariableLatchTest.class);
   
   
   public void testLatchOnSingleThread() throws Exception
   {
      VariableLatch latch = new VariableLatch();
      
      for (int i=1;i<=10000;i++)
      {
         latch.up();
         assertEquals(i, latch.getCount());
      }
      
      for (int i=10000; i>0; i--)
      {
         assertEquals(i, latch.getCount());
         latch.down();
         assertEquals(i-1, latch.getCount());
      }
      
      latch.waitCompletion();
   }

   public void testTimeout() throws Exception
   {
      VariableLatch latch = new VariableLatch();
      
      latch.up();

      long start = System.currentTimeMillis();
      try
      {
         latch.waitCompletion(1000);
         fail ("It was suppsoed to throw an exception");
      }
      catch (Exception ignored)
      {
      }
      long end = System.currentTimeMillis();
      
      assertTrue ("Timeout didn't work correctly", end - start >= 1000 && end - start < 2000);
   }

   /** 
    * 
    * This test will open numberOfThreads threads, and add numberOfAdds on the VariableLatch
    * After those addthreads are finished, the latch count should be numberOfThreads * numberOfAdds
    * Then it will open numberOfThreads threads again releasing numberOfAdds on the VariableLatch
    * After those releaseThreads are finished, the latch count should be 0
    * And all the waiting threads should be finished also
    * 
    * @throws Exception
    */
   public void testLatchOnMultiThread() throws Exception
   {
      final VariableLatch latch = new VariableLatch();
      
      latch.up(); // We hold at least one, so ThreadWaits won't go away  
      
      final int numberOfThreads = 100;
      final int numberOfAdds = 1000;
      
      class ThreadWait extends Thread
      {
         boolean waiting = true;
         public void run()
         {
            try
            {
               latch.waitCompletion(5000);
            }
            catch (Exception e)
            {
               log.error(e);
            }
            waiting = false;
         }
      }
      
      class ThreadAdd extends Thread
      {
         CountDownLatch latchReady; 
         CountDownLatch latchStart;
         
         ThreadAdd(CountDownLatch latchReady, CountDownLatch latchStart)
         {
            this.latchReady = latchReady;
            this.latchStart = latchStart;
         }
         
         public void run()
         {
            try
            {
               latchReady.countDown();
               // Everybody should start at the same time, to worse concurrency effects
               latchStart.await();
               for (int i=0; i< numberOfAdds; i++)
               {
                  latch.up();
               }
            }
            catch (Exception e)
            {
               log.error(e.getMessage(), e);
            }
         }
      }
      
      CountDownLatch latchReady = new CountDownLatch(numberOfThreads);
      CountDownLatch latchStart = new CountDownLatch(1);
      
      ThreadAdd[] threadAdds = new ThreadAdd[numberOfThreads];
      ThreadWait waits[] = new ThreadWait[numberOfThreads];
      
      
      for (int i=0; i< numberOfThreads; i++)
      {
         threadAdds[i] = new ThreadAdd(latchReady, latchStart);
         threadAdds[i].start();
         waits[i] = new ThreadWait();
         waits[i].start();
      }
      
      latchReady.await();
      latchStart.countDown();
      
      
      for (int i=0; i< numberOfThreads; i++)
      {
         threadAdds[i].join();
      }
      
      for (int i=0; i< numberOfThreads; i++)
      {
         assertTrue(waits[i].waiting);
      }
      
      assertEquals(numberOfThreads * numberOfAdds + 1, latch.getCount());
      
      class ThreadDown extends Thread
      {
         CountDownLatch latchReady; 
         CountDownLatch latchStart;
         
         ThreadDown(CountDownLatch latchReady, CountDownLatch latchStart)
         {
            this.latchReady = latchReady;
            this.latchStart = latchStart;
         }
         
         public void run()
         {
            try
            {
               latchReady.countDown();
               // Everybody should start at the same time, to worse concurrency effects
               latchStart.await();
               for (int i=0; i< numberOfAdds; i++)
               {
                  latch.down();
               }
            }
            catch (Exception e)
            {
               log.error(e.getMessage(), e);
            }
         }
      }
      
      latchReady = new CountDownLatch(numberOfThreads);
      latchStart = new CountDownLatch(1);
      
      ThreadDown down[] = new ThreadDown[numberOfThreads];
      
      for (int i=0; i< numberOfThreads; i++)
      {
         down[i] = new ThreadDown(latchReady, latchStart);
         down[i].start();
      }
      
      latchReady.await();
      latchStart.countDown();
      
      for (int i=0; i< numberOfThreads; i++)
      {
         down[i].join();
      }
      
      assertEquals(1, latch.getCount());

      for (int i=0; i< numberOfThreads; i++)
      {
         assertTrue(waits[i].waiting);
      }

      latch.down();
      
      for (int i=0; i< numberOfThreads; i++)
      {
         waits[i].join();
      }
      
      assertEquals(0, latch.getCount());
      
      for (int i=0; i< numberOfThreads; i++)
      {
         assertFalse (waits[i].waiting);
      }
   }
   
   public void testReuseLatch() throws Exception
   {
      final VariableLatch latch = new VariableLatch();
      latch.up();
      
      class ThreadWait extends Thread
      {
         boolean waiting = false;
         Exception e;
         CountDownLatch readyLatch = new CountDownLatch(1);
         public void run()
         {
            waiting = true;
            readyLatch.countDown();
            try
            {
               latch.waitCompletion(5);
            }
            catch (Exception e)
            {
               log.error(e);
               this.e = e;
            }
             waiting = false;
         }
      }
      
      ThreadWait t = new ThreadWait();
      t.start();
      
      t.readyLatch.await();
      
      assertEquals(true, t.waiting);
      
      latch.down();

      t.join();

      assertEquals(false, t.waiting);

      assertNull(t.e);
      
      latch.up();
      
      t = new ThreadWait();
      t.start();
      
      t.readyLatch.await();
      
      assertEquals(true, t.waiting);

      latch.down();
      
      t.join();
      
      assertEquals(false, t.waiting);
      
      assertNull(t.e);
      
      
      
      
   }
   

}
