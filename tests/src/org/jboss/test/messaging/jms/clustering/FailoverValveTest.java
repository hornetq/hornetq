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

package org.jboss.test.messaging.jms.clustering;

import org.jboss.jms.client.FailoverValve;
import org.jboss.test.messaging.MessagingTestCase;
import EDU.oswego.cs.dl.util.concurrent.Slot;


/**
 * Regression Tests for a dead lock condition fixed on FailoverValve.
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class FailoverValveTest extends MessagingTestCase
{
   // Constants ------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   Object semaphore = new Object();
   boolean running = false;

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public FailoverValveTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------


   // You can have multiple threads trying to close the valve at the same time.
   public void testMultipleThreadsClosingValve() throws Exception
   {
      FailoverValve valve = new FailoverValve();

      ValveThread threads[] = new ValveThread[100];

      for (int i = 0; i < threads.length; i++)
      {
         threads[i] = new ValveThread(valve);
      }

      for (int i = 0; i < threads.length; i++)
      {
         threads[i].start();
      }

      // time to a line up
      Thread.sleep(1000);

      synchronized (semaphore)
      {
         running = true;
         semaphore.notifyAll();
      }

      for (int i = 0; i < threads.length; i++)
      {
         threads[i].join();
         if (threads[i].failed)
         {
            fail("One of threads had a failure, look at logs");
         }
      }
   }

   // Validate weird usages that are supposed to throw exceptions
   public void testValidateExceptions() throws Exception
   {
      try
      {
         FailoverValve  valve = new FailoverValve ();
         valve.open();
         valve.close();
         valve.close(); // closing without opening should throw an exception
         fail("Valve.close didn't generate an exception on an extra close, Valve is not safe!");
      }
      catch (Throwable e)
      {
         //e.printStackTrace();
      }

      try
      {
         FailoverValve valve = new FailoverValve ();
         valve.enter();
         valve.leave();
         valve.leave(); // extra leave call, should throw an exception
         fail("Valve.close didn't generate an exception, Valve is not safe!");
      }
      catch (Throwable e)
      {
         //e.printStackTrace();
      }

   }

   public void testSimpleClose() throws Exception
   {
      final FailoverValve valve = new FailoverValve(2000);
      final Slot slot = new Slot();

      // prevent the valve from being possible to close

      valve.enter();

      new Thread(new Runnable()
      {
         public void run()
         {
            try
            {
               valve.close();
               slot.put("CLOSED");
            }
            catch(InterruptedException e)
            {
               log.error(e);
            }
         }
      }, "Closer").start();

      log.info("attempting to close for 5 secs ...");

      // valve cannot be closed at this time
      Object o = slot.poll(5000);
      assertNull(o);

      // exit the valve
      valve.leave();
      o = slot.take();
      assertNotNull(o);
      assertEquals("CLOSED", o);
   }

   public void testSimpleClose2() throws Exception
   {
      final FailoverValve valve = new FailoverValve(2000);
      final Slot slot = new Slot();

      // flip-flop the valve

      valve.enter();
      valve.leave();

      new Thread(new Runnable()
      {
         public void run()
         {
            try
            {
               valve.close();
               slot.put("CLOSED");
            }
            catch(InterruptedException e)
            {
               log.error(e);
            }
         }
      }, "Closer").start();

      log.info("attempting to close ...");

      // valve cannot be closed at this time
      Object o = slot.poll(5000);
      assertNotNull(o);
      assertEquals("CLOSED", o);
   }


   // This testcase is invalid!
   // You can't close the valve until all the threads are completed...
   // or all threads are trying to close the valve.
   //
   // You can call close whenever you want.. .but you can't complete that execution
   //   if there is a thread holding a readLock.
   /*public void testConcurrentClose() throws Exception
   {
      int THREAD_COUNT = 10;
      final FailoverValve valve = new FailoverValve(10000);
      final Slot[] slot = new Slot[THREAD_COUNT];

      // prevent the valve from being possible to close

      log.info("entering the valve");

      valve.enter();

      // attempt to close the valve from 10 concurrent threads
      for(int i = 0; i < THREAD_COUNT; i++)
      {
         slot[i] = new Slot();
         final int ii = i;

         new Thread(new Runnable()
         {
            public void run()
            {
               try
               {
                  log.info("attempting to close");

                  valve.close();
                  slot[ii].put("CLOSED");
               }
               catch(InterruptedException e)
               {
                  log.error(e);
               }
            }
         }, "Closer" + i).start();

      }

      // wait a second so they'll attempt closing
      log.info("sleeping for 2 seconds");
      Thread.sleep(2000);
      log.info("slept");

      // make sure none closed the valve

      for(int i = 0; i < THREAD_COUNT; i++)
      {
         Object o = slot[i].peek();
         assertNull(o);
      }

      log.info("leaving the valve");
      valve.leave();

      // the valve should be "closeable", so all waiting threads, plus a new one, should be able
      // to close it

      final Slot loneSlot = new Slot();

      new Thread(new Runnable()
      {
         public void run()
         {
            try
            {
               log.info("attempting to close");
               valve.close();
               loneSlot.put("CLOSED");
            }
            catch(InterruptedException e)
            {
               log.error(e);
            }
         }
      }, "LoneCloser").start();


      log.info("valve should be closed by now ...");
      Object o = loneSlot.poll(3000);
      assertNotNull(o);
      assertEquals("CLOSED", o);

      for(int i = 0; i < THREAD_COUNT; i++)
      {
         o = slot[i].poll(3000);
         assertNull(o);
         assertEquals("CLOSED", o);
      }
   }  TODO: Delete this TestCase... I'm keeping it for now! */


   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

   class ValveThread extends Thread
   {
      FailoverValve valve;

      boolean failed;

      public ValveThread(FailoverValve  valve)
      {
         this.valve = valve;
      }

      public void run()
      {
         try
         {
            synchronized (semaphore)
            {
               if (!running)
               {
                  semaphore.wait();
               }
            }
            for (int i = 0; i < 10; i++)
            {
               valve.enter();
            }
            valve.close();
            valve.open();
            for (int i = 0; i < 10; i++)
            {
               valve.leave();
            }
         }
         catch (Exception e)
         {
            e.printStackTrace();
            failed = true;
         }
      }
   }

}
