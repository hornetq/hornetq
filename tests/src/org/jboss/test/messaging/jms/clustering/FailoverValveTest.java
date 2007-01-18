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
import org.jboss.logging.Logger;
import org.jboss.test.messaging.MessagingTestCase;


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
   private static final Logger log = Logger.getLogger(FailoverValveTest.class);

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

      assertEquals (0, valve.getActiveLocks());
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
