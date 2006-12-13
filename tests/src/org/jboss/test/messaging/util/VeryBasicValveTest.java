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

package org.jboss.test.messaging.util;

import junit.framework.TestCase;
import org.jboss.logging.Logger;
import org.jboss.jms.util.Valve;

/**
 * This verifies the very basic functionality of ConnectionState.Valve.
 * Two functions can't enter at the same time, and this will test that routine
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision:$</tt>
 *          <p/>
 *          $Id:$
 */
public class VeryBasicValveTest extends TestCase
{
   private static Logger log = Logger.getLogger(VeryBasicValveTest.class);

   static int counter = 0;
   static int counterWait = 0;

   static boolean started=false;
   static Object startSemaphore = new Object();
   static Valve valve = new Valve();

   // multiple threads opening/closing a thread.
   // only one should be able to open it
   public static class SomeThread extends Thread
   {
      int threadId;
      public SomeThread(int threadId)
      {
         this.threadId = threadId;
      }
      public void run()
      {
         try
         {
            log.info("Starting Thread " + threadId);
            synchronized (startSemaphore)
            {
               if (!started)
               {
                  startSemaphore.wait();
               }
            }
            //log.info("Thread " + threadId + "Opening valve");
            if (!valve.open())
            {
               //log.info("Valve couldn't be opened at thread " + threadId);
               synchronized (VeryBasicValveTest.class)
               {
                  counterWait ++;
               }
            } else
            {

               //log.info("Thread " + threadId + " could open the valve");

               //Thread.sleep(1000);

               synchronized (VeryBasicValveTest.class)
               {
                  counter ++;
               }
               valve.close();
            }

            //log.info("Thread " + threadId + " is now closing the valve");

         }
         catch (Exception e)
         {
            e.printStackTrace();
         }
      }
   }


   public void testValve() throws Exception
   {
      SomeThread thread[] = new SomeThread[2500];

      for (int i=0; i<thread.length; i++)
      {
         thread[i] = new SomeThread(i);
      }

      for (int i=0; i<thread.length; i++)
      {
         thread[i].start();
      }

      synchronized (startSemaphore)
      {
         started=true;
         startSemaphore.notifyAll();
      }

      for (int i = 0; i < thread.length; i++)
      {
         thread[i].join();
      }

      assertEquals(1, counter);
      assertEquals(thread.length-1, counterWait);

   }

}
