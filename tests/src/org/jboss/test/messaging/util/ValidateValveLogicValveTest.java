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
import org.jboss.messaging.util.Valve;

/**
 *
 * There is a relevant comment into <@link ValidateValveLogicReadWriteTest> 
 * @see ValidateValveLogicReadWriteTest
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision:$</tt>
 *          <p/>
 *          $Id:$
 */
public class ValidateValveLogicValveTest extends TestCase
{
   private static Logger log = Logger.getLogger(ValidateValveLogicValveTest.class);

   boolean useCounterA;

   public synchronized boolean isUseCounterA()
   {
      return useCounterA;
   }

   public synchronized void setUseCounterA(boolean useCounterA)
   {
      this.useCounterA = useCounterA;
   }


   boolean keepRunning=true;

   long counterA = 0;
   long counterB = 0;

   public synchronized void addCounterA()
   {
      counterA++;
   }

   public synchronized void addCounterB()
   {
      counterB++;
   }


   public synchronized long getCounterA()
   {
      return counterA;
   }

   public synchronized long getCounterB()
   {
      return counterB;
   }

   Valve valve = new Valve();

   boolean started=false;
   private Object startSemaphore = new Object();


   public void testValveLogic() throws Exception
   {
      ValidateValveLogicValveTest.ThreadRead readThreads [] = new ValidateValveLogicValveTest.ThreadRead[1000];
      for (int i=0; i<readThreads.length; i++)
      {
         readThreads[i] = new ValidateValveLogicValveTest.ThreadRead(i);
      }

      for (int i=0; i<readThreads.length; i++)
      {
         readThreads[i].start();
      }

      synchronized (startSemaphore)
      {
         started=true;
         startSemaphore.notifyAll();
      }


      log.info("Sleeping 10 seconds");
      Thread.sleep(10000);

      log.info("Acquiring write lock");
      valve.open();

      // Time to wait current calls to finish
      // As valve doesn't need synchronization on running threads,
      // this sleep is necessary to wait everybody to be on the waiting condition.
      // And this is meant to be this way. We don't want to stop threads ot calls that are already working,
      // We want to stop new ones only 
      Thread.sleep(1000);

      setUseCounterA(false);

      long counterAOriginal = getCounterA();
      long counterBOriginal = getCounterB();


      log.info ("Waiting 5 seconds");
      Thread.sleep(5000);

      assertEquals(counterAOriginal, getCounterA());
      assertEquals(counterBOriginal, getCounterB());

      valve.reset();

      valve.isOpened(true);

      log.info("Acquiring read lock");

      counterBOriginal = getCounterB();

      log.info ("Waiting 5 seconds");
      Thread.sleep(5000);

      log.info ("Threads produced " + (getCounterB() - counterBOriginal));
      assertEquals(counterAOriginal, getCounterA());
      assertTrue(getCounterB()>counterBOriginal);

      keepRunning = false;


      for (int i=0; i<readThreads.length; i++)
      {
         readThreads[i].join();
      }

   }

   // multiple threads opening/closing a thread.
   // only one should be able to open it
   public class ThreadRead extends Thread
   {
      int threadId;
      public ThreadRead(int threadId)
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

            while (keepRunning)
            {
               valve.isOpened(true);
               if (isUseCounterA())
               {
                  //log.info("Thread " + threadId + " adding A");
                  addCounterA();
               }
               else
               {
                  //log.info("Thread " + threadId + " adding B");
                  addCounterB();
               }
            }

         }
         catch (Exception e)
         {
            e.printStackTrace();
         }
      }
   }

}
