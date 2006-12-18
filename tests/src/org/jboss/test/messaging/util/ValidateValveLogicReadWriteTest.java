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
import EDU.oswego.cs.dl.util.concurrent.ReadWriteLock;
import EDU.oswego.cs.dl.util.concurrent.ReentrantWriterPreferenceReadWriteLock;
import EDU.oswego.cs.dl.util.concurrent.ReaderPreferenceReadWriteLock;

/**
 *
 * ValidateValveLogicReadWriteTest and ValidateValveLogicValveTest were written
 * to validate lock mechanism to be used on ValveAspect.
 *
 * ValidateValveLogicTestValveTest uses org.jboss.messaging.util.Valve which avoid locking on reading threads
 * ValidateValveLogicReadWriteTest uses oswego.concurrent.ReadWriteLock which adds synchronization into reading threads also
 *
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision:$</tt>
 *          <p/>
 *          $Id:$
 */
public class ValidateValveLogicReadWriteTest extends TestCase
{
   private static Logger log = Logger.getLogger(ValidateValveLogicReadWriteTest.class);

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

   private ReadWriteLock lockValve ;
   boolean started=false;
   private Object startSemaphore = new Object();


   protected void setUp() throws Exception
   {
      super.setUp();    //To change body of overridden methods use File | Settings | File Templates.
      counterA=0;
      counterB=0;
      keepRunning=true;
      useCounterA=true;
      
   }

   public void testValveWithReentrantWriterPreferenceReadWriteLock() throws Exception
   {
      log.info("++testValveWithReentrantWriterPreferenceReadWriteLock");
      lockValve = new ReentrantWriterPreferenceReadWriteLock();
      internaltestValveLogic();
   }

   public void testValveWithReaderPreferenceReadWriteLock() throws Exception
   {
      log.info("++testValveWithReaderPreferenceReadWriteLock");
      lockValve = new ReaderPreferenceReadWriteLock();
      internaltestValveLogic();
   }

   public void internaltestValveLogic() throws Exception
   {
      ThreadRead readThreads [] = new ThreadRead[1000];
      for (int i=0; i<readThreads.length; i++)
      {
         readThreads[i] = new ThreadRead(i);
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
      lockValve.writeLock().acquire();
      setUseCounterA(false);

      long counterAOriginal = getCounterA();
      long counterBOriginal = getCounterB();

      log.info ("Waiting 5 seconds");
      Thread.sleep(5000);

      assertEquals(counterAOriginal, getCounterA());
      assertEquals(counterBOriginal, getCounterB());

      lockValve.writeLock().release();


      log.info("Acquiring read lock");
      lockValve.readLock().acquire();

      counterBOriginal = getCounterB();

      log.info ("Waiting 5 seconds");
      Thread.sleep(5000);

      log.info("Threads produced " + (getCounterB() - counterBOriginal));
      assertEquals(counterAOriginal, getCounterA());
      assertTrue(getCounterB()>counterBOriginal);
      lockValve.readLock().release();

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
            //log.info("Starting Thread " + threadId);
            synchronized (startSemaphore)
            {
               if (!started)
               {
                  startSemaphore.wait();
               }
            }

            while (keepRunning)
            {
               lockValve.readLock().acquire();
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
               lockValve.readLock().release();
            }

         }
         catch (Exception e)
         {
            e.printStackTrace();
         }
      }
   }

}
