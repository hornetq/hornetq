/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.unit.util;

import java.util.concurrent.CountDownLatch;

import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.ConcurrentHashSet;
import org.jboss.messaging.util.TimeAndCounterIDGenerator;

/**
 * A TimeAndCounterIDGeneratorTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created 24-Sep-08 3:42:25 PM
 *
 *
 */
public class TimeAndCounterIDGeneratorTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testCalculation()
   {
      TimeAndCounterIDGenerator seq = new TimeAndCounterIDGenerator();
      long max = 100000;

      long lastNr = 0;

      for (long i = 0; i < max; i++)
      {
         if (i % 1 == 1000)
         {
            seq.refresh();
         }

         long seqNr = seq.generateID();

         assertTrue("The sequence generator should aways generate crescent numbers", seqNr > lastNr);

         lastNr = seqNr;
      }

   }

   public void testCalculationOnMultiThread() throws Throwable
   {

      for (int i = 0; i < 10; i++)
      {
         internaltestCalculationOnMultiThread();
      }
   }

   public void internaltestCalculationOnMultiThread() throws Throwable
   {
      final ConcurrentHashSet<Long> hashSet = new ConcurrentHashSet<Long>();

      final TimeAndCounterIDGenerator seq = new TimeAndCounterIDGenerator();

      seq.setInternalID(Integer.MAX_VALUE - 50);

      final int NUMBER_OF_THREADS = 100;

      final int NUMBER_OF_IDS = 10;

      final CountDownLatch latchAlign = new CountDownLatch(NUMBER_OF_THREADS);

      final CountDownLatch latchStart = new CountDownLatch(1);

      class T1 extends Thread
      {
         Throwable e;

         @Override
         public void run()
         {
            try
            {
               latchAlign.countDown();
               latchStart.await();

               long lastValue = 0l;
               for (int i = 0; i < NUMBER_OF_IDS; i++)
               {
                  long value = seq.generateID();
                  assertTrue(hex(value) + " should be greater than " + hex(lastValue) + " on seq " + seq.toString(),
                             value > lastValue);
                  lastValue = value;

                  hashSet.add(value);
               }
            }
            catch (Throwable e)
            {
               this.e = e;
            }
         }

      };

      T1[] arrays = new T1[NUMBER_OF_THREADS];

      for (int i = 0; i < arrays.length; i++)
      {
         arrays[i] = new T1();
         arrays[i].start();
      }

      latchAlign.await();

      latchStart.countDown();

      for (T1 t : arrays)
      {
         t.join();
         if (t.e != null)
         {
            throw t.e;
         }
      }

      assertEquals(NUMBER_OF_THREADS * NUMBER_OF_IDS, hashSet.size());

      hashSet.clear();

   }

   private static String hex(final long value)
   {
      return String.format("%1$X", value);
   }

}
