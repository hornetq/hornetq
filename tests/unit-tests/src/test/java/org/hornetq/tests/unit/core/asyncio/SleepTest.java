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

package org.hornetq.tests.unit.core.asyncio;

import junit.framework.Assert;
import junit.framework.TestSuite;

import org.hornetq.core.asyncio.impl.AsynchronousFileImpl;
import org.hornetq.tests.util.UnitTestCase;

/**
 * A SleepTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class SleepTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static TestSuite suite()
   {
      return UnitTestCase.createAIOTestSuite(SleepTest.class);
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testNanoSleep() throws Exception
   {
      AsynchronousFileImpl.setNanoSleepInterval(1);
      AsynchronousFileImpl.nanoSleep();

      long timeInterval = 1000000;
      long nloops = 1000;

      AsynchronousFileImpl.setNanoSleepInterval((int)timeInterval);

      long time = System.currentTimeMillis();

      for (long i = 0; i < nloops; i++)
      {
         AsynchronousFileImpl.nanoSleep();
      }

      long end = System.currentTimeMillis();

      long expectedTime = timeInterval * nloops / 1000000l;

      System.out.println("TotalTime = " + (end - time) + " expected = " + expectedTime);

      Assert.assertTrue(end - time >= expectedTime);

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      if (!AsynchronousFileImpl.isLoaded())
      {
         Assert.fail(String.format("libAIO is not loaded on %s %s %s",
                                   System.getProperty("os.name"),
                                   System.getProperty("os.arch"),
                                   System.getProperty("os.version")));
      }
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
