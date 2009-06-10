/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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


package org.jboss.messaging.tests.unit.core.asyncio;

import junit.framework.TestCase;

import org.jboss.messaging.core.asyncio.impl.AsynchronousFileImpl;

/**
 * A SleepTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class SleepTest extends TestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

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
      
      for (long i = 0 ; i < nloops; i++)
      {
         AsynchronousFileImpl.nanoSleep();
      }
      
      long end = System.currentTimeMillis();
      
      long expectedTime = (timeInterval * nloops / 1000000l);
      
      System.out.println("TotalTime = " +  (end - time) + " expected = " + expectedTime);
      
      assertTrue((end - time) >= expectedTime);
      
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      if (!AsynchronousFileImpl.isLoaded())
      {
         fail(String.format("libAIO is not loaded on %s %s %s",
                            System.getProperty("os.name"),
                            System.getProperty("os.arch"),
                            System.getProperty("os.version")));
      }
   }
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
