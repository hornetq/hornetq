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

package org.jboss.messaging.util;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A TimeAndCounterIDGenerator
 * <p>
 * This IDGenerator doesn't support more than 16777215 IDs per 16 millisecond. It would throw an exception if this happens.
 * </p>
 * 
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a> Created Sep 24, 2008 11:54:10 AM
 */
public class TimeAndCounterIDGenerator implements IDGenerator
{
   // Constants ----------------------------------------------------

   /**
    * Bits to move the date accordingly to MASK_TIME
    */
   private static final int BITS_TO_MOVE = 20;

   public static final long MASK_TIME = 0x7fffffffff0l;
   
   //44 bits of time and 20 bits of counter

   public static final long ID_MASK = 0xffffffl;

   private static final long TIME_ID_MASK = 0x7fffffffff000000l;

   // Attributes ----------------------------------------------------

   private final AtomicLong counter = new AtomicLong(0);

   private volatile boolean wrapped = false;

   private volatile long tmMark;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public TimeAndCounterIDGenerator()
   {
      refresh();
   }

   // Public --------------------------------------------------------

   // Public --------------------------------------------------------

   public long generateID()
   {
      long idReturn = counter.incrementAndGet();

      if ((idReturn & ID_MASK) == 0)
      {
         final long timePortion = idReturn & TIME_ID_MASK;

         // Wrapping ID logic

         if (timePortion >= newTM())
         {
            // Unlikely to happen

            wrapped = true;

         }
         else
         {
            // Else.. no worry... we will just accept the new time portion being added
            // This time-mark would have been generated some time ago, so this is ok.
            // tmMark is just a cache to validate the MaxIDs, so there is no need to make it atomic (synchronized)
            tmMark = timePortion;
         }
      }

      if (wrapped)
      {
         // This will only happen if a computer can generate more than ID_MASK ids (16 million IDs per 16
         // milliseconds)
         // If this wrapping code starts to happen, it needs revision
         throw new IllegalStateException("The IDGenerator is being overlaped, and it needs revision as the system generated more than " + ID_MASK +
                                         " ids per 16 milliseconds which exceeded the IDgenerator limit");
      }

      return idReturn;
   }

   public long getCurrentID()
   {
      return counter.get();
   }

   
   // for use in testcases
   public long getInternalTimeMark()
   {
      return tmMark;
   }
   
   // for use in testcases
   public void setInternalID(final long id)
   {
      counter.set(tmMark | id);
   }

   // for use in testcases
   public void setInternalDate(final long date)
   {
      tmMark = (date & MASK_TIME) << BITS_TO_MOVE;
      counter.set(tmMark);
   }

   public synchronized void refresh()
   {
      long oldTm = tmMark;
      long newTm = newTM();

      while (newTm <= oldTm)
      {
         newTm = newTM();
      }
      tmMark = newTm;
      counter.set(tmMark);
   }

   @Override
   public String toString()
   {
      long currentCounter = counter.get();
      return "SequenceGenerator(tmMark=" + hex(tmMark) +
             ", CurrentCounter = " +
             currentCounter +
             ", HexCurrentCounter = " +
             hex(currentCounter) +
             ")";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private long newTM()
   {
      return (System.currentTimeMillis() & MASK_TIME) << BITS_TO_MOVE;
   }

   private String hex(final long x)
   {
      return String.format("%1$X", x);
   }

}
