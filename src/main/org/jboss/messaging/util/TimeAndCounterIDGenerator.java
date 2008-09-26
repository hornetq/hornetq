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
 * Note: This sequence generator is valid as long as you generate less than 268435455 (fffffff) IDs per millisecond
 * </p>
 * <p>
 * (what is impossible at this point, This class alone will probably take a few seconds to generate this many IDs)
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
   private static final int BITS_TO_MOVE = 28;

   // We take one bit out, as we don't want negative numbers
   // With 4 bytes + 4 bits, we would minimize the possibility of duplicate IDs.
   // The date portion would be repeated on every 397 days
   //
   // 4 bytes + 4 bits without the signal bit
   public static final long MASK_TIME = 0x7ffffffffl;

   // Attributes ----------------------------------------------------

   private final AtomicLong counter = new AtomicLong(0);

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

      long retValue = counter.incrementAndGet();

      // The probability of a negative is very low.
      // The server has to be started at the exact millisecond (or very close) to when
      // System.currentTimeMillis() == **7ffffffffl or **fffffffffl(what would
      // happen every 397 days and few hours), for instance:
      // (117FFFFFFFF = Sat Feb 09 15:00:42 GMT-06:00 2008).
      // But I still wanted to verify this for correctness.
      while (retValue < 0)
      {
         refresh();
         retValue = counter.incrementAndGet();
      }

      return retValue;
   }

   public long getCurrentID()
   {
      return counter.get();
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
      long oldTm = counter.get() >> BITS_TO_MOVE;
      long newTm = newTM();

      // To avoid quick restarts on testcases.
      // In a real scenario this will never happen, as refresh is called only on constructor or when the first bit on
      // the counter explodes
      // And that would happen only at one specific millisecond every 368 days, and that would never hit this case
      while (newTm == oldTm)
      {
         try
         {
            Thread.sleep(20);
         }
         catch (InterruptedException e)
         {
         }
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
