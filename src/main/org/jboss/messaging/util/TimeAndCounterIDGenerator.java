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
 * Note: This sequence generator is valid as long as you generate less than 268435455 (fffffff) IDs per 250 millisecond
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
   private static final int BITS_TO_MOVE = 20;

    public static final long MASK_TIME = 0xFFFFFFFFF00l;

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
      return counter.incrementAndGet();
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
      long oldTm = tmMark;
      long newTm = newTM();

      while (newTm == oldTm)
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
