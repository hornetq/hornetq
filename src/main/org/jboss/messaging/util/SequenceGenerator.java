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
 * A SequenceGenerator
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
  * 
 * Created Sep 24, 2008 11:54:10 AM
 *
 *
 */
public class SequenceGenerator
{

   // (0x7fffffff) We take one bit out, as we don't want negative numbers
   // (take out the signal bit before merging the numbers)
   private static final long MASK_TIME = Integer.MAX_VALUE;

   // Attributes ----------------------------------------------------

   /**
    * Using a long just to avoid making cast conversions on every ID generated
    */
   private final AtomicLong counter = new AtomicLong(0);

   private volatile long tmMark;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SequenceGenerator()
   {
      refresh();
   }

   // Public --------------------------------------------------------

   public long generateID()
   {

      long value = counter.incrementAndGet();

      if (value >= Integer.MAX_VALUE)
      {
         synchronized (this)
         {
            if (counter.get() >= Integer.MAX_VALUE)
            {
               refresh();
            }
            value = counter.incrementAndGet();
         }
      }

      return tmMark | value;
   }

   public void setInternalID(final long id)
   {
      counter.set(id);
   }

   public synchronized void refresh()
   {
      long newTm = newTM();

      // To avoid quick restarts.
      // This shouldn't ever happen.
      // I doubt any system will be able to generate more than Integer.MAX_VALUE
      // ids per millisecond.
      // This would be used only on testcases validating the logic of the class
      while (newTm <= tmMark)
      {
         System.out.println("Equals!!!!");
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
      counter.set(0);
   }

   @Override
   public String toString()
   {
      return "SequenceGenerator(tmMark=" + String.format("%1$X", tmMark) + ", counter = " + counter.get() + ")";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private long newTM()
   {
      return (System.currentTimeMillis() & MASK_TIME) << 32;
   }

   // Inner classes -------------------------------------------------

}
