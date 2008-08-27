/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.management;

import static javax.management.openmbean.SimpleType.INTEGER;
import static javax.management.openmbean.SimpleType.STRING;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import org.jboss.logging.Logger;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class DayCounterInfo
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(DayCounterInfo.class);

   public static final CompositeType TYPE;
   private static final String MESSAGE_TYPE_NAME = "DayCounterInfo";
   private static final String MESSAGE_TABULAR_TYPE_NAME = "TabularDayCounterInfo";
   private static final String[] ITEM_NAMES = new String[] { "date", "00",
         "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11",
         "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22",
         "23", "total" };
   private static final String[] ITEM_DESCRIPTIONS = new String[] { "Date",
         "Messages received during the 1st hour",
         "Messages received during the 2nd hour",
         "Messages received during the 3rd hour",
         "Messages received during the 4th hour",
         "Messages received during the 5th hour",
         "Messages received during the 6th hour",
         "Messages received during the 7th hour",
         "Messages received during the 8th hour",
         "Messages received during the 9th hour",
         "Messages received during the 10th hour",
         "Messages received during the 11th hour",
         "Messages received during the 12th hour",
         "Messages received during the 13th hour",
         "Messages received during the 14th hour",
         "Messages received during the 15th hour",
         "Messages received during the 16th hour",
         "Messages received during the 17th hour",
         "Messages received during the 18th hour",
         "Messages received during the 19th hour",
         "Messages received during the 20th hour",
         "Messages received during the 21th hour",
         "Messages received during the 22th hour",
         "Messages received during the 23th hour",
         "Messages received during the 24th hour",
         "Total of messages for the day" };
   private static final OpenType[] TYPES;
   private static final TabularType TABULAR_TYPE;

   static
   {
      try
      {
         TYPES = new OpenType[] { STRING, INTEGER, INTEGER, INTEGER, INTEGER,
               INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER,
               INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER,
               INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER };
         TYPE = new CompositeType(MESSAGE_TYPE_NAME,
               "Information for a DayCounterInfo", ITEM_NAMES,
               ITEM_DESCRIPTIONS, TYPES);
         TABULAR_TYPE = new TabularType(MESSAGE_TABULAR_TYPE_NAME,
               "Information for Tabular DayCounterInfo", TYPE,
               new String[] { "date" });
      } catch (OpenDataException e)
      {
         log.error("Unable to create open types for a DayCounter", e);
         throw new IllegalStateException(e);
      }
   }

   // Attributes ----------------------------------------------------

   private final String date;
   private final int[] counters;

   // Static --------------------------------------------------------

   public static TabularData toTabularData(final DayCounterInfo[] infos)
         throws OpenDataException
   {
      TabularData data = new TabularDataSupport(TABULAR_TYPE);
      for (DayCounterInfo info : infos)
      {
         data.put(info.toCompositeData());
      }
      return data;
   }

   // Constructors --------------------------------------------------

   public DayCounterInfo(final String date, final int[] counters)
   {
      this.date = date;
      this.counters = counters;
   }

   // Public --------------------------------------------------------

   public CompositeData toCompositeData()
   {
      try
      {
         // 1 for the date, 24 for the hours, 1 for the total
         Object[] objects = new Object[1 + 24 + 1];
         objects[0] = date;
         int total = 0;
         for (int i = 0; i < counters.length; i++)
         {
            int value = counters[i];
            if (value == -1)
            {
               objects[1 + i] = 0;
            } else
            {
               objects[1 + i] = value;
               total += value;
            }
         }
         objects[objects.length - 1] = total;
         return new CompositeDataSupport(TYPE, ITEM_NAMES, objects);
      } catch (OpenDataException e)
      {
         log.error("Unable to create a CompositeData from a DayCounter", e);
         return null;
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
