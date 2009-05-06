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

import java.util.Arrays;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.utils.json.JSONArray;
import org.jboss.messaging.utils.json.JSONException;
import org.jboss.messaging.utils.json.JSONObject;


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

   // Attributes ----------------------------------------------------

   private final String date;
   private final int[] counters;

   // Static --------------------------------------------------------

   public static String toJSON(final DayCounterInfo[] infos) throws JSONException
   {
      JSONObject json = new JSONObject();
      JSONArray counters = new JSONArray();
      for (DayCounterInfo info : infos)
      {
         JSONObject counter = new JSONObject();
         counter.put("date", info.getDate());
         counter.put("counters", Arrays.asList(info.getCounters()));
         counters.put(counter);
      }
      json.put("dayCounters", counters);
      return json.toString();
   }
   
   public static DayCounterInfo[] fromJSON(final String jsonString) throws JSONException
   {
      
      JSONObject json = new JSONObject(jsonString);
      JSONArray dayCounters = json.getJSONArray("dayCounters");
      DayCounterInfo[] infos = new DayCounterInfo[dayCounters.length()];
      for (int i = 0; i < dayCounters.length(); i++)
      {
         
         JSONObject counter = (JSONObject)dayCounters.get(i);
         JSONArray hour = (JSONArray)counter.getJSONArray("counters").get(0);
         int[] hourCounters = new int[24];
         for (int j = 0; j < 24; j++)
         {
            hourCounters[j] = hour.getInt(j);
         }
         DayCounterInfo info = new DayCounterInfo(counter.getString("date"), hourCounters);
         infos[i] = info;
      }
      return infos;
   }
   
   // Constructors --------------------------------------------------

   public DayCounterInfo(final String date, final int[] counters)
   {
      this.date = date;
      this.counters = counters;
   }

   // Public --------------------------------------------------------

   public String getDate()
   {
      return date;
   }
   
   public int[] getCounters()
   {
      return counters;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
