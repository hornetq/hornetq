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

package org.jboss.messaging.core.messagecounter.impl;

import java.text.DateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.StringTokenizer;

import javax.management.openmbean.TabularData;

import org.jboss.messaging.core.management.DayCounterInfo;
import org.jboss.messaging.core.messagecounter.MessageCounter;
import org.jboss.messaging.core.messagecounter.MessageCounter.DayCounter;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class MessageCounterHelper
{
   // Constants -----------------------------------------------------

   private static DateFormat DATE_FORMAT = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.MEDIUM);

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static String listMessageCounterHistory(final MessageCounter counter) throws Exception
   {
      List<DayCounter> history = counter.getHistory();
      DayCounterInfo[] infos = new DayCounterInfo[history.size()];
      for (int i = 0; i < infos.length; i++)
      {
         DayCounter dayCounter = history.get(i);
         int[] counters = dayCounter.getCounters();
         GregorianCalendar date = dayCounter.getDate();

         DateFormat dateFormat = DateFormat.getDateInstance(DateFormat.SHORT);
         String strData = dateFormat.format(date.getTime());
         infos[i] = new DayCounterInfo(strData, counters);
      }
      return DayCounterInfo.toJSON(infos);
   }
   
   public static String listMessageCounterAsHTML(MessageCounter[] counters)
   {
      if (counters == null)
         return null;

      String ret = "<table class=\"jbm-message-counter\">\n" 
                   + "<tr>"
                   + "<th>Type</th>"
                   + "<th>Name</th>"
                   + "<th>Subscription</th>"
                   + "<th>Durable</th>"
                   + "<th>Count</th>"
                   + "<th>CountDelta</th>"
                   + "<th>Depth</th>"
                   + "<th>DepthDelta</th>"
                   + "<th>Last Add</th>"
                   + "<th>Last Update</th>"
                   + "</tr>\n";

      for (int i = 0; i < counters.length; i++)
      {
         MessageCounter counter = counters[i];
         String type = counter.isDestinationTopic() ? "Topic" : "Queue";
         String subscription = counter.getDestinationSubscription();
         if (subscription == null)
         {
            subscription = "-";
         }
         String durableStr = "-"; // makes no sense for a queue
         if (counter.isDestinationTopic())
         {
            durableStr = Boolean.toString(counter.isDestinationDurable());            
         }
         ret += "<tr bgcolor=\"#" + ((i % 2) == 0 ? "FFFFFF" : "F0F0F0") + "\">";

         ret += "<td>" + type + "</td>";
         ret += "<td>" + counter.getDestinationName() + "</td>";
         ret += "<td>" + subscription + "</td>";
         ret += "<td>" + durableStr + "</td>";
         ret += "<td>" + counter.getCount() + "</td>";
         ret += "<td>" + prettify(counter.getCountDelta()) + "</td>";
         ret += "<td>" + prettify(counter.getMessageCount()) + "</td>";
         ret += "<td>" + prettify(counter.getMessageCountDelta()) + "</td>";
         ret += "<td>" + asDate(counter.getLastAddedMessageTime()) + "</td>";
         ret += "<td>" + asDate(counter.getLastUpdate()) + "</td>";
         
         ret += "</tr>\n";
      }

      ret += "</table>\n";

      return ret;
   }

   public static String listMessageCounterHistoryAsHTML(MessageCounter[] counters)
   {
      if (counters == null)
         return null;

      String ret = "<ul>\n";

      for (int i = 0; i < counters.length; i++)
      {
         ret += "<li>\n";
         ret += "  <ul>\n";

         ret += "    <li>";
         // destination name
         ret += (counters[i].isDestinationTopic() ? "Topic '" : "Queue '") + counters[i].getDestinationName() + "'";
         ret += "</li>\n";

         if (counters[i].getDestinationSubscription() != null)
         {
            ret += "    <li>";
            ret += "Subscription '" + counters[i].getDestinationSubscription() + "'";
            ret += "</li>\n";
         }

         ret += "    <li>";
         // table header
         ret += "<table class=\"jbm-message-counter-history\">\n";
         ret += "<tr><th>Date</th>";

         for (int j = 0; j < 24; j++)
            ret += "<th>" + j + "</th>";

         ret += "<th>Total</th></tr>\n";

         // get history data as CSV string
         StringTokenizer tokens = new StringTokenizer(counters[i].getHistoryAsString(), ",\n");

         // get history day count
         int days = Integer.parseInt(tokens.nextToken());

         for (int j = 0; j < days; j++)
         {
            // next day counter row
            ret += "<tr bgcolor=\"#" + ((j % 2) == 0 ? "FFFFFF" : "F0F0F0") + "\">";

            // date
            ret += "<td>" + tokens.nextToken() + "</td>";

            // 24 hour counters
            int total = 0;

            for (int k = 0; k < 24; k++)
            {
               int value = Integer.parseInt(tokens.nextToken().trim());

               if (value == -1)
               {
                  ret += "<td></td>";
               }
               else
               {
                  ret += "<td>" + value + "</td>";

                  total += value;
               }
            }

            ret += "<td>" + total + "</td></tr>\n";
         }

         ret += "</table></li>\n";
         ret += "  </ul>\n";
         ret += "</li>\n";
      }

      ret += "</ul>\n";

      return ret;
   }
   
   private static String prettify(long value)
   {
      if (value == 0)
      {
         return "-";
      }
      return Long.toString(value);
   }
   
   private static String asDate(long time)
   {
      if (time > 0)
      {
         return DATE_FORMAT.format(new Date(time));
      }
      else
      {
         return "-";
      }
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
