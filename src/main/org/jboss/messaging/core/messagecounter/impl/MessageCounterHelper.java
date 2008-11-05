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

import java.util.StringTokenizer;

import org.jboss.messaging.core.messagecounter.MessageCounter;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class MessageCounterHelper
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

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
                   + "</tr>\n";

      for (int i = 0; i < counters.length; i++)
      {
         String data = counters[i].getCounterAsString();
         StringTokenizer token = new StringTokenizer(data, ",");
         String value;

         ret += "<tr bgcolor=\"#" + ((i % 2) == 0 ? "FFFFFF" : "F0F0F0") + "\">";

         ret += "<td>" + token.nextToken() + "</td>"; // type
         ret += "<td>" + token.nextToken() + "</td>"; // name
         ret += "<td>" + token.nextToken() + "</td>"; // subscription
         ret += "<td>" + token.nextToken() + "</td>"; // durable

         ret += "<td>" + token.nextToken() + "</td>"; // count

         value = token.nextToken(); // countDelta

         if (value.equalsIgnoreCase("0"))
            value = "-";

         ret += "<td>" + value + "</td>";

         ret += "<td>" + token.nextToken() + "</td>"; // depth

         value = token.nextToken(); // depthDelta

         if (value.equalsIgnoreCase("0"))
            value = "-";

         ret += "<td>" + value + "</td>";

         ret += "<td>" + token.nextToken() + "</td>"; // date last add

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
         ret += (counters[i].getDestinationTopic() ? "Topic '" : "Queue '") + counters[i].getDestinationName() + "'";
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

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
