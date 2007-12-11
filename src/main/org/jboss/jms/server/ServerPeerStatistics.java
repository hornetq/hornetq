/*
   * JBoss, Home of Professional Open Source
   * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.jms.server;

import java.io.CharArrayWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import javax.transaction.xa.Xid;

import org.jboss.aop.microcontainer.aspects.jmx.JMX;
import org.jboss.jms.server.destination.ManagedQueue;
import org.jboss.jms.server.destination.ManagedTopic;
import org.jboss.jms.server.endpoint.ServerConnectionEndpoint;
import org.jboss.jms.server.messagecounter.MessageCounter;
import org.jboss.jms.server.messagecounter.MessageStatistics;
import org.jboss.util.JBossStringBuilder;

/**
 * exposed via jmx to show a cleaner sepration. most of these methods delegate to the serverpeer
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
@JMX(name = "jboss.messaging:service=ServerPeerStatistics", exposedInterface = JmsServerStatistics.class)
public class ServerPeerStatistics implements JmsServerStatistics
{
   private ServerPeer serverPeer;


   public void setServerPeer(ServerPeer serverPeer)
   {
      this.serverPeer = serverPeer;
   }

   public int getMessageCountForQueue(String queue) throws Exception
   {
      return getQueue(queue).getMessageCount();
   }

   public int getDeliveringCountForQueue(String queue) throws Exception
   {
      return getQueue(queue).getDeliveringCount();
   }

   public int getScheduledMessageCountForQueue(String queue) throws Exception
   {
      return getQueue(queue).getQueue().getScheduledCount();
   }

   public MessageCounter getMessageCounterForQueue(String queue) throws Exception
   {
      return getQueue(queue).getMessageCounter();
   }

   public MessageStatistics getMessageStatisticsForQueue(String queue) throws Exception
   {
      List counters = new ArrayList();
      counters.add(getQueue(queue).getMessageCounter());

      List stats = MessageCounter.getMessageStatistics(counters);

      return (MessageStatistics)stats.get(0);
   }

   public int getConsumerCountForQueue(String queue) throws Exception
   {
      return getQueue(queue).getConsumersCount();
   }

   public void resetMessageCounterForQueue(String queue) throws Exception
   {
      getMessageCounterForQueue(queue).resetCounter();
   }

   public void resetMessageCounterHistoryForQueue(String queue) throws Exception
   {
      getMessageCounterForQueue(queue).resetHistory();
   }

   public List listAllMessagesForQueue(String queue) throws Exception
   {
      return getQueue(queue).listAllMessages(null);
   }

   public List listAllMessagesForQueue(String queue, String selector) throws Exception
   {
      return getQueue(queue).listAllMessages(selector);
   }

   public List listDurableMessagesForQueue(String queue) throws Exception
   {
      return getQueue(queue).listDurableMessages(null);
   }

   public List listDurableMessagesForQueue(String queue, String selector) throws Exception
   {
      return getQueue(queue).listDurableMessages(selector);
   }

   public List listNonDurableMessagesForQueue(String queue) throws Exception
   {
      return getQueue(queue).listNonDurableMessages(null);
   }

   public List listNonDurableMessagesForQueue(String queue, String selector) throws Exception
   {
      return getQueue(queue).listNonDurableMessages(selector);
   }

   public String listMessageCounterAsHTMLForQueue(String queue) throws Exception
   {
      return listMessageCounterAsHTML(new MessageCounter[] { getMessageCounterForQueue(queue) });
   }

   public String listMessageCounterHistoryAsHTMLForQueue(String queue) throws Exception
   {
      return listMessageCounterHistoryAsHTML(new MessageCounter[] {getMessageCounterForQueue(queue)});
   }

   public int getAllMessageCountForTopic(String topicName) throws Exception
   {
      return getTopic(topicName).getAllMessageCount();
   }

   public int getDurableMessageCountForTopic(String topicName) throws Exception
   {
      return getTopic(topicName).getDurableMessageCount();
   }

   public int getNonDurableMessageCountForTopic(String topicName) throws Exception
   {
      return getTopic(topicName).getNonDurableMessageCount();
   }

   public int getAllSubscriptionsCountForTopic(String topicName) throws Exception
   {
      return getTopic(topicName).getAllSubscriptionsCount();
   }

   public int getDurableSubscriptionsCountForTopic(String topicName) throws Exception
   {
      return getTopic(topicName).getDurableSubscriptionsCount();
   }

   public int getNonDurableSubscriptionsCountForTopic(String topicName) throws Exception
   {
      return getTopic(topicName).getNonDurableSubscriptionsCount();
   }

   public void removeAllMessagesForTopic(String topicName) throws Throwable
   {
      getTopic(topicName).removeAllMessages();
   }

   public List listAllSubscriptionsForTopic(String topicName) throws Exception
   {
      return getTopic(topicName).listAllSubscriptions();
   }

   public List listDurableSubscriptionsForTopic(String topicName) throws Exception
   {
      return getTopic(topicName).listDurableSubscriptions();
   }

   public List listNonDurableSubscriptionsForTopic(String topicName) throws Exception
   {
      return getTopic(topicName).listNonDurableSubscriptions();
   }

   public String listAllSubscriptionsAsHTMLForTopic(String topicName) throws Exception
   {
      return getTopic(topicName).listAllSubscriptionsAsHTML();
   }

   public String listDurableSubscriptionsAsHTMLForTopic(String topicName) throws Exception
   {
      return getTopic(topicName).listDurableSubscriptionsAsHTML();
   }

   public String listNonDurableSubscriptionsAsHTMLForTopic(String topicName) throws Exception
   {
      return getTopic(topicName).listNonDurableSubscriptionsAsHTML();
   }

   public List listAllMessagesForTopic(String topicName, String subscriptionId) throws Exception
   {
      return getTopic(topicName).listAllMessages(subscriptionId, null);
   }

   public List listAllMessagesForTopic(String topicName, String subscriptionId, String selector) throws Exception
   {
      return getTopic(topicName).listAllMessages(subscriptionId, selector);
   }

   public List listDurableMessagesForTopic(String topicName, String subscriptionId) throws Exception
   {
      return getTopic(topicName).listDurableMessages(subscriptionId, null);
   }

   public List listDurableMessagesForTopic(String topicName, String subscriptionId, String selector) throws Exception
   {
      return getTopic(topicName).listDurableMessages(subscriptionId, selector);
   }

   public List listNonDurableMessagesForTopic(String topicName, String subscriptionId) throws Exception
   {
      return getTopic(topicName).listNonDurableMessages(subscriptionId, null);
   }

   public List listNonDurableMessagesForTopic(String topicName, String subscriptionId, String selector) throws Exception
   {
      return getTopic(topicName).listNonDurableMessages(subscriptionId, selector);
   }

   public List getMessageCountersForTopic(String topicName) throws Exception
   {
      return getTopic(topicName).getMessageCounters();
   }

   public String showActiveClientsAsHTML() throws Exception
   {
      CharArrayWriter charArray = new CharArrayWriter();
      PrintWriter out = new PrintWriter(charArray);

      List endpoints = serverPeer.getConnectionManager().getActiveConnections();

      out.println("<table><tr><td>ID</td><td>Host</td><td>User</td><td>#Sessions</td></tr>");
      for (Iterator iter = endpoints.iterator(); iter.hasNext();)
      {
         ServerConnectionEndpoint endpoint = (ServerConnectionEndpoint) iter.next();

         out.println("<tr>");
         out.println("<td>" + endpoint.toString() + "</td>");
         // FIXME display URI of client
         out.println("<td>" + endpoint.getUsername() + "</td>");
         out.println("<td>" + endpoint.getSessions().size() + "</td>");
         out.println("</tr>");
      }

      out.println("</table>");


      return charArray.toString();
   }
   
   public String showPreparedTransactionsAsHTML()
   {
      List txs = serverPeer.getTxRepository().getPreparedTransactions();
      JBossStringBuilder buffer = new JBossStringBuilder();
      buffer.append("<table width=\"100%\" border=\"1\" cellpadding=\"1\" cellspacing=\"1\">");
      buffer.append("<tr><th>Xid</th></tr>");
      for (Iterator i = txs.iterator(); i.hasNext();)
      {
         Xid xid = (Xid) i.next();
         if (xid != null)
         {
            buffer.append("<tr><td>");
            buffer.append(xid);
            buffer.append("</td></tr>");
         }
      }
      buffer.append("</table>");
      return buffer.toString();
   }

   public String listMessageCountersAsHTML() throws Exception
   {
      List counters = serverPeer.getMessageCounters();

      Collections.sort(counters, new Comparator()
      {
         public int compare(Object o1, Object o2)
         {
            MessageCounter m1 = (MessageCounter) o1;
            MessageCounter m2 = (MessageCounter) o2;
            return m1.getDestinationName().compareTo(m2.getDestinationName());
         }
      });

      String ret =
              "<table width=\"100%\" border=\"1\" cellpadding=\"1\" cellspacing=\"1\">"
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
                      + "</tr>";

      String strNameLast = null;
      String strTypeLast = null;
      String strDestLast = null;

      String destData = "";
      int destCount = 0;

      int countTotal = 0;
      int countDeltaTotal = 0;
      int depthTotal = 0;
      int depthDeltaTotal = 0;

      int i = 0; // define outside of for statement, so variable
      // still exists after for loop, because it is
      // needed during output of last module data string

      Iterator iter = counters.iterator();

      while (iter.hasNext())
      {
         MessageCounter counter = (MessageCounter) iter.next();

         // get counter data
         StringTokenizer tokens = new StringTokenizer(counter.getCounterAsString(), ",");

         String strType = tokens.nextToken();
         String strName = tokens.nextToken();
         String strSub = tokens.nextToken();
         String strDurable = tokens.nextToken();

         String strDest = strType + "-" + strName;

         String strCount = tokens.nextToken();
         String strCountDelta = tokens.nextToken();
         String strDepth = tokens.nextToken();
         String strDepthDelta = tokens.nextToken();
         String strDate = tokens.nextToken();

         // update total count / depth values
         countTotal += Integer.parseInt(strCount);
         depthTotal += Integer.parseInt(strDepth);

         countDeltaTotal += Integer.parseInt(strCountDelta);
         depthDeltaTotal += Integer.parseInt(strDepthDelta);

         if (strCountDelta.equalsIgnoreCase("0"))
            strCountDelta = "-"; // looks better

         if (strDepthDelta.equalsIgnoreCase("0"))
            strDepthDelta = "-"; // looks better

         // output destination counter data as HTML table row
         // ( for topics with multiple subscriptions output
         //   type + name field as rowspans, looks better )
         if (strDestLast != null && strDestLast.equals(strDest))
         {
            // still same destination -> append destination subscription data
            destData += "<tr bgcolor=\"#" + ((i % 2) == 0 ? "FFFFFF" : "F0F0F0") + "\">";
            destCount += 1;
         }
         else
         {
            // startnew destination data
            if (strDestLast != null)
            {
               // store last destination data string
               ret += "<tr bgcolor=\"#"
                       + ((i % 2) == 0 ? "FFFFFF" : "F0F0F0")
                       + "\"><td rowspan=\""
                       + destCount
                       + "\">"
                       + strTypeLast
                       + "</td><td rowspan=\""
                       + destCount
                       + "\">"
                       + strNameLast
                       + "</td>"
                       + destData;

               destData = "";
            }

            destCount = 1;
         }

         // counter data row
         destData += "<td>"
                 + strSub
                 + "</td>"
                 + "<td>"
                 + strDurable
                 + "</td>"
                 + "<td>"
                 + strCount
                 + "</td>"
                 + "<td>"
                 + strCountDelta
                 + "</td>"
                 + "<td>"
                 + strDepth
                 + "</td>"
                 + "<td>"
                 + strDepthDelta
                 + "</td>"
                 + "<td>"
                 + strDate
                 + "</td>";

         // store current destination data for change detection
         strTypeLast = strType;
         strNameLast = strName;
         strDestLast = strDest;
      }

      if (strDestLast != null)
      {
         // store last module data string
         ret += "<tr bgcolor=\"#"
                 + ((i % 2) == 0 ? "FFFFFF" : "F0F0F0")
                 + "\"><td rowspan=\""
                 + destCount
                 + "\">"
                 + strTypeLast
                 + "</td><td rowspan=\""
                 + destCount
                 + "\">"
                 + strNameLast
                 + "</td>"
                 + destData;
      }

      // append summation info
      ret += "<tr>"
              + "<td><![CDATA[ ]]></td><td><![CDATA[ ]]></td>"
              + "<td><![CDATA[ ]]></td><td><![CDATA[ ]]></td><td>"
              + countTotal
              + "</td><td>"
              + (countDeltaTotal == 0 ? "-" : Integer.toString(countDeltaTotal))
              + "</td><td>"
              + depthTotal
              + "</td><td>"
              + (depthDeltaTotal == 0 ? "-" : Integer.toString(depthDeltaTotal))
              + "</td><td>Total</td></tr></table>";

      return ret;
   }
   private ManagedQueue getQueue(String queue) throws Exception
   {
      return (ManagedQueue) serverPeer.getDestinationManager().getDestination(queue, true);
   }

   private ManagedTopic getTopic(String queue) throws Exception
   {
      return (ManagedTopic) serverPeer.getDestinationManager().getDestination(queue, false);
   }

   /**
    * List message counters as HTML table
    *
    * @return String
    */
   protected String listMessageCounterAsHTML(MessageCounter[] counters)
   {
      if (counters == null)
         return null;

      String ret = "<table width=\"100%\" border=\"1\" cellpadding=\"1\" cellspacing=\"1\">"  +
                   "<tr>"                  +
                   "<th>Type</th>"         +
                   "<th>Name</th>"         +
                   "<th>Subscription</th>" +
                   "<th>Durable</th>"      +
                   "<th>Count</th>"        +
                   "<th>CountDelta</th>"   +
                   "<th>Depth</th>"        +
                   "<th>DepthDelta</th>"   +
                   "<th>Last Add</th>"     +
                   "</tr>";

      for( int i=0; i<counters.length; i++ )
      {
         String            data = counters[i].getCounterAsString();
         StringTokenizer token = new StringTokenizer( data, ",");
         String            value;

         ret += "<tr bgcolor=\"#" + ( (i%2)==0 ? "FFFFFF" : "F0F0F0") + "\">";

         ret += "<td>" + token.nextToken() + "</td>"; // type
         ret += "<td>" + token.nextToken() + "</td>"; // name
         ret += "<td>" + token.nextToken() + "</td>"; // subscription
         ret += "<td>" + token.nextToken() + "</td>"; // durable

         ret += "<td>" + token.nextToken() + "</td>"; // count

         value = token.nextToken(); // countDelta

         if( value.equalsIgnoreCase("0") )
             value = "-";

         ret += "<td>" + value + "</td>";

         ret += "<td>" + token.nextToken() + "</td>"; // depth

         value = token.nextToken(); // depthDelta

         if( value.equalsIgnoreCase("0") )
             value = "-";

         ret += "<td>" + value + "</td>";

         ret += "<td>" + token.nextToken() + "</td>"; // date last add

         ret += "</tr>";
      }

      ret += "</table>";

      return ret;
   }

   /**
    * List destination message counter history as HTML table
    *
    * @return String
    */
   protected String listMessageCounterHistoryAsHTML(MessageCounter[] counters)
   {
      if (counters == null)
         return null;

      String           ret = "";

      for( int i=0; i<counters.length; i++ )
      {
         // destination name
         ret += ( counters[i].getDestinationTopic() ? "Topic '" : "Queue '" );
         ret += counters[i].getDestinationName() + "'";

         if( counters[i].getDestinationSubscription() != null )
            ret += "Subscription '" + counters[i].getDestinationSubscription() + "'";


         // table header
         ret += "<table width=\"100%\" border=\"1\" cellpadding=\"1\" cellspacing=\"1\">"  +
                "<tr>"                  +
                "<th>Date</th>";

         for( int j = 0; j < 24; j++ )
            ret += "<th width=\"4%\">" + j + "</th>";

         ret += "<th>Total</th></tr>";

         // get history data as CSV string
         StringTokenizer tokens = new StringTokenizer( counters[i].getHistoryAsString(), ",\n");

         // get history day count
         int days = Integer.parseInt( tokens.nextToken() );

         for( int j=0; j<days; j++ )
         {
            // next day counter row
            ret += "<tr bgcolor=\"#" + ((j%2)==0 ? "FFFFFF" : "F0F0F0") + "\">";

            // date
            ret += "<td>" + tokens.nextToken() + "</td>";

            // 24 hour counters
            int total = 0;

            for( int k=0; k<24; k++ )
            {
               int value = Integer.parseInt( tokens.nextToken().trim() );

               if( value == -1 )
               {
                    ret += "<td></td>";
               }
               else
               {
                    ret += "<td>" + value + "</td>";

                    total += value;
               }
            }

            ret += "<td>" + total + "</td></tr>";
         }

         ret += "</table><br><br>";
      }

      return ret;
   }
}
