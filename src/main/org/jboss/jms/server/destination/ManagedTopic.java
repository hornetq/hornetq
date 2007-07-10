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
package org.jboss.jms.server.destination;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.jboss.jms.server.JMSCondition;
import org.jboss.jms.server.messagecounter.MessageCounter;
import org.jboss.jms.server.selector.Selector;
import org.jboss.messaging.core.contract.Binding;
import org.jboss.messaging.core.contract.Message;
import org.jboss.messaging.core.contract.Queue;
import org.jboss.messaging.util.MessageQueueNameHelper;

/**
 * A ManagedTopic
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:alex.fu@novell.com">Alex Fu</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class ManagedTopic extends ManagedDestination
{  
   public ManagedTopic()
   {      
   }
   
   public ManagedTopic(String name, int fullSize, int pageSize, int downCacheSize, boolean clustered)
   {
      super(name, fullSize, pageSize, downCacheSize, clustered);           
   }

   public void removeAllMessages() throws Throwable
   {
      Collection queues = serverPeer.getPostOfficeInstance().getQueuesForCondition(new JMSCondition(false, name), true);
   	
      //XXX How to lock down all subscriptions?
      Iterator iter = queues.iterator();
      while (iter.hasNext())
      {
         Queue queue = (Queue)iter.next();
         queue.removeAllReferences();
      }
   }
   
   public int getAllMessageCount() throws Exception
   {
      return getMessageCount(ALL);
   }     
   
   public int getDurableMessageCount() throws Exception
   {
      return getMessageCount(DURABLE);
   }
   
   public int getNonDurableMessageCount() throws Exception
   {
      return getMessageCount(NON_DURABLE);
   }
   
   public int getAllSubscriptionsCount() throws Exception
   {
      Collection queues = serverPeer.getPostOfficeInstance().getQueuesForCondition(new JMSCondition(false, name), true);
   	
      return queues.size();         
   }
      
   public int getDurableSubscriptionsCount() throws Exception
   {
      return getSubscriptionsCount(true);
   }
   
   public int getNonDurableSubscriptionsCount() throws Exception
   {
      return getSubscriptionsCount(false);
   }
   
   public List listAllSubscriptions() throws Exception
   {
      return listSubscriptions(ALL);
   }
   
   public List listDurableSubscriptions() throws Exception
   {
      return listSubscriptions(DURABLE);
   }
   
   public List listNonDurableSubscriptions() throws Exception
   {
      return listSubscriptions(NON_DURABLE);
   }
   

   public String listAllSubscriptionsAsHTML() throws Exception
   {
      return listSubscriptionsAsHTML(ALL);
   }
   
   public String listDurableSubscriptionsAsHTML() throws Exception
   {
      return listSubscriptionsAsHTML(DURABLE);
   }
   
   public String listNonDurableSubscriptionsAsHTML() throws Exception
   {
      return listSubscriptionsAsHTML(NON_DURABLE);
   }
   
   
   public List listAllMessages(String subId, String selector) throws Exception
   {
      return listMessages(ALL, subId, selector);
   }
   
   public List listDurableMessages(String subId, String selector) throws Exception
   {
      return listMessages(DURABLE, subId, selector);
   }
   
   public List listNonDurableMessages(String subId, String selector) throws Exception
   {
      return listMessages(NON_DURABLE, subId, selector);
   }
   
   public List getMessageCounters() throws Exception
   {
      List counters = new ArrayList();
      
      // We deploy any queues corresponding to pre-existing durable subscriptions
      Collection queues = serverPeer.getPostOfficeInstance().getQueuesForCondition(new JMSCondition(false, name), true);
   	
      Iterator iter = queues.iterator();
      
      while (iter.hasNext())
      {
         Queue queue = (Queue)iter.next();
         
         String counterName = TopicService.SUBSCRIPTION_MESSAGECOUNTER_PREFIX + queue.getName();
         
         MessageCounter counter = serverPeer.getMessageCounterManager().getMessageCounter(counterName);
         
         if (counter == null)
         {
            throw new IllegalStateException("Cannot find counter with name " + counterName);
         }
         
         counters.add(counter);
      }
      
      return counters; 
   }
   
   public boolean isQueue()
   {
      return false;
   }
   
   public void setMessageCounterHistoryDayLimit(int limit) throws Exception
   {
      super.setMessageCounterHistoryDayLimit(limit);
      
      List counters = getMessageCounters();
      
      Iterator iter = counters.iterator();
      
      while (iter.hasNext())
      {
         MessageCounter counter = (MessageCounter)iter.next();
         
         counter.setHistoryLimit(limit);
      }
   }
   
   // Private -------------------------------------------------------------
   
   private List listMessages(int type, String subId, String selector) throws Exception
   { 
      List msgs = new ArrayList();
      
      if (subId == null || "".equals(subId.trim()))
      {
         return msgs;
      }
      
      Binding binding = serverPeer.getPostOfficeInstance().getBindingForQueueName(subId);
      
      if (binding == null || !binding.queue.isActive())
      {
         throw new IllegalArgumentException("Cannot find subscription with id " + subId);
      }
      
      Selector sel = null;
      
      if (selector != null && "".equals(selector.trim()))
      {
         selector = null;
      }
         
      if (selector != null)
      {  
         sel = new Selector(selector);
      }
      
      List allMsgs = binding.queue.browse(sel);
      
      Iterator iter = allMsgs.iterator();
      
      while (iter.hasNext())
      {
         Message msg = (Message)iter.next();
         
         if (type == ALL || (type == DURABLE && msg.isReliable()) || (type == NON_DURABLE && !msg.isReliable()))
         {
            msgs.add(msg);
         }
      }
      
      return msgs;
   }
   
   private List listSubscriptions(int type) throws Exception
   {      
      List subs = new ArrayList();
   
      Collection queues = serverPeer.getPostOfficeInstance().getQueuesForCondition(new JMSCondition(false, name), true);
   	
      Iterator iter = queues.iterator();
      
      while (iter.hasNext())
      {
         Queue queue = (Queue)iter.next();
         
         if (type == ALL || (type == DURABLE && queue.isRecoverable()) || (type == NON_DURABLE && !queue.isRecoverable()))
         {         
            String subName = null;
            String clientID = null;
            
            if (queue.isRecoverable())
            {
               MessageQueueNameHelper helper = MessageQueueNameHelper.createHelper(queue.getName());
               subName = helper.getSubName();
               clientID = helper.getClientId();
            }
            
            SubscriptionInfo info = new SubscriptionInfo(queue.getName(), queue.isRecoverable(), subName, clientID,
                     queue.getFilter() == null ? null : queue.getFilter().getFilterString(), queue.getMessageCount(), queue.getMaxSize());
            
            subs.add(info);
         }
      }
      
      return subs;
   }
   
   private int getMessageCount(int type) throws Exception
   {
      Collection queues = serverPeer.getPostOfficeInstance().getQueuesForCondition(new JMSCondition(false, name), true);
   	
      Iterator iter = queues.iterator();
      
      int count = 0;
      
      while (iter.hasNext())
      {
         Queue queue = (Queue)iter.next();
         
         if (type == ALL || (type == DURABLE && queue.isRecoverable())
             || (type == NON_DURABLE && !queue.isRecoverable()))
         {            
            count += queue.getMessageCount();
         }
      }

      return count;
   }  
   
   private int getSubscriptionsCount(boolean durable) throws Exception
   {
      Collection queues = serverPeer.getPostOfficeInstance().getQueuesForCondition(new JMSCondition(false, name), true);
   	
      Iterator iter = queues.iterator();
      
      int count = 0;
      
      while (iter.hasNext())
      {
         Queue queue = (Queue)iter.next();
         
         if ((queue.isRecoverable() && durable) || (!queue.isRecoverable() && !durable))
         {
            count++;
         }
      }

      return count;
   }
   
   
   private String listSubscriptionsAsHTML(int type) throws Exception
   {
      Collection queues = serverPeer.getPostOfficeInstance().getQueuesForCondition(new JMSCondition(false, name), true);
   	  
      StringBuffer sb = new StringBuffer();
      
      sb.append("<table width=\"100%\" border=\"1\" cellpadding=\"1\" cellspacing=\"1\">"  +
                  "<tr>"                  +
                  "<th>Id</th>"         +
                  "<th>Durable</th>" +
                  "<th>Subscription Name</th>"      +
                  "<th>Client ID</th>"        +
                  "<th>Selector</th>"   +
                  "<th>Message Count</th>"        +
                  "<th>Max Size</th>"   +
                  "</tr>");
      
      Iterator iter = queues.iterator();
      while (iter.hasNext())
      {
         Queue queue = (Queue)iter.next();

         if (type == ALL || (type == DURABLE && queue.isRecoverable())
                  || (type == NON_DURABLE && !queue.isRecoverable()))
         {
            
            String filterString = queue.getFilter() != null ? queue.getFilter().getFilterString() : null;
                     
            String subName = null;
            String clientID = null;
            
            if (queue.isRecoverable())
            {
               MessageQueueNameHelper helper = MessageQueueNameHelper.createHelper(queue.getName());
               subName = helper.getSubName();
               clientID = helper.getClientId();
            }
            
            sb.append("<tr><td>").append(queue.getName()).append("</td>");
            sb.append("<td>").append(queue.isRecoverable() ? "Durable" : "Non Durable").append("</td>");
            sb.append("<td>").append(subName != null ? subName : "").append("</td>");
            sb.append("<td>").append(clientID != null ? clientID : "").append("</td>");
            sb.append("<td>").append(filterString != null ? filterString : "").append("</td>");
            sb.append("<td>").append(queue.getMessageCount()).append("</td>");
            sb.append("<td>").append(queue.getMaxSize()).append("</td>");
            sb.append("</tr>");
         }
      }
      sb.append("</table>");
      
      return sb.toString();                                
   }
      
}
