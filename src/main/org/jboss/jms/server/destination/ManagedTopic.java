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
import java.util.Iterator;
import java.util.List;

import javax.jms.InvalidSelectorException;

import org.jboss.jms.selector.Selector;
import org.jboss.jms.util.MessageQueueNameHelper;
import org.jboss.messaging.core.local.Queue;
import org.jboss.messaging.core.plugin.postoffice.Binding;

/**
 * A ManagedTopic
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:alex.fu@novell.com">Alex Fu</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class ManagedTopic extends ManagedDestination
{
   public ManagedTopic()
   {      
   }
   
   public ManagedTopic(String name, int fullSize, int pageSize, int downCacheSize)
   {
      super(name, fullSize, pageSize, downCacheSize);
   }

   public void removeAllMessages() throws Throwable
   {
      List subs = postOffice.listBindingsForCondition(name);
      
      //XXX How to lock down all subscriptions?
      Iterator iter = subs.iterator();
      while (iter.hasNext())
      {
         Binding binding = (Binding)iter.next();
         Queue queue = binding.getQueue();
         queue.removeAllReferences();
      }
   }
   
   public int subscriptionCount() throws Exception
   {
      List subs = postOffice.listBindingsForCondition(name);
      
      return subs.size();         
   }
   
   public int subscriptionCount(boolean durable) throws Exception
   {
      List subs = postOffice.listBindingsForCondition(name);
      
      Iterator iter = subs.iterator();
      
      int count = 0;
      
      while (iter.hasNext())
      {
         Binding binding = (Binding)iter.next();
         
         if ((binding.isDurable() && durable) || (!binding.isDurable() && !durable))
         {
            count++;
         }
      }

      return count;
   }
   
   public String listSubscriptionsAsText() throws Exception
   {
      List subs = postOffice.listBindingsForCondition(name);
      
      return getSubscriptionsAsText(subs, true) + getSubscriptionsAsText(subs, false);
   }
   
   public String listSubscriptionsAsText(boolean durable) throws Exception
   {
      List subs = postOffice.listBindingsForCondition(name);
      
      return getSubscriptionsAsText(subs, durable);
   }
   
   public List listMessagesDurableSub(String subName, String clientID, String selector)
      throws Exception
   {
      List subs = postOffice.listBindingsForCondition(name);
      
      return getMessagesFromDurableSub(subs, subName, clientID, trimSelector(selector));
   }
   
   public List listMessagesNonDurableSub(long channelID, String selector)
      throws Exception
   {
      List subs = postOffice.listBindingsForCondition(name);
      
      return getMessagesFromNonDurableSub(subs, channelID, trimSelector(selector));
   }
   
   public boolean isQueue()
   {
      return false;
   }
   
   // Private -------------------------------------------------------------
   
   
   
   private String getSubscriptionsAsText(List bindings, boolean durable)
   {
      StringBuffer sb = new StringBuffer();
      Iterator iter = bindings.iterator();
      while (iter.hasNext())
      {
         Binding binding = (Binding)iter.next();
         
         String filterString = binding.getFilter() != null ? binding.getFilter().getFilterString() : null;
                  
         if (durable && binding.isDurable())
         {                      
            MessageQueueNameHelper helper = MessageQueueNameHelper.createHelper(binding.getQueueName());
            
            sb.append("Durable, subscriptionID=\"");
            sb.append(binding.getChannelId());    
            sb.append("\", name=\"");
            sb.append(helper.getSubName());
            sb.append("\", clientID=\"");
            sb.append(helper.getClientId());
            sb.append("\", selector=\"");
            sb.append(filterString);
            sb.append("\"\n");
         }
         else if (!durable && !binding.isDurable())
         {            
            sb.append("Non-durable, subscriptionID=\"");
            sb.append(binding.getChannelId());
            sb.append("\", selector=\"");
            sb.append(filterString);
            sb.append("\"\n");
         }
      }
      return sb.toString();
   }
     
   // Test if the durable subscriptions match
   private boolean matchDurableSubscription(String name, String clientID, Binding binding)
   {
      // Validate the name
      if (null == name)
         throw new IllegalArgumentException();
      // Must be durable
      if (!binding.isDurable())
         return false;
      
      MessageQueueNameHelper helper = MessageQueueNameHelper.createHelper(binding.getQueueName());

      // Subscription name check
      if (!name.equals(helper.getSubName()))
         return false;
      // Client ID check: if no client ID specified, it's considered as matched 
      if (null == clientID || 0 == clientID.length())
         return true;
      if (!clientID.equals(helper.getClientId()))
         return false;
      return true;
   }
   
   // Test if the non-durable subscriptions match
   private boolean matchNonDurableSubscription(long channelID, Binding binding)
   {
      // Must be non-durable
      if (binding.isDurable())
         return false;
      // Channel ID must be the same
      if (channelID != binding.getChannelId())
         return false;
      return true;
   }
   
   private List getMessagesFromDurableSub(List bindings, String name,
            String clientID, String selector) throws InvalidSelectorException
   {
      Iterator iter = bindings.iterator();
      while (iter.hasNext())
      {
         Binding binding = (Binding)iter.next();
         // If subID matches, then get message list from the subscription
         if (matchDurableSubscription(name, clientID, binding))
         {
            Queue queue = binding.getQueue();
            return queue.browse(null == selector ? null : new Selector(selector));
         }
      }   
      // No match, return an empty list
      return new ArrayList();
   }
   
   private List getMessagesFromNonDurableSub(List bindings, long channelID, String selector) throws InvalidSelectorException
   {
      Iterator iter = bindings.iterator();
      while (iter.hasNext())
      {
         Binding binding = (Binding)iter.next();
         // If subID matches, then get message list from the subscription
         if (matchNonDurableSubscription(channelID, binding))
            return binding.getQueue().browse(null == selector ? null : new Selector(selector));
      }   
      // No match, return an empty list
      return new ArrayList();
   }
   
   private String trimSelector(String selector)
   {
      if (selector == null)
      {
         return null;
      }

      selector = selector.trim();

      if (selector.length() == 0)
      {
         return null;
      }

      return selector;
   }

}
