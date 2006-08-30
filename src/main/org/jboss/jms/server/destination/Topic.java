/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.destination;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;

import org.jboss.jms.destination.JBossTopic;
import org.jboss.jms.selector.Selector;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.util.ExceptionUtil;
import org.jboss.jms.util.MessageQueueNameHelper;
import org.jboss.jms.util.XMLUtil;
import org.jboss.messaging.core.local.MessageQueue;
import org.jboss.messaging.core.memory.MemoryManager;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.plugin.exchange.Binding;


/**
 * A deployable JBoss Messaging topic.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:alex.fu@novell.com">Alex Fu</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class Topic extends DestinationServiceSupport
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public Topic()
   {
      super(false);
   }

   public Topic(boolean createProgrammatically)
   {
      super(createProgrammatically);
   }

   // JMX managed attributes ----------------------------------------

   // JMX managed operations ----------------------------------------
   
   public synchronized void startService() throws Exception
   {
      try
      {
         started = true;
   
         if (serviceName != null)
         {
            name = serviceName.getKeyProperty("name");
         }
   
         if (name == null || name.length() == 0)
         {
            throw new IllegalStateException( "The " + (isQueue() ? "queue" : "topic") + " " +
                                             "name was not properly set in the service's" +
                                             "ObjectName");
         }
   
         ServerPeer serverPeer = (ServerPeer)server.getAttribute(serverPeerObjectName, "Instance");

         dm = serverPeer.getDestinationManager();
         sm = serverPeer.getSecurityManager();
         exchange = serverPeer.getTopicExchangeDelegate();
         
         MessageStore ms = serverPeer.getMessageStore();
         PersistenceManager pm = serverPeer.getPersistenceManagerDelegate();
        
         //We deploy any queues corresponding to pre-existing durable subscriptions
         exchange.reloadQueues(name, ms, pm, fullSize, pageSize, downCacheSize);
                  
         JBossTopic t = new JBossTopic(name, fullSize, pageSize, downCacheSize);
         
         jndiName = dm.registerDestination(t, jndiName, securityConfig);
         
         log.debug(this + " security configuration: " + (securityConfig == null ?
            "null" : "\n" + XMLUtil.elementToString(securityConfig)));

         log.info(this + " started, fullSize=" + fullSize + ", pageSize=" + pageSize + ", downCacheSize=" + downCacheSize);
      }
      catch (Throwable t)
      {
         ExceptionUtil.handleJMXInvocation(t, this + " startService");
      }
   }

   public void stopService() throws Exception
   {
      try
      {
         dm.unregisterDestination(new JBossTopic(name));
         
         //When undeploying a topic, any non durable subscriptions will be removed
         //Any durable subscriptions will survive in persistent storage, but be removed
         //from memory
         
         //First we remove any data for a non durable sub - a non durable sub might have data in the
         //database since it might have paged
         
         List bindings = exchange.listBindingsForWildcard(name);
         
         Iterator iter = bindings.iterator();
         while (iter.hasNext())            
         {
            Binding binding = (Binding)iter.next();
            
            if (!binding.isDurable())
            {
               binding.getQueue().removeAllReferences();
            }
         }
          
         //We undeploy the queues for the subscriptions - this also unbinds the bindings
         exchange.unloadQueues(name);
         
         started = false;
         log.info(this + " stopped");
      }
      catch (Throwable t)
      {
         ExceptionUtil.handleJMXInvocation(t, this + " stopService");
      }
   }

   /**
    * Remove all messages from subscription's storage.
    */
   public void removeAllMessages() throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Topic is stopped.");
            return;
         }
         
         List subs = exchange.listBindingsForWildcard(name);
         
         //XXX How to lock down all subscriptions?
         Iterator iter = subs.iterator();
         while (iter.hasNext())
         {
            Binding binding = (Binding)iter.next();
            MessageQueue queue = binding.getQueue();
            queue.removeAllReferences();
         }
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " removeAllMessages");
      } 
   }
   
   /**
    * All subscription count
    * @return all subscription count
    * @throws JMSException
    */
   public int subscriptionCount() throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Topic is stopped.");
            return 0;
         }
   
         List subs = exchange.listBindingsForWildcard(name);
         
         return subs.size();         
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " subscriptionCount");
      } 
   }

   /**
    * Durable/nondurable subscription count
    * @param durable If true return durable subscription count.
    *                If false return nondurable subscription count.
    * @return either durable or nondurable subscription count depending on param.
    * @throws JMSException
    */
   public int subscriptionCount(boolean durable) throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Topic is stopped.");
            return 0;
         }
         
         List subs = exchange.listBindingsForWildcard(name);
         
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
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " subscriptionCount");
      } 
   }

   /**
    * XXX Placeholder
    * Get all subscription list.
    * @return List of CoreSubscription. Never null. 
    * @throws JMSException
    * @see ManageableTopic#getSubscriptions()
    */
   /*
   public List listSubscriptions() throws JMSException
   {
      JBossTopic jbt = new JBossTopic(name);
      ManageableTopic t = (ManageableTopic)cm.getCoreDestination(jbt);
      return t.getSubscriptions();
   }
   */

   /**
    * Returns a human readable list containing the names of current subscriptions.
    * @return String of subscription list. Never null.
    */
   public String listSubscriptionsAsText() throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Topic is stopped.");
            return "";
         }
   
         List subs = exchange.listBindingsForWildcard(name);
         
         return getSubscriptionsAsText(subs, true) + getSubscriptionsAsText(subs, false);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " listSubscriptionsAsText");
      } 
   }
   
   

   /**
    * Returns a human readable list containing the names of current subscriptions.
    * @param durable If true, return durable subscription list.
    *                If false, return non-durable subscription list.
    * @return String of subscription list. Never null.
    */
   public String listSubscriptionsAsText(boolean durable) throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Topic is stopped.");
            return "";
         }
   
         List subs = exchange.listBindingsForWildcard(name);
         
         return getSubscriptionsAsText(subs, durable);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " listSubscriptionsAsText");
      } 
   }

   /**
    * XXX Placeholder
    * Get durable/non-durable subscription list.
    * @param durable If true, return durable subscription list.
    *                If false, return non-durable subscription list.
    * @return List of CoreDurableSubscription/CoreSubscription. Never null.
    * @throws JMSException
    * @see ManageableTopic#getSubscriptions(boolean)
    */
   /*
   public List listSubscriptions(boolean durable) throws JMSException
   {
      JBossTopic jbt = new JBossTopic(name);
      ManageableTopic t = (ManageableTopic)cm.getCoreDestination(jbt);
      return t.getSubscriptions(durable);
   }
   */
   
   /**
    * XXX Placeholder
    * Get messages from certain subscription.
    * @param channelID @see #listSubscriptions()
    * @param clientID @see #listSubscriptions()
    * @param subName @see #listSubscriptions()
    * @param selector Filter expression
    * @return list of javax.jms.Message
    * @throws JMSException
    * @see ManageableTopic#getMessages(long, String, String, String)
    */
   /*
   public List listMessages(long channelID, String clientID, String subName, String selector)
      throws JMSException
   {
      JBossTopic jbt = new JBossTopic(name);
      ManageableTopic t = (ManageableTopic)cm.getCoreDestination(jbt);
      return t.getMessages(channelID, clientID, subName, trimSelector(selector));
   }
   */
   
   /**
    * Get messages from a durable subscription.
    * @param subName Subscription name.
    * @param clientID Client ID.
    * @param selector Filter expression.
    * @return list of javax.jms.Message
    * @throws JMSException
    * @see ManageableTopic#getMessagesFromDurableSub(String, String, String)
    */
   public List listMessagesDurableSub(String subName, String clientID, String selector)
      throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Topic is stopped.");
            return new ArrayList();
         }
   
         List subs = exchange.listBindingsForWildcard(name);
         
         return getMessagesFromDurableSub(subs, subName, clientID, trimSelector(selector));
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " listMessagesDurableSub");
      } 
   }
   
   /**
    * Get messages from a non-durable subscription.
    * @param channelID
    * @param selector Filter expression.
    * @return list of javax.jms.Message
    * @throws JMSException
    * @see ManageableTopic#getMessagesFromNonDurableSub(Long, String)
    */
   public List listMessagesNonDurableSub(long channelID, String selector)
      throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Topic is stopped.");
            return new ArrayList();
         }
         
         List subs = exchange.listBindingsForWildcard(name);
         
         return getMessagesFromNonDurableSub(subs, channelID, trimSelector(selector));
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " listMessagesNonDurableSub");
      } 
   }

   
   // TODO implement these:

//   int getAllMessageCount();
//
//   int getDurableMessageCount();
//
//   int getNonDurableMessageCount();
//
//   int getAllSubscriptionsCount();
//
//   int getDurableSubscriptionsCount();
//
//   int getNonDurableSubscriptionsCount();
//
//   java.util.List listAllSubscriptions();
//
//   java.util.List listDurableSubscriptions();
//
//   java.util.List listNonDurableSubscriptions();
//
//   java.util.List listMessages(java.lang.String id) throws java.lang.Exception;
//
//   java.util.List listMessages(java.lang.String id, java.lang.String selector) throws java.lang.Exception;
//
//   List listNonDurableMessages(String id, String sub) throws Exception;
//
//   List listNonDurableMessages(String id, String sub, String selector) throws Exception;
//
//   List listDurableMessages(String id, String name) throws Exception;
//
//   List listDurableMessages(String id, String name, String selector) throws Exception;

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected boolean isQueue()
   {
      return false;
   }
   
   protected String getSubscriptionsAsText(List bindings, boolean durable)
   {
      StringBuffer sb = new StringBuffer();
      Iterator iter = bindings.iterator();
      while (iter.hasNext())
      {
         Binding binding = (Binding)iter.next();
                  
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
            sb.append(binding.getSelector());
            sb.append("\", noLocal=\"");
            sb.append(binding.isNoLocal());
            sb.append("\"\n");
         }
         else if (!durable && !binding.isDurable())
         {            
            sb.append("Non-durable, subscriptionID=\"");
            sb.append(binding.getChannelId());
            sb.append("\", selector=\"");
            sb.append(binding.getSelector());
            sb.append("\", noLocal=\"");
            sb.append(binding.isNoLocal());
            sb.append("\"\n");
         }
      }
      return sb.toString();
   }
   
   protected List getMessagesFromDurableSub(List bindings, String name,
                                            String clientID, String selector) throws InvalidSelectorException
   {
      Iterator iter = bindings.iterator();
      while (iter.hasNext())
      {
         Binding binding = (Binding)iter.next();
         // If subID matches, then get message list from the subscription
         if (matchDurableSubscription(name, clientID, binding))
         {
            MessageQueue queue = binding.getQueue();
            return queue.browse(null == selector ? null : new Selector(selector));
         }
      }   
      // No match, return an empty list
      return new ArrayList();
   }
   
   /**
    * @see ManageableTopic#getMessagesFromNonDurableSub(Long, String)
    */
   public List getMessagesFromNonDurableSub(List bindings, long channelID, String selector) throws InvalidSelectorException
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
   
   /**
    * XXX Placeholder
    * @see ManageableTopic#getSubscriptions()
    */
   /*
   public List getSubscriptions()
   {
      ArrayList list = new ArrayList();
      Iterator iter = iterator();
      while (iter.hasNext())
      {
         CoreSubscription sub = (CoreSubscription)iter.next();
         if (sub instanceof CoreDurableSubscription)
            list.add(new String[]{ 
                        Long.toString(sub.getChannelID()), 
                        ((CoreDurableSubscription)sub).getClientID(),
                        ((CoreDurableSubscription)sub).getName()});
         else
            list.add(new String[]{ 
                  Long.toString(sub.getChannelID()), "", ""});
      }
      return list;
   }
   */
   
   
   /**
    * XXX Placeholder
    * @see ManageableTopic#getSubscriptions(boolean)
    */
   /*
   public List getSubscriptions(boolean durable)
   {
      ArrayList list = new ArrayList();
      Iterator iter = iterator();
      while (iter.hasNext())
      {
         CoreSubscription sub = (CoreSubscription)iter.next();
         if (sub instanceof CoreDurableSubscription && durable)
            list.add(new String[]{ 
                        Long.toString(sub.getChannelID()), 
                        ((CoreDurableSubscription)sub).getClientID(),
                        ((CoreDurableSubscription)sub).getName()});
         else if (!(sub instanceof CoreDurableSubscription) && !durable)
            list.add(new String[]{ 
                  Long.toString(sub.getChannelID()), "", ""});
      }
      return list;
   }
   */   
   
   /**
    * XXX Placeholder
    * @see ManageableTopic#getMessages(long, String, String, String)
    */
   /*
   public List getMessages(long channelID, String clientID, String subName, String selector) throws InvalidSelectorException
   {
      Iterator iter = iterator();
      while (iter.hasNext())
      {
         CoreSubscription sub = (CoreSubscription)iter.next();
         // If subID matches, then get message list from the subscription
         if (matchSubscription(channelID, clientID, subName, sub))
            return sub.browse(null == selector ? null : new Selector(selector));
      }   
      // No match, return an empty list
      return new ArrayList();
   }
   */

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
