/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.destination;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import javax.jms.JMSException;

import org.jboss.jms.util.ExceptionUtil;
import org.jboss.jms.util.XMLUtil;
import org.jboss.messaging.core.local.PagingFilteredQueue;
import org.jboss.messaging.core.plugin.postoffice.Binding;

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
public class TopicService extends DestinationServiceSupport
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public TopicService()
   {
      destination = new ManagedTopic();      
   }
   
   public TopicService(boolean createdProgrammatically)
   {
      super(createdProgrammatically);
      
      destination = new ManagedTopic();      
   }

   // JMX managed attributes ----------------------------------------

   // JMX managed operations ----------------------------------------
   
   public synchronized void startService() throws Exception
   {
      super.startService();
      
      try
      {
         postOffice = serverPeer.getTopicPostOfficeInstance();
         
         destination.setPostOffice(postOffice);
           
         // We deploy any queues corresponding to pre-existing durable subscriptions
         Collection bindings = postOffice.listBindingsForCondition(destination.getName());
         Iterator iter = bindings.iterator();
         while (iter.hasNext())
         {
            Binding binding = (Binding)iter.next();
            
            PagingFilteredQueue queue = (PagingFilteredQueue)binding.getQueue();

            queue.deactivate();
            queue.activate(destination.getFullSize(), destination.getPageSize(), destination.getDownCacheSize());            
         }

         // push security update to the server
         sm.setSecurityConfig(isQueue(), destination.getName(), destination.getSecurityConfig());
                   
         dm.registerDestination(destination);
         
         log.debug(this + " security configuration: " + (destination.getSecurityConfig() == null ?
            "null" : "\n" + XMLUtil.elementToString(destination.getSecurityConfig())));
         
         started = true;

         log.info(this + " started, fullSize=" + destination.getFullSize() + ", pageSize=" + destination.getPageSize() + ", downCacheSize=" + destination.getDownCacheSize());
      }
      catch (Throwable t)
      {
         ExceptionUtil.handleJMXInvocation(t, this + " startService");
      }
   }

   public synchronized void stopService() throws Exception
   {
      try
      {
         dm.unregisterDestination(destination);
         
         //When undeploying a topic, any non durable subscriptions will be removed
         //Any durable subscriptions will survive in persistent storage, but be removed
         //from memory
         
         //First we remove any data for a non durable sub - a non durable sub might have data in the
         //database since it might have paged
         
         Collection bindings = postOffice.listBindingsForCondition(destination.getName());
         
         Iterator iter = bindings.iterator();
         while (iter.hasNext())            
         {
            Binding binding = (Binding)iter.next();
            
            PagingFilteredQueue queue = (PagingFilteredQueue)binding.getQueue();
            
            if (!queue.isRecoverable())
            {
               queue.removeAllReferences();
            }
                        
            queue.deactivate();
         }
          
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
         
         ((ManagedTopic)destination).removeAllMessages();
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
   
         return ((ManagedTopic)destination).subscriptionCount();        
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
         
         return ((ManagedTopic)destination).subscriptionCount(durable);
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
   
         return ((ManagedTopic)destination).listSubscriptionsAsText();

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
   
         return ((ManagedTopic)destination).listSubscriptionsAsText(durable);
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
   
         return ((ManagedTopic)destination).listMessagesDurableSub(subName, clientID, selector);
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
         
         return ((ManagedTopic)destination).listMessagesNonDurableSub(channelID, selector);
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
