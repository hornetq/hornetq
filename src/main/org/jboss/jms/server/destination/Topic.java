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
import org.jboss.jms.server.subscription.DurableSubscription;
import org.jboss.jms.server.subscription.Subscription;


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

   /**
    * Remove all messages from subscription's storage.
    */
   public void removeAllMessages() throws JMSException
   {
      if (!started)
      {
         log.warn("Topic is stopped.");
         return;
      }
      JBossTopic jbt = new JBossTopic(name);
      org.jboss.messaging.core.local.Topic t = (org.jboss.messaging.core.local.Topic)cm.getCoreDestination(jbt);
      
      //XXX How to lock down all subscriptions?
      Iterator iter = t.iterator();
      while (iter.hasNext())
      {
         Object sub = iter.next();
         ((Subscription)sub).removeAllMessages();
      }
   }
   
   /**
    * All subscription count
    * @return all subscription count
    * @throws JMSException
    */
   public int subscriptionCount() throws JMSException
   {
      if (!started)
      {
         log.warn("Topic is stopped.");
         return 0;
      }

      JBossTopic jbt = new JBossTopic(name);
      org.jboss.messaging.core.local.Topic t = (org.jboss.messaging.core.local.Topic)cm.getCoreDestination(jbt);
     
      int count = 0;
      Iterator iter = t.iterator();
      while (iter.hasNext())
      {
         count++;
         iter.next();
      }
      return count;
   }

   /**
    * Durable/nondurable subscription count
    * @param durable If true return durable subscription count.
    *                If false return nondurable subscription count.
    * @return either durable or nondurable subscription count depending on param.
    * @throws JMSException
    */
   public int subscriptionCount(boolean durable) throws JMSException
   {
      if (!started)
      {
         log.warn("Topic is stopped.");
         return 0;
      }

      JBossTopic jbt = new JBossTopic(name);
      org.jboss.messaging.core.local.Topic t = (org.jboss.messaging.core.local.Topic)cm.getCoreDestination(jbt);
      
      int count = 0;
      Iterator iter = t.iterator();
      while (iter.hasNext())
      {
         Subscription sub = (Subscription)iter.next();
         if (sub.isRecoverable() ^ !durable)
         {
            count++;
         }
      }
      return count;
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
   public String listSubscriptionsAsText() throws JMSException
   {
      if (!started)
      {
         log.warn("Topic is stopped.");
         return "";
      }

      JBossTopic jbt = new JBossTopic(name);
      org.jboss.messaging.core.local.Topic t = (org.jboss.messaging.core.local.Topic)cm.getCoreDestination(jbt);
      return getSubscriptionsAsText(t, true) + getSubscriptionsAsText(t, false);
   }
   
   

   /**
    * Returns a human readable list containing the names of current subscriptions.
    * @param durable If true, return durable subscription list.
    *                If false, return non-durable subscription list.
    * @return String of subscription list. Never null.
    */
   public String listSubscriptionsAsText(boolean durable) throws JMSException
   {
      if (!started)
      {
         log.warn("Topic is stopped.");
         return "";
      }

      JBossTopic jbt = new JBossTopic(name);
      org.jboss.messaging.core.local.Topic t = (org.jboss.messaging.core.local.Topic)cm.getCoreDestination(jbt);
      return getSubscriptionsAsText(t, durable);
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
    * @param name Subscription name.
    * @param clientID Client ID.
    * @param selector Filter expression.
    * @return list of javax.jms.Message
    * @throws JMSException
    * @see ManageableTopic#getMessagesFromDurableSub(String, String, String)
    */
   public List listMessagesDurableSub(String name, String clientID, String selector)
      throws JMSException
   {
      if (!started)
      {
         log.warn("Topic is stopped.");
         return new ArrayList();
      }

      JBossTopic jbt = new JBossTopic(this.name);
      org.jboss.messaging.core.local.Topic t = (org.jboss.messaging.core.local.Topic)cm.getCoreDestination(jbt);
      return getMessagesFromDurableSub(t, name, clientID, trimSelector(selector));
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
      throws JMSException
   {
      if (!started)
      {
         log.warn("Topic is stopped.");
         return new ArrayList();
      }
      
      JBossTopic jbt = new JBossTopic(this.name);
      org.jboss.messaging.core.local.Topic t = (org.jboss.messaging.core.local.Topic)cm.getCoreDestination(jbt);
      return getMessagesFromNonDurableSub(t, channelID, trimSelector(selector));
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
   
   protected String getSubscriptionsAsText(org.jboss.messaging.core.local.Topic t, boolean durable)
   {
      StringBuffer sb = new StringBuffer();
      Iterator iter = t.iterator();
      while (iter.hasNext())
      {
         Subscription sub = (Subscription)iter.next();
         if (durable && sub.isRecoverable())
         {
            sb.append(sub.asText());
         }
         else if (!durable && !sub.isRecoverable())
         {
            sb.append(sub.asText());
         }
      }
      return sb.toString();
   }
   
   protected List getMessagesFromDurableSub(org.jboss.messaging.core.local.Topic t, String name, String clientID, String selector) throws InvalidSelectorException
   {
      Iterator iter = t.iterator();
      while (iter.hasNext())
      {
         Subscription sub = (Subscription)iter.next();
         // If subID matches, then get message list from the subscription
         if (matchDurableSubscription(name, clientID, sub))
            return sub.browse(null == selector ? null : new Selector(selector));
      }   
      // No match, return an empty list
      return new ArrayList();
   }
   
   /**
    * @see ManageableTopic#getMessagesFromNonDurableSub(Long, String)
    */
   public List getMessagesFromNonDurableSub(org.jboss.messaging.core.local.Topic t, long channelID, String selector) throws InvalidSelectorException
   {
      Iterator iter = t.iterator();
      while (iter.hasNext())
      {
         Subscription sub = (Subscription)iter.next();
         // If subID matches, then get message list from the subscription
         if (matchNonDurableSubscription(channelID, sub))
            return sub.browse(null == selector ? null : new Selector(selector));
      }   
      // No match, return an empty list
      return new ArrayList();
   }

   // Test if the durable subscriptions match
   private boolean matchDurableSubscription(String name, String clientID, Subscription sub)
   {
      // Validate the name
      if (null == name)
         throw new IllegalArgumentException();
      // Must be durable
      if (!sub.isRecoverable())
         return false;

      DurableSubscription duraSub = (DurableSubscription)sub;
      // Subscription name check
      if (!name.equals(duraSub.getName()))
         return false;
      // Client ID check: if no client ID specified, it's considered as matched 
      if (null == clientID || 0 == clientID.length())
         return true;
      if (!clientID.equals(duraSub.getClientID()))
         return false;
      return true;
   }
   
   // Test if the non-durable subscriptions match
   private boolean matchNonDurableSubscription(long channelID, Subscription sub)
   {
      // Must be non-durable
      if (sub.isRecoverable())
         return false;
      // Channel ID must be the same
      if (channelID != sub.getChannelID())
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
