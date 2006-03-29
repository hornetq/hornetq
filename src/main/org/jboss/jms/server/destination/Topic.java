/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.destination;

import java.util.List;

import javax.jms.JMSException;

import org.jboss.jms.destination.JBossTopic;
import org.jboss.messaging.core.local.ManageableTopic;


/**
 * A deployable JBoss Messaging topic.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
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
      JBossTopic jbt = new JBossTopic(name);
      ManageableTopic t = (ManageableTopic)cm.getCoreDestination(jbt);
      t.removeAllMessages();
   }

   /**
    * All subscription count
    * @return all subscription count
    * @throws JMSException
    */
   public int subscriptionCount() throws JMSException
   {
      JBossTopic jbt = new JBossTopic(name);
      ManageableTopic t = (ManageableTopic)cm.getCoreDestination(jbt);
      return t.subscriptionCount();
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
      JBossTopic jbt = new JBossTopic(name);
      ManageableTopic t = (ManageableTopic)cm.getCoreDestination(jbt);
      return t.subscriptionCount(durable);
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
      JBossTopic jbt = new JBossTopic(name);
      ManageableTopic t = (ManageableTopic)cm.getCoreDestination(jbt);
      return t.getSubscriptionsAsText(true) + t.getSubscriptionsAsText(false);
   }

   /**
    * Returns a human readable list containing the names of current subscriptions.
    * @param durable If true, return durable subscription list.
    *                If false, return non-durable subscription list.
    * @return String of subscription list. Never null.
    */
   public String listSubscriptionsAsText(boolean durable) throws JMSException
   {
      JBossTopic jbt = new JBossTopic(name);
      ManageableTopic t = (ManageableTopic)cm.getCoreDestination(jbt);
      return t.getSubscriptionsAsText(durable);
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
      JBossTopic jbt = new JBossTopic(this.name);
      ManageableTopic t = (ManageableTopic)cm.getCoreDestination(jbt);
      return t.getMessagesFromDurableSub(name, clientID, trimSelector(selector));
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
      JBossTopic jbt = new JBossTopic(this.name);
      ManageableTopic t = (ManageableTopic)cm.getCoreDestination(jbt);
      return t.getMessagesFromNonDurableSub(channelID, trimSelector(selector));
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

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
