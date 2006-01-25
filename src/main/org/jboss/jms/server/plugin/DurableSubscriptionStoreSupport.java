/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.plugin;

import java.util.Map;

import javax.jms.JMSException;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.local.DurableSubscription;
import org.jboss.messaging.core.local.Topic;
import org.jboss.messaging.core.plugin.contract.TransactionLogDelegate;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.plugin.contract.DurableSubscriptionStoreDelegate;
import org.jboss.jms.server.plugin.contract.MessageStoreDelegate;
import org.jboss.system.ServiceMBeanSupport;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;

/**
 * In-memory layer of a durable subscription store. Since a durable subscription <b>must</b> be
 * stored in persistent storage, this class can be never used by itself, hence its abstract
 * qualifier.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.com">Ovidiu Feodorov</a>
 *
 * $Id$
 */
public abstract class DurableSubscriptionStoreSupport
   extends ServiceMBeanSupport implements DurableSubscriptionStoreDelegate
{

   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(DurableSubscriptionStoreSupport.class);
   
   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------
   
   protected Map subscriptions;

   // Constructors --------------------------------------------------
   
   public DurableSubscriptionStoreSupport()
   {
      subscriptions = new ConcurrentReaderHashMap();
   }

   // ServiceMBeanSupport overrides ---------------------------------

   // DurableSubscriptionStoreDelegate implementation ---------------

   public DurableSubscription createDurableSubscription(String topicName,
                                                        String clientID,
                                                        String subscriptionName,
                                                        String selector,
                                                        boolean noLocal)
      throws JMSException
   {
      Map subs = (Map)subscriptions.get(clientID);
      if (subs == null)
      {
         subs = new ConcurrentReaderHashMap();
         subscriptions.put(clientID, subs);
      }

      DurableSubscription subscription =
         internalCreateDurableSubscription(clientID, subscriptionName, topicName, selector, noLocal);

      subs.put(subscriptionName, subscription);

      return subscription;
   }

   public DurableSubscription getDurableSubscription(String clientID,
                                                     String subscriptionName)
      throws JMSException
   {
      Map subs = (Map)subscriptions.get(clientID);
      return subs == null ? null : (DurableSubscription)subs.get(subscriptionName);
   }


   public boolean removeDurableSubscription(String clientID, String subscriptionName)
      throws JMSException
   {
      if (clientID == null)
      {
         throw new JMSException("Client ID must be set for connection!");
      }

      Map subs = (Map)subscriptions.get(clientID);

      if (subs == null)
      {
         return false;
      }

      if (log.isTraceEnabled()) { log.trace("removing durable subscription " + subscriptionName); }

      DurableSubscription removed = (DurableSubscription)subs.remove(subscriptionName);

      if (subs.size() == 0)
      {
         subscriptions.remove(clientID);
      }

      return removed != null;
   }

   // Public --------------------------------------------------------

   // TODO this should go away! Replace it with proper dependencies.
   private ServerPeer serverPeer;

   // TODO this should go away! Replace it with proper dependencies.
   public void setServerPeer(ServerPeer serverPeer)
   {
      this.serverPeer = serverPeer;
   }

   // TODO this should go away! Replace it with proper dependencies.
   private TransactionLogDelegate tl;

   // TODO this should go away! Replace it with proper dependencies.
   public void setTransactionLog(TransactionLogDelegate tl)
   {
      this.tl = tl;
   }

   // TODO this should go away! Replace it with proper dependencies.
   private MessageStoreDelegate ms;

   // TODO this should go away! Replace it with proper dependencies.
   public void setMessageStore(MessageStoreDelegate ms)
   {
      this.ms = ms;
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   private DurableSubscription internalCreateDurableSubscription(String clientID, String subName,
                                                                 String topicName, String selector,
                                                                 boolean noLocal)
         throws JMSException
   {
      Topic topic = (Topic)serverPeer.getDestinationManager().getCoreDestination(false, topicName);
      if (topic == null)
      {
         throw new javax.jms.IllegalStateException("Topic " + topicName + " is not loaded");
      }

      return new DurableSubscription(clientID, subName, topic, selector, noLocal, ms, tl);
   }
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
   
}
