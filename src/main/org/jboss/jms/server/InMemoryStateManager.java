/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.jms.JMSException;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.local.DurableSubscription;
import org.jboss.messaging.core.local.Topic;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;

/**
 * In memory implementation of StateManager
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * $Id$
 */
public class InMemoryStateManager implements StateManager 
{
   
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(InMemoryStateManager.class);
   
   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------
   
   protected Map subscriptions;
   
   protected ServerPeer serverPeer;
   
   // Constructors --------------------------------------------------
   
   public InMemoryStateManager(ServerPeer serverPeer)
   {
      subscriptions = new ConcurrentReaderHashMap();
      this.serverPeer = serverPeer;
   }
      
   // Public --------------------------------------------------------
   
   public DurableSubscription getDurableSubscription(String clientID, String subscriptionName)
      throws JMSException
   {
      Map subs = (Map)subscriptions.get(clientID);
      return subs == null ? null : (DurableSubscription)subs.get(subscriptionName);
   }
   
   public DurableSubscription createDurableSubscription(String topicName, String clientID,
                                                        String subscriptionName,
         String selector) throws JMSException
   {
      Map subs = (Map)subscriptions.get(clientID);
      if (subs == null)
      {
         subs = new ConcurrentReaderHashMap();
         subscriptions.put(clientID, subs);
      }
      
      if (subs.get(subscriptionName) != null)
      {
         throw new IllegalStateException("Client with client id " + clientID + " already has " +
                                         "a subscription with name " + subscriptionName);
      }
 
      DurableSubscription subscription =
            internalCreateDurableSubscription(clientID, subscriptionName, topicName, selector);
            
      subs.put(subscriptionName, subscription);
      
      return subscription;
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
                       
      if (log.isTraceEnabled()) { log.trace("Removing durable sub: " + subscriptionName); }
      
      DurableSubscription removed = (DurableSubscription)subs.remove(subscriptionName);
      
      if (subs.size() == 0)
      {
         subscriptions.remove(clientID);
      }
      
      return removed != null;
   }
   
   public String getPreConfiguredClientID(String username) throws JMSException
   {
      return null;
   }
   
   public Set loadDurableSubscriptionsForTopic(String topicName) throws JMSException
   {
      return Collections.EMPTY_SET;
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   private DurableSubscription internalCreateDurableSubscription(String clientID, String subName,
                                                                 String topicName, String selector)
         throws JMSException
   {
      Topic topic = (Topic)serverPeer.getDestinationManager().getCoreDestination(false, topicName);
      if (topic == null)
      {
         throw new javax.jms.IllegalStateException("Topic " + topicName + " is not loaded");
      }

      DurableSubscription sub =
            new DurableSubscription(clientID, subName, topic, selector,
                                    serverPeer.getMessageStore(),
                                    serverPeer.getPersistenceManager());;

      return sub;
   }
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
   
}
