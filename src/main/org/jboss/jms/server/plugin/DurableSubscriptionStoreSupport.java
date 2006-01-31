/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.plugin;

import java.util.Map;
import java.util.Collections;
import java.util.Set;
import java.util.HashSet;

import javax.jms.JMSException;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.local.CoreDurableSubscription;
import org.jboss.messaging.core.local.Topic;
import org.jboss.messaging.core.plugin.contract.TransactionLog;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.jms.server.DestinationManager;
import org.jboss.jms.server.plugin.contract.DurableSubscriptionStore;
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
   extends ServiceMBeanSupport implements DurableSubscriptionStore
{

   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(DurableSubscriptionStoreSupport.class);
   
   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Map<clientID - Map<subscriptionName - CoreDurableSubscription>>
   protected Map subscriptions;

   // Constructors --------------------------------------------------
   
   public DurableSubscriptionStoreSupport()
   {
      subscriptions = new ConcurrentReaderHashMap();
   }

   // ServiceMBeanSupport overrides ---------------------------------

   protected void startService() throws Exception
   {
      log.debug(this + " started");
   }

   protected void stopService() throws Exception
   {
      log.debug(this + " stopped");
   }

   // DurableSubscriptionStore implementation ---------------

   public CoreDurableSubscription createDurableSubscription(String topicName,
                                                        String clientID,
                                                        String subscriptionName,
                                                        String selector,
                                                        boolean noLocal,
                                                        DestinationManager dm,
                                                        MessageStore ms,
                                                        TransactionLog tl)
      throws JMSException
   {
      Map subs = (Map)subscriptions.get(clientID);
      if (subs == null)
      {
         subs = new ConcurrentReaderHashMap();
         subscriptions.put(clientID, subs);
      }

      CoreDurableSubscription subscription =
         internalCreateDurableSubscription(clientID,
                                           subscriptionName,
                                           topicName,
                                           selector,
                                           noLocal,
                                           dm, ms, tl);

      subs.put(subscriptionName, subscription);

      if (log.isTraceEnabled()) { log.trace(this + " created " + subscription); }

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

      if (log.isTraceEnabled()) { log.trace("removing durable subscription " + subscriptionName); }

      CoreDurableSubscription removed = (CoreDurableSubscription)subs.remove(subscriptionName);

      if (subs.size() == 0)
      {
         subscriptions.remove(clientID);
      }

      return removed != null;
   }

//   public void clearSubscriptionsForTopic(String topicName) throws JMSException
//   {
//      List toRemove = new ArrayList();
//      for(Iterator i = subscriptions.keySet().iterator(); i.hasNext(); )
//      {
//         String clientID = (String)i.next();
//         Map subs = (Map)subscriptions.get(clientID);
//         for(Iterator j = subs.keySet().iterator(); j.hasNext(); )
//         {
//            String name = (String)j.next();
//            CoreDurableSubscription ds = (CoreDurableSubscription)subs.get(name);
//            if (ds.getTopic().getName().equals(topicName))
//            {
//               toRemove.add(ds);
//            }
//         }
//
//         if (subs.keySet().size() == 0)
//         {
//            i.remove();
//         }
//      }
//
//      for(Iterator i = toRemove.iterator(); i.hasNext(); )
//      {
//         CoreDurableSubscription ds = (CoreDurableSubscription)i.next();
//         removeDurableSubscription(ds.getClientID(), ds.getName());
//      }
//
//      if (log.isTraceEnabled()) { log.trace(toRemove.size() + " durable subscription(s) removed for topic " + topicName); }
//   }


   // JMX Managed Operations ----------------------------------------

   /**
    * @return a Set<String>. It may return an empty Set, but never null.
    */
   public Set getSubscriptions(String clientID)
   {
      Map m = (Map)subscriptions.get(clientID);
      if (m == null)
      {
         return Collections.EMPTY_SET;
      }
      // concurrent keyset is not serializable
      Set result = new HashSet();
      result.addAll(m.keySet());
      return result;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   protected CoreDurableSubscription getDurableSubscription(String clientID,
                                                        String subscriptionName) throws JMSException
   {
      Map subs = (Map)subscriptions.get(clientID);
      return subs == null ? null : (CoreDurableSubscription)subs.get(subscriptionName);
   }

   // Private -------------------------------------------------------

   private CoreDurableSubscription internalCreateDurableSubscription(String clientID,
                                                                 String subName,
                                                                 String topicName,
                                                                 String selector,
                                                                 boolean noLocal,
                                                                 DestinationManager dm,
                                                                 MessageStore ms,
                                                                 TransactionLog tl)
         throws JMSException
   {
      Topic topic = (Topic)dm.getCoreDestination(false, topicName);
      if (topic == null)
      {
         throw new javax.jms.IllegalStateException("Topic " + topicName + " is not loaded");
      }

      return new CoreDurableSubscription(clientID, subName, topic, selector, noLocal, ms, tl);
   }

   // Inner classes -------------------------------------------------
   
}
