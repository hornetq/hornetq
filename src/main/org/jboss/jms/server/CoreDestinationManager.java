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

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.jms.Destination;
import javax.jms.JMSException;

import org.jboss.jms.destination.JBossDestination;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.CoreDestination;
import org.jboss.messaging.core.local.DurableSubscription;
import org.jboss.messaging.core.local.Queue;
import org.jboss.messaging.core.local.Topic;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;

/**
 * Manages access to destinations (local or distributed). There is a single CoreDestinationManager
 * instance for each server peer.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
class CoreDestinationManager
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(CoreDestinationManager.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // <name - CoreDestination>
   protected Map queueMap;

   // <name - CoreDestination>
   protected Map topicMap;

   protected FacadeDestinationManager destinationManager;

   // Constructors --------------------------------------------------

   CoreDestinationManager(FacadeDestinationManager destinationManager) throws Exception
   {
      queueMap = new ConcurrentReaderHashMap();
      topicMap = new ConcurrentReaderHashMap();
      this.destinationManager = destinationManager;

      log.debug("CoreDestinationManager created");
   }

   // Public --------------------------------------------------------

   /**
    * Returns the core destination that corresponds to the given destination name.
    *
    * @return the CoreDestination instance or null if there isn't a mapping for the given
    *         destination.
    *
    * @exception JMSException - thrown if the JNDI destination cannot be mapped on a core
    *            destination.
    */
   CoreDestination getCoreDestination(boolean isQueue, String name) throws JMSException
   {
      if (log.isTraceEnabled()) { log.trace("getting core " + (isQueue ? "queue" : "topic") + " for " + name); }

      if (isQueue)
      {
         return (CoreDestination)queueMap.get(name);
      }
      else
      {
         return (CoreDestination)topicMap.get(name);
      }
   }

   /**
    * Add a JMS Deestination.
    * 
    * @param jmsDestination - the JMS destination to add.
    *
    * @throws JMSException if the destination with that name already exists
    */
   void addCoreDestination(Destination jmsDestination) throws JMSException
   {
      JBossDestination d = (JBossDestination)jmsDestination;
      String name = d.getName();
      boolean isQueue = d.isQueue();

      CoreDestination cd = getCoreDestination(isQueue, name);
      if (cd != null)
      {
         throw new JMSException("Destination " + jmsDestination + " already exists");
      }

      ServerPeer sp = destinationManager.getServerPeer();

      // TODO I am using LocalQueues for the time being, switch to distributed Queues
      if (isQueue)
      {
         cd = new Queue(name, sp.getMessageStoreDelegate(), sp.getTransactionLogDelegate());
         
         try
         {
            // we load the queue with any state it might have in the db
            ((Queue)cd).load();
         }
         catch (Exception e)
         {
            log.error("Failed to load queue state", e);
            JMSException e2 = new JMSException("Failed to load queue state");
            e2.setLinkedException(e);
            throw e2;
         }
         
         queueMap.put(name, cd);
      }
      else
      {
         // TODO I am using LocalTopics for the time being, switch to distributed Topics
         cd = new Topic(name, sp.getMessageStoreDelegate());
         
         topicMap.put(name, cd);
         
         //TODO
         //The following piece of code may be better placed either in the Topic itself
         //or in the StateManager - I'm not sure it really belongs here
         
         //Load any durable subscriptions for the Topic
         Set durableSubs =
            sp.getDurableSubscriptionStoreDelegate().loadDurableSubscriptionsForTopic(name);

         Iterator iter = durableSubs.iterator();
         while (iter.hasNext())
         {
            DurableSubscription sub = (DurableSubscription)iter.next();
            //load the state of the dub
            try
            {
               sub.load();
            }
            catch (Exception e)
            {
               log.error("Failed to load queue state", e);
               JMSException e2 = new JMSException("Failed to load durable subscription state");
               e2.setLinkedException(e);
               throw e2;
            }
            //and subscribe it to the Topic
            sub.subscribe();
         }
      }
   }
   
   /**
    * Remove an AbstractDestination.
    */
   CoreDestination removeCoreDestination(boolean isQueue, String name)
   {
  
      if (isQueue)
      {
         return (CoreDestination)queueMap.remove(name);
      }
      else
      {
         return (CoreDestination)topicMap.remove(name);
      }
      
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}




