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

import java.util.Map;

import javax.jms.Destination;
import javax.jms.JMSException;

import org.jboss.jms.destination.JBossDestination;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.Channel;
import org.jboss.messaging.core.Distributor;
import org.jboss.messaging.core.Receiver;
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

   // <name - AbstractDestination>
   protected Map queueMap;

   // <name - AbstractDestination>
   protected Map topicMap;

   protected DestinationManagerImpl destinationManager;

   // Constructors --------------------------------------------------

   CoreDestinationManager(DestinationManagerImpl destinationManager) throws Exception
   {
      queueMap = new ConcurrentReaderHashMap();
      topicMap = new ConcurrentReaderHashMap();
      this.destinationManager = destinationManager;
   }

   // Public --------------------------------------------------------

   /**
    * Returns the core destination that corresponds to the given destination name.
    *
    * @return the AbstractDestination instance or null if there isn't a mapping for the given
    *         destination.
    *
    * @exception JMSException - thrown if the JNDI destination cannot be mapped on a core
    *            destination.
    */
   Distributor getCoreDestination(boolean isQueue, String name) throws JMSException
   {
      if (log.isTraceEnabled()) { log.trace("getting core " + (isQueue ? "queue" : "topic")
                                            + " for " + name); }

      if (isQueue)
      {
         return (Distributor)queueMap.get(name);
      }
      else
      {
         return (Distributor)topicMap.get(name);
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

      Distributor c = getCoreDestination(isQueue, name);
      if (c != null)
      {
         throw new JMSException("Destination " + jmsDestination + " already exists");
      }

      ServerPeer sp = destinationManager.getServerPeer();

      // TODO I am using LocalQueues for the time being, switch to distributed Queues
      if (isQueue)
      {
         c = new Queue(name,
                       sp.getMessageStore(),
                       sp.getPersistenceManager());
         queueMap.put(name, c);
      }
      else
      {
         // TODO I am using LocalTopics for the time being, switch to distributed Topics
         c = new Topic(name);
         topicMap.put(name, c);
      }
      
      //We also remove all message data here just in case there was any data left around in the database
      //from a previous failure
      try
      {
         sp.getPersistenceManager().removeAllMessageData(name);
         
         //FIXME - Also need to remove any message refs stored in memory for this destination
         
      }
      catch (Exception e)
      {
         log.error("Failed to remove message data", e);
      }
      
   }
   
   /**
    * Remove an AbstractDestination.
    */
   Distributor removeCoreDestination(boolean isQueue, String name)
   {
      ServerPeer sp = destinationManager.getServerPeer();
      try
      {
         sp.getPersistenceManager().removeAllMessageData(name);
         
         //FIXME - Also need to remove any message refs stored in memory for this destination
         
      }
      catch (Exception e)
      {
         log.error("Failed to remove message data", e);
      }
      if (isQueue)
      {
         return (Distributor)queueMap.remove(name);
      }
      else
      {
         return (Distributor)topicMap.remove(name);
      }
      
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}




