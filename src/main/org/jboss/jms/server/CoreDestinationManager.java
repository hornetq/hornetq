/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server;

import org.jboss.jms.destination.JBossDestination;
import org.jboss.messaging.core.local.AbstractDestination;
import org.jboss.messaging.core.local.LocalQueue;
import org.jboss.messaging.core.local.LocalTopic;
import org.jboss.logging.Logger;

import javax.jms.Destination;
import javax.jms.JMSException;

import java.util.Map;
import java.util.HashMap;

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
      queueMap = new HashMap();
      topicMap = new HashMap();
      this.destinationManager = destinationManager;
   }

   // Public --------------------------------------------------------

   /**
    * Returns the core abstract destination that corresponds to the given destination name.
    *
    * @return the AbstractDestination instance or null if there isn't a mapping for the given
    *         destination.
    *
    * @exception JMSException - thrown if the JNDI destination cannot be mapped on a core
    *            destination.
    */
   AbstractDestination getCoreDestination(boolean isQueue, String name) throws JMSException
   {
      if (log.isTraceEnabled()) { log.trace("getting core " + (isQueue ? "queue" : "topic")
                                            + " for " + name); }

      if (isQueue)
      {
         return (AbstractDestination)queueMap.get(name);
      }
      else
      {
         return (AbstractDestination)topicMap.get(name);
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

      AbstractDestination ad = getCoreDestination(isQueue, name);
      if (ad != null)
      {
         throw new JMSException("Destination " + jmsDestination + " already exists");
      }

      ServerPeer serverPeer = destinationManager.getServerPeer();

      // TODO I am using LocalQueues for the time being, switch to distributed Queues
      if (isQueue)
      {
         ad = new LocalQueue(name);
         queueMap.put(name, ad);
      }
      else
      {
         // TODO I am using LocalTopics for the time being, switch to distributed Topics
         ad = new LocalTopic(name);
         topicMap.put(name, ad);
      }

      ad.setAcknowledgmentStore(serverPeer.getAcknowledgmentStore());
      ad.setMessageStore(serverPeer.getMessageStore());

      // make the destination transactional if there is a transaction manager available
      ad.setTransactionManager(serverPeer.getTransactionManager());
   }
   
   /**
    * Remove an AbstractDestination.
    */
   AbstractDestination removeCoreDestination(boolean isQueue, String name)
   {
      if (isQueue)
      {
         return (AbstractDestination)queueMap.remove(name);
      }
      else
      {
         return (AbstractDestination)topicMap.remove(name);
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}




