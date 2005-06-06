/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server;

import org.jboss.jms.destination.JBossDestination;
import org.jboss.messaging.core.local.LocalQueue;
import org.jboss.messaging.core.local.LocalTopic;
import org.jboss.messaging.core.local.AbstractDestination;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.jms.Destination;
import javax.jms.JMSException;

import java.util.Map;
import java.util.HashMap;

/**
 * Manages access to distributed destinations. There is a single DestinationManager instance for
 * each server peer.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class DestinationManager
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected ServerPeer serverPeer;
   protected Context ic;
   // <name(String) - AbstractDestination>
   protected Map topics;
   // <name(String) - AbstractDestination>
   protected Map queues;

   // Constructors --------------------------------------------------

   public DestinationManager(ServerPeer serverPeer) throws Exception
   {
      this.serverPeer = serverPeer;
      ic = new InitialContext(serverPeer.getJNDIEnvironment());
      topics = new HashMap();
      queues = new HashMap();
   }

   // Public --------------------------------------------------------

   /**
    * Returns the core abstract destination that corresponds to the given JNDI destination.
    * @exception Exception - thrown if the JNDI destination cannot be mapped on a core destination.
    */
   public AbstractDestination getDestination(Destination dest)
      throws JMSException
   {
      JBossDestination d = (JBossDestination)dest;
      String name = d.getName();
      boolean isQueue = d.isQueue();

      if (!d.isTemporary())
      {
         try
         {
            ic.lookup("/messaging/" + (isQueue ? "queues/" : "topics/") + name);
         }
         catch (NamingException e)
         {
            throw new JMSException("Destination " + name + " is not bound in JNDI");
         }
      }
      
      AbstractDestination ad = isQueue ?
                               (AbstractDestination)queues.get(name) :
                               (AbstractDestination)topics.get(name);
                               
      return ad;
   }

   /**
    * Add a JMS Deestination to the manager
    * 
    * @param dest The JMS destination to add
    * @throws JMSException If the destination with that name already exists in the manager
    */
   public void addDestination(Destination dest)
      throws JMSException
   {
      JBossDestination d = (JBossDestination)dest;
      String name = d.getName();
      boolean isQueue = d.isQueue();
      
      AbstractDestination ad = getDestination(dest);
      if (ad != null)
      {
         throw new JMSException("Destination with name:" + name + " already exists");
      }
      
      // TODO I am using LocalQueues for the time being, switch to distributed Queues
      if (isQueue)
      {
         ad = new LocalQueue(name);

         ad.setAcknowledgmentStore(serverPeer.getAcknowledgmentStore());
         ad.setMessageStore(serverPeer.getMessageStore());
         queues.put(name, ad);
      }
      else
      {
         // TODO I am using LocalTopics for the time being, switch to distributed Topics
         ad = new LocalTopic(name);
         topics.put(name, ad);
      }

      // make the destination transactional if there is a transaction manager available
      ad.setTransactionManager(serverPeer.getTransactionManager()); 
   }
   
   /**
    * Remove a JMS Destination from the manager
    * 
    * @param name The name of the JMS Destination to remove
    * @throws JMSException If there is no such destination to remove
    */
   public void removeDestination(String name)
      throws JMSException
   {
      Object removed = queues.remove(name);
      
      if (removed == null)
      {
         removed = topics.remove(name);
      }
      
      if (removed == null)
      {
         throw new JMSException("Cannot find destination:" + name + " to remove");
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}





