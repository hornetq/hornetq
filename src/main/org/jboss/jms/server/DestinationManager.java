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
import javax.jms.Destination;
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
   protected Map topics;
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
   public AbstractDestination getDestination(Destination dest) throws Exception
   {
      JBossDestination d = (JBossDestination)dest;
      String name = d.getName();
      boolean isQueue = d.isQueue();

      ic.lookup("/messaging/" + (isQueue ? "queues/" : "topics/") + name);

      AbstractDestination ad = isQueue ?
                               (AbstractDestination)queues.get(name) :
                               (AbstractDestination)topics.get(name);

      if (ad == null)
      {
         // TODO I am using LocalQueues for the time being, switch to distributed Queues
         if (isQueue)
         {
            ad = new LocalQueue(name);
            queues.put(name, ad);
         }
         else
         {
            // TODO I am using LocalTopics for the time being, switch to distributed Topics
            ad = new LocalTopic(name);
            topics.put(name, ad);
         }
      }
      return ad;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}





