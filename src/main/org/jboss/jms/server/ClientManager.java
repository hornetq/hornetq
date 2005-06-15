/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server;

import org.jboss.jms.server.endpoint.ServerConnectionDelegate;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.io.Serializable;

/**
 * Manages client connections. There is a single ClientManager instance for each server peer.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ClientManager
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected ServerPeer serverPeer;
   protected Map connections;
   protected Map subscriptions;

   // Constructors --------------------------------------------------

   public ClientManager(ServerPeer serverPeer)
   {
      this.serverPeer = serverPeer;
      connections = new ConcurrentReaderHashMap();
      subscriptions = new ConcurrentReaderHashMap();
   }

   // Public --------------------------------------------------------

   public ServerConnectionDelegate putConnectionDelegate(Serializable connectionID,
                                                         ServerConnectionDelegate d)
   {
      return (ServerConnectionDelegate)connections.put(connectionID, d);
   }

   public ServerConnectionDelegate getConnectionDelegate(Serializable connectionID)
   {
      return (ServerConnectionDelegate)connections.get(connectionID);
   }
   
   public Set getDurableSubscriptions(String clientID)
   {
      Map subs = (Map)subscriptions.get(clientID);
      return subs != null ? new HashSet(subs.values()) : Collections.EMPTY_SET;
   }
   
   public DurableSubscriptionHolder getDurableSubscription(String clientID, String subscriptionName)
   {
      Map subs = (Map)subscriptions.get(clientID);
      return subs == null ? null : (DurableSubscriptionHolder)subs.get(subscriptionName);
   }
   
   public void addDurableSubscription(String clientID, String subscriptionName,
                                      DurableSubscriptionHolder subscription)
   {
      Map subs = (Map)subscriptions.get(clientID);
      if (subs == null)
      {
         subs = new ConcurrentReaderHashMap();
         subscriptions.put(clientID, subs);
      }
      subs.put(subscriptionName, subscription);
   }
   
   public DurableSubscriptionHolder removeDurableSubscription(String clientID,
                                                              String subscriptionName)
   {
      Map subs = (Map)subscriptions.get(clientID);
      
      if (subs == null) return null;
      
      DurableSubscriptionHolder removed = (DurableSubscriptionHolder)subs.remove(subscriptionName);
      
      if (subs.size() == 0)
      {
         subscriptions.remove(clientID);
      }
      
      return removed;
   }
   

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
