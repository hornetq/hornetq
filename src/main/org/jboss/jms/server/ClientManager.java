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

   private int connectionIDCounter;

   protected ServerPeer serverPeer;
   protected Map connections;
   protected Map subscriptions;

   // Constructors --------------------------------------------------

   public ClientManager(ServerPeer serverPeer)
   {
      this.serverPeer = serverPeer;
      connections = new ConcurrentReaderHashMap();
      subscriptions = new ConcurrentReaderHashMap();
      connectionIDCounter = 0;
   }

   // Public --------------------------------------------------------

   public ServerConnectionDelegate putConnectionDelegate(String clientID,
                                                         ServerConnectionDelegate d)
   {
      return (ServerConnectionDelegate)connections.put(clientID, d);
   }

   public ServerConnectionDelegate getConnectionDelegate(String clientID)
   {
      return (ServerConnectionDelegate)connections.get(clientID);
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
   

   /**
    * @return the active connections clientIDs (as Strings)
    */
   public Set getConnections()
   {
      return connections.keySet();
   }


   /**
    * Generates a clientID that is unique per this ClientManager instance
    */
   /*
   public String generateConnectionID()
   {
      int id;
      synchronized(this)
      {
         id = connectionIDCounter++;
      }
      return serverPeer.getServerPeerID() + "-Connection" + id;
   }
   */

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
