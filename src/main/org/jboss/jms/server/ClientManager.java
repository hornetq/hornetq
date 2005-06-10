/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server;

import org.jboss.jms.server.endpoint.ServerConnectionDelegate;
import org.jboss.jms.server.endpoint.ServerConnectionDelegate;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;

/**
 * Manages client connections. There is a single ClientManager instance for each server peer.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ClientManager
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private int clientIDCounter;

   protected ServerPeer serverPeer;
   protected Map connections;

   // Constructors --------------------------------------------------

   public ClientManager(ServerPeer serverPeer)
   {
      this.serverPeer = serverPeer;
      connections = new HashMap();
      clientIDCounter = 0;
   }

   // Public --------------------------------------------------------

   public ServerConnectionDelegate putConnectionDelegate(String clientID,
                                                         ServerConnectionDelegate d)
   {
      synchronized(connections)
      {
         return (ServerConnectionDelegate)connections.put(clientID, d);
      }
   }

   public ServerConnectionDelegate getConnectionDelegate(String clientID)
   {
      synchronized(connections)
      {
         return (ServerConnectionDelegate)connections.get(clientID);
      }
   }

   /**
    * @return the active connections clientIDs (as Strings)
    */
   public Set getConnections()
   {
      synchronized(connections)
      {
         return connections.keySet();
      }
   }


   /**
    * Generates a clientID that is unique per this ClientManager instance
    */
   public String generateClientID()
   {
      int id;
      synchronized(this)
      {
         id = clientIDCounter++;
      }
      return serverPeer.getServerPeerID() + "-Connection" + id;
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
