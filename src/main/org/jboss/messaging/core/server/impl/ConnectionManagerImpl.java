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
package org.jboss.messaging.core.server.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.jboss.messaging.core.client.FailureListener;
import org.jboss.messaging.core.client.impl.JMSClientVMIdentifier;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.RemotingException;
import org.jboss.messaging.core.server.ConnectionManager;
import org.jboss.messaging.core.server.ServerConnection;

/**
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision: 3778 $</tt>
 *
 * $Id: ConnectionManagerImpl.java 3778 2008-02-24 12:15:29Z timfox $
 */
public class ConnectionManagerImpl implements ConnectionManager, FailureListener
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ConnectionManagerImpl.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private Map<Long /* remoting session ID */, List<ServerConnection>> endpoints;

   private Set<ServerConnection> activeServerConnections;

   // the clients maps is for information only: to better identify the clients of
   // jboss messaging using their VM ID
   private Map<Long /* remoting session id */, String /* client vm id */> clients;
   
   // Constructors ---------------------------------------------------------------------------------

   public ConnectionManagerImpl()
   {
      endpoints = new HashMap<Long, List<ServerConnection>>();
      activeServerConnections = new HashSet<ServerConnection>();
      clients = new HashMap<Long, String>();
   }

   // ConnectionManager implementation -------------------------------------------------------------

   public synchronized void registerConnection(String clientVMID, long remotingClientSessionID,
         ServerConnection endpoint)
   {    
      List<ServerConnection> connectionEndpoints = endpoints.get(remotingClientSessionID);

      if (connectionEndpoints == null)
      {
         connectionEndpoints = new ArrayList<ServerConnection>();
         endpoints.put(remotingClientSessionID, connectionEndpoints);
      }

      connectionEndpoints.add(endpoint);

      activeServerConnections.add(endpoint);

      clients.put(remotingClientSessionID, clientVMID);

      log.debug("registered connection " + endpoint + " as " + remotingClientSessionID);
   }
   
   public synchronized ServerConnection unregisterConnection(long remotingClientSessionID,
         ServerConnection endpoint)
   {
      List<ServerConnection> connectionEndpoints = endpoints.get(remotingClientSessionID);

      if (connectionEndpoints != null)
      {
         boolean removed = connectionEndpoints.remove(endpoint);

         if (removed)
         {
            activeServerConnections.remove(endpoint);
         }

         log.debug("unregistered connection " + endpoint + " with remoting session ID " + remotingClientSessionID);

         if (connectionEndpoints.isEmpty())
         {
            endpoints.remove(remotingClientSessionID);           
            clients.remove(remotingClientSessionID);
         }

         return endpoint;
      }
      return null;
   }
   
   public synchronized List<ServerConnection> getActiveConnections()
   {
      // I will make a copy to avoid ConcurrentModification
      List<ServerConnection> list = new ArrayList<ServerConnection>();
      list.addAll(activeServerConnections);
      return list;
   }      
      
   // MessagingComponent implementation ------------------------------------------------------------
   
   public void start() throws Exception
   {
      //NOOP
   }
   
   public void stop() throws Exception
   {
      //NOOP
   }

   // FailureListener implementation --------------------------------------------------------------
   
   public void onFailure(MessagingException me)
   {
      if (me instanceof RemotingException)
      {
         RemotingException re = (RemotingException) me;
         handleClientFailure(re.getSessionID(), true);
      }
   }
   
   // Public ---------------------------------------------------------------------------------------

   public String toString()
   {
      return "ConnectionManager[" + Integer.toHexString(hashCode()) + "]";
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   /**
    * @param clientToServer - true if the failure has been detected on a direct connection from
    *        client to this server, false if the failure has been detected while trying to send a
    *        callback from this server to the client.
    */
   private synchronized void handleClientFailure(long remotingSessionID, boolean clientToServer)
   {
      String clientVMID = clients.get(remotingSessionID);

      if (clientVMID == null)
      {
         return;
      }
      
      log.warn("A problem has been detected " +
         (clientToServer ?
            "with the connection to remote client ":
            "trying to send a message to remote client ") +
         remotingSessionID + ", client VM ID=" + clientVMID + ". It is possible the client has exited without closing " +
         "its connection(s) or the network has failed. All connection resources " +
         "corresponding to that client process will now be removed.");

      closeConsumers(remotingSessionID);
      
      dump();
   }

   private synchronized void closeConsumers(long remotingClientSessionID)
   {
      List<ServerConnection> connectionEndpoints = endpoints.get(remotingClientSessionID);
      // the connection endpoints are copied in a new list to avoid concurrent modification exception
      List<ServerConnection> copy;
      if (connectionEndpoints != null)
         copy = new ArrayList<ServerConnection>(connectionEndpoints);
      else
         copy = new ArrayList<ServerConnection>();
         
      for (ServerConnection sce : copy)
      {
         try
         {
            log.debug("clearing up state for connection " + sce);
            sce.close();
            log.debug("cleared up state for connection " + sce);
         }
         catch (Exception e)
         {
            log.error("Failed to close connection", e);
         }          
      }
   }
   
   private void dump()
   {
      if (log.isDebugEnabled())
      {
         StringBuffer buff = new StringBuffer("*********** Dumping connections\n");
         buff.append("this client VM ID: ").append(JMSClientVMIdentifier.instance).append("\n");
         buff.append("remoting session ID <----> client VM ID:\n");
         if (clients.size() == 0)
         {
            buff.append("    No registered sessions\n");
         }
         for (Entry<Long, String> client : clients.entrySet())
         {
            long remotingSessionID = client.getKey();
            String clientVMID = client.getValue();
            buff.append("    ").append(remotingSessionID).append(" <----> ").append(clientVMID).append("\n");
         }
         buff.append("remoting session ID -----> server connection endpoints:\n");
         if (endpoints.size() == 0)
         {
            buff.append("    No registered endpoints\n");
         }
         for (Entry<Long, List<ServerConnection>> entry : endpoints.entrySet())
         {
            List<ServerConnection> connectionEndpoints = entry.getValue();
            buff.append("    "  + entry.getKey() + "----->\n");
            for (ServerConnection sce : connectionEndpoints)
            {
               buff.append("        " + sce + " (" + System.identityHashCode(sce) + ")\n");
            }
         }
         buff.append("*** Dumped connections");
         
         log.debug(buff);
      }
   }
   
   // Inner classes --------------------------------------------------------------------------------

}
