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
package org.jboss.jms.server.connectionmanager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.JMSException;

import org.jboss.jms.delegate.ConnectionEndpoint;
import org.jboss.jms.server.ConnectionManager;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.contract.ClusterNotification;
import org.jboss.messaging.core.contract.ClusterNotificationListener;
import org.jboss.messaging.core.contract.Replicator;
import org.jboss.messaging.util.Util;
import org.jboss.messaging.util.ConcurrentHashSet;
import org.jboss.remoting.Client;
import org.jboss.remoting.ClientDisconnectedException;
import org.jboss.remoting.ConnectionListener;
import org.jboss.remoting.callback.ServerInvokerCallbackHandler;

/**
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class SimpleConnectionManager implements ConnectionManager, ConnectionListener, ClusterNotificationListener
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(SimpleConnectionManager.class);

   // Static ---------------------------------------------------------------------------------------

   private static boolean trace = log.isTraceEnabled();

   // Attributes -----------------------------------------------------------------------------------

   private Map</** VMID */String, Map</** RemoteSessionID */String, ConnectionEndpoint>> jmsClients;

   // Map<remotingClientSessionID<String> - jmsClientVMID<String>
   private Map remotingSessions;

   // Set<ConnectionEndpoint>
   private Set activeConnectionEndpoints;

   private Map</** CFUniqueName*/ String, ConnectionFactoryCallbackInformation> cfCallbackInfo;
   
   private Replicator replicator;

   // Constructors ---------------------------------------------------------------------------------

   public SimpleConnectionManager()
   {
      jmsClients = new HashMap();
      remotingSessions = new HashMap();
      activeConnectionEndpoints = new HashSet();
      cfCallbackInfo = new ConcurrentHashMap<String, ConnectionFactoryCallbackInformation>();
   }

   // ConnectionManager implementation -------------------------------------------------------------

   public synchronized void registerConnection(String jmsClientVMID,
                                               String remotingClientSessionID,
                                               ConnectionEndpoint endpoint)
   {    
      Map<String, ConnectionEndpoint> endpoints = jmsClients.get(jmsClientVMID);
      
      if (endpoints == null)
      {
         endpoints = new HashMap();
         jmsClients.put(jmsClientVMID, endpoints);
      }
      
      endpoints.put(remotingClientSessionID, endpoint);
      
      remotingSessions.put(remotingClientSessionID, jmsClientVMID);

      activeConnectionEndpoints.add(endpoint);
      
      log.debug("registered connection " + endpoint + " as " +
                Util.guidToString(remotingClientSessionID));
   }

   public synchronized ConnectionEndpoint unregisterConnection(String jmsClientVMId,
                                                               String remotingClientSessionID)
   {
      Map<String, ConnectionEndpoint> endpoints = jmsClients.get(jmsClientVMId);
      
      if (endpoints != null)
      {
         ConnectionEndpoint e = endpoints.remove(remotingClientSessionID);

         if (e != null)
         {
            endpoints.remove(e);
            activeConnectionEndpoints.remove(e);
         }

         log.debug("unregistered connection " + e + " with remoting session ID " +
               Util.guidToString(remotingClientSessionID));
         
         if (endpoints.isEmpty())
         {
            jmsClients.remove(jmsClientVMId);
         }
         
         remotingSessions.remove(remotingClientSessionID);
         
         return e;
      }
      return null;
   }
   
   public synchronized List getActiveConnections()
   {
      // I will make a copy to avoid ConcurrentModification
      ArrayList list = new ArrayList();
      list.addAll(activeConnectionEndpoints);
      return list;
   }
      
   public synchronized void handleClientFailure(String remotingSessionID, boolean clientToServer)
   {
      String jmsClientID = (String)remotingSessions.get(remotingSessionID);

      if (jmsClientID == null)
      {
         log.warn(this + " cannot look up remoting session ID " + remotingSessionID);
      }

      log.warn("A problem has been detected " +
         (clientToServer ?
            "with the connection to remote client ":
            "trying to send a message to remote client ") +
         remotingSessionID + ". It is possible the client has exited without closing " +
         "its connection(s) or there is a network problem. All connection resources " +
         "corresponding to that client process will now be removed.");

      closeConsumersForClientVMID(jmsClientID);
   }
   
   // ConnectionListener implementation ------------------------------------------------------------

   /**
    * Be aware that ConnectionNotifier uses to call this method with null Throwables.
    *
    * @param t - plan for it to be null!
    */
   public void handleConnectionException(Throwable t, Client client)
   {  
      if (t instanceof ClientDisconnectedException)
      {
         // This is OK
         if (trace) { log.trace(this + " notified that client " + client + " has disconnected"); }
         return;
      }
      else
      {
         if (trace) { log.trace(this + " detected failure on client " + client, t); }
      }

      String remotingSessionID = client.getSessionId();
      
      if (remotingSessionID != null)
      {
         handleClientFailure(remotingSessionID, true);
      }
   }

   /** Synchronized is not really needed.. just to be safe as this is not supposed to be highly contended */
   public synchronized void addConnectionFactoryCallback(String uniqueName, String JVMID, ServerInvokerCallbackHandler handler)
   {
      getCFInfo(uniqueName).addClient(JVMID, handler);
   }

   /** Synchronized is not really needed.. just to be safe as this is not supposed to be highly contended */
   public synchronized void removeConnectionFactoryCallback(String uniqueName, String JVMID, ServerInvokerCallbackHandler handler)
   {
      getCFInfo(uniqueName).removeHandler(JVMID, handler);
   }

   /** Synchronized is not really needed.. just to be safe as this is not supposed to be highly contended */
   public synchronized ServerInvokerCallbackHandler[] getConnectionFactoryCallback(String uniqueName)
   {
      return getCFInfo(uniqueName).getAllHandlers();
   }

   // ClusterNotificationListener implementation ---------------------------------------------------


   /**
    * Closing connections that are coming from a failed node
    * @param notification
    */
   public void notify(ClusterNotification notification)
	{	
		if (notification.type == ClusterNotification.TYPE_NODE_LEAVE)
		{

         log.trace("SimpleConnectionManager was notified about node leaving from node " +
                    notification.nodeID);
         try
			{
				//We remove any consumers with the same JVMID as the node that just failed
				//This will remove any message suckers from a failed node
				//This is important to workaround a remoting bug where sending messages down a broken connection
				//can cause a deadlock with the bisocket transport
				
				//Get the jvm id for the failed node
				
				Map ids = replicator.get(Replicator.JVM_ID_KEY);
				
				if (ids == null)
				{
               log.trace("Cannot find jvmid map");
					throw new IllegalStateException("Cannot find jvmid map");
				}
				
				int failedNodeID = notification.nodeID;
				
				String clientVMID = (String)ids.get(new Integer(failedNodeID));
				
				if (clientVMID == null)
				{
               log.error("Cannot find ClientVMID for failed node " + failedNodeID);
					throw new IllegalStateException("Cannot find clientVMID for failed node " + failedNodeID);
				}
				
				//Close the consumers corresponding to that VM

            log.trace("Closing consumers for clientVMID=" + clientVMID);

            closeConsumersForClientVMID(clientVMID);
			}
			catch (Exception e)
			{
				log.error("Failed to process failover start", e);
			}
		}		
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

   // Public ---------------------------------------------------------------------------------------

   /*
    * Used in testing only
    */
   public synchronized boolean containsRemotingSession(String remotingClientSessionID)
   {
      return remotingSessions.containsKey(remotingClientSessionID);
   }

   /*
    * Used in testing only
    */
   public synchronized Map getClients()
   {
      return Collections.unmodifiableMap(jmsClients);
   }
   
   public void injectReplicator(Replicator replicator)
   {
   	this.replicator = replicator;
   }


   public String toString()
   {
      return "ConnectionManager[" + Integer.toHexString(hashCode()) + "]";
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   private ConnectionFactoryCallbackInformation getCFInfo(String uniqueName)
   {
      ConnectionFactoryCallbackInformation callback = cfCallbackInfo.get(uniqueName);
      if (callback == null)
      {
         callback = new ConnectionFactoryCallbackInformation(uniqueName);
         cfCallbackInfo.put(uniqueName, callback);
         callback = cfCallbackInfo.get(uniqueName);
      }
      return callback;
   }


   private synchronized void closeConsumersForClientVMID(String jmsClientID)
   {
   	// Remoting only provides one pinger per invoker, not per connection therefore when the pinger
      // dies we must close ALL connections corresponding to that jms client ID.

      Map<String, ConnectionEndpoint> endpoints = jmsClients.get(jmsClientID);

      if (endpoints != null)
      {
         List<ConnectionEndpoint> sces = new ArrayList();

         for(Map.Entry<String, ConnectionEndpoint> entry: endpoints.entrySet())
         {
            ConnectionEndpoint sce = entry.getValue();
            sces.add(sce);
         }

         // Now close the end points - this will result in a callback into unregisterConnection
         // to remove the data from the jmsClients and sessions maps.
         // Note we do this outside the loop to prevent ConcurrentModificationException

         for(ConnectionEndpoint sce: sces )
         {
            try
            {
      			log.debug("clPearing up state for connection " + sce);
               sce.closing();
               sce.close();
               log.debug("cleared up state for connection " + sce);
            }
            catch (JMSException e)
            {
               log.error("Failed to close connection", e);
            }          
         }
      }

      for (ConnectionFactoryCallbackInformation cfInfo: cfCallbackInfo.values())
      {
         ServerInvokerCallbackHandler[] handlers = cfInfo.getAllHandlers(jmsClientID);
         for (ServerInvokerCallbackHandler handler: handlers)
         {
            try
            {
               handler.getCallbackClient().disconnect();
            }
            catch (Throwable e)
            {
               log.warn (e, e);
            }

            try
            {
               handler.destroy();
            }
            catch (Throwable e)
            {
               log.warn (e, e);
            }

            cfInfo.removeHandler(jmsClientID, handler);
         }

      }

   }

   // Inner classes --------------------------------------------------------------------------------

   /** Class used to organize Callbacks on ClusteredConnectionFactories */
   static class ConnectionFactoryCallbackInformation
   {

      // We keep two lists, one containing all clients a CF will have to maintain and another
      //   organized by JVMId as we will need that organization when cleaning up dead clients
      String uniqueName;
      Map</**VMID */ String , /** Active clients*/ConcurrentHashSet<ServerInvokerCallbackHandler>> clientHandlersByVM;
      ConcurrentHashSet<ServerInvokerCallbackHandler> clientHandlers;


      public ConnectionFactoryCallbackInformation(String uniqueName)
      {
         this.uniqueName = uniqueName;
         this.clientHandlersByVM = new ConcurrentHashMap<String, ConcurrentHashSet<ServerInvokerCallbackHandler>>();
         this.clientHandlers = new ConcurrentHashSet<ServerInvokerCallbackHandler>();
      }

      public void addClient(String vmID, ServerInvokerCallbackHandler handler)
      {
         clientHandlers.add(handler);
         getHandlersList(vmID).add(handler);
      }

      public ServerInvokerCallbackHandler[] getAllHandlers(String vmID)
      {
         Set<ServerInvokerCallbackHandler> list = getHandlersList(vmID);
         ServerInvokerCallbackHandler[] array = new ServerInvokerCallbackHandler[list.size()];
         return (ServerInvokerCallbackHandler[])list.toArray(array);
      }

      public ServerInvokerCallbackHandler[] getAllHandlers()
      {
         ServerInvokerCallbackHandler[] array = new ServerInvokerCallbackHandler[clientHandlers.size()];
         return (ServerInvokerCallbackHandler[])clientHandlers.toArray(array);
      }

      public void removeHandler(String vmID, ServerInvokerCallbackHandler handler)
      {
         clientHandlers.remove(handler);
         getHandlersList(vmID).remove(handler);
      }

      private ConcurrentHashSet<ServerInvokerCallbackHandler> getHandlersList(String vmID)
      {
         ConcurrentHashSet<ServerInvokerCallbackHandler> perVMList = clientHandlersByVM.get(vmID);
         if (perVMList == null)
         {
            perVMList = new ConcurrentHashSet<ServerInvokerCallbackHandler>();
            clientHandlersByVM.put(vmID, perVMList);
            perVMList = clientHandlersByVM.get(vmID);
         }
         return perVMList;
      }

   }

}
