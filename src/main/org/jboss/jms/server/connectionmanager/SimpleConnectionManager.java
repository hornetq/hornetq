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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jms.JMSException;

import org.jboss.jms.delegate.ConnectionEndpoint;
import org.jboss.jms.server.ConnectionManager;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.contract.ClusterNotification;
import org.jboss.messaging.core.contract.ClusterNotificationListener;
import org.jboss.messaging.core.contract.Replicator;
import org.jboss.messaging.util.Util;
import org.jboss.remoting.Client;
import org.jboss.remoting.ClientDisconnectedException;
import org.jboss.remoting.ConnectionListener;

/**
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ovidiu@jboss.org">Ovidiu Feodorov</a>
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

   // Map<jmsClientVMID<String> - Map<remotingClientSessionID<String> - ConnectionEndpoint>>
   private Map jmsClients;

   // Map<remotingClientSessionID<String> - jmsClientVMID<String>
   private Map remotingSessions;

   // Set<ConnectionEndpoint>
   private Set activeConnectionEndpoints;
   
   private Replicator replicator;

   // Constructors ---------------------------------------------------------------------------------

   public SimpleConnectionManager()
   {
      jmsClients = new HashMap();
      remotingSessions = new HashMap();
      activeConnectionEndpoints = new HashSet();
   }

   // ConnectionManager implementation -------------------------------------------------------------

   public synchronized void registerConnection(String jmsClientVMID,
                                               String remotingClientSessionID,
                                               ConnectionEndpoint endpoint)
   {    
      Map endpoints = (Map)jmsClients.get(jmsClientVMID);
      
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
      Map endpoints = (Map)jmsClients.get(jmsClientVMId);
      
      if (endpoints != null)
      {
         ConnectionEndpoint e = (ConnectionEndpoint)endpoints.remove(remotingClientSessionID);

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

      if (jmsClientID != null)
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
   
   // ClusterNotificationListener implementation ---------------------------------------------------


   /**
    * Closing connections that are coming from a failed node
    * @param notification
    */
   public void notify(ClusterNotification notification)
	{
		if (notification.type == ClusterNotification.TYPE_FAILOVER_START)
		{

         log.trace("SimpleConnectionManager was notified about FailoverStart from node " +
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
   
   private synchronized void closeConsumersForClientVMID(String jmsClientID)
   {
   	// Remoting only provides one pinger per invoker, not per connection therefore when the pinger
      // dies we must close ALL connections corresponding to that jms client ID.

      Map endpoints = (Map)jmsClients.get(jmsClientID);

      if (endpoints != null)
      {
         List sces = new ArrayList();

         for(Iterator i = endpoints.entrySet().iterator(); i.hasNext(); )
         {
            Map.Entry entry = (Map.Entry)i.next();
            ConnectionEndpoint sce = (ConnectionEndpoint)entry.getValue();
            sces.add(sce);
         }

         // Now close the end points - this will result in a callback into unregisterConnection
         // to remove the data from the jmsClients and sessions maps.
         // Note we do this outside the loop to prevent ConcurrentModificationException

         for(Iterator i = sces.iterator(); i.hasNext(); )
         {
            ConnectionEndpoint sce = (ConnectionEndpoint)i.next();

            try
            {
      			log.debug("clearing up state for connection " + sce);
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
   }

   // Inner classes --------------------------------------------------------------------------------

}
