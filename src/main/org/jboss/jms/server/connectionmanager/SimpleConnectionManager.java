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
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.JMSException;

import org.jboss.jms.server.ConnectionManager;
import org.jboss.jms.server.endpoint.ConnectionEndpoint;
import org.jboss.messaging.util.Logger;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.util.ConcurrentHashSet;
import org.jboss.messaging.util.Util;

/**
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class SimpleConnectionManager implements ConnectionManager
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(SimpleConnectionManager.class);

   // Static ---------------------------------------------------------------------------------------

   private static boolean trace = log.isTraceEnabled();

   // Attributes -----------------------------------------------------------------------------------

   private Map</** VMID */String, Map</** RemoteSessionID */String, ConnectionEndpoint>> jmsClients;

   // Map<remotingClientSessionID<String> - jmsClientVMID<String>
   private Map<String, String> remotingSessions;

   // Set<ConnectionEndpoint>
   private Set<ConnectionEndpoint> activeConnectionEndpoints;

   private Map</** CFUniqueName*/ String, ConnectionFactoryCallbackInformation> cfCallbackInfo;
   
   // Constructors ---------------------------------------------------------------------------------

   public SimpleConnectionManager()
   {
      jmsClients = new HashMap<String, Map<String, ConnectionEndpoint>>();
      remotingSessions = new HashMap<String, String>();
      activeConnectionEndpoints = new HashSet<ConnectionEndpoint>();
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
         endpoints = new HashMap<String, ConnectionEndpoint>();
         
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
      List<ConnectionEndpoint> list = new ArrayList<ConnectionEndpoint>();
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
         remotingSessionID + ", jmsClientID=" + jmsClientID + ". It is possible the client has exited without closing " +
         "its connection(s) or the network has failed. All connection resources " +
         "corresponding to that client process will now be removed.");

      closeConsumersForClientVMID(jmsClientID);
   }
   
   /** Synchronized is not really needed.. just to be safe as this is not supposed to be highly contended */
   public void addConnectionFactoryCallback(String uniqueName, String vmID,
         String remotingSessionID, PacketSender sender)
   {
      remotingSessions.put(remotingSessionID, vmID);
      getCFInfo(uniqueName).addClient(vmID, sender);      
   }
   /** Synchronized is not really needed.. just to be safe as this is not supposed to be highly contended */
   public synchronized void removeConnectionFactoryCallback(String uniqueName, String vmid,
         PacketSender sender)
   {
      getCFInfo(uniqueName).removeSender(vmid, sender);   
   }
   
   /** Synchronized is not really needed.. just to be safe as this is not supposed to be highly contended */
   public synchronized PacketSender[] getConnectionFactorySenders(String uniqueName)
   {
      return getCFInfo(uniqueName).getAllSenders();
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
      if (jmsClientID == null)
      {
         return;
      }
      // Remoting only provides one pinger per invoker, not per connection therefore when the pinger
      // dies we must close ALL connections corresponding to that jms client ID.

      Map<String, ConnectionEndpoint> endpoints = jmsClients.get(jmsClientID);

      if (endpoints != null)
      {
         List<ConnectionEndpoint> sces = new ArrayList<ConnectionEndpoint>();

         for (Map.Entry<String, ConnectionEndpoint> entry: endpoints.entrySet())
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
               sce.closing(-1);
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
         PacketSender[] senders = cfInfo.getAllSenders(jmsClientID);
         for (PacketSender sender: senders)
         {
            cfInfo.removeSender(jmsClientID, sender);
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
      Map</**VMID */ String , /** Active clients*/ConcurrentHashSet<PacketSender>> clientSendersByVM;
      ConcurrentHashSet<PacketSender> clientSenders;


      public ConnectionFactoryCallbackInformation(String uniqueName)
      {
         this.uniqueName = uniqueName;
         this.clientSendersByVM = new ConcurrentHashMap<String, ConcurrentHashSet<PacketSender>>();
         this.clientSenders = new ConcurrentHashSet<PacketSender>();
      }

      public void addClient(String vmID, PacketSender sender)
      {
         clientSenders.add(sender);
         getSendersList(vmID).add(sender);
      }
      
      public PacketSender[] getAllSenders(String vmID)
      {
         Set<PacketSender> list = getSendersList(vmID);
         return (PacketSender[]) list.toArray(new PacketSender[list.size()]);
      }

      public PacketSender[] getAllSenders()
      {
         return (PacketSender[]) clientSenders.toArray(new PacketSender[clientSenders.size()]);
      }

      public void removeSender(String vmID, PacketSender sender)
      {
         clientSenders.remove(sender);
         getSendersList(vmID).remove(sender);
      }
      
      private ConcurrentHashSet<PacketSender> getSendersList(String vmID)
      {
         ConcurrentHashSet<PacketSender> perVMList = clientSendersByVM.get(vmID);
         if (perVMList == null)
         {
            perVMList = new ConcurrentHashSet<PacketSender>();
            clientSendersByVM.put(vmID, perVMList);
            perVMList = clientSendersByVM.get(vmID);
         }
         return perVMList;
      }
   }
   
   private void dump()
   {
   	log.debug("***********Dumping conn map");
   	for (Iterator iter = jmsClients.entrySet().iterator(); iter.hasNext(); )
   	{
   		Map.Entry entry = (Map.Entry)iter.next();
   		
   		String jmsClientVMID = (String)entry.getKey();
   		
   		Map endpoints = (Map)entry.getValue();
   		
   		log.debug(jmsClientVMID + "----->");
   		
   		for (Iterator iter2 = endpoints.entrySet().iterator(); iter2.hasNext(); )
      	{
   			Map.Entry entry2 = (Map.Entry)iter2.next();
   			
   			String sessionID = (String)entry2.getKey();
   			
   			ConnectionEndpoint endpoint = (ConnectionEndpoint)entry2.getValue();
   			
   			log.debug("            " + sessionID + "------>" + System.identityHashCode(endpoint));
      	}
   	}
   	log.debug("*** Dumped conn map");
   }

}
