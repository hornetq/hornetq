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
package org.jboss.jms.server.connectionfactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.jms.client.delegate.ClientConnectionFactoryDelegate;
import org.jboss.jms.client.delegate.ClusteredClientConnectionFactoryDelegate;
import org.jboss.jms.server.ConnectionFactoryManager;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.Version;
import org.jboss.jms.server.endpoint.ServerConnectionFactoryEndpoint;
import org.jboss.jms.server.endpoint.advised.ConnectionFactoryAdvised;
import org.jboss.jms.server.remoting.JMSDispatcher;
import org.jboss.jms.util.JNDIUtil;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.plugin.contract.FailoverMapper;
import org.jboss.messaging.core.plugin.contract.ReplicationListener;
import org.jboss.messaging.core.plugin.contract.Replicator;
import org.jboss.messaging.core.plugin.postoffice.cluster.DefaultClusteredPostOffice;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ConnectionFactoryJNDIMapper
   implements ConnectionFactoryManager, ReplicationListener
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(ConnectionFactoryJNDIMapper.class);
   
   // Static --------------------------------------------------------
   
   private static final String CF_PREFIX = "CF_";
   
   // Attributes ----------------------------------------------------
   
   protected Context initialContext;
   protected ServerPeer serverPeer;

   // Map<uniqueName<String> - ServerConnectionFactoryEndpoint>
   protected Map endpoints;

   // Map<uniqueName<String> - ClientConnectionFactoryDelegate> (not just clustered delegates)
   protected Map delegates;
   
   protected Replicator replicator;
   
   /*
   We cache the map of node->failover node in here.
   This is then updated when node joins or leaves the cluster via the replicationListener
   When new cfs are deployed we use the cached map
   */
   protected Map failoverMap;
   
   // Constructors --------------------------------------------------
   
   public ConnectionFactoryJNDIMapper(ServerPeer serverPeer) throws Exception
   {
      this.serverPeer = serverPeer;
      endpoints = new HashMap();
      delegates = new HashMap();
   }
   
   // ConnectionFactoryManager implementation -----------------------

   public synchronized void registerConnectionFactory(String uniqueName,
                                                      String clientID,
                                                      JNDIBindings jndiBindings,
                                                      String locatorURI,
                                                      boolean clientPing,
                                                      int prefetchSize,
                                                      int defaultTempQueueFullSize,
                                                      int defaultTempQueuePageSize,
                                                      int defaultTempQueueDownCacheSize,
                                                      boolean clustered)
      throws Exception
   {
      log.info("Registering connection factory with name " + uniqueName + " bindings " + jndiBindings);
      
      //Sanity check
      if (delegates.containsKey(uniqueName))
      {
         throw new IllegalArgumentException("There's already a connection factory registered with name " + uniqueName);
      }
      
      int id = serverPeer.getNextObjectID();
      
      Version version = serverPeer.getVersion();

      ServerConnectionFactoryEndpoint endpoint =
         new ServerConnectionFactoryEndpoint(id, serverPeer, clientID,
                                             jndiBindings, prefetchSize,
                                             defaultTempQueueFullSize,
                                             defaultTempQueuePageSize,
                                             defaultTempQueueDownCacheSize);
      endpoints.put(uniqueName, endpoint);
      
      ClientConnectionFactoryDelegate delegate = null;

      if (clustered)
      {
         setupReplicator();
      }
      else
      {
         log.info("ConnectionFactoryJNDIMapper is non clustered");
      }
      
      boolean creatingClustered = clustered && replicator != null;
      
      ClientConnectionFactoryDelegate localDelegate =
         new ClientConnectionFactoryDelegate(id, serverPeer.getServerPeerID(),
                                             locatorURI, version, clientPing);
      
      /*
       * When registering a new clustered connection factory i should first create it with the available delegates
       * then send the replication message.
       * We then listen for connection factories added to global state using the replication listener
       * and then update their connection factory list.
       * This will happen locally too, so we will get the replication message locally - to avoid updating it again
       * we can ignore any "add" replication updates that originate from the current node.
       */
      
      if (creatingClustered)
      {
         //Replicate the change - we will ignore this locally
         
         replicator.put(CF_PREFIX + uniqueName, localDelegate);
         
         //Create a clustered delegate
         
         Map localDelegates = replicator.get(CF_PREFIX + uniqueName);
         
         delegate = createClusteredDelegate(localDelegates);
         
      }
      else
      {
         delegate = localDelegate;
      }
      
      log.trace("Adding delegates factory " + uniqueName + " pointing to " + delegate);
      
      delegates.put(uniqueName, delegate);

      //Now bind it in JNDI
      rebindConnectionFactory(initialContext, jndiBindings, delegate);
      
      //Registering with the dispatcher should always be the last thing otherwise a client could use
      //a partially initialised object
      JMSDispatcher.instance.registerTarget(new Integer(id), new ConnectionFactoryAdvised(endpoint));
   }
   
   public synchronized void unregisterConnectionFactory(String uniqueName, boolean clustered) throws Exception
   {
      log.trace("ConnectionFactory " + uniqueName + " being unregistered");
      ServerConnectionFactoryEndpoint endpoint =
         (ServerConnectionFactoryEndpoint)endpoints.remove(uniqueName);
      
      if (endpoint == null)
      {
         throw new IllegalArgumentException("Cannot find endpoint with name " + uniqueName);
      }
      
      JNDIBindings jndiBindings = endpoint.getJNDIBindings();
      
      if (jndiBindings != null)
      {
         List jndiNames = jndiBindings.getNames();
         for(Iterator i = jndiNames.iterator(); i.hasNext(); )
         {
            String jndiName = (String)i.next();
            initialContext.unbind(jndiName);
            log.debug(jndiName + " unregistered");
         }
      }

      log.trace("Removing delegate from delegates list with key=" + uniqueName + " at serverPeerID=" +
                  this.serverPeer.getServerPeerID());

      ClientConnectionFactoryDelegate delegate =
         (ClientConnectionFactoryDelegate)delegates.remove(uniqueName);
      
      if (delegate == null)
      {
         throw new IllegalArgumentException("Cannot find factory with name " + uniqueName);
      }
      
      if (clustered)
      {
         setupReplicator();
         
         // Remove from replicants

         if (replicator != null)
         {         
            //There may be no clustered post office deployed
            if (!replicator.remove(CF_PREFIX + uniqueName))
            {
               throw new IllegalStateException("Cannot find replicant to remove: " + CF_PREFIX + uniqueName);
            }
         }
         
      }
      
      JMSDispatcher.instance.unregisterTarget(new Integer(endpoint.getID()));
   }
   
   // MessagingComponent implementation -----------------------------
   
   public void start() throws Exception
   {
      initialContext = new InitialContext();
      
      log.debug("started");
   }
   
   public void stop() throws Exception
   {
      initialContext.close();
      
      if (replicator != null)
      {
         replicator.unregisterListener(this);
      }
      
      log.debug("stopped");
   }
   
   // ReplicationListener interface ----------------------------------
   
   public synchronized void onReplicationChange(Serializable key, Map updatedReplicantMap,
                                                boolean added, int originatingNodeId)
   {
      log.info("Got replication call " + key + " node=" + serverPeer.getServerPeerID() + " replicants=" + updatedReplicantMap + " added=");
      try
      {         
         if (!(key instanceof String))
         {
            return;
         }
         
         String sKey = (String)key;

         if (sKey.equals(DefaultClusteredPostOffice.ADDRESS_INFO_KEY))
         {
           /* 
            We respond to changes in the node-address mapping
            This will be replicated whan a node joins / leaves the group
            When this happens we need to recalculate the failoverMap
            and rebind all connection factories with the new mapping
            We cannot just reference a single map since the objects are bound in JNDI
            in serialized form
            */
            log.info("responding to node - adress info change. Recalculating mapping and rebinding cfs");
            
            recalculateFailoverMap(updatedReplicantMap);
            
            //rebind
            Iterator iter = endpoints.entrySet().iterator();
            
            while (iter.hasNext())
            {
               Map.Entry entry = (Map.Entry)iter.next();
               
               String uniqueName = (String)entry.getKey();
               
               ServerConnectionFactoryEndpoint endpoint =
                  (ServerConnectionFactoryEndpoint)entry.getValue();
               
               ClusteredClientConnectionFactoryDelegate del = (ClusteredClientConnectionFactoryDelegate)delegates.get(uniqueName);
               
               if (del == null)
               {
                  throw new IllegalStateException("Cannot find cf with name " + uniqueName);
               }
               
               del.setFailoverMap(failoverMap);
               
               rebindConnectionFactory(initialContext, endpoint.getJNDIBindings(), del);
            }
            
         }
         else if (sKey.startsWith(CF_PREFIX) && originatingNodeId != serverPeer.getServerPeerID())
         {
            /*
            A connection factory has been deployed / undeployed - we need to update the local delegate arrays inside the clustered
            connection factories with the same name
            We don't recalculate the failover map since the number of nodes in the group hasn't changed
            We also ignore any local changes since the cf will already be bound locally with the new
            local delegate in the array
            */
            
            String uniqueName = sKey.substring(CF_PREFIX.length());
            
            log.info("Connection factory with unique name " + uniqueName + " has been added / removed");
            
            ClusteredClientConnectionFactoryDelegate del = (ClusteredClientConnectionFactoryDelegate)delegates.get(uniqueName);
            
            if (del == null)
            {
               throw new IllegalStateException("Cannot find cf with name " + uniqueName);
            }
            
            ClientConnectionFactoryDelegate[] delArr = 
               (ClientConnectionFactoryDelegate[])updatedReplicantMap.values().toArray(new ClientConnectionFactoryDelegate[updatedReplicantMap.size()]);

            log.info("Updating delsArr with size " + delArr.length);
            
            del.setDelegates(delArr);
            
            ServerConnectionFactoryEndpoint endpoint =
               (ServerConnectionFactoryEndpoint)endpoints.get(uniqueName);
            
            if (endpoint == null)
            {
               throw new IllegalStateException("Cannot find endpoint with name " + uniqueName);
            }
            
            rebindConnectionFactory(initialContext, endpoint.getJNDIBindings(), del);
            
         }            
      }
      catch (Exception e)
      {
         log.error("Failed to rebind connection factory", e);
      }
   }
   
   // Public --------------------------------------------------------
   
   public void injectReplicator(Replicator replicator)
   {
      this.replicator = replicator;
      
      replicator.registerListener(this);
   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   private void setupReplicator() throws Exception
   {
      this.serverPeer.getPostOfficeInstance();
   }

   private void recalculateFailoverMap(Map nodeAddressMap) throws Exception
   {     
      FailoverMapper mapper = replicator.getFailoverMapper();
      
      failoverMap = mapper.generateMapping(nodeAddressMap.keySet());
   }
   
   /**
    * @param localDelegates - Map<Integer(nodeId) - ClientConnectionFactoryDelegate>
    */
   private ClusteredClientConnectionFactoryDelegate createClusteredDelegate(Map localDelegates) throws Exception
   {
      //TODO: make it trace after the code is stable
      log.info("Updating FailoverDelegates " + localDelegates.size() + " on serverPeer:" + serverPeer.getServerPeerID());

      ClientConnectionFactoryDelegate[] delArr = 
         (ClientConnectionFactoryDelegate[])localDelegates.values().toArray(new ClientConnectionFactoryDelegate[localDelegates.size()]);

      //If the map is not cached - generate it now
      
      if (failoverMap == null)
      {
         Map nodeAddressMap = replicator.get(DefaultClusteredPostOffice.ADDRESS_INFO_KEY);
         
         if (nodeAddressMap == null)
         {
            throw new IllegalStateException("Cannot find address node mapping!");
         }
         
         recalculateFailoverMap(nodeAddressMap);
      }

      // The main delegated is needed for the construction of ClusteredClientConnectionFactoryDelegate
      // ClusteredClientConnectionFactoryDelegate extends ClientConnectionFactoryDelegate and it will
      // need the current server's delegate properties to be bound to ObjectId, ServerLocator and
      // other connection properties.
      //
      // The ClusteredCFDelegate will copy these properties on its contructor defined bellow after this loop
      ClientConnectionFactoryDelegate mainDelegate = null;
      
      for(Iterator i = localDelegates.values().iterator(); i.hasNext();)
      {
         ClientConnectionFactoryDelegate del = (ClientConnectionFactoryDelegate)i.next();
            
          if (del.getServerId() == this.serverPeer.getServerPeerID())
          {
             // sanity check
             if (mainDelegate != null)
             {
                throw new IllegalStateException("There are two servers with serverID=" + this.serverPeer.getServerPeerID() + ", verify your clustering configuration");
             }
             mainDelegate = del;
          }
      }
            
      return new ClusteredClientConnectionFactoryDelegate(mainDelegate, delArr, failoverMap);
   }
   
   private void rebindConnectionFactory(Context ic,
                                        JNDIBindings jndiBindings,
                                        ClientConnectionFactoryDelegate delegate)
      throws NamingException
   {
      JBossConnectionFactory cf = new JBossConnectionFactory(delegate);

      if (jndiBindings != null)
      {
         List jndiNames = jndiBindings.getNames();
         for(Iterator i = jndiNames.iterator(); i.hasNext(); )
         {
            String jndiName = (String)i.next();
            log.info("Rebinding " + jndiName + " CF=" + cf );
            JNDIUtil.rebind(ic, jndiName, cf);
         }
      }
   }

   // Inner classes -------------------------------------------------
}
