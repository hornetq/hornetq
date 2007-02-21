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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.jboss.aop.AspectManager;
import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.jms.client.delegate.ClientClusteredConnectionFactoryDelegate;
import org.jboss.jms.client.delegate.ClientConnectionFactoryDelegate;
import org.jboss.jms.client.plugin.LoadBalancingFactory;
import org.jboss.jms.client.plugin.LoadBalancingPolicy;
import org.jboss.jms.delegate.ConnectionFactoryDelegate;
import org.jboss.jms.server.ConnectionFactoryManager;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.Version;
import org.jboss.jms.server.endpoint.ServerConnectionFactoryEndpoint;
import org.jboss.jms.server.endpoint.advised.ConnectionFactoryAdvised;
import org.jboss.jms.util.JNDIUtil;
import org.jboss.jms.wireformat.Dispatcher;
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
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ConnectionFactoryJNDIMapper.class);

   private static final String CF_PREFIX = "CF_";

   // Static ---------------------------------------------------------------------------------------

   private static boolean trace = log.isTraceEnabled();

   // Attributes -----------------------------------------------------------------------------------

   protected Context initialContext;
   protected ServerPeer serverPeer;

   // Map<uniqueName<String> - ServerConnectionFactoryEndpoint>
   protected Map endpoints;

   // Map<uniqueName<String> - ClientConnectionFactoryDelegate> (not just clustered delegates)
   protected Map delegates;

   private Replicator replicator;

   // Map<Integer(nodeID)->Integer(failoverNodeID)>
   // The map is updated when a node joins of leaves the cluster via the replicationListener. When
   // a new ConnectionFactories are deployed we use the cached map.
   protected Map failoverMap;

   // Constructors ---------------------------------------------------------------------------------

   public ConnectionFactoryJNDIMapper(ServerPeer serverPeer) throws Exception
   {
      this.serverPeer = serverPeer;
      endpoints = new HashMap();
      delegates = new HashMap();
   }

   // ConnectionFactoryManager implementation ------------------------------------------------------

   /**
    * @param loadBalancingFactory - ignored for non-clustered connection factories.
    */
   public synchronized void registerConnectionFactory(String uniqueName,
                                                      String clientID,
                                                      JNDIBindings jndiBindings,
                                                      String locatorURI,
                                                      boolean clientPing,
                                                      int prefetchSize,
                                                      int defaultTempQueueFullSize,
                                                      int defaultTempQueuePageSize,
                                                      int defaultTempQueueDownCacheSize,
                                                      int dupsOKBatchSize,
                                                      boolean clustered,
                                                      LoadBalancingFactory loadBalancingFactory)
      throws Exception
   {
      log.debug(this + " registering connection factory '" + uniqueName +
         "', bindings: " + jndiBindings);

      // Sanity check
      if (delegates.containsKey(uniqueName))
      {
         throw new IllegalArgumentException("There's already a connection factory " +
                                            "registered with name " + uniqueName);
      }

      int id = serverPeer.getNextObjectID();
      Version version = serverPeer.getVersion();

      ServerConnectionFactoryEndpoint endpoint =
         new ServerConnectionFactoryEndpoint(id, serverPeer, clientID,
                                             jndiBindings, prefetchSize,
                                             defaultTempQueueFullSize,
                                             defaultTempQueuePageSize,
                                             defaultTempQueueDownCacheSize,
                                             dupsOKBatchSize);
      endpoints.put(uniqueName, endpoint);

      ConnectionFactoryDelegate delegate = null;

      if (clustered)
      {
         setupReplicator();
      }

      boolean creatingClustered = clustered && replicator != null;

      ClientConnectionFactoryDelegate localDelegate =
         new ClientConnectionFactoryDelegate(id, serverPeer.getServerPeerID(),
                                             locatorURI, version, clientPing);

      log.debug(this + " created local delegate " + localDelegate);

      // When registering a new clustered connection factory I should first create it with the
      // available delegates then send the replication message. We then listen for connection
      // factories added to global state using the replication listener and then update their
      // connection factory list. This will happen locally too, so we will get the replication
      // message locally - to avoid updating it again we can ignore any "add" replication updates
      // that originate from the current node.

      if (creatingClustered)
      {
         // Replicate the change - we will ignore this locally

         replicator.put(CF_PREFIX + uniqueName, localDelegate);

         // Create a clustered delegate

         Map localDelegates = replicator.get(CF_PREFIX + uniqueName);
         delegate = createClusteredDelegate(localDelegates.values(), loadBalancingFactory);

         log.debug(this + " created clustered delegate " + delegate);
      }
      else
      {
         delegate = localDelegate;
      }

      log.trace(this + " adding delegates factory " + uniqueName + " pointing to " + delegate);

      delegates.put(uniqueName, delegate);

      // Now bind it in JNDI
      rebindConnectionFactory(initialContext, jndiBindings, delegate);
      
      ConnectionFactoryAdvised advised;
      
      // Need to synchronized to prevent a deadlock
      // See http://jira.jboss.com/jira/browse/JBMESSAGING-797
      synchronized (AspectManager.instance())
      {       
         advised = new ConnectionFactoryAdvised(endpoint);
      }

      // Registering with the dispatcher should always be the last thing otherwise a client could
      // use a partially initialised object
      Dispatcher.instance.registerTarget(id, advised);
   }

   public synchronized void unregisterConnectionFactory(String uniqueName, boolean clustered)
      throws Exception
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

      if (trace) { log.trace("Removing delegate from delegates list with key=" + uniqueName + " at serverPeerID=" + this.serverPeer.getServerPeerID()); }

      ConnectionFactoryDelegate delegate = (ConnectionFactoryDelegate)delegates.remove(uniqueName);

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
               throw new IllegalStateException("Cannot find replicant to remove: " +
                  CF_PREFIX + uniqueName);
            }
         }

      }

      Dispatcher.instance.unregisterTarget(endpoint.getID(), endpoint);
   }

   // MessagingComponent implementation ------------------------------------------------------------

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

   // ReplicationListener interface ----------------------------------------------------------------

   /**
    * @param updatedReplicantMap Map<Integer(nodeID)-Map<>>
    */
   public synchronized void onReplicationChange(Serializable key, Map updatedReplicantMap,
                                                boolean added, int originatorNodeID)
   {
      log.debug(this + " received " + key + " replication change from node " + originatorNodeID +
         ", new map " + updatedReplicantMap);

      try
      {
         if (!(key instanceof String))
         {
            return;
         }

         String sKey = (String)key;

         if (sKey.equals(DefaultClusteredPostOffice.ADDRESS_INFO_KEY))
         {
            // We respond to changes in the node-address mapping. This will be replicated whan a
            // node joins / leaves the group. When this happens we need to recalculate the
            // failoverMap and rebind all connection factories with the new mapping. We cannot just
            // reference a single map since the objects are bound in JNDI in serialized form.

            failoverMap = recalculateFailoverMap(updatedReplicantMap.keySet());

            // Rebind

            for(Iterator i = endpoints.entrySet().iterator(); i.hasNext(); )
            {
               Map.Entry entry = (Map.Entry)i.next();
               String uniqueName = (String)entry.getKey();

               Object del = delegates.get(uniqueName);

               if (del == null)
               {
                  throw new IllegalStateException(
                     "Cannot find connection factory with name " + uniqueName);
               }

               if (del instanceof ClientClusteredConnectionFactoryDelegate)
               {
                  ((ClientClusteredConnectionFactoryDelegate)del).setFailoverMap(failoverMap);
               }
            }
         }
         else if (sKey.startsWith(CF_PREFIX) && originatorNodeID != serverPeer.getServerPeerID())
         {
            // A connection factory has been deployed / undeployed - we need to update the local
            // delegate arrays inside the clustered connection factories with the same name. We
            // don't recalculate the failover map since the number of nodes in the group hasn't
            // changed. We also ignore any local changes since the cf will already be bound locally
            // with the new local delegate in the array

            String uniqueName = sKey.substring(CF_PREFIX.length());

            log.debug(this + " received '" + uniqueName +
                                "' connection factory update " + updatedReplicantMap);

            ClientClusteredConnectionFactoryDelegate del =
               (ClientClusteredConnectionFactoryDelegate)delegates.get(uniqueName);

            if (del == null)
            {
               throw new IllegalStateException("Cannot find cf with name " + uniqueName);
            }

            List newDels = sortDelegatesOnServerID(updatedReplicantMap.values());

            ClientConnectionFactoryDelegate[] delArr =
               (ClientConnectionFactoryDelegate[])newDels.
                  toArray(new ClientConnectionFactoryDelegate[newDels.size()]);

            del.setDelegates(delArr);

            ServerConnectionFactoryEndpoint endpoint =
               (ServerConnectionFactoryEndpoint)endpoints.get(uniqueName);

            if (endpoint == null)
            {
               throw new IllegalStateException("Cannot find endpoint with name " + uniqueName);
            }

            rebindConnectionFactory(initialContext, endpoint.getJNDIBindings(), del);

            endpoint.updateClusteredClients(delArr, failoverMap);
         }
      }
      catch (Exception e)
      {
         log.error("Failed to rebind connection factory", e);
      }
   }

   // Public ---------------------------------------------------------------------------------------

   public void injectReplicator(Replicator replicator)
   {
      this.replicator = replicator;
      replicator.registerListener(this);
   }

   public String toString()
   {
      return "Server[" + serverPeer.getServerPeerID() + "].ConnFactoryJNDIMapper";
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   private void setupReplicator() throws Exception
   {
      serverPeer.getPostOfficeInstance();
   }

   /**
    * @param nodeIDs Set<Integer(nodeID)>
    * @return Map<Integer(nodeID)->Integer(failoverNodeID)>
    */
   private Map recalculateFailoverMap(Set nodeIDs) throws Exception
   {
      FailoverMapper mapper = replicator.getFailoverMapper();
      return mapper.generateMapping(nodeIDs);
   }

   /**
    * @param localDelegates - Collection<ClientConnectionFactoryDelegate>
    */
   private ClientClusteredConnectionFactoryDelegate
      createClusteredDelegate(Collection localDelegates, LoadBalancingFactory loadBalancingFactory)
      throws Exception
   {
      log.trace(this + " creating a clustered ConnectionFactoryDelegate based on " + localDelegates);

      // First sort the local delegates in order of server ID
      List sortedLocalDelegates = sortDelegatesOnServerID(localDelegates);

      ClientConnectionFactoryDelegate[] delegates =
         (ClientConnectionFactoryDelegate[])sortedLocalDelegates.
            toArray(new ClientConnectionFactoryDelegate[sortedLocalDelegates.size()]);

      // If the map is not cached - generate it now

      if (failoverMap == null)
      {
         Map nodeAddressMap = replicator.get(DefaultClusteredPostOffice.ADDRESS_INFO_KEY);

         if (nodeAddressMap == null)
         {
            throw new IllegalStateException("Cannot find address node mapping!");
         }

         failoverMap = recalculateFailoverMap(nodeAddressMap.keySet());
      }

      LoadBalancingPolicy lbp = loadBalancingFactory.createLoadBalancingPolicy(delegates);
      return new ClientClusteredConnectionFactoryDelegate(delegates, failoverMap, lbp);
   }

   private void rebindConnectionFactory(Context ic, JNDIBindings jndiBindings,
                                        ConnectionFactoryDelegate delegate)
      throws NamingException
   {
      JBossConnectionFactory cf = new JBossConnectionFactory(delegate);

      if (jndiBindings != null)
      {
         List jndiNames = jndiBindings.getNames();
         for(Iterator i = jndiNames.iterator(); i.hasNext(); )
         {
            String jndiName = (String)i.next();
            log.debug(this + " rebinding " + cf + " as " + jndiName);
            JNDIUtil.rebind(ic, jndiName, cf);
         }
      }
   }

   private List sortDelegatesOnServerID(Collection delegates)
   {
      List localDels = new ArrayList(delegates);

      Collections.sort(localDels,
         new Comparator()
         {
            public int compare(Object obj1, Object obj2)
            {
               ClientConnectionFactoryDelegate del1 = (ClientConnectionFactoryDelegate)obj1;
               ClientConnectionFactoryDelegate del2 = (ClientConnectionFactoryDelegate)obj2;
               return del1.getServerID() - del2.getServerID();
            }
         });

      return localDels;
   }

   // Inner classes --------------------------------------------------------------------------------
}
