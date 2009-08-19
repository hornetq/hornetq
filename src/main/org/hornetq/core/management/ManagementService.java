/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
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

package org.hornetq.core.management;

import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import javax.management.ObjectName;

import org.hornetq.core.cluster.DiscoveryGroup;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.cluster.BridgeConfiguration;
import org.hornetq.core.config.cluster.BroadcastGroupConfiguration;
import org.hornetq.core.config.cluster.ClusterConnectionConfiguration;
import org.hornetq.core.config.cluster.DiscoveryGroupConfiguration;
import org.hornetq.core.config.cluster.DivertConfiguration;
import org.hornetq.core.management.impl.MessagingServerControlImpl;
import org.hornetq.core.messagecounter.MessageCounterManager;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.remoting.server.RemotingService;
import org.hornetq.core.remoting.spi.Acceptor;
import org.hornetq.core.security.Role;
import org.hornetq.core.server.Divert;
import org.hornetq.core.server.MessagingComponent;
import org.hornetq.core.server.MessagingServer;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.QueueFactory;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.cluster.Bridge;
import org.hornetq.core.server.cluster.BroadcastGroup;
import org.hornetq.core.server.cluster.ClusterConnection;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.transaction.ResourceManager;
import org.hornetq.utils.SimpleString;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public interface ManagementService extends NotificationService, MessagingComponent
{
   // Configuration

   MessageCounterManager getMessageCounterManager();

   String getClusterUser();

   String getClusterPassword();

   SimpleString getManagementAddress();

   SimpleString getManagementNotificationAddress();

   ReplicationOperationInvoker getReplicationOperationInvoker();

   // Resource Registration

   MessagingServerControlImpl registerServer(PostOffice postOffice,
                                         StorageManager storageManager,
                                         Configuration configuration,
                                         HierarchicalRepository<AddressSettings> addressSettingsRepository,
                                         HierarchicalRepository<Set<Role>> securityRepository,
                                         ResourceManager resourceManager,
                                         RemotingService remotingService,
                                         MessagingServer messagingServer,
                                         QueueFactory queueFactory,
                                         ScheduledExecutorService scheduledThreadPool,
                                         boolean backup) throws Exception;

   void unregisterServer() throws Exception;

   void registerInJMX(ObjectName objectName, Object managedResource) throws Exception;

   void unregisterFromJMX(final ObjectName objectName) throws Exception;

   void registerInRegistry(String resourceName, Object managedResource);

   void unregisterFromRegistry(final String resourceName);

   void registerAddress(SimpleString address) throws Exception;

   void unregisterAddress(SimpleString address) throws Exception;

   void registerQueue(Queue queue, SimpleString address, StorageManager storageManager) throws Exception;

   void unregisterQueue(SimpleString name, SimpleString address) throws Exception;

   void registerAcceptor(Acceptor acceptor, TransportConfiguration configuration) throws Exception;

   void unregisterAcceptors();

   void registerDivert(Divert divert, DivertConfiguration config) throws Exception;

   void unregisterDivert(SimpleString name) throws Exception;

   void registerBroadcastGroup(BroadcastGroup broadcastGroup, BroadcastGroupConfiguration configuration) throws Exception;

   void unregisterBroadcastGroup(String name) throws Exception;

   void registerDiscoveryGroup(DiscoveryGroup discoveryGroup, DiscoveryGroupConfiguration configuration) throws Exception;

   void unregisterDiscoveryGroup(String name) throws Exception;

   void registerBridge(Bridge bridge, BridgeConfiguration configuration) throws Exception;

   void unregisterBridge(String name) throws Exception;

   void registerCluster(ClusterConnection cluster, ClusterConnectionConfiguration configuration) throws Exception;

   void unregisterCluster(String name) throws Exception;

   Object getResource(String resourceName);

   ServerMessage handleMessage(ServerMessage message) throws Exception;

}
