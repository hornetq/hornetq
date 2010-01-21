/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.core.server;

import java.util.List;
import java.util.Set;

import javax.management.MBeanServer;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.management.impl.HornetQServerControlImpl;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.protocol.core.Channel;
import org.hornetq.core.protocol.core.CoreRemotingConnection;
import org.hornetq.core.remoting.server.RemotingService;
import org.hornetq.core.replication.ReplicationEndpoint;
import org.hornetq.core.security.Role;
import org.hornetq.core.server.cluster.ClusterManager;
import org.hornetq.core.server.group.GroupingHandler;
import org.hornetq.core.server.management.ManagementService;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.transaction.ResourceManager;
import org.hornetq.core.version.Version;
import org.hornetq.spi.core.security.HornetQSecurityManager;
import org.hornetq.utils.ExecutorFactory;

/**
 * This interface defines the internal interface of the HornetQ Server exposed to other components of the server. The
 * external management interface of the HornetQ Server is defined by the HornetQServerManagement interface This
 * interface is never exposed outside the HornetQ server, e.g. by JMX or other means
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public interface HornetQServer extends HornetQComponent
{
   Configuration getConfiguration();

   RemotingService getRemotingService();

   StorageManager getStorageManager();

   ManagementService getManagementService();

   HornetQSecurityManager getSecurityManager();

   MBeanServer getMBeanServer();

   Version getVersion();

   HornetQServerControlImpl getHornetQServerControl();

   void registerActivateCallback(ActivateCallback callback);

   void unregisterActivateCallback(ActivateCallback callback);

   /** The journal at the backup server has to be equivalent as the journal used on the live node. 
    *  Or else the backup node is out of sync. */
   ReplicationEndpoint connectToReplicationEndpoint(Channel channel) throws Exception;

   ServerSession createSession(String name,
                               String username,
                               String password,
                               int minLargeMessageSize,
                               CoreRemotingConnection remotingConnection,
                               boolean autoCommitSends,
                               boolean autoCommitAcks,
                               boolean preAcknowledge,
                               boolean xa) throws Exception;

   void removeSession(String name) throws Exception;

   ServerSession getSession(String name);

   Set<ServerSession> getSessions();

   boolean isStarted();

   HierarchicalRepository<Set<Role>> getSecurityRepository();

   HierarchicalRepository<AddressSettings> getAddressSettingsRepository();

   int getConnectionCount();

   PostOffice getPostOffice();

   QueueFactory getQueueFactory();

   ResourceManager getResourceManager();

   List<ServerSession> getSessions(String connectionID);

   ClusterManager getClusterManager();

   SimpleString getNodeID();

   boolean isInitialised();

   Queue createQueue(SimpleString address,
                     SimpleString queueName,
                     SimpleString filter,
                     boolean durable,
                     boolean temporary) throws Exception;

   Queue deployQueue(SimpleString address,
                     SimpleString queueName,
                     SimpleString filterString,
                     boolean durable,
                     boolean temporary) throws Exception;

   void destroyQueue(SimpleString queueName, ServerSession session) throws Exception;

   ExecutorFactory getExecutorFactory();

   void setGroupingHandler(GroupingHandler groupingHandler);

   GroupingHandler getGroupingHandler();

   boolean checkActivate() throws Exception;
}
