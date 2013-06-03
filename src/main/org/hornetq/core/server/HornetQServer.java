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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import javax.management.MBeanServer;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.config.BridgeConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.DivertConfiguration;
import org.hornetq.core.management.impl.HornetQServerControlImpl;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.protocol.core.Channel;
import org.hornetq.core.remoting.server.RemotingService;
import org.hornetq.core.replication.ReplicationEndpoint;
import org.hornetq.core.replication.ReplicationManager;
import org.hornetq.core.security.Role;
import org.hornetq.core.server.cluster.ClusterManager;
import org.hornetq.core.server.group.GroupingHandler;
import org.hornetq.core.server.impl.ConnectorsService;
import org.hornetq.core.server.management.ManagementService;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.transaction.ResourceManager;
import org.hornetq.core.version.Version;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.protocol.SessionCallback;
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

   /** This method was created mainly for testing but it may be used in scenarios where
    *  you need to have more than one Server inside the same VM.
    *  This identity will be exposed on logs what may help you to debug issues on the log traces and debugs.*/
   void setIdentity(String identity);

   String getIdentity();

   String describe();

   Configuration getConfiguration();

   RemotingService getRemotingService();

   StorageManager getStorageManager();

   PagingManager getPagingManager();

   ManagementService getManagementService();

   HornetQSecurityManager getSecurityManager();

   MBeanServer getMBeanServer();

   Version getVersion();

   NodeManager getNodeManager();

   /**
    * Returns the resource to manage this HornetQ server.
    *
    * Using this control will throw IllegalStateException if the
    * server is not properly started.
    */
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
                               RemotingConnection remotingConnection,
                               boolean autoCommitSends,
                               boolean autoCommitAcks,
                               boolean preAcknowledge,
                               boolean xa,
                               String defaultAddress,
                               SessionCallback callback) throws Exception;

   void removeSession(String name) throws Exception;

   Set<ServerSession> getSessions();

   boolean isStarted();

   boolean isStopped();

   HierarchicalRepository<Set<Role>> getSecurityRepository();

   HierarchicalRepository<AddressSettings> getAddressSettingsRepository();

   int getConnectionCount();

   PostOffice getPostOffice();

   QueueFactory getQueueFactory();

   ResourceManager getResourceManager();

   List<ServerSession> getSessions(String connectionID);

   /** will return true if there is any session wth this key */
   ServerSession lookupSession(String metakey, String metavalue);

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

   Queue locateQueue(SimpleString queueName) throws Exception;

   void destroyQueue(SimpleString queueName) throws Exception;

   void destroyQueue(SimpleString queueName, ServerSession session) throws Exception;

   void destroyQueue(SimpleString queueName, ServerSession session, boolean checkConsumerCount) throws Exception;

   String destroyConnectionWithSessionMetadata(String metaKey, String metaValue) throws Exception;

   ScheduledExecutorService getScheduledPool();

   ExecutorService getThreadPool();

   ExecutorFactory getExecutorFactory();

   void setGroupingHandler(GroupingHandler groupingHandler);

   GroupingHandler getGroupingHandler();

   ReplicationEndpoint getReplicationEndpoint();

   ReplicationManager getReplicationManager();

   boolean checkActivate() throws Exception;

   void deployDivert(DivertConfiguration config) throws Exception;

   void destroyDivert(SimpleString name) throws Exception;

   ConnectorsService getConnectorsService();

   void deployBridge(BridgeConfiguration config) throws Exception;

   void destroyBridge(String name) throws Exception;

   ServerSession getSessionByID(String sessionID);

   void threadDump(String reason);

   void stop(boolean failoverOnServerShutdown) throws Exception;
}
