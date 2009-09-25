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

import org.hornetq.core.config.Configuration;
import org.hornetq.core.management.ManagementService;
import org.hornetq.core.management.impl.HornetQServerControlImpl;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.wireformat.CreateSessionResponseMessage;
import org.hornetq.core.remoting.impl.wireformat.ReattachSessionResponseMessage;
import org.hornetq.core.remoting.server.RemotingService;
import org.hornetq.core.security.HornetQSecurityManager;
import org.hornetq.core.security.Role;
import org.hornetq.core.server.cluster.ClusterManager;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.transaction.ResourceManager;
import org.hornetq.core.version.Version;
import org.hornetq.utils.ExecutorFactory;
import org.hornetq.utils.SimpleString;
import org.hornetq.utils.UUID;

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

   ReattachSessionResponseMessage reattachSession(RemotingConnection connection, String name, int lastReceivedCommandID) throws Exception;

   CreateSessionResponseMessage createSession(String name,
                                              long channelID,                                              
                                              String username,
                                              String password,
                                              int minLargeMessageSize,
                                              int incrementingVersion,
                                              RemotingConnection remotingConnection,
                                              boolean autoCommitSends,
                                              boolean autoCommitAcks,
                                              boolean preAcknowledge,
                                              boolean xa,
                                              int producerWindowSize) throws Exception;

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

  // void initialiseBackup(UUID nodeID, long currentMessageID) throws Exception;

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
}
