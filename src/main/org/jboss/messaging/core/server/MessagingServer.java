/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors by
 * the @authors tag. See the copyright.txt in the distribution for a full listing of individual contributors. This is
 * free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of the License, or (at your option) any later version.
 * This software is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public License along with this software; if not,
 * write to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.core.server;

import java.util.List;
import java.util.Set;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.management.ManagementService;
import org.jboss.messaging.core.management.MessagingServerControlMBean;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateSessionResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ReattachSessionResponseMessage;
import org.jboss.messaging.core.remoting.server.RemotingService;
import org.jboss.messaging.core.security.JBMSecurityManager;
import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.server.cluster.ClusterManager;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.core.transaction.ResourceManager;
import org.jboss.messaging.core.version.Version;
import org.jboss.messaging.utils.Pair;
import org.jboss.messaging.utils.SimpleString;
import org.jboss.messaging.utils.UUID;

/**
 * This interface defines the internal interface of the Messaging Server exposed to other components of the server. The
 * external management interface of the Messaging Server is defined by the MessagingServerManagement interface This
 * interface is never exposed outside the messaging server, e.g. by JMX or other means
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public interface MessagingServer extends MessagingComponent
{
   void setConfiguration(Configuration configuration);

   Configuration getConfiguration();

   void setRemotingService(RemotingService remotingService);

   RemotingService getRemotingService();

   void setStorageManager(StorageManager storageManager);

   StorageManager getStorageManager();

   JBMSecurityManager getSecurityManager();

   void setSecurityManager(JBMSecurityManager securityManager);

   ManagementService getManagementService();

   void setManagementService(ManagementService managementService);

   Version getVersion();

   ReattachSessionResponseMessage reattachSession(RemotingConnection connection, String name, int lastReceivedCommandID) throws Exception;

   CreateSessionResponseMessage createSession(String name,
                                              long channelID,
                                              long replicatedSessionID,
                                              String username,
                                              String password,
                                              int minLargeMessageSize,
                                              int incrementingVersion,
                                              RemotingConnection remotingConnection,
                                              boolean autoCommitSends,
                                              boolean autoCommitAcks,
                                              boolean preAcknowledge,
                                              boolean xa,
                                              int sendWindowSize) throws Exception;

   void replicateCreateSession(String name,
                               long channelID,
                               long originalSessionID,
                               String username,
                               String password,
                               int minLargeMessageSize,
                               int incrementingVersion,
                               RemotingConnection remotingConnection,
                               boolean autoCommitSends,
                               boolean autoCommitAcks,
                               boolean preAcknowledge,
                               boolean xa,
                               int sendWindowSize) throws Exception;

   void removeSession(String name) throws Exception;

   ServerSession getSession(String name);

   Set<ServerSession> getSessions();

   boolean isStarted();

   MessagingServerControlMBean getServerManagement();

   HierarchicalRepository<Set<Role>> getSecurityRepository();

   HierarchicalRepository<AddressSettings> getAddressSettingsRepository();

   int getConnectionCount();

   PostOffice getPostOffice();

   ResourceManager getResourceManager();

   List<ServerSession> getSessions(String connectionID);

   ClusterManager getClusterManager();

   QueueFactory getQueueFactory();

   SimpleString getNodeID();

   UUID getUUID();

   Channel getReplicatingChannel();
}
