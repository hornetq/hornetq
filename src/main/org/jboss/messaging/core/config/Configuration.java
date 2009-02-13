/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat
 * Middleware LLC, and individual contributors by the @authors tag. See the
 * copyright.txt in the distribution for a full listing of individual
 * contributors.
 * 
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 * 
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.core.config;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jboss.messaging.core.config.cluster.BridgeConfiguration;
import org.jboss.messaging.core.config.cluster.BroadcastGroupConfiguration;
import org.jboss.messaging.core.config.cluster.ClusterConnectionConfiguration;
import org.jboss.messaging.core.config.cluster.DiscoveryGroupConfiguration;
import org.jboss.messaging.core.config.cluster.DivertConfiguration;
import org.jboss.messaging.core.config.cluster.QueueConfiguration;
import org.jboss.messaging.core.server.JournalType;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * A Configuration
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface Configuration extends Serializable
{
   // General attributes -------------------------------------------------------------------

   boolean isClustered();

   void setClustered(boolean clustered);

   boolean isBackup();

   void setBackup(boolean backup);
   
   long getQueueActivationTimeout();
   
   void setQueueActivationTimeout(long timeout);
   
   int getScheduledThreadPoolMaxSize();

   void setScheduledThreadPoolMaxSize(int maxSize);

   long getSecurityInvalidationInterval();

   void setSecurityInvalidationInterval(long interval);

   boolean isSecurityEnabled();

   void setSecurityEnabled(boolean enabled);

   boolean isRequireDestinations();

   void setRequireDestinations(boolean require);

   boolean isJMXManagementEnabled();

   void setJMXManagementEnabled(boolean enabled);

   List<String> getInterceptorClassNames();

   void setInterceptorClassNames(List<String> interceptors);

   long getConnectionScanPeriod();
   
   void setConnectionScanPeriod(long scanPeriod);
   
   long getConnectionTTLOverride();

   void setConnectionTTLOverride(long ttl);

   Set<TransportConfiguration> getAcceptorConfigurations();

   void setAcceptorConfigurations(Set<TransportConfiguration> infos);
   
   Map<String, TransportConfiguration> getConnectorConfigurations();

   void setConnectorConfigurations(Map<String, TransportConfiguration> infos);   

   String getBackupConnectorName();

   void setBackupConnectorName(String name);
   
   List<BroadcastGroupConfiguration> getBroadcastGroupConfigurations();
   
   void setBroadcastGroupConfigurations(List<BroadcastGroupConfiguration> configs);
   
   Map<String, DiscoveryGroupConfiguration> getDiscoveryGroupConfigurations();
   
   void setDiscoveryGroupConfigurations(Map<String, DiscoveryGroupConfiguration> configs);
   
   List<BridgeConfiguration> getBridgeConfigurations();

   void setBridgeConfigurations(final List<BridgeConfiguration> configs);
   
   List<DivertConfiguration> getDivertConfigurations();

   void setDivertConfigurations(final List<DivertConfiguration> configs);
   
   List<ClusterConnectionConfiguration> getClusterConfigurations();

   void setClusterConfigurations(final List<ClusterConnectionConfiguration> configs);
   
   List<QueueConfiguration> getQueueConfigurations();

   void setQueueConfigurations(final List<QueueConfiguration> configs);
   
   SimpleString getManagementAddress();
   
   void setManagementAddress(SimpleString address);

   SimpleString getManagementNotificationAddress();
   
   String getManagementClusterPassword();

   long getManagementRequestTimeout();

   int getIDCacheSize();
   
   void setIDCacheSize(int idCacheSize);
   
   boolean isPersistIDCache();
   
   void setPersistIDCache(boolean persist);

   
   // Journal related attributes ------------------------------------------------------------

   String getBindingsDirectory();

   void setBindingsDirectory(String dir);

   String getJournalDirectory();

   void setJournalDirectory(String dir);

   JournalType getJournalType();

   void setJournalType(JournalType type);

   boolean isJournalSyncTransactional();

   void setJournalSyncTransactional(boolean sync);

   boolean isJournalSyncNonTransactional();

   void setJournalSyncNonTransactional(boolean sync);

   int getJournalFileSize();

   void setJournalFileSize(int size);

   int getJournalMinFiles();

   void setJournalMinFiles(int files);

   int getJournalMaxAIO();

   void setJournalMaxAIO(int maxAIO);

   void setJournalBufferReuseSize(int reuseSize);

   int getJournalBufferReuseSize();

   boolean isCreateBindingsDir();

   void setCreateBindingsDir(boolean create);

   boolean isCreateJournalDir();

   void setCreateJournalDir(boolean create);

   
   // Paging Properties --------------------------------------------------------------------
   
   int getPagingMaxThreads();
   
   void setPagingMaxThread(int pagingMaxThreads);
   
   String getPagingDirectory();

   void setPagingDirectory(String dir);

   long getPagingMaxGlobalSizeBytes();

   void setPagingMaxGlobalSizeBytes(long maxGlobalSize);
   
   long getPagingDefaultSize();
   
   void setPagingDefaultSize(long pageSize);
   
   // Large Messages Properties ------------------------------------------------------------
   
   String getLargeMessagesDirectory();
   
   void setLargeMessagesDirectory(String directory);

   boolean isWildcardRoutingEnabled();

   void setWildcardRoutingEnabled(boolean enabled);

   long getTransactionTimeout();

   void setTransactionTimeout(long timeout);

   boolean isMessageCounterEnabled();

   long getTransactionTimeoutScanPeriod();

   void setTransactionTimeoutScanPeriod(long period);

   long getMessageExpiryScanPeriod();

   void setMessageExpiryScanPeriod(long messageExpiryScanPeriod);

   int getMessageExpiryThreadPriority();

   void setMessageExpiryThreadPriority(int messageExpiryThreadPriority);

}
