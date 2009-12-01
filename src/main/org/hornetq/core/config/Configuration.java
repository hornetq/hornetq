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

package org.hornetq.core.config;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hornetq.core.config.cluster.BridgeConfiguration;
import org.hornetq.core.config.cluster.BroadcastGroupConfiguration;
import org.hornetq.core.config.cluster.ClusterConnectionConfiguration;
import org.hornetq.core.config.cluster.DiscoveryGroupConfiguration;
import org.hornetq.core.config.cluster.DivertConfiguration;
import org.hornetq.core.config.cluster.QueueConfiguration;
import org.hornetq.core.server.JournalType;
import org.hornetq.core.server.group.impl.GroupingHandlerConfiguration;
import org.hornetq.utils.SimpleString;

/**
 * 
 * A Configuration
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface Configuration extends Serializable
{
   public void start() throws Exception;

   public void stop() throws Exception;

   public boolean isStarted();

   
   // General attributes -------------------------------------------------------------------

   boolean isClustered();

   void setClustered(boolean clustered);
   
   boolean isPersistDeliveryCountBeforeDelivery();

   void setPersistDeliveryCountBeforeDelivery(boolean persistDeliveryCountBeforeDelivery);

   boolean isBackup();

   void setBackup(boolean backup);
   
   boolean isSharedStore();
   
   void setSharedStore(boolean sharedStore);
   
   boolean isFileDeploymentEnabled();
   
   void setFileDeploymentEnabled(boolean enable);
   
   boolean isPersistenceEnabled();
   
   void setPersistenceEnabled(boolean enable);
   
   long getFileDeployerScanPeriod();
   
   void setFileDeployerScanPeriod(long period);

   int getThreadPoolMaxSize();

   void setThreadPoolMaxSize(int maxSize);

   int getScheduledThreadPoolMaxSize();

   void setScheduledThreadPoolMaxSize(int maxSize);

   long getSecurityInvalidationInterval();

   void setSecurityInvalidationInterval(long interval);

   boolean isSecurityEnabled();

   void setSecurityEnabled(boolean enabled);

   boolean isJMXManagementEnabled();

   void setJMXManagementEnabled(boolean enabled);

   String getJMXDomain();
   
   void setJMXDomain(String domain);
   
   List<String> getInterceptorClassNames();

   void setInterceptorClassNames(List<String> interceptors);

   long getConnectionTTLOverride();

   void setConnectionTTLOverride(long ttl);
   
   boolean isAsyncConnectionExecutionEnabled();
   
   void setEnabledAsyncConnectionExecution(boolean enabled);

   Set<TransportConfiguration> getAcceptorConfigurations();

   void setAcceptorConfigurations(Set<TransportConfiguration> infos);

   Map<String, TransportConfiguration> getConnectorConfigurations();

   void setConnectorConfigurations(Map<String, TransportConfiguration> infos);

   String getBackupConnectorName();
   
   int getBackupWindowSize();
   
   void setBackupWindowSize(int windowSize);

   void setBackupConnectorName(String name);

   List<BroadcastGroupConfiguration> getBroadcastGroupConfigurations();

   void setBroadcastGroupConfigurations(List<BroadcastGroupConfiguration> configs);

   Map<String, DiscoveryGroupConfiguration> getDiscoveryGroupConfigurations();

   void setDiscoveryGroupConfigurations(Map<String, DiscoveryGroupConfiguration> configs);

   GroupingHandlerConfiguration getGroupingHandlerConfiguration();

   void setGroupingHandlerConfiguration(GroupingHandlerConfiguration groupingHandlerConfiguration);

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
   
   void setManagementNotificationAddress(SimpleString address);

   String getManagementClusterUser();
   
   void setManagementClusterUser(String user);
   
   String getManagementClusterPassword();
   
   void setManagementClusterPassword(String password);

   long getManagementRequestTimeout();
   
   void setManagementRequestTimeout(long timeout);

   int getIDCacheSize();

   void setIDCacheSize(int idCacheSize);

   boolean isPersistIDCache();

   void setPersistIDCache(boolean persist);
   
   String getLogDelegateFactoryClassName();
   
   void setLogDelegateFactoryClassName(String className);
   
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
   
   int getJournalCompactMinFiles();
   
   void setJournalCompactMinFiles(int minFiles);
   
   int getJournalCompactPercentage();
   
   void setJournalCompactPercentage(int percentage);

   int getJournalMinFiles();

   void setJournalMinFiles(int files);

   //AIO and NIO need different values for these params
   
   int getJournalMaxIO_AIO();

   void setJournalMaxIO_AIO(int journalMaxIO);

   int getJournalBufferTimeout_AIO();

   void setJournalBufferTimeout_AIO(int journalBufferTimeout);

   int getJournalBufferSize_AIO();

   void setJournalBufferSize_AIO(int journalBufferSize);
   
   
   int getJournalMaxIO_NIO();

   void setJournalMaxIO_NIO(int journalMaxIO);

   int getJournalBufferTimeout_NIO();

   void setJournalBufferTimeout_NIO(int journalBufferTimeout);

   int getJournalBufferSize_NIO();

   void setJournalBufferSize_NIO(int journalBufferSize);
   
    
   boolean isCreateBindingsDir();

   void setCreateBindingsDir(boolean create);

   boolean isCreateJournalDir();

   void setCreateJournalDir(boolean create);
   
   boolean isLogJournalWriteRate();
   
   void setLogJournalWriteRate(boolean rate);

   //Undocumented attributes

   int getJournalPerfBlastPages();
   
   void setJournalPerfBlastPages(int pages);

   long getServerDumpInterval();

   void setServerDumpInterval(long interval);

   int getMemoryWarningThreshold();
   
   void setMemoryWarningThreshold(int memoryWarningThreshold);
   
   long getMemoryMeasureInterval();

   void setMemoryMeasureInterval(long memoryMeasureInterval);

   boolean isRunSyncSpeedTest();

   void setRunSyncSpeedTest(boolean run);

   // Paging Properties --------------------------------------------------------------------

   String getPagingDirectory();

   void setPagingDirectory(String dir);

   // Large Messages Properties ------------------------------------------------------------

   String getLargeMessagesDirectory();

   void setLargeMessagesDirectory(String directory);

   // Other Properties ---------------------------------------------------------------------

   boolean isWildcardRoutingEnabled();

   void setWildcardRoutingEnabled(boolean enabled);

   long getTransactionTimeout();

   void setTransactionTimeout(long timeout);

   boolean isMessageCounterEnabled();
   
   void setMessageCounterEnabled(boolean enabled);

   long getMessageCounterSamplePeriod();

   int getMessageCounterMaxDayHistory();
   
   void setMessageCounterMaxDayHistory(int maxDayHistory);

   long getTransactionTimeoutScanPeriod();

   void setTransactionTimeoutScanPeriod(long period);

   long getMessageExpiryScanPeriod();

   void setMessageExpiryScanPeriod(long messageExpiryScanPeriod);

   int getMessageExpiryThreadPriority();

   void setMessageExpiryThreadPriority(int messageExpiryThreadPriority);



}
