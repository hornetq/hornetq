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

import org.hornetq.SimpleString;
import org.hornetq.core.config.cluster.BridgeConfiguration;
import org.hornetq.core.config.cluster.BroadcastGroupConfiguration;
import org.hornetq.core.config.cluster.ClusterConnectionConfiguration;
import org.hornetq.core.config.cluster.DiscoveryGroupConfiguration;
import org.hornetq.core.config.cluster.DivertConfiguration;
import org.hornetq.core.config.cluster.QueueConfiguration;
import org.hornetq.core.remoting.Interceptor;
import org.hornetq.core.server.JournalType;
import org.hornetq.core.server.group.impl.GroupingHandlerConfiguration;

/**
 * 
 * A Configuration is used to configure HornetQ servers.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface Configuration extends Serializable
{
   // General attributes -------------------------------------------------------------------

   /**
    * Returns whether this server is clustered.
    */
   boolean isClustered();

   void setClustered(boolean clustered);

   /**
    * Returns whether delivery count is persisted before messages are delivered to the consumers.
    */
   boolean isPersistDeliveryCountBeforeDelivery();

   void setPersistDeliveryCountBeforeDelivery(boolean persistDeliveryCountBeforeDelivery);

   /**
    * Returns {@code true} if this server is a backup, {@code false} if it is a live server.
    * <br>
    * If a backup server has been activated, returns {@code false}.
    */
   boolean isBackup();

   void setBackup(boolean backup);

   /**
    * Returns whether this server shares its data store with a corresponding live or backup server.
    */
   boolean isSharedStore();

   void setSharedStore(boolean sharedStore);

   boolean isFileDeploymentEnabled();

   void setFileDeploymentEnabled(boolean enable);

   /**
    * Returns whether this server is using persistence and store data.
    */
   boolean isPersistenceEnabled();

   void setPersistenceEnabled(boolean enable);

   long getFileDeployerScanPeriod();

   void setFileDeployerScanPeriod(long period);

   /**
    * Returns the maximum number of threads in the thread pool.
    */
   int getThreadPoolMaxSize();

   void setThreadPoolMaxSize(int maxSize);

   /**
    * Returns the maximum number of threads in the <em>scheduled</em> thread pool.
    */
   int getScheduledThreadPoolMaxSize();

   void setScheduledThreadPoolMaxSize(int maxSize);

   /**
    * Returns the interval time (in milliseconds) to invalidate security credentials.
    */
   long getSecurityInvalidationInterval();

   void setSecurityInvalidationInterval(long interval);

   /**
    * Returns whether security is enabled for this server.
    */
   boolean isSecurityEnabled();

   void setSecurityEnabled(boolean enabled);

   boolean isJMXManagementEnabled();

   void setJMXManagementEnabled(boolean enabled);

   String getJMXDomain();

   void setJMXDomain(String domain);

   /**
    * Returns the list of interceptors used by this server.
    * 
    * @see Interceptor
    */
   List<String> getInterceptorClassNames();

   void setInterceptorClassNames(List<String> interceptors);

   /**
    * Returns the connection time to live.
    * <br>
    * This value overrides the connection time to live <em>sent by the client</em>.
    */
   long getConnectionTTLOverride();

   void setConnectionTTLOverride(long ttl);

   boolean isAsyncConnectionExecutionEnabled();

   void setEnabledAsyncConnectionExecution(boolean enabled);

   Set<TransportConfiguration> getAcceptorConfigurations();

   void setAcceptorConfigurations(Set<TransportConfiguration> infos);

   /**
    * Returns the connectors configured for this server.
    */
   Map<String, TransportConfiguration> getConnectorConfigurations();

   void setConnectorConfigurations(Map<String, TransportConfiguration> infos);

   /**
    * Returns the name of the connector used to connect to the backup.
    * <br>
    * If this server has no backup or is itself a backup, the value is {@code null}.
    */
   String getBackupConnectorName();

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

   /**
    * Returns the management address of this server.
    * <br>
    * Clients can send management messages to this address to manage this server.
    */
    SimpleString getManagementAddress();

   void setManagementAddress(SimpleString address);

   /**
    * Returns the management notification address of this server.
    * <br>
    * Clients can bind queues to this address to receive management notifications emitted by this server.
    */
   SimpleString getManagementNotificationAddress();

   void setManagementNotificationAddress(SimpleString address);

   String getManagementClusterUser();

   void setManagementClusterUser(String user);

   String getManagementClusterPassword();

   void setManagementClusterPassword(String password);

   /**
    * Returns the size of the cache for pre-creating message IDs.
    */
   int getIDCacheSize();

   void setIDCacheSize(int idCacheSize);

   /**
    * Returns whether message ID cache is persisted.
    */
   boolean isPersistIDCache();

   void setPersistIDCache(boolean persist);

   String getLogDelegateFactoryClassName();

   void setLogDelegateFactoryClassName(String className);

   // Journal related attributes ------------------------------------------------------------

   /**
    * Returns the file system directory used to store bindings.
    */
   String getBindingsDirectory();

   void setBindingsDirectory(String dir);

   /**
    * Returns the file system directory used to store journal log.
    */
   String getJournalDirectory();

   void setJournalDirectory(String dir);

   /**
    * Returns the type of journal used by this server (either {@code NIO} or {@code ASYNCIO}).
    */
   JournalType getJournalType();

   void setJournalType(JournalType type);

   /**
    * Returns whether the journal is synchronized when receiving transactional data.
    */
   boolean isJournalSyncTransactional();

   void setJournalSyncTransactional(boolean sync);

   /**
    * Returns whether the journal is synchronized when receiving non-transactional data.
    */
   boolean isJournalSyncNonTransactional();

   void setJournalSyncNonTransactional(boolean sync);

   /**
    * Returns the size (in bytes) of each journal files.
    */
   int getJournalFileSize();

   void setJournalFileSize(int size);

   /**
    * Returns the minimal number of journal files before compacting.
    */
   int getJournalCompactMinFiles();

   void setJournalCompactMinFiles(int minFiles);

   /**
    * Return the percentage of live data before compacting the journal.
    */
   int getJournalCompactPercentage();

   void setJournalCompactPercentage(int percentage);

   /**
    * Returns the number of journal files to pre-create.
    */
   int getJournalMinFiles();

   void setJournalMinFiles(int files);

   // AIO and NIO need different values for these params

   /**
    * Returns the maximum number of write requests that can be in the AIO queue at any given time.
    */
   int getJournalMaxIO_AIO();

   void setJournalMaxIO_AIO(int journalMaxIO);

   /**
    * Returns the timeout (in nanoseconds) used to flush buffers in the AIO queueu.
    */
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

   /**
    * Returns whether the bindings directory is created on this server startup.
    */
   boolean isCreateBindingsDir();

   void setCreateBindingsDir(boolean create);

   /**
    * Returns whether the journal directory is created on this server startup.
    */
   boolean isCreateJournalDir();

   void setCreateJournalDir(boolean create);

   boolean isLogJournalWriteRate();

   void setLogJournalWriteRate(boolean rate);

   // Undocumented attributes

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

   /**
    * Returns the file system directory used to store paging files.
    */
   String getPagingDirectory();

   void setPagingDirectory(String dir);

   // Large Messages Properties ------------------------------------------------------------

   /**
    * Returns the file system directory used to store large messages.
    */
   String getLargeMessagesDirectory();

   void setLargeMessagesDirectory(String directory);

   // Other Properties ---------------------------------------------------------------------

   /**
    * Returns whether wildcard routing is supported by this server.
    */
   boolean isWildcardRoutingEnabled();

   void setWildcardRoutingEnabled(boolean enabled);

   /**
    * Returns the timeout (in milliseconds) after which transactions is removed 
    * from the resource manager after it was created.
    */
   long getTransactionTimeout();

   void setTransactionTimeout(long timeout);

   /**
    * Returns whether message counter is enabled for this server.
    */
   boolean isMessageCounterEnabled();

   void setMessageCounterEnabled(boolean enabled);

   /**
    * Returns the sample period (in milliseconds) to take message counter snapshot.
    */
   long getMessageCounterSamplePeriod();

   /**
    * Sets the sample period to take message counter snapshot.
    * 
    * @param newPeriod value must be greater than 1000ms
    */
   void setMessageCounterSamplePeriod(long period);

   /**
    * Returns the maximum number of days kept in memory for message counter.
    */
   int getMessageCounterMaxDayHistory();

   /**
    * Sets the maximum number of days kept in memory for message counter.
    * 
    * @param count value must be greater than 0
    */
   void setMessageCounterMaxDayHistory(int maxDayHistory);

   /**
    * Returns the frequency (in milliseconds)  to scan transactions to detect which transactions 
    * have timed out.
    */
   long getTransactionTimeoutScanPeriod();

   void setTransactionTimeoutScanPeriod(long period);

   /**
    * Returns the frequency (in milliseconds)  to scan messages to detect which messages 
    * have expired.
    */
   long getMessageExpiryScanPeriod();

   void setMessageExpiryScanPeriod(long messageExpiryScanPeriod);

   /**
    * Returns the priority of the thread used to scan message expiration.
    */
   int getMessageExpiryThreadPriority();

   void setMessageExpiryThreadPriority(int messageExpiryThreadPriority);

}
