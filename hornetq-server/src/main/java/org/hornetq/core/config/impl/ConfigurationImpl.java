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

package org.hornetq.core.config.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hornetq.api.config.HornetQDefaultConfiguration;
import org.hornetq.api.core.BroadcastGroupConfiguration;
import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.core.config.BridgeConfiguration;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.ConnectorServiceConfiguration;
import org.hornetq.core.config.CoreQueueConfiguration;
import org.hornetq.core.config.DivertConfiguration;
import org.hornetq.core.journal.impl.JournalConstants;
import org.hornetq.core.security.Role;
import org.hornetq.core.server.JournalType;
import org.hornetq.core.server.group.impl.GroupingHandlerConfiguration;
import org.hornetq.core.settings.impl.AddressSettings;

/**
 * @author <a href="mailto:ataylor@redhat.com>Andy Taylor</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class ConfigurationImpl implements Configuration
{
   // Constants ------------------------------------------------------------------------------

   public static final JournalType DEFAULT_JOURNAL_TYPE = JournalType.ASYNCIO;

   private static final long serialVersionUID = 4077088945050267843L;

   // Attributes -----------------------------------------------------------------------------

   private String name = "ConfigurationImpl::" + System.identityHashCode(this);

   private String nodeGroupName = null;

   protected boolean backup = HornetQDefaultConfiguration.DEFAULT_BACKUP;

   private boolean allowAutoFailBack = HornetQDefaultConfiguration.DEFAULT_ALLOW_AUTO_FAILBACK;

   private boolean sharedStore = HornetQDefaultConfiguration.DEFAULT_SHARED_STORE;

   protected boolean fileDeploymentEnabled = HornetQDefaultConfiguration.DEFAULT_FILE_DEPLOYMENT_ENABLED;

   private boolean persistenceEnabled = HornetQDefaultConfiguration.DEFAULT_PERSISTENCE_ENABLED;

   protected long fileDeploymentScanPeriod = HornetQDefaultConfiguration.DEFAULT_FILE_DEPLOYER_SCAN_PERIOD;

   private boolean persistDeliveryCountBeforeDelivery =
            HornetQDefaultConfiguration.DEFAULT_PERSIST_DELIVERY_COUNT_BEFORE_DELIVERY;

   private int scheduledThreadPoolMaxSize = HornetQDefaultConfiguration.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE;

   private int threadPoolMaxSize = HornetQDefaultConfiguration.DEFAULT_THREAD_POOL_MAX_SIZE;

   private long securityInvalidationInterval = HornetQDefaultConfiguration.DEFAULT_SECURITY_INVALIDATION_INTERVAL;

   private boolean securityEnabled = HornetQDefaultConfiguration.DEFAULT_SECURITY_ENABLED;

   protected boolean jmxManagementEnabled = HornetQDefaultConfiguration.DEFAULT_JMX_MANAGEMENT_ENABLED;

   protected String jmxDomain = HornetQDefaultConfiguration.DEFAULT_JMX_DOMAIN;

   protected long connectionTTLOverride = HornetQDefaultConfiguration.DEFAULT_CONNECTION_TTL_OVERRIDE;

   protected boolean asyncConnectionExecutionEnabled = HornetQDefaultConfiguration.DEFAULT_ASYNC_CONNECTION_EXECUTION_ENABLED;

   private long messageExpiryScanPeriod = HornetQDefaultConfiguration.DEFAULT_MESSAGE_EXPIRY_SCAN_PERIOD;

   private int messageExpiryThreadPriority = HornetQDefaultConfiguration.DEFAULT_MESSAGE_EXPIRY_THREAD_PRIORITY;

   protected int idCacheSize = HornetQDefaultConfiguration.DEFAULT_ID_CACHE_SIZE;

   private boolean persistIDCache = HornetQDefaultConfiguration.DEFAULT_PERSIST_ID_CACHE;

   private List<String> interceptorClassNames = new ArrayList<String>();

   protected Map<String, TransportConfiguration> connectorConfigs = new HashMap<String, TransportConfiguration>();

   private Set<TransportConfiguration> acceptorConfigs = new HashSet<TransportConfiguration>();

   protected List<BridgeConfiguration> bridgeConfigurations = new ArrayList<BridgeConfiguration>();

   protected List<DivertConfiguration> divertConfigurations = new ArrayList<DivertConfiguration>();

   protected List<ClusterConnectionConfiguration> clusterConfigurations = new ArrayList<ClusterConnectionConfiguration>();

   private List<CoreQueueConfiguration> queueConfigurations = new ArrayList<CoreQueueConfiguration>();

   protected List<BroadcastGroupConfiguration> broadcastGroupConfigurations = new ArrayList<BroadcastGroupConfiguration>();

   protected Map<String, DiscoveryGroupConfiguration> discoveryGroupConfigurations = new LinkedHashMap<String, DiscoveryGroupConfiguration>();

   // Paging related attributes ------------------------------------------------------------

   private String pagingDirectory = HornetQDefaultConfiguration.DEFAULT_PAGING_DIR;

   // File related attributes -----------------------------------------------------------

   private int maxConcurrentPageIO = HornetQDefaultConfiguration.DEFAULT_MAX_CONCURRENT_PAGE_IO;

   protected String largeMessagesDirectory = HornetQDefaultConfiguration.DEFAULT_LARGE_MESSAGES_DIR;

   protected String bindingsDirectory = HornetQDefaultConfiguration.DEFAULT_BINDINGS_DIRECTORY;

   protected boolean createBindingsDir = HornetQDefaultConfiguration.DEFAULT_CREATE_BINDINGS_DIR;

   protected String journalDirectory = HornetQDefaultConfiguration.DEFAULT_JOURNAL_DIR;

   protected boolean createJournalDir = HornetQDefaultConfiguration.DEFAULT_CREATE_JOURNAL_DIR;

   public JournalType journalType = ConfigurationImpl.DEFAULT_JOURNAL_TYPE;

   protected boolean journalSyncTransactional = HornetQDefaultConfiguration.DEFAULT_JOURNAL_SYNC_TRANSACTIONAL;

   protected boolean journalSyncNonTransactional = HornetQDefaultConfiguration.DEFAULT_JOURNAL_SYNC_NON_TRANSACTIONAL;

   protected int journalCompactMinFiles = HornetQDefaultConfiguration.DEFAULT_JOURNAL_COMPACT_MIN_FILES;

   protected int journalCompactPercentage = HornetQDefaultConfiguration.DEFAULT_JOURNAL_COMPACT_PERCENTAGE;

   protected int journalFileSize = HornetQDefaultConfiguration.DEFAULT_JOURNAL_FILE_SIZE;

   protected int journalMinFiles = HornetQDefaultConfiguration.DEFAULT_JOURNAL_MIN_FILES;

   // AIO and NIO need different values for these attributes

   protected int journalMaxIO_AIO = HornetQDefaultConfiguration.DEFAULT_JOURNAL_MAX_IO_AIO;

   protected int journalBufferTimeout_AIO = HornetQDefaultConfiguration.DEFAULT_JOURNAL_BUFFER_TIMEOUT_AIO;

   protected int journalBufferSize_AIO = HornetQDefaultConfiguration.DEFAULT_JOURNAL_BUFFER_SIZE_AIO;

   protected int journalMaxIO_NIO = HornetQDefaultConfiguration.DEFAULT_JOURNAL_MAX_IO_NIO;

   protected int journalBufferTimeout_NIO = HornetQDefaultConfiguration.DEFAULT_JOURNAL_BUFFER_TIMEOUT_NIO;

   protected int journalBufferSize_NIO = HornetQDefaultConfiguration.DEFAULT_JOURNAL_BUFFER_SIZE_NIO;

   protected boolean logJournalWriteRate = HornetQDefaultConfiguration.DEFAULT_JOURNAL_LOG_WRITE_RATE;

   protected int journalPerfBlastPages = HornetQDefaultConfiguration.DEFAULT_JOURNAL_PERF_BLAST_PAGES;

   protected boolean runSyncSpeedTest = HornetQDefaultConfiguration.DEFAULT_RUN_SYNC_SPEED_TEST;

   private boolean wildcardRoutingEnabled = HornetQDefaultConfiguration.DEFAULT_WILDCARD_ROUTING_ENABLED;

   private boolean messageCounterEnabled = HornetQDefaultConfiguration.DEFAULT_MESSAGE_COUNTER_ENABLED;

   private long messageCounterSamplePeriod = HornetQDefaultConfiguration.DEFAULT_MESSAGE_COUNTER_SAMPLE_PERIOD;

   private int messageCounterMaxDayHistory = HornetQDefaultConfiguration.DEFAULT_MESSAGE_COUNTER_MAX_DAY_HISTORY;

   private long transactionTimeout = HornetQDefaultConfiguration.DEFAULT_TRANSACTION_TIMEOUT;

   private long transactionTimeoutScanPeriod = HornetQDefaultConfiguration.DEFAULT_TRANSACTION_TIMEOUT_SCAN_PERIOD;

   private SimpleString managementAddress = HornetQDefaultConfiguration.DEFAULT_MANAGEMENT_ADDRESS;

   private SimpleString managementNotificationAddress = HornetQDefaultConfiguration.DEFAULT_MANAGEMENT_NOTIFICATION_ADDRESS;

   protected String clusterUser = HornetQDefaultConfiguration.DEFAULT_CLUSTER_USER;

   protected String clusterPassword = HornetQDefaultConfiguration.DEFAULT_CLUSTER_PASSWORD;

   private long serverDumpInterval = HornetQDefaultConfiguration.DEFAULT_SERVER_DUMP_INTERVAL;

   protected boolean failoverOnServerShutdown = HornetQDefaultConfiguration.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN;

   // percentage of free memory which triggers warning from the memory manager
   private int memoryWarningThreshold = HornetQDefaultConfiguration.DEFAULT_MEMORY_WARNING_THRESHOLD;

   private long memoryMeasureInterval = HornetQDefaultConfiguration.DEFAULT_MEMORY_MEASURE_INTERVAL;

   protected GroupingHandlerConfiguration groupingHandlerConfiguration;

   private Map<String, AddressSettings> addressesSettings = new HashMap<String, AddressSettings>();

   private Map<String, Set<Role>> securitySettings = new HashMap<String, Set<Role>>();

   protected List<ConnectorServiceConfiguration> connectorServiceConfigurations = new ArrayList<ConnectorServiceConfiguration>();

   private long failbackDelay = HornetQDefaultConfiguration.DEFAULT_FAILBACK_DELAY;

   private boolean checkForLiveServer = HornetQDefaultConfiguration.DEFAULT_CHECK_FOR_LIVE_SERVER;

   private boolean maskPassword = HornetQDefaultConfiguration.DEFAULT_MASK_PASSWORD;

   private transient String passwordCodec;

   private String replicationClusterName;

   // Public -------------------------------------------------------------------------

   public boolean isClustered()
   {
      return !getClusterConfigurations().isEmpty();
   }

   public boolean isAllowAutoFailBack()
   {
      return allowAutoFailBack;
   }

   public void setAllowAutoFailBack(boolean allowAutoFailBack)
   {
      this.allowAutoFailBack = allowAutoFailBack;
   }

   public boolean isBackup()
   {
      return backup;
   }

   public boolean isFileDeploymentEnabled()
   {
      return fileDeploymentEnabled;
   }

   public void setFileDeploymentEnabled(final boolean enable)
   {
      fileDeploymentEnabled = enable;
   }

   public boolean isPersistenceEnabled()
   {
      return persistenceEnabled;
   }

   public void setPersistenceEnabled(final boolean enable)
   {
      persistenceEnabled = enable;
   }

   public long getFileDeployerScanPeriod()
   {
      return fileDeploymentScanPeriod;
   }

   public void setFileDeployerScanPeriod(final long period)
   {
      fileDeploymentScanPeriod = period;
   }

   /**
    * @return the persistDeliveryCountBeforeDelivery
    */
   public boolean isPersistDeliveryCountBeforeDelivery()
   {
      return persistDeliveryCountBeforeDelivery;
   }

   public void setPersistDeliveryCountBeforeDelivery(final boolean persistDeliveryCountBeforeDelivery)
   {
      this.persistDeliveryCountBeforeDelivery = persistDeliveryCountBeforeDelivery;
   }

   public void setBackup(final boolean backup)
   {
      this.backup = backup;
   }

   public boolean isSharedStore()
   {
      return sharedStore;
   }

   public void setSharedStore(final boolean sharedStore)
   {
      this.sharedStore = sharedStore;
   }

   public int getScheduledThreadPoolMaxSize()
   {
      return scheduledThreadPoolMaxSize;
   }

   public void setScheduledThreadPoolMaxSize(final int maxSize)
   {
      scheduledThreadPoolMaxSize = maxSize;
   }

   public int getThreadPoolMaxSize()
   {
      return threadPoolMaxSize;
   }

   public void setThreadPoolMaxSize(final int maxSize)
   {
      threadPoolMaxSize = maxSize;
   }

   public long getSecurityInvalidationInterval()
   {
      return securityInvalidationInterval;
   }

   public void setSecurityInvalidationInterval(final long interval)
   {
      securityInvalidationInterval = interval;
   }

   public long getConnectionTTLOverride()
   {
      return connectionTTLOverride;
   }

   public void setConnectionTTLOverride(final long ttl)
   {
      connectionTTLOverride = ttl;
   }

   public boolean isAsyncConnectionExecutionEnabled()
   {
      return asyncConnectionExecutionEnabled;
   }

   public void setEnabledAsyncConnectionExecution(final boolean enabled)
   {
      asyncConnectionExecutionEnabled = enabled;
   }

   public List<String> getInterceptorClassNames()
   {
      return interceptorClassNames;
   }

   public void setInterceptorClassNames(final List<String> interceptors)
   {
      interceptorClassNames = interceptors;
   }

   public Set<TransportConfiguration> getAcceptorConfigurations()
   {
      return acceptorConfigs;
   }

   public void setAcceptorConfigurations(final Set<TransportConfiguration> infos)
   {
      acceptorConfigs = infos;
   }

   public Map<String, TransportConfiguration> getConnectorConfigurations()
   {
      return connectorConfigs;
   }

   public void setConnectorConfigurations(final Map<String, TransportConfiguration> infos)
   {
      connectorConfigs = infos;
   }

   public GroupingHandlerConfiguration getGroupingHandlerConfiguration()
   {
      return groupingHandlerConfiguration;
   }

   public void setGroupingHandlerConfiguration(final GroupingHandlerConfiguration groupingHandlerConfiguration)
   {
      this.groupingHandlerConfiguration = groupingHandlerConfiguration;
   }

   public List<BridgeConfiguration> getBridgeConfigurations()
   {
      return bridgeConfigurations;
   }

   public void setBridgeConfigurations(final List<BridgeConfiguration> configs)
   {
      bridgeConfigurations = configs;
   }

   public List<BroadcastGroupConfiguration> getBroadcastGroupConfigurations()
   {
      return broadcastGroupConfigurations;
   }

   public void setBroadcastGroupConfigurations(final List<BroadcastGroupConfiguration> configs)
   {
      broadcastGroupConfigurations = configs;
   }

   public List<ClusterConnectionConfiguration> getClusterConfigurations()
   {
      return clusterConfigurations;
   }

   public void setClusterConfigurations(final List<ClusterConnectionConfiguration> configs)
   {
      clusterConfigurations = configs;
   }

   public List<DivertConfiguration> getDivertConfigurations()
   {
      return divertConfigurations;
   }

   public void setDivertConfigurations(final List<DivertConfiguration> configs)
   {
      divertConfigurations = configs;
   }

   public List<CoreQueueConfiguration> getQueueConfigurations()
   {
      return queueConfigurations;
   }

   public void setQueueConfigurations(final List<CoreQueueConfiguration> configs)
   {
      queueConfigurations = configs;
   }

   public Map<String, DiscoveryGroupConfiguration> getDiscoveryGroupConfigurations()
   {
      return discoveryGroupConfigurations;
   }

   public void setDiscoveryGroupConfigurations(final Map<String, DiscoveryGroupConfiguration> discoveryGroupConfigurations)
   {
      this.discoveryGroupConfigurations = discoveryGroupConfigurations;
   }

   public int getIDCacheSize()
   {
      return idCacheSize;
   }

   public void setIDCacheSize(final int idCacheSize)
   {
      this.idCacheSize = idCacheSize;
   }

   public boolean isPersistIDCache()
   {
      return persistIDCache;
   }

   public void setPersistIDCache(final boolean persist)
   {
      persistIDCache = persist;
   }

   public String getBindingsDirectory()
   {
      return bindingsDirectory;
   }

   public void setBindingsDirectory(final String dir)
   {
      bindingsDirectory = dir;
   }


   /* (non-Javadoc)
    * @see org.hornetq.core.config.Configuration#getPageMaxConcurrentIO()
    */
   public int getPageMaxConcurrentIO()
   {
      return maxConcurrentPageIO;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.config.Configuration#setPageMaxConcurrentIO(int)
    */
   public void setPageMaxConcurrentIO(int maxIO)
   {
      this.maxConcurrentPageIO = maxIO;
   }


   public String getJournalDirectory()
   {
      return journalDirectory;
   }

   public void setJournalDirectory(final String dir)
   {
      journalDirectory = dir;
   }

   public JournalType getJournalType()
   {
      return journalType;
   }

   public void setPagingDirectory(final String dir)
   {
      pagingDirectory = dir;
   }

   public String getPagingDirectory()
   {
      return pagingDirectory;
   }

   public void setJournalType(final JournalType type)
   {
      journalType = type;
   }

   public boolean isJournalSyncTransactional()
   {
      return journalSyncTransactional;
   }

   public void setJournalSyncTransactional(final boolean sync)
   {
      journalSyncTransactional = sync;
   }

   public boolean isJournalSyncNonTransactional()
   {
      return journalSyncNonTransactional;
   }

   public void setJournalSyncNonTransactional(final boolean sync)
   {
      journalSyncNonTransactional = sync;
   }

   public int getJournalFileSize()
   {
      return journalFileSize;
   }

   public void setJournalFileSize(final int size)
   {
      journalFileSize = size;
   }

   public int getJournalMinFiles()
   {
      return journalMinFiles;
   }

   public void setJournalMinFiles(final int files)
   {
      journalMinFiles = files;
   }

   public boolean isLogJournalWriteRate()
   {
      return logJournalWriteRate;
   }

   public void setLogJournalWriteRate(final boolean logJournalWriteRate)
   {
      this.logJournalWriteRate = logJournalWriteRate;
   }

   public int getJournalPerfBlastPages()
   {
      return journalPerfBlastPages;
   }

   public void setJournalPerfBlastPages(final int journalPerfBlastPages)
   {
      this.journalPerfBlastPages = journalPerfBlastPages;
   }

   public boolean isRunSyncSpeedTest()
   {
      return runSyncSpeedTest;
   }

   public void setRunSyncSpeedTest(final boolean run)
   {
      runSyncSpeedTest = run;
   }

   public boolean isCreateBindingsDir()
   {
      return createBindingsDir;
   }

   public void setCreateBindingsDir(final boolean create)
   {
      createBindingsDir = create;
   }

   public boolean isCreateJournalDir()
   {
      return createJournalDir;
   }

   public void setCreateJournalDir(final boolean create)
   {
      createJournalDir = create;
   }

   public boolean isWildcardRoutingEnabled()
   {
      return wildcardRoutingEnabled;
   }

   public void setWildcardRoutingEnabled(final boolean enabled)
   {
      wildcardRoutingEnabled = enabled;
   }

   public long getTransactionTimeout()
   {
      return transactionTimeout;
   }

   public void setTransactionTimeout(final long timeout)
   {
      transactionTimeout = timeout;
   }

   public long getTransactionTimeoutScanPeriod()
   {
      return transactionTimeoutScanPeriod;
   }

   public void setTransactionTimeoutScanPeriod(final long period)
   {
      transactionTimeoutScanPeriod = period;
   }

   public long getMessageExpiryScanPeriod()
   {
      return messageExpiryScanPeriod;
   }

   public void setMessageExpiryScanPeriod(final long messageExpiryScanPeriod)
   {
      this.messageExpiryScanPeriod = messageExpiryScanPeriod;
   }

   public int getMessageExpiryThreadPriority()
   {
      return messageExpiryThreadPriority;
   }

   public void setMessageExpiryThreadPriority(final int messageExpiryThreadPriority)
   {
      this.messageExpiryThreadPriority = messageExpiryThreadPriority;
   }

   public boolean isSecurityEnabled()
   {
      return securityEnabled;
   }

   public void setSecurityEnabled(final boolean enabled)
   {
      securityEnabled = enabled;
   }

   public boolean isJMXManagementEnabled()
   {
      return jmxManagementEnabled;
   }

   public void setJMXManagementEnabled(final boolean enabled)
   {
      jmxManagementEnabled = enabled;
   }

   public String getJMXDomain()
   {
      return jmxDomain;
   }

   public void setJMXDomain(final String domain)
   {
      jmxDomain = domain;
   }

   public String getLargeMessagesDirectory()
   {
      return largeMessagesDirectory;
   }

   public void setLargeMessagesDirectory(final String directory)
   {
      largeMessagesDirectory = directory;
   }

   public boolean isMessageCounterEnabled()
   {
      return messageCounterEnabled;
   }

   public void setMessageCounterEnabled(final boolean enabled)
   {
      messageCounterEnabled = enabled;
   }

   public long getMessageCounterSamplePeriod()
   {
      return messageCounterSamplePeriod;
   }

   public void setMessageCounterSamplePeriod(final long period)
   {
      messageCounterSamplePeriod = period;
   }

   public int getMessageCounterMaxDayHistory()
   {
      return messageCounterMaxDayHistory;
   }

   public void setMessageCounterMaxDayHistory(final int maxDayHistory)
   {
      messageCounterMaxDayHistory = maxDayHistory;
   }

   public SimpleString getManagementAddress()
   {
      return managementAddress;
   }

   public void setManagementAddress(final SimpleString address)
   {
      managementAddress = address;
   }

   public SimpleString getManagementNotificationAddress()
   {
      return managementNotificationAddress;
   }

   public void setManagementNotificationAddress(final SimpleString address)
   {
      managementNotificationAddress = address;
   }

   public String getClusterUser()
   {
      return clusterUser;
   }

   public void setClusterUser(final String user)
   {
      clusterUser = user;
   }

   public String getClusterPassword()
   {
      return clusterPassword;
   }

   public boolean isFailoverOnServerShutdown()
   {
      return failoverOnServerShutdown;
   }

   public void setFailoverOnServerShutdown(boolean failoverOnServerShutdown)
   {
      this.failoverOnServerShutdown = failoverOnServerShutdown;
   }

   public void setClusterPassword(final String theclusterPassword)
   {
      clusterPassword = theclusterPassword;
   }

   public int getJournalCompactMinFiles()
   {
      return journalCompactMinFiles;
   }

   public int getJournalCompactPercentage()
   {
      return journalCompactPercentage;
   }

   public void setJournalCompactMinFiles(final int minFiles)
   {
      journalCompactMinFiles = minFiles;
   }

   public void setJournalCompactPercentage(final int percentage)
   {
      journalCompactPercentage = percentage;
   }

   public long getServerDumpInterval()
   {
      return serverDumpInterval;
   }

   public void setServerDumpInterval(final long intervalInMilliseconds)
   {
      serverDumpInterval = intervalInMilliseconds;
   }

   public int getMemoryWarningThreshold()
   {
      return memoryWarningThreshold;
   }

   public void setMemoryWarningThreshold(final int memoryWarningThreshold)
   {
      this.memoryWarningThreshold = memoryWarningThreshold;
   }

   public long getMemoryMeasureInterval()
   {
      return memoryMeasureInterval;
   }

   public void setMemoryMeasureInterval(final long memoryMeasureInterval)
   {
      this.memoryMeasureInterval = memoryMeasureInterval;
   }

   public int getJournalMaxIO_AIO()
   {
      return journalMaxIO_AIO;
   }

   public void setJournalMaxIO_AIO(final int journalMaxIO)
   {
      journalMaxIO_AIO = journalMaxIO;
   }

   public int getJournalBufferTimeout_AIO()
   {
      return journalBufferTimeout_AIO;
   }

   public void setJournalBufferTimeout_AIO(final int journalBufferTimeout)
   {
      journalBufferTimeout_AIO = journalBufferTimeout;
   }

   public int getJournalBufferSize_AIO()
   {
      return journalBufferSize_AIO;
   }

   public void setJournalBufferSize_AIO(final int journalBufferSize)
   {
      journalBufferSize_AIO = journalBufferSize;
   }

   public int getJournalMaxIO_NIO()
   {
      return journalMaxIO_NIO;
   }

   public void setJournalMaxIO_NIO(final int journalMaxIO)
   {
      journalMaxIO_NIO = journalMaxIO;
   }

   public int getJournalBufferTimeout_NIO()
   {
      return journalBufferTimeout_NIO;
   }

   public void setJournalBufferTimeout_NIO(final int journalBufferTimeout)
   {
      journalBufferTimeout_NIO = journalBufferTimeout;
   }

   public int getJournalBufferSize_NIO()
   {
      return journalBufferSize_NIO;
   }

   public void setJournalBufferSize_NIO(final int journalBufferSize)
   {
      journalBufferSize_NIO = journalBufferSize;
   }

   @Override
   public boolean equals(final Object obj)
   {
      if (this == obj)
      {
         return true;
      }
      if (obj == null)
      {
         return false;
      }
      if (getClass() != obj.getClass())
      {
         return false;
      }
      ConfigurationImpl other = (ConfigurationImpl)obj;
      if (asyncConnectionExecutionEnabled != other.asyncConnectionExecutionEnabled)
      {
         return false;
      }
      if (backup != other.backup)
      {
         return false;
      }
      if (sharedStore != other.sharedStore)
      {
         return false;
      }
      if (bindingsDirectory == null)
      {
         if (other.bindingsDirectory != null)
         {
            return false;
         }
      }
      else if (!bindingsDirectory.equals(other.bindingsDirectory))
      {
         return false;
      }
      if (connectionTTLOverride != other.connectionTTLOverride)
      {
         return false;
      }
      if (createBindingsDir != other.createBindingsDir)
      {
         return false;
      }
      if (createJournalDir != other.createJournalDir)
      {
         return false;
      }

      if (fileDeploymentEnabled != other.fileDeploymentEnabled)
      {
         return false;
      }
      if (fileDeploymentScanPeriod != other.fileDeploymentScanPeriod)
      {
         return false;
      }
      if (idCacheSize != other.idCacheSize)
      {
         return false;
      }
      if (jmxManagementEnabled != other.jmxManagementEnabled)
      {
         return false;
      }
      if (journalBufferSize_AIO != other.journalBufferSize_AIO)
      {
         return false;
      }
      if (journalBufferTimeout_AIO != other.journalBufferTimeout_AIO)
      {
         return false;
      }
      if (journalMaxIO_AIO != other.journalMaxIO_AIO)
      {
         return false;
      }
      if (journalBufferSize_NIO != other.journalBufferSize_NIO)
      {
         return false;
      }
      if (journalBufferTimeout_NIO != other.journalBufferTimeout_NIO)
      {
         return false;
      }
      if (journalMaxIO_NIO != other.journalMaxIO_NIO)
      {
         return false;
      }
      if (journalCompactMinFiles != other.journalCompactMinFiles)
      {
         return false;
      }
      if (journalCompactPercentage != other.journalCompactPercentage)
      {
         return false;
      }
      if (journalDirectory == null)
      {
         if (other.journalDirectory != null)
         {
            return false;
         }
      }
      else if (!journalDirectory.equals(other.journalDirectory))
      {
         return false;
      }
      if (journalFileSize != other.journalFileSize)
      {
         return false;
      }

      if (journalMinFiles != other.journalMinFiles)
      {
         return false;
      }
      if (journalPerfBlastPages != other.journalPerfBlastPages)
      {
         return false;
      }
      if (journalSyncNonTransactional != other.journalSyncNonTransactional)
      {
         return false;
      }
      if (journalSyncTransactional != other.journalSyncTransactional)
      {
         return false;
      }
      if (journalType == null)
      {
         if (other.journalType != null)
         {
            return false;
         }
      }
      else if (!journalType.equals(other.journalType))
      {
         return false;
      }
      if (largeMessagesDirectory == null)
      {
         if (other.largeMessagesDirectory != null)
         {
            return false;
         }
      }
      else if (!largeMessagesDirectory.equals(other.largeMessagesDirectory))
      {
         return false;
      }
      if (logJournalWriteRate != other.logJournalWriteRate)
      {
         return false;
      }
      if (managementAddress == null)
      {
         if (other.managementAddress != null)
         {
            return false;
         }
      }
      else if (!managementAddress.equals(other.managementAddress))
      {
         return false;
      }
      if (failoverOnServerShutdown != other.isFailoverOnServerShutdown())
      {
         return false;
      }
      if (clusterPassword == null)
      {
         if (other.clusterPassword != null)
         {
            return false;
         }
      }
      else if (!clusterPassword.equals(other.clusterPassword))
      {
         return false;
      }
      if (clusterUser == null)
      {
         if (other.clusterUser != null)
         {
            return false;
         }
      }
      else if (!clusterUser.equals(other.clusterUser))
      {
         return false;
      }
      if (managementNotificationAddress == null)
      {
         if (other.managementNotificationAddress != null)
         {
            return false;
         }
      }
      else if (!managementNotificationAddress.equals(other.managementNotificationAddress))
      {
         return false;
      }
      if (messageCounterEnabled != other.messageCounterEnabled)
      {
         return false;
      }
      if (messageCounterMaxDayHistory != other.messageCounterMaxDayHistory)
      {
         return false;
      }
      if (messageCounterSamplePeriod != other.messageCounterSamplePeriod)
      {
         return false;
      }
      if (messageExpiryScanPeriod != other.messageExpiryScanPeriod)
      {
         return false;
      }
      if (messageExpiryThreadPriority != other.messageExpiryThreadPriority)
      {
         return false;
      }
      if (pagingDirectory == null)
      {
         if (other.pagingDirectory != null)
         {
            return false;
         }
      }
      else if (!pagingDirectory.equals(other.pagingDirectory))
      {
         return false;
      }
      if (persistDeliveryCountBeforeDelivery != other.persistDeliveryCountBeforeDelivery)
      {
         return false;
      }
      if (persistIDCache != other.persistIDCache)
      {
         return false;
      }
      if (persistenceEnabled != other.persistenceEnabled)
      {
         return false;
      }
      if (scheduledThreadPoolMaxSize != other.scheduledThreadPoolMaxSize)
      {
         return false;
      }
      if (securityEnabled != other.securityEnabled)
      {
         return false;
      }
      if (securityInvalidationInterval != other.securityInvalidationInterval)
      {
         return false;
      }
      if (serverDumpInterval != other.serverDumpInterval)
      {
         return false;
      }
      if (threadPoolMaxSize != other.threadPoolMaxSize)
      {
         return false;
      }
      if (transactionTimeout != other.transactionTimeout)
      {
         return false;
      }
      if (transactionTimeoutScanPeriod != other.transactionTimeoutScanPeriod)
      {
         return false;
      }
      if (wildcardRoutingEnabled != other.wildcardRoutingEnabled)
      {
         return false;
      }

      return true;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.config.Configuration#getAddressesSettings()
    */
   public Map<String, AddressSettings> getAddressesSettings()
   {
      return addressesSettings;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.config.Configuration#setAddressesSettings(java.util.Map)
    */
   public void setAddressesSettings(final Map<String, AddressSettings> addressesSettings)
   {
      this.addressesSettings = addressesSettings;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.config.Configuration#getSecurityRoles()
    */
   public Map<String, Set<Role>> getSecurityRoles()
   {
      return securitySettings;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.config.Configuration#setSecuritySettings(java.util.Map)
    */
   public void setSecurityRoles(final Map<String, Set<Role>> securitySettings)
   {
      this.securitySettings = securitySettings;
   }

   public List<ConnectorServiceConfiguration> getConnectorServiceConfigurations()
   {
      return this.connectorServiceConfigurations;
   }

   public long getFailbackDelay()
   {
      return failbackDelay;
   }

   public void setFailbackDelay(long failbackDelay)
   {
      this.failbackDelay = failbackDelay;
   }

   public boolean isCheckForLiveServer()
   {
      return checkForLiveServer;
   }

   public void setCheckForLiveServer(boolean checkForLiveServer)
   {
      this.checkForLiveServer = checkForLiveServer;
   }

   public void setConnectorServiceConfigurations(final List<ConnectorServiceConfiguration> configs)
   {
      this.connectorServiceConfigurations = configs;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.config.Configuration#getName()
    */
   public String getName()
   {
      return name;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.config.Configuration#setName(java.lang.String)
    */
   public void setName(String name)
   {
      this.name = name;
   }

   public String getBackupGroupName()
   {
      return nodeGroupName;
   }

   public void setBackupGroupName(String nodeGroupName)
   {
      this.nodeGroupName = nodeGroupName;
   }

   @Override
   public String toString()
   {
      StringBuffer sb = new StringBuffer("HornetQ Configuration (");
      sb.append("clustered=").append(isClustered()).append(",");
      sb.append("backup=").append(backup).append(",");
      sb.append("sharedStore=").append(sharedStore).append(",");
      sb.append("journalDirectory=").append(journalDirectory).append(",");
      sb.append("bindingsDirectory=").append(bindingsDirectory).append(",");
      sb.append("largeMessagesDirectory=").append(largeMessagesDirectory).append(",");
      sb.append("pagingDirectory=").append(pagingDirectory);
      sb.append(")");
      return sb.toString();
   }

   public boolean isMaskPassword()
   {
      return maskPassword;
   }

   public void setMaskPassword(boolean maskPassword)
   {
      this.maskPassword = maskPassword;
   }

   public void setPasswordCodec(String codec)
   {
      passwordCodec = codec;
   }

   public String getPasswordCodec()
   {
      return passwordCodec;
   }

   @Override
   public void setReplicationClustername(String clusterName)
   {
      this.replicationClusterName = clusterName;
   }

   @Override
   public String getReplicationClustername()
   {
      return replicationClusterName;
   }

}
