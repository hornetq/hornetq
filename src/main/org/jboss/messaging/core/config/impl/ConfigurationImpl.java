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

package org.jboss.messaging.core.config.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.cluster.BridgeConfiguration;
import org.jboss.messaging.core.config.cluster.BroadcastGroupConfiguration;
import org.jboss.messaging.core.config.cluster.ClusterConnectionConfiguration;
import org.jboss.messaging.core.config.cluster.DiscoveryGroupConfiguration;
import org.jboss.messaging.core.config.cluster.DivertConfiguration;
import org.jboss.messaging.core.config.cluster.QueueConfiguration;
import org.jboss.messaging.core.server.JournalType;
import org.jboss.messaging.utils.SimpleString;

/**
 * @author <a href="mailto:ataylor@redhat.com>Andy Taylor</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class ConfigurationImpl implements Configuration
{
   // Constants ------------------------------------------------------------------------------

   private static final long serialVersionUID = 4077088945050267843L;

   public static final boolean DEFAULT_CLUSTERED = false;

   public static final boolean DEFAULT_PERSIST_DELIVERY_COUNT_BEFORE_DELIVERY = false;

   public static final boolean DEFAULT_BACKUP = false;

   public static final boolean DEFAULT_ENABLE_FILE_DEPLOYMENT = false;
   
   public static final boolean DEFAULT_ENABLE_PERSISTENCE = true;

   public static final long DEFAULT_FILE_DEPLOYER_SCAN_PERIOD = 5000;

   public static final long DEFAULT_QUEUE_ACTIVATION_TIMEOUT = 30000;

   public static final int DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE = 30;

   public static final long DEFAULT_SECURITY_INVALIDATION_INTERVAL = 10000;

   public static final boolean DEFAULT_SECURITY_ENABLED = true;

   public static final boolean DEFAULT_JMX_MANAGEMENT_ENABLED = true;

   public static final long DEFAULT_CONNECTION_SCAN_PERIOD = 1000;

   public static final long DEFAULT_CONNECTION_TTL_OVERRIDE = -1;

   public static final String DEFAULT_BINDINGS_DIRECTORY = "data/bindings";

   public static final boolean DEFAULT_CREATE_BINDINGS_DIR = true;

   public static final String DEFAULT_JOURNAL_DIR = "data/journal";

   public static final String DEFAULT_PAGING_DIR = "data/paging";

   public static final int DEFAULT_PAGE_MAX_THREADS = 10;

   public static final int DEFAULT_PAGE_WATERMARK_SIZE = 10 * 1024 * 1024;

   public static final long DEFAULT_PAGE_MAX_GLOBAL_SIZE = -1;

   public static final String DEFAULT_LARGE_MESSAGES_DIR = "data/largemessages";

   public static final boolean DEFAULT_CREATE_JOURNAL_DIR = true;

   public static final JournalType DEFAULT_JOURNAL_TYPE = JournalType.ASYNCIO;

   public static final boolean DEFAULT_JOURNAL_SYNC_TRANSACTIONAL = true;

   public static final boolean DEFAULT_JOURNAL_SYNC_NON_TRANSACTIONAL = false;

   public static final int DEFAULT_JOURNAL_FILE_SIZE = 10485760;

   public static final int DEFAULT_JOURNAL_MIN_FILES = 10;

   public static final int DEFAULT_JOURNAL_MAX_AIO = 5000;

   public static final int DEFAULT_JOURNAL_REUSE_BUFFER_SIZE = -1;

   public static final boolean DEFAULT_WILDCARD_ROUTING_ENABLED = true;

   public static final boolean DEFAULT_MESSAGE_COUNTER_ENABLED = false;

   public static final long DEFAULT_MESSAGE_COUNTER_SAMPLE_PERIOD = 10000;
   
   public static final int DEFAULT_MESSAGE_COUNTER_MAX_DAY_HISTORY = 10;
   
   public static final long DEFAULT_TRANSACTION_TIMEOUT = 60000;

   public static final long DEFAULT_TRANSACTION_TIMEOUT_SCAN_PERIOD = 1000;

   public static final SimpleString DEFAULT_MANAGEMENT_ADDRESS = new SimpleString("jbm.management");

   public static final SimpleString DEFAULT_MANAGEMENT_NOTIFICATION_ADDRESS = new SimpleString("jbm.notifications");

   public static final String DEFAULT_MANAGEMENT_CLUSTER_PASSWORD = "CHANGE ME!!";

   public static final long DEFAULT_MANAGEMENT_REQUEST_TIMEOUT = 5000;

   public static final long DEFAULT_BROADCAST_PERIOD = 5000;

   public static final long DEFAULT_BROADCAST_REFRESH_TIMEOUT = 10000;

   public static final long DEFAULT_MESSAGE_EXPIRY_SCAN_PERIOD = 30000;

   public static final int DEFAULT_MESSAGE_EXPIRY_THREAD_PRIORITY = 3;

   public static final int DEFAULT_ID_CACHE_SIZE = 2000;

   public static final boolean DEFAULT_PERSIST_ID_CACHE = true;

   public static final boolean DEFAULT_CLUSTER_DUPLICATE_DETECTION = true;

   public static final boolean DEFAULT_CLUSTER_FORWARD_WHEN_NO_CONSUMERS = false;

   public static final int DEFAULT_CLUSTER_MAX_HOPS = 1;

   public static final boolean DEFAULT_DIVERT_EXCLUSIVE = false;

   public static final boolean DEFAULT_BRIDGE_DUPLICATE_DETECTION = true;

   // Attributes -----------------------------------------------------------------------------

   protected boolean clustered = DEFAULT_CLUSTERED;

   protected boolean backup = DEFAULT_BACKUP;

   protected boolean enableFileDeployment = DEFAULT_ENABLE_FILE_DEPLOYMENT;
   
   protected boolean enablePersistence = DEFAULT_ENABLE_PERSISTENCE;

   protected long fileDeploymentScanPeriod = DEFAULT_FILE_DEPLOYER_SCAN_PERIOD;

   protected boolean persistDeliveryCountBeforeDelivery = DEFAULT_PERSIST_DELIVERY_COUNT_BEFORE_DELIVERY;

   protected long queueActivationTimeout = DEFAULT_QUEUE_ACTIVATION_TIMEOUT;

   protected int scheduledThreadPoolMaxSize = DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE;

   protected long securityInvalidationInterval = DEFAULT_SECURITY_INVALIDATION_INTERVAL;

   protected boolean securityEnabled = DEFAULT_SECURITY_ENABLED;

   protected boolean jmxManagementEnabled = DEFAULT_JMX_MANAGEMENT_ENABLED;

   protected long connectionScanPeriod = DEFAULT_CONNECTION_SCAN_PERIOD;

   protected long connectionTTLOverride = DEFAULT_CONNECTION_TTL_OVERRIDE;

   protected long messageExpiryScanPeriod = DEFAULT_MESSAGE_EXPIRY_SCAN_PERIOD;

   protected int messageExpiryThreadPriority = DEFAULT_MESSAGE_EXPIRY_THREAD_PRIORITY;

   protected int idCacheSize = DEFAULT_ID_CACHE_SIZE;

   protected boolean persistIDCache = DEFAULT_PERSIST_ID_CACHE;

   protected List<String> interceptorClassNames = new ArrayList<String>();

   protected Map<String, TransportConfiguration> connectorConfigs = new HashMap<String, TransportConfiguration>();

   protected Set<TransportConfiguration> acceptorConfigs = new HashSet<TransportConfiguration>();

   protected String backupConnectorName;

   protected List<BridgeConfiguration> bridgeConfigurations = new ArrayList<BridgeConfiguration>();

   protected List<DivertConfiguration> divertConfigurations = new ArrayList<DivertConfiguration>();

   protected List<ClusterConnectionConfiguration> clusterConfigurations = new ArrayList<ClusterConnectionConfiguration>();

   protected List<QueueConfiguration> queueConfigurations = new ArrayList<QueueConfiguration>();

   protected List<BroadcastGroupConfiguration> broadcastGroupConfigurations = new ArrayList<BroadcastGroupConfiguration>();

   protected Map<String, DiscoveryGroupConfiguration> discoveryGroupConfigurations = new LinkedHashMap<String, DiscoveryGroupConfiguration>();

   // Paging related attributes ------------------------------------------------------------

   protected long pagingMaxGlobalSize = DEFAULT_PAGE_MAX_GLOBAL_SIZE;

   protected int pageWatermarkSize = DEFAULT_PAGE_WATERMARK_SIZE;

   protected String pagingDirectory = DEFAULT_PAGING_DIR;

   protected int pagingMaxThreads = DEFAULT_PAGE_MAX_THREADS;

   // File related attributes -----------------------------------------------------------

   protected String largeMessagesDirectory = DEFAULT_LARGE_MESSAGES_DIR;

   protected String bindingsDirectory = DEFAULT_BINDINGS_DIRECTORY;

   protected boolean createBindingsDir = DEFAULT_CREATE_BINDINGS_DIR;

   protected String journalDirectory = DEFAULT_JOURNAL_DIR;

   protected boolean createJournalDir = DEFAULT_CREATE_JOURNAL_DIR;

   public JournalType journalType = DEFAULT_JOURNAL_TYPE;

   protected boolean journalSyncTransactional = DEFAULT_JOURNAL_SYNC_TRANSACTIONAL;

   protected boolean journalSyncNonTransactional = DEFAULT_JOURNAL_SYNC_NON_TRANSACTIONAL;

   protected int journalFileSize = DEFAULT_JOURNAL_FILE_SIZE;

   protected int journalMinFiles = DEFAULT_JOURNAL_MIN_FILES;

   protected int journalMaxAIO = DEFAULT_JOURNAL_MAX_AIO;

   protected int journalBufferReuseSize = DEFAULT_JOURNAL_REUSE_BUFFER_SIZE;

   protected boolean wildcardRoutingEnabled = DEFAULT_WILDCARD_ROUTING_ENABLED;

   protected boolean messageCounterEnabled = DEFAULT_MESSAGE_COUNTER_ENABLED;

   protected long messageCounterSamplePeriod = DEFAULT_MESSAGE_COUNTER_SAMPLE_PERIOD;

   protected int messageCounterMaxDayHistory = DEFAULT_MESSAGE_COUNTER_MAX_DAY_HISTORY;
   
   protected long transactionTimeout = DEFAULT_TRANSACTION_TIMEOUT;

   protected long transactionTimeoutScanPeriod = DEFAULT_TRANSACTION_TIMEOUT_SCAN_PERIOD;

   protected SimpleString managementAddress = DEFAULT_MANAGEMENT_ADDRESS;

   protected SimpleString managementNotificationAddress = DEFAULT_MANAGEMENT_NOTIFICATION_ADDRESS;

   protected String managementClusterPassword = DEFAULT_MANAGEMENT_CLUSTER_PASSWORD;

   protected long managementRequestTimeout = DEFAULT_MANAGEMENT_REQUEST_TIMEOUT;

   // MessagingComponent implementation ----------------------------------------------

   public void start() throws Exception
   {
   }

   public void stop() throws Exception
   {
   }

   public boolean isStarted()
   {
      return true;
   }

   // Public -------------------------------------------------------------------------

   public boolean isClustered()
   {
      return clustered;
   }

   public void setClustered(final boolean clustered)
   {
      this.clustered = clustered;
   }

   public boolean isBackup()
   {
      return backup;
   }

   public boolean isEnableFileDeployment()
   {
      return enableFileDeployment;
   }

   public void setEnableFileDeployment(final boolean enable)
   {
      enableFileDeployment = enable;
   }
   
   public boolean isEnablePersistence()
   {
      return this.enablePersistence;
   }
   
   public void setEnablePersistence(boolean enable)
   {
      this.enablePersistence = enable;
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

   /**
    * @param strictJMS the strictJMS to set
    */
   public void setPersistDeliveryCountBeforeDelivery(final boolean persistDeliveryCountBeforeDelivery)
   {
      this.persistDeliveryCountBeforeDelivery = persistDeliveryCountBeforeDelivery;
   }

   public void setBackup(final boolean backup)
   {
      this.backup = backup;
   }

   public long getQueueActivationTimeout()
   {
      return queueActivationTimeout;
   }

   public void setQueueActivationTimeout(final long timeout)
   {
      queueActivationTimeout = timeout;
   }

   public int getScheduledThreadPoolMaxSize()
   {
      return scheduledThreadPoolMaxSize;
   }

   public void setScheduledThreadPoolMaxSize(final int maxSize)
   {
      scheduledThreadPoolMaxSize = maxSize;
   }

   public long getSecurityInvalidationInterval()
   {
      return securityInvalidationInterval;
   }

   public void setSecurityInvalidationInterval(final long interval)
   {
      securityInvalidationInterval = interval;
   }

   public long getConnectionScanPeriod()
   {
      return connectionScanPeriod;
   }

   public void setConnectionScanPeriod(final long scanPeriod)
   {
      connectionScanPeriod = scanPeriod;
   }

   public long getConnectionTTLOverride()
   {
      return connectionTTLOverride;
   }

   public void setConnectionTTLOverride(final long ttl)
   {
      connectionTTLOverride = ttl;
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

   public String getBackupConnectorName()
   {
      return backupConnectorName;
   }

   public void setBackupConnectorName(final String backupConnectorName)
   {
      this.backupConnectorName = backupConnectorName;
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

   public List<QueueConfiguration> getQueueConfigurations()
   {
      return queueConfigurations;
   }

   public void setQueueConfigurations(final List<QueueConfiguration> configs)
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

   public int getPagingMaxThreads()
   {
      return pagingMaxThreads;
   }

   public void setPagingMaxThread(final int pagingMaxThreads)
   {
      this.pagingMaxThreads = pagingMaxThreads;
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

   public int getJournalMaxAIO()
   {
      return journalMaxAIO;
   }

   public void setJournalMaxAIO(final int maxAIO)
   {
      journalMaxAIO = maxAIO;
   }

   public int getJournalMinFiles()
   {
      return journalMinFiles;
   }

   public void setJournalMinFiles(final int files)
   {
      journalMinFiles = files;
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

   public void setJournalBufferReuseSize(final int reuseSize)
   {
      journalBufferReuseSize = reuseSize;
   }

   public int getJournalBufferReuseSize()
   {
      return journalBufferReuseSize;
   }

   public long getPagingMaxGlobalSizeBytes()
   {
      return pagingMaxGlobalSize;
   }

   public void setPagingMaxGlobalSizeBytes(final long maxGlobalSize)
   {
      pagingMaxGlobalSize = maxGlobalSize;
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.config.Configuration#getPagingDefaultSize()
    */
   public int getPagingGlobalWatermarkSize()
   {
      return pageWatermarkSize;
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.config.Configuration#setPagingDefaultSize(long)
    */
   public void setPagingGlobalWatermarkSize(final int pageSize)
   {
      pageWatermarkSize = pageSize;
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

   public long getMessageCounterSamplePeriod()
   {
      return messageCounterSamplePeriod;
   }

   public int getMessageCounterMaxDayHistory()
   {
      return messageCounterMaxDayHistory;
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

   public String getManagementClusterPassword()
   {
      return managementClusterPassword;
   }

   public void setManagementClusterPassword(final String clusterPassword)
   {
      managementClusterPassword = clusterPassword;
   }

   public long getManagementRequestTimeout()
   {
      return managementRequestTimeout;
   }

   public void setManagementRequestTimeout(final long managementRequestTimeout)
   {
      this.managementRequestTimeout = managementRequestTimeout;
   }

   @Override
   public boolean equals(final Object other)
   {
      if (this == other)
      {
         return true;
      }

      if (other instanceof Configuration == false)
      {
         return false;
      }

      Configuration cother = (Configuration)other;

      return cother.isClustered() == isClustered() && cother.isCreateBindingsDir() == isCreateBindingsDir() &&
             cother.isCreateJournalDir() == isCreateJournalDir() &&
             cother.isJournalSyncNonTransactional() == isJournalSyncNonTransactional() &&
             cother.isJournalSyncTransactional() == isJournalSyncTransactional() &&
             cother.isSecurityEnabled() == isSecurityEnabled() &&
             cother.isWildcardRoutingEnabled() == isWildcardRoutingEnabled() &&
             cother.getLargeMessagesDirectory().equals(getLargeMessagesDirectory()) &&
             cother.getBindingsDirectory().equals(getBindingsDirectory()) &&
             cother.getJournalDirectory().equals(getJournalDirectory()) &&
             cother.getJournalFileSize() == getJournalFileSize() &&
             cother.getJournalMaxAIO() == getJournalMaxAIO() &&
             cother.getJournalMinFiles() == getJournalMinFiles() &&
             cother.getJournalType() == getJournalType() &&
             cother.getScheduledThreadPoolMaxSize() == getScheduledThreadPoolMaxSize() &&
             cother.getSecurityInvalidationInterval() == getSecurityInvalidationInterval() &&
             cother.getManagementAddress().equals(getManagementAddress()) &&
             cother.getPagingGlobalWatermarkSize() == getPagingGlobalWatermarkSize();
   }
}
