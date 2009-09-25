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

package org.hornetq.tests.unit.core.config.impl;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.config.impl.FileConfiguration;

/**
 * 
 * A DefaultsFileConfigurationTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class DefaultsFileConfigurationTest extends ConfigurationImplTest
{
   public void testDefaults()
   {
      
      assertEquals(ConfigurationImpl.DEFAULT_CLUSTERED, conf.isClustered());

      assertEquals(ConfigurationImpl.DEFAULT_BACKUP, conf.isBackup());

      assertEquals(ConfigurationImpl.DEFAULT_SHARED_STORE, conf.isSharedStore());
      
      assertEquals(ConfigurationImpl.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE, conf.getScheduledThreadPoolMaxSize());
      
      assertEquals(ConfigurationImpl.DEFAULT_THREAD_POOL_MAX_SIZE, conf.getThreadPoolMaxSize());

      assertEquals(ConfigurationImpl.DEFAULT_SECURITY_INVALIDATION_INTERVAL, conf.getSecurityInvalidationInterval());

      assertEquals(ConfigurationImpl.DEFAULT_SECURITY_ENABLED, conf.isSecurityEnabled());

      assertEquals(ConfigurationImpl.DEFAULT_JMX_MANAGEMENT_ENABLED, conf.isJMXManagementEnabled());

      assertEquals(0, conf.getInterceptorClassNames().size());
  
      assertEquals(ConfigurationImpl.DEFAULT_CONNECTION_TTL_OVERRIDE, conf.getConnectionTTLOverride());

      assertEquals(0, conf.getAcceptorConfigurations().size());

      assertEquals(emptyMap(), conf.getConnectorConfigurations());

      assertEquals(null, conf.getBackupConnectorName());

      assertEquals(emptyList(), conf.getBroadcastGroupConfigurations());
      
      assertEquals(emptyMap(), conf.getDiscoveryGroupConfigurations());
      
      assertEquals(emptyList(), conf.getBridgeConfigurations());

      assertEquals(emptyList(), conf.getDivertConfigurations());

      assertEquals(emptyList(), conf.getClusterConfigurations());

      assertEquals(emptyList(), conf.getQueueConfigurations());

      assertEquals(ConfigurationImpl.DEFAULT_MANAGEMENT_ADDRESS, conf.getManagementAddress());
      
      assertEquals(ConfigurationImpl.DEFAULT_MANAGEMENT_NOTIFICATION_ADDRESS, conf.getManagementNotificationAddress());
      
      assertEquals(ConfigurationImpl.DEFAULT_MANAGEMENT_CLUSTER_USER, conf.getManagementClusterUser());

      assertEquals(ConfigurationImpl.DEFAULT_MANAGEMENT_CLUSTER_PASSWORD, conf.getManagementClusterPassword());

      assertEquals(ConfigurationImpl.DEFAULT_MANAGEMENT_REQUEST_TIMEOUT, conf.getManagementRequestTimeout());

      assertEquals(ConfigurationImpl.DEFAULT_ID_CACHE_SIZE, conf.getIDCacheSize());
      
      assertEquals(ConfigurationImpl.DEFAULT_PERSIST_ID_CACHE, conf.isPersistIDCache());
      
      assertEquals(ConfigurationImpl.DEFAULT_BINDINGS_DIRECTORY, conf.getBindingsDirectory());

      assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_DIR, conf.getJournalDirectory());

      assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_TYPE, conf.getJournalType());

      assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_SYNC_TRANSACTIONAL, conf.isJournalSyncTransactional());

      assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_SYNC_NON_TRANSACTIONAL, conf.isJournalSyncNonTransactional());

      assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_FILE_SIZE, conf.getJournalFileSize());

      assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_COMPACT_MIN_FILES, conf.getJournalCompactMinFiles());
      
      assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_COMPACT_PERCENTAGE, conf.getJournalCompactPercentage());
      
      assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_MIN_FILES, conf.getJournalMinFiles());

      assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_MAX_AIO, conf.getJournalMaxAIO());

      assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_AIO_BUFFER_TIMEOUT, conf.getAIOBufferTimeout());

      assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_AIO_BUFFER_SIZE, conf.getAIOBufferSize());
      
      assertEquals(ConfigurationImpl.DEFAULT_CREATE_BINDINGS_DIR, conf.isCreateBindingsDir());

      assertEquals(ConfigurationImpl.DEFAULT_CREATE_JOURNAL_DIR, conf.isCreateJournalDir());

      assertEquals(ConfigurationImpl.DEFAULT_PAGING_DIR, conf.getPagingDirectory());
      
      assertEquals(ConfigurationImpl.DEFAULT_LARGE_MESSAGES_DIR, conf.getLargeMessagesDirectory());
      
      assertEquals(ConfigurationImpl.DEFAULT_WILDCARD_ROUTING_ENABLED, conf.isWildcardRoutingEnabled());

      assertEquals(ConfigurationImpl.DEFAULT_TRANSACTION_TIMEOUT, conf.getTransactionTimeout());

      assertEquals(ConfigurationImpl.DEFAULT_MESSAGE_COUNTER_ENABLED, conf.isMessageCounterEnabled());

      assertEquals(ConfigurationImpl.DEFAULT_TRANSACTION_TIMEOUT_SCAN_PERIOD, conf.getTransactionTimeoutScanPeriod());

      assertEquals(ConfigurationImpl.DEFAULT_MESSAGE_EXPIRY_SCAN_PERIOD, conf.getMessageExpiryScanPeriod());

      assertEquals(ConfigurationImpl.DEFAULT_MESSAGE_EXPIRY_THREAD_PRIORITY, conf.getMessageExpiryThreadPriority());
   }
   
   // Protected ---------------------------------------------------------------------------------------------
   
   protected Configuration createConfiguration() throws Exception
   {
      FileConfiguration fc = new FileConfiguration();
      
      fc.setConfigurationUrl("ConfigurationTest-defaults.xml");
      
      fc.start();
      
      return fc;
   }

}

