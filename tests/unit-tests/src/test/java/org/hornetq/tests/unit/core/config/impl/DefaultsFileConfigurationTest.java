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

import java.util.Collections;

import junit.framework.Assert;

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
   @Override
   public void testDefaults()
   {

      Assert.assertEquals(ConfigurationImpl.DEFAULT_CLUSTERED, conf.isClustered());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_BACKUP, conf.isBackup());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_SHARED_STORE, conf.isSharedStore());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,
                          conf.getScheduledThreadPoolMaxSize());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_THREAD_POOL_MAX_SIZE, conf.getThreadPoolMaxSize());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_SECURITY_INVALIDATION_INTERVAL,
                          conf.getSecurityInvalidationInterval());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_SECURITY_ENABLED, conf.isSecurityEnabled());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_JMX_MANAGEMENT_ENABLED, conf.isJMXManagementEnabled());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_JMX_DOMAIN, conf.getJMXDomain());

      Assert.assertEquals(0, conf.getInterceptorClassNames().size());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_CONNECTION_TTL_OVERRIDE, conf.getConnectionTTLOverride());

      Assert.assertEquals(0, conf.getAcceptorConfigurations().size());

      Assert.assertEquals(Collections.emptyMap(), conf.getConnectorConfigurations());

      Assert.assertEquals(Collections.emptyList(), conf.getBroadcastGroupConfigurations());

      Assert.assertEquals(Collections.emptyMap(), conf.getDiscoveryGroupConfigurations());

      Assert.assertEquals(Collections.emptyList(), conf.getBridgeConfigurations());

      Assert.assertEquals(Collections.emptyList(), conf.getDivertConfigurations());

      Assert.assertEquals(Collections.emptyList(), conf.getClusterConfigurations());

      Assert.assertEquals(Collections.emptyList(), conf.getQueueConfigurations());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_MANAGEMENT_ADDRESS, conf.getManagementAddress());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_MANAGEMENT_NOTIFICATION_ADDRESS,
                          conf.getManagementNotificationAddress());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_CLUSTER_USER, conf.getClusterUser());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_CLUSTER_PASSWORD, conf.getClusterPassword());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_ID_CACHE_SIZE, conf.getIDCacheSize());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_PERSIST_ID_CACHE, conf.isPersistIDCache());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_BINDINGS_DIRECTORY, conf.getBindingsDirectory());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_DIR, conf.getJournalDirectory());

      Assert.assertEquals(getDefaultJournalType(), conf.getJournalType());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_SYNC_TRANSACTIONAL, conf.isJournalSyncTransactional());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_SYNC_NON_TRANSACTIONAL,
                          conf.isJournalSyncNonTransactional());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_FILE_SIZE, conf.getJournalFileSize());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_COMPACT_MIN_FILES, conf.getJournalCompactMinFiles());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_COMPACT_PERCENTAGE, conf.getJournalCompactPercentage());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_MIN_FILES, conf.getJournalMinFiles());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_MAX_IO_AIO, conf.getJournalMaxIO_AIO());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_BUFFER_TIMEOUT_AIO, conf.getJournalBufferTimeout_AIO());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_BUFFER_SIZE_AIO, conf.getJournalBufferSize_AIO());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_MAX_IO_NIO, conf.getJournalMaxIO_NIO());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_BUFFER_TIMEOUT_NIO, conf.getJournalBufferTimeout_NIO());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_BUFFER_SIZE_NIO, conf.getJournalBufferSize_NIO());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_CREATE_BINDINGS_DIR, conf.isCreateBindingsDir());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_CREATE_JOURNAL_DIR, conf.isCreateJournalDir());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_PAGING_DIR, conf.getPagingDirectory());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_LARGE_MESSAGES_DIR, conf.getLargeMessagesDirectory());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_WILDCARD_ROUTING_ENABLED, conf.isWildcardRoutingEnabled());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_TRANSACTION_TIMEOUT, conf.getTransactionTimeout());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_MESSAGE_COUNTER_ENABLED, conf.isMessageCounterEnabled());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_TRANSACTION_TIMEOUT_SCAN_PERIOD,
                          conf.getTransactionTimeoutScanPeriod());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_MESSAGE_EXPIRY_SCAN_PERIOD, conf.getMessageExpiryScanPeriod());

      Assert.assertEquals(ConfigurationImpl.DEFAULT_MESSAGE_EXPIRY_THREAD_PRIORITY,
                          conf.getMessageExpiryThreadPriority());
   }

   // Protected ---------------------------------------------------------------------------------------------

   @Override
   protected Configuration createConfiguration() throws Exception
   {
      FileConfiguration fc = new FileConfiguration();

      fc.setConfigurationUrl("ConfigurationTest-defaults.xml");

      fc.start();

      return fc;
   }

}
