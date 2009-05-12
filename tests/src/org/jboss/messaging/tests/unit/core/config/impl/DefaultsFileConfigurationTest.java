/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */ 

package org.jboss.messaging.tests.unit.core.config.impl;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.config.impl.FileConfiguration;

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

      assertEquals(ConfigurationImpl.DEFAULT_QUEUE_ACTIVATION_TIMEOUT, conf.getQueueActivationTimeout());
      
      assertEquals(ConfigurationImpl.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE, conf.getScheduledThreadPoolMaxSize());
      
      assertEquals(ConfigurationImpl.DEFAULT_THREAD_POOL_MAX_SIZE, conf.getThreadPoolMaxSize());

      assertEquals(ConfigurationImpl.DEFAULT_SECURITY_INVALIDATION_INTERVAL, conf.getSecurityInvalidationInterval());

      assertEquals(ConfigurationImpl.DEFAULT_SECURITY_ENABLED, conf.isSecurityEnabled());

      assertEquals(ConfigurationImpl.DEFAULT_JMX_MANAGEMENT_ENABLED, conf.isJMXManagementEnabled());

      assertEquals(0, conf.getInterceptorClassNames().size());

      assertEquals(ConfigurationImpl.DEFAULT_CONNECTION_SCAN_PERIOD, conf.getConnectionScanPeriod());
      
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

      assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_MIN_FILES, conf.getJournalMinFiles());

      assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_MAX_AIO, conf.getJournalMaxAIO());

      assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_REUSE_BUFFER_SIZE, conf.getJournalBufferReuseSize());

      assertEquals(ConfigurationImpl.DEFAULT_CREATE_BINDINGS_DIR, conf.isCreateBindingsDir());

      assertEquals(ConfigurationImpl.DEFAULT_CREATE_JOURNAL_DIR, conf.isCreateJournalDir());

      assertEquals(ConfigurationImpl.DEFAULT_PAGING_DIR, conf.getPagingDirectory());

      assertEquals(ConfigurationImpl.DEFAULT_PAGE_MAX_GLOBAL_SIZE, conf.getPagingMaxGlobalSizeBytes());

      assertEquals(ConfigurationImpl.DEFAULT_PAGE_WATERMARK_SIZE, conf.getPagingGlobalWatermarkSize());
      
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

