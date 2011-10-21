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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import junit.framework.Assert;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.journal.impl.JournalConstants;
import org.hornetq.core.server.JournalType;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.UnitTestCase;

/**
 * 
 * A ConfigurationImplTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ConfigurationImplTest extends UnitTestCase
{
   protected Configuration conf;

   public void testDefaults()
   {
      Assert.assertEquals(ConfigurationImpl.DEFAULT_CLUSTERED, conf.isClustered());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_BACKUP, conf.isBackup());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_SHARED_STORE, conf.isSharedStore());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,
                          conf.getScheduledThreadPoolMaxSize());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_SECURITY_INVALIDATION_INTERVAL,
                          conf.getSecurityInvalidationInterval());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_SECURITY_ENABLED, conf.isSecurityEnabled());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_LOG_DELEGATE_FACTORY_CLASS_NAME,
                          conf.getLogDelegateFactoryClassName());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_BINDINGS_DIRECTORY, conf.getBindingsDirectory());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_CREATE_BINDINGS_DIR, conf.isCreateBindingsDir());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_DIR, conf.getJournalDirectory());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_CREATE_JOURNAL_DIR, conf.isCreateJournalDir());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_TYPE, conf.getJournalType());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_SYNC_TRANSACTIONAL, conf.isJournalSyncTransactional());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_SYNC_NON_TRANSACTIONAL,
                          conf.isJournalSyncNonTransactional());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_FILE_SIZE, conf.getJournalFileSize());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_MIN_FILES, conf.getJournalMinFiles());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_MAX_IO_AIO, conf.getJournalMaxIO_AIO());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_MAX_IO_NIO, conf.getJournalMaxIO_NIO());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_WILDCARD_ROUTING_ENABLED, conf.isWildcardRoutingEnabled());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_TRANSACTION_TIMEOUT, conf.getTransactionTimeout());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_MESSAGE_EXPIRY_SCAN_PERIOD, conf.getMessageExpiryScanPeriod()); // OK
      Assert.assertEquals(ConfigurationImpl.DEFAULT_MESSAGE_EXPIRY_THREAD_PRIORITY,
                          conf.getMessageExpiryThreadPriority()); // OK
      Assert.assertEquals(ConfigurationImpl.DEFAULT_TRANSACTION_TIMEOUT_SCAN_PERIOD,
                          conf.getTransactionTimeoutScanPeriod()); // OK
      Assert.assertEquals(ConfigurationImpl.DEFAULT_MANAGEMENT_ADDRESS, conf.getManagementAddress()); // OK
      Assert.assertEquals(ConfigurationImpl.DEFAULT_MANAGEMENT_NOTIFICATION_ADDRESS,
                          conf.getManagementNotificationAddress()); // OK
      Assert.assertEquals(ConfigurationImpl.DEFAULT_CLUSTER_USER, conf.getClusterUser()); // OK
      Assert.assertEquals(ConfigurationImpl.DEFAULT_CLUSTER_PASSWORD, conf.getClusterPassword()); // OK
      Assert.assertEquals(ConfigurationImpl.DEFAULT_PERSISTENCE_ENABLED, conf.isPersistenceEnabled());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_FILE_DEPLOYMENT_ENABLED, conf.isFileDeploymentEnabled());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_PERSIST_DELIVERY_COUNT_BEFORE_DELIVERY,
                          conf.isPersistDeliveryCountBeforeDelivery());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_FILE_DEPLOYER_SCAN_PERIOD, conf.getFileDeployerScanPeriod());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_THREAD_POOL_MAX_SIZE, conf.getThreadPoolMaxSize());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_JMX_MANAGEMENT_ENABLED, conf.isJMXManagementEnabled());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_CONNECTION_TTL_OVERRIDE, conf.getConnectionTTLOverride());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_ASYNC_CONNECTION_EXECUTION_ENABLED,
                          conf.isAsyncConnectionExecutionEnabled());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_PAGING_DIR, conf.getPagingDirectory());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_LARGE_MESSAGES_DIR, conf.getLargeMessagesDirectory());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_COMPACT_PERCENTAGE, conf.getJournalCompactPercentage());
      Assert.assertEquals(JournalConstants.DEFAULT_JOURNAL_BUFFER_TIMEOUT_AIO, conf.getJournalBufferTimeout_AIO());
      Assert.assertEquals(JournalConstants.DEFAULT_JOURNAL_BUFFER_TIMEOUT_NIO, conf.getJournalBufferTimeout_NIO());
      Assert.assertEquals(JournalConstants.DEFAULT_JOURNAL_BUFFER_SIZE_AIO, conf.getJournalBufferSize_AIO());
      Assert.assertEquals(JournalConstants.DEFAULT_JOURNAL_BUFFER_SIZE_NIO, conf.getJournalBufferSize_NIO());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_LOG_WRITE_RATE, conf.isLogJournalWriteRate());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_PERF_BLAST_PAGES, conf.getJournalPerfBlastPages());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_MESSAGE_COUNTER_ENABLED, conf.isMessageCounterEnabled());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_MESSAGE_COUNTER_MAX_DAY_HISTORY,
                          conf.getMessageCounterMaxDayHistory());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_MESSAGE_COUNTER_SAMPLE_PERIOD, conf.getMessageCounterSamplePeriod());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_ID_CACHE_SIZE, conf.getIDCacheSize());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_PERSIST_ID_CACHE, conf.isPersistIDCache());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_SERVER_DUMP_INTERVAL, conf.getServerDumpInterval());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_MEMORY_WARNING_THRESHOLD, conf.getMemoryWarningThreshold());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_MEMORY_MEASURE_INTERVAL, conf.getMemoryMeasureInterval());
   }

   public void testSetGetAttributes()
   {
      for (int j = 0; j < 100; j++)
      {
         boolean b = RandomUtil.randomBoolean();
         conf.setClustered(b);
         Assert.assertEquals(b, conf.isClustered());

         b = RandomUtil.randomBoolean();
         conf.setBackup(b);
         Assert.assertEquals(b, conf.isBackup());

         b = RandomUtil.randomBoolean();
         conf.setSharedStore(b);
         Assert.assertEquals(b, conf.isSharedStore());

         int i = RandomUtil.randomInt();
         conf.setScheduledThreadPoolMaxSize(i);
         Assert.assertEquals(i, conf.getScheduledThreadPoolMaxSize());

         long l = RandomUtil.randomLong();
         conf.setSecurityInvalidationInterval(l);
         Assert.assertEquals(l, conf.getSecurityInvalidationInterval());

         b = RandomUtil.randomBoolean();
         conf.setSecurityEnabled(b);
         Assert.assertEquals(b, conf.isSecurityEnabled());

         String s = RandomUtil.randomString();
         conf.setBindingsDirectory(s);
         Assert.assertEquals(s, conf.getBindingsDirectory());

         b = RandomUtil.randomBoolean();
         conf.setCreateBindingsDir(b);
         Assert.assertEquals(b, conf.isCreateBindingsDir());

         s = RandomUtil.randomString();
         conf.setJournalDirectory(s);
         Assert.assertEquals(s, conf.getJournalDirectory());

         b = RandomUtil.randomBoolean();
         conf.setCreateJournalDir(b);
         Assert.assertEquals(b, conf.isCreateJournalDir());

         i = RandomUtil.randomInt() % 2;
         JournalType journal = i == 0 ? JournalType.ASYNCIO : JournalType.NIO;
         conf.setJournalType(journal);
         Assert.assertEquals(journal, conf.getJournalType());

         b = RandomUtil.randomBoolean();
         conf.setJournalSyncTransactional(b);
         Assert.assertEquals(b, conf.isJournalSyncTransactional());

         b = RandomUtil.randomBoolean();
         conf.setJournalSyncNonTransactional(b);
         Assert.assertEquals(b, conf.isJournalSyncNonTransactional());

         i = RandomUtil.randomInt();
         conf.setJournalFileSize(i);
         Assert.assertEquals(i, conf.getJournalFileSize());

         i = RandomUtil.randomInt();
         conf.setJournalMinFiles(i);
         Assert.assertEquals(i, conf.getJournalMinFiles());

         i = RandomUtil.randomInt();
         conf.setJournalMaxIO_AIO(i);
         Assert.assertEquals(i, conf.getJournalMaxIO_AIO());

         i = RandomUtil.randomInt();
         conf.setJournalMaxIO_NIO(i);
         Assert.assertEquals(i, conf.getJournalMaxIO_NIO());

         s = RandomUtil.randomString();
         conf.setManagementAddress(new SimpleString(s));
         Assert.assertEquals(s, conf.getManagementAddress().toString());

         i = RandomUtil.randomInt();
         conf.setMessageExpiryThreadPriority(i);
         Assert.assertEquals(i, conf.getMessageExpiryThreadPriority());

         l = RandomUtil.randomLong();
         conf.setMessageExpiryScanPeriod(l);
         Assert.assertEquals(l, conf.getMessageExpiryScanPeriod());

         b = RandomUtil.randomBoolean();
         conf.setPersistDeliveryCountBeforeDelivery(b);
         Assert.assertEquals(b, conf.isPersistDeliveryCountBeforeDelivery());

         b = RandomUtil.randomBoolean();
         conf.setEnabledAsyncConnectionExecution(b);
         Assert.assertEquals(b, conf.isAsyncConnectionExecutionEnabled());

         b = RandomUtil.randomBoolean();
         conf.setFileDeploymentEnabled(b);
         Assert.assertEquals(b, conf.isFileDeploymentEnabled());

         b = RandomUtil.randomBoolean();
         conf.setPersistenceEnabled(b);
         Assert.assertEquals(b, conf.isPersistenceEnabled());

         b = RandomUtil.randomBoolean();
         conf.setJMXManagementEnabled(b);
         Assert.assertEquals(b, conf.isJMXManagementEnabled());

         l = RandomUtil.randomLong();
         conf.setFileDeployerScanPeriod(l);
         Assert.assertEquals(l, conf.getFileDeployerScanPeriod());

         l = RandomUtil.randomLong();
         conf.setConnectionTTLOverride(l);
         Assert.assertEquals(l, conf.getConnectionTTLOverride());

         i = RandomUtil.randomInt();
         conf.setThreadPoolMaxSize(i);
         Assert.assertEquals(i, conf.getThreadPoolMaxSize());

         SimpleString ss = RandomUtil.randomSimpleString();
         conf.setManagementNotificationAddress(ss);
         Assert.assertEquals(ss, conf.getManagementNotificationAddress());

         s = RandomUtil.randomString();
         conf.setClusterUser(s);
         Assert.assertEquals(s, conf.getClusterUser());

         i = RandomUtil.randomInt();
         conf.setIDCacheSize(i);
         Assert.assertEquals(i, conf.getIDCacheSize());

         b = RandomUtil.randomBoolean();
         conf.setPersistIDCache(b);
         Assert.assertEquals(b, conf.isPersistIDCache());

         i = RandomUtil.randomInt();
         conf.setJournalCompactMinFiles(i);
         Assert.assertEquals(i, conf.getJournalCompactMinFiles());

         i = RandomUtil.randomInt();
         conf.setJournalCompactPercentage(i);
         Assert.assertEquals(i, conf.getJournalCompactPercentage());

         i = RandomUtil.randomInt();
         conf.setJournalBufferSize_AIO(i);
         Assert.assertEquals(i, conf.getJournalBufferSize_AIO());

         i = RandomUtil.randomInt();
         conf.setJournalBufferTimeout_AIO(i);
         Assert.assertEquals(i, conf.getJournalBufferTimeout_AIO());

         i = RandomUtil.randomInt();
         conf.setJournalBufferSize_NIO(i);
         Assert.assertEquals(i, conf.getJournalBufferSize_NIO());

         i = RandomUtil.randomInt();
         conf.setJournalBufferTimeout_NIO(i);
         Assert.assertEquals(i, conf.getJournalBufferTimeout_NIO());

         b = RandomUtil.randomBoolean();
         conf.setLogJournalWriteRate(b);
         Assert.assertEquals(b, conf.isLogJournalWriteRate());

         i = RandomUtil.randomInt();
         conf.setJournalPerfBlastPages(i);
         Assert.assertEquals(i, conf.getJournalPerfBlastPages());

         l = RandomUtil.randomLong();
         conf.setServerDumpInterval(l);
         Assert.assertEquals(l, conf.getServerDumpInterval());

         s = RandomUtil.randomString();
         conf.setPagingDirectory(s);
         Assert.assertEquals(s, conf.getPagingDirectory());

         s = RandomUtil.randomString();
         conf.setLargeMessagesDirectory(s);
         Assert.assertEquals(s, conf.getLargeMessagesDirectory());

         b = RandomUtil.randomBoolean();
         conf.setWildcardRoutingEnabled(b);
         Assert.assertEquals(b, conf.isWildcardRoutingEnabled());

         l = RandomUtil.randomLong();
         conf.setTransactionTimeout(l);
         Assert.assertEquals(l, conf.getTransactionTimeout());

         b = RandomUtil.randomBoolean();
         conf.setMessageCounterEnabled(b);
         Assert.assertEquals(b, conf.isMessageCounterEnabled());

         l = RandomUtil.randomPositiveLong();
         conf.setMessageCounterSamplePeriod(l);
         Assert.assertEquals(l, conf.getMessageCounterSamplePeriod());

         i = RandomUtil.randomInt();
         conf.setMessageCounterMaxDayHistory(i);
         Assert.assertEquals(i, conf.getMessageCounterMaxDayHistory());

         l = RandomUtil.randomLong();
         conf.setTransactionTimeoutScanPeriod(l);
         Assert.assertEquals(l, conf.getTransactionTimeoutScanPeriod());

         s = RandomUtil.randomString();
         conf.setClusterPassword(s);
         Assert.assertEquals(s, conf.getClusterPassword());
      }
   }

   public void testGetSetInterceptors()
   {
      final String name1 = "uqwyuqywuy";
      final String name2 = "yugyugyguyg";

      conf.getInterceptorClassNames().add(name1);
      conf.getInterceptorClassNames().add(name2);

      Assert.assertTrue(conf.getInterceptorClassNames().contains(name1));
      Assert.assertTrue(conf.getInterceptorClassNames().contains(name2));
      Assert.assertFalse(conf.getInterceptorClassNames().contains("iijij"));
   }

   public void testSerialize() throws Exception
   {
      boolean b = RandomUtil.randomBoolean();
      conf.setClustered(b);
      Assert.assertEquals(b, conf.isClustered());

      b = RandomUtil.randomBoolean();
      conf.setBackup(b);
      Assert.assertEquals(b, conf.isBackup());

      b = RandomUtil.randomBoolean();
      conf.setSharedStore(b);

      int i = RandomUtil.randomInt();
      conf.setScheduledThreadPoolMaxSize(i);
      Assert.assertEquals(i, conf.getScheduledThreadPoolMaxSize());

      long l = RandomUtil.randomLong();
      conf.setSecurityInvalidationInterval(l);
      Assert.assertEquals(l, conf.getSecurityInvalidationInterval());

      b = RandomUtil.randomBoolean();
      conf.setSecurityEnabled(b);
      Assert.assertEquals(b, conf.isSecurityEnabled());

      String s = RandomUtil.randomString();
      conf.setBindingsDirectory(s);
      Assert.assertEquals(s, conf.getBindingsDirectory());

      b = RandomUtil.randomBoolean();
      conf.setCreateBindingsDir(b);
      Assert.assertEquals(b, conf.isCreateBindingsDir());

      s = RandomUtil.randomString();
      conf.setJournalDirectory(s);
      Assert.assertEquals(s, conf.getJournalDirectory());

      b = RandomUtil.randomBoolean();
      conf.setCreateJournalDir(b);
      Assert.assertEquals(b, conf.isCreateJournalDir());

      i = RandomUtil.randomInt() % 2;
      JournalType journal = i == 0 ? JournalType.ASYNCIO : JournalType.NIO;
      conf.setJournalType(journal);
      Assert.assertEquals(journal, conf.getJournalType());

      b = RandomUtil.randomBoolean();
      conf.setJournalSyncTransactional(b);
      Assert.assertEquals(b, conf.isJournalSyncTransactional());

      b = RandomUtil.randomBoolean();
      conf.setJournalSyncNonTransactional(b);
      Assert.assertEquals(b, conf.isJournalSyncNonTransactional());

      i = RandomUtil.randomInt();
      conf.setJournalFileSize(i);
      Assert.assertEquals(i, conf.getJournalFileSize());

      i = RandomUtil.randomInt();
      conf.setJournalMinFiles(i);
      Assert.assertEquals(i, conf.getJournalMinFiles());

      i = RandomUtil.randomInt();
      conf.setJournalMaxIO_AIO(i);
      Assert.assertEquals(i, conf.getJournalMaxIO_AIO());

      i = RandomUtil.randomInt();
      conf.setJournalMaxIO_NIO(i);
      Assert.assertEquals(i, conf.getJournalMaxIO_NIO());

      s = RandomUtil.randomString();
      conf.setManagementAddress(new SimpleString(s));
      Assert.assertEquals(s, conf.getManagementAddress().toString());

      i = RandomUtil.randomInt();
      conf.setMessageExpiryThreadPriority(i);
      Assert.assertEquals(i, conf.getMessageExpiryThreadPriority());

      l = RandomUtil.randomLong();
      conf.setMessageExpiryScanPeriod(l);
      Assert.assertEquals(l, conf.getMessageExpiryScanPeriod());

      b = RandomUtil.randomBoolean();
      conf.setPersistDeliveryCountBeforeDelivery(b);
      Assert.assertEquals(b, conf.isPersistDeliveryCountBeforeDelivery());

      b = RandomUtil.randomBoolean();
      conf.setEnabledAsyncConnectionExecution(b);
      Assert.assertEquals(b, conf.isAsyncConnectionExecutionEnabled());

      b = RandomUtil.randomBoolean();
      conf.setFileDeploymentEnabled(b);
      Assert.assertEquals(b, conf.isFileDeploymentEnabled());

      b = RandomUtil.randomBoolean();
      conf.setPersistenceEnabled(b);
      Assert.assertEquals(b, conf.isPersistenceEnabled());

      b = RandomUtil.randomBoolean();
      conf.setJMXManagementEnabled(b);
      Assert.assertEquals(b, conf.isJMXManagementEnabled());

      l = RandomUtil.randomLong();
      conf.setFileDeployerScanPeriod(l);
      Assert.assertEquals(l, conf.getFileDeployerScanPeriod());

      l = RandomUtil.randomLong();
      conf.setConnectionTTLOverride(l);
      Assert.assertEquals(l, conf.getConnectionTTLOverride());

      i = RandomUtil.randomInt();
      conf.setThreadPoolMaxSize(i);
      Assert.assertEquals(i, conf.getThreadPoolMaxSize());


      SimpleString ss = RandomUtil.randomSimpleString();
      conf.setManagementNotificationAddress(ss);
      Assert.assertEquals(ss, conf.getManagementNotificationAddress());

      s = RandomUtil.randomString();
      conf.setClusterUser(s);
      Assert.assertEquals(s, conf.getClusterUser());

      i = RandomUtil.randomInt();
      conf.setIDCacheSize(i);
      Assert.assertEquals(i, conf.getIDCacheSize());

      b = RandomUtil.randomBoolean();
      conf.setPersistIDCache(b);
      Assert.assertEquals(b, conf.isPersistIDCache());

      i = RandomUtil.randomInt();
      conf.setJournalCompactMinFiles(i);
      Assert.assertEquals(i, conf.getJournalCompactMinFiles());

      i = RandomUtil.randomInt();
      conf.setJournalCompactPercentage(i);
      Assert.assertEquals(i, conf.getJournalCompactPercentage());

      i = RandomUtil.randomInt();
      conf.setJournalBufferSize_AIO(i);
      Assert.assertEquals(i, conf.getJournalBufferSize_AIO());

      i = RandomUtil.randomInt();
      conf.setJournalBufferTimeout_AIO(i);
      Assert.assertEquals(i, conf.getJournalBufferTimeout_AIO());

      i = RandomUtil.randomInt();
      conf.setJournalBufferSize_NIO(i);
      Assert.assertEquals(i, conf.getJournalBufferSize_NIO());

      i = RandomUtil.randomInt();
      conf.setJournalBufferTimeout_NIO(i);
      Assert.assertEquals(i, conf.getJournalBufferTimeout_NIO());

      b = RandomUtil.randomBoolean();
      conf.setLogJournalWriteRate(b);
      Assert.assertEquals(b, conf.isLogJournalWriteRate());

      i = RandomUtil.randomInt();
      conf.setJournalPerfBlastPages(i);
      Assert.assertEquals(i, conf.getJournalPerfBlastPages());

      l = RandomUtil.randomLong();
      conf.setServerDumpInterval(l);
      Assert.assertEquals(l, conf.getServerDumpInterval());

      s = RandomUtil.randomString();
      conf.setPagingDirectory(s);
      Assert.assertEquals(s, conf.getPagingDirectory());

      s = RandomUtil.randomString();
      conf.setLargeMessagesDirectory(s);
      Assert.assertEquals(s, conf.getLargeMessagesDirectory());

      b = RandomUtil.randomBoolean();
      conf.setWildcardRoutingEnabled(b);
      Assert.assertEquals(b, conf.isWildcardRoutingEnabled());

      l = RandomUtil.randomLong();
      conf.setTransactionTimeout(l);
      Assert.assertEquals(l, conf.getTransactionTimeout());

      b = RandomUtil.randomBoolean();
      conf.setMessageCounterEnabled(b);
      Assert.assertEquals(b, conf.isMessageCounterEnabled());

      l = RandomUtil.randomPositiveLong();
      conf.setMessageCounterSamplePeriod(l);
      Assert.assertEquals(l, conf.getMessageCounterSamplePeriod());

      i = RandomUtil.randomInt();
      conf.setMessageCounterMaxDayHistory(i);
      Assert.assertEquals(i, conf.getMessageCounterMaxDayHistory());

      l = RandomUtil.randomLong();
      conf.setTransactionTimeoutScanPeriod(l);
      Assert.assertEquals(l, conf.getTransactionTimeoutScanPeriod());

      s = RandomUtil.randomString();
      conf.setClusterPassword(s);
      Assert.assertEquals(s, conf.getClusterPassword());

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(conf);
      oos.flush();

      ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
      ObjectInputStream ois = new ObjectInputStream(bais);
      Configuration conf2 = (Configuration)ois.readObject();

      Assert.assertTrue(conf.equals(conf2));
   }

   // Protected ----------------------------------------------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      conf = createConfiguration();
   }

   protected Configuration createConfiguration() throws Exception
   {
      return new ConfigurationImpl();
   }

   // Private --------------------------------------------------------------------------------------------

}
