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

import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.cluster.BridgeConfiguration;
import org.hornetq.core.config.cluster.BroadcastGroupConfiguration;
import org.hornetq.core.config.cluster.ClusterConnectionConfiguration;
import org.hornetq.core.config.cluster.DiscoveryGroupConfiguration;
import org.hornetq.core.config.cluster.DivertConfiguration;
import org.hornetq.core.config.impl.FileConfiguration;
import org.hornetq.core.server.JournalType;
import org.hornetq.utils.SimpleString;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public class FileConfigurationTest extends ConfigurationImplTest
{
   @Override
   public void testDefaults()
   {
      //Check they match the values from the test file
      assertEquals(false, conf.isPersistenceEnabled());
      assertEquals(true, conf.isFileDeploymentEnabled());
      assertEquals(true, conf.isClustered());
      assertEquals(true, conf.isFileDeploymentEnabled());
      assertEquals(12345, conf.getScheduledThreadPoolMaxSize());
      assertEquals(54321, conf.getThreadPoolMaxSize());      
      assertEquals(false, conf.isSecurityEnabled());
      assertEquals(5423, conf.getSecurityInvalidationInterval());
      assertEquals(true, conf.isWildcardRoutingEnabled());
      assertEquals(new SimpleString("Giraffe"), conf.getManagementAddress());
      assertEquals(91, conf.getManagementRequestTimeout());
      assertEquals(new SimpleString("Whatever"), conf.getManagementNotificationAddress());
      assertEquals("Frog", conf.getManagementClusterUser());
      assertEquals("Wombat", conf.getManagementClusterPassword());
      assertEquals(false, conf.isJMXManagementEnabled());
      assertEquals("gro.qtenroh", conf.getJMXDomain());
      assertEquals(true, conf.isMessageCounterEnabled());
      assertEquals(5, conf.getMessageCounterMaxDayHistory());
      assertEquals(123456, conf.getMessageCounterSamplePeriod());
      assertEquals(12345, conf.getConnectionTTLOverride());
      assertEquals(98765, conf.getTransactionTimeout());
      assertEquals(56789, conf.getTransactionTimeoutScanPeriod());
      assertEquals(10111213, conf.getMessageExpiryScanPeriod());
      assertEquals("ocelot", conf.getLogDelegateFactoryClassName());
      assertEquals(8, conf.getMessageExpiryThreadPriority());
      assertEquals(127, conf.getIDCacheSize());
      assertEquals(true, conf.isPersistIDCache());
      assertEquals(true, conf.isBackup());
      assertEquals(true, conf.isSharedStore());
      assertEquals(true, conf.isPersistDeliveryCountBeforeDelivery());      
      assertEquals("pagingdir", conf.getPagingDirectory());
      assertEquals("somedir", conf.getBindingsDirectory());
      assertEquals(false, conf.isCreateBindingsDir());
      assertEquals("somedir2", conf.getJournalDirectory());
      assertEquals(false, conf.isCreateJournalDir());
      assertEquals(JournalType.NIO, conf.getJournalType());
      assertEquals(10000, conf.getAIOBufferSize());
      assertEquals(true, conf.isAIOFlushOnSync());      
      assertEquals(1000, conf.getAIOBufferTimeout());      
      assertEquals(false, conf.isJournalSyncTransactional());
      assertEquals(true, conf.isJournalSyncNonTransactional());
      assertEquals(12345678, conf.getJournalFileSize());
      assertEquals(100, conf.getJournalMinFiles());      
      assertEquals(123, conf.getJournalCompactMinFiles());
      assertEquals(33, conf.getJournalCompactPercentage());
      assertEquals(56546, conf.getJournalMaxAIO());
      assertEquals("largemessagesdir", conf.getLargeMessagesDirectory());
      assertEquals(95, conf.getMemoryWarningThreshold());
      
      assertEquals(2, conf.getInterceptorClassNames().size());
      assertTrue(conf.getInterceptorClassNames().contains("org.hornetq.tests.unit.core.config.impl.TestInterceptor1"));
      assertTrue(conf.getInterceptorClassNames().contains("org.hornetq.tests.unit.core.config.impl.TestInterceptor2"));
      
      assertEquals("backup-connector", conf.getBackupConnectorName());

      assertEquals(3, conf.getConnectorConfigurations().size());
      
      TransportConfiguration tc = conf.getConnectorConfigurations().get("connector1");
      assertNotNull(tc);
      assertEquals("org.hornetq.tests.unit.core.config.impl.TestConnectorFactory1", tc.getFactoryClassName());
      Object param = tc.getParams().get("a1");
      assertTrue(param instanceof String);
      assertEquals("v1", param);
      param = tc.getParams().get("a2");
      assertTrue(param instanceof Long);
      assertEquals(123, ((Long)param).longValue());
      param = tc.getParams().get("a3");
      assertTrue(param instanceof Integer);
      assertEquals(345, param);
      param = tc.getParams().get("a4");
      assertTrue(param instanceof String);
      assertEquals("v4", param);
      
      tc = conf.getConnectorConfigurations().get("connector2");
      assertNotNull(tc);
      assertEquals("org.hornetq.tests.unit.core.config.impl.TestConnectorFactory2", tc.getFactoryClassName());
      param = tc.getParams().get("b1");
      assertTrue(param instanceof String);
      assertEquals("w1", param);
      param = tc.getParams().get("b2");
      assertTrue(param instanceof Long);
      assertEquals(234, ((Long)param).longValue());
      param = tc.getParams().get("b3");
      assertTrue(param instanceof Integer);
      assertEquals(456, param);
      param = tc.getParams().get("b4");
      assertTrue(param instanceof String);
      assertEquals("w4", param);

      tc = conf.getConnectorConfigurations().get("backup-connector");
      assertNotNull(tc);
      assertEquals("org.hornetq.tests.unit.core.config.impl.TestConnectorFactory3", tc.getFactoryClassName());
      param = tc.getParams().get("c1");
      assertTrue(param instanceof String);
      assertEquals("x1", param);
      param = tc.getParams().get("c2");
      assertTrue(param instanceof Long);
      assertEquals(345, ((Long)param).longValue());
      param = tc.getParams().get("c3");
      assertTrue(param instanceof Integer);
      assertEquals(567, param);
      param = tc.getParams().get("c4");
      assertTrue(param instanceof String);
      assertEquals("x4", param);

      assertEquals(2, conf.getAcceptorConfigurations().size());
      for (TransportConfiguration ac: conf.getAcceptorConfigurations())
      {
         if (ac.getFactoryClassName().equals("org.hornetq.tests.unit.core.config.impl.TestAcceptorFactory1"))
         {
            assertEquals("org.hornetq.tests.unit.core.config.impl.TestAcceptorFactory1", ac.getFactoryClassName());
            param = ac.getParams().get("d1");
            assertTrue(param instanceof String);
            assertEquals("y1", param);
            param = ac.getParams().get("d2");
            assertTrue(param instanceof Long);
            assertEquals(456, ((Long)param).longValue());
            param = ac.getParams().get("d3");
            assertTrue(param instanceof Integer);
            assertEquals(678, param);
            param = ac.getParams().get("d4");
            assertTrue(param instanceof String);
            assertEquals("y4", param);
         }
         else
         {
            assertEquals("org.hornetq.tests.unit.core.config.impl.TestAcceptorFactory2", ac.getFactoryClassName());
            param = ac.getParams().get("e1");
            assertTrue(param instanceof String);
            assertEquals("z1", param);
            param = ac.getParams().get("e2");
            assertTrue(param instanceof Long);
            assertEquals(567, ((Long)param).longValue());
            param = ac.getParams().get("e3");
            assertTrue(param instanceof Integer);
            assertEquals(789, param);
            param = ac.getParams().get("e4");
            assertTrue(param instanceof String);
            assertEquals("z4", param);
         }
      }
      
      assertEquals(2, conf.getBroadcastGroupConfigurations().size());
      for (BroadcastGroupConfiguration bc : conf.getBroadcastGroupConfigurations())
      {
         if (bc.getName().equals("bg1"))
         {
            assertEquals("bg1", bc.getName());
            assertEquals(10999, bc.getLocalBindPort());
            assertEquals("192.168.0.120", bc.getGroupAddress());
            assertEquals(11999, bc.getGroupPort());
            assertEquals(12345, bc.getBroadcastPeriod());
            assertEquals("connector1", bc.getConnectorInfos().get(0).a);
            assertEquals(null, bc.getConnectorInfos().get(0).b);
         }
         else
         {
            assertEquals("bg2", bc.getName());
            assertEquals(12999, bc.getLocalBindPort());
            assertEquals("192.168.0.121", bc.getGroupAddress());
            assertEquals(13999, bc.getGroupPort());
            assertEquals(23456, bc.getBroadcastPeriod());
            assertEquals("connector2", bc.getConnectorInfos().get(0).a);
            assertEquals("backup-connector", bc.getConnectorInfos().get(0).b);
         }
      }
      
      assertEquals(2, conf.getDiscoveryGroupConfigurations().size());
      DiscoveryGroupConfiguration dc = conf.getDiscoveryGroupConfigurations().get("dg1");
      assertEquals("dg1", dc.getName());
      assertEquals("192.168.0.120", dc.getGroupAddress());
      assertEquals(11999, dc.getGroupPort());
      assertEquals(12345, dc.getRefreshTimeout());
      
      dc = conf.getDiscoveryGroupConfigurations().get("dg2");
      assertEquals("dg2", dc.getName());
      assertEquals("192.168.0.121", dc.getGroupAddress());
      assertEquals(12999, dc.getGroupPort());
      assertEquals(23456, dc.getRefreshTimeout());
      
      assertEquals(2, conf.getDivertConfigurations().size());
      for (DivertConfiguration dic : conf.getDivertConfigurations())
      {
         if (dic.getName().equals("divert1"))
         {
            assertEquals("divert1", dic.getName());
            assertEquals("routing-name1", dic.getRoutingName());
            assertEquals("address1", dic.getAddress());
            assertEquals("forwarding-address1", dic.getForwardingAddress());
            assertEquals("speed > 88", dic.getFilterString());
            assertEquals("org.foo.Transformer", dic.getTransformerClassName());
            assertEquals(true, dic.isExclusive());
         }
         else
         {
            assertEquals("divert2", dic.getName());
            assertEquals("routing-name2", dic.getRoutingName());
            assertEquals("address2", dic.getAddress());
            assertEquals("forwarding-address2", dic.getForwardingAddress());
            assertEquals("speed < 88", dic.getFilterString());
            assertEquals("org.foo.Transformer2", dic.getTransformerClassName());
            assertEquals(false, dic.isExclusive());
         }
      }
      
      assertEquals(2, conf.getBridgeConfigurations().size());
      for (BridgeConfiguration bc : conf.getBridgeConfigurations())
      {
         if (bc.getName().equals("bridge1"))
         {
            assertEquals("bridge1", bc.getName());
            assertEquals("queue1", bc.getQueueName());
            assertEquals("bridge-forwarding-address1", bc.getForwardingAddress());
            assertEquals("sku > 1", bc.getFilterString());
            assertEquals("org.foo.BridgeTransformer", bc.getTransformerClassName());
            assertEquals(3, bc.getRetryInterval());
            assertEquals(0.2, bc.getRetryIntervalMultiplier());
            assertEquals(2, bc.getReconnectAttempts());
            assertEquals(false, bc.isFailoverOnServerShutdown());
            assertEquals(true, bc.isUseDuplicateDetection());
            assertEquals("connector1", bc.getConnectorPair().a);
            assertEquals(null, bc.getConnectorPair().b);
            assertEquals(null, bc.getDiscoveryGroupName());
         }
         else
         {
            assertEquals("bridge2", bc.getName());
            assertEquals("queue2", bc.getQueueName());
            assertEquals("bridge-forwarding-address2", bc.getForwardingAddress());
            assertEquals(null, bc.getFilterString());
            assertEquals(null, bc.getTransformerClassName());
            assertEquals(null, bc.getConnectorPair());
            assertEquals("dg1", bc.getDiscoveryGroupName());            
         }
      }
      
      assertEquals(2, conf.getClusterConfigurations().size());
      for (ClusterConnectionConfiguration ccc : conf.getClusterConfigurations())
      {
         if (ccc.getName().equals("cluster-connection1"))
         {
            assertEquals("cluster-connection1", ccc.getName());
            assertEquals("queues1", ccc.getAddress());
            assertEquals(3, ccc.getRetryInterval());
            assertEquals(true, ccc.isDuplicateDetection());
            assertEquals(false, ccc.isForwardWhenNoConsumers());
            assertEquals(1, ccc.getMaxHops());
            assertEquals("connector1", ccc.getStaticConnectorNamePairs().get(0).a);
            assertEquals("backup-connector", ccc.getStaticConnectorNamePairs().get(0).b);
            assertEquals("connector2", ccc.getStaticConnectorNamePairs().get(1).a);
            assertEquals(null, ccc.getStaticConnectorNamePairs().get(1).b);
            assertEquals(null, ccc.getDiscoveryGroupName());
         }
         else
         {
            assertEquals("cluster-connection2", ccc.getName());
            assertEquals("queues2", ccc.getAddress());
            assertEquals(4, ccc.getRetryInterval());
            assertEquals(false, ccc.isDuplicateDetection());
            assertEquals(true, ccc.isForwardWhenNoConsumers());
            assertEquals(2, ccc.getMaxHops());
            assertEquals(null, ccc.getStaticConnectorNamePairs());
            assertEquals("dg1", ccc.getDiscoveryGroupName());            
         }
      }
      
   }
   
   public void testSetGetConfigurationURL()
   {
      final String file = "ghuuhhu";
      
      FileConfiguration fc = new FileConfiguration();
      
      fc.setConfigurationUrl(file);
      
      assertEquals(file, fc.getConfigurationUrl());
      
   }

   // Protected ---------------------------------------------------------------------------------------------
   
   protected Configuration createConfiguration() throws Exception
   {
      FileConfiguration fc = new FileConfiguration();
      
      fc.setConfigurationUrl("ConfigurationTest-full-config.xml");
      
      fc.start();
      
      return fc;
   }

}
