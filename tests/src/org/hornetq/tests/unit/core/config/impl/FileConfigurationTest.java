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

import junit.framework.Assert;

import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.config.BridgeConfiguration;
import org.hornetq.core.config.BroadcastGroupConfiguration;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.DivertConfiguration;
import org.hornetq.core.config.impl.FileConfiguration;
import org.hornetq.core.security.Role;
import org.hornetq.core.server.JournalType;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public class FileConfigurationTest extends ConfigurationImplTest
{
   @Override
   public void testDefaults()
   {
      // Check they match the values from the test file
      Assert.assertEquals("SomeNameForUseOnTheApplicationServer", conf.getName());
      Assert.assertEquals(false, conf.isPersistenceEnabled());
      Assert.assertEquals(true, conf.isFileDeploymentEnabled());
      Assert.assertEquals(true, conf.isClustered());
      Assert.assertEquals(true, conf.isFileDeploymentEnabled());
      Assert.assertEquals(12345, conf.getScheduledThreadPoolMaxSize());
      Assert.assertEquals(54321, conf.getThreadPoolMaxSize());
      Assert.assertEquals(false, conf.isSecurityEnabled());
      Assert.assertEquals(5423, conf.getSecurityInvalidationInterval());
      Assert.assertEquals(true, conf.isWildcardRoutingEnabled());
      Assert.assertEquals(new SimpleString("Giraffe"), conf.getManagementAddress());
      Assert.assertEquals(new SimpleString("Whatever"), conf.getManagementNotificationAddress());
      Assert.assertEquals("Frog", conf.getClusterUser());
      Assert.assertEquals("Wombat", conf.getClusterPassword());
      Assert.assertEquals(false, conf.isJMXManagementEnabled());
      Assert.assertEquals("gro.qtenroh", conf.getJMXDomain());
      Assert.assertEquals(true, conf.isMessageCounterEnabled());
      Assert.assertEquals(5, conf.getMessageCounterMaxDayHistory());
      Assert.assertEquals(123456, conf.getMessageCounterSamplePeriod());
      Assert.assertEquals(12345, conf.getConnectionTTLOverride());
      Assert.assertEquals(98765, conf.getTransactionTimeout());
      Assert.assertEquals(56789, conf.getTransactionTimeoutScanPeriod());
      Assert.assertEquals(10111213, conf.getMessageExpiryScanPeriod());
      Assert.assertEquals("ocelot", conf.getLogDelegateFactoryClassName());
      Assert.assertEquals(8, conf.getMessageExpiryThreadPriority());
      Assert.assertEquals(127, conf.getIDCacheSize());
      Assert.assertEquals(true, conf.isPersistIDCache());
      Assert.assertEquals(true, conf.isBackup());
      Assert.assertEquals(true, conf.isSharedStore());
      Assert.assertEquals(true, conf.isPersistDeliveryCountBeforeDelivery());
      Assert.assertEquals("pagingdir", conf.getPagingDirectory());
      Assert.assertEquals("somedir", conf.getBindingsDirectory());
      Assert.assertEquals(false, conf.isCreateBindingsDir());
      Assert.assertEquals("somedir2", conf.getJournalDirectory());
      Assert.assertEquals(false, conf.isCreateJournalDir());
      Assert.assertEquals(JournalType.NIO, conf.getJournalType());
      Assert.assertEquals(10000, conf.getJournalBufferSize_NIO());
      Assert.assertEquals(1000, conf.getJournalBufferTimeout_NIO());
      Assert.assertEquals(56546, conf.getJournalMaxIO_NIO());

      Assert.assertEquals(false, conf.isJournalSyncTransactional());
      Assert.assertEquals(true, conf.isJournalSyncNonTransactional());
      Assert.assertEquals(12345678, conf.getJournalFileSize());
      Assert.assertEquals(100, conf.getJournalMinFiles());
      Assert.assertEquals(123, conf.getJournalCompactMinFiles());
      Assert.assertEquals(33, conf.getJournalCompactPercentage());

      Assert.assertEquals("largemessagesdir", conf.getLargeMessagesDirectory());
      Assert.assertEquals(95, conf.getMemoryWarningThreshold());

      Assert.assertEquals(2, conf.getInterceptorClassNames().size());
      Assert.assertTrue(conf.getInterceptorClassNames()
                            .contains("org.hornetq.tests.unit.core.config.impl.TestInterceptor1"));
      Assert.assertTrue(conf.getInterceptorClassNames()
                            .contains("org.hornetq.tests.unit.core.config.impl.TestInterceptor2"));


      Assert.assertEquals(2, conf.getConnectorConfigurations().size());

      TransportConfiguration tc = conf.getConnectorConfigurations().get("connector1");
      Assert.assertNotNull(tc);
      Assert.assertEquals("org.hornetq.tests.unit.core.config.impl.TestConnectorFactory1", tc.getFactoryClassName());
      Assert.assertEquals("v1", tc.getParams().get("a1"));
      Assert.assertEquals("123", tc.getParams().get("a2"));
      Assert.assertEquals("345", tc.getParams().get("a3"));
      Assert.assertEquals("v4", tc.getParams().get("a4"));

      tc = conf.getConnectorConfigurations().get("connector2");
      Assert.assertNotNull(tc);
      Assert.assertEquals("org.hornetq.tests.unit.core.config.impl.TestConnectorFactory2", tc.getFactoryClassName());
      Assert.assertEquals("w1", tc.getParams().get("b1"));
      Assert.assertEquals("234", tc.getParams().get("b2"));

      Assert.assertEquals(2, conf.getAcceptorConfigurations().size());
      for (TransportConfiguration ac : conf.getAcceptorConfigurations())
      {
         if (ac.getFactoryClassName().equals("org.hornetq.tests.unit.core.config.impl.TestAcceptorFactory1"))
         {
            Assert.assertEquals("org.hornetq.tests.unit.core.config.impl.TestAcceptorFactory1",
                                ac.getFactoryClassName());
            Assert.assertEquals("y1", ac.getParams().get("d1"));
            Assert.assertEquals("456", ac.getParams().get("d2"));
         }
         else
         {
            Assert.assertEquals("org.hornetq.tests.unit.core.config.impl.TestAcceptorFactory2",
                                ac.getFactoryClassName());
            Assert.assertEquals("z1", ac.getParams().get("e1"));
            Assert.assertEquals("567", ac.getParams().get("e2"));
         }
      }

      Assert.assertEquals(2, conf.getBroadcastGroupConfigurations().size());
      for (BroadcastGroupConfiguration bc : conf.getBroadcastGroupConfigurations())
      {
         if (bc.getName().equals("bg1"))
         {
            Assert.assertEquals("bg1", bc.getName());
            Assert.assertEquals(10999, bc.getLocalBindPort());
            Assert.assertEquals("192.168.0.120", bc.getGroupAddress());
            Assert.assertEquals(11999, bc.getGroupPort());
            Assert.assertEquals(12345, bc.getBroadcastPeriod());
            Assert.assertEquals("connector1", bc.getConnectorInfos().get(0));
         }
         else
         {
            Assert.assertEquals("bg2", bc.getName());
            Assert.assertEquals(12999, bc.getLocalBindPort());
            Assert.assertEquals("192.168.0.121", bc.getGroupAddress());
            Assert.assertEquals(13999, bc.getGroupPort());
            Assert.assertEquals(23456, bc.getBroadcastPeriod());
            Assert.assertEquals("connector2", bc.getConnectorInfos().get(0));
         }
      }

      Assert.assertEquals(2, conf.getDiscoveryGroupConfigurations().size());
      DiscoveryGroupConfiguration dc = conf.getDiscoveryGroupConfigurations().get("dg1");
      Assert.assertEquals("dg1", dc.getName());
      Assert.assertEquals("192.168.0.120", dc.getGroupAddress());
      assertEquals("172.16.8.10", dc.getLocalBindAddress());
      Assert.assertEquals(11999, dc.getGroupPort());
      Assert.assertEquals(12345, dc.getRefreshTimeout());

      dc = conf.getDiscoveryGroupConfigurations().get("dg2");
      Assert.assertEquals("dg2", dc.getName());
      Assert.assertEquals("192.168.0.121", dc.getGroupAddress());
      assertEquals("172.16.8.11", dc.getLocalBindAddress());
      Assert.assertEquals(12999, dc.getGroupPort());
      Assert.assertEquals(23456, dc.getRefreshTimeout());

      Assert.assertEquals(2, conf.getDivertConfigurations().size());
      for (DivertConfiguration dic : conf.getDivertConfigurations())
      {
         if (dic.getName().equals("divert1"))
         {
            Assert.assertEquals("divert1", dic.getName());
            Assert.assertEquals("routing-name1", dic.getRoutingName());
            Assert.assertEquals("address1", dic.getAddress());
            Assert.assertEquals("forwarding-address1", dic.getForwardingAddress());
            Assert.assertEquals("speed > 88", dic.getFilterString());
            Assert.assertEquals("org.foo.Transformer", dic.getTransformerClassName());
            Assert.assertEquals(true, dic.isExclusive());
         }
         else
         {
            Assert.assertEquals("divert2", dic.getName());
            Assert.assertEquals("routing-name2", dic.getRoutingName());
            Assert.assertEquals("address2", dic.getAddress());
            Assert.assertEquals("forwarding-address2", dic.getForwardingAddress());
            Assert.assertEquals("speed < 88", dic.getFilterString());
            Assert.assertEquals("org.foo.Transformer2", dic.getTransformerClassName());
            Assert.assertEquals(false, dic.isExclusive());
         }
      }

      Assert.assertEquals(2, conf.getBridgeConfigurations().size());
      for (BridgeConfiguration bc : conf.getBridgeConfigurations())
      {
         if (bc.getName().equals("bridge1"))
         {
            Assert.assertEquals("bridge1", bc.getName());
            Assert.assertEquals("queue1", bc.getQueueName());
            Assert.assertEquals(4, bc.getMinLargeMessageSize());
            Assert.assertEquals("bridge-forwarding-address1", bc.getForwardingAddress());
            Assert.assertEquals("sku > 1", bc.getFilterString());
            Assert.assertEquals("org.foo.BridgeTransformer", bc.getTransformerClassName());
            Assert.assertEquals(3, bc.getRetryInterval());
            Assert.assertEquals(0.2, bc.getRetryIntervalMultiplier());
            Assert.assertEquals(2, bc.getReconnectAttempts());
            Assert.assertEquals(true, bc.isUseDuplicateDetection());
            Assert.assertEquals("connector1", bc.getStaticConnectors().get(0));
            Assert.assertEquals(null, bc.getDiscoveryGroupName());
         }
         else
         {
            Assert.assertEquals("bridge2", bc.getName());
            Assert.assertEquals("queue2", bc.getQueueName());
            Assert.assertEquals("bridge-forwarding-address2", bc.getForwardingAddress());
            Assert.assertEquals(null, bc.getFilterString());
            Assert.assertEquals(null, bc.getTransformerClassName());
            Assert.assertEquals(null, bc.getStaticConnectors());
            Assert.assertEquals("dg1", bc.getDiscoveryGroupName());
         }
      }

      Assert.assertEquals(2, conf.getClusterConfigurations().size());
      for (ClusterConnectionConfiguration ccc : conf.getClusterConfigurations())
      {
         if (ccc.getName().equals("cluster-connection1"))
         {
            Assert.assertEquals("cluster-connection1", ccc.getName());
            Assert.assertEquals(321, ccc.getMinLargeMessageSize());
            Assert.assertEquals("queues1", ccc.getAddress());
            Assert.assertEquals(3, ccc.getRetryInterval());
            Assert.assertEquals(true, ccc.isDuplicateDetection());
            Assert.assertEquals(false, ccc.isForwardWhenNoConsumers());
            Assert.assertEquals(1, ccc.getMaxHops());
            Assert.assertEquals(123, ccc.getCallTimeout());
            Assert.assertEquals("connector1", ccc.getStaticConnectors().get(0));
            Assert.assertEquals("connector2", ccc.getStaticConnectors().get(1));
            Assert.assertEquals(null, ccc.getDiscoveryGroupName());
         }
         else
         {
            Assert.assertEquals("cluster-connection2", ccc.getName());
            Assert.assertEquals("queues2", ccc.getAddress());
            Assert.assertEquals(4, ccc.getRetryInterval());
            Assert.assertEquals(456, ccc.getCallTimeout());
            Assert.assertEquals(false, ccc.isDuplicateDetection());
            Assert.assertEquals(true, ccc.isForwardWhenNoConsumers());
            Assert.assertEquals(2, ccc.getMaxHops());
            Assert.assertEquals(null, ccc.getStaticConnectors());
            Assert.assertEquals("dg1", ccc.getDiscoveryGroupName());
         }
      }
      
      
      assertEquals(2, conf.getAddressesSettings().size());
      
      assertTrue(conf.getAddressesSettings().get("a1") != null);
      assertTrue(conf.getAddressesSettings().get("a2") != null);
      
      assertEquals("a1.1", conf.getAddressesSettings().get("a1").getDeadLetterAddress().toString());
      assertEquals("a1.2", conf.getAddressesSettings().get("a1").getExpiryAddress().toString());
      assertEquals(1, conf.getAddressesSettings().get("a1").getRedeliveryDelay());
      assertEquals(81781728121878l, conf.getAddressesSettings().get("a1").getMaxSizeBytes());
      assertEquals(81738173872337l, conf.getAddressesSettings().get("a1").getPageSizeBytes());
      assertEquals(10, conf.getAddressesSettings().get("a1").getPageCacheMaxSize());
      assertEquals(4, conf.getAddressesSettings().get("a1").getMessageCounterHistoryDayLimit());

      assertEquals("a2.1", conf.getAddressesSettings().get("a2").getDeadLetterAddress().toString());
      assertEquals("a2.2", conf.getAddressesSettings().get("a2").getExpiryAddress().toString());
      assertEquals(5, conf.getAddressesSettings().get("a2").getRedeliveryDelay());
      assertEquals(932489234928324l, conf.getAddressesSettings().get("a2").getMaxSizeBytes());
      assertEquals(7126716262626l, conf.getAddressesSettings().get("a2").getPageSizeBytes());
      assertEquals(20, conf.getAddressesSettings().get("a2").getPageCacheMaxSize());
      assertEquals(8, conf.getAddressesSettings().get("a2").getMessageCounterHistoryDayLimit());
      
      
      assertEquals(2, conf.getQueueConfigurations().size());
      
      assertEquals("queue1", conf.getQueueConfigurations().get(0).getName());
      assertEquals("address1", conf.getQueueConfigurations().get(0).getAddress());
      assertEquals("color='red'", conf.getQueueConfigurations().get(0).getFilterString());
      assertEquals(false, conf.getQueueConfigurations().get(0).isDurable());
      
      assertEquals("queue2", conf.getQueueConfigurations().get(1).getName());
      assertEquals("address2", conf.getQueueConfigurations().get(1).getAddress());
      assertEquals("color='blue'", conf.getQueueConfigurations().get(1).getFilterString());
      assertEquals(false, conf.getQueueConfigurations().get(1).isDurable());
      
      assertEquals(2, conf.getSecurityRoles().size());

      assertTrue(conf.getSecurityRoles().containsKey("a1"));
      
      assertTrue(conf.getSecurityRoles().containsKey("a2"));
      
      Role a1Role = conf.getSecurityRoles().get("a1").toArray(new Role[1])[0];
      
      assertFalse(a1Role.isSend());
      assertFalse(a1Role.isConsume());
      assertFalse(a1Role.isCreateDurableQueue());
      assertFalse(a1Role.isDeleteDurableQueue());
      assertTrue(a1Role.isCreateNonDurableQueue());
      assertFalse(a1Role.isDeleteNonDurableQueue());
      assertFalse(a1Role.isManage());
      
      Role a2Role = conf.getSecurityRoles().get("a2").toArray(new Role[1])[0];
      
      assertFalse(a2Role.isSend());
      assertFalse(a2Role.isConsume());
      assertFalse(a2Role.isCreateDurableQueue());
      assertFalse(a2Role.isDeleteDurableQueue());
      assertFalse(a2Role.isCreateNonDurableQueue());
      assertTrue(a2Role.isDeleteNonDurableQueue());
      assertFalse(a2Role.isManage());
      

   }

   public void testSetGetConfigurationURL()
   {
      final String file = "ghuuhhu";

      FileConfiguration fc = new FileConfiguration();

      fc.setConfigurationUrl(file);

      Assert.assertEquals(file, fc.getConfigurationUrl());

   }

   // Protected ---------------------------------------------------------------------------------------------

   @Override
   protected Configuration createConfiguration() throws Exception
   {
      FileConfiguration fc = new FileConfiguration();

      fc.setConfigurationUrl("ConfigurationTest-full-config.xml");

      fc.start();

      return fc;
   }

}
