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

package org.hornetq.tests.integration.management;

import static org.hornetq.tests.util.RandomUtil.randomBoolean;
import static org.hornetq.tests.util.RandomUtil.randomString;

import java.util.HashMap;
import java.util.Map;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.management.HornetQServerControl;
import org.hornetq.core.management.ObjectNameBuilder;
import org.hornetq.core.management.QueueControl;
import org.hornetq.core.messagecounter.impl.MessageCounterManagerImpl;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.utils.SimpleString;
import org.hornetq.utils.json.JSONArray;
import org.hornetq.utils.json.JSONObject;

/**
 * A QueueControlTest
 *
 * @author jmesnil
 * 
 * Created 26 nov. 2008 14:18:48
 *
 *
 */
public class HornetQServerControlTest extends ManagementTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private HornetQServer server;

   private Configuration conf;

   private TransportConfiguration connectorConfig;

   // Static --------------------------------------------------------

   private static boolean contains(String name, String[] strings)
   {
      boolean found = false;
      for (String str : strings)
      {
         if (name.equals(str))
         {
            found = true;
            break;
         }
      } 
      return found;
   }
   
   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testGetAttributes() throws Exception
   {
      HornetQServerControl serverControl = createManagementControl();

      assertEquals(server.getVersion().getFullVersion(), serverControl.getVersion());

      assertEquals(conf.isClustered(), serverControl.isClustered());     
      assertEquals(conf.isPersistDeliveryCountBeforeDelivery(), serverControl.isPersistDeliveryCountBeforeDelivery());
      assertEquals(conf.isBackup(), serverControl.isBackup());
      assertEquals(conf.isSharedStore(), serverControl.isSharedStore());
      assertEquals(conf.getScheduledThreadPoolMaxSize(), serverControl.getScheduledThreadPoolMaxSize());
      assertEquals(conf.getThreadPoolMaxSize(), serverControl.getThreadPoolMaxSize());
      assertEquals(conf.getSecurityInvalidationInterval(), serverControl.getSecurityInvalidationInterval());
      assertEquals(conf.isSecurityEnabled(), serverControl.isSecurityEnabled());
      assertEquals(conf.getInterceptorClassNames().size(), serverControl.getInterceptorClassNames().length);     
      assertEquals(conf.getConnectionTTLOverride(), serverControl.getConnectionTTLOverride());
      assertEquals(conf.getBackupConnectorName(), serverControl.getBackupConnectorName());
      assertEquals(conf.getManagementAddress().toString(), serverControl.getManagementAddress());
      assertEquals(conf.getManagementNotificationAddress().toString(), serverControl.getManagementNotificationAddress());
      assertEquals(conf.getManagementRequestTimeout(), serverControl.getManagementRequestTimeout());
      assertEquals(conf.getIDCacheSize(), serverControl.getIDCacheSize());
      assertEquals(conf.isPersistIDCache(), serverControl.isPersistIDCache());
      assertEquals(conf.getBindingsDirectory(), serverControl.getBindingsDirectory());
      assertEquals(conf.getJournalDirectory(), serverControl.getJournalDirectory());
      assertEquals(conf.getJournalType().toString(), serverControl.getJournalType());
      assertEquals(conf.isJournalSyncTransactional(), serverControl.isJournalSyncTransactional());
      assertEquals(conf.isJournalSyncNonTransactional(), serverControl.isJournalSyncNonTransactional());
      assertEquals(conf.getJournalFileSize(), serverControl.getJournalFileSize());
      assertEquals(conf.getJournalMinFiles(), serverControl.getJournalMinFiles());
      assertEquals(conf.getJournalMaxAIO(), serverControl.getJournalMaxAIO());
      assertEquals(conf.getAIOBufferSize(), serverControl.getAIOBufferSize());
      assertEquals(conf.getAIOBufferTimeout(), serverControl.getAIOBufferTimeout());      
      assertEquals(conf.isCreateBindingsDir(), serverControl.isCreateBindingsDir());
      assertEquals(conf.isCreateJournalDir(), serverControl.isCreateJournalDir());      
      assertEquals(conf.getPagingDirectory(), serverControl.getPagingDirectory());
      assertEquals(conf.getLargeMessagesDirectory(), serverControl.getLargeMessagesDirectory());
      assertEquals(conf.isWildcardRoutingEnabled(), serverControl.isWildcardRoutingEnabled());
      assertEquals(conf.getTransactionTimeout(), serverControl.getTransactionTimeout());
      assertEquals(conf.isMessageCounterEnabled(), serverControl.isMessageCounterEnabled());
      assertEquals(conf.getTransactionTimeoutScanPeriod(), serverControl.getTransactionTimeoutScanPeriod());
      assertEquals(conf.getMessageExpiryScanPeriod(), serverControl.getMessageExpiryScanPeriod());
      assertEquals(conf.getMessageExpiryThreadPriority(), serverControl.getMessageExpiryThreadPriority());
      assertEquals(conf.getJournalCompactMinFiles(), serverControl.getJournalCompactMinFiles());
      assertEquals(conf.getJournalCompactPercentage(), serverControl.getJournalCompactPercentage());
      assertEquals(conf.isPersistenceEnabled(), serverControl.isPersistenceEnabled());
   }

   public void testGetConnectors() throws Exception
   {
      HornetQServerControl serverControl = createManagementControl();

      Object[] connectorData = serverControl.getConnectors();
      assertNotNull(connectorData);
      assertEquals(1, connectorData.length);

      Object[] config = (Object[])connectorData[0];           

      assertEquals(connectorConfig.getName(), config[0]);
   }
   
   public void testGetConnectorsAsJSON() throws Exception
   {
      HornetQServerControl serverControl = createManagementControl();

      String jsonString = serverControl.getConnectorsAsJSON();
      assertNotNull(jsonString);
      JSONArray array = new JSONArray(jsonString);
      assertEquals(1, array.length());
      JSONObject data = array.getJSONObject(0);
      assertEquals(connectorConfig.getName(), data.optString("name"));
      assertEquals(connectorConfig.getFactoryClassName(), data.optString("factoryClassName"));
      assertEquals(connectorConfig.getParams().size(), data.getJSONObject("params").length());
   }

   public void testCreateAndDestroyQueue() throws Exception
   {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString name = RandomUtil.randomSimpleString();

      HornetQServerControl serverControl = createManagementControl();

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name));

      serverControl.createQueue(address.toString(), name.toString());

      checkResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name));
      QueueControl queueControl = ManagementControlHelper.createQueueControl(address, name, mbeanServer);
      assertEquals(address.toString(), queueControl.getAddress());
      assertEquals(name.toString(), queueControl.getName());
      assertNull(queueControl.getFilter());
      assertEquals(true, queueControl.isDurable());
      assertEquals(false, queueControl.isTemporary());

      serverControl.destroyQueue(name.toString());

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name));
   }

   public void testCreateAndDestroyQueue_2() throws Exception
   {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString name = RandomUtil.randomSimpleString();
      String filter = "color = 'green'";
      boolean durable = true;

      HornetQServerControl serverControl = createManagementControl();

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name));

      serverControl.createQueue(address.toString(), name.toString(), filter, durable);

      checkResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name));
      QueueControl queueControl = ManagementControlHelper.createQueueControl(address, name, mbeanServer);
      assertEquals(address.toString(), queueControl.getAddress());
      assertEquals(name.toString(), queueControl.getName());
      assertEquals(filter, queueControl.getFilter());
      assertEquals(durable, queueControl.isDurable());
      assertEquals(false, queueControl.isTemporary());

      serverControl.destroyQueue(name.toString());

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name));
   }
   
   public void testCreateAndDestroyQueue_3() throws Exception
   {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString name = RandomUtil.randomSimpleString();
      boolean durable = true;

      HornetQServerControl serverControl = createManagementControl();

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name));

      serverControl.createQueue(address.toString(), name.toString(), durable);

      checkResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name));
      QueueControl queueControl = ManagementControlHelper.createQueueControl(address, name, mbeanServer);
      assertEquals(address.toString(), queueControl.getAddress());
      assertEquals(name.toString(), queueControl.getName());
      assertNull(queueControl.getFilter());
      assertEquals(durable, queueControl.isDurable());
      assertEquals(false, queueControl.isTemporary());

      serverControl.destroyQueue(name.toString());

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name));
   }

   public void testCreateAndDestroyQueueWithNullFilter() throws Exception
   {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString name = RandomUtil.randomSimpleString();
      String filter = null;
      boolean durable = true;

      HornetQServerControl serverControl = createManagementControl();

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name));

      serverControl.createQueue(address.toString(), name.toString(), filter, durable);

      checkResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name));
      QueueControl queueControl = ManagementControlHelper.createQueueControl(address, name, mbeanServer);
      assertEquals(address.toString(), queueControl.getAddress());
      assertEquals(name.toString(), queueControl.getName());
      assertNull(queueControl.getFilter());
      assertEquals(durable, queueControl.isDurable());
      assertEquals(false, queueControl.isTemporary());

      serverControl.destroyQueue(name.toString());

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name));
   }
   
   public void testCreateAndDestroyQueueWithEmptyStringForFilter() throws Exception
   {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString name = RandomUtil.randomSimpleString();
      String filter = "";
      boolean durable = true;

      HornetQServerControl serverControl = createManagementControl();

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name));

      serverControl.createQueue(address.toString(), name.toString(), filter, durable);

      checkResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name));
      QueueControl queueControl = ManagementControlHelper.createQueueControl(address, name, mbeanServer);
      assertEquals(address.toString(), queueControl.getAddress());
      assertEquals(name.toString(), queueControl.getName());
      assertNull(queueControl.getFilter());
      assertEquals(durable, queueControl.isDurable());
      assertEquals(false, queueControl.isTemporary());

      serverControl.destroyQueue(name.toString());

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name));
   }
   
   public void testGetQueueNames() throws Exception
   {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString name = RandomUtil.randomSimpleString();

      HornetQServerControl serverControl = createManagementControl();

      // due to replication, there can be another queue created for replicating
      // management operations

      assertFalse(contains(name.toString(), serverControl.getQueueNames()));
      
      serverControl.createQueue(address.toString(), name.toString());
      assertTrue(contains(name.toString(), serverControl.getQueueNames()));

      serverControl.destroyQueue(name.toString());
      assertFalse(contains(name.toString(), serverControl.getQueueNames()));
   }

   public void testGetAddressNames() throws Exception
   {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString name = RandomUtil.randomSimpleString();

      HornetQServerControl serverControl = createManagementControl();

      // due to replication, there can be another queue created for replicating
      // management operations

      assertFalse(contains(address.toString(), serverControl.getAddressNames()));
      
      serverControl.createQueue(address.toString(), name.toString());
      assertTrue(contains(address.toString(), serverControl.getAddressNames()));

      serverControl.destroyQueue(name.toString());
      assertFalse(contains(address.toString(), serverControl.getAddressNames()));
   }

   public void testMessageCounterMaxDayCount() throws Exception
   {
      HornetQServerControl serverControl = createManagementControl();

      assertEquals(MessageCounterManagerImpl.DEFAULT_MAX_DAY_COUNT, serverControl.getMessageCounterMaxDayCount());

      int newCount = 100;
      serverControl.setMessageCounterMaxDayCount(newCount);

      assertEquals(newCount, serverControl.getMessageCounterMaxDayCount());

      try
      {
         serverControl.setMessageCounterMaxDayCount(-1);
         fail();
      }
      catch (Exception e)
      {
      }

      try
      {
         serverControl.setMessageCounterMaxDayCount(0);
         fail();
      }
      catch (Exception e)
      {
      }

      assertEquals(newCount, serverControl.getMessageCounterMaxDayCount());
   }

   public void testGetMessageCounterSamplePeriod() throws Exception
   {
      HornetQServerControl serverControl = createManagementControl();

      assertEquals(MessageCounterManagerImpl.DEFAULT_SAMPLE_PERIOD, serverControl.getMessageCounterSamplePeriod());

      long newSample = 20000;
      serverControl.setMessageCounterSamplePeriod(newSample);

      assertEquals(newSample, serverControl.getMessageCounterSamplePeriod());

      try
      {
         serverControl.setMessageCounterSamplePeriod(-1);
         fail();
      }
      catch (Exception e)
      {
      }

      try
      {
         serverControl.setMessageCounterSamplePeriod(0);
         fail();
      }
      catch (Exception e)
      {
      }

      try
      {
         serverControl.setMessageCounterSamplePeriod(MessageCounterManagerImpl.MIN_SAMPLE_PERIOD - 1);
         fail();
      }
      catch (Exception e)
      {
      }

      assertEquals(newSample, serverControl.getMessageCounterSamplePeriod());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      Map<String, Object> params = new HashMap<String, Object>();
      params.put(randomString(), randomBoolean());
      connectorConfig = new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                                          params,
                                                                          randomString());

      conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      
      server = HornetQ.newHornetQServer(conf, mbeanServer, false);
      conf.getConnectorConfigurations().put(connectorConfig.getName(), connectorConfig);
      server.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      if (server != null)
      {
         server.stop();
      }
      
      server = null;
      
      connectorConfig = null;

      super.tearDown();
   }

   protected HornetQServerControl createManagementControl() throws Exception
   {
      return ManagementControlHelper.createHornetQServerControl(mbeanServer);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
