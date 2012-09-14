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

import java.util.HashMap;
import java.util.Map;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import junit.framework.Assert;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.core.management.AddressSettingsInfo;
import org.hornetq.api.core.management.BridgeControl;
import org.hornetq.api.core.management.DivertControl;
import org.hornetq.api.core.management.HornetQServerControl;
import org.hornetq.api.core.management.ObjectNameBuilder;
import org.hornetq.api.core.management.QueueControl;
import org.hornetq.api.core.management.RoleInfo;
import org.hornetq.core.asyncio.impl.AsynchronousFileImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.messagecounter.impl.MessageCounterManagerImpl;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.core.transaction.impl.XidImpl;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.UUIDGenerator;
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

   private static boolean contains(final String name, final String[] strings)
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

      Assert.assertEquals(server.getVersion().getFullVersion(), serverControl.getVersion());

      Assert.assertEquals(conf.isClustered(), serverControl.isClustered());
      Assert.assertEquals(conf.isPersistDeliveryCountBeforeDelivery(),
                          serverControl.isPersistDeliveryCountBeforeDelivery());
      Assert.assertEquals(conf.isBackup(), serverControl.isBackup());
      Assert.assertEquals(conf.isSharedStore(), serverControl.isSharedStore());
      Assert.assertEquals(conf.getScheduledThreadPoolMaxSize(), serverControl.getScheduledThreadPoolMaxSize());
      Assert.assertEquals(conf.getThreadPoolMaxSize(), serverControl.getThreadPoolMaxSize());
      Assert.assertEquals(conf.getSecurityInvalidationInterval(), serverControl.getSecurityInvalidationInterval());
      Assert.assertEquals(conf.isSecurityEnabled(), serverControl.isSecurityEnabled());
      Assert.assertEquals(conf.isAsyncConnectionExecutionEnabled(), serverControl.isAsyncConnectionExecutionEnabled());
      Assert.assertEquals(conf.getInterceptorClassNames().size(), serverControl.getInterceptorClassNames().length);
      Assert.assertEquals(conf.getConnectionTTLOverride(), serverControl.getConnectionTTLOverride());
      //Assert.assertEquals(conf.getBackupConnectorName(), serverControl.getBackupConnectorName());
      Assert.assertEquals(conf.getManagementAddress().toString(), serverControl.getManagementAddress());
      Assert.assertEquals(conf.getManagementNotificationAddress().toString(),
                          serverControl.getManagementNotificationAddress());
      Assert.assertEquals(conf.getIDCacheSize(), serverControl.getIDCacheSize());
      Assert.assertEquals(conf.isPersistIDCache(), serverControl.isPersistIDCache());
      Assert.assertEquals(conf.getBindingsDirectory(), serverControl.getBindingsDirectory());
      Assert.assertEquals(conf.getJournalDirectory(), serverControl.getJournalDirectory());
      Assert.assertEquals(conf.getJournalType().toString(), serverControl.getJournalType());
      Assert.assertEquals(conf.isJournalSyncTransactional(), serverControl.isJournalSyncTransactional());
      Assert.assertEquals(conf.isJournalSyncNonTransactional(), serverControl.isJournalSyncNonTransactional());
      Assert.assertEquals(conf.getJournalFileSize(), serverControl.getJournalFileSize());
      Assert.assertEquals(conf.getJournalMinFiles(), serverControl.getJournalMinFiles());
      if (AsynchronousFileImpl.isLoaded())
      {
         Assert.assertEquals(conf.getJournalMaxIO_AIO(), serverControl.getJournalMaxIO());
         Assert.assertEquals(conf.getJournalBufferSize_AIO(), serverControl.getJournalBufferSize());
         Assert.assertEquals(conf.getJournalBufferTimeout_AIO(), serverControl.getJournalBufferTimeout());
      }
      Assert.assertEquals(conf.isCreateBindingsDir(), serverControl.isCreateBindingsDir());
      Assert.assertEquals(conf.isCreateJournalDir(), serverControl.isCreateJournalDir());
      Assert.assertEquals(conf.getPagingDirectory(), serverControl.getPagingDirectory());
      Assert.assertEquals(conf.getLargeMessagesDirectory(), serverControl.getLargeMessagesDirectory());
      Assert.assertEquals(conf.isWildcardRoutingEnabled(), serverControl.isWildcardRoutingEnabled());
      Assert.assertEquals(conf.getTransactionTimeout(), serverControl.getTransactionTimeout());
      Assert.assertEquals(conf.isMessageCounterEnabled(), serverControl.isMessageCounterEnabled());
      Assert.assertEquals(conf.getTransactionTimeoutScanPeriod(), serverControl.getTransactionTimeoutScanPeriod());
      Assert.assertEquals(conf.getMessageExpiryScanPeriod(), serverControl.getMessageExpiryScanPeriod());
      Assert.assertEquals(conf.getMessageExpiryThreadPriority(), serverControl.getMessageExpiryThreadPriority());
      Assert.assertEquals(conf.getJournalCompactMinFiles(), serverControl.getJournalCompactMinFiles());
      Assert.assertEquals(conf.getJournalCompactPercentage(), serverControl.getJournalCompactPercentage());
      Assert.assertEquals(conf.isPersistenceEnabled(), serverControl.isPersistenceEnabled());
      Assert.assertEquals(conf.isFailoverOnServerShutdown(), serverControl.isFailoverOnServerShutdown());
   }

   public void testGetConnectors() throws Exception
   {
      HornetQServerControl serverControl = createManagementControl();

      Object[] connectorData = serverControl.getConnectors();
      Assert.assertNotNull(connectorData);
      Assert.assertEquals(1, connectorData.length);

      Object[] config = (Object[])connectorData[0];

      Assert.assertEquals(connectorConfig.getName(), config[0]);
   }

   public void testGetConnectorsAsJSON() throws Exception
   {
      HornetQServerControl serverControl = createManagementControl();

      String jsonString = serverControl.getConnectorsAsJSON();
      Assert.assertNotNull(jsonString);
      JSONArray array = new JSONArray(jsonString);
      Assert.assertEquals(1, array.length());
      JSONObject data = array.getJSONObject(0);
      Assert.assertEquals(connectorConfig.getName(), data.optString("name"));
      Assert.assertEquals(connectorConfig.getFactoryClassName(), data.optString("factoryClassName"));
      Assert.assertEquals(connectorConfig.getParams().size(), data.getJSONObject("params").length());
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
      Assert.assertEquals(address.toString(), queueControl.getAddress());
      Assert.assertEquals(name.toString(), queueControl.getName());
      Assert.assertNull(queueControl.getFilter());
      Assert.assertEquals(true, queueControl.isDurable());
      Assert.assertEquals(false, queueControl.isTemporary());

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
      Assert.assertEquals(address.toString(), queueControl.getAddress());
      Assert.assertEquals(name.toString(), queueControl.getName());
      Assert.assertEquals(filter, queueControl.getFilter());
      Assert.assertEquals(durable, queueControl.isDurable());
      Assert.assertEquals(false, queueControl.isTemporary());

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
      Assert.assertEquals(address.toString(), queueControl.getAddress());
      Assert.assertEquals(name.toString(), queueControl.getName());
      Assert.assertNull(queueControl.getFilter());
      Assert.assertEquals(durable, queueControl.isDurable());
      Assert.assertEquals(false, queueControl.isTemporary());

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
      Assert.assertEquals(address.toString(), queueControl.getAddress());
      Assert.assertEquals(name.toString(), queueControl.getName());
      Assert.assertNull(queueControl.getFilter());
      Assert.assertEquals(durable, queueControl.isDurable());
      Assert.assertEquals(false, queueControl.isTemporary());

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
      Assert.assertEquals(address.toString(), queueControl.getAddress());
      Assert.assertEquals(name.toString(), queueControl.getName());
      Assert.assertNull(queueControl.getFilter());
      Assert.assertEquals(durable, queueControl.isDurable());
      Assert.assertEquals(false, queueControl.isTemporary());

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

      Assert.assertFalse(HornetQServerControlTest.contains(name.toString(), serverControl.getQueueNames()));

      serverControl.createQueue(address.toString(), name.toString());
      Assert.assertTrue(HornetQServerControlTest.contains(name.toString(), serverControl.getQueueNames()));

      serverControl.destroyQueue(name.toString());
      Assert.assertFalse(HornetQServerControlTest.contains(name.toString(), serverControl.getQueueNames()));
   }

   public void testGetAddressNames() throws Exception
   {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString name = RandomUtil.randomSimpleString();

      HornetQServerControl serverControl = createManagementControl();

      // due to replication, there can be another queue created for replicating
      // management operations

      Assert.assertFalse(HornetQServerControlTest.contains(address.toString(), serverControl.getAddressNames()));

      serverControl.createQueue(address.toString(), name.toString());
      Assert.assertTrue(HornetQServerControlTest.contains(address.toString(), serverControl.getAddressNames()));

      serverControl.destroyQueue(name.toString());
      Assert.assertFalse(HornetQServerControlTest.contains(address.toString(), serverControl.getAddressNames()));
   }

   public void testMessageCounterMaxDayCount() throws Exception
   {
      HornetQServerControl serverControl = createManagementControl();

      Assert.assertEquals(MessageCounterManagerImpl.DEFAULT_MAX_DAY_COUNT, serverControl.getMessageCounterMaxDayCount());

      int newCount = 100;
      serverControl.setMessageCounterMaxDayCount(newCount);

      Assert.assertEquals(newCount, serverControl.getMessageCounterMaxDayCount());

      try
      {
         serverControl.setMessageCounterMaxDayCount(-1);
         Assert.fail();
      }
      catch (Exception e)
      {
      }

      try
      {
         serverControl.setMessageCounterMaxDayCount(0);
         Assert.fail();
      }
      catch (Exception e)
      {
      }

      Assert.assertEquals(newCount, serverControl.getMessageCounterMaxDayCount());
   }

   public void testGetMessageCounterSamplePeriod() throws Exception
   {
      HornetQServerControl serverControl = createManagementControl();

      Assert.assertEquals(MessageCounterManagerImpl.DEFAULT_SAMPLE_PERIOD,
                          serverControl.getMessageCounterSamplePeriod());

      long newSample = 20000;
      serverControl.setMessageCounterSamplePeriod(newSample);

      Assert.assertEquals(newSample, serverControl.getMessageCounterSamplePeriod());

      try
      {
         serverControl.setMessageCounterSamplePeriod(-1);
         Assert.fail();
      }
      catch (Exception e)
      {
      }

      try
      {
         serverControl.setMessageCounterSamplePeriod(0);
         Assert.fail();
      }
      catch (Exception e)
      {
      }

      try
      {
         serverControl.setMessageCounterSamplePeriod(MessageCounterManagerImpl.MIN_SAMPLE_PERIOD - 1);
         Assert.fail();
      }
      catch (Exception e)
      {
      }

      Assert.assertEquals(newSample, serverControl.getMessageCounterSamplePeriod());
   }

   protected void restartServer() throws Exception
   {
      server.stop();
      server.start();
   }
   
   public void testSecuritySettings() throws Exception
   {
      HornetQServerControl serverControl = createManagementControl();
      String addressMatch = "test.#";
      String exactAddress = "test.whatever";

      assertEquals(0, serverControl.getRoles(addressMatch).length);
      serverControl.addSecuritySettings(addressMatch, "foo", "foo, bar", "foo", "bar", "foo, bar", "", "");

      // Restart the server. Those settings should be persisted

      restartServer();

      serverControl = createManagementControl();

      String rolesAsJSON = serverControl.getRolesAsJSON(exactAddress);
      RoleInfo[] roleInfos = RoleInfo.from(rolesAsJSON);
      assertEquals(2, roleInfos.length);
      RoleInfo fooRole = null;
      RoleInfo barRole = null;
      if (roleInfos[0].getName().equals("foo"))
      {
         fooRole = roleInfos[0];
         barRole = roleInfos[1];
      }
      else
      {
         fooRole = roleInfos[1];
         barRole = roleInfos[0];
      }
      assertTrue(fooRole.isSend());
      assertTrue(fooRole.isConsume());
      assertTrue(fooRole.isCreateDurableQueue());
      assertFalse(fooRole.isDeleteDurableQueue());
      assertTrue(fooRole.isCreateNonDurableQueue());
      assertFalse(fooRole.isDeleteNonDurableQueue());
      assertFalse(fooRole.isManage());

      assertFalse(barRole.isSend());
      assertTrue(barRole.isConsume());
      assertFalse(barRole.isCreateDurableQueue());
      assertTrue(barRole.isDeleteDurableQueue());
      assertTrue(barRole.isCreateNonDurableQueue());
      assertFalse(barRole.isDeleteNonDurableQueue());
      assertFalse(barRole.isManage());

      serverControl.removeSecuritySettings(addressMatch);
      assertEquals(0, serverControl.getRoles(exactAddress).length);
   }

   public void testAddressSettings() throws Exception
   {
      HornetQServerControl serverControl = createManagementControl();
      String addressMatch = "test.#";
      String exactAddress = "test.whatever";

      String DLA = "someDLA";
      String expiryAddress = "someExpiry";
      long expiryDelay = -1;
      boolean lastValueQueue = true;
      int deliveryAttempts = 1;
      long maxSizeBytes = 20;
      int pageSizeBytes = 10;
      int pageMaxCacheSize = 7;
      long redeliveryDelay = 4;
      double redeliveryMultiplier = 1;
      long maxRedeliveryDelay = 1000;
      long redistributionDelay = 5;
      boolean sendToDLAOnNoRoute = true;
      String addressFullMessagePolicy = "PAGE";

      serverControl.addAddressSettings(addressMatch,
                                       DLA,
                                       expiryAddress,
                                       expiryDelay,
                                       lastValueQueue,
                                       deliveryAttempts,
                                       maxSizeBytes,
                                       pageSizeBytes,
                                       pageMaxCacheSize,
                                       redeliveryDelay,
                                       redeliveryMultiplier,
                                       maxRedeliveryDelay,
                                       redistributionDelay,
                                       sendToDLAOnNoRoute,
                                       addressFullMessagePolicy);


      boolean ex = false;
      try
      {
         serverControl.addAddressSettings(addressMatch,
                                          DLA,
                                          expiryAddress,
                                          expiryDelay,
                                          lastValueQueue,
                                          deliveryAttempts,
                                          100,
                                          1000,
                                          pageMaxCacheSize,
                                          redeliveryDelay,
                                          redeliveryMultiplier,
                                          maxRedeliveryDelay,
                                          redistributionDelay,
                                          sendToDLAOnNoRoute,
                                          addressFullMessagePolicy);
      }
      catch (Exception expected)
      {
         ex=true;
      }
      
      assertTrue("Exception expected", ex);
       //restartServer();
      serverControl = createManagementControl();

      String jsonString = serverControl.getAddressSettingsAsJSON(exactAddress);
      AddressSettingsInfo info = AddressSettingsInfo.from(jsonString);
      
      assertEquals(DLA, info.getDeadLetterAddress());
      assertEquals(expiryAddress, info.getExpiryAddress());
      assertEquals(lastValueQueue, info.isLastValueQueue());
      assertEquals(deliveryAttempts, info.getMaxDeliveryAttempts());
      assertEquals(maxSizeBytes, info.getMaxSizeBytes());
      assertEquals(pageMaxCacheSize, info.getPageCacheMaxSize());
      assertEquals(pageSizeBytes, info.getPageSizeBytes());
      assertEquals(redeliveryDelay, info.getRedeliveryDelay());
      assertEquals(redeliveryMultiplier, info.getRedeliveryMultiplier());
      assertEquals(maxRedeliveryDelay, info.getMaxRedeliveryDelay());
      assertEquals(redistributionDelay, info.getRedistributionDelay());
      assertEquals(sendToDLAOnNoRoute, info.isSendToDLAOnNoRoute());
      assertEquals(addressFullMessagePolicy, info.getAddressFullMessagePolicy());
      
      serverControl.addAddressSettings(addressMatch,
                                       DLA,
                                       expiryAddress,
                                       expiryDelay,
                                       lastValueQueue,
                                       deliveryAttempts,
                                       -1,
                                       1000,
                                       pageMaxCacheSize,
                                       redeliveryDelay,
                                       redeliveryMultiplier,
                                       maxRedeliveryDelay,
                                       redistributionDelay,
                                       sendToDLAOnNoRoute,
                                       addressFullMessagePolicy);
      

      jsonString = serverControl.getAddressSettingsAsJSON(exactAddress);
      info = AddressSettingsInfo.from(jsonString);
      
      assertEquals(DLA, info.getDeadLetterAddress());
      assertEquals(expiryAddress, info.getExpiryAddress());
      assertEquals(lastValueQueue, info.isLastValueQueue());
      assertEquals(deliveryAttempts, info.getMaxDeliveryAttempts());
      assertEquals(-1, info.getMaxSizeBytes());
      assertEquals(pageMaxCacheSize, info.getPageCacheMaxSize());
      assertEquals(1000, info.getPageSizeBytes());
      assertEquals(redeliveryDelay, info.getRedeliveryDelay());
      assertEquals(redeliveryMultiplier, info.getRedeliveryMultiplier());
      assertEquals(maxRedeliveryDelay, info.getMaxRedeliveryDelay());
      assertEquals(redistributionDelay, info.getRedistributionDelay());
      assertEquals(sendToDLAOnNoRoute, info.isSendToDLAOnNoRoute());
      assertEquals(addressFullMessagePolicy, info.getAddressFullMessagePolicy());
      
      
      ex = false;
      try
      {
         serverControl.addAddressSettings(addressMatch,
                                          DLA,
                                          expiryAddress,
                                          expiryDelay,
                                          lastValueQueue,
                                          deliveryAttempts,
                                          -2,
                                          1000,
                                          pageMaxCacheSize,
                                          redeliveryDelay,
                                          redeliveryMultiplier,
                                          maxRedeliveryDelay,
                                          redistributionDelay,
                                          sendToDLAOnNoRoute,
                                          addressFullMessagePolicy);
      }
      catch (Exception e)
      {
         ex = true;
      }
      
      
      assertTrue("Supposed to have an exception called", ex);



   }

   public void testCreateAndDestroyDivert() throws Exception
   {
      String address = RandomUtil.randomString();
      String name = RandomUtil.randomString();
      String routingName = RandomUtil.randomString();
      String forwardingAddress = RandomUtil.randomString();

      HornetQServerControl serverControl = createManagementControl();

      checkNoResource(ObjectNameBuilder.DEFAULT.getDivertObjectName(name));
      assertEquals(0, serverControl.getDivertNames().length);
      
      serverControl.createDivert(name.toString(), routingName, address, forwardingAddress, true, null, null);
      
      checkResource(ObjectNameBuilder.DEFAULT.getDivertObjectName(name));
      DivertControl divertControl = ManagementControlHelper.createDivertControl(name.toString(), mbeanServer);
      assertEquals(name.toString(), divertControl.getUniqueName());
      assertEquals(address, divertControl.getAddress());
      assertEquals(forwardingAddress, divertControl.getForwardingAddress());
      assertEquals(routingName, divertControl.getRoutingName());
      assertTrue(divertControl.isExclusive());
      assertNull(divertControl.getFilter());
      assertNull(divertControl.getTransformerClassName());
      String[] divertNames = serverControl.getDivertNames();
      assertEquals(1, divertNames.length);
      assertEquals(name, divertNames[0]);
      
      // check that a message sent to the address is diverted exclusively
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));
      
      ClientSessionFactory csf = createSessionFactory(locator);
      ClientSession session = csf.createSession();

      String divertQueue = RandomUtil.randomString();
      String queue = RandomUtil.randomString();
      session.createQueue(forwardingAddress, divertQueue);
      session.createQueue(address, queue);

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createMessage(false);
      String text = RandomUtil.randomString();
      message.putStringProperty("prop", text);
      producer.send(message);
      
      ClientConsumer consumer = session.createConsumer(queue);
      ClientConsumer divertedConsumer = session.createConsumer(divertQueue);
      
      session.start();

      assertNull(consumer.receiveImmediate());
      message = divertedConsumer.receive(5000);
      assertNotNull(message);
      assertEquals(text, message.getStringProperty("prop"));

      serverControl.destroyDivert(name.toString());

      checkNoResource(ObjectNameBuilder.DEFAULT.getDivertObjectName(name));
      assertEquals(0, serverControl.getDivertNames().length);      

      // check that a message is no longer diverted
      message = session.createMessage(false);
      String text2 = RandomUtil.randomString();
      message.putStringProperty("prop", text2);
      producer.send(message);

      assertNull(divertedConsumer.receiveImmediate());
      message = consumer.receive(5000);
      assertNotNull(message);
      assertEquals(text2, message.getStringProperty("prop"));

      consumer.close();
      divertedConsumer.close();
      session.deleteQueue(queue);
      session.deleteQueue(divertQueue);
      session.close();
      
      locator.close();
      
   }

   public void testCreateAndDestroyBridge() throws Exception
   {
      String name = RandomUtil.randomString();
      String sourceAddress = RandomUtil.randomString();
      String sourceQueue = RandomUtil.randomString();
      String targetAddress = RandomUtil.randomString();
      String targetQueue = RandomUtil.randomString();

      HornetQServerControl serverControl = createManagementControl();

      checkNoResource(ObjectNameBuilder.DEFAULT.getBridgeObjectName(name));
      assertEquals(0, serverControl.getBridgeNames().length);

      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));
      ClientSessionFactory csf = createSessionFactory(locator);
      ClientSession session = csf.createSession();

      session.createQueue(sourceAddress, sourceQueue);
      session.createQueue(targetAddress, targetQueue);
      
      serverControl.createBridge(name,
                                 sourceQueue,
                                 targetAddress,
                                 null, // forwardingAddress
                                 null, // filterString
                                 HornetQClient.DEFAULT_RETRY_INTERVAL, 
                                 HornetQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                                 HornetQClient.DEFAULT_RECONNECT_ATTEMPTS,
                                  false, // duplicateDetection
                                 1, // confirmationWindowSize
                                 HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                 connectorConfig.getName(), // liveConnector
                                 false,
                                 false,
                                 null,
                                 null);

      checkResource(ObjectNameBuilder.DEFAULT.getBridgeObjectName(name));
      String[] bridgeNames = serverControl.getBridgeNames();
      assertEquals(1, bridgeNames.length);
      assertEquals(name, bridgeNames[0]);

      BridgeControl bridgeControl = ManagementControlHelper.createBridgeControl(name, mbeanServer);
      assertEquals(name, bridgeControl.getName());
      assertTrue(bridgeControl.isStarted());

      // check that a message sent to the sourceAddress is put in the tagetQueue
      ClientProducer producer = session.createProducer(sourceAddress);
      ClientMessage message = session.createMessage(false);
      String text = RandomUtil.randomString();
      message.putStringProperty("prop", text);
      producer.send(message);
      
      session.start();

      ClientConsumer targetConsumer = session.createConsumer(targetQueue);
      message = targetConsumer.receive(5000);
      assertNotNull(message);
      assertEquals(text, message.getStringProperty("prop"));

      ClientConsumer sourceConsumer = session.createConsumer(sourceQueue);
      assertNull(sourceConsumer.receiveImmediate());
      
      serverControl.destroyBridge(name);

      checkNoResource(ObjectNameBuilder.DEFAULT.getBridgeObjectName(name));
      assertEquals(0, serverControl.getBridgeNames().length);

      // check that a message is no longer diverted
      message = session.createMessage(false);
      String text2 = RandomUtil.randomString();
      message.putStringProperty("prop", text2);
      producer.send(message);

      assertNull(targetConsumer.receiveImmediate());
      message = sourceConsumer.receive(5000);
      assertNotNull(message);
      assertEquals(text2, message.getStringProperty("prop"));

      sourceConsumer.close();
      targetConsumer.close();
      
      session.deleteQueue(sourceQueue);
      session.deleteQueue(targetQueue);
      
      session.close();
      
      locator.close();
   }

   public void testListPreparedTransactionDetails() throws Exception
   {
      SimpleString atestq = new SimpleString("BasicXaTestq");
      Xid xid = newXID();
      
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));
      ClientSessionFactory csf = createSessionFactory(locator);
      ClientSession clientSession = csf.createSession(true, false, false);
      clientSession.createQueue(atestq, atestq, null, true);

      ClientMessage m1 = createTextMessage(clientSession, "");
      ClientMessage m2 = createTextMessage(clientSession, "");
      ClientMessage m3 = createTextMessage(clientSession, "");
      ClientMessage m4 = createTextMessage(clientSession, "");
      m1.putStringProperty("m1", "m1");
      m2.putStringProperty("m2", "m2");
      m3.putStringProperty("m3", "m3");
      m4.putStringProperty("m4", "m4");
      ClientProducer clientProducer = clientSession.createProducer(atestq);
      clientSession.start(xid, XAResource.TMNOFLAGS);
      clientProducer.send(m1);
      clientProducer.send(m2);
      clientProducer.send(m3);
      clientProducer.send(m4);
      clientSession.end(xid, XAResource.TMSUCCESS);
      clientSession.prepare(xid);

      HornetQServerControl serverControl = createManagementControl();
      
      JSONArray jsonArray = new JSONArray(serverControl.listProducersInfoAsJSON());
      
      assertEquals(1, jsonArray.length());
      assertEquals(4, ((JSONObject)jsonArray.get(0)).getInt("msgSent"));

      clientSession.close();
      locator.close();

      String txDetails = serverControl.listPreparedTransactionDetailsAsJSON();

      Assert.assertTrue(txDetails.matches(".*m1.*"));
      Assert.assertTrue(txDetails.matches(".*m2.*"));
      Assert.assertTrue(txDetails.matches(".*m3.*"));
      Assert.assertTrue(txDetails.matches(".*m4.*"));
   }
   
   public void testListPreparedTransactionDetailsAsHTML() throws Exception
   {
      SimpleString atestq = new SimpleString("BasicXaTestq");
      Xid xid = newXID();
      
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));
      ClientSessionFactory csf = createSessionFactory(locator);
      ClientSession clientSession = csf.createSession(true, false, false);
      clientSession.createQueue(atestq, atestq, null, true);

      ClientMessage m1 = createTextMessage(clientSession, "");
      ClientMessage m2 = createTextMessage(clientSession, "");
      ClientMessage m3 = createTextMessage(clientSession, "");
      ClientMessage m4 = createTextMessage(clientSession, "");
      m1.putStringProperty("m1", "m1");
      m2.putStringProperty("m2", "m2");
      m3.putStringProperty("m3", "m3");
      m4.putStringProperty("m4", "m4");
      ClientProducer clientProducer = clientSession.createProducer(atestq);
      clientSession.start(xid, XAResource.TMNOFLAGS);
      clientProducer.send(m1);
      clientProducer.send(m2);
      clientProducer.send(m3);
      clientProducer.send(m4);
      clientSession.end(xid, XAResource.TMSUCCESS);
      clientSession.prepare(xid);

      clientSession.close();
      locator.close();

      HornetQServerControl serverControl = createManagementControl();
      String html = serverControl.listPreparedTransactionDetailsAsHTML();

      Assert.assertTrue(html.matches(".*m1.*"));
      Assert.assertTrue(html.matches(".*m2.*"));
      Assert.assertTrue(html.matches(".*m3.*"));
      Assert.assertTrue(html.matches(".*m4.*"));
   }

   public void testCommitPreparedTransactions() throws Exception
   {
      SimpleString recQueue = new SimpleString("BasicXaTestqRec");
      SimpleString sendQueue = new SimpleString("BasicXaTestqSend");

      byte[] globalTransactionId = UUIDGenerator.getInstance().generateStringUUID().getBytes();
      Xid xid = new XidImpl("xa1".getBytes(), 1, globalTransactionId);
      Xid xid2 = new XidImpl("xa2".getBytes(), 1, globalTransactionId);
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));
      ClientSessionFactory csf = createSessionFactory(locator);
      ClientSession clientSession = csf.createSession(true, false, false);
      clientSession.createQueue(recQueue, recQueue, null, true);
      clientSession.createQueue(sendQueue, sendQueue, null, true);
      ClientMessage m1 = createTextMessage(clientSession, "");
      m1.putStringProperty("m1", "m1");
      ClientProducer clientProducer = clientSession.createProducer(recQueue);
      clientProducer.send(m1);
      locator.close();


      ServerLocator receiveLocator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));
      ClientSessionFactory receiveCsf = createSessionFactory(receiveLocator);
      ClientSession receiveClientSession = receiveCsf.createSession(true, false, false);
      ClientConsumer consumer = receiveClientSession.createConsumer(recQueue);


      ServerLocator sendLocator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));
      ClientSessionFactory sendCsf = createSessionFactory(sendLocator);
      ClientSession sendClientSession = sendCsf.createSession(true, false, false);
      ClientProducer producer = sendClientSession.createProducer(sendQueue);

      receiveClientSession.start(xid, XAResource.TMNOFLAGS);
      receiveClientSession.start();
      sendClientSession.start(xid2, XAResource.TMNOFLAGS);

      ClientMessage m = consumer.receive(5000);
      assertNotNull(m);

      producer.send(m);


      receiveClientSession.end(xid, XAResource.TMSUCCESS);
      sendClientSession.end(xid2, XAResource.TMSUCCESS);

      receiveClientSession.prepare(xid);
      sendClientSession.prepare(xid2);

      HornetQServerControl serverControl = createManagementControl();

      sendLocator.close();
      receiveLocator.close();

      boolean success = serverControl.commitPreparedTransaction(XidImpl.toBase64String(xid));

      success = serverControl.commitPreparedTransaction(XidImpl.toBase64String(xid));

      System.out.println("HornetQServerControlTest.testCommitPreparedTransactions");
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      Map<String, Object> params = new HashMap<String, Object>();
      //params.put(RandomUtil.randomString(), RandomUtil.randomBoolean());
      connectorConfig = new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                   params,
                                                   RandomUtil.randomString());

      conf = createDefaultConfig(false);
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.getAcceptorConfigurations().clear();
      conf.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      server = HornetQServers.newHornetQServer(conf, mbeanServer, true);
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

