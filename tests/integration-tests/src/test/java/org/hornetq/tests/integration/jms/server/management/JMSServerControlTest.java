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

package org.hornetq.tests.integration.jms.server.management;
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XASession;
import javax.naming.NamingException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.junit.Assert;

import org.hornetq.api.config.HornetQDefaultConfiguration;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.management.AddressControl;
import org.hornetq.api.core.management.ObjectNameBuilder;
import org.hornetq.api.core.management.ResourceNames;
import org.hornetq.api.jms.management.JMSServerControl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.postoffice.QueueBinding;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.client.HornetQDestination;
import org.hornetq.jms.client.HornetQQueueConnectionFactory;
import org.hornetq.jms.persistence.JMSStorageManager;
import org.hornetq.jms.persistence.config.PersistedConnectionFactory;
import org.hornetq.jms.persistence.config.PersistedDestination;
import org.hornetq.jms.persistence.config.PersistedJNDI;
import org.hornetq.jms.persistence.config.PersistedType;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.tests.integration.management.ManagementControlHelper;
import org.hornetq.tests.integration.management.ManagementTestBase;
import org.hornetq.tests.unit.util.InVMContext;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.json.JSONArray;

/**
 * A JMSServerControlTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * Created 14 nov. 2008 13:35:10
 *
 *
 */
public class JMSServerControlTest extends ManagementTestBase
{
   // Attributes ----------------------------------------------------

   protected InVMContext context;

   private HornetQServer server;

   private JMSServerManagerImpl serverManager;

   private FakeJMSStorageManager fakeJMSStorageManager;

   // Static --------------------------------------------------------

   private static String toCSV(final Object[] objects)
   {
      StringBuilder str = new StringBuilder();
      for (int i = 0; i < objects.length; i++)
      {
         if (i > 0)
         {
            str.append(", ");
         }
         str.append(objects[i]);
      }
      return str.toString();
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   /** Number of consumers used by the test itself */
   protected int getNumberOfConsumers()
   {
      return 0;
   }

   @Test
   public void testGetVersion() throws Exception
   {
      JMSServerControl control = createManagementControl();
      String version = control.getVersion();
      Assert.assertEquals(serverManager.getVersion(), version);
   }

   @Test
   public void testCreateQueueWithBindings() throws Exception
   {
      String[] bindings = new String[3];
      bindings[0] = RandomUtil.randomString();
      bindings[1] = RandomUtil.randomString();
      bindings[2] = RandomUtil.randomString();
      String queueName = RandomUtil.randomString();

      String bindingsCSV = JMSServerControlTest.toCSV(bindings);
      UnitTestCase.checkNoBinding(context, bindingsCSV);

      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      JMSServerControl control = createManagementControl();
      control.createQueue(queueName, bindingsCSV);

      Object o = UnitTestCase.checkBinding(context, bindings[0]);
      Assert.assertTrue(o instanceof Queue);
      Queue queue = (Queue)o;
      Assert.assertEquals(queueName, queue.getQueueName());
      o = UnitTestCase.checkBinding(context, bindings[1]);
      Assert.assertTrue(o instanceof Queue);
      queue = (Queue)o;
      Assert.assertEquals(queueName, queue.getQueueName());
      o = UnitTestCase.checkBinding(context, bindings[2]);
      Assert.assertTrue(o instanceof Queue);
      queue = (Queue)o;
      Assert.assertEquals(queueName, queue.getQueueName());
      checkResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      Assert.assertNotNull(fakeJMSStorageManager.destinationMap.get(queueName));
      Assert.assertNotNull(fakeJMSStorageManager.persistedJNDIMap.get(queueName));
      Assert.assertTrue(fakeJMSStorageManager.persistedJNDIMap.get(queueName).contains(bindings[0]));
      Assert.assertTrue(fakeJMSStorageManager.persistedJNDIMap.get(queueName).contains(bindings[1]));
      Assert.assertTrue(fakeJMSStorageManager.persistedJNDIMap.get(queueName).contains(bindings[2]));
   }

   @Test
   public void testCreateQueueWithCommaBindings() throws Exception
   {
      String[] bindings = new String[3];
      bindings[0] = "first&comma;first";
      bindings[1] = "second&comma;second";
      bindings[2] = "third&comma;third";
      String queueName = RandomUtil.randomString();

      String bindingsCSV = JMSServerControlTest.toCSV(bindings);
      UnitTestCase.checkNoBinding(context, bindingsCSV);

      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      JMSServerControl control = createManagementControl();
      control.createQueue(queueName, bindingsCSV);

      Object o = UnitTestCase.checkBinding(context, "first,first");
      Assert.assertTrue(o instanceof Queue);
      Queue queue = (Queue)o;
      Assert.assertEquals(queueName, queue.getQueueName());
      o = UnitTestCase.checkBinding(context, "second,second");
      Assert.assertTrue(o instanceof Queue);
      queue = (Queue)o;
      Assert.assertEquals(queueName, queue.getQueueName());
      o = UnitTestCase.checkBinding(context, "third,third");
      Assert.assertTrue(o instanceof Queue);
      queue = (Queue)o;
      Assert.assertEquals(queueName, queue.getQueueName());
      checkResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      Assert.assertNotNull(fakeJMSStorageManager.destinationMap.get(queueName));
      Assert.assertNotNull(fakeJMSStorageManager.persistedJNDIMap.get(queueName));
      Assert.assertTrue(fakeJMSStorageManager.persistedJNDIMap.get(queueName).contains("first,first"));
      Assert.assertTrue(fakeJMSStorageManager.persistedJNDIMap.get(queueName).contains("second,second"));
      Assert.assertTrue(fakeJMSStorageManager.persistedJNDIMap.get(queueName).contains("third,third"));
   }

   @Test
   public void testCreateQueueWithSelector() throws Exception
   {
      String[] bindings = new String[3];
      bindings[0] = RandomUtil.randomString();
      bindings[1] = RandomUtil.randomString();
      bindings[2] = RandomUtil.randomString();
      String queueName = RandomUtil.randomString();

      String bindingsCSV = JMSServerControlTest.toCSV(bindings);
      UnitTestCase.checkNoBinding(context, bindingsCSV);

      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      JMSServerControl control = createManagementControl();
      String selector = "foo='bar'";
      control.createQueue(queueName, bindingsCSV, selector);

      Object o = UnitTestCase.checkBinding(context, bindings[0]);
      Assert.assertTrue(o instanceof Queue);
      Queue queue = (Queue)o;
      // assertEquals(((HornetQDestination)queue).get);
      Assert.assertEquals(queueName, queue.getQueueName());
      Assert.assertEquals(selector, server.getPostOffice()
                                          .getBinding(new SimpleString("jms.queue." + queueName))
                                          .getFilter()
                                          .getFilterString()
                                          .toString());
      o = UnitTestCase.checkBinding(context, bindings[1]);
      Assert.assertTrue(o instanceof Queue);
      queue = (Queue)o;
      Assert.assertEquals(queueName, queue.getQueueName());
      Assert.assertEquals(selector, server.getPostOffice()
                                          .getBinding(new SimpleString("jms.queue." + queueName))
                                          .getFilter()
                                          .getFilterString()
                                          .toString());
      o = UnitTestCase.checkBinding(context, bindings[2]);
      Assert.assertTrue(o instanceof Queue);
      queue = (Queue)o;
      Assert.assertEquals(queueName, queue.getQueueName());
      Assert.assertEquals(selector, server.getPostOffice()
                                          .getBinding(new SimpleString("jms.queue." + queueName))
                                          .getFilter()
                                          .getFilterString()
                                          .toString());
      checkResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      Assert.assertNotNull(fakeJMSStorageManager.destinationMap.get(queueName));
      Assert.assertNotNull(fakeJMSStorageManager.persistedJNDIMap.get(queueName));
      Assert.assertTrue(fakeJMSStorageManager.persistedJNDIMap.get(queueName).contains(bindings[0]));
      Assert.assertTrue(fakeJMSStorageManager.persistedJNDIMap.get(queueName).contains(bindings[1]));
      Assert.assertTrue(fakeJMSStorageManager.persistedJNDIMap.get(queueName).contains(bindings[2]));
   }

   @Test
   public void testCreateNonDurableQueue() throws Exception
   {
      String queueName = RandomUtil.randomString();
      String binding = RandomUtil.randomString();

      UnitTestCase.checkNoBinding(context, binding);
      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      JMSServerControl control = createManagementControl();
      control.createQueue(queueName, binding, null, false);

      Object o = UnitTestCase.checkBinding(context, binding);
      Assert.assertTrue(o instanceof Queue);
      Queue queue = (Queue)o;
      Assert.assertEquals(queueName, queue.getQueueName());
      QueueBinding queueBinding = (QueueBinding)server.getPostOffice()
                                                      .getBinding(new SimpleString("jms.queue." + queueName));
      assertFalse(queueBinding.getQueue().isDurable());
      checkResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      // queue is not durable => not stored
      Assert.assertNull(fakeJMSStorageManager.destinationMap.get(queueName));
      Assert.assertNull(fakeJMSStorageManager.persistedJNDIMap.get(queueName));
   }

   @Test
   public void testDestroyQueue() throws Exception
   {
      String queueJNDIBinding = RandomUtil.randomString();
      String queueName = RandomUtil.randomString();

      UnitTestCase.checkNoBinding(context, queueJNDIBinding);
      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      JMSServerControl control = createManagementControl();
      control.createQueue(queueName, queueJNDIBinding);

      UnitTestCase.checkBinding(context, queueJNDIBinding);
      checkResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      control.destroyQueue(queueName);

      UnitTestCase.checkNoBinding(context, queueJNDIBinding);
      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      Assert.assertNull(fakeJMSStorageManager.destinationMap.get(queueName));
   }

   @Test
   public void testGetQueueNames() throws Exception
   {
      String queueJNDIBinding = RandomUtil.randomString();
      String queueName = RandomUtil.randomString();

      JMSServerControl control = createManagementControl();
      Assert.assertEquals(0, control.getQueueNames().length);

      control.createQueue(queueName, queueJNDIBinding);

      String[] names = control.getQueueNames();
      Assert.assertEquals(1, names.length);
      Assert.assertEquals(queueName, names[0]);

      control.destroyQueue(queueName);

      Assert.assertEquals(0, control.getQueueNames().length);
   }

   @Test
   public void testCreateTopic() throws Exception
   {
      String[] bindings = new String[3];
      bindings[0] = RandomUtil.randomString();
      bindings[1] = RandomUtil.randomString();
      bindings[2] = RandomUtil.randomString();
      String topicJNDIBinding = JMSServerControlTest.toCSV(bindings);
      UnitTestCase.checkNoBinding(context, topicJNDIBinding);
      String topicName = RandomUtil.randomString();

      UnitTestCase.checkNoBinding(context, topicJNDIBinding);
      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSTopicObjectName(topicName));

      JMSServerControl control = createManagementControl();
      control.createTopic(topicName, topicJNDIBinding);

      Object o = UnitTestCase.checkBinding(context, bindings[0]);
      Assert.assertTrue(o instanceof Topic);
      Topic topic = (Topic)o;
      Assert.assertEquals(topicName, topic.getTopicName());
      o = UnitTestCase.checkBinding(context, bindings[1]);
      Assert.assertTrue(o instanceof Topic);
      topic = (Topic)o;
      Assert.assertEquals(topicName, topic.getTopicName());
      o = UnitTestCase.checkBinding(context, bindings[2]);
      Assert.assertTrue(o instanceof Topic);
      topic = (Topic)o;
      Assert.assertEquals(topicName, topic.getTopicName());
      checkResource(ObjectNameBuilder.DEFAULT.getJMSTopicObjectName(topicName));

      Assert.assertNotNull(fakeJMSStorageManager.destinationMap.get(topicName));
      Assert.assertNotNull(fakeJMSStorageManager.persistedJNDIMap.get(topicName));
      Assert.assertTrue(fakeJMSStorageManager.persistedJNDIMap.get(topicName).contains(bindings[0]));
      Assert.assertTrue(fakeJMSStorageManager.persistedJNDIMap.get(topicName).contains(bindings[1]));
      Assert.assertTrue(fakeJMSStorageManager.persistedJNDIMap.get(topicName).contains(bindings[2]));
   }

   @Test
   public void testDestroyTopic() throws Exception
   {
      String topicJNDIBinding = RandomUtil.randomString();
      String topicName = RandomUtil.randomString();

      UnitTestCase.checkNoBinding(context, topicJNDIBinding);
      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSTopicObjectName(topicName));

      JMSServerControl control = createManagementControl();
      control.createTopic(topicName, topicJNDIBinding);

      checkResource(ObjectNameBuilder.DEFAULT.getJMSTopicObjectName(topicName));
      Topic topic = (Topic)context.lookup(topicJNDIBinding);
      assertNotNull(topic);
      HornetQConnectionFactory cf = new HornetQConnectionFactory(false,
                                                                 new TransportConfiguration(InVMConnectorFactory.class.getName()));
      Connection connection = cf.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      // create a consumer will create a Core queue bound to the topic address
      session.createConsumer(topic);

      String topicAddress = HornetQDestination.createTopicAddressFromName(topicName).toString();
      AddressControl addressControl = (AddressControl)server.getManagementService()
                                                            .getResource(ResourceNames.CORE_ADDRESS + topicAddress);
      assertNotNull(addressControl);

      assertTrue(addressControl.getQueueNames().length > 0);

      connection.close();
      control.destroyTopic(topicName);

      assertNull(server.getManagementService().getResource(ResourceNames.CORE_ADDRESS + topicAddress));
      UnitTestCase.checkNoBinding(context, topicJNDIBinding);
      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSTopicObjectName(topicName));

      Assert.assertNull(fakeJMSStorageManager.destinationMap.get(topicName));
   }

   @Test
   public void testListAllConsumers() throws Exception
   {
      String topicJNDIBinding = RandomUtil.randomString();
      String topicName = RandomUtil.randomString();

      UnitTestCase.checkNoBinding(context, topicJNDIBinding);
      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSTopicObjectName(topicName));

      JMSServerControl control = createManagementControl();
      control.createTopic(topicName, topicJNDIBinding);

      checkResource(ObjectNameBuilder.DEFAULT.getJMSTopicObjectName(topicName));
      Topic topic = (Topic)context.lookup(topicJNDIBinding);
      assertNotNull(topic);
      HornetQConnectionFactory cf =
               new HornetQConnectionFactory(false, new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      Connection connection = cf.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      // create a consumer will create a Core queue bound to the topic address
      MessageConsumer cons = session.createConsumer(topic);

      JSONArray jsonArray = new JSONArray(control.listAllConsumersAsJSON());

      assertEquals(1 + getNumberOfConsumers(), jsonArray.length());

      cons.close();

      jsonArray = new JSONArray(control.listAllConsumersAsJSON());

      assertEquals(getNumberOfConsumers(), jsonArray.length());

      String topicAddress = HornetQDestination.createTopicAddressFromName(topicName).toString();
      AddressControl addressControl = (AddressControl)server.getManagementService()
                                                            .getResource(ResourceNames.CORE_ADDRESS + topicAddress);
      assertNotNull(addressControl);

      assertTrue(addressControl.getQueueNames().length > 0);

      connection.close();
      control.destroyTopic(topicName);

      assertNull(server.getManagementService().getResource(ResourceNames.CORE_ADDRESS + topicAddress));
      UnitTestCase.checkNoBinding(context, topicJNDIBinding);
      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSTopicObjectName(topicName));

      Assert.assertNull(fakeJMSStorageManager.destinationMap.get(topicName));
   }

   @Test
   public void testGetTopicNames() throws Exception
   {
      String topicJNDIBinding = RandomUtil.randomString();
      String topicName = RandomUtil.randomString();

      JMSServerControl control = createManagementControl();
      Assert.assertEquals(0, control.getTopicNames().length);

      control.createTopic(topicName, topicJNDIBinding);

      String[] names = control.getTopicNames();
      Assert.assertEquals(1, names.length);
      Assert.assertEquals(topicName, names[0]);

      control.destroyTopic(topicName);

      Assert.assertEquals(0, control.getTopicNames().length);
   }

   @Test
   public void testCreateConnectionFactory_3b() throws Exception
   {
      server.getConfiguration()
            .getConnectorConfigurations()
            .put("tst", new TransportConfiguration(INVM_CONNECTOR_FACTORY));

      doCreateConnectionFactory(new ConnectionFactoryCreator()
      {
         public void createConnectionFactory(final JMSServerControl control,
                                             final String cfName,
                                             final Object[] bindings) throws Exception
         {
            String jndiBindings = JMSServerControlTest.toCSV(bindings);

            control.createConnectionFactory(cfName, false, false, 0, "tst", jndiBindings);
         }
      });
   }

   @Test
   public void testCreateConnectionFactory_CompleteList() throws Exception
   {
      JMSServerControl control = createManagementControl();
      control.createConnectionFactory("test", //name
                                      true, // ha
                                      false, // useDiscovery
                                      1, // cfType
                                      "invm", // connectorNames
                                      "tst", // jndiBindins
                                      "tst", // clientID
                                      1, // clientFailureCheckPeriod
                                      1,  // connectionTTL
                                      1, // callTimeout
                                      1, //callFailoverTimeout
                                      1, // minLargeMessageSize
                                      true, // compressLargeMessages
                                      1, // consumerWindowSize
                                      1, // consumerMaxRate
                                      1, // confirmationWindowSize
                                      1, // ProducerWindowSize
                                      1, // producerMaxRate
                                      true, // blockOnACK
                                      true, // blockOnDurableSend
                                      true, // blockOnNonDurableSend
                                      true, // autoGroup
                                      true, // preACK
                                      HornetQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME, // loadBalancingClassName
                                      1, // transactionBatchSize
                                      1, // dupsOKBatchSize
                                      true, // useGlobalPools
                                      1, // scheduleThreadPoolSize
                                      1, // threadPoolMaxSize
                                      1, // retryInterval
                                      1, // retryIntervalMultiplier
                                      1, // maxRetryInterval
                                      1, // reconnectAttempts
                                      true, // failoverOnInitialConnection
                                      "tst"); // groupID


      HornetQQueueConnectionFactory cf = (HornetQQueueConnectionFactory)context.lookup("tst");

      assertEquals(true, cf.isHA());
      assertEquals("tst", cf.getClientID());
      assertEquals(1, cf.getClientFailureCheckPeriod());
      assertEquals(1, cf.getConnectionTTL());
      assertEquals(1, cf.getCallTimeout());
      assertEquals(1, cf.getCallFailoverTimeout());
      assertEquals(1, cf.getMinLargeMessageSize());
      assertEquals(true, cf.isCompressLargeMessage());
      assertEquals(1, cf.getConsumerWindowSize());
      assertEquals(1, cf.getConfirmationWindowSize());
      assertEquals(1, cf.getProducerWindowSize());
      assertEquals(1, cf.getProducerMaxRate());
      assertEquals(true, cf.isBlockOnAcknowledge());
      assertEquals(true, cf.isBlockOnDurableSend());
      assertEquals(true, cf.isBlockOnNonDurableSend());
      assertEquals(true, cf.isAutoGroup());
      assertEquals(true, cf.isPreAcknowledge());
      assertEquals(HornetQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME, cf.getConnectionLoadBalancingPolicyClassName());
      assertEquals(1, cf.getTransactionBatchSize());
      assertEquals(1, cf.getDupsOKBatchSize());
      assertEquals(true, cf.isUseGlobalPools());
      assertEquals(1, cf.getScheduledThreadPoolMaxSize());
      assertEquals(1, cf.getThreadPoolMaxSize());
      assertEquals(1, cf.getRetryInterval());
      assertEquals(1.0, cf.getRetryIntervalMultiplier(), 0.000001);
      assertEquals(1, cf.getMaxRetryInterval());
      assertEquals(1, cf.getReconnectAttempts());
      assertEquals(true, cf.isFailoverOnInitialConnection());
      assertEquals("tst", cf.getGroupID());

      stopServer();

      startServer();

      control = createManagementControl();

      cf = (HornetQQueueConnectionFactory)context.lookup("tst");

      assertEquals(true, cf.isHA());
      assertEquals("tst", cf.getClientID());
      assertEquals(1, cf.getClientFailureCheckPeriod());
      assertEquals(1, cf.getConnectionTTL());
      assertEquals(1, cf.getCallTimeout());
      assertEquals(1, cf.getMinLargeMessageSize());
      assertEquals(true, cf.isCompressLargeMessage());
      assertEquals(1, cf.getConsumerWindowSize());
      assertEquals(1, cf.getConfirmationWindowSize());
      assertEquals(1, cf.getProducerWindowSize());
      assertEquals(1, cf.getProducerMaxRate());
      assertEquals(true, cf.isBlockOnAcknowledge());
      assertEquals(true, cf.isBlockOnDurableSend());
      assertEquals(true, cf.isBlockOnNonDurableSend());
      assertEquals(true, cf.isAutoGroup());
      assertEquals(true, cf.isPreAcknowledge());
      assertEquals(HornetQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME, cf.getConnectionLoadBalancingPolicyClassName());
      assertEquals(1, cf.getTransactionBatchSize());
      assertEquals(1, cf.getDupsOKBatchSize());
      assertEquals(true, cf.isUseGlobalPools());
      assertEquals(1, cf.getScheduledThreadPoolMaxSize());
      assertEquals(1, cf.getThreadPoolMaxSize());
      assertEquals(1, cf.getRetryInterval());
      assertEquals(1.0, cf.getRetryIntervalMultiplier(), 0.000001);
      assertEquals(1, cf.getMaxRetryInterval());
      assertEquals(1, cf.getReconnectAttempts());
      assertEquals(true, cf.isFailoverOnInitialConnection());
      assertEquals("tst", cf.getGroupID());

      control.destroyConnectionFactory("test");

      ObjectNameBuilder nameBuilder = ObjectNameBuilder.create(HornetQDefaultConfiguration.getDefaultJmxDomain());
      assertFalse(mbeanServer.isRegistered(nameBuilder.getConnectionFactoryObjectName("test")));

      stopServer();

      startServer();

      assertFalse(mbeanServer.isRegistered(nameBuilder.getConnectionFactoryObjectName("test")));


      try
      {
         cf = (HornetQQueueConnectionFactory)context.lookup("tst");
         fail("Failure expected");
      }
      catch (NamingException e)
      {
      }


   }

   @Test
   public void testListPreparedTransactionDetails() throws Exception
   {
      Xid xid = newXID();

      JMSServerControl control = createManagementControl();
      String cfJNDIBinding = "/cf";
      String cfName = "cf";

      server.getConfiguration()
            .getConnectorConfigurations()
            .put("tst", new TransportConfiguration(INVM_CONNECTOR_FACTORY));

      control.createConnectionFactory(cfName, false, false, 3, "tst", cfJNDIBinding);

      control.createQueue("q", "/q");

      XAConnectionFactory cf = (XAConnectionFactory)context.lookup("/cf");
      Destination dest = (Destination)context.lookup("/q");
      XAConnection conn = cf.createXAConnection();
      XASession ss = conn.createXASession();
      TextMessage m1 = ss.createTextMessage("m1");
      TextMessage m2 = ss.createTextMessage("m2");
      TextMessage m3 = ss.createTextMessage("m3");
      TextMessage m4 = ss.createTextMessage("m4");
      MessageProducer mp = ss.createProducer(dest);
      XAResource xa = ss.getXAResource();
      xa.start(xid, XAResource.TMNOFLAGS);
      mp.send(m1);
      mp.send(m2);
      mp.send(m3);
      mp.send(m4);
      xa.end(xid, XAResource.TMSUCCESS);
      xa.prepare(xid);

      ss.close();

      control.listPreparedTransactionDetailsAsJSON();
   }

   @Test
   public void testListPreparedTranscationDetailsAsHTML() throws Exception
   {
      Xid xid = newXID();

      JMSServerControl control = createManagementControl();
      TransportConfiguration tc = new TransportConfiguration(InVMConnectorFactory.class.getName());
      String cfJNDIBinding = "/cf";
      String cfName = "cf";

      server.getConfiguration()
            .getConnectorConfigurations()
            .put("tst", new TransportConfiguration(INVM_CONNECTOR_FACTORY));

      control.createConnectionFactory(cfName, false, false, 3, "tst", cfJNDIBinding);

      control.createQueue("q", "/q");

      XAConnectionFactory cf = (XAConnectionFactory)context.lookup("/cf");
      Destination dest = (Destination)context.lookup("/q");
      XAConnection conn = cf.createXAConnection();
      XASession ss = conn.createXASession();
      TextMessage m1 = ss.createTextMessage("m1");
      TextMessage m2 = ss.createTextMessage("m2");
      TextMessage m3 = ss.createTextMessage("m3");
      TextMessage m4 = ss.createTextMessage("m4");
      MessageProducer mp = ss.createProducer(dest);
      XAResource xa = ss.getXAResource();
      xa.start(xid, XAResource.TMNOFLAGS);
      mp.send(m1);
      mp.send(m2);
      mp.send(m3);
      mp.send(m4);
      xa.end(xid, XAResource.TMSUCCESS);
      xa.prepare(xid);

      ss.close();

      control.listPreparedTransactionDetailsAsHTML();
   }


   @Test
   public void testRemoteClientIDConnection() throws Exception
   {
      JMSServerControl control = createManagementControl();

      HornetQConnectionFactory cf = new HornetQConnectionFactory(false,
         new TransportConfiguration(InVMConnectorFactory.class.getName()));
      Connection connection = cf.createConnection();

      connection.setClientID("someID");

      Connection connection2 = cf.createConnection();
      boolean failed = false;

      try
      {
         connection2.setClientID("someID");
      }
      catch (JMSException e)
      {
         failed = true;
      }

      assertTrue(failed);

      System.out.println(control.closeConnectionWithClientID("someID"));

      connection2.setClientID("someID");


      failed = false;
      Connection connection3 = cf.createConnection();

      try
      {
         connection3.setClientID("someID");
      }
      catch (JMSException e)
      {
         failed = true;
      }

      assertTrue(failed);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      startServer();
   }

   /**
    * @throws Exception
    */
   protected void startServer() throws Exception
   {
      Configuration conf = createBasicConfig();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.setPersistenceEnabled(true);

      conf.getAcceptorConfigurations().add(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY));
      conf.getAcceptorConfigurations().add(new TransportConfiguration(INVM_ACCEPTOR_FACTORY));
      conf.getConnectorConfigurations().put("netty", new TransportConfiguration(NETTY_CONNECTOR_FACTORY));
      conf.getConnectorConfigurations().put("invm", new TransportConfiguration(INVM_CONNECTOR_FACTORY));

      server = addServer(HornetQServers.newHornetQServer(conf, mbeanServer, true));

      serverManager = new JMSServerManagerImpl(server);
      context = new InVMContext();
      serverManager.setContext(context);
      serverManager.start();
      serverManager.activated();

      this.fakeJMSStorageManager = new FakeJMSStorageManager(serverManager.getJMSStorageManager());

      serverManager.replaceStorageManager(fakeJMSStorageManager);
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      try
      {
         stopServer();
      }
      finally
      {
         super.tearDown();
      }
   }

   /**
    * @throws Exception
    */
   protected void stopServer() throws Exception
   {
      serverManager.stop();

      server.stop();

      serverManager = null;

      server = null;
   }

   protected JMSServerControl createManagementControl() throws Exception
   {
      return ManagementControlHelper.createJMSServerControl(mbeanServer);
   }

   // Private -------------------------------------------------------

   private void doCreateConnectionFactory(final ConnectionFactoryCreator creator) throws Exception
   {
      Object[] cfJNDIBindings = new Object[] { RandomUtil.randomString(),
                                              RandomUtil.randomString(),
                                              RandomUtil.randomString() };

      String cfName = RandomUtil.randomString();

      for (Object cfJNDIBinding : cfJNDIBindings)
      {
         UnitTestCase.checkNoBinding(context, cfJNDIBinding.toString());
      }
      checkNoResource(ObjectNameBuilder.DEFAULT.getConnectionFactoryObjectName(cfName));

      JMSServerControl control = createManagementControl();
      creator.createConnectionFactory(control, cfName, cfJNDIBindings);

      for (Object cfJNDIBinding : cfJNDIBindings)
      {
         Object o = UnitTestCase.checkBinding(context, cfJNDIBinding.toString());
         Assert.assertTrue(o instanceof ConnectionFactory);
         ConnectionFactory cf = (ConnectionFactory)o;
         Connection connection = cf.createConnection();
         connection.close();
      }
      checkResource(ObjectNameBuilder.DEFAULT.getConnectionFactoryObjectName(cfName));

      Assert.assertNotNull(fakeJMSStorageManager.connectionFactoryMap.get(cfName));
      Assert.assertTrue(fakeJMSStorageManager.persistedJNDIMap.get(cfName).contains(cfJNDIBindings[0]));
      Assert.assertTrue(fakeJMSStorageManager.persistedJNDIMap.get(cfName).contains(cfJNDIBindings[1]));
      Assert.assertTrue(fakeJMSStorageManager.persistedJNDIMap.get(cfName).contains(cfJNDIBindings[2]));
   }

   // Inner classes -------------------------------------------------

   interface ConnectionFactoryCreator
   {
      void createConnectionFactory(JMSServerControl control, String cfName, Object[] bindings) throws Exception;
   }

   class FakeJMSStorageManager implements JMSStorageManager
   {
      Map<String, PersistedDestination> destinationMap = new HashMap<String, PersistedDestination>();

      Map<String, PersistedConnectionFactory> connectionFactoryMap = new HashMap<String, PersistedConnectionFactory>();

      ConcurrentHashMap<String, List<String>> persistedJNDIMap = new ConcurrentHashMap<String, List<String>>();

      JMSStorageManager delegate;

      public FakeJMSStorageManager(JMSStorageManager delegate)
      {
         this.delegate = delegate;
      }

      public void storeDestination(PersistedDestination destination) throws Exception
      {
         destinationMap.put(destination.getName(), destination);
         delegate.storeDestination(destination);
      }

      public void deleteDestination(PersistedType type, String name) throws Exception
      {
         destinationMap.remove(name);
         delegate.deleteDestination(type, name);
      }

      public List<PersistedDestination> recoverDestinations()
      {
         return delegate.recoverDestinations();
      }

      public void deleteConnectionFactory(String connectionFactory) throws Exception
      {
         connectionFactoryMap.remove(connectionFactory);
         delegate.deleteConnectionFactory(connectionFactory);
      }

      public void storeConnectionFactory(PersistedConnectionFactory connectionFactory) throws Exception
      {
         connectionFactoryMap.put(connectionFactory.getName(), connectionFactory);
         delegate.storeConnectionFactory(connectionFactory);
      }

      public List<PersistedConnectionFactory> recoverConnectionFactories()
      {
         return delegate.recoverConnectionFactories();
      }

      public void addJNDI(PersistedType type, String name, String... address) throws Exception
      {
         persistedJNDIMap.putIfAbsent(name, new ArrayList<String>());
         for (String ad : address)
         {
            persistedJNDIMap.get(name).add(ad);
         }
         delegate.addJNDI(type, name, address);
      }

      public List<PersistedJNDI> recoverPersistedJNDI() throws Exception
      {
         return delegate.recoverPersistedJNDI();
      }

      public void deleteJNDI(PersistedType type, String name, String address) throws Exception
      {
         persistedJNDIMap.get(name).remove(address);
         delegate.deleteJNDI(type, name, address);
      }

      public void deleteJNDI(PersistedType type, String name) throws Exception
      {
         persistedJNDIMap.get(name).clear();
         delegate.deleteJNDI(type, name);
      }

      public void start() throws Exception
      {
         delegate.start();
      }

      public void stop() throws Exception
      {
         delegate.stop();
      }

      public boolean isStarted()
      {
         return delegate.isStarted();
      }

      public void load() throws Exception
      {
         delegate.load();
      }
   }

}