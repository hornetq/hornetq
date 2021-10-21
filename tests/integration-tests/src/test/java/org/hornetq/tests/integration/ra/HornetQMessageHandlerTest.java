/*
 * Copyright 2005-2014 Red Hat, Inc.
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
package org.hornetq.tests.integration.ra;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.resource.ResourceException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSession.QueueQuery;
import org.hornetq.api.core.client.SessionFailureListener;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ServerLocatorImpl;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.impl.LocalQueueBinding;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.ra.HornetQResourceAdapter;
import org.hornetq.ra.inflow.HornetQActivation;
import org.hornetq.ra.inflow.HornetQActivationSpec;
import org.hornetq.tests.util.UnitTestCase;
import org.junit.Test;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         Created May 20, 2010
 */
public class HornetQMessageHandlerTest extends HornetQRATestBase
{

   @Override
   public boolean useSecurity()
   {
      return false;
   }

   @Test
   public void testSimpleMessageReceivedOnQueue() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = newResourceAdapter();
      MyBootstrapContext ctx = new MyBootstrapContext();
      qResourceAdapter.start(ctx);
      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);
      qResourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      CountDownLatch latch = new CountDownLatch(1);
      DummyMessageEndpoint endpoint = new DummyMessageEndpoint(latch);
      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint, false);
      qResourceAdapter.endpointActivation(endpointFactory, spec);
      ClientSession session = locator.createSessionFactory().createSession();
      ClientProducer clientProducer = session.createProducer(MDBQUEUEPREFIXED);
      ClientMessage message = session.createMessage(true);
      message.getBodyBuffer().writeString("teststring");
      clientProducer.send(message);
      session.close();
      latch.await(5, TimeUnit.SECONDS);

      assertNotNull(endpoint.lastMessage);
      assertEquals(endpoint.lastMessage.getCoreMessage().getBodyBuffer().readString(), "teststring");

      qResourceAdapter.endpointDeactivation(endpointFactory, spec);

      qResourceAdapter.stop();
   }

   @Test
   public void testObjectMessageReceiveSerializationControl() throws Exception
   {
      String blackList = "org.hornetq.tests.integration.ra";
      String whiteList = "*";
      testDeserialization(blackList, whiteList, false);
   }

   @Test
   public void testObjectMessageReceiveSerializationControl1() throws Exception
   {
      String blackList = "some.other.pkg";
      String whiteList = "org.hornetq.tests.integration.ra";
      testDeserialization(blackList, whiteList, true);
   }

   @Test
   public void testObjectMessageReceiveSerializationControl2() throws Exception
   {
      String blackList = "*";
      String whiteList = "org.hornetq.tests.integration.ra";
      testDeserialization(blackList, whiteList, false);
   }

   @Test
   public void testObjectMessageReceiveSerializationControl3() throws Exception
   {
      String blackList = "org.hornetq.tests";
      String whiteList = "org.hornetq.tests.integration.ra";
      testDeserialization(blackList, whiteList, false);
   }

   @Test
   public void testObjectMessageReceiveSerializationControl4() throws Exception
   {
      String blackList = null;
      String whiteList = "some.other.pkg";
      testDeserialization(blackList, whiteList, false);
   }

   @Test
   public void testObjectMessageReceiveSerializationControl5() throws Exception
   {
      String blackList = null;
      String whiteList = null;
      testDeserialization(blackList, whiteList, true);
   }

   private void testDeserialization(String blackList, String whiteList, boolean shouldSucceed) throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = newResourceAdapter();
      qResourceAdapter.setDeserializationBlackList(blackList);
      qResourceAdapter.setDeserializationWhiteList(whiteList);

      MyBootstrapContext ctx = new MyBootstrapContext();
      qResourceAdapter.start(ctx);

      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);
      qResourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      CountDownLatch latch = new CountDownLatch(1);
      DummyMessageEndpoint endpoint = new DummyMessageEndpoint(latch);
      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint, false);
      qResourceAdapter.endpointActivation(endpointFactory, spec);

      HashMap<String, Object> transportConfig = new HashMap<String, Object>();

      HornetQConnectionFactory jmsFactory = new HornetQConnectionFactory(new ServerLocatorImpl(false, new TransportConfiguration(InVMConnectorFactory.class.getName(), transportConfig)));
      Connection connection = jmsFactory.createConnection();

      try
      {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue jmsQueue = session.createQueue(MDBQUEUE);
         ObjectMessage objMsg = session.createObjectMessage();
         objMsg.setObject(new DummySerializable());
         MessageProducer producer = session.createProducer(jmsQueue);
         producer.send(objMsg);
      }
      finally
      {
         connection.close();
      }

      latch.await(5, TimeUnit.SECONDS);

      assertNotNull(endpoint.lastMessage);

      ObjectMessage objMsg = (ObjectMessage) endpoint.lastMessage;

      try
      {
         Object obj = objMsg.getObject();
         assertTrue("deserialization should fail but got: " + obj, shouldSucceed);
         assertTrue(obj instanceof DummySerializable);
      }
      catch (JMSException e)
      {
         assertFalse("got unexpected exception: " + e, shouldSucceed);
      }

      qResourceAdapter.endpointDeactivation(endpointFactory, spec);

      qResourceAdapter.stop();
   }

   @Test
   public void testSimpleMessageReceivedOnQueueManyMessages() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = newResourceAdapter();
      MyBootstrapContext ctx = new MyBootstrapContext();
      qResourceAdapter.start(ctx);
      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);
      qResourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      CountDownLatch latch = new CountDownLatch(15);
      MultipleEndpoints endpoint = new MultipleEndpoints(latch, null, false);
      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint, false);
      qResourceAdapter.endpointActivation(endpointFactory, spec);
      ClientSession session = locator.createSessionFactory().createSession();
      ClientProducer clientProducer = session.createProducer(MDBQUEUEPREFIXED);
      for (int i = 0; i < 15; i++)
      {
         ClientMessage message = session.createMessage(true);
         message.getBodyBuffer().writeString("teststring" + i);
         clientProducer.send(message);
      }
      session.close();
      latch.await(5, TimeUnit.SECONDS);

      qResourceAdapter.endpointDeactivation(endpointFactory, spec);

      qResourceAdapter.stop();
   }

   @Test
   public void testSimpleMessageReceivedOnQueueManyMessagesAndInterrupt() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = newResourceAdapter();
      MyBootstrapContext ctx = new MyBootstrapContext();
      qResourceAdapter.start(ctx);
      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);
      qResourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      CountDownLatch latch = new CountDownLatch(15);
      CountDownLatch latchDone = new CountDownLatch(15);
      MultipleEndpoints endpoint = new MultipleEndpoints(latch, latchDone, true);
      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint, false);
      qResourceAdapter.endpointActivation(endpointFactory, spec);
      ClientSession session = locator.createSessionFactory().createSession();
      ClientProducer clientProducer = session.createProducer(MDBQUEUEPREFIXED);
      for (int i = 0; i < 15; i++)
      {
         ClientMessage message = session.createMessage(true);
         message.getBodyBuffer().writeString("teststring" + i);
         clientProducer.send(message);
      }
      session.close();
      latch.await(5, TimeUnit.SECONDS);

      qResourceAdapter.endpointDeactivation(endpointFactory, spec);

      latchDone.await(5, TimeUnit.SECONDS);

      assertEquals(15, endpoint.messages.intValue());
      assertEquals(0, endpoint.interrupted.intValue());

      qResourceAdapter.stop();
   }

   @Test
   public void testSimpleMessageReceivedOnQueueManyMessagesAndInterruptTimeout() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = newResourceAdapter();
      MyBootstrapContext ctx = new MyBootstrapContext();
      qResourceAdapter.start(ctx);
      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setCallTimeout(500L);
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);
      qResourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      CountDownLatch latch = new CountDownLatch(15);
      CountDownLatch latchDone = new CountDownLatch(15);
      MultipleEndpoints endpoint = new MultipleEndpoints(latch, latchDone, true);
      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint, false);
      qResourceAdapter.endpointActivation(endpointFactory, spec);
      ClientSession session = locator.createSessionFactory().createSession();
      ClientProducer clientProducer = session.createProducer(MDBQUEUEPREFIXED);
      for (int i = 0; i < 15; i++)
      {
         ClientMessage message = session.createMessage(true);
         message.getBodyBuffer().writeString("teststring" + i);
         clientProducer.send(message);
      }
      session.close();
      latch.await(5, TimeUnit.SECONDS);

      qResourceAdapter.endpointDeactivation(endpointFactory, spec);

      latchDone.await(5, TimeUnit.SECONDS);

      assertEquals(15, endpoint.messages.intValue());
      //half onmessage interrupted
      assertEquals(8, endpoint.interrupted.intValue());

      qResourceAdapter.stop();
   }
   /**
    * @return
    */
   protected HornetQResourceAdapter newResourceAdapter()
   {
      HornetQResourceAdapter qResourceAdapter = new HornetQResourceAdapter();
      qResourceAdapter.setTransactionManagerLocatorClass("");
      qResourceAdapter.setTransactionManagerLocatorMethod("");
      qResourceAdapter.setConnectorClassName(UnitTestCase.INVM_CONNECTOR_FACTORY);
      return qResourceAdapter;
   }

   @Test
   public void testServerShutdownAndReconnect() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = newResourceAdapter();
      qResourceAdapter.setReconnectAttempts(-1);
      qResourceAdapter.setCallTimeout(500L);
      qResourceAdapter.setTransactionManagerLocatorClass("");
      qResourceAdapter.setTransactionManagerLocatorMethod("");
      qResourceAdapter.setRetryInterval(500L);
      MyBootstrapContext ctx = new MyBootstrapContext();
      qResourceAdapter.start(ctx);
      // This is just to register a listener
      final CountDownLatch failedLatch = new CountDownLatch(1);
      ClientSessionFactoryInternal factoryListener = (ClientSessionFactoryInternal) qResourceAdapter.getDefaultHornetQConnectionFactory().getServerLocator().createSessionFactory();
      factoryListener.addFailureListener(new SessionFailureListener()
      {

         @Override
         public void connectionFailed(HornetQException exception, boolean failedOver)
         {
         }

         @Override
         public void beforeReconnect(HornetQException exception)
         {
            failedLatch.countDown();
         }
      });
      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);
      CountDownLatch latch = new CountDownLatch(1);
      DummyMessageEndpoint endpoint = new DummyMessageEndpoint(latch);
      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint, false);
      qResourceAdapter.endpointActivation(endpointFactory, spec);
      ClientSession session = locator.createSessionFactory().createSession();
      ClientProducer clientProducer = session.createProducer(MDBQUEUEPREFIXED);
      ClientMessage message = session.createMessage(true);
      message.getBodyBuffer().writeString("teststring");
      clientProducer.send(message);
      session.close();
      latch.await(5, TimeUnit.SECONDS);

      assertNotNull(endpoint.lastMessage);
      assertEquals(endpoint.lastMessage.getCoreMessage().getBodyBuffer().readString(), "teststring");


      server.stop();

      assertTrue(failedLatch.await(5, TimeUnit.SECONDS));

      qResourceAdapter.endpointDeactivation(endpointFactory, spec);

      qResourceAdapter.stop();
   }

   @Test
   public void testInvalidAckMode() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = newResourceAdapter();
      MyBootstrapContext ctx = new MyBootstrapContext();
      qResourceAdapter.start(ctx);
      HornetQActivationSpec spec = new HornetQActivationSpec();
      try
      {
         spec.setAcknowledgeMode("CLIENT_ACKNOWLEDGE");
         fail("should throw exception");
      }
      catch (java.lang.IllegalArgumentException e)
      {
         //pass
      }
      qResourceAdapter.stop();
   }

   @Test
   public void testSimpleMessageReceivedOnQueueInLocalTX() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = newResourceAdapter();
      qResourceAdapter.setUseLocalTx(true);
      MyBootstrapContext ctx = new MyBootstrapContext();
      qResourceAdapter.start(ctx);
      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);
      qResourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      CountDownLatch latch = new CountDownLatch(1);
      ExceptionDummyMessageEndpoint endpoint = new ExceptionDummyMessageEndpoint(latch);
      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint, false);
      qResourceAdapter.endpointActivation(endpointFactory, spec);
      ClientSession session = locator.createSessionFactory().createSession();
      ClientProducer clientProducer = session.createProducer(MDBQUEUEPREFIXED);
      ClientMessage message = session.createMessage(true);
      message.getBodyBuffer().writeString("teststring");
      clientProducer.send(message);
      latch.await(5, TimeUnit.SECONDS);

      assertNull(endpoint.lastMessage);


      latch = new CountDownLatch(1);
      endpoint.reset(latch);
      clientProducer.send(message);
      session.close();
      latch.await(5, TimeUnit.SECONDS);

      assertNotNull(endpoint.lastMessage);
      assertEquals(endpoint.lastMessage.getCoreMessage().getBodyBuffer().readString(), "teststring");

      qResourceAdapter.endpointDeactivation(endpointFactory, spec);
      qResourceAdapter.stop();
   }

   @Test
   public void testSimpleMessageReceivedOnQueueWithSelector() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = newResourceAdapter();
      MyBootstrapContext ctx = new MyBootstrapContext();
      qResourceAdapter.start(ctx);
      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);
      spec.setMessageSelector("color='red'");
      qResourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      CountDownLatch latch = new CountDownLatch(1);
      DummyMessageEndpoint endpoint = new DummyMessageEndpoint(latch);
      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint, false);
      qResourceAdapter.endpointActivation(endpointFactory, spec);
      ClientSession session = locator.createSessionFactory().createSession();
      ClientProducer clientProducer = session.createProducer(MDBQUEUEPREFIXED);
      ClientMessage message = session.createMessage(true);
      message.getBodyBuffer().writeString("blue");
      message.putStringProperty("color", "blue");
      clientProducer.send(message);
      message = session.createMessage(true);
      message.getBodyBuffer().writeString("red");
      message.putStringProperty("color", "red");
      clientProducer.send(message);
      session.close();
      latch.await(5, TimeUnit.SECONDS);

      assertNotNull(endpoint.lastMessage);
      assertEquals(endpoint.lastMessage.getCoreMessage().getBodyBuffer().readString(), "red");

      qResourceAdapter.endpointDeactivation(endpointFactory, spec);
      qResourceAdapter.stop();
   }

   @Test
   public void testEndpointDeactivated() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = newResourceAdapter();
      MyBootstrapContext ctx = new MyBootstrapContext();
      qResourceAdapter.start(ctx);
      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);
      qResourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      CountDownLatch latch = new CountDownLatch(1);
      DummyMessageEndpoint endpoint = new DummyMessageEndpoint(latch);
      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint, false);
      qResourceAdapter.endpointActivation(endpointFactory, spec);
      Binding binding = server.getPostOffice().getBinding(MDBQUEUEPREFIXEDSIMPLE);
      assertEquals(((LocalQueueBinding) binding).getQueue().getConsumerCount(), 15);
      qResourceAdapter.endpointDeactivation(endpointFactory, spec);
      assertEquals(((LocalQueueBinding) binding).getQueue().getConsumerCount(), 0);
      assertTrue(endpoint.released);
      qResourceAdapter.stop();
   }

   @Test
   public void testMaxSessions() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = newResourceAdapter();
      MyBootstrapContext ctx = new MyBootstrapContext();
      qResourceAdapter.start(ctx);
      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setMaxSession(1);
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);
      qResourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      CountDownLatch latch = new CountDownLatch(1);
      DummyMessageEndpoint endpoint = new DummyMessageEndpoint(latch);
      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint, false);
      qResourceAdapter.endpointActivation(endpointFactory, spec);
      Binding binding = server.getPostOffice().getBinding(MDBQUEUEPREFIXEDSIMPLE);
      assertEquals(((LocalQueueBinding) binding).getQueue().getConsumerCount(), 1);
      qResourceAdapter.endpointDeactivation(endpointFactory, spec);
      qResourceAdapter.stop();
   }

   @Test
   public void testSimpleTopic() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = newResourceAdapter();
      MyBootstrapContext ctx = new MyBootstrapContext();
      qResourceAdapter.start(ctx);
      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Topic");
      spec.setDestination("mdbTopic");
      qResourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      CountDownLatch latch = new CountDownLatch(1);
      DummyMessageEndpoint endpoint = new DummyMessageEndpoint(latch);
      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint, false);
      qResourceAdapter.endpointActivation(endpointFactory, spec);
      ClientSession session = locator.createSessionFactory().createSession();
      ClientProducer clientProducer = session.createProducer("jms.topic.mdbTopic");
      ClientMessage message = session.createMessage(true);
      message.getBodyBuffer().writeString("test");
      clientProducer.send(message);

      latch.await(5, TimeUnit.SECONDS);

      assertNotNull(endpoint.lastMessage);
      assertEquals(endpoint.lastMessage.getCoreMessage().getBodyBuffer().readString(), "test");

      qResourceAdapter.endpointDeactivation(endpointFactory, spec);
      qResourceAdapter.stop();
   }

   @Test
   public void testDurableSubscription() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = newResourceAdapter();
      MyBootstrapContext ctx = new MyBootstrapContext();
      qResourceAdapter.start(ctx);
      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Topic");
      spec.setDestination("mdbTopic");
      spec.setSubscriptionDurability("Durable");
      spec.setSubscriptionName("durable-mdb");
      spec.setClientID("id-1");
      qResourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      CountDownLatch latch = new CountDownLatch(1);
      DummyMessageEndpoint endpoint = new DummyMessageEndpoint(latch);
      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint, false);
      qResourceAdapter.endpointActivation(endpointFactory, spec);
      ClientSession session = locator.createSessionFactory().createSession();
      ClientProducer clientProducer = session.createProducer("jms.topic.mdbTopic");
      ClientMessage message = session.createMessage(true);
      message.getBodyBuffer().writeString("1");
      clientProducer.send(message);

      latch.await(5, TimeUnit.SECONDS);

      assertNotNull(endpoint.lastMessage);
      assertEquals(endpoint.lastMessage.getCoreMessage().getBodyBuffer().readString(), "1");

      qResourceAdapter.endpointDeactivation(endpointFactory, spec);

      message = session.createMessage(true);
      message.getBodyBuffer().writeString("2");
      clientProducer.send(message);

      latch = new CountDownLatch(1);
      endpoint = new DummyMessageEndpoint(latch);
      endpointFactory = new DummyMessageEndpointFactory(endpoint, false);
      qResourceAdapter.endpointActivation(endpointFactory, spec);
      latch.await(5, TimeUnit.SECONDS);

      assertNotNull(endpoint.lastMessage);
      assertEquals(endpoint.lastMessage.getCoreMessage().getBodyBuffer().readString(), "2");
      latch = new CountDownLatch(1);
      endpoint.reset(latch);
      message = session.createMessage(true);
      message.getBodyBuffer().writeString("3");
      clientProducer.send(message);
      latch.await(5, TimeUnit.SECONDS);

      assertNotNull(endpoint.lastMessage);
      assertEquals(endpoint.lastMessage.getCoreMessage().getBodyBuffer().readString(), "3");
      qResourceAdapter.endpointDeactivation(endpointFactory, spec);
      qResourceAdapter.stop();
   }

   @Test
   public void testNonDurableSubscription() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = newResourceAdapter();
      MyBootstrapContext ctx = new MyBootstrapContext();
      qResourceAdapter.start(ctx);
      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Topic");
      spec.setDestination("mdbTopic");
      qResourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      CountDownLatch latch = new CountDownLatch(1);
      DummyMessageEndpoint endpoint = new DummyMessageEndpoint(latch);
      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint, false);
      qResourceAdapter.endpointActivation(endpointFactory, spec);
      ClientSession session = locator.createSessionFactory().createSession();
      ClientProducer clientProducer = session.createProducer("jms.topic.mdbTopic");
      ClientMessage message = session.createMessage(true);
      message.getBodyBuffer().writeString("1");
      clientProducer.send(message);

      latch.await(5, TimeUnit.SECONDS);

      assertNotNull(endpoint.lastMessage);
      assertEquals(endpoint.lastMessage.getCoreMessage().getBodyBuffer().readString(), "1");

      qResourceAdapter.endpointDeactivation(endpointFactory, spec);

      message = session.createMessage(true);
      message.getBodyBuffer().writeString("2");
      clientProducer.send(message);

      latch = new CountDownLatch(1);
      endpoint = new DummyMessageEndpoint(latch);
      endpointFactory = new DummyMessageEndpointFactory(endpoint, false);
      qResourceAdapter.endpointActivation(endpointFactory, spec);
      message = session.createMessage(true);
      message.getBodyBuffer().writeString("3");
      clientProducer.send(message);
      latch.await(5, TimeUnit.SECONDS);

      assertNotNull(endpoint.lastMessage);
      assertEquals(endpoint.lastMessage.getCoreMessage().getBodyBuffer().readString(), "3");
      qResourceAdapter.endpointDeactivation(endpointFactory, spec);
      qResourceAdapter.stop();
   }

   //https://issues.jboss.org/browse/JBPAPP-8017
   @Test
   public void testNonDurableSubscriptionDeleteAfterCrash() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = newResourceAdapter();
      qResourceAdapter.setTransactionManagerLocatorClass("");
      qResourceAdapter.setTransactionManagerLocatorMethod("");
      MyBootstrapContext ctx = new MyBootstrapContext();
      qResourceAdapter.start(ctx);
      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Topic");
      spec.setDestination("mdbTopic");
      qResourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      CountDownLatch latch = new CountDownLatch(1);
      DummyMessageEndpoint endpoint = new DummyMessageEndpoint(latch);
      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint, false);
      qResourceAdapter.endpointActivation(endpointFactory, spec);

      ClientSession session = locator.createSessionFactory().createSession();
      ClientProducer clientProducer = session.createProducer("jms.topic.mdbTopic");
      ClientMessage message = session.createMessage(true);
      message.getBodyBuffer().writeString("1");
      clientProducer.send(message);

      latch.await(5, TimeUnit.SECONDS);

      assertNotNull(endpoint.lastMessage);
      assertEquals(endpoint.lastMessage.getCoreMessage().getBodyBuffer().readString(), "1");

      HornetQActivation activation = lookupActivation(qResourceAdapter);

      SimpleString tempQueueName = activation.getTopicTemporaryQueue();

      QueueQuery query = session.queueQuery(tempQueueName);
      assertTrue(query.isExists());

      //this should be enough to simulate the crash
      qResourceAdapter.getDefaultHornetQConnectionFactory().close();
      qResourceAdapter.stop();

      query = session.queueQuery(tempQueueName);

      assertFalse(query.isExists());
   }

   @Test
   public void testSelectorChangedWithTopic() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = newResourceAdapter();

      MyBootstrapContext ctx = new MyBootstrapContext();

      qResourceAdapter.start(ctx);

      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Topic");
      spec.setDestination("mdbTopic");
      spec.setSubscriptionDurability("Durable");
      spec.setSubscriptionName("durable-mdb");
      spec.setClientID("id-1");
      spec.setMessageSelector("foo='bar'");

      qResourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);

      CountDownLatch latch = new CountDownLatch(1);

      DummyMessageEndpoint endpoint = new DummyMessageEndpoint(latch);
      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint, false);
      qResourceAdapter.endpointActivation(endpointFactory, spec);
      ClientSession session = locator.createSessionFactory().createSession();
      ClientProducer clientProducer = session.createProducer("jms.topic.mdbTopic");
      ClientMessage message = session.createMessage(true);
      message.getBodyBuffer().writeString("1");
      message.putStringProperty("foo", "bar");
      clientProducer.send(message);

      latch.await(5, TimeUnit.SECONDS);

      assertNotNull(endpoint.lastMessage);
      assertEquals(endpoint.lastMessage.getCoreMessage().getBodyBuffer().readString(), "1");

      qResourceAdapter.endpointDeactivation(endpointFactory, spec);

      message = session.createMessage(true);
      message.getBodyBuffer().writeString("2");
      message.putStringProperty("foo", "bar");
      clientProducer.send(message);

      latch = new CountDownLatch(1);
      endpoint = new DummyMessageEndpoint(latch);
      //change the selector forcing the queue to be recreated
      spec.setMessageSelector("foo='abar'");
      endpointFactory = new DummyMessageEndpointFactory(endpoint, false);
      qResourceAdapter.endpointActivation(endpointFactory, spec);
      message = session.createMessage(true);
      message.getBodyBuffer().writeString("3");
      message.putStringProperty("foo", "abar");
      clientProducer.send(message);
      latch.await(5, TimeUnit.SECONDS);

      assertNotNull(endpoint.lastMessage);
      assertEquals(endpoint.lastMessage.getCoreMessage().getBodyBuffer().readString(), "3");
      qResourceAdapter.endpointDeactivation(endpointFactory, spec);
      qResourceAdapter.stop();
   }

   @Test
   public void testSharedSubscription() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = newResourceAdapter();
      MyBootstrapContext ctx = new MyBootstrapContext();
      qResourceAdapter.start(ctx);

      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Topic");
      spec.setDestination("mdbTopic");
      spec.setSubscriptionDurability("Durable");
      spec.setSubscriptionName("durable-mdb");
      spec.setClientID("id-1");
      spec.setSetupAttempts(1);
      spec.setShareSubscriptions(true);
      spec.setMaxSession(1);

      HornetQActivationSpec spec2 = new HornetQActivationSpec();
      spec2.setResourceAdapter(qResourceAdapter);
      spec2.setUseJNDI(false);
      spec2.setDestinationType("javax.jms.Topic");
      spec2.setDestination("mdbTopic");
      spec2.setSubscriptionDurability("Durable");
      spec2.setSubscriptionName("durable-mdb");
      spec2.setClientID("id-1");
      spec2.setSetupAttempts(1);
      spec2.setShareSubscriptions(true);
      spec2.setMaxSession(1);


      CountDownLatch latch = new CountDownLatch(5);
      DummyMessageEndpoint endpoint = new DummyMessageEndpoint(latch);
      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint, false);
      qResourceAdapter.endpointActivation(endpointFactory, spec);

      CountDownLatch latch2 = new CountDownLatch(5);
      DummyMessageEndpoint endpoint2 = new DummyMessageEndpoint(latch2);
      DummyMessageEndpointFactory endpointFactory2 = new DummyMessageEndpointFactory(endpoint2, false);
      qResourceAdapter.endpointActivation(endpointFactory2, spec2);

      ClientSession session = locator.createSessionFactory().createSession();
      ClientProducer clientProducer = session.createProducer("jms.topic.mdbTopic");

      for (int i = 0; i < 10; i++)
      {
         ClientMessage message = session.createMessage(true);
         message.getBodyBuffer().writeString("" + i);
         clientProducer.send(message);
      }
      session.commit();

      assertTrue(latch.await(5, TimeUnit.SECONDS));
      assertTrue(latch2.await(5, TimeUnit.SECONDS));

      assertNotNull(endpoint.lastMessage);
      assertNotNull(endpoint2.lastMessage);

      qResourceAdapter.endpointDeactivation(endpointFactory, spec);
      qResourceAdapter.endpointDeactivation(endpointFactory2, spec2);
      qResourceAdapter.stop();

   }

   @Test
   public void testSelectorNotChangedWithTopic() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = newResourceAdapter();
      MyBootstrapContext ctx = new MyBootstrapContext();
      qResourceAdapter.start(ctx);
      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Topic");
      spec.setDestination("mdbTopic");
      spec.setSubscriptionDurability("Durable");
      spec.setSubscriptionName("durable-mdb");
      spec.setClientID("id-1");
      spec.setMessageSelector("foo='bar'");
      qResourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      CountDownLatch latch = new CountDownLatch(1);
      DummyMessageEndpoint endpoint = new DummyMessageEndpoint(latch);
      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint, false);
      qResourceAdapter.endpointActivation(endpointFactory, spec);
      ClientSession session = locator.createSessionFactory().createSession();
      ClientProducer clientProducer = session.createProducer("jms.topic.mdbTopic");
      ClientMessage message = session.createMessage(true);
      message.getBodyBuffer().writeString("1");
      message.putStringProperty("foo", "bar");
      clientProducer.send(message);

      latch.await(5, TimeUnit.SECONDS);

      assertNotNull(endpoint.lastMessage);
      assertEquals(endpoint.lastMessage.getCoreMessage().getBodyBuffer().readString(), "1");

      qResourceAdapter.endpointDeactivation(endpointFactory, spec);

      message = session.createMessage(true);
      message.getBodyBuffer().writeString("2");
      message.putStringProperty("foo", "bar");
      clientProducer.send(message);

      latch = new CountDownLatch(1);
      endpoint = new DummyMessageEndpoint(latch);
      endpointFactory = new DummyMessageEndpointFactory(endpoint, false);
      qResourceAdapter.endpointActivation(endpointFactory, spec);
      latch.await(5, TimeUnit.SECONDS);

      assertNotNull(endpoint.lastMessage);
      assertEquals(endpoint.lastMessage.getCoreMessage().getBodyBuffer().readString(), "2");
      qResourceAdapter.endpointDeactivation(endpointFactory, spec);
      qResourceAdapter.stop();

   }

   class ExceptionDummyMessageEndpoint extends DummyMessageEndpoint
   {
      boolean throwException = true;

      public ExceptionDummyMessageEndpoint(CountDownLatch latch)
      {
         super(latch);
      }

      @Override
      public void onMessage(Message message)
      {
         if (throwException)
         {
            throwException = false;
            throw new IllegalStateException("boo!");
         }
         super.onMessage(message);
      }
   }

   class MultipleEndpoints extends DummyMessageEndpoint
   {
      private final CountDownLatch latch;
      private final CountDownLatch latchDone;
      private final boolean pause;
      AtomicInteger messages = new AtomicInteger(0);
      AtomicInteger interrupted = new AtomicInteger(0);

      public MultipleEndpoints(CountDownLatch latch, CountDownLatch latchDone, boolean pause)
      {
         super(latch);
         this.latch = latch;
         this.latchDone = latchDone;
         this.pause = pause;
      }

      @Override
      public void beforeDelivery(Method method) throws NoSuchMethodException, ResourceException
      {

      }

      @Override
      public void afterDelivery() throws ResourceException
      {

      }

      @Override
      public void release()
      {

      }

      @Override
      public void onMessage(Message message)
      {
         try
         {
            latch.countDown();
            if (pause && messages.getAndIncrement() % 2 == 0)
            {
               try
               {
                  System.out.println("pausing for 2 secs");
                  Thread.sleep(2000);
               }
               catch (InterruptedException e)
               {
                  interrupted.incrementAndGet();
               }
            }
         }
         finally
         {
            if (latchDone != null)
            {
               latchDone.countDown();
            }
         }
      }
   }


   static class DummySerializable implements Serializable
   {
   }
}
