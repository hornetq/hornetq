/*
 * Copyright 2009 Red Hat, Inc.
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package org.hornetq.tests.integration.ra;

import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.jms.client.HornetQMessage;
import org.hornetq.ra.HornetQResourceAdapter;
import org.hornetq.ra.inflow.HornetQActivationSpec;
import org.hornetq.tests.util.ServiceTestBase;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.resource.ResourceException;
import javax.resource.spi.BootstrapContext;
import javax.resource.spi.UnavailableException;
import javax.resource.spi.XATerminator;
import javax.resource.spi.endpoint.MessageEndpoint;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.resource.spi.work.*;
import javax.transaction.xa.XAResource;
import java.lang.reflect.Method;
import java.util.Timer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         Created May 20, 2010
 */
public class HornetQMessageHandlerTest  extends ServiceTestBase
{
   private Configuration configuration;

   private HornetQServer server;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      clearData();
      configuration = createDefaultConfig();
      configuration.setSecurityEnabled(false);
      server = createServer(true, configuration);
      server.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      if (server != null)
      {
         try
         {
            server.stop();
            server = null;
         }
         catch (Exception e)
         {
            // ignore
         }
      }
      super.tearDown();
   }

   public void testSelectorChangedWithTopic() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = new HornetQResourceAdapter();
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
      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint);
      qResourceAdapter.endpointActivation(endpointFactory, spec);
      ClientSession session = createFactory(false).createSession();
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
      endpointFactory = new DummyMessageEndpointFactory(endpoint);
      qResourceAdapter.endpointActivation(endpointFactory, spec);
      message = session.createMessage(true);
      message.getBodyBuffer().writeString("3");
      message.putStringProperty("foo", "abar");
      clientProducer.send(message);
      latch.await(5, TimeUnit.SECONDS);

      assertNotNull(endpoint.lastMessage);
      assertEquals(endpoint.lastMessage.getCoreMessage().getBodyBuffer().readString(), "3");
      qResourceAdapter.endpointDeactivation(endpointFactory, spec);
   }

   public void testSelectorNotChangedWithTopic() throws Exception
   {      
      HornetQResourceAdapter qResourceAdapter = new HornetQResourceAdapter();
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
      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint);
      qResourceAdapter.endpointActivation(endpointFactory, spec);
      ClientSession session = createFactory(false).createSession();
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
      endpointFactory = new DummyMessageEndpointFactory(endpoint);
      qResourceAdapter.endpointActivation(endpointFactory, spec);
      latch.await(5, TimeUnit.SECONDS);

      assertNotNull(endpoint.lastMessage);
      assertEquals(endpoint.lastMessage.getCoreMessage().getBodyBuffer().readString(), "2");
      qResourceAdapter.endpointDeactivation(endpointFactory, spec);

   }

   class DummyMessageEndpointFactory implements MessageEndpointFactory
   {
      private DummyMessageEndpoint endpoint;

      public DummyMessageEndpointFactory(DummyMessageEndpoint endpoint)
      {
         this.endpoint = endpoint;
      }

      public MessageEndpoint createEndpoint(XAResource xaResource) throws UnavailableException
      {
         return endpoint;
      }

      public boolean isDeliveryTransacted(Method method) throws NoSuchMethodException
      {
         return false;
      }
   }

   class DummyMessageEndpoint implements MessageEndpoint, MessageListener
   {
      public CountDownLatch latch;

      private HornetQMessage lastMessage;

      public DummyMessageEndpoint(CountDownLatch latch)
      {
         this.latch = latch;
      }

      public void beforeDelivery(Method method) throws NoSuchMethodException, ResourceException
      {

      }

      public void afterDelivery() throws ResourceException
      {      
         if(latch != null)
         {
            latch.countDown();
         }
      }

      public void release()
      {

      }

      public void onMessage(Message message)
      {
         lastMessage = (HornetQMessage) message;
      }
   }

   class MyBootstrapContext implements BootstrapContext
   {
      WorkManager workManager = new DummyWorkManager();

      public Timer createTimer() throws UnavailableException
      {
         return null;
      }

      public WorkManager getWorkManager()
      {
         return workManager;
      }

      public XATerminator getXATerminator()
      {
         return null;
      }

      class DummyWorkManager implements WorkManager
      {
         public void doWork(Work work) throws WorkException
         {
         }

         public void doWork(Work work, long l, ExecutionContext executionContext, WorkListener workListener) throws WorkException
         {
         }

         public long startWork(Work work) throws WorkException
         {
            return 0;
         }

         public long startWork(Work work, long l, ExecutionContext executionContext, WorkListener workListener) throws WorkException
         {
            return 0;
         }

         public void scheduleWork(Work work) throws WorkException
         {
            work.run();
         }

         public void scheduleWork(Work work, long l, ExecutionContext executionContext, WorkListener workListener) throws WorkException
         {
         }
      }
   }
}
