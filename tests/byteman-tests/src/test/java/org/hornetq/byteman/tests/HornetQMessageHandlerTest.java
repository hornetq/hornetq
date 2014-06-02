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
package org.hornetq.byteman.tests;

import javax.jms.Message;
import javax.resource.ResourceException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionManagerImple;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.server.Queue;
import org.hornetq.ra.HornetQResourceAdapter;
import org.hornetq.ra.inflow.HornetQActivationSpec;
import org.hornetq.tests.integration.ra.HornetQRATestBase;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         Created May 20, 2010
 */
@RunWith(BMUnitRunner.class)
public class HornetQMessageHandlerTest extends HornetQRATestBase
{

   protected boolean usePersistence()
   {
      return true;
   }


   @Override
   public boolean useSecurity()
   {
      return false;
   }

   @Test
   @BMRules
      (
         rules =
            {
               @BMRule
                  (
                     name = "interrupt",
                     targetClass = "org.hornetq.core.client.impl.ClientSessionImpl",
                     targetMethod = "flushAcks",
                     targetLocation = "ENTRY",
                     action = "org.hornetq.byteman.tests.HornetQMessageHandlerTest.interrupt();"
                  )
            }
      )
   public void testSimpleMessageReceivedOnQueue() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = newResourceAdapter();

      qResourceAdapter.setTransactionManagerLocatorClass(DummyTMLocator.class.getName());
      qResourceAdapter.setTransactionManagerLocatorMethod("getTM");

      MyBootstrapContext ctx = new MyBootstrapContext();

      qResourceAdapter.setConnectorClassName(NETTY_CONNECTOR_FACTORY);
      qResourceAdapter.start(ctx);

      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setMaxSession(1);
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);

      CountDownLatch latch = new CountDownLatch(1);

      XADummyEndpoint endpoint = new XADummyEndpoint(latch);

      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint, true);

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

      Binding binding = server.getPostOffice().getBinding(SimpleString.toSimpleString(MDBQUEUEPREFIXED));
      assertEquals(1, ((Queue) binding.getBindable()).getMessageCount());

      server.stop();
      server.start();

      ClientSessionFactory factory = locator.createSessionFactory();
      session = factory.createSession(true, true);

      session.start();
      ClientConsumer consumer = session.createConsumer(MDBQUEUEPREFIXED);
      assertNotNull(consumer.receive(5000));
      session.close();

   }

   static volatile boolean inAfterDelivery = false;

   public static void interrupt()
   {
      inAfterDelivery = false;
      Thread.currentThread().interrupt();
   }

   Transaction currentTX;

   public class XADummyEndpoint extends DummyMessageEndpoint
   {

      ClientSession session;

      public XADummyEndpoint(CountDownLatch latch) throws SystemException
      {
         super(latch);
         try
         {
            session = locator.createSessionFactory().createSession(true, false, false);
         }
         catch (Throwable e)
         {
            throw new RuntimeException(e);
         }
      }

      @Override
      public void beforeDelivery(Method method) throws NoSuchMethodException, ResourceException
      {
         super.beforeDelivery(method);
         try
         {
            DummyTMLocator.tm.begin();
            currentTX = DummyTMLocator.tm.getTransaction();
            currentTX.enlistResource(xaResource);
         }
         catch (Throwable e)
         {
            throw new RuntimeException(e.getMessage(), e);
         }
      }

      public void onMessage(Message message)
      {
         super.onMessage(message);
//         try
//         {
//            lastMessage = (HornetQMessage) message;
//            currentTX.enlistResource(session);
//            ClientProducer prod = session.createProducer()
//         }
//         catch (Exception e)
//         {
//            e.printStackTrace();
//         }


      }


      @Override
      public void afterDelivery() throws ResourceException
      {
         inAfterDelivery = true;
         try
         {
            currentTX.commit();
         }
         catch (Throwable e)
         {
            throw new RuntimeException(e);
         }
         super.afterDelivery();
      }
   }

   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      DummyTMLocator.startTM();
   }


   @After
   public void tearDown() throws Exception
   {
      disableCheckThread();
      super.tearDown();
      DummyTMLocator.stopTM();
   }


   public static class DummyTMLocator
   {
      public static TransactionManagerImple tm;

      public static void stopTM()
      {
         tm = null;
      }

      public static void startTM()
      {
         tm = new TransactionManagerImple();
      }

      public TransactionManager getTM()
      {
         return tm;
      }
   }

}
