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

import javax.jms.Message;
import javax.resource.ResourceException;
import javax.resource.spi.UnavailableException;
import javax.resource.spi.endpoint.MessageEndpoint;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;
import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.arjuna.ats.arjuna.coordinator.TransactionReaper;
import com.arjuna.ats.arjuna.coordinator.TxControl;
import com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionManagerImple;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.server.ServerSession;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.ra.HornetQResourceAdapter;
import org.hornetq.ra.inflow.HornetQActivation;
import org.hornetq.ra.inflow.HornetQActivationSpec;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.tests.util.RandomUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Simulates several messages being received over multiple instances with reconnects during the process.
 */
public class MDBMultipleHandlersServerDisconnectTest extends HornetQRATestBase
{
   final ConcurrentHashMap<Integer, AtomicInteger> mapCounter = new ConcurrentHashMap<Integer, AtomicInteger>();

   volatile HornetQResourceAdapter resourceAdapter;

   ServerLocator nettyLocator;

   @Before
   public void setUp() throws Exception
   {
      nettyLocator = createNettyNonHALocator();
      nettyLocator.setRetryInterval(10);
      nettyLocator.setReconnectAttempts(-1);
      mapCounter.clear();
      resourceAdapter = null;
      super.setUp();
      createQueue(true, "outQueue");
      DummyTMLocator.startTM();
   }

   @After
   public void tearDown() throws Exception
   {
      DummyTMLocator.stopTM();
      super.tearDown();
   }

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
   public void testMultipleSessions() throws Exception
   {
      AddressSettings settings = new AddressSettings();
      settings.setRedeliveryDelay(1000);
      settings.setMaxDeliveryAttempts(-1);
      server.getAddressSettingsRepository().clear();
      server.getAddressSettingsRepository().addMatch("#", settings);
      HornetQResourceAdapter qResourceAdapter = newResourceAdapter();
      resourceAdapter = qResourceAdapter;

      qResourceAdapter.setTransactionManagerLocatorClass(DummyTMLocator.class.getName());
      qResourceAdapter.setTransactionManagerLocatorMethod("getTM");

      MyBootstrapContext ctx = new MyBootstrapContext();

      qResourceAdapter.setConnectorClassName(NETTY_CONNECTOR_FACTORY);
      qResourceAdapter.start(ctx);

      final int NUMBER_OF_SESSIONS = 10;

      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setMaxSession(NUMBER_OF_SESSIONS);
      spec.setTransactionTimeout(1);
      spec.setReconnectAttempts(-1);
      spec.setReconnectInterval(1000);
      spec.setCallTimeout(1000L);
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);
      spec.setConsumerWindowSize(1024 * 1024);
//      spec.setConfirmationWindowSize(1024 * 10);

      TestEndpointFactory endpointFactory = new TestEndpointFactory(true);
      qResourceAdapter.endpointActivation(endpointFactory, spec);

      Assert.assertEquals(1, resourceAdapter.getActivations().values().size());
      HornetQActivation activation = resourceAdapter.getActivations().values().toArray(new HornetQActivation[1])[0];

      final int NUMBER_OF_MESSAGES = 1000;


      Thread producer = new Thread()
      {
         public void run()
         {
            try
            {
               ServerLocator locator = createInVMLocator(0);
               ClientSessionFactory factory = locator.createSessionFactory();
               ClientSession session = factory.createSession(false, false);

               ClientProducer clientProducer = session.createProducer(MDBQUEUEPREFIXED);

               for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
               {

                  ClientMessage message = session.createMessage(true);

                  message.getBodyBuffer().writeString("teststring " + i);
                  message.putIntProperty("i", i);

                  clientProducer.send(message);

                  if (i % 100 == 0)
                  {
                     session.commit();
                  }
               }
               session.commit();
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }

         }
      };

      producer.start();


      // This thread will keep bugging the handlers.
      // if they behave well with XA, the test pass!
      final AtomicBoolean running = new AtomicBoolean(true);

      Thread buggerThread = new Thread()
      {
         public void run()
         {
            while (running.get())
            {
               try
               {
                  Thread.sleep(RandomUtil.randomInterval(1000, 2000));
               }
               catch (InterruptedException intex)
               {
                  intex.printStackTrace();
                  return;
               }

               List<ServerSession> serverSessions = new LinkedList<ServerSession>();

               for (ServerSession session: server.getSessions())
               {
                  if (session.getMetaData("resource-adapter") != null)
                  {
                     serverSessions.add(session);
                  }
               }

               System.err.println("Contains " + serverSessions.size() + " RA sessions");

               if (serverSessions.size() > 0)
               {

                  int randomBother = RandomUtil.randomInterval(0, serverSessions.size() - 1);
                  System.out.println("bugging session " + randomBother);

                  RemotingConnection connection = serverSessions.get(randomBother).getRemotingConnection();

                  RemotingConnection serverConn = server.getRemotingService().removeConnection(connection.getID());
                  if (serverConn != null)
                  {
                     serverConn.fail(new HornetQException("Am I Bugging you? random= " + randomBother));
                  }
               }
            }

         }
      };

      buggerThread.start();

      ServerLocator locator = createInVMLocator(0);
      ClientSessionFactory factory = locator.createSessionFactory();
      ClientSession session = factory.createSession(false, false);
      session.start();

      ClientConsumer consumer = session.createConsumer("jms.queue.outQueue");

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         ClientMessage message = consumer.receive(60000);
         if (message == null)
         {
            break;
         }

         Assert.assertNotNull(message);
         message.acknowledge();

         Integer value = message.getIntProperty("i");
         AtomicInteger mapCount = new AtomicInteger(1);

         mapCount = mapCounter.putIfAbsent(value, mapCount);

         if (mapCount != null)
         {
            mapCount.incrementAndGet();
         }

         if (i % 200 == 0)
         {
            System.out.println("received " + i);
            session.commit();
         }
      }

      session.commit();
      Assert.assertNull(consumer.receiveImmediate());


      boolean failed = false;
      for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         AtomicInteger atomicInteger = mapCounter.get(Integer.valueOf(i));

         if (atomicInteger == null)
         {
            System.out.println("didn't receive message with i=" + i);
            failed = true;
         }
         else if (atomicInteger.get() > 1)
         {
            System.out.println("message with i=" + i + " received " + atomicInteger.get() + " times");
            failed = true;
         }
      }

      if (failed)
      {
         System.err.println("Test Failures!!!");
         System.exit(-1);
      }

      Assert.assertFalse(failed);

      System.out.println("Received " + NUMBER_OF_MESSAGES + " messages");


      running.set(false);

      buggerThread.join();
      producer.join();

      qResourceAdapter.stop();


      session.close();
   }

   protected class TestEndpointFactory implements MessageEndpointFactory
   {
      private final boolean isDeliveryTransacted;

      public TestEndpointFactory(boolean deliveryTransacted)
      {
         isDeliveryTransacted = deliveryTransacted;
      }

      public MessageEndpoint createEndpoint(XAResource xaResource) throws UnavailableException
      {
         TestEndpoint retEnd = new TestEndpoint();
         if (xaResource != null)
         {
            retEnd.setXAResource(xaResource);
         }
         return retEnd;
      }

      public boolean isDeliveryTransacted(Method method) throws NoSuchMethodException
      {
         return isDeliveryTransacted;
      }
   }


   public class TestEndpoint extends DummyMessageEndpoint
   {
      ClientSessionFactory factory;
      ClientSession endpointSession;
      ClientProducer producer;

      Transaction currentTX;

      public TestEndpoint()
      {
         super(null);
         try
         {
            factory = nettyLocator.createSessionFactory();
//            buggingList.add(factory);
            endpointSession = factory.createSession(true, false, false);
            producer = endpointSession.createProducer("jms.queue.outQueue");
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
//         try
//         {
//            System.out.println(Thread.currentThread().getName() + "**** onMessage enter " + message.getIntProperty("i"));
//         }
//         catch (Exception e)
//         {
//         }

         Integer value = 0;

         try
         {
            value = message.getIntProperty("i");
         }
         catch (Exception e)
         {

         }

         super.onMessage(message);

         try
         {
            currentTX.enlistResource(endpointSession);
            ClientMessage message1 = endpointSession.createMessage(true);
            message1.putIntProperty("i", message.getIntProperty("i"));
            producer.send(message1);
            currentTX.delistResource(endpointSession, XAResource.TMSUCCESS);


            if (RandomUtil.randomInterval(0, 10) == 5)
            {
               System.err.println("Waiting a long time for a timeout");
               Thread.sleep(1100);
            }
            else
            {
               System.out.println("passed fine!!!");
            }
         }
         catch (Exception e)
         {
            e.printStackTrace();
            try
            {
               currentTX.setRollbackOnly();
            }
            catch (Exception ex)
            {
            }
            e.printStackTrace();
//            throw new RuntimeException(e);
         }
      }

      @Override
      public void afterDelivery() throws ResourceException
      {
         try
         {
            DummyTMLocator.tm.commit();
//            currentTX.commit();
         }
         catch (Throwable e)
         {
         }
         super.afterDelivery();
      }
   }

   public static class DummyTMLocator
   {
      public static TransactionManagerImple tm;

      public static void stopTM()
      {
         try
         {
            TransactionReaper.terminate(true);
            TxControl.disable(true);
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }
         tm = null;
      }

      public static void startTM()
      {
         tm = new TransactionManagerImple();
         TxControl.enable();
      }

      public TransactionManager getTM()
      {
         return tm;
      }
   }
}
