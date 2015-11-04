package org.hornetq.tests.integration.ra;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.core.server.Queue;
import org.hornetq.ra.HornetQResourceAdapter;
import org.hornetq.ra.inflow.HornetQActivation;
import org.hornetq.ra.inflow.HornetQActivationSpec;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class HornetQClusteredTest extends HornetQRAClusteredTestBase
{

   /*
   * the second server has no queue so this tests for partial initialisation
   * */
   @Test
   public void testShutdownOnPartialConnect() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = newResourceAdapter();
      MyBootstrapContext ctx = new MyBootstrapContext();
      qResourceAdapter.setHA(true);
      qResourceAdapter.start(ctx);
      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setSetupAttempts(0);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);
      spec.setHA(true);
      qResourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY + "," + INVM_CONNECTOR_FACTORY);
      qResourceAdapter.setConnectionParameters("server-id=0, server-id=1");
      CountDownLatch latch = new CountDownLatch(1);
      DummyMessageEndpoint endpoint = new DummyMessageEndpoint(latch);
      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint, false);
      qResourceAdapter.endpointActivation(endpointFactory, spec);
      //make sure thet activation didnt start, i.e. no MDB consumers
      assertEquals(((Queue)server.getPostOffice().getBinding(MDBQUEUEPREFIXEDSIMPLE).getBindable()).getConsumerCount(), 0);
      qResourceAdapter.endpointDeactivation(endpointFactory, spec);

      qResourceAdapter.stop();
   }


   /**
    * https://bugzilla.redhat.com/show_bug.cgi?id=1029076
    * Look at the logs for this test, if you see exceptions it's an issue.
    * @throws Exception
    */
   @Test
   public void testNonDurableInCluster() throws Exception
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


      HornetQActivation activation =  lookupActivation(qResourceAdapter);

      SimpleString tempQueue = activation.getTopicTemporaryQueue();

      assertNotNull(server.locateQueue(tempQueue));
      assertNotNull(secondaryServer.locateQueue(tempQueue));


      latch.await(5, TimeUnit.SECONDS);

      assertNotNull(endpoint.lastMessage);
      assertEquals(endpoint.lastMessage.getCoreMessage().getBodyBuffer().readString(), "test");

      qResourceAdapter.endpointDeactivation(endpointFactory, spec);
      qResourceAdapter.stop();

      assertNull(server.locateQueue(tempQueue));
      assertNull(secondaryServer.locateQueue(tempQueue));

   }

   @Test
   public void testRebalance() throws Exception
   {
      final int CONSUMER_COUNT = 10;
      secondaryJmsServer.createQueue(true, MDBQUEUE, null, true, "/jms/" + MDBQUEUE);

      HornetQResourceAdapter qResourceAdapter = newResourceAdapter();
      MyBootstrapContext ctx = new MyBootstrapContext();
      qResourceAdapter.start(ctx);
      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);
      spec.setRebalanceConnections(true);
      spec.setMaxSession(CONSUMER_COUNT);
      spec.setSetupAttempts(5);
      spec.setSetupInterval(200);
      spec.setHA(true); // if this isn't true then the toplogy listener won't get nodeDown notifications
      spec.setCallTimeout(500L); // if this isn't set then it may take a long time for tearDown to occur on the MDB connection
      qResourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      CountDownLatch latch = new CountDownLatch(1);
      DummyMessageEndpoint endpoint = new DummyMessageEndpoint(latch);
      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint, false);
      qResourceAdapter.endpointActivation(endpointFactory, spec);

      Queue primaryQueue = server.locateQueue(MDBQUEUEPREFIXEDSIMPLE);
      Queue secondaryQueue = secondaryServer.locateQueue(MDBQUEUEPREFIXEDSIMPLE);

      assertTrue(primaryQueue.getConsumerCount() < CONSUMER_COUNT);
      assertTrue(secondaryQueue.getConsumerCount() < CONSUMER_COUNT);
      assertTrue(primaryQueue.getConsumerCount() + secondaryQueue.getConsumerCount() == CONSUMER_COUNT);

      ClientSession session = addClientSession(locator.createSessionFactory().createSession());
      ClientProducer clientProducer = session.createProducer(MDBQUEUEPREFIXED);
      ClientMessage message = session.createMessage(true);
      message.getBodyBuffer().writeString("test");
      clientProducer.send(message);

      latch.await(5, TimeUnit.SECONDS);

      assertNotNull(endpoint.lastMessage);
      assertEquals(endpoint.lastMessage.getCoreMessage().getBodyBuffer().readString(), "test");

      for (int i = 0; i < 10; i++)
      {
         secondaryServer.stop();

         long mark = System.currentTimeMillis();
         long timeout = 5000;
         while (primaryQueue.getConsumerCount() < CONSUMER_COUNT && (System.currentTimeMillis() - mark) < timeout)
         {
            Thread.sleep(100);
         }

         assertTrue(primaryQueue.getConsumerCount() == CONSUMER_COUNT);

         secondaryServer.start();
         waitForServer(secondaryServer);
         secondaryQueue = secondaryServer.locateQueue(MDBQUEUEPREFIXEDSIMPLE);

         mark = System.currentTimeMillis();
         while (((primaryQueue.getConsumerCount() + secondaryQueue.getConsumerCount()) < (CONSUMER_COUNT) || primaryQueue.getConsumerCount() == CONSUMER_COUNT) && (System.currentTimeMillis() - mark) <= timeout)
         {
            Thread.sleep(100);
         }

         assertTrue(primaryQueue.getConsumerCount() < CONSUMER_COUNT);
         assertTrue(secondaryQueue.getConsumerCount() < CONSUMER_COUNT);
         assertTrue(primaryQueue.getConsumerCount() + secondaryQueue.getConsumerCount() == CONSUMER_COUNT);
      }

      qResourceAdapter.endpointDeactivation(endpointFactory, spec);
      qResourceAdapter.stop();
   }
}
