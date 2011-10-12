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

package org.hornetq.tests.integration.cluster.failover;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.core.client.SessionFailureListener;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.impl.InVMNodeManager;
import org.hornetq.jms.client.HornetQTextMessage;

/**
 * @author <a href="mailto:andy.taylor@jboss.com">Andy Taylor</a>
 *         Date: Dec 21, 2010
 *         Time: 12:04:16 PM
 */
public class FailBackAutoTest extends FailoverTestBase
{
   Logger log = Logger.getLogger(FailBackAutoTest.class);
   private ServerLocatorInternal locator;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      locator = getServerLocator();
   }

   @Override
   protected void tearDown() throws Exception
   {
      if (locator != null)
      {
         try
         {
            locator.close();
         }
         catch (Exception e)
         {
            //
         }
      }
      super.tearDown();
   }
 
   public void testAutoFailback() throws Exception
   {
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setFailoverOnInitialConnection(true);
      locator.setReconnectAttempts(-1);
      ((ServerLocatorInternal)locator).setIdentity("testAutoFailback");
       
      ClientSessionFactoryInternal sf = createSessionFactoryAndWaitForTopology(locator, 2);
      final CountDownLatch latch = new CountDownLatch(1);

      ClientSession session = sendAndConsume(sf, true);
      
      System.out.println(locator.getTopology().describe());

      MyListener listener = new MyListener(latch);

      session.addFailureListener(listener);
      
      System.out.println(locator.getTopology().describe());

      liveServer.crash();
      
      assertTrue(latch.await(5, TimeUnit.SECONDS));
      
      log.info("backup (nowLive) topology = " + backupServer.getServer().getClusterManager().getDefaultConnection().getTopology().describe());
      
      log.info("Server Crash!!!");

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      ClientMessage message = session.createMessage(true);

      setBody(0, message);

      producer.send(message);

      verifyMessageOnServer(1, 1);

      System.out.println(locator.getTopology().describe());
      

      session.removeFailureListener(listener);

      final CountDownLatch latch2 = new CountDownLatch(1);

      listener = new MyListener(latch2);

      session.addFailureListener(listener);

      log.info("******* starting live server back");
      liveServer.start();
      
      Thread.sleep(1000);
      
      System.out.println("After failback: " + locator.getTopology().describe());

      assertTrue(latch2.await(5, TimeUnit.SECONDS));

      message = session.createMessage(true);

      setBody(1, message);

      producer.send(message);

      session.close();

      verifyMessageOnServer(0, 1);

      sf.close();

      Assert.assertEquals(0, sf.numSessions());

      Assert.assertEquals(0, sf.numConnections());
   }

   /**
    * @throws Exception
    * @throws HornetQException
    */
   private void verifyMessageOnServer(final int server, final int numberOfMessages) throws Exception, HornetQException
   {
      ServerLocator backupLocator = createInVMLocator(server);
      ClientSessionFactory factorybkp = backupLocator.createSessionFactory();
      ClientSession sessionbkp = factorybkp.createSession(false, false);
      sessionbkp.start();
      ClientConsumer consumerbkp = sessionbkp.createConsumer(ADDRESS);
      for (int i = 0 ; i < numberOfMessages; i++)
      {
         ClientMessage msg = consumerbkp.receive(1000);
         assertNotNull(msg);
         msg.acknowledge();
         sessionbkp.commit();
      }
      sessionbkp.close();
      factorybkp.close();
      backupLocator.close();
   }

   public void testAutoFailbackThenFailover() throws Exception
   {
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setFailoverOnInitialConnection(true);
      locator.setReconnectAttempts(-1);
      ClientSessionFactoryInternal sf = createSessionFactoryAndWaitForTopology(locator, 2);
      CountDownLatch latch = new CountDownLatch(1);

      ClientSession session = sendAndConsume(sf, true);

      MyListener listener = new MyListener(latch);

      session.addFailureListener(listener);

      liveServer.crash();

      assertTrue(latch.await(5, TimeUnit.SECONDS));

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      ClientMessage message = session.createMessage(true);

      setBody(0, message);

      producer.send(message);

      session.removeFailureListener(listener);

      CountDownLatch latch2 = new CountDownLatch(1);

      listener = new MyListener(latch2);

      session.addFailureListener(listener);

      log.info("restarting live node now");
      liveServer.start();
      
      assertTrue(latch2.await(5, TimeUnit.SECONDS));

      message = session.createMessage(true);

      setBody(1, message);

      producer.send(message);

      CountDownLatch latch3 = new CountDownLatch(1);

      session.removeFailureListener(listener);

      listener = new MyListener(latch3);

      session.addFailureListener(listener);

      waitForBackup(sf, 10);

      liveServer.crash();

      assertTrue(latch3.await(5, TimeUnit.SECONDS));

      session.close();

      sf.close();

      Assert.assertEquals(0, sf.numSessions());

      Assert.assertEquals(0, sf.numConnections());
   }

   protected void createConfigs() throws Exception
   {
      nodeManager = new InVMNodeManager();

      backupConfig = super.createDefaultConfig();
      backupConfig.getAcceptorConfigurations().clear();
      backupConfig.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(false));
      backupConfig.setSecurityEnabled(false);
      backupConfig.setSharedStore(true);
      backupConfig.setBackup(true);
      backupConfig.setClustered(true);
      backupConfig.setFailbackDelay(1000);
      TransportConfiguration liveConnector = getConnectorTransportConfiguration(true);
      TransportConfiguration backupConnector = getConnectorTransportConfiguration(false);
      backupConfig.getConnectorConfigurations().put(liveConnector.getName(), liveConnector);
      backupConfig.getConnectorConfigurations().put(backupConnector.getName(), backupConnector);
      ArrayList<String> staticConnectors = new ArrayList<String>();
      staticConnectors.add(liveConnector.getName());
      ClusterConnectionConfiguration cccLive = new ClusterConnectionConfiguration("cluster1", "jms", backupConnector.getName(), -1, false, false, 1, 1,
            staticConnectors, false);
      backupConfig.getClusterConfigurations().add(cccLive);
      backupServer = createBackupServer();

      liveConfig = super.createDefaultConfig();
      liveConfig.getAcceptorConfigurations().clear();
      liveConfig.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(true));
      liveConfig.setSecurityEnabled(false);
      liveConfig.setSharedStore(true);
      liveConfig.setFailbackDelay(1000);
      liveConfig.setClustered(true);
      List<String> pairs = new ArrayList<String>();
      pairs.add(backupConnector.getName());
      ClusterConnectionConfiguration ccc0 = new ClusterConnectionConfiguration("cluster1", "jms", liveConnector.getName(), -1, false, false, 1, 1,
            pairs, false);
      liveConfig.getClusterConfigurations().add(ccc0);
      liveConfig.getConnectorConfigurations().put(liveConnector.getName(), liveConnector);
      liveConfig.getConnectorConfigurations().put(backupConnector.getName(), backupConnector);
      liveServer = createLiveServer();
   }

   @Override
   protected TransportConfiguration getAcceptorTransportConfiguration(final boolean live)
   {
      return getInVMTransportAcceptorConfiguration(live);
   }

   @Override
   protected TransportConfiguration getConnectorTransportConfiguration(final boolean live)
   {
      return getInVMConnectorTransportConfiguration(live);
   }


   private ClientSession sendAndConsume(final ClientSessionFactory sf, final boolean createQueue) throws Exception
   {
      ClientSession session = sf.createSession(false, true, true);

      if (createQueue)
      {
         session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);
      }

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(HornetQTextMessage.TYPE,
               false,
               0,
               System.currentTimeMillis(),
               (byte) 1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBodyBuffer().writeString("aardvarks");
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive();

         Assert.assertEquals("aardvarks", message2.getBodyBuffer().readString());

         Assert.assertEquals(i, message2.getObjectProperty(new SimpleString("count")));

         message2.acknowledge();
      }

      ClientMessage message3 = consumer.receiveImmediate();
      
      consumer.close();

      Assert.assertNull(message3);

      return session;
   }

   /**
    * @param i
    * @param message
    * @throws Exception
    */
   protected void setBody(final int i, final ClientMessage message) throws Exception
   {
      message.getBodyBuffer().writeString("message" + i);
   }

   class MyListener implements SessionFailureListener
   {
      private final CountDownLatch latch;

      public MyListener(CountDownLatch latch)
      {
         this.latch = latch;
      }

      public void connectionFailed(final HornetQException me, boolean failedOver)
      {
         System.out.println("Failed, me");
         latch.countDown();
      }

      public void beforeReconnect(HornetQException exception)
      {
         System.out.println("MyListener.beforeReconnect");
      }
   }
}
