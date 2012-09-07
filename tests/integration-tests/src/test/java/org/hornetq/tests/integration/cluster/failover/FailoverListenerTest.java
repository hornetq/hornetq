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

import junit.framework.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.FailoverEventListener;
import org.hornetq.api.core.client.FailoverEventType;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.server.impl.InVMNodeManager;
import org.hornetq.jms.client.HornetQTextMessage;
import org.hornetq.tests.integration.IntegrationTestLogger;
import org.hornetq.tests.util.TransportConfigurationUtils;

/**
 * @author <a href="mailto:andy.taylor@jboss.com">Andy Taylor</a>
 * @author <a href="mailto:flemming.harms@gmail.com">Flemming Harms</a>
 */
public class FailoverListenerTest extends FailoverTestBase
{
   private final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;
   private ServerLocatorInternal locator;
   private ClientSessionFactoryInternal sf;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      locator = getServerLocator();
   }

	/**
	 * Test if two servers is running and one of them is failing that we 
	 * trigger the expected events for {@link FailoverEventListener}
	 * @throws Exception
	 */
	public void testFailoverListerCall() throws Exception  
	{
      createSessionFactory(2);

      SessionFactoryFailoverListener listener = new SessionFactoryFailoverListener();
      sf.addFailoverListener(listener);
      ClientSession session = sendAndConsume(sf, true);

      liveServer.crash();
      assertEquals(FailoverEventType.FAILURE_DETECTED,listener.getFailoverEventType().get(0));

      log.info("backup (nowLive) topology = " + backupServer.getServer().getClusterManager().getDefaultConnection(null).getTopology().describe());

      log.info("Server Crash!!!");
      
      Thread.sleep(1000);
      //the backup server should be online by now
      assertEquals(FailoverEventType.FAILOVER_COMPLETED,listener.getFailoverEventType().get(1));

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      ClientMessage message = session.createMessage(true);

      setBody(0, message);

      producer.send(message);

      verifyMessageOnServer(1, 1);

      log.info("******* starting live server back");
      liveServer.start();
      Thread.sleep(1000);
      //starting the live server trigger a failover event
      assertEquals(FailoverEventType.FAILURE_DETECTED,listener.getFailoverEventType().get(2));

      //the life server should be online by now
      assertEquals(FailoverEventType.FAILOVER_COMPLETED,listener.getFailoverEventType().get(3));

      System.out.println("After failback: " + locator.getTopology().describe());

      message = session.createMessage(true);

      setBody(1, message);

      producer.send(message);

      session.close();

      verifyMessageOnServer(0, 1);

      wrapUpSessionFactory();
      assertEquals("Expected 4 FailoverEvents to be triggered", 4,listener.getFailoverEventType().size());
   }

   /**
    * @throws Exception
    * @throws HornetQException
    */
   private void verifyMessageOnServer(final int server, final int numberOfMessages) throws Exception, HornetQException
   {
      ServerLocator backupLocator = createInVMLocator(server);
      ClientSessionFactory factorybkp = addSessionFactory(createSessionFactory(backupLocator));
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

	/**
	 * Test that if the only server is running and failing we trigger
	 * the event FailoverEventType.FAILOVER_FAILED in the end
	 * @throws Exception
	 */
   public void testFailoverFailed() throws Exception
   {
	  locator.setBlockOnNonDurableSend(true);
	  locator.setBlockOnDurableSend(true);
	  locator.setFailoverOnInitialConnection(true); // unnecessary?
	  locator.setReconnectAttempts(1);
	  sf = createSessionFactoryAndWaitForTopology(locator, 2);

	  //make sure no backup server is running
      backupServer.stop();

      SessionFactoryFailoverListener listener = new SessionFactoryFailoverListener();
      sf.addFailoverListener(listener);
      ClientSession session = sendAndConsume(sf, true);

      liveServer.crash(session);
      assertEquals(FailoverEventType.FAILURE_DETECTED,listener.getFailoverEventType().get(0));

      assertEquals(FailoverEventType.FAILOVER_FAILED,listener.getFailoverEventType().get(1));
    
      assertEquals("Expected 2 FailoverEvents to be triggered", 2, listener.getFailoverEventType().size());
      session.close();

      wrapUpSessionFactory();
   }

   private void createSessionFactory(int members) throws Exception
   {
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setFailoverOnInitialConnection(true); // unnecessary?
      locator.setReconnectAttempts(-1);
      sf = createSessionFactoryAndWaitForTopology(locator, members);
   }

   private void wrapUpSessionFactory()
   {
      sf.close();
      Assert.assertEquals(0, sf.numSessions());
      Assert.assertEquals(0, sf.numConnections());
   }

   @Override
   protected void createConfigs() throws Exception
   {
      nodeManager = new InVMNodeManager();

      backupConfig = super.createDefaultConfig();
      backupConfig.getAcceptorConfigurations().clear();
      backupConfig.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(false));
      backupConfig.setSecurityEnabled(false);
      backupConfig.setSharedStore(true);
      backupConfig.setBackup(true);
      backupConfig.setFailbackDelay(1000);
      TransportConfiguration liveConnector = getConnectorTransportConfiguration(true);
      TransportConfiguration backupConnector = getConnectorTransportConfiguration(false);
      backupConfig.getConnectorConfigurations().put(liveConnector.getName(), liveConnector);
      backupConfig.getConnectorConfigurations().put(backupConnector.getName(), backupConnector);
      basicClusterConnectionConfig(backupConfig, backupConnector.getName(), liveConnector.getName());
      backupServer = createTestableServer(backupConfig);

      liveConfig = super.createDefaultConfig();
      liveConfig.getAcceptorConfigurations().clear();
      liveConfig.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(true));
      liveConfig.setSecurityEnabled(false);
      liveConfig.setSharedStore(true);
      liveConfig.setFailbackDelay(1000);
      basicClusterConnectionConfig(liveConfig, liveConnector.getName(), backupConnector.getName());
      liveConfig.getConnectorConfigurations().put(liveConnector.getName(), liveConnector);
      liveConfig.getConnectorConfigurations().put(backupConnector.getName(), backupConnector);
      liveServer = createTestableServer(liveConfig);
   }

   @Override
   protected TransportConfiguration getAcceptorTransportConfiguration(final boolean live)
   {
	      return TransportConfigurationUtils.getInVMAcceptor(live);
   }

   @Override
   protected TransportConfiguration getConnectorTransportConfiguration(final boolean live)
   {
	      return TransportConfigurationUtils.getInVMConnector(live);
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
   
   
   public class SessionFactoryFailoverListener implements FailoverEventListener {

		private ArrayList<FailoverEventType> failoverTypeEvent = new ArrayList<FailoverEventType>();

		public ArrayList<FailoverEventType> getFailoverEventType() 
		{
			return this.failoverTypeEvent;
		}

		@Override
		public void failoverEvent(FailoverEventType eventType) 
		{
			this.failoverTypeEvent.add(eventType);
			log.info("Failover event just happen : "+eventType.toString());
		}

	}

}
