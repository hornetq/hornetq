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

package org.hornetq.tests.integration.jms.cluster;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.Assert;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.FailoverEventListener;
import org.hornetq.api.core.client.FailoverEventType;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.remoting.impl.invm.InVMRegistry;
import org.hornetq.core.remoting.impl.invm.TransportConstants;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.NodeManager;
import org.hornetq.core.server.impl.InVMNodeManager;
import org.hornetq.jms.client.HornetQConnection;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.client.HornetQDestination;
import org.hornetq.jms.client.HornetQSession;
import org.hornetq.jms.server.JMSServerManager;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.tests.integration.IntegrationTestLogger;
import org.hornetq.tests.integration.jms.server.management.JMSUtil;
import org.hornetq.tests.unit.util.InVMContext;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.ServiceTestBase;

/**
 *
 * A JMSFailoverTest
 *
 * A simple test to test setFailoverListener when using the JMS API.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:flemming.harms@gmail.com">Flemming Harms</a>
 *
 */
public class JMSFailoverListenerTest extends ServiceTestBase
{
   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   protected InVMContext ctx1 = new InVMContext();

   protected InVMContext ctx2 = new InVMContext();

   protected Configuration backupConf;

   protected Configuration liveConf;

   protected JMSServerManager liveJMSService;

   protected HornetQServer liveService;

   protected JMSServerManager backupJMSService;

   protected HornetQServer backupService;

   protected Map<String, Object> backupParams = new HashMap<String, Object>();

   private TransportConfiguration backuptc;

   private TransportConfiguration livetc;

   private TransportConfiguration liveAcceptortc;

   private TransportConfiguration backupAcceptortc;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------


   public void testAutomaticFailover() throws Exception
   {
      HornetQConnectionFactory jbcf = HornetQJMSClient.createConnectionFactoryWithHA(JMSFactoryType.CF, livetc);
      jbcf.setReconnectAttempts(-1);
      jbcf.setBlockOnDurableSend(true);
      jbcf.setBlockOnNonDurableSend(true);

      // Note we set consumer window size to a value so we can verify that consumer credit re-sending
      // works properly on failover
      // The value is small enough that credits will have to be resent several time

      final int numMessages = 10;

      final int bodySize = 1000;

      jbcf.setConsumerWindowSize(numMessages * bodySize / 10);

      HornetQConnection conn = JMSUtil.createConnectionAndWaitForTopology(jbcf, 2, 5);

      MyFailoverListener listener = new MyFailoverListener();

      conn.setFailoverListener(listener);

      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      ClientSession coreSession = ((HornetQSession)sess).getCoreSession();

      SimpleString jmsQueueName = new SimpleString(HornetQDestination.JMS_QUEUE_ADDRESS_PREFIX + "myqueue");

      coreSession.createQueue(jmsQueueName, jmsQueueName, null, true);

      Queue queue = sess.createQueue("myqueue");

      MessageProducer producer = sess.createProducer(queue);

      producer.setDeliveryMode(DeliveryMode.PERSISTENT);

      MessageConsumer consumer = sess.createConsumer(queue);

      byte[] body = RandomUtil.randomBytes(bodySize);

      for (int i = 0; i < numMessages; i++)
      {
         BytesMessage bm = sess.createBytesMessage();

         bm.writeBytes(body);

         producer.send(bm);
      }

      conn.start();

      JMSFailoverListenerTest.log.info("sent messages and started connection");

      Thread.sleep(2000);

      JMSUtil.crash(liveService, ((HornetQSession)sess).getCoreSession());

      Assert.assertEquals(FailoverEventType.FAILURE_DETECTED, listener.getEventTypeList().get(0));
      for (int i = 0; i < numMessages; i++)
      {
         JMSFailoverListenerTest.log.info("got message " + i);

         BytesMessage bm = (BytesMessage)consumer.receive(1000);

         Assert.assertNotNull(bm);

         Assert.assertEquals(body.length, bm.getBodyLength());
      }

      TextMessage tm = (TextMessage)consumer.receiveNoWait();

      Assert.assertNull(tm);
      Assert.assertEquals(FailoverEventType.FAILOVER_COMPLETED, listener.getEventTypeList().get(1));

      conn.close();
      Assert.assertEquals("Expected 2 FailoverEvents to be triggered", 2, listener.getEventTypeList().size());
   }

   public void testManualFailover() throws Exception
   {
      HornetQConnectionFactory jbcfLive =
               HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF,
                                                                 new TransportConfiguration(INVM_CONNECTOR_FACTORY));

      jbcfLive.setBlockOnNonDurableSend(true);
      jbcfLive.setBlockOnDurableSend(true);

      HornetQConnectionFactory jbcfBackup =
               HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF,
                                                                 new TransportConfiguration(INVM_CONNECTOR_FACTORY,
                                                                                            backupParams));
      jbcfBackup.setBlockOnNonDurableSend(true);
      jbcfBackup.setBlockOnDurableSend(true);
      jbcfBackup.setInitialConnectAttempts(-1);
      jbcfBackup.setReconnectAttempts(-1);

      HornetQConnection connLive = (HornetQConnection) jbcfLive.createConnection();

      MyFailoverListener listener = new MyFailoverListener();

      connLive.setFailoverListener(listener);

      Session sessLive = connLive.createSession(false, Session.AUTO_ACKNOWLEDGE);

      ClientSession coreSessionLive = ((HornetQSession)sessLive).getCoreSession();

      SimpleString jmsQueueName = new SimpleString(HornetQDestination.JMS_QUEUE_ADDRESS_PREFIX + "myqueue");

      coreSessionLive.createQueue(jmsQueueName, jmsQueueName, null, true);

      Queue queue = sessLive.createQueue("myqueue");

      final int numMessages = 1000;

      MessageProducer producerLive = sessLive.createProducer(queue);

      for (int i = 0; i < numMessages; i++)
      {
         TextMessage tm = sessLive.createTextMessage("message" + i);

         producerLive.send(tm);
      }

      // Note we block on P send to make sure all messages get to server before failover

      JMSUtil.crash(liveService, coreSessionLive);
      Assert.assertEquals(FailoverEventType.FAILURE_DETECTED, listener.getEventTypeList().get(0));
      connLive.close();

      // Now recreate on backup

      Connection connBackup = jbcfBackup.createConnection();

      Session sessBackup = connBackup.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageConsumer consumerBackup = sessBackup.createConsumer(queue);

      connBackup.start();

      for (int i = 0; i < numMessages; i++)
      {
         TextMessage tm = (TextMessage)consumerBackup.receive(1000);

         Assert.assertNotNull(tm);

         Assert.assertEquals("message" + i, tm.getText());
      }

      TextMessage tm = (TextMessage)consumerBackup.receiveNoWait();
      Assert.assertEquals(FailoverEventType.FAILOVER_FAILED, listener.getEventTypeList().get(1));
      Assert.assertEquals("Expected 2 FailoverEvents to be triggered", 2, listener.getEventTypeList().size());
      Assert.assertNull(tm);

      connBackup.close();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      startServers();
   }

   /**
    * @throws Exception
    */
   protected void startServers() throws Exception
   {
      NodeManager nodeManager = new InVMNodeManager();
      backuptc = new TransportConfiguration(INVM_CONNECTOR_FACTORY, backupParams);
      livetc = new TransportConfiguration(INVM_CONNECTOR_FACTORY);

      liveAcceptortc = new TransportConfiguration(INVM_ACCEPTOR_FACTORY);

      backupAcceptortc = new TransportConfiguration(INVM_ACCEPTOR_FACTORY, backupParams);

      backupConf = createBasicConfig(0);

      backupConf.getAcceptorConfigurations().add(backupAcceptortc);
      backupConf.getConnectorConfigurations().put(livetc.getName(), livetc);
      backupConf.getConnectorConfigurations().put(backuptc.getName(), backuptc);
      basicClusterConnectionConfig(backupConf, backuptc.getName(), livetc.getName());

      backupConf.setSecurityEnabled(false);
      backupConf.setJournalType(getDefaultJournalType());
      backupParams.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      backupConf.getAcceptorConfigurations().add(new TransportConfiguration(INVM_ACCEPTOR_FACTORY, backupParams));
      backupConf.setBackup(true);
      backupConf.setSharedStore(true);
      backupConf.setBindingsDirectory(getBindingsDir());
      backupConf.setJournalMinFiles(2);
      backupConf.setJournalDirectory(getJournalDir());
      backupConf.setPagingDirectory(getPageDir());
      backupConf.setLargeMessagesDirectory(getLargeMessagesDir());
      backupConf.setPersistenceEnabled(true);
      backupService = new ServiceTestBase.InVMNodeManagerServer(backupConf, nodeManager);

      backupJMSService = new JMSServerManagerImpl(backupService);

      backupJMSService.setContext(ctx2);

      backupJMSService.getHornetQServer().setIdentity("JMSBackup");
      log.info("Starting backup");
      backupJMSService.start();

      liveConf = createBasicConfig(0);

      liveConf.setJournalDirectory(getJournalDir());
      liveConf.setBindingsDirectory(getBindingsDir());

      liveConf.setSecurityEnabled(false);
      liveConf.getAcceptorConfigurations().add(liveAcceptortc);
      basicClusterConnectionConfig(liveConf, livetc.getName());
      liveConf.setSharedStore(true);
      liveConf.setJournalType(getDefaultJournalType());
      liveConf.setBindingsDirectory(getBindingsDir());
      liveConf.setJournalMinFiles(2);
      liveConf.setJournalDirectory(getJournalDir());
      liveConf.setPagingDirectory(getPageDir());
      liveConf.setLargeMessagesDirectory(getLargeMessagesDir());
      liveConf.getConnectorConfigurations().put(livetc.getName(), livetc);
      liveConf.setPersistenceEnabled(true);
      liveService = new InVMNodeManagerServer(liveConf, nodeManager);

      liveJMSService = new JMSServerManagerImpl(liveService);

      liveJMSService.setContext(ctx1);

      liveJMSService.getHornetQServer().setIdentity("JMSLive");
      log.info("Starting life");

      liveJMSService.start();

      JMSUtil.waitForServer(backupService);
   }

   @Override
   protected void tearDown() throws Exception
   {
      backupJMSService.stop();

      liveJMSService.stop();

      Assert.assertEquals(0, InVMRegistry.instance.size());

      liveService = null;

      liveJMSService = null;

      backupJMSService = null;

      ctx1 = null;

      ctx2 = null;

      backupService = null;

      backupParams = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   
   private static class MyFailoverListener implements FailoverEventListener
   {
	private ArrayList<FailoverEventType> eventTypeList = new ArrayList<FailoverEventType>();
	
	public ArrayList<FailoverEventType> getEventTypeList()
	{
		return eventTypeList;
	}
	
	public void failoverEvent(FailoverEventType eventType) 
	{
		eventTypeList.add(eventType);
		log.info("Failover event just happen : "+eventType.toString());
	}
	
   }
}
