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

import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.invm.InVMRegistry;
import org.hornetq.core.remoting.impl.invm.TransportConstants;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.jms.HornetQQueue;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.client.HornetQSession;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.SimpleString;

/**
 * 
 * A JMSFailoverTest
 *
 * A simple test to test failover when using the JMS API.
 * Most of the failover tests are done on the Core API.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 7 Nov 2008 11:13:39
 *
 *
 */
public class JMSFailoverTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(JMSFailoverTest.class);

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private HornetQServer liveService;

   private HornetQServer backupService;

   private Map<String, Object> backupParams = new HashMap<String, Object>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testAutomaticFailover() throws Exception
   {
      HornetQConnectionFactory jbcf = new HornetQConnectionFactory(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                   new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                              backupParams));

      jbcf.setBlockOnPersistentSend(true);
      jbcf.setBlockOnNonPersistentSend(true);

      Connection conn = jbcf.createConnection();

      MyExceptionListener listener = new MyExceptionListener();

      conn.setExceptionListener(listener);

      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      ClientSession coreSession = ((HornetQSession)sess).getCoreSession();

      RemotingConnection coreConn = ((ClientSessionInternal)coreSession).getConnection();

      SimpleString jmsQueueName = new SimpleString(HornetQQueue.JMS_QUEUE_ADDRESS_PREFIX + "myqueue");

      coreSession.createQueue(jmsQueueName, jmsQueueName, null, true);

      Queue queue = sess.createQueue("myqueue");

      final int numMessages = 1000;

      MessageProducer producer = sess.createProducer(queue);

      producer.setDeliveryMode(DeliveryMode.PERSISTENT);

      MessageConsumer consumer = sess.createConsumer(queue);

      for (int i = 0; i < numMessages; i++)
      {
         TextMessage tm = sess.createTextMessage("message" + i);

         producer.send(tm);
      }

      conn.start();

      HornetQException me = new HornetQException(HornetQException.NOT_CONNECTED);

      coreConn.fail(me);

      for (int i = 0; i < numMessages; i++)
      {
         TextMessage tm = (TextMessage)consumer.receive(1000);

         assertNotNull(tm);

         assertEquals("message" + i, tm.getText());
      }

      TextMessage tm = (TextMessage)consumer.receive(1000);

      assertNull(tm);

      conn.close();

      assertNotNull(listener.e);
      
      assertTrue(me == listener.e.getCause());
   }

   public void testManualFailover() throws Exception
   {
      HornetQConnectionFactory jbcfLive = new HornetQConnectionFactory(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));

      jbcfLive.setBlockOnNonPersistentSend(true);
      jbcfLive.setBlockOnPersistentSend(true);

      HornetQConnectionFactory jbcfBackup = new HornetQConnectionFactory(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                    backupParams));
      jbcfBackup.setBlockOnNonPersistentSend(true);
      jbcfBackup.setBlockOnPersistentSend(true);

      Connection connLive = jbcfLive.createConnection();

      MyExceptionListener listener = new MyExceptionListener();

      connLive.setExceptionListener(listener);

      Session sessLive = connLive.createSession(false, Session.AUTO_ACKNOWLEDGE);

      ClientSession coreSessionLive = ((HornetQSession)sessLive).getCoreSession();

      RemotingConnection coreConnLive = ((ClientSessionInternal)coreSessionLive).getConnection();

      SimpleString jmsQueueName = new SimpleString(HornetQQueue.JMS_QUEUE_ADDRESS_PREFIX + "myqueue");

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

      HornetQException me = new HornetQException(HornetQException.NOT_CONNECTED);

      coreConnLive.fail(me);

      assertNotNull(listener.e);

      JMSException je = listener.e;

      assertEquals(me, je.getCause());

      connLive.close();

      // Now recreate on backup

      Connection connBackup = jbcfBackup.createConnection();

      Session sessBackup = connBackup.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageConsumer consumerBackup = sessBackup.createConsumer(queue);

      connBackup.start();

      for (int i = 0; i < numMessages; i++)
      {
         TextMessage tm = (TextMessage)consumerBackup.receive(1000);

         assertNotNull(tm);

         assertEquals("message" + i, tm.getText());
      }

      TextMessage tm = (TextMessage)consumerBackup.receive(1000);

      assertNull(tm);

      connBackup.close();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      Configuration backupConf = new ConfigurationImpl();
      backupConf.setSecurityEnabled(false);
      backupParams.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      backupConf.getAcceptorConfigurations()
                .add(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory", backupParams));
      backupConf.setBackup(true);
      backupConf.setSharedStore(true);
      backupConf.setBindingsDirectory(getBindingsDir());
      backupConf.setJournalMinFiles(2);
      backupConf.setJournalDirectory(getJournalDir());
      backupConf.setPagingDirectory(getPageDir());
      backupConf.setLargeMessagesDirectory(getLargeMessagesDir());
      backupService = HornetQ.newHornetQServer(backupConf, true);
      backupService.start();

      Configuration liveConf = new ConfigurationImpl();
      liveConf.setSecurityEnabled(false);
      liveConf.getAcceptorConfigurations()
              .add(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory"));
      liveConf.setSharedStore(true);
      liveConf.setBindingsDirectory(getBindingsDir());
      liveConf.setJournalMinFiles(2);
      liveConf.setJournalDirectory(getJournalDir());
      liveConf.setPagingDirectory(getPageDir());
      liveConf.setLargeMessagesDirectory(getLargeMessagesDir());

      liveService = HornetQ.newHornetQServer(liveConf, true);
      liveService.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      backupService.stop();

      liveService.stop();

      assertEquals(0, InVMRegistry.instance.size());

      liveService = null;

      backupService = null;

      backupParams = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private static class MyExceptionListener implements ExceptionListener
   {
      volatile JMSException e;

      public void onException(final JMSException e)
      {
         this.e = e;
      }
   }

}
