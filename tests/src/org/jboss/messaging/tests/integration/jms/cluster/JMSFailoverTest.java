/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat
 * Middleware LLC, and individual contributors by the @authors tag. See the
 * copyright.txt in the distribution for a full listing of individual
 * contributors.
 * 
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 * 
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.tests.integration.jms.cluster;

import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.TestCase;

import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.client.impl.ClientSessionImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.invm.InVMRegistry;
import org.jboss.messaging.core.remoting.impl.invm.TransportConstants;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.jms.client.JBossSession;
import org.jboss.messaging.util.SimpleString;

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
public class JMSFailoverTest extends TestCase
{
   private static final Logger log = Logger.getLogger(JMSFailoverTest.class);

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private MessagingService liveService;

   private MessagingService backupService;

   private final Map<String, Object> backupParams = new HashMap<String, Object>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testAutomaticFailover() throws Exception
   {
      JBossConnectionFactory jbcf = new JBossConnectionFactory(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                               new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                          backupParams),
                                                               ClientSessionFactoryImpl.DEFAULT_PING_PERIOD,
                                                               ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT,
                                                               null,
                                                               ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE,
                                                               ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE,
                                                               ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE,
                                                               ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE,
                                                               ClientSessionFactoryImpl.DEFAULT_SEND_WINDOW_SIZE,
                                                               ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE,
                                                               ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                                                               ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND,
                                                               ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_PERSISTENT_SEND,
                                                               ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP_ID,
                                                               ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS);

      Connection conn = jbcf.createConnection();

      MyExceptionListener listener = new MyExceptionListener();

      conn.setExceptionListener(listener);

      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      ClientSession coreSession = ((JBossSession)sess).getCoreSession();

      RemotingConnection coreConn = ((ClientSessionImpl)coreSession).getConnection();

      SimpleString jmsQueueName = new SimpleString(JBossQueue.JMS_QUEUE_ADDRESS_PREFIX + "myqueue");

      coreSession.createQueue(jmsQueueName, jmsQueueName, null, false, false);

      Queue queue = sess.createQueue("myqueue");

      final int numMessages = 1000;

      MessageProducer producer = sess.createProducer(queue);

      MessageConsumer consumer = sess.createConsumer(queue);

      for (int i = 0; i < numMessages; i++)
      {
         TextMessage tm = sess.createTextMessage("message" + i);

         producer.send(tm);
      }

      conn.start();

      MessagingException me = new MessagingException(MessagingException.NOT_CONNECTED);

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

      JMSException je = listener.e;

      assertEquals(me, je.getCause());
   }

   public void testManualFailover() throws Exception
   {
      JBossConnectionFactory jbcfLive = new JBossConnectionFactory(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                   null,
                                                                   ClientSessionFactoryImpl.DEFAULT_PING_PERIOD,
                                                                   ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT,
                                                                   null,                                                                   
                                                                   ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE,
                                                                   ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE,
                                                                   ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE,
                                                                   ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE,
                                                                   ClientSessionFactoryImpl.DEFAULT_SEND_WINDOW_SIZE,
                                                                   ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE,
                                                                   ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                                                                   ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND,
                                                                   true,
                                                                   ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP_ID,
                                                                   ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS);

      JBossConnectionFactory jbcfBackup = new JBossConnectionFactory(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                backupParams),
                                                                     null,
                                                                     ClientSessionFactoryImpl.DEFAULT_PING_PERIOD,
                                                                     ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT,
                                                                     null,
                                                                     ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE,
                                                                     ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE,
                                                                     ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE,
                                                                     ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE,
                                                                     ClientSessionFactoryImpl.DEFAULT_SEND_WINDOW_SIZE,
                                                                     ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE,
                                                                     ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                                                                     ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND,
                                                                     ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_PERSISTENT_SEND,
                                                                     ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP_ID,
                                                                     ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS);

      Connection connLive = jbcfLive.createConnection();

      MyExceptionListener listener = new MyExceptionListener();

      connLive.setExceptionListener(listener);

      Session sessLive = connLive.createSession(false, Session.AUTO_ACKNOWLEDGE);

      ClientSession coreSessionLive = ((JBossSession)sessLive).getCoreSession();

      RemotingConnection coreConnLive = ((ClientSessionImpl)coreSessionLive).getConnection();

      SimpleString jmsQueueName = new SimpleString(JBossQueue.JMS_QUEUE_ADDRESS_PREFIX + "myqueue");

      coreSessionLive.createQueue(jmsQueueName, jmsQueueName, null, false, false);

      Queue queue = sessLive.createQueue("myqueue");

      final int numMessages = 1000;

      MessageProducer producerLive = sessLive.createProducer(queue);

      for (int i = 0; i < numMessages; i++)
      {
         TextMessage tm = sessLive.createTextMessage("message" + i);

         producerLive.send(tm);
      }
      
      // Note we block on NP send to make sure all messages get to server before failover

      MessagingException me = new MessagingException(MessagingException.NOT_CONNECTED);

      coreConnLive.fail(me);

      assertNotNull(listener.e);

      JMSException je = listener.e;

      assertEquals(me, je.getCause());

      connLive.close();

      // Now recreate on backup

      Connection connBackup = jbcfBackup.createConnection();

      log.info("creating session on backup");
      Session sessBackup = connBackup.createSession(false, Session.AUTO_ACKNOWLEDGE);

      log.info("created on backup");

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
      Configuration backupConf = new ConfigurationImpl();
      backupConf.setSecurityEnabled(false);
      backupParams.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      backupConf.getAcceptorConfigurations()
                .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory",
                                                backupParams));
      backupConf.setBackup(true);
      backupService = MessagingServiceImpl.newNullStorageMessagingServer(backupConf);
      backupService.start();

      Configuration liveConf = new ConfigurationImpl();
      liveConf.setSecurityEnabled(false);
      liveConf.getAcceptorConfigurations()
              .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory"));
      liveConf.setBackupConnectorConfiguration(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                          backupParams));
      liveService = MessagingServiceImpl.newNullStorageMessagingServer(liveConf);
      liveService.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      assertEquals(0, backupService.getServer().getRemotingService().getConnections().size());

      backupService.stop();

      assertEquals(0, liveService.getServer().getRemotingService().getConnections().size());

      liveService.stop();

      assertEquals(0, InVMRegistry.instance.size());
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
