/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.jboss.messaging.tests.integration.jms.consumer;

import junit.framework.TestCase;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.jms.client.JBossSession;
import org.jboss.messaging.jms.server.impl.JMSServerManagerImpl;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.tests.integration.jms.management.NullInitialContext;
import org.jboss.messaging.util.SimpleString;

import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.MessageProducer;
import javax.jms.MessageConsumer;
import javax.jms.Message;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ConsumerTest extends TestCase
{
   private MessagingService service;

   private JMSServerManagerImpl serverManager;

   private JBossConnectionFactory cf;

   private static final String Q_NAME = "ConsumerTestQueue";

   private JBossQueue jBossQueue;

   @Override
   protected void setUp() throws Exception
   {
      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.getAcceptorConfigurations()
            .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory"));
      service = MessagingServiceImpl.newNullStorageMessagingServer(conf);
      service.start();
      serverManager = JMSServerManagerImpl.newJMSServerManagerImpl(service.getServer());
      serverManager.start();
      serverManager.setInitialContext(new NullInitialContext());
      serverManager.createQueue(Q_NAME, Q_NAME);
      cf = new JBossConnectionFactory(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
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
                                                             ClientSessionFactoryImpl.DEFAULT_BIG_MESSAGE_SIZE,
                                                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                                                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND,
                                                             true,
                                                             ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP,
                                                             ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS,
                                                             true);
   }

   @Override
   protected void tearDown() throws Exception
   {
      cf = null;
      if (service != null && service.isStarted())
      {
         try
         {
            service.stop();
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }
         service = null;

      }
   }

   public void testPreCommitAcks() throws Exception
   {
      Connection conn = cf.createConnection();
      Session session = conn.createSession(false, JBossSession.SERVER_ACKNOWLEDGE);
      jBossQueue = new JBossQueue(Q_NAME);
      MessageProducer producer = session.createProducer(jBossQueue);
      MessageConsumer consumer = session.createConsumer(jBossQueue);
      int noOfMessages = 100;
      for(int i = 0; i < noOfMessages; i++)
      {
         producer.send(session.createTextMessage("m" + i));
      }

      conn.start();
      for(int i = 0; i < noOfMessages; i++)
      {
         Message m = consumer.receive(500);
         assertNotNull(m);
      }
       // assert that all the messages are there and none have been acked
      SimpleString queueName = new SimpleString(JBossQueue.JMS_QUEUE_ADDRESS_PREFIX + Q_NAME);
      assertEquals(service.getServer().getPostOffice().getBinding(queueName).getQueue().getDeliveringCount(), 0);
      assertEquals(service.getServer().getPostOffice().getBinding(queueName).getQueue().getMessageCount(), 0);
      session.close();
   }
}
