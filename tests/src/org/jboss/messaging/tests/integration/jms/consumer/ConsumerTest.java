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

import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONNECTION_TTL;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MAX_RETRIES_AFTER_FAILOVER;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MAX_RETRIES_BEFORE_FAILOVER;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PING_PERIOD;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL_MULTIPLIER;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_SEND_WINDOW_SIZE;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.TestCase;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.jms.client.JBossSession;
import org.jboss.messaging.jms.server.impl.JMSServerManagerImpl;
import org.jboss.messaging.tests.integration.jms.management.NullInitialContext;
import org.jboss.messaging.util.SimpleString;

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
      service = MessagingServiceImpl.newNullStorageMessagingService(conf);
      service.start();
      serverManager = JMSServerManagerImpl.newJMSServerManagerImpl(service.getServer());
      serverManager.start();
      serverManager.setInitialContext(new NullInitialContext());
      serverManager.createQueue(Q_NAME, Q_NAME);
      cf = new JBossConnectionFactory(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                      null,
                                      DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                                      DEFAULT_PING_PERIOD,
                                      DEFAULT_CONNECTION_TTL,
                                      DEFAULT_CALL_TIMEOUT,
                                      null,
                                      DEFAULT_ACK_BATCH_SIZE,
                                      DEFAULT_ACK_BATCH_SIZE,
                                      DEFAULT_CONSUMER_WINDOW_SIZE,
                                      DEFAULT_CONSUMER_MAX_RATE,
                                      DEFAULT_SEND_WINDOW_SIZE,
                                      DEFAULT_PRODUCER_MAX_RATE,
                                      DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                      DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                                      DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND,
                                      true,
                                      DEFAULT_AUTO_GROUP,
                                      DEFAULT_MAX_CONNECTIONS,
                                      true,                            
                                      DEFAULT_RETRY_INTERVAL,
                                      DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                                      DEFAULT_MAX_RETRIES_BEFORE_FAILOVER,
                                      DEFAULT_MAX_RETRIES_AFTER_FAILOVER);
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
      for (int i = 0; i < noOfMessages; i++)
      {
         producer.send(session.createTextMessage("m" + i));
      }

      conn.start();
      for (int i = 0; i < noOfMessages; i++)
      {
         Message m = consumer.receive(500);
         assertNotNull(m);
      }
      // assert that all the messages are there and none have been acked
      SimpleString queueName = new SimpleString(JBossQueue.JMS_QUEUE_ADDRESS_PREFIX + Q_NAME);
      assertEquals(0, service.getServer().getPostOffice().getBinding(queueName).getQueue().getDeliveringCount());
      assertEquals(0, service.getServer().getPostOffice().getBinding(queueName).getQueue().getMessageCount());
      session.close();
   }
}
