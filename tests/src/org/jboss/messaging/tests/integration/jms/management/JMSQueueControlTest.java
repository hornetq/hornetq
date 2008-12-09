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

package org.jboss.messaging.tests.integration.jms.management;

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
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL_MULTIPLIER;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_SEND_WINDOW_SIZE;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.lang.management.ManagementFactory;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.management.MBeanServerInvocationHandler;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import junit.framework.TestCase;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.jms.server.impl.JMSServerManagerImpl;
import org.jboss.messaging.jms.server.management.JMSQueueControlMBean;
import org.jboss.messaging.jms.server.management.impl.JMSManagementServiceImpl;

/**
 * A QueueControlTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 14 nov. 2008 13:35:10
 *
 *
 */
public class JMSQueueControlTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private MessagingService service;

   private JMSServerManagerImpl serverManager;

   private Queue queue;

   // Static --------------------------------------------------------

   private static JMSQueueControlMBean createQueueControl(Queue queue) throws Exception
   {
      JMSQueueControlMBean queueControl = (JMSQueueControlMBean)MBeanServerInvocationHandler.newProxyInstance(ManagementFactory.getPlatformMBeanServer(),
                                                                                                              JMSManagementServiceImpl.getJMSQueueObjectName(queue.getQueueName()),
                                                                                                              JMSQueueControlMBean.class,
                                                                                                              false);
      return queueControl;
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testGetXXXCount() throws Exception
   {
      JMSQueueControlMBean queueControl = createQueueControl(queue);

      assertEquals(0, queueControl.getMessageCount());
      assertEquals(0, queueControl.getConsumerCount());

      MessageConsumer consumer = JMSUtil.createConsumer(queue, true);

      assertEquals(1, queueControl.getConsumerCount());

      JMSUtil.sendMessages(queue, 2);

      Thread.sleep(100);

      assertEquals(2, queueControl.getMessageCount());
      assertEquals(2, queueControl.getMessagesAdded());

      assertNotNull(consumer.receive(500));
      assertNotNull(consumer.receive(500));

      Thread.sleep(100);

      assertEquals(0, queueControl.getMessageCount());
      assertEquals(2, queueControl.getMessagesAdded());

      consumer.close();

      assertEquals(0, queueControl.getConsumerCount());
   }

   public void testRemoveMessage() throws Exception
   {
      JMSQueueControlMBean queueControl = createQueueControl(queue);
      assertEquals(0, queueControl.getMessageCount());

      JMSUtil.sendMessages(queue, 2);

      assertEquals(2, queueControl.getMessageCount());

      TabularData data = queueControl.listAllMessages();
      assertEquals(2, data.size());

      // retrieve the first message info
      CompositeData compositeData = (CompositeData)data.values().iterator().next();
      String messageID = (String)compositeData.get("JMSMessageID");

      queueControl.removeMessage(messageID);

      assertEquals(1, queueControl.getMessageCount());
   }

   public void testRemoveAllMessages() throws Exception
   {
      JMSQueueControlMBean queueControl = createQueueControl(queue);
      assertEquals(0, queueControl.getMessageCount());

      JMSUtil.sendMessages(queue, 2);

      assertEquals(2, queueControl.getMessageCount());

      queueControl.removeAllMessages();

      assertEquals(0, queueControl.getMessageCount());

      MessageConsumer consumer = JMSUtil.createConsumer(queue, true);
      assertNull(consumer.receive(500));
   }

   public void testRemoveMatchingMessages() throws Exception
   {
      JMSQueueControlMBean queueControl = createQueueControl(queue);
      assertEquals(0, queueControl.getMessageCount());

      JBossConnectionFactory cf = new JBossConnectionFactory(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
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
                                                             DEFAULT_PRE_ACKNOWLEDGE,                                                        
                                                             DEFAULT_RETRY_INTERVAL,
                                                             DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                                                             DEFAULT_MAX_RETRIES_BEFORE_FAILOVER,
                                                             DEFAULT_MAX_RETRIES_AFTER_FAILOVER);

      Connection conn = cf.createConnection();

      Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = s.createProducer(queue);

      Message message = s.createMessage();
      message.setStringProperty("foo", "bar");
      producer.send(message);

      message = s.createMessage();
      message.setStringProperty("foo", "baz");
      producer.send(message);

      assertEquals(2, queueControl.getMessageCount());

      int removedMatchingMessagesCount = queueControl.removeMatchingMessages("foo = 'bar'");
      assertEquals(1, removedMatchingMessagesCount);

      assertEquals(1, queueControl.getMessageCount());

      MessageConsumer consumer = JMSUtil.createConsumer(queue, true);
      Message msg = consumer.receive(500);
      assertNotNull(msg);
      assertEquals("baz", msg.getStringProperty("foo"));
   }

   public void testChangeMessagePriority() throws Exception
   {
      JMSQueueControlMBean queueControl = createQueueControl(queue);

      JMSUtil.sendMessages(queue, 1);

      assertEquals(1, queueControl.getMessageCount());

      TabularData data = queueControl.listAllMessages();
      // retrieve the first message info
      CompositeData compositeData = (CompositeData)data.values().iterator().next();
      String messageID = (String)compositeData.get("JMSMessageID");
      int currentPriority = (Integer)compositeData.get("JMSPriority");
      int newPriority = 9;

      assertTrue(newPriority != currentPriority);

      queueControl.changeMessagePriority(messageID, newPriority);

      MessageConsumer consumer = JMSUtil.createConsumer(queue, true);
      Message message = consumer.receive(500);
      assertNotNull(message);
      assertEquals(newPriority, message.getJMSPriority());
   }

   public void testExpireMessage() throws Exception
   {
      JMSQueueControlMBean queueControl = createQueueControl(queue);
      String expiryQueueName = randomString();
      JBossQueue expiryQueue = new JBossQueue(expiryQueueName);
      serverManager.createQueue(expiryQueueName, expiryQueueName);
      queueControl.setExpiryAddress(expiryQueue.getAddress());
      JMSQueueControlMBean expiryQueueControl = createQueueControl(expiryQueue);

      JMSUtil.sendMessages(queue, 1);

      assertEquals(1, queueControl.getMessageCount());
      assertEquals(0, expiryQueueControl.getMessageCount());

      TabularData data = queueControl.listAllMessages();
      // retrieve the first message info
      CompositeData compositeData = (CompositeData)data.values().iterator().next();
      String messageID = (String)compositeData.get("JMSMessageID");

      assertTrue(queueControl.expireMessage(messageID));

      assertEquals(0, queueControl.getMessageCount());
      assertEquals(1, expiryQueueControl.getMessageCount());

      MessageConsumer consumer = JMSUtil.createConsumer(expiryQueue, true);
      Message message = consumer.receive(500);
      assertNotNull(message);
      assertEquals(messageID, message.getJMSMessageID());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

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

      String queueName = randomString();
      serverManager.createQueue(queueName, queueName);
      queue = new JBossQueue(queueName);
   }

   @Override
   protected void tearDown() throws Exception
   {
      service.stop();

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}