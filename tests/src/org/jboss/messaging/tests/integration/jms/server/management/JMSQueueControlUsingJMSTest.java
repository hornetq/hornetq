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

package org.jboss.messaging.tests.integration.jms.server.management;

import static org.jboss.messaging.core.config.impl.ConfigurationImpl.DEFAULT_MANAGEMENT_ADDRESS;
import static org.jboss.messaging.tests.integration.management.ManagementControlHelper.createJMSQueueControl;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;
import static org.jboss.messaging.tests.util.RandomUtil.randomSimpleString;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.util.Map;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.naming.Context;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.management.ResourceNames;
import org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.jms.server.impl.JMSServerManagerImpl;
import org.jboss.messaging.jms.server.management.JMSQueueControlMBean;
import org.jboss.messaging.tests.integration.management.ManagementControlHelper;
import org.jboss.messaging.tests.integration.management.ManagementTestBase;
import org.jboss.messaging.tests.unit.util.InVMContext;
import org.jboss.messaging.utils.SimpleString;

/**
 * 
 * A JMSQueueControlUsingJMSTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class JMSQueueControlUsingJMSTest extends ManagementTestBase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private MessagingServer server;

   private JMSServerManagerImpl serverManager;

   protected JBossQueue queue;

   protected Context context;
   
   protected JMSMessagingProxy proxy;
   
   private QueueConnection connection;

   private QueueSession session;
   
   protected JBossQueue expiryQueue;
   
   protected JMSMessagingProxy expiryProxy;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testGetAttributes() throws Exception
   {     
      assertEquals(queue.getName(), proxy.retrieveAttributeValue("Name"));
      assertEquals(queue.getAddress(), proxy.retrieveAttributeValue("Address"));
      assertEquals(queue.isTemporary(), proxy.retrieveAttributeValue("Temporary"));
   }

   public void testGetXXXCount() throws Exception
   {      

      assertEquals(0, proxy.retrieveAttributeValue("MessageCount"));
      assertEquals(0, proxy.retrieveAttributeValue("ConsumerCount"));

      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      MessageConsumer consumer = JMSUtil.createConsumer(connection, queue);

      assertEquals(1, proxy.retrieveAttributeValue("ConsumerCount"));

      JMSUtil.sendMessages(queue, 2);

      assertEquals(2, proxy.retrieveAttributeValue("MessageCount"));
      assertEquals(2, proxy.retrieveAttributeValue("MessagesAdded"));

      connection.start();

      assertNotNull(consumer.receive(500));
      assertNotNull(consumer.receive(500));

      assertEquals(0, proxy.retrieveAttributeValue("MessageCount"));
      assertEquals(2, proxy.retrieveAttributeValue("MessagesAdded"));

      consumer.close();

      assertEquals(0, proxy.retrieveAttributeValue("ConsumerCount"));
      
      connection.close();
   }

   public void testRemoveMessage() throws Exception
   {
      

      assertEquals(0, proxy.retrieveAttributeValue("MessageCount"));

      JMSUtil.sendMessages(queue, 2);

      assertEquals(2, proxy.retrieveAttributeValue("MessageCount"));

      Object[] maps = (Object[])proxy.invokeOperation("listAllMessages", null);
      assertEquals(2, maps.length);

      // retrieve the first message info
      Map map = (Map)maps[0];
      Map props = (Map)map.get("properties");
      String messageID = (String)props.get("JMSMessageID");

      proxy.invokeOperation("removeMessage", messageID);

      assertEquals(1, proxy.retrieveAttributeValue("MessageCount"));
   }

   public void testRemoveMessageWithUnknownMessage() throws Exception
   {
      String unknownMessageID = randomString();
      
      assertEquals(0, proxy.retrieveAttributeValue("MessageCount"));

      try
      {
         proxy.invokeOperation("removeMessage", unknownMessageID);
         fail("should throw an exception is the message ID is unknown");
      }
      catch (Exception e)
      {
      }
   }

   public void testRemoveAllMessages() throws Exception
   {
      

      assertEquals(0, proxy.retrieveAttributeValue("MessageCount"));

      JMSUtil.sendMessages(queue, 2);

      assertEquals(2, proxy.retrieveAttributeValue("MessageCount"));

      proxy.invokeOperation("removeAllMessages");

      assertEquals(0, proxy.retrieveAttributeValue("MessageCount"));

      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      connection.start();
      
      MessageConsumer consumer = JMSUtil.createConsumer(connection, queue);
      assertNull(consumer.receive(500));
      
      connection.close();
   }

   public void testRemoveMatchingMessages() throws Exception
   {
      

      assertEquals(0, proxy.retrieveAttributeValue("MessageCount"));

      Connection conn = createConnection();

      Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = s.createProducer(queue);

      Message message = s.createMessage();
      message.setStringProperty("foo", "bar");
      producer.send(message);

      message = s.createMessage();
      message.setStringProperty("foo", "baz");
      producer.send(message);

      assertEquals(2, proxy.retrieveAttributeValue("MessageCount"));

      int removedMatchingMessagesCount = (Integer)proxy.invokeOperation("removeMatchingMessages", new Object[] {"foo = 'bar'"});
      assertEquals(1, removedMatchingMessagesCount);

      assertEquals(1, proxy.retrieveAttributeValue("MessageCount"));

      conn.start();
      MessageConsumer consumer = JMSUtil.createConsumer(conn, queue);
      Message msg = consumer.receive(500);
      assertNotNull(msg);
      assertEquals("baz", msg.getStringProperty("foo"));
      
      conn.close();
   }

   public void testChangeMessagePriority() throws Exception
   {
      

      JMSUtil.sendMessages(queue, 1);

      assertEquals(1, proxy.retrieveAttributeValue("MessageCount"));

      Object[] maps = (Object[])proxy.invokeOperation("listAllMessages");
      // retrieve the first message info
      Map map = (Map)maps[0];
      Map props = (Map)map.get("properties");
      String messageID = (String)props.get("JMSMessageID");
      int currentPriority = ((Long)map.get("JMSPriority")).intValue();
      int newPriority = 9;

      assertTrue(newPriority != currentPriority);

      proxy.invokeOperation("changeMessagePriority", messageID, newPriority);

      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      connection.start();
      MessageConsumer consumer = JMSUtil.createConsumer(connection, queue);
      Message message = consumer.receive(500);
      assertNotNull(message);
      assertEquals(newPriority, message.getJMSPriority());
      
      connection.close();
   }

   public void testChangeMessagePriorityWithInvalidPriority() throws Exception
   {
      byte invalidPriority = (byte)23;

      

      String[] messageIDs = JMSUtil.sendMessages(queue, 1);

      assertEquals(1, proxy.retrieveAttributeValue("MessageCount"));

      try
      {
         proxy.invokeOperation("changeMessagePriority", messageIDs[0], invalidPriority);
         fail("must throw an exception if the new priority is not a valid value");
      }
      catch (Exception e)
      {
      }

      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      connection.start();
      MessageConsumer consumer = JMSUtil.createConsumer(connection, queue);
      Message message = consumer.receive(500);
      assertNotNull(message);
      assertTrue(message.getJMSPriority() != invalidPriority);
      
      connection.close();
   }

   public void testChangeMessagePriorityWithUnknownMessageID() throws Exception
   {
      String unkownMessageID = randomString();

      

      try
      {
         proxy.invokeOperation("changeMessagePriority", unkownMessageID, 7);
         fail();
      }
      catch (Exception e)
      {
      }
   }

   public void testGetExpiryAddress() throws Exception
   {
      final SimpleString expiryAddress = randomSimpleString();

     
      assertNull(proxy.retrieveAttributeValue("ExpiryAddress"));

      server.getAddressSettingsRepository().addMatch(queue.getAddress(), new AddressSettings()
      {
         @Override
         public SimpleString getExpiryAddress()
         {
            return expiryAddress;
         }
      });

      assertEquals(expiryAddress.toString(), proxy.retrieveAttributeValue("ExpiryAddress"));
   }

   public void testSetExpiryAddress() throws Exception
   {
      final String expiryAddress = randomString();

      

      assertNull(proxy.retrieveAttributeValue("ExpiryAddress"));

      proxy.invokeOperation("setExpiryAddress", expiryAddress);
      assertEquals(expiryAddress, proxy.retrieveAttributeValue("ExpiryAddress"));
   }

   public void testExpireMessage() throws Exception
   {            
      proxy.invokeOperation("setExpiryAddress", expiryQueue.getAddress());

      String[] messageIDs = JMSUtil.sendMessages(queue, 1);

      assertEquals(1, proxy.retrieveAttributeValue("MessageCount"));
      assertEquals(0, expiryProxy.retrieveAttributeValue("MessageCount"));

      assertTrue((Boolean)proxy.invokeOperation("expireMessage", messageIDs[0]));

      assertEquals(0, proxy.retrieveAttributeValue("MessageCount"));
      assertEquals(1, expiryProxy.retrieveAttributeValue("MessageCount"));

      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      connection.start();

      MessageConsumer consumer = JMSUtil.createConsumer(connection, expiryQueue);
      Message message = consumer.receive(500);
      assertNotNull(message);
      assertEquals(messageIDs[0], message.getJMSMessageID());
      
      connection.close();
   }

   public void testExpireMessageWithUnknownMessageID() throws Exception
   {
      String unknownMessageID = randomString();

      

      try
      {
         proxy.invokeOperation("ExpireMessage", unknownMessageID);
         fail();
      }
      catch (Exception e)
      {
      }
   }

   public void testExpireMessagesWithFilter() throws Exception
   {
      String key = new String("key");
      long matchingValue = randomLong();
      long unmatchingValue = matchingValue + 1;
      String filter = key + " =" + matchingValue;

      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      // send on queue
      JMSUtil.sendMessageWithProperty(session, queue, key, matchingValue);
      JMSUtil.sendMessageWithProperty(session, queue, key, unmatchingValue);

      connection.close();

      
      assertEquals(2, proxy.retrieveAttributeValue("MessageCount"));

      int expiredMessagesCount = (Integer)proxy.invokeOperation("expireMessages", filter);
      assertEquals(1, expiredMessagesCount);
      assertEquals(1, proxy.retrieveAttributeValue("MessageCount"));

      // consume the unmatched message from queue
      JMSUtil.consumeMessages(1, queue);
   }

   public void testCountMessagesWithFilter() throws Exception
   {
      String key = "key";
      long matchingValue = randomLong();
      long unmatchingValue = matchingValue + 1;

      

      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      JMSUtil.sendMessageWithProperty(session, queue, key, matchingValue);
      JMSUtil.sendMessageWithProperty(session, queue, key, unmatchingValue);
      JMSUtil.sendMessageWithProperty(session, queue, key, matchingValue);

      assertEquals(3, proxy.retrieveAttributeValue("MessageCount"));

      assertEquals(2, proxy.invokeOperation("countMessages", key + " =" + matchingValue));
      assertEquals(1, proxy.invokeOperation("countMessages", key + " =" + unmatchingValue));

      session.close();
   }

   public void testGetDeadLetterAddress() throws Exception
   {
      final SimpleString deadLetterAddress = randomSimpleString();
      
      assertNull(proxy.retrieveAttributeValue("DeadLetterAddress"));

      server.getAddressSettingsRepository().addMatch(queue.getAddress(), new AddressSettings()
      {
         @Override
         public SimpleString getDeadLetterAddress()
         {
            return deadLetterAddress;
         }
      });

      assertEquals(deadLetterAddress.toString(), proxy.retrieveAttributeValue("DeadLetterAddress"));
   }

   public void testSetDeadLetterAddress() throws Exception
   {
      final String deadLetterAddress = randomString();
      
      assertNull(proxy.retrieveAttributeValue("DeadLetterAddress"));

      proxy.invokeOperation("setDeadLetterAddress", deadLetterAddress);
      assertEquals(deadLetterAddress, proxy.retrieveAttributeValue("DeadLetterAddress"));
   }

   public void testSendMessageToDLQ() throws Exception
   {
      String deadLetterQueue = randomString();
      serverManager.createQueue(deadLetterQueue, deadLetterQueue);
      JBossQueue dlq = new JBossQueue(deadLetterQueue);

      Connection conn = createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = sess.createProducer(queue);

      // send 2 messages on queue
      Message message = sess.createMessage();
      producer.send(message);
      producer.send(sess.createMessage());

      conn.close();

      
      JMSQueueControlMBean dlqControl = ManagementControlHelper.createJMSQueueControl(dlq, mbeanServer);

      assertEquals(2, proxy.retrieveAttributeValue("MessageCount"));
      assertEquals(0, dlqControl.getMessageCount());

      proxy.invokeOperation("setDeadLetterAddress", dlq.getAddress());

      boolean movedToDeadLetterAddress = (Boolean)proxy.invokeOperation("sendMessageToDLQ", message.getJMSMessageID());
      assertTrue(movedToDeadLetterAddress);
      assertEquals(1, proxy.retrieveAttributeValue("MessageCount"));
      assertEquals(1, dlqControl.getMessageCount());

      // check there is a single message to consume from queue
      JMSUtil.consumeMessages(1, queue);

      // check there is a single message to consume from deadletter queue
      JMSUtil.consumeMessages(1, dlq);

      serverManager.destroyQueue(deadLetterQueue);
   }

   public void testSendMessageToDLQWithUnknownMessageID() throws Exception
   {
      String unknownMessageID = randomString();

      

      try
      {
         proxy.invokeOperation("sendMessageToDLQ", unknownMessageID);
         fail();
      }
      catch (Exception e)
      {
      }

   }

   public void testMoveAllMessages() throws Exception
   {
      String otherQueueName = randomString();

      serverManager.createQueue(otherQueueName, otherQueueName);
      JBossQueue otherQueue = new JBossQueue(otherQueueName);

      // send on queue
      JMSUtil.sendMessages(queue, 2);

      
      assertEquals(2, proxy.retrieveAttributeValue("MessageCount"));

      // moved all messages to otherQueue
      int movedMessagesCount = (Integer)proxy.invokeOperation("moveAllMessages", otherQueueName);
      assertEquals(2, movedMessagesCount);
      assertEquals(0, proxy.retrieveAttributeValue("MessageCount"));

      // check there is no message to consume from queue
      JMSUtil.consumeMessages(0, queue);

      // consume the message from otherQueue
      JMSUtil.consumeMessages(2, otherQueue);

      serverManager.destroyQueue(otherQueueName);
   }

   public void testMoveAllMessagesToUknownQueue() throws Exception
   {
      String unknownQueue = randomString();

      
      try
      {
         proxy.invokeOperation("moveAllMessages", unknownQueue);
         fail();
      }
      catch (Exception e)
      {
      }
   }

   public void testMoveMatchingMessages() throws Exception
   {
      String key = "key";
      long matchingValue = randomLong();
      long unmatchingValue = matchingValue + 1;
      String filter = "key = " + matchingValue;
      String otherQueueName = randomString();

      serverManager.createQueue(otherQueueName, otherQueueName);
      JBossQueue otherQueue = new JBossQueue(otherQueueName);

      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      // send on queue
      JMSUtil.sendMessageWithProperty(session, queue, key, matchingValue);
      JMSUtil.sendMessageWithProperty(session, queue, key, unmatchingValue);

      
      assertEquals(2, proxy.retrieveAttributeValue("MessageCount"));

      // moved matching messages to otherQueue
      int movedMessagesCount = (Integer)proxy.invokeOperation("moveMatchingMessages", filter, otherQueueName);
      assertEquals(1, movedMessagesCount);
      assertEquals(1, proxy.retrieveAttributeValue("MessageCount"));

      connection.start();
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(500);
      assertNotNull(message);
      assertEquals(unmatchingValue, message.getLongProperty(key));
      assertNull(consumer.receive(500));

      JMSUtil.consumeMessages(1, otherQueue);

      serverManager.destroyQueue(otherQueueName);

      connection.close();
   }

   public void testMoveMessage() throws Exception
   {
      String otherQueueName = randomString();

      serverManager.createQueue(otherQueueName, otherQueueName);
      JBossQueue otherQueue = new JBossQueue(otherQueueName);

      String[] messageIDs = JMSUtil.sendMessages(queue, 1);
      
      
      assertEquals(1, proxy.retrieveAttributeValue("MessageCount"));

      boolean moved = (Boolean)proxy.invokeOperation("moveMessage", messageIDs[0], otherQueueName);
      assertTrue(moved);
      assertEquals(0, proxy.retrieveAttributeValue("MessageCount"));

      JMSUtil.consumeMessages(0, queue);
      JMSUtil.consumeMessages(1, otherQueue);

      serverManager.destroyQueue(otherQueueName);
   }
   
   public void testMoveMessageWithUnknownMessageID() throws Exception
   {
      String unknownMessageID = randomString();
      String otherQueueName = randomString();

      serverManager.createQueue(otherQueueName, otherQueueName);

      
      assertEquals(0, proxy.retrieveAttributeValue("MessageCount"));

      try
      {
         proxy.invokeOperation("moveMessage", unknownMessageID, otherQueueName);
         fail();
      }
      catch (Exception e)
      {
      }

      serverManager.destroyQueue(otherQueueName);
   }
   
   public void testMoveMessageToUnknownQueue() throws Exception
   {
      String unknwonQueue = randomString();

      String[] messageIDs = JMSUtil.sendMessages(queue, 1);
      
      
      assertEquals(1, proxy.retrieveAttributeValue("MessageCount"));

      try
      {
         proxy.invokeOperation("moveMessage", messageIDs[0], unknwonQueue);
         fail();
      }
      catch (Exception e)
      {
      }

      JMSUtil.consumeMessages(1, queue);
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      server = Messaging.newMessagingServer(conf, mbeanServer, false);
      server.start();

      serverManager = new JMSServerManagerImpl(server);
      context = new InVMContext();
      serverManager.setContext(context);
      serverManager.start();
      serverManager.activated();

      String queueName = randomString();
      serverManager.createQueue(queueName, queueName);
      queue = new JBossQueue(queueName);
      
      JBossConnectionFactory cf = new JBossConnectionFactory(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      connection = cf.createQueueConnection();
      session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
      connection.start();
      
      JBossQueue managementQueue = new JBossQueue(DEFAULT_MANAGEMENT_ADDRESS.toString(),
                                                  DEFAULT_MANAGEMENT_ADDRESS.toString());
      proxy = new JMSMessagingProxy(session,
                                    managementQueue,
                                    ResourceNames.JMS_QUEUE + queue.getQueueName());
      
      String expiryQueueName = randomString();
      serverManager.createQueue(expiryQueueName, expiryQueueName);
      expiryQueue = new JBossQueue(expiryQueueName);
      
      
      expiryProxy = new JMSMessagingProxy(session,
                                          managementQueue,
                                          ResourceNames.JMS_QUEUE + expiryQueue.getQueueName());
                                   
   }

   @Override
   protected void tearDown() throws Exception
   {
      connection.close();
      
      server.stop();

      super.tearDown();
   }

   // Private -------------------------------------------------------

   private Connection createConnection() throws JMSException
   {
      JBossConnectionFactory cf = new JBossConnectionFactory(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      
      cf.setBlockOnPersistentSend(true);
                
      return cf.createConnection();
   }

   // Inner classes -------------------------------------------------

}