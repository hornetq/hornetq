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

package org.hornetq.tests.integration.jms.server.management;
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.naming.Context;

import org.junit.Assert;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.api.jms.management.JMSQueueControl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.client.HornetQDestination;
import org.hornetq.jms.client.HornetQQueue;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.tests.integration.management.ManagementControlHelper;
import org.hornetq.tests.integration.management.ManagementTestBase;
import org.hornetq.tests.unit.util.InVMContext;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.utils.UUIDGenerator;
import org.hornetq.utils.json.JSONArray;

/**
 * A QueueControlTest
 *
 * on this testContaining WithRealData will use real data on the journals
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * Created 14 nov. 2008 13:35:10
 *
 *
 */
public class JMSQueueControlTest extends ManagementTestBase
{
   private HornetQServer server;

   private JMSServerManagerImpl serverManager;

   protected HornetQQueue queue;

   protected String queueName;

   protected Context context;

   @Test
   public void testGetAttributes() throws Exception
   {
      JMSQueueControl queueControl = createManagementControl();

      Assert.assertEquals(queue.getName(), queueControl.getName());
      Assert.assertEquals(queue.getAddress(), queueControl.getAddress());
      Assert.assertEquals(queue.isTemporary(), queueControl.isTemporary());
   }

   @Test
   public void testGetXXXCount() throws Exception
   {
      JMSQueueControl queueControl = createManagementControl();

      Assert.assertEquals(0, queueControl.getMessageCount());
      Assert.assertEquals(0, queueControl.getConsumerCount());

      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      MessageConsumer consumer = JMSUtil.createConsumer(connection, queue);

      Assert.assertEquals(1, queueControl.getConsumerCount());

      JSONArray jsonArray = new JSONArray(queueControl.listConsumersAsJSON());

      assertEquals(1, jsonArray.length());

      JMSUtil.sendMessages(queue, 2);

      Assert.assertEquals(2, queueControl.getMessageCount());
      Assert.assertEquals(2, queueControl.getMessagesAdded());

      connection.start();

      Assert.assertNotNull(consumer.receive(500));
      Assert.assertNotNull(consumer.receive(500));

      Assert.assertEquals(0, queueControl.getMessageCount());
      Assert.assertEquals(2, queueControl.getMessagesAdded());

      consumer.close();

      Assert.assertEquals(0, queueControl.getConsumerCount());

      connection.close();
   }

   @Test
   public void testListMessagesWithNullFilter() throws Exception
   {
      JMSQueueControl queueControl = createManagementControl();

      Assert.assertEquals(0, queueControl.getMessageCount());

      String[] ids = JMSUtil.sendMessages(queue, 2);

      Assert.assertEquals(2, queueControl.getMessageCount());

      Map<String, Object>[] data = queueControl.listMessages(null);
      Assert.assertEquals(2, data.length);
      System.out.println(data[0].keySet());
      Assert.assertEquals(ids[0], data[0].get("JMSMessageID").toString());
      Assert.assertEquals(ids[1], data[1].get("JMSMessageID").toString());

      JMSUtil.consumeMessages(2, queue);

      data = queueControl.listMessages(null);
      Assert.assertEquals(0, data.length);
   }

   @Test
   public void testListMessagesAsJSONWithNullFilter() throws Exception
   {
      JMSQueueControl queueControl = createManagementControl();

      Assert.assertEquals(0, queueControl.getMessageCount());

      String[] ids = JMSUtil.sendMessages(queue, 2);

      Assert.assertEquals(2, queueControl.getMessageCount());

      String jsonString = queueControl.listMessagesAsJSON(null);
      Assert.assertNotNull(jsonString);
      JSONArray array = new JSONArray(jsonString);
      Assert.assertEquals(2, array.length());
      Assert.assertEquals(ids[0], array.getJSONObject(0).get("JMSMessageID"));
      Assert.assertEquals(ids[1], array.getJSONObject(1).get("JMSMessageID"));

      JMSUtil.consumeMessages(2, queue);

      jsonString = queueControl.listMessagesAsJSON(null);
      Assert.assertNotNull(jsonString);
      array = new JSONArray(jsonString);
      Assert.assertEquals(0, array.length());
   }

   @Test
   public void testRemoveMessage() throws Exception
   {
      JMSQueueControl queueControl = createManagementControl();

      Assert.assertEquals(0, queueControl.getMessageCount());

      JMSUtil.sendMessages(queue, 2);

      Assert.assertEquals(2, queueControl.getMessageCount());

      Map<String, Object>[] data = queueControl.listMessages(null);
      Assert.assertEquals(2, data.length);

      System.out.println(data[0]);
      // retrieve the first message info
      Set<String> keySet = data[0].keySet();
      Iterator<String> it = keySet.iterator();
      while (it.hasNext())
      {
         System.out.println(it.next());
      }
      String messageID = (String)data[0].get("JMSMessageID");

      queueControl.removeMessage(messageID.toString());

      Assert.assertEquals(1, queueControl.getMessageCount());
   }

   @Test
   public void testRemoveMessageWithUnknownMessage() throws Exception
   {
      String unknownMessageID = RandomUtil.randomString();

      JMSQueueControl queueControl = createManagementControl();

      Assert.assertEquals(0, queueControl.getMessageCount());

      try
      {
         queueControl.removeMessage(unknownMessageID);
         Assert.fail("should throw an exception is the message ID is unknown");
      }
      catch (Exception e)
      {
      }
   }

   @Test
   public void testRemoveAllMessages() throws Exception
   {
      JMSQueueControl queueControl = createManagementControl();

      Assert.assertEquals(0, queueControl.getMessageCount());

      JMSUtil.sendMessages(queue, 2);

      Assert.assertEquals(2, queueControl.getMessageCount());

      queueControl.removeMessages(null);

      Assert.assertEquals(0, queueControl.getMessageCount());

      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      connection.start();

      MessageConsumer consumer = JMSUtil.createConsumer(connection, queue);
      Assert.assertNull(consumer.receiveNoWait());

      connection.close();
   }

   @Test
   public void testRemoveMatchingMessages() throws Exception
   {
      JMSQueueControl queueControl = createManagementControl();

      Assert.assertEquals(0, queueControl.getMessageCount());

      Connection conn = createConnection();

      Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = s.createProducer(queue);

      Message message = s.createMessage();
      message.setStringProperty("foo", "bar");
      producer.send(message);

      message = s.createMessage();
      message.setStringProperty("foo", "baz");
      producer.send(message);

      Assert.assertEquals(2, queueControl.getMessageCount());

      int removedMatchingMessagesCount = queueControl.removeMessages("foo = 'bar'");
      Assert.assertEquals(1, removedMatchingMessagesCount);

      Assert.assertEquals(1, queueControl.getMessageCount());

      conn.start();
      MessageConsumer consumer = JMSUtil.createConsumer(conn, queue);
      Message msg = consumer.receive(500);
      Assert.assertNotNull(msg);
      Assert.assertEquals("baz", msg.getStringProperty("foo"));

      conn.close();
   }

   @Test
   public void testChangeMessagePriority() throws Exception
   {
      JMSQueueControl queueControl = createManagementControl();

      JMSUtil.sendMessages(queue, 1);

      Assert.assertEquals(1, queueControl.getMessageCount());

      Map<String, Object>[] data = queueControl.listMessages(null);
      // retrieve the first message info
      String messageID = (String)data[0].get("JMSMessageID");
      int currentPriority = ((Number)data[0].get("JMSPriority")).intValue();
      int newPriority = 9;

      Assert.assertTrue(newPriority != currentPriority);

      queueControl.changeMessagePriority(messageID.toString(), newPriority);

      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      connection.start();
      MessageConsumer consumer = JMSUtil.createConsumer(connection, queue);
      Message message = consumer.receive(500);
      Assert.assertNotNull(message);
      Assert.assertEquals(newPriority, message.getJMSPriority());

      connection.close();
   }

   @Test
   public void testChangeMessagePriorityWithInvalidPriority() throws Exception
   {
      byte invalidPriority = (byte)23;

      JMSQueueControl queueControl = createManagementControl();

      String[] messageIDs = JMSUtil.sendMessages(queue, 1);

      Assert.assertEquals(1, queueControl.getMessageCount());

      try
      {
         queueControl.changeMessagePriority(messageIDs[0], invalidPriority);
         Assert.fail("must throw an exception if the new priority is not a valid value");
      }
      catch (Exception e)
      {
      }

      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      connection.start();
      MessageConsumer consumer = JMSUtil.createConsumer(connection, queue);
      Message message = consumer.receive(500);
      Assert.assertNotNull(message);
      Assert.assertTrue(message.getJMSPriority() != invalidPriority);

      connection.close();
   }

   @Test
   public void testChangeMessagePriorityWithUnknownMessageID() throws Exception
   {
      String unkownMessageID = RandomUtil.randomString();

      JMSQueueControl queueControl = createManagementControl();

      try
      {
         queueControl.changeMessagePriority(unkownMessageID, 7);
         Assert.fail();
      }
      catch (Exception e)
      {
      }
   }

   @Test
   public void testChangeMessagesPriority() throws Exception
   {
      String key = "key";
      long matchingValue = RandomUtil.randomLong();
      long unmatchingValue = matchingValue + 1;
      String filter = "key = " + matchingValue;
      int newPriority = 9;

      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      // send on queue
      Message msg_1 = JMSUtil.sendMessageWithProperty(session, queue, key, matchingValue);
      Message msg_2 = JMSUtil.sendMessageWithProperty(session, queue, key, unmatchingValue);

      JMSQueueControl queueControl = createManagementControl();
      Assert.assertEquals(2, queueControl.getMessageCount());

      int changedMessagesCount = queueControl.changeMessagesPriority(filter, newPriority);
      Assert.assertEquals(1, changedMessagesCount);
      Assert.assertEquals(2, queueControl.getMessageCount());

      connection.start();
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(500);
      Assert.assertNotNull(message);
      Assert.assertEquals(msg_1.getJMSMessageID(), message.getJMSMessageID());
      Assert.assertEquals(9, message.getJMSPriority());
      Assert.assertEquals(matchingValue, message.getLongProperty(key));

      message = consumer.receive(500);
      Assert.assertNotNull(message);
      Assert.assertEquals(msg_2.getJMSMessageID(), message.getJMSMessageID());
      Assert.assertEquals(unmatchingValue, message.getLongProperty(key));

      Assert.assertNull(consumer.receiveNoWait());

      connection.close();
   }

   @Test
   public void testGetExpiryAddress() throws Exception
   {
      final SimpleString expiryAddress = RandomUtil.randomSimpleString();

      JMSQueueControl queueControl = createManagementControl();

      Assert.assertNull(queueControl.getExpiryAddress());

      server.getAddressSettingsRepository().addMatch(queue.getAddress(), new AddressSettings()
      {
         private static final long serialVersionUID = -7668739851356971411L;

         @Override
         public SimpleString getExpiryAddress()
         {
            return expiryAddress;
         }
      });

      Assert.assertEquals(expiryAddress.toString(), queueControl.getExpiryAddress());
   }

   @Test
   public void testSetExpiryAddress() throws Exception
   {
      final String expiryAddress = RandomUtil.randomString();

      JMSQueueControl queueControl = createManagementControl();

      Assert.assertNull(queueControl.getExpiryAddress());

      queueControl.setExpiryAddress(expiryAddress);
      Assert.assertEquals(expiryAddress, queueControl.getExpiryAddress());
   }

   @Test
   public void testDuplicateJNDI() throws Exception
   {
      String someQueue = RandomUtil.randomString();
      String someOtherQueue = RandomUtil.randomString();
      serverManager.createQueue(false, someQueue, null, true, someQueue, "/duplicate");
      boolean exception = false;
      try
      {
         serverManager.createQueue(false, someOtherQueue, null, true, someOtherQueue, "/duplicate");
      }
      catch (Exception e)
      {
         exception = true;
      }

      assertTrue(exception);
   }

   @Test
   public void testExpireMessage() throws Exception
   {
      JMSQueueControl queueControl = createManagementControl();
      String expiryQueueName = RandomUtil.randomString();
      HornetQQueue expiryQueue = (HornetQQueue)HornetQJMSClient.createQueue(expiryQueueName);
      serverManager.createQueue(false, expiryQueueName, null, true, expiryQueueName);
      queueControl.setExpiryAddress(expiryQueue.getAddress());

      JMSQueueControl expiryQueueControl = ManagementControlHelper.createJMSQueueControl(expiryQueue, mbeanServer);

      String[] messageIDs = JMSUtil.sendMessages(queue, 1);

      Assert.assertEquals(1, queueControl.getMessageCount());
      Assert.assertEquals(0, expiryQueueControl.getMessageCount());

      Assert.assertTrue(queueControl.expireMessage(messageIDs[0]));

      Assert.assertEquals(0, queueControl.getMessageCount());
      Assert.assertEquals(1, expiryQueueControl.getMessageCount());

      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      connection.start();

      MessageConsumer consumer = JMSUtil.createConsumer(connection, expiryQueue);
      Message message = consumer.receive(500);
      Assert.assertNotNull(message);
      Assert.assertEquals(messageIDs[0], message.getJMSMessageID());

      connection.close();
   }

   @Test
   public void testExpireMessageWithUnknownMessageID() throws Exception
   {
      String unknownMessageID = RandomUtil.randomString();

      JMSQueueControl queueControl = createManagementControl();

      try
      {
         queueControl.expireMessage(unknownMessageID);
         Assert.fail();
      }
      catch (Exception e)
      {
      }
   }

   @Test
   public void testExpireMessagesWithFilter() throws Exception
   {
      String key = new String("key");
      long matchingValue = RandomUtil.randomLong();
      long unmatchingValue = matchingValue + 1;
      String filter = key + " =" + matchingValue;

      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      // send on queue
      JMSUtil.sendMessageWithProperty(session, queue, key, matchingValue);
      JMSUtil.sendMessageWithProperty(session, queue, key, unmatchingValue);

      connection.close();

      JMSQueueControl queueControl = createManagementControl();
      Assert.assertEquals(2, queueControl.getMessageCount());

      int expiredMessagesCount = queueControl.expireMessages(filter);
      Assert.assertEquals(1, expiredMessagesCount);
      Assert.assertEquals(1, queueControl.getMessageCount());

      // consume the unmatched message from queue
      JMSUtil.consumeMessages(1, queue);
   }

   @Test
   public void testCountMessagesWithFilter() throws Exception
   {
      String key = "key";
      long matchingValue = RandomUtil.randomLong();
      long unmatchingValue = matchingValue + 1;

      JMSQueueControl queueControl = createManagementControl();

      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      JMSUtil.sendMessageWithProperty(session, queue, key, matchingValue);
      JMSUtil.sendMessageWithProperty(session, queue, key, unmatchingValue);
      JMSUtil.sendMessageWithProperty(session, queue, key, matchingValue);

      Assert.assertEquals(3, queueControl.getMessageCount());

      Assert.assertEquals(2, queueControl.countMessages(key + " =" + matchingValue));
      Assert.assertEquals(1, queueControl.countMessages(key + " =" + unmatchingValue));

      connection.close();
   }

   @Test
   public void testGetDeadLetterAddress() throws Exception
   {
      final SimpleString deadLetterAddress = RandomUtil.randomSimpleString();

      JMSQueueControl queueControl = createManagementControl();

      Assert.assertNull(queueControl.getDeadLetterAddress());

      server.getAddressSettingsRepository().addMatch(queue.getAddress(), new AddressSettings()
      {
         private static final long serialVersionUID = -5979378001862611598L;

         @Override
         public SimpleString getDeadLetterAddress()
         {
            return deadLetterAddress;
         }
      });

      Assert.assertEquals(deadLetterAddress.toString(), queueControl.getDeadLetterAddress());
   }

   @Test
   public void testSetDeadLetterAddress() throws Exception
   {
      final String deadLetterAddress = RandomUtil.randomString();

      JMSQueueControl queueControl = createManagementControl();

      Assert.assertNull(queueControl.getDeadLetterAddress());

      queueControl.setDeadLetterAddress(deadLetterAddress);
      Assert.assertEquals(deadLetterAddress, queueControl.getDeadLetterAddress());
   }

   @Test
   public void testSendMessageToDeadLetterAddress() throws Exception
   {
      String deadLetterQueue = RandomUtil.randomString();
      serverManager.createQueue(false, deadLetterQueue, null, true, deadLetterQueue);
      HornetQQueue dlq = (HornetQQueue)HornetQJMSClient.createQueue(deadLetterQueue);

      Connection conn = createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = sess.createProducer(queue);

      // send 2 messages on queue
      Message message = sess.createMessage();
      producer.send(message);
      producer.send(sess.createMessage());

      conn.close();

      JMSQueueControl queueControl = createManagementControl();
      JMSQueueControl dlqControl = ManagementControlHelper.createJMSQueueControl(dlq, mbeanServer);

      Assert.assertEquals(2, queueControl.getMessageCount());
      Assert.assertEquals(0, dlqControl.getMessageCount());

      queueControl.setDeadLetterAddress(dlq.getAddress());

      boolean movedToDeadLetterAddress = queueControl.sendMessageToDeadLetterAddress(message.getJMSMessageID());
      Assert.assertTrue(movedToDeadLetterAddress);
      Assert.assertEquals(1, queueControl.getMessageCount());
      Assert.assertEquals(1, dlqControl.getMessageCount());

      // check there is a single message to consume from queue
      JMSUtil.consumeMessages(1, queue);

      // check there is a single message to consume from deadletter queue
      JMSUtil.consumeMessages(1, dlq);

      serverManager.destroyQueue(deadLetterQueue);
   }

   @Test
   public void testSendMessageToDeadLetterAddressWithUnknownMessageID() throws Exception
   {
      String unknownMessageID = RandomUtil.randomString();

      JMSQueueControl queueControl = createManagementControl();

      try
      {
         queueControl.sendMessageToDeadLetterAddress(unknownMessageID);
         Assert.fail();
      }
      catch (Exception e)
      {
      }

   }

   @Test
   public void testSendMessagesToDeadLetterAddress() throws Exception
   {
      String key = "key";
      long matchingValue = RandomUtil.randomLong();
      long unmatchingValue = matchingValue + 1;
      String filter = "key = " + matchingValue;

      String deadLetterQueue = RandomUtil.randomString();
      serverManager.createQueue(false, deadLetterQueue, null, true, deadLetterQueue);
      HornetQQueue dlq = (HornetQQueue)HornetQJMSClient.createQueue(deadLetterQueue);

      Connection conn = createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      // send 2 messages on queue
      JMSUtil.sendMessageWithProperty(sess, queue, key, matchingValue);
      JMSUtil.sendMessageWithProperty(sess, queue, key, unmatchingValue);

      JMSQueueControl queueControl = createManagementControl();
      JMSQueueControl dlqControl = ManagementControlHelper.createJMSQueueControl(dlq, mbeanServer);

      Assert.assertEquals(2, queueControl.getMessageCount());
      Assert.assertEquals(0, dlqControl.getMessageCount());

      queueControl.setDeadLetterAddress(dlq.getAddress());

      int deadMessageCount = queueControl.sendMessagesToDeadLetterAddress(filter);
      Assert.assertEquals(1, deadMessageCount);
      Assert.assertEquals(1, queueControl.getMessageCount());
      Assert.assertEquals(1, dlqControl.getMessageCount());

      conn.start();
      MessageConsumer consumer = sess.createConsumer(queue);

      Message message = consumer.receive(500);
      Assert.assertNotNull(message);
      Assert.assertEquals(unmatchingValue, message.getLongProperty(key));

      // check there is a single message to consume from deadletter queue
      JMSUtil.consumeMessages(1, dlq);

      conn.close();

      serverManager.destroyQueue(deadLetterQueue);
   }

   @Test
   public void testMoveMessages() throws Exception
   {
      String otherQueueName = RandomUtil.randomString();

      serverManager.createQueue(false, otherQueueName, null, true, otherQueueName);
      HornetQDestination otherQueue = (HornetQDestination)HornetQJMSClient.createQueue(otherQueueName);

      // send on queue
      JMSUtil.sendMessages(queue, 2);

      JMSQueueControl queueControl = createManagementControl();
      Assert.assertEquals(2, queueControl.getMessageCount());

      // moved all messages to otherQueue
      int movedMessagesCount = queueControl.moveMessages(null, otherQueueName);
      Assert.assertEquals(2, movedMessagesCount);
      Assert.assertEquals(0, queueControl.getMessageCount());

      // check there is no message to consume from queue
      JMSUtil.consumeMessages(0, queue);

      // consume the message from otherQueue
      JMSUtil.consumeMessages(2, otherQueue);

      serverManager.destroyQueue(otherQueueName);
   }

   @Test
   public void testMoveMessagesToUknownQueue() throws Exception
   {
      String unknownQueue = RandomUtil.randomString();

      JMSQueueControl queueControl = createManagementControl();

      try
      {
         queueControl.moveMessages(null, unknownQueue);
         Assert.fail();
      }
      catch (Exception e)
      {
      }
   }

   @Test
   public void testMoveMatchingMessages() throws Exception
   {
      String key = "key";
      long matchingValue = RandomUtil.randomLong();
      long unmatchingValue = matchingValue + 1;
      String filter = "key = " + matchingValue;
      String otherQueueName = RandomUtil.randomString();

      serverManager.createQueue(false, otherQueueName, null, true, otherQueueName);
      HornetQDestination otherQueue = (HornetQDestination)HornetQJMSClient.createQueue(otherQueueName);

      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      // send on queue
      JMSUtil.sendMessageWithProperty(session, queue, key, matchingValue);
      JMSUtil.sendMessageWithProperty(session, queue, key, unmatchingValue);

      JMSQueueControl queueControl = createManagementControl();
      Assert.assertEquals(2, queueControl.getMessageCount());

      // moved matching messages to otherQueue
      int movedMessagesCount = queueControl.moveMessages(filter, otherQueueName);
      Assert.assertEquals(1, movedMessagesCount);
      Assert.assertEquals(1, queueControl.getMessageCount());

      connection.start();
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(500);
      Assert.assertNotNull(message);
      Assert.assertEquals(unmatchingValue, message.getLongProperty(key));
      Assert.assertNull(consumer.receiveNoWait());

      JMSUtil.consumeMessages(1, otherQueue);

      serverManager.destroyQueue(otherQueueName);

      connection.close();
   }

   @Test
   public void testMoveMessage() throws Exception
   {
      String otherQueueName = RandomUtil.randomString();

      serverManager.createQueue(false, otherQueueName, null, true, otherQueueName);
      HornetQDestination otherQueue = (HornetQDestination)HornetQJMSClient.createQueue(otherQueueName);

      String[] messageIDs = JMSUtil.sendMessages(queue, 1);

      JMSQueueControl queueControl = createManagementControl();
      Assert.assertEquals(1, queueControl.getMessageCount());

      boolean moved = queueControl.moveMessage(messageIDs[0], otherQueueName);
      Assert.assertTrue(moved);
      Assert.assertEquals(0, queueControl.getMessageCount());

      JMSUtil.consumeMessages(0, queue);
      JMSUtil.consumeMessages(1, otherQueue);

      serverManager.destroyQueue(otherQueueName);
   }


   @Test
   public void testMoveMessagesWithDuplicateIDSet() throws Exception
   {
      String otherQueueName = RandomUtil.randomString();

      serverManager.createQueue(false, otherQueueName, null, true, otherQueueName);
      HornetQDestination otherQueue = (HornetQDestination)HornetQJMSClient.createQueue(otherQueueName);

      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(true, true);

      ClientProducer prod1 = session.createProducer(queue.getAddress());
      ClientProducer prod2 = session.createProducer(otherQueue.getAddress());

      for (int i = 0; i < 10; i++)
      {
         ClientMessage msg = session.createMessage(true);

         msg.putStringProperty(org.hornetq.api.core.Message.HDR_DUPLICATE_DETECTION_ID, new SimpleString("dupl-" + i));

         prod1.send(msg);
         if (i < 5)
         {
            prod2.send(msg);
         }
      }

      session.commit();

      JMSQueueControl queueControl = createManagementControl();
      JMSQueueControl otherQueueControl = ManagementControlHelper.createJMSQueueControl((HornetQQueue)otherQueue,
                                                                                        mbeanServer);

      Assert.assertEquals(10, queueControl.getMessageCount());

      int moved = queueControl.moveMessages(null, otherQueueName, true);

      assertEquals(10, moved);

      assertEquals(0, queueControl.getDeliveringCount());

      session.start();

      ClientConsumer cons1 = session.createConsumer(queue.getAddress());

      assertNull(cons1.receiveImmediate());

      cons1.close();

      ClientConsumer cons2 = session.createConsumer(otherQueue.getAddress());

      for (int i = 0; i < 10; i++)
      {
         ClientMessage msg = cons2.receive(10000);

         assertNotNull(msg);

         msg.acknowledge();
      }

      cons2.close();

      session.close();

      sf.close();

      locator.close();

      Assert.assertEquals(0, queueControl.getMessageCount());

      Assert.assertEquals(0, otherQueueControl.getMessageCount());

      serverManager.destroyQueue(otherQueueName);
   }


   @Test
   public void testMoveIndividualMessagesWithDuplicateIDSetUsingI() throws Exception
   {
      String otherQueueName = RandomUtil.randomString();

      serverManager.createQueue(false, otherQueueName, null, true, otherQueueName);
      HornetQDestination otherQueue = (HornetQDestination)HornetQJMSClient.createQueue(otherQueueName);

      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(true, true);

      ClientProducer prod1 = session.createProducer(queue.getAddress());
      ClientProducer prod2 = session.createProducer(otherQueue.getAddress());

      String [] ids = new String[10];

      for (int i = 0; i < 10; i++)
      {
         ClientMessage msg = session.createMessage(true);

         msg.putStringProperty(org.hornetq.api.core.Message.HDR_DUPLICATE_DETECTION_ID, new SimpleString("dupl-" + i));

         msg.setUserID(UUIDGenerator.getInstance().generateUUID());

         prod1.send(msg);

         ids[i] = "ID:" + msg.getUserID().toString();
         if (i < 5)
         {
            msg.setUserID(UUIDGenerator.getInstance().generateUUID());
            prod2.send(msg);
         }
      }

      session.commit();

      JMSQueueControl queueControl = createManagementControl();
      JMSQueueControl otherQueueControl = ManagementControlHelper.createJMSQueueControl((HornetQQueue)otherQueue,
                                                                                        mbeanServer);

      Assert.assertEquals(10, queueControl.getMessageCount());

      for (int i = 0 ; i < 10; i++)
      {
         queueControl.moveMessage(ids[i], otherQueueName, true);
      }

      assertEquals(0, queueControl.getDeliveringCount());

      session.start();

      ClientConsumer cons1 = session.createConsumer(queue.getAddress());

      assertNull(cons1.receiveImmediate());

      cons1.close();

      ClientConsumer cons2 = session.createConsumer(otherQueue.getAddress());

      for (int i = 0; i < 10; i++)
      {
         ClientMessage msg = cons2.receive(10000);

         assertNotNull(msg);

         msg.acknowledge();
      }

      cons2.close();

      session.close();

      sf.close();

      locator.close();

      Assert.assertEquals(0, queueControl.getMessageCount());

      Assert.assertEquals(0, otherQueueControl.getMessageCount());

      serverManager.destroyQueue(otherQueueName);
   }

   @Test
   public void testMoveMessagesWithDuplicateIDSetSingleMessage() throws Exception
   {
      String otherQueueName = RandomUtil.randomString();

      serverManager.createQueue(false, otherQueueName, null, true, otherQueueName);
      HornetQDestination otherQueue = (HornetQDestination)HornetQJMSClient.createQueue(otherQueueName);

      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(true, true);

      ClientProducer prod1 = session.createProducer(queue.getAddress());
      ClientProducer prod2 = session.createProducer(otherQueue.getAddress());

      ClientMessage msg = session.createMessage(true);

      msg.putStringProperty(org.hornetq.api.core.Message.HDR_DUPLICATE_DETECTION_ID, new SimpleString("dupl-1"));

      prod1.send(msg);
      prod2.send(msg);

      JMSQueueControl queueControl = createManagementControl();
      JMSQueueControl otherQueueControl = ManagementControlHelper.createJMSQueueControl((HornetQQueue)otherQueue,
                                                                                        mbeanServer);

      Assert.assertEquals(1, queueControl.getMessageCount());
      Assert.assertEquals(1, otherQueueControl.getMessageCount());

      int moved = queueControl.moveMessages(null, otherQueueName, true);

      assertEquals(1, moved);

      assertEquals(0, queueControl.getDeliveringCount());

      session.start();

      ClientConsumer cons1 = session.createConsumer(queue.getAddress());

      assertNull(cons1.receiveImmediate());

      cons1.close();

      ClientConsumer cons2 = session.createConsumer(otherQueue.getAddress());

      msg = cons2.receive(10000);

      assertNotNull(msg);

      msg.acknowledge();

      cons2.close();

      session.close();

      sf.close();

      locator.close();

      Assert.assertEquals(0, queueControl.getMessageCount());

      Assert.assertEquals(0, otherQueueControl.getMessageCount());

      serverManager.destroyQueue(otherQueueName);
   }

   @Test
   public void testMoveMessageWithUnknownMessageID() throws Exception
   {
      String unknownMessageID = RandomUtil.randomString();
      String otherQueueName = RandomUtil.randomString();

      serverManager.createQueue(false, otherQueueName, null, true, otherQueueName);

      JMSQueueControl queueControl = createManagementControl();
      Assert.assertEquals(0, queueControl.getMessageCount());

      try
      {
         queueControl.moveMessage(unknownMessageID, otherQueueName);
         Assert.fail();
      }
      catch (Exception e)
      {
      }

      serverManager.destroyQueue(otherQueueName);
   }

   @Test
   public void testDeleteWithPaging() throws Exception
   {
      AddressSettings pagedSetting = new AddressSettings();
      pagedSetting.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      pagedSetting.setPageSizeBytes(10 * 1024);
      pagedSetting.setMaxSizeBytes(100 * 1024);
      server.getAddressSettingsRepository().addMatch("#", pagedSetting);

      serverManager.createQueue(true, "pagedTest", null, true, "/queue/pagedTest");

      HornetQQueue pagedQueue = (HornetQQueue)context.lookup("/queue/pagedTest");


      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(true, true);

      ClientProducer prod = session.createProducer(pagedQueue.getAddress());

      ClientMessage msg = session.createMessage(true);

      msg.getBodyBuffer().writeBytes(new byte[90 * 1024]);
      for (int i = 0 ; i < 100; i++)
      {
         prod.send(msg);
      }

      JMSQueueControl control = createManagementControl(pagedQueue);

      assertEquals(100, control.removeMessages("     "));



      session.start();

      ClientConsumer consumer = session.createConsumer(pagedQueue.getAddress());

      assertNull(consumer.receive(300));


      session.close();

      sf.close();
      locator.close();
   }


   @Test
   public void testDeleteWithPagingAndFilter() throws Exception
   {
      AddressSettings pagedSetting = new AddressSettings();
      pagedSetting.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      pagedSetting.setPageSizeBytes(10 * 1024);
      pagedSetting.setMaxSizeBytes(100 * 1024);
      server.getAddressSettingsRepository().addMatch("#", pagedSetting);

      serverManager.createQueue(true, "pagedTest", null, true, "/queue/pagedTest");

      HornetQQueue pagedQueue = (HornetQQueue)context.lookup("/queue/pagedTest");


      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(true, true);

      ClientProducer prod = session.createProducer(pagedQueue.getAddress());
      for (int i = 0 ; i < 200; i++)
      {
         ClientMessage msg = session.createMessage(true);
         msg.getBodyBuffer().writeBytes(new byte[90 * 1024]);
         msg.putBooleanProperty("even", i % 2 == 0);
         prod.send(msg);
      }

      JMSQueueControl control = createManagementControl(pagedQueue);

      assertEquals(100, control.removeMessages("even=true"));

      session.start();

      ClientConsumer consumer = session.createConsumer(pagedQueue.getAddress());



      for (int i = 0 ; i < 100; i++)
      {
         ClientMessage msg = consumer.receive(1000);
         assertNotNull(msg);
         msg.acknowledge();
         assertFalse(msg.getBooleanProperty("even").booleanValue());
      }

      assertNull(consumer.receive(300));


      session.close();


      sf.close();
      locator.close();
   }

   @Test
   public void testMoveMessageToUnknownQueue() throws Exception
   {
      String unknwonQueue = RandomUtil.randomString();

      String[] messageIDs = JMSUtil.sendMessages(queue, 1);

      JMSQueueControl queueControl = createManagementControl();
      Assert.assertEquals(1, queueControl.getMessageCount());

      try
      {
         queueControl.moveMessage(messageIDs[0], unknwonQueue);
         Assert.fail();
      }
      catch (Exception e)
      {
      }

      JMSUtil.consumeMessages(1, queue);
   }

   @Test
   public void testPauseAndResume()
   {

      try
      {
         JMSQueueControl queueControl = createManagementControl();

         Assert.assertFalse(queueControl.isPaused());
         queueControl.pause();
         Assert.assertTrue(queueControl.isPaused());
         queueControl.resume();
         Assert.assertFalse(queueControl.isPaused());
      }
      catch (Exception e)
      {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
   }

   /**
    * on this testContaining WithRealData will use real data on the journals
    * @throws Exception
    */
   @Test
   public void testQueueAddJndiWithRealData() throws Exception
   {
      String testQueueName = "testQueueAddJndi";
      serverManager.createQueue(true, testQueueName, null, true, testQueueName);
      HornetQQueue testQueue = (HornetQQueue)HornetQJMSClient.createQueue(testQueueName);

      JMSQueueControl queueControl = createManagementControl(testQueue);
      String[] bindings = queueControl.getJNDIBindings();

      String newJndi = "newTestQueueAddJndi";

      for (String b : bindings)
      {
         assertFalse(b.equals(newJndi));
      }
      queueControl.addJNDI(newJndi);

      bindings = queueControl.getJNDIBindings();
      boolean newBindingAdded = false;
      for (String b : bindings)
      {
         if (b.equals(newJndi))
         {
            newBindingAdded = true;
         }
      }
      assertTrue(newBindingAdded);

      serverManager.stop();

      serverManager.start();

      testQueue = (HornetQQueue)HornetQJMSClient.createQueue(testQueueName);

      queueControl = createManagementControl(testQueue);

      bindings = queueControl.getJNDIBindings();
      newBindingAdded = false;
      for (String b : bindings)
      {
         if (b.equals(newJndi))
         {
            newBindingAdded = true;
         }
      }
      assertTrue(newBindingAdded);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      Configuration conf = createBasicConfig();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.getAcceptorConfigurations().add(new TransportConfiguration(INVM_ACCEPTOR_FACTORY));
      conf.setFileDeploymentEnabled(false);
      server = createServer(this.getName().contains("WithRealData"), conf, mbeanServer);

      serverManager = new JMSServerManagerImpl(server);
      context = new InVMContext();
      serverManager.setContext(context);
      serverManager.start();
      serverManager.activated();

      queueName = RandomUtil.randomString();
      serverManager.createQueue(false, queueName, null, true, queueName);
      queue = (HornetQQueue)HornetQJMSClient.createQueue(queueName);
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      stopComponent(serverManager);
      serverManager = null;

      server = null;

      queue = null;

      context.close();

      context = null;

      super.tearDown();
   }

   protected JMSQueueControl createManagementControl() throws Exception
   {
      return createManagementControl(queue);
   }

   protected JMSQueueControl createManagementControl(HornetQQueue queueParameter) throws Exception
   {
      return ManagementControlHelper.createJMSQueueControl(queueParameter, mbeanServer);
   }

   // Private -------------------------------------------------------

   private Connection createConnection() throws JMSException
   {
      HornetQConnectionFactory cf = HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF,
                                                                                                                new TransportConfiguration(InVMConnectorFactory.class.getName()));

      cf.setBlockOnDurableSend(true);

      return cf.createConnection();
   }

   // Inner classes -------------------------------------------------

}