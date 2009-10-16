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

import static org.hornetq.tests.integration.management.ManagementControlHelper.createHornetQServerControl;
import static org.hornetq.tests.integration.management.ManagementControlHelper.createJMSQueueControl;
import static org.hornetq.tests.util.RandomUtil.randomLong;
import static org.hornetq.tests.util.RandomUtil.randomSimpleString;
import static org.hornetq.tests.util.RandomUtil.randomString;

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

import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.management.HornetQServerControl;
import org.hornetq.core.management.QueueControl;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.jms.HornetQQueue;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.jms.server.management.JMSQueueControl;
import org.hornetq.tests.integration.management.ManagementControlHelper;
import org.hornetq.tests.integration.management.ManagementTestBase;
import org.hornetq.tests.unit.util.InVMContext;
import org.hornetq.utils.SimpleString;
import org.hornetq.utils.json.JSONArray;

/**
 * A QueueControlTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 14 nov. 2008 13:35:10
 *
 *
 */
public class JMSQueueControlTest extends ManagementTestBase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private HornetQServer server;

   private JMSServerManagerImpl serverManager;

   protected HornetQQueue queue;

   protected Context context;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testGetAttributes() throws Exception
   {
      JMSQueueControl queueControl = createManagementControl();

      assertEquals(queue.getName(), queueControl.getName());
      assertEquals(queue.getAddress(), queueControl.getAddress());
      assertEquals(queue.isTemporary(), queueControl.isTemporary());
   }

   public void testGetXXXCount() throws Exception
   {
      JMSQueueControl queueControl = createManagementControl();

      assertEquals(0, queueControl.getMessageCount());
      assertEquals(0, queueControl.getConsumerCount());

      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      MessageConsumer consumer = JMSUtil.createConsumer(connection, queue);

      assertEquals(1, queueControl.getConsumerCount());

      JMSUtil.sendMessages(queue, 2);

      assertEquals(2, queueControl.getMessageCount());
      assertEquals(2, queueControl.getMessagesAdded());

      connection.start();

      assertNotNull(consumer.receive(500));
      assertNotNull(consumer.receive(500));

      assertEquals(0, queueControl.getMessageCount());
      assertEquals(2, queueControl.getMessagesAdded());

      consumer.close();

      assertEquals(0, queueControl.getConsumerCount());

      connection.close();
   }

   public void testListMessagesWithNullFilter() throws Exception
   {
      JMSQueueControl queueControl = createManagementControl();

      assertEquals(0, queueControl.getMessageCount());

      String[] ids = JMSUtil.sendMessages(queue, 2);

      assertEquals(2, queueControl.getMessageCount());

      Map<String, Object>[] data = queueControl.listMessages(null);
      assertEquals(2, data.length);
      System.out.println(data[0].keySet());
      assertEquals(ids[0], data[0].get("JMSMessageID").toString());
      assertEquals(ids[1], data[1].get("JMSMessageID").toString());

      JMSUtil.consumeMessages(2, queue);

      data = queueControl.listMessages(null);
      assertEquals(0, data.length);
   }

   public void testListMessagesAsJSONWithNullFilter() throws Exception
   {
      JMSQueueControl queueControl = createManagementControl();

      assertEquals(0, queueControl.getMessageCount());

      String[] ids = JMSUtil.sendMessages(queue, 2);

      assertEquals(2, queueControl.getMessageCount());

      String jsonString = queueControl.listMessagesAsJSON(null);
      assertNotNull(jsonString);
      JSONArray array = new JSONArray(jsonString);
      assertEquals(2, array.length());
      assertEquals(ids[0], array.getJSONObject(0).get("JMSMessageID"));
      assertEquals(ids[1], array.getJSONObject(1).get("JMSMessageID"));

      JMSUtil.consumeMessages(2, queue);

      jsonString = queueControl.listMessagesAsJSON(null);
      assertNotNull(jsonString);
      array = new JSONArray(jsonString);
      assertEquals(0, array.length());
   }

   public void testRemoveMessage() throws Exception
   {
      JMSQueueControl queueControl = createManagementControl();

      assertEquals(0, queueControl.getMessageCount());

      JMSUtil.sendMessages(queue, 2);

      assertEquals(2, queueControl.getMessageCount());

      Map<String, Object>[] data = queueControl.listMessages(null);
      assertEquals(2, data.length);

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

      assertEquals(1, queueControl.getMessageCount());
   }

   public void testRemoveMessageWithUnknownMessage() throws Exception
   {
      String unknownMessageID = randomString();

      JMSQueueControl queueControl = createManagementControl();

      assertEquals(0, queueControl.getMessageCount());

      try
      {
         queueControl.removeMessage(unknownMessageID);
         fail("should throw an exception is the message ID is unknown");
      }
      catch (Exception e)
      {
      }
   }

   public void testRemoveAllMessages() throws Exception
   {
      JMSQueueControl queueControl = createManagementControl();

      assertEquals(0, queueControl.getMessageCount());

      JMSUtil.sendMessages(queue, 2);

      assertEquals(2, queueControl.getMessageCount());

      queueControl.removeMessages(null);

      assertEquals(0, queueControl.getMessageCount());

      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      connection.start();

      MessageConsumer consumer = JMSUtil.createConsumer(connection, queue);
      assertNull(consumer.receive(500));

      connection.close();
   }

   public void testRemoveMatchingMessages() throws Exception
   {
      JMSQueueControl queueControl = createManagementControl();

      assertEquals(0, queueControl.getMessageCount());

      Connection conn = createConnection();

      Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = s.createProducer(queue);

      Message message = s.createMessage();
      message.setStringProperty("foo", "bar");
      producer.send(message);

      message = s.createMessage();
      message.setStringProperty("foo", "baz");
      producer.send(message);

      assertEquals(2, queueControl.getMessageCount());

      int removedMatchingMessagesCount = queueControl.removeMessages("foo = 'bar'");
      assertEquals(1, removedMatchingMessagesCount);

      assertEquals(1, queueControl.getMessageCount());

      conn.start();
      MessageConsumer consumer = JMSUtil.createConsumer(conn, queue);
      Message msg = consumer.receive(500);
      assertNotNull(msg);
      assertEquals("baz", msg.getStringProperty("foo"));

      conn.close();
   }

   public void testChangeMessagePriority() throws Exception
   {
      JMSQueueControl queueControl = createManagementControl();

      JMSUtil.sendMessages(queue, 1);

      assertEquals(1, queueControl.getMessageCount());

      Map<String, Object>[] data = queueControl.listMessages(null);
      // retrieve the first message info
      String messageID = (String)data[0].get("JMSMessageID");
      int currentPriority = ((Number)data[0].get("JMSPriority")).intValue();
      int newPriority = 9;

      assertTrue(newPriority != currentPriority);

      queueControl.changeMessagePriority(messageID.toString(), newPriority);

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

      JMSQueueControl queueControl = createManagementControl();

      String[] messageIDs = JMSUtil.sendMessages(queue, 1);

      assertEquals(1, queueControl.getMessageCount());

      try
      {
         queueControl.changeMessagePriority(messageIDs[0], invalidPriority);
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

      JMSQueueControl queueControl = createManagementControl();

      try
      {
         queueControl.changeMessagePriority(unkownMessageID, 7);
         fail();
      }
      catch (Exception e)
      {
      }
   }
   
   public void testChangeMessagesPriority() throws Exception
   {
      String key = "key";
      long matchingValue = randomLong();
      long unmatchingValue = matchingValue + 1;
      String filter = "key = " + matchingValue;
      int newPriority = 9;

      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      // send on queue
      Message msg_1 = JMSUtil.sendMessageWithProperty(session, queue, key, matchingValue);
      Message msg_2 = JMSUtil.sendMessageWithProperty(session, queue, key, unmatchingValue);

      JMSQueueControl queueControl = createManagementControl();
      assertEquals(2, queueControl.getMessageCount());

      int changedMessagesCount = queueControl.changeMessagesPriority(filter, newPriority);
      assertEquals(1, changedMessagesCount);
      assertEquals(2, queueControl.getMessageCount());

      connection.start();
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(500);
      assertNotNull(message);
      assertEquals(msg_1.getJMSMessageID(), message.getJMSMessageID());
      assertEquals(9, message.getJMSPriority());
      assertEquals(matchingValue, message.getLongProperty(key));

      message = consumer.receive(500);
      assertNotNull(message);
      assertEquals(msg_2.getJMSMessageID(), message.getJMSMessageID());
      assertEquals(unmatchingValue, message.getLongProperty(key));

      assertNull(consumer.receive(500));

      connection.close();
   }

   public void testGetExpiryAddress() throws Exception
   {
      final SimpleString expiryAddress = randomSimpleString();

      JMSQueueControl queueControl = createManagementControl();

      assertNull(queueControl.getExpiryAddress());

      server.getAddressSettingsRepository().addMatch(queue.getAddress(), new AddressSettings()
      {
         @Override
         public SimpleString getExpiryAddress()
         {
            return expiryAddress;
         }
      });

      assertEquals(expiryAddress.toString(), queueControl.getExpiryAddress());
   }

   public void testSetExpiryAddress() throws Exception
   {
      final String expiryAddress = randomString();

      JMSQueueControl queueControl = createManagementControl();

      assertNull(queueControl.getExpiryAddress());

      queueControl.setExpiryAddress(expiryAddress);
      assertEquals(expiryAddress, queueControl.getExpiryAddress());
   }

   public void testExpireMessage() throws Exception
   {
      JMSQueueControl queueControl = createManagementControl();
      String expiryQueueName = randomString();
      HornetQQueue expiryQueue = new HornetQQueue(expiryQueueName);
      serverManager.createQueue(expiryQueueName, expiryQueueName, null, true);
      queueControl.setExpiryAddress(expiryQueue.getAddress());

      JMSQueueControl expiryQueueControl = createJMSQueueControl(expiryQueue, mbeanServer);

      String[] messageIDs = JMSUtil.sendMessages(queue, 1);

      assertEquals(1, queueControl.getMessageCount());
      assertEquals(0, expiryQueueControl.getMessageCount());

      assertTrue(queueControl.expireMessage(messageIDs[0]));

      assertEquals(0, queueControl.getMessageCount());
      assertEquals(1, expiryQueueControl.getMessageCount());

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

      JMSQueueControl queueControl = createManagementControl();

      try
      {
         queueControl.expireMessage(unknownMessageID);
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

      JMSQueueControl queueControl = createManagementControl();
      assertEquals(2, queueControl.getMessageCount());

      int expiredMessagesCount = queueControl.expireMessages(filter);
      assertEquals(1, expiredMessagesCount);
      assertEquals(1, queueControl.getMessageCount());

      // consume the unmatched message from queue
      JMSUtil.consumeMessages(1, queue);
   }

   public void testCountMessagesWithFilter() throws Exception
   {
      String key = "key";
      long matchingValue = randomLong();
      long unmatchingValue = matchingValue + 1;

      JMSQueueControl queueControl = createManagementControl();

      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      JMSUtil.sendMessageWithProperty(session, queue, key, matchingValue);
      JMSUtil.sendMessageWithProperty(session, queue, key, unmatchingValue);
      JMSUtil.sendMessageWithProperty(session, queue, key, matchingValue);

      assertEquals(3, queueControl.getMessageCount());

      assertEquals(2, queueControl.countMessages(key + " =" + matchingValue));
      assertEquals(1, queueControl.countMessages(key + " =" + unmatchingValue));

      connection.close();
   }

   public void testGetDeadLetterAddress() throws Exception
   {
      final SimpleString deadLetterAddress = randomSimpleString();

      JMSQueueControl queueControl = createManagementControl();

      assertNull(queueControl.getDeadLetterAddress());

      server.getAddressSettingsRepository().addMatch(queue.getAddress(), new AddressSettings()
      {
         @Override
         public SimpleString getDeadLetterAddress()
         {
            return deadLetterAddress;
         }
      });

      assertEquals(deadLetterAddress.toString(), queueControl.getDeadLetterAddress());
   }

   public void testSetDeadLetterAddress() throws Exception
   {
      final String deadLetterAddress = randomString();

      JMSQueueControl queueControl = createManagementControl();

      assertNull(queueControl.getDeadLetterAddress());

      queueControl.setDeadLetterAddress(deadLetterAddress);
      assertEquals(deadLetterAddress, queueControl.getDeadLetterAddress());
   }

   public void testSendMessageToDeadLetterAddress() throws Exception
   {
      String deadLetterQueue = randomString();
      serverManager.createQueue(deadLetterQueue, deadLetterQueue, null, true);
      HornetQQueue dlq = new HornetQQueue(deadLetterQueue);

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

      assertEquals(2, queueControl.getMessageCount());
      assertEquals(0, dlqControl.getMessageCount());

      queueControl.setDeadLetterAddress(dlq.getAddress());

      boolean movedToDeadLetterAddress = queueControl.sendMessageToDeadLetterAddress(message.getJMSMessageID());
      assertTrue(movedToDeadLetterAddress);
      assertEquals(1, queueControl.getMessageCount());
      assertEquals(1, dlqControl.getMessageCount());

      // check there is a single message to consume from queue
      JMSUtil.consumeMessages(1, queue);

      // check there is a single message to consume from deadletter queue
      JMSUtil.consumeMessages(1, dlq);

      serverManager.destroyQueue(deadLetterQueue);
   }

   public void testSendMessageToDeadLetterAddressWithUnknownMessageID() throws Exception
   {
      String unknownMessageID = randomString();

      JMSQueueControl queueControl = createManagementControl();

      try
      {
         queueControl.sendMessageToDeadLetterAddress(unknownMessageID);
         fail();
      }
      catch (Exception e)
      {
      }

   }
   
   public void testSendMessagesToDeadLetterAddress() throws Exception
   {
      String key = "key";
      long matchingValue = randomLong();
      long unmatchingValue = matchingValue + 1;
      String filter = "key = " + matchingValue;

      String deadLetterQueue = randomString();
      serverManager.createQueue(deadLetterQueue, deadLetterQueue, null, true);
      HornetQQueue dlq = new HornetQQueue(deadLetterQueue);

      Connection conn = createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      // send 2 messages on queue
      JMSUtil.sendMessageWithProperty(sess, queue, key, matchingValue);
      JMSUtil.sendMessageWithProperty(sess, queue, key, unmatchingValue);

      JMSQueueControl queueControl = createManagementControl();
      JMSQueueControl dlqControl = ManagementControlHelper.createJMSQueueControl(dlq, mbeanServer);

      assertEquals(2, queueControl.getMessageCount());
      assertEquals(0, dlqControl.getMessageCount());

      queueControl.setDeadLetterAddress(dlq.getAddress());

      int deadMessageCount = queueControl.sendMessagesToDeadLetterAddress(filter);
      assertEquals(1, deadMessageCount);
      assertEquals(1, queueControl.getMessageCount());
      assertEquals(1, dlqControl.getMessageCount());

      conn.start();
      MessageConsumer consumer = sess.createConsumer(queue);

      Message message = consumer.receive(500);
      assertNotNull(message);
      assertEquals(unmatchingValue, message.getLongProperty(key));

      // check there is a single message to consume from deadletter queue
      JMSUtil.consumeMessages(1, dlq);

      conn.close();
      
      serverManager.destroyQueue(deadLetterQueue);
   }

   public void testMoveMessages() throws Exception
   {
      String otherQueueName = randomString();

      serverManager.createQueue(otherQueueName, otherQueueName, null, true);
      HornetQQueue otherQueue = new HornetQQueue(otherQueueName);

      // send on queue
      JMSUtil.sendMessages(queue, 2);

      JMSQueueControl queueControl = createManagementControl();
      assertEquals(2, queueControl.getMessageCount());

      // moved all messages to otherQueue
      int movedMessagesCount = queueControl.moveMessages(null, otherQueueName);
      assertEquals(2, movedMessagesCount);
      assertEquals(0, queueControl.getMessageCount());

      // check there is no message to consume from queue
      JMSUtil.consumeMessages(0, queue);

      // consume the message from otherQueue
      JMSUtil.consumeMessages(2, otherQueue);

      serverManager.destroyQueue(otherQueueName);
   }

   public void testMoveMessagesToUknownQueue() throws Exception
   {
      String unknownQueue = randomString();

      JMSQueueControl queueControl = createManagementControl();

      try
      {
         queueControl.moveMessages(null, unknownQueue);
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

      serverManager.createQueue(otherQueueName, otherQueueName, null, true);
      HornetQQueue otherQueue = new HornetQQueue(otherQueueName);

      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      // send on queue
      JMSUtil.sendMessageWithProperty(session, queue, key, matchingValue);
      JMSUtil.sendMessageWithProperty(session, queue, key, unmatchingValue);

      JMSQueueControl queueControl = createManagementControl();
      assertEquals(2, queueControl.getMessageCount());

      // moved matching messages to otherQueue
      int movedMessagesCount = queueControl.moveMessages(filter, otherQueueName);
      assertEquals(1, movedMessagesCount);
      assertEquals(1, queueControl.getMessageCount());

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

      serverManager.createQueue(otherQueueName, otherQueueName, null, true);
      HornetQQueue otherQueue = new HornetQQueue(otherQueueName);

      String[] messageIDs = JMSUtil.sendMessages(queue, 1);

      JMSQueueControl queueControl = createManagementControl();
      assertEquals(1, queueControl.getMessageCount());

      boolean moved = queueControl.moveMessage(messageIDs[0], otherQueueName);
      assertTrue(moved);
      assertEquals(0, queueControl.getMessageCount());

      JMSUtil.consumeMessages(0, queue);
      JMSUtil.consumeMessages(1, otherQueue);

      serverManager.destroyQueue(otherQueueName);
   }

   public void testMoveMessageWithUnknownMessageID() throws Exception
   {
      String unknownMessageID = randomString();
      String otherQueueName = randomString();

      serverManager.createQueue(otherQueueName, otherQueueName, null, true);

      JMSQueueControl queueControl = createManagementControl();
      assertEquals(0, queueControl.getMessageCount());

      try
      {
         queueControl.moveMessage(unknownMessageID, otherQueueName);
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

      JMSQueueControl queueControl = createManagementControl();
      assertEquals(1, queueControl.getMessageCount());

      try
      {
         queueControl.moveMessage(messageIDs[0], unknwonQueue);
         fail();
      }
      catch (Exception e)
      {
      }

      JMSUtil.consumeMessages(1, queue);
   }
   

   public void testPauseAndResume()
   {

      try
      {
         JMSQueueControl queueControl = createManagementControl();
         
         assertFalse(queueControl.isPaused());
         queueControl.pause();
         assertTrue(queueControl.isPaused());
         queueControl.resume();
         assertFalse(queueControl.isPaused());
      }
      catch (Exception e)
      {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
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
      server = HornetQ.newHornetQServer(conf, mbeanServer, false);
      server.start();

      serverManager = new JMSServerManagerImpl(server);
      context = new InVMContext();
      serverManager.setContext(context);
      serverManager.start();
      serverManager.activated();

      String queueName = randomString();
      serverManager.createQueue(queueName, queueName, null, true);
      queue = new HornetQQueue(queueName);
   }

   @Override
   protected void tearDown() throws Exception
   {
      serverManager.stop();

      server.stop();

      serverManager = null;

      server = null;

      queue = null;

      context.close();

      context = null;

      super.tearDown();
   }

   protected JMSQueueControl createManagementControl() throws Exception
   {
      return createJMSQueueControl(queue, mbeanServer);
   }

   // Private -------------------------------------------------------

   private Connection createConnection() throws JMSException
   {
      HornetQConnectionFactory cf = new HornetQConnectionFactory(new TransportConfiguration(InVMConnectorFactory.class.getName()));

      cf.setBlockOnPersistentSend(true);

      return cf.createConnection();
   }

   // Inner classes -------------------------------------------------

}