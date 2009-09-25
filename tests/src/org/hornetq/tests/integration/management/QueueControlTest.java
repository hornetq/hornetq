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

package org.hornetq.tests.integration.management;

import static org.hornetq.tests.integration.management.ManagementControlHelper.createHornetQServerControl;
import static org.hornetq.tests.integration.management.ManagementControlHelper.createQueueControl;
import static org.hornetq.tests.util.RandomUtil.randomBoolean;
import static org.hornetq.tests.util.RandomUtil.randomInt;
import static org.hornetq.tests.util.RandomUtil.randomLong;
import static org.hornetq.tests.util.RandomUtil.randomSimpleString;
import static org.hornetq.tests.util.RandomUtil.randomString;

import java.util.Map;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.management.DayCounterInfo;
import org.hornetq.core.management.HornetQServerControl;
import org.hornetq.core.management.MessageCounterInfo;
import org.hornetq.core.management.QueueControl;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.messagecounter.impl.MessageCounterManagerImpl;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.utils.SimpleString;
import org.hornetq.utils.json.JSONArray;

/**
 * A QueueControlTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class QueueControlTest extends ManagementTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   protected HornetQServer server;

   protected ClientSession session;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testAttributes() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      SimpleString filter = new SimpleString("color = 'blue'");
      boolean durable = randomBoolean();

      session.createQueue(address, queue, filter, durable);

      QueueControl queueControl = createManagementControl(address, queue);
      assertEquals(queue.toString(), queueControl.getName());
      assertEquals(address.toString(), queueControl.getAddress());
      assertEquals(filter.toString(), queueControl.getFilter());
      assertEquals(durable, queueControl.isDurable());
      assertEquals(false, queueControl.isTemporary());

      session.deleteQueue(queue);
   }

   public void testGetNullFilter() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, null, false);

      QueueControl queueControl = createManagementControl(address, queue);
      assertEquals(queue.toString(), queueControl.getName());
      assertEquals(null, queueControl.getFilter());

      session.deleteQueue(queue);
   }

   public void testGetDeadLetterAddress() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      final SimpleString deadLetterAddress = randomSimpleString();

      session.createQueue(address, queue, null, false);

      QueueControl queueControl = createManagementControl(address, queue);
      assertNull(queueControl.getDeadLetterAddress());

      server.getAddressSettingsRepository().addMatch(address.toString(), new AddressSettings()
      {
         @Override
         public SimpleString getDeadLetterAddress()
         {
            return deadLetterAddress;
         }
      });

      assertEquals(deadLetterAddress.toString(), queueControl.getDeadLetterAddress());

      session.deleteQueue(queue);
   }

   public void testSetDeadLetterAddress() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      String deadLetterAddress = randomString();

      session.createQueue(address, queue, null, false);

      QueueControl queueControl = createManagementControl(address, queue);
      queueControl.setDeadLetterAddress(deadLetterAddress);

      assertEquals(deadLetterAddress, queueControl.getDeadLetterAddress());

      session.deleteQueue(queue);
   }

   public void testGetExpiryAddress() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      final SimpleString expiryAddress = randomSimpleString();

      session.createQueue(address, queue, null, false);

      QueueControl queueControl = createManagementControl(address, queue);
      assertNull(queueControl.getExpiryAddress());

      server.getAddressSettingsRepository().addMatch(address.toString(), new AddressSettings()
      {
         @Override
         public SimpleString getExpiryAddress()
         {
            return expiryAddress;
         }
      });

      assertEquals(expiryAddress.toString(), queueControl.getExpiryAddress());

      session.deleteQueue(queue);
   }

   public void testSetExpiryAddress() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      String expiryAddress = randomString();

      session.createQueue(address, queue, null, false);

      QueueControl queueControl = createManagementControl(address, queue);
      queueControl.setExpiryAddress(expiryAddress);

      assertEquals(expiryAddress, queueControl.getExpiryAddress());

      session.deleteQueue(queue);
   }

   public void testGetConsumerCount() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, null, false);

      QueueControl queueControl = createManagementControl(address, queue);

      assertEquals(0, queueControl.getConsumerCount());

      ClientConsumer consumer = session.createConsumer(queue);
      assertEquals(1, queueControl.getConsumerCount());

      consumer.close();
      assertEquals(0, queueControl.getConsumerCount());

      session.deleteQueue(queue);
   }

   public void testGetMessageCount() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, null, false);

      QueueControl queueControl = createManagementControl(address, queue);
      assertEquals(0, queueControl.getMessageCount());

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createClientMessage(false));
      assertEquals(1, queueControl.getMessageCount());

      consumeMessages(1, session, queue);

      assertEquals(0, queueControl.getMessageCount());

      session.deleteQueue(queue);
   }

   public void testGetMessagesAdded() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, null, false);

      QueueControl queueControl = createManagementControl(address, queue);
      assertEquals(0, queueControl.getMessagesAdded());

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createClientMessage(false));
      assertEquals(1, queueControl.getMessagesAdded());
      producer.send(session.createClientMessage(false));
      assertEquals(2, queueControl.getMessagesAdded());

      consumeMessages(2, session, queue);

      assertEquals(2, queueControl.getMessagesAdded());

      session.deleteQueue(queue);
   }

   public void testGetScheduledCount() throws Exception
   {
      long delay = 2000;
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, null, false);

      QueueControl queueControl = createManagementControl(address, queue);
      assertEquals(0, queueControl.getScheduledCount());

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createClientMessage(false);
      message.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, System.currentTimeMillis() + delay);
      producer.send(message);

      assertEquals(1, queueControl.getScheduledCount());
      consumeMessages(0, session, queue);

      Thread.sleep(delay);

      assertEquals(0, queueControl.getScheduledCount());
      consumeMessages(1, session, queue);

      session.deleteQueue(queue);
   }

   public void testListScheduledMessages() throws Exception
   {
      long delay = 2000;
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      int intValue = randomInt();
      session.createQueue(address, queue, null, false);

      QueueControl queueControl = createManagementControl(address, queue);

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createClientMessage(false);
      message.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, System.currentTimeMillis() + delay);
      message.putIntProperty(new SimpleString("key"), intValue);
      producer.send(message);
      // unscheduled message
      producer.send(session.createClientMessage(false));

      Map<String, Object>[] messages = queueControl.listScheduledMessages();
      assertEquals(1, messages.length);
      assertEquals(intValue, ((Number)messages[0].get("key")).intValue());

      Thread.sleep(delay + 500);

      messages = queueControl.listScheduledMessages();
      assertEquals(0, messages.length);

      consumeMessages(2, session, queue);

      session.deleteQueue(queue);
   }

   public void testListScheduledMessagesAsJSON() throws Exception
   {
      long delay = 2000;
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      int intValue = randomInt();
      session.createQueue(address, queue, null, false);

      QueueControl queueControl = createManagementControl(address, queue);

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createClientMessage(false);
      message.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, System.currentTimeMillis() + delay);
      message.putIntProperty(new SimpleString("key"), intValue);
      producer.send(message);
      // unscheduled message
      producer.send(session.createClientMessage(false));

      String jsonString = queueControl.listScheduledMessagesAsJSON();
      assertNotNull(jsonString);
      JSONArray array = new JSONArray(jsonString);
      assertEquals(1, array.length());
      assertEquals(intValue, array.getJSONObject(0).get("key"));

      Thread.sleep(delay + 500);

      jsonString = queueControl.listScheduledMessagesAsJSON();
      assertNotNull(jsonString);
      array = new JSONArray(jsonString);
      assertEquals(0, array.length());

      consumeMessages(2, session, queue);

      session.deleteQueue(queue);
   }

   public void testGetDeliveringCount() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, null, false);

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createClientMessage(false));

      QueueControl queueControl = createManagementControl(address, queue);
      assertEquals(0, queueControl.getDeliveringCount());

      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage message = consumer.receive(500);
      assertNotNull(message);
      assertEquals(1, queueControl.getDeliveringCount());

      message.acknowledge();
      session.commit();
      assertEquals(0, queueControl.getDeliveringCount());

      consumer.close();
      session.deleteQueue(queue);
   }

   public void testListMessagesAsJSONWithNullFilter() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      int intValue = randomInt();
      session.createQueue(address, queue, null, false);

      QueueControl queueControl = createManagementControl(address, queue);

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createClientMessage(false);
      message.putIntProperty(new SimpleString("key"), intValue);
      producer.send(message);

      String jsonString = queueControl.listMessagesAsJSON(null);
      assertNotNull(jsonString);
      JSONArray array = new JSONArray(jsonString);
      assertEquals(1, array.length());
      assertEquals(intValue, array.getJSONObject(0).get("key"));

      consumeMessages(1, session, queue);

      jsonString = queueControl.listMessagesAsJSON(null);
      assertNotNull(jsonString);
      array = new JSONArray(jsonString);
      assertEquals(0, array.length());

      session.deleteQueue(queue);
   }

   public void testListMessagesWithFilter() throws Exception
   {
      SimpleString key = new SimpleString("key");
      long matchingValue = randomLong();
      long unmatchingValue = matchingValue + 1;
      String filter = key + " =" + matchingValue;

      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, null, false);
      QueueControl queueControl = createManagementControl(address, queue);

      ClientProducer producer = session.createProducer(address);
      ClientMessage matchingMessage = session.createClientMessage(false);
      matchingMessage.putLongProperty(key, matchingValue);
      producer.send(matchingMessage);
      ClientMessage unmatchingMessage = session.createClientMessage(false);
      unmatchingMessage.putLongProperty(key, unmatchingValue);
      producer.send(unmatchingMessage);

      Map<String, Object>[] messages = queueControl.listMessages(filter);
      assertEquals(1, messages.length);
      assertEquals(matchingValue, messages[0].get("key"));

      consumeMessages(2, session, queue);

      messages = queueControl.listMessages(filter);
      assertEquals(0, messages.length);

      session.deleteQueue(queue);
   }

   public void testListMessagesWithNullFilter() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, null, false);
      QueueControl queueControl = createManagementControl(address, queue);

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createClientMessage(false));
      producer.send(session.createClientMessage(false));

      Map<String, Object>[] messages = queueControl.listMessages(null);
      assertEquals(2, messages.length);

      consumeMessages(2, session, queue);

      messages = queueControl.listMessages(null);
      assertEquals(0, messages.length);

      session.deleteQueue(queue);
   }

   public void testListMessagesWithEmptyFilter() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, null, false);
      QueueControl queueControl = createManagementControl(address, queue);

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createClientMessage(false));
      producer.send(session.createClientMessage(false));

      Map<String, Object>[] messages = queueControl.listMessages("");
      assertEquals(2, messages.length);

      consumeMessages(2, session, queue);

      messages = queueControl.listMessages("");
      assertEquals(0, messages.length);

      session.deleteQueue(queue);
   }

   public void testListMessagesAsJSONWithFilter() throws Exception
   {
      SimpleString key = new SimpleString("key");
      long matchingValue = randomLong();
      long unmatchingValue = matchingValue + 1;
      String filter = key + " =" + matchingValue;

      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, null, false);
      QueueControl queueControl = createManagementControl(address, queue);

      ClientProducer producer = session.createProducer(address);
      ClientMessage matchingMessage = session.createClientMessage(false);
      matchingMessage.putLongProperty(key, matchingValue);
      producer.send(matchingMessage);
      ClientMessage unmatchingMessage = session.createClientMessage(false);
      unmatchingMessage.putLongProperty(key, unmatchingValue);
      producer.send(unmatchingMessage);

      String jsonString = queueControl.listMessagesAsJSON(filter);
      assertNotNull(jsonString);
      JSONArray array = new JSONArray(jsonString);
      assertEquals(1, array.length());
      assertEquals(matchingValue, array.getJSONObject(0).get("key"));

      consumeMessages(2, session, queue);

      jsonString = queueControl.listMessagesAsJSON(filter);
      assertNotNull(jsonString);
      array = new JSONArray(jsonString);
      assertEquals(0, array.length());

      session.deleteQueue(queue);
   }

   /**
    * <ol>
    * <li>send a message to queue</li>
    * <li>move all messages from queue to otherQueue using management method</li>
    * <li>check there is no message to consume from queue</li>
    * <li>consume the message from otherQueue</li>
    * </ol>
    */
   public void testMoveMessages() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      SimpleString otherAddress = randomSimpleString();
      SimpleString otherQueue = randomSimpleString();

      session.createQueue(address, queue, null, false);
      session.createQueue(otherAddress, otherQueue, null, false);
      ClientProducer producer = session.createProducer(address);

      // send on queue
      ClientMessage message = session.createClientMessage(false);
      SimpleString key = randomSimpleString();
      long value = randomLong();
      message.putLongProperty(key, value);
      producer.send(message);

      QueueControl queueControl = createManagementControl(address, queue);
      assertEquals(1, queueControl.getMessageCount());

      // moved all messages to otherQueue
      int movedMessagesCount = queueControl.moveMessages(null, otherQueue.toString());
      assertEquals(1, movedMessagesCount);
      assertEquals(0, queueControl.getMessageCount());

      // check there is no message to consume from queue
      consumeMessages(0, session, queue);

      // consume the message from otherQueue
      ClientConsumer otherConsumer = session.createConsumer(otherQueue);
      ClientMessage m = otherConsumer.receive(500);
      assertEquals(value, m.getProperty(key));

      m.acknowledge();

      session.deleteQueue(queue);
      otherConsumer.close();
      session.deleteQueue(otherQueue);
   }

   public void testMoveMessagesToUnknownQueue() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      SimpleString unknownQueue = randomSimpleString();

      session.createQueue(address, queue, null, false);
      ClientProducer producer = session.createProducer(address);

      // send on queue
      ClientMessage message = session.createClientMessage(false);
      SimpleString key = randomSimpleString();
      long value = randomLong();
      message.putLongProperty(key, value);
      producer.send(message);

      QueueControl queueControl = createManagementControl(address, queue);
      assertEquals(1, queueControl.getMessageCount());

      // moved all messages to unknown queue
      try
      {
         queueControl.moveMessages(null, unknownQueue.toString());
         fail("operation must fail if the other queue does not exist");
      }
      catch (Exception e)
      {
      }
      assertEquals(1, queueControl.getMessageCount());

      consumeMessages(1, session, queue);

      session.deleteQueue(queue);
   }

   /**
    * <ol>
    * <li>send 2 message to queue</li>
    * <li>move messages from queue to otherQueue using management method <em>with filter</em></li>
    * <li>consume the message which <strong>did not</strong> matches the filter from queue</li>
    * <li>consume the message which <strong>did</strong> matches the filter from otherQueue</li>
    * </ol>
    */

   public void testMoveMessagesWithFilter() throws Exception
   {
      SimpleString key = new SimpleString("key");
      long matchingValue = randomLong();
      long unmatchingValue = matchingValue + 1;

      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      SimpleString otherAddress = randomSimpleString();
      SimpleString otherQueue = randomSimpleString();

      session.createQueue(address, queue, null, false);
      session.createQueue(otherAddress, otherQueue, null, false);
      ClientProducer producer = session.createProducer(address);

      // send on queue
      ClientMessage matchingMessage = session.createClientMessage(false);
      matchingMessage.putLongProperty(key, matchingValue);
      producer.send(matchingMessage);
      ClientMessage unmatchingMessage = session.createClientMessage(false);
      unmatchingMessage.putLongProperty(key, unmatchingValue);
      producer.send(unmatchingMessage);

      QueueControl queueControl = createManagementControl(address, queue);
      assertEquals(2, queueControl.getMessageCount());

      // moved matching messages to otherQueue
      int movedMatchedMessagesCount = queueControl.moveMessages(key + " =" + matchingValue, otherQueue.toString());
      assertEquals(1, movedMatchedMessagesCount);
      assertEquals(1, queueControl.getMessageCount());

      // consume the unmatched message from queue
      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage m = consumer.receive(500);
      assertNotNull(m);
      assertEquals(unmatchingValue, m.getProperty(key));

      // consume the matched message from otherQueue
      ClientConsumer otherConsumer = session.createConsumer(otherQueue);
      m = otherConsumer.receive(500);
      assertNotNull(m);
      assertEquals(matchingValue, m.getProperty(key));

      m.acknowledge();

      consumer.close();
      session.deleteQueue(queue);
      otherConsumer.close();
      session.deleteQueue(otherQueue);
   }

   public void testMoveMessage() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      SimpleString otherAddress = randomSimpleString();
      SimpleString otherQueue = randomSimpleString();

      session.createQueue(address, queue, null, false);
      session.createQueue(otherAddress, otherQueue, null, false);
      ClientProducer producer = session.createProducer(address);

      // send 2 messages on queue
      producer.send(session.createClientMessage(false));
      producer.send(session.createClientMessage(false));

      QueueControl queueControl = createManagementControl(address, queue);
      QueueControl otherQueueControl = createManagementControl(otherAddress, otherQueue);
      assertEquals(2, queueControl.getMessageCount());
      assertEquals(0, otherQueueControl.getMessageCount());

      // the message IDs are set on the server
      Map<String, Object>[] messages = queueControl.listMessages(null);
      assertEquals(2, messages.length);
      long messageID = (Long)messages[0].get("messageID");

      boolean moved = queueControl.moveMessage(messageID, otherQueue.toString());
      assertTrue(moved);
      assertEquals(1, queueControl.getMessageCount());
      assertEquals(1, otherQueueControl.getMessageCount());

      consumeMessages(1, session, queue);
      consumeMessages(1, session, otherQueue);

      session.deleteQueue(queue);
      session.deleteQueue(otherQueue);
   }

   public void testMoveMessageToUnknownQueue() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      SimpleString unknownQueue = randomSimpleString();

      session.createQueue(address, queue, null, false);
      ClientProducer producer = session.createProducer(address);

      // send 2 messages on queue
      producer.send(session.createClientMessage(false));

      QueueControl queueControl = createManagementControl(address, queue);
      assertEquals(1, queueControl.getMessageCount());

      // the message IDs are set on the server
      Map<String, Object>[] messages = queueControl.listMessages(null);
      assertEquals(1, messages.length);
      long messageID = (Long)messages[0].get("messageID");

      // moved all messages to unknown queue
      try
      {
         queueControl.moveMessage(messageID, unknownQueue.toString());
         fail("operation must fail if the other queue does not exist");
      }
      catch (Exception e)
      {
      }
      assertEquals(1, queueControl.getMessageCount());

      consumeMessages(1, session, queue);

      session.deleteQueue(queue);
   }

   /**
    * <ol>
    * <li>send 2 message to queue</li>
    * <li>remove messages from queue using management method <em>with filter</em></li>
    * <li>check there is only one message to consume from queue</li>
    * </ol>
    */

   public void testRemoveMessages() throws Exception
   {
      SimpleString key = new SimpleString("key");
      long matchingValue = randomLong();
      long unmatchingValue = matchingValue + 1;

      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, null, false);
      ClientProducer producer = session.createProducer(address);

      // send on queue
      ClientMessage matchingMessage = session.createClientMessage(false);
      matchingMessage.putLongProperty(key, matchingValue);
      producer.send(matchingMessage);
      ClientMessage unmatchingMessage = session.createClientMessage(false);
      unmatchingMessage.putLongProperty(key, unmatchingValue);
      producer.send(unmatchingMessage);

      QueueControl queueControl = createManagementControl(address, queue);
      assertEquals(2, queueControl.getMessageCount());

      // removed matching messages to otherQueue
      int removedMatchedMessagesCount = queueControl.removeMessages(key + " =" + matchingValue);
      assertEquals(1, removedMatchedMessagesCount);
      assertEquals(1, queueControl.getMessageCount());

      // consume the unmatched message from queue
      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage m = consumer.receive(500);
      assertNotNull(m);
      assertEquals(unmatchingValue, m.getProperty(key));

      m.acknowledge();

      // check there is no other message to consume:
      m = consumer.receive(500);
      assertNull(m);

      consumer.close();
      session.deleteQueue(queue);
   }

   public void testRemoveMessagesWithNullFilter() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, null, false);
      ClientProducer producer = session.createProducer(address);

      // send on queue
      producer.send(session.createClientMessage(false));
      producer.send(session.createClientMessage(false));

      QueueControl queueControl = createManagementControl(address, queue);
      assertEquals(2, queueControl.getMessageCount());

      // removed matching messages to otherQueue
      int removedMatchedMessagesCount = queueControl.removeMessages(null);
      assertEquals(2, removedMatchedMessagesCount);
      assertEquals(0, queueControl.getMessageCount());

      session.deleteQueue(queue);
   }

   public void testRemoveMessagesWithEmptyFilter() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, null, false);
      ClientProducer producer = session.createProducer(address);

      // send on queue
      producer.send(session.createClientMessage(false));
      producer.send(session.createClientMessage(false));

      QueueControl queueControl = createManagementControl(address, queue);
      assertEquals(2, queueControl.getMessageCount());

      // removed matching messages to otherQueue
      int removedMatchedMessagesCount = queueControl.removeMessages("");
      assertEquals(2, removedMatchedMessagesCount);
      assertEquals(0, queueControl.getMessageCount());

      session.deleteQueue(queue);
   }

   public void testRemoveMessage() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, null, false);
      ClientProducer producer = session.createProducer(address);

      // send 2 messages on queue
      producer.send(session.createClientMessage(false));
      producer.send(session.createClientMessage(false));

      QueueControl queueControl = createManagementControl(address, queue);
      assertEquals(2, queueControl.getMessageCount());

      // the message IDs are set on the server
      Map<String, Object>[] messages = queueControl.listMessages(null);
      assertEquals(2, messages.length);
      long messageID = (Long)messages[0].get("messageID");

      // delete 1st message
      boolean deleted = queueControl.removeMessage(messageID);
      assertTrue(deleted);
      assertEquals(1, queueControl.getMessageCount());

      // check there is a single message to consume from queue
      consumeMessages(1, session, queue);

      session.deleteQueue(queue);
   }

   public void testCountMessagesWithFilter() throws Exception
   {
      SimpleString key = new SimpleString("key");
      long matchingValue = randomLong();
      long unmatchingValue = matchingValue + 1;

      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, null, false);
      ClientProducer producer = session.createProducer(address);

      // send on queue
      ClientMessage matchingMessage = session.createClientMessage(false);
      matchingMessage.putLongProperty(key, matchingValue);
      ClientMessage unmatchingMessage = session.createClientMessage(false);
      unmatchingMessage.putLongProperty(key, unmatchingValue);
      producer.send(matchingMessage);
      producer.send(unmatchingMessage);
      producer.send(matchingMessage);

      QueueControl queueControl = createManagementControl(address, queue);
      assertEquals(3, queueControl.getMessageCount());

      assertEquals(2, queueControl.countMessages(key + " =" + matchingValue));
      assertEquals(1, queueControl.countMessages(key + " =" + unmatchingValue));

      session.deleteQueue(queue);
   }

   public void testExpireMessagesWithFilter() throws Exception
   {
      SimpleString key = new SimpleString("key");
      long matchingValue = randomLong();
      long unmatchingValue = matchingValue + 1;

      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, null, false);
      ClientProducer producer = session.createProducer(address);

      // send on queue
      ClientMessage matchingMessage = session.createClientMessage(false);
      matchingMessage.putLongProperty(key, matchingValue);
      producer.send(matchingMessage);
      ClientMessage unmatchingMessage = session.createClientMessage(false);
      unmatchingMessage.putLongProperty(key, unmatchingValue);
      producer.send(unmatchingMessage);

      QueueControl queueControl = createManagementControl(address, queue);
      assertEquals(2, queueControl.getMessageCount());

      int expiredMessagesCount = queueControl.expireMessages(key + " =" + matchingValue);
      assertEquals(1, expiredMessagesCount);
      assertEquals(1, queueControl.getMessageCount());

      // consume the unmatched message from queue
      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage m = consumer.receive(500);
      assertNotNull(m);
      assertEquals(unmatchingValue, m.getProperty(key));

      m.acknowledge();

      // check there is no other message to consume:
      m = consumer.receive(500);
      assertNull(m);

      consumer.close();
      session.deleteQueue(queue);
      session.close();
   }

   public void testExpireMessage() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      SimpleString expiryAddress = randomSimpleString();
      SimpleString expiryQueue = randomSimpleString();

      session.createQueue(address, queue, null, false);
      session.createQueue(expiryAddress, expiryQueue, null, false);
      ClientProducer producer = session.createProducer(address);

      // send on queue
      producer.send(session.createClientMessage(false));

      QueueControl queueControl = createManagementControl(address, queue);
      QueueControl expiryQueueControl = createManagementControl(expiryAddress, expiryQueue);
      assertEquals(1, queueControl.getMessageCount());
      assertEquals(0, expiryQueueControl.getMessageCount());

      // the message IDs are set on the server
      Map<String, Object>[] messages = queueControl.listMessages(null);
      assertEquals(1, messages.length);
      long messageID = (Long)messages[0].get("messageID");

      queueControl.setExpiryAddress(expiryAddress.toString());
      boolean expired = queueControl.expireMessage(messageID);
      assertTrue(expired);
      assertEquals(0, queueControl.getMessageCount());
      assertEquals(1, expiryQueueControl.getMessageCount());

      consumeMessages(0, session, queue);
      consumeMessages(1, session, expiryQueue);

      session.deleteQueue(queue);
      session.deleteQueue(expiryQueue);
      session.close();
   }

   public void testSendMessageToDeadLetterAddress() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      SimpleString deadLetterAddress = randomSimpleString();
      SimpleString deadLetterQueue = randomSimpleString();

      session.createQueue(address, queue, null, false);
      session.createQueue(deadLetterAddress, deadLetterQueue, null, false);
      ClientProducer producer = session.createProducer(address);

      // send 2 messages on queue
      producer.send(session.createClientMessage(false));
      producer.send(session.createClientMessage(false));

      QueueControl queueControl = createManagementControl(address, queue);
      QueueControl deadLetterQueueControl = createManagementControl(deadLetterAddress, deadLetterQueue);
      assertEquals(2, queueControl.getMessageCount());

      // the message IDs are set on the server
      Map<String, Object>[] messages = queueControl.listMessages(null);
      assertEquals(2, messages.length);
      long messageID = (Long)messages[0].get("messageID");

      queueControl.setDeadLetterAddress(deadLetterAddress.toString());

      assertEquals(0, deadLetterQueueControl.getMessageCount());
      boolean movedToDeadLetterAddress = queueControl.sendMessageToDeadLetterAddress(messageID);
      assertTrue(movedToDeadLetterAddress);
      assertEquals(1, queueControl.getMessageCount());
      assertEquals(1, deadLetterQueueControl.getMessageCount());

      // check there is a single message to consume from queue
      consumeMessages(1, session, queue);

      // check there is a single message to consume from deadletter queue
      consumeMessages(1, session, deadLetterQueue);

      session.deleteQueue(queue);
      session.deleteQueue(deadLetterQueue);
   }

   public void testChangeMessagePriority() throws Exception
   {
      byte originalPriority = (byte)1;
      byte newPriority = (byte)8;

      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, null, false);
      ClientProducer producer = session.createProducer(address);

      ClientMessage message = session.createClientMessage(false);
      message.setPriority(originalPriority);
      producer.send(message);

      QueueControl queueControl = createManagementControl(address, queue);
      assertEquals(1, queueControl.getMessageCount());

      // the message IDs are set on the server
      Map<String, Object>[] messages = queueControl.listMessages(null);
      assertEquals(1, messages.length);
      long messageID = (Long)messages[0].get("messageID");

      boolean priorityChanged = queueControl.changeMessagePriority(messageID, newPriority);
      assertTrue(priorityChanged);

      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage m = consumer.receive(500);
      assertNotNull(m);
      assertEquals(newPriority, m.getPriority());

      consumer.close();
      session.deleteQueue(queue);
   }

   public void testChangeMessagePriorityWithInvalidValue() throws Exception
   {
      byte invalidPriority = (byte)23;

      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, null, false);
      ClientProducer producer = session.createProducer(address);

      ClientMessage message = session.createClientMessage(false);
      producer.send(message);

      QueueControl queueControl = createManagementControl(address, queue);
      assertEquals(1, queueControl.getMessageCount());

      // the message IDs are set on the server
      Map<String, Object>[] messages = queueControl.listMessages(null);
      assertEquals(1, messages.length);
      long messageID = (Long)messages[0].get("messageID");

      try
      {
         queueControl.changeMessagePriority(messageID, invalidPriority);
         fail("operation fails when priority value is < 0 or > 9");
      }
      catch (Exception e)
      {
      }

      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage m = consumer.receive(500);
      assertNotNull(m);
      assertTrue(invalidPriority != m.getPriority());

      consumer.close();
      session.deleteQueue(queue);
   }

   public void testListMessageCounter() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, null, false);
      QueueControl queueControl = createManagementControl(address, queue);

      HornetQServerControl serverControl = createHornetQServerControl(mbeanServer);
      serverControl.enableMessageCounters();
      serverControl.setMessageCounterSamplePeriod(MessageCounterManagerImpl.MIN_SAMPLE_PERIOD);

      String jsonString = queueControl.listMessageCounter();
      MessageCounterInfo info = MessageCounterInfo.fromJSON(jsonString);

      assertEquals(0, info.getDepth());
      assertEquals(0, info.getCount());

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createClientMessage(false));

      Thread.sleep(MessageCounterManagerImpl.MIN_SAMPLE_PERIOD * 2);
      jsonString = queueControl.listMessageCounter();
      info = MessageCounterInfo.fromJSON(jsonString);
      assertEquals(1, info.getDepth());
      assertEquals(1, info.getDepthDelta());
      assertEquals(1, info.getCount());
      assertEquals(1, info.getCountDelta());

      producer.send(session.createClientMessage(false));

      Thread.sleep(MessageCounterManagerImpl.MIN_SAMPLE_PERIOD * 2);
      jsonString = queueControl.listMessageCounter();
      info = MessageCounterInfo.fromJSON(jsonString);
      assertEquals(2, info.getDepth());
      assertEquals(1, info.getDepthDelta());
      assertEquals(2, info.getCount());
      assertEquals(1, info.getCountDelta());

      consumeMessages(2, session, queue);

      Thread.sleep(MessageCounterManagerImpl.MIN_SAMPLE_PERIOD * 2);
      jsonString = queueControl.listMessageCounter();
      info = MessageCounterInfo.fromJSON(jsonString);
      assertEquals(0, info.getDepth());
      assertEquals(-2, info.getDepthDelta());
      assertEquals(2, info.getCount());
      assertEquals(0, info.getCountDelta());

      session.deleteQueue(queue);
   }

   public void testResetMessageCounter() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, null, false);
      QueueControl queueControl = createManagementControl(address, queue);

      HornetQServerControl serverControl = createHornetQServerControl(mbeanServer);
      serverControl.enableMessageCounters();
      serverControl.setMessageCounterSamplePeriod(MessageCounterManagerImpl.MIN_SAMPLE_PERIOD);

      String jsonString = queueControl.listMessageCounter();
      MessageCounterInfo info = MessageCounterInfo.fromJSON(jsonString);

      assertEquals(0, info.getDepth());
      assertEquals(0, info.getCount());

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createClientMessage(false));

      Thread.sleep(MessageCounterManagerImpl.MIN_SAMPLE_PERIOD * 2);
      jsonString = queueControl.listMessageCounter();
      info = MessageCounterInfo.fromJSON(jsonString);
      assertEquals(1, info.getDepth());
      assertEquals(1, info.getDepthDelta());
      assertEquals(1, info.getCount());
      assertEquals(1, info.getCountDelta());

      consumeMessages(1, session, queue);

      Thread.sleep(MessageCounterManagerImpl.MIN_SAMPLE_PERIOD * 2);
      jsonString = queueControl.listMessageCounter();
      info = MessageCounterInfo.fromJSON(jsonString);
      assertEquals(0, info.getDepth());
      assertEquals(-1, info.getDepthDelta());
      assertEquals(1, info.getCount());
      assertEquals(0, info.getCountDelta());

      queueControl.resetMessageCounter();

      Thread.sleep(MessageCounterManagerImpl.MIN_SAMPLE_PERIOD * 2);
      jsonString = queueControl.listMessageCounter();
      info = MessageCounterInfo.fromJSON(jsonString);
      assertEquals(0, info.getDepth());
      assertEquals(0, info.getDepthDelta());
      assertEquals(0, info.getCount());
      assertEquals(0, info.getCountDelta());

      session.deleteQueue(queue);
   }

   public void testListMessageCounterAsHTML() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, null, false);
      QueueControl queueControl = createManagementControl(address, queue);

      String history = queueControl.listMessageCounterAsHTML();
      assertNotNull(history);

      session.deleteQueue(queue);
   }

   public void testListMessageCounterHistory() throws Exception
   {
      long counterPeriod = 1000;
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, null, false);
      QueueControl queueControl = createManagementControl(address, queue);

      HornetQServerControl serverControl = createHornetQServerControl(mbeanServer);
      serverControl.enableMessageCounters();
      serverControl.setMessageCounterSamplePeriod(counterPeriod);

      String jsonString = queueControl.listMessageCounterHistory();
      DayCounterInfo[] infos = DayCounterInfo.fromJSON(jsonString);
      assertEquals(1, infos.length);

      session.deleteQueue(queue);
   }

   public void testListMessageCounterHistoryAsHTML() throws Exception
   {
      long counterPeriod = 1000;
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, null, false);
      QueueControl queueControl = createManagementControl(address, queue);

      HornetQServerControl serverControl = createHornetQServerControl(mbeanServer);
      serverControl.enableMessageCounters();
      serverControl.setMessageCounterSamplePeriod(counterPeriod);

      String history = queueControl.listMessageCounterHistoryAsHTML();
      assertNotNull(history);

      session.deleteQueue(queue);

   }

   public void testPauseAndResume()
   {
      long counterPeriod = 1000;
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      try
      {
         session.createQueue(address, queue, null, false);
         QueueControl queueControl = createManagementControl(address, queue);

         HornetQServerControl serverControl = createHornetQServerControl(mbeanServer);
         serverControl.enableMessageCounters();
         serverControl.setMessageCounterSamplePeriod(counterPeriod);
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

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnNonPersistentSend(true);
      session = sf.createSession(false, true, false);
      session.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      session.close();

      server.stop();

      session = null;

      server = null;

      super.tearDown();
   }

   protected QueueControl createManagementControl(SimpleString address, SimpleString queue) throws Exception
   {
      QueueControl queueControl = createQueueControl(address, queue, mbeanServer);
      
      return queueControl;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
