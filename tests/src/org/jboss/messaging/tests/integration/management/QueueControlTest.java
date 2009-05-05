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

package org.jboss.messaging.tests.integration.management;

import static org.jboss.messaging.tests.integration.management.ManagementControlHelper.createMessagingServerControl;
import static org.jboss.messaging.tests.integration.management.ManagementControlHelper.createQueueControl;
import static org.jboss.messaging.tests.util.RandomUtil.randomBoolean;
import static org.jboss.messaging.tests.util.RandomUtil.randomInt;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;
import static org.jboss.messaging.tests.util.RandomUtil.randomSimpleString;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.util.Map;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.management.DayCounterInfo;
import org.jboss.messaging.core.management.MessageCounterInfo;
import org.jboss.messaging.core.management.MessagingServerControlMBean;
import org.jboss.messaging.core.management.QueueControlMBean;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.messagecounter.impl.MessageCounterManagerImpl;
import org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.utils.SimpleString;

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

   protected MessagingServer server;

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

      QueueControlMBean queueControl = createManagementControl(address, queue);
      assertEquals(queue.toString(), queueControl.getName());
      assertEquals(address.toString(), queueControl.getAddress());
      assertEquals(filter.toString(), queueControl.getFilter());
      assertEquals(durable, queueControl.isDurable());
      assertEquals(false, queueControl.isTemporary());
      assertEquals(false, queueControl.isBackup());

      session.deleteQueue(queue);
   }

   public void testGetNullFilter() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, null, false);

      QueueControlMBean queueControl = createManagementControl(address, queue);
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

      QueueControlMBean queueControl = createManagementControl(address, queue);
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

      QueueControlMBean queueControl = createManagementControl(address, queue);
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

      QueueControlMBean queueControl = createManagementControl(address, queue);
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

      QueueControlMBean queueControl = createManagementControl(address, queue);
      queueControl.setExpiryAddress(expiryAddress);

      assertEquals(expiryAddress, queueControl.getExpiryAddress());

      session.deleteQueue(queue);
   }

   public void testGetConsumerCount() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, null, false);

      QueueControlMBean queueControl = createManagementControl(address, queue);

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

      QueueControlMBean queueControl = createManagementControl(address, queue);
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

      QueueControlMBean queueControl = createManagementControl(address, queue);
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

      QueueControlMBean queueControl = createManagementControl(address, queue);
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

      QueueControlMBean queueControl = createManagementControl(address, queue);

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createClientMessage(false);
      message.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, System.currentTimeMillis() + delay);
      message.putIntProperty(new SimpleString("key"), intValue);
      producer.send(message);
      // unscheduled message
      producer.send(session.createClientMessage(false));

      Map<String, Object>[] messages = queueControl.listScheduledMessages();
      assertEquals(1, messages.length);     
      assertEquals(intValue, messages[0].get("key"));

      Thread.sleep(delay);

      messages = queueControl.listScheduledMessages();
      assertEquals(0, messages.length);

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

      QueueControlMBean queueControl = createManagementControl(address, queue);
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

   public void testListAllMessages() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      int intValue = randomInt();
      session.createQueue(address, queue, null, false);

      QueueControlMBean queueControl = createManagementControl(address, queue);

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createClientMessage(false);
      message.putIntProperty(new SimpleString("key"), intValue);
      producer.send(message);

      Map<String, Object>[] messages =  queueControl.listAllMessages();
      assertEquals(1, messages.length);     
      assertEquals(intValue, messages[0].get("key"));

      consumeMessages(1, session, queue);

      messages = queueControl.listAllMessages();
      assertEquals(0, messages.length);

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
      QueueControlMBean queueControl = createManagementControl(address, queue);

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

   /**
    * <ol>
    * <li>send a message to queue</li>
    * <li>move all messages from queue to otherQueue using management method</li>
    * <li>check there is no message to consume from queue</li>
    * <li>consume the message from otherQueue</li>
    * </ol>
    */
   public void testMoveAllMessages() throws Exception
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

      QueueControlMBean queueControl = createManagementControl(address, queue);
      assertEquals(1, queueControl.getMessageCount());

      // moved all messages to otherQueue
      int movedMessagesCount = queueControl.moveAllMessages(otherQueue.toString());
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

   public void testMoveAllMessagesToUnknownQueue() throws Exception
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

      QueueControlMBean queueControl = createManagementControl(address, queue);
      assertEquals(1, queueControl.getMessageCount());

      // moved all messages to unknown queue
      try
      {
         queueControl.moveAllMessages(unknownQueue.toString());
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

   public void testMoveMatchingMessages() throws Exception
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

      QueueControlMBean queueControl = createManagementControl(address, queue);
      assertEquals(2, queueControl.getMessageCount());

      // moved matching messages to otherQueue
      int movedMatchedMessagesCount = queueControl.moveMatchingMessages(key + " =" + matchingValue,
                                                                        otherQueue.toString());
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

      QueueControlMBean queueControl = createManagementControl(address, queue);
      QueueControlMBean otherQueueControl = createManagementControl(otherAddress, otherQueue);
      assertEquals(2, queueControl.getMessageCount());
      assertEquals(0, otherQueueControl.getMessageCount());

      // the message IDs are set on the server
      Map<String, Object>[] messages = queueControl.listAllMessages();   
      assertEquals(2, messages.length);
      long messageID = (Long)messages[0].get("MessageID");

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

      QueueControlMBean queueControl = createManagementControl(address, queue);
      assertEquals(1, queueControl.getMessageCount());

      // the message IDs are set on the server
      Map<String, Object>[] messages = queueControl.listAllMessages(); 
      assertEquals(1, messages.length);
      long messageID = (Long)messages[0].get("MessageID");

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
    * <li>send 2 messages to queue</li>
    * <li>remove all messages using management method</li>
    * <li>check there is no message to consume from queue</li>
    * <li>consume the message from otherQueue</li>
    * </ol>
    */
   public void testRemoveAllMessages() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, null, false);
      ClientProducer producer = session.createProducer(address);

      // send 2 messages on queue
      producer.send(session.createClientMessage(false));
      producer.send(session.createClientMessage(false));

      QueueControlMBean queueControl = createManagementControl(address, queue);
      assertEquals(2, queueControl.getMessageCount());

      // delete all messages
      int deletedMessagesCount = queueControl.removeAllMessages();
      assertEquals(2, deletedMessagesCount);
      assertEquals(0, queueControl.getMessageCount());

      // check there is no message to consume from queue
      consumeMessages(0, session, queue);

      session.deleteQueue(queue);
   }

   /**
    * <ol>
    * <li>send 2 message to queue</li>
    * <li>remove messages from queue using management method <em>with filter</em></li>
    * <li>check there is only one message to consume from queue</li>
    * </ol>
    */

   public void testRemoveMatchingMessages() throws Exception
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

      QueueControlMBean queueControl = createManagementControl(address, queue);
      assertEquals(2, queueControl.getMessageCount());

      // removed matching messages to otherQueue
      int removedMatchedMessagesCount = queueControl.removeMatchingMessages(key + " =" + matchingValue);
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

   public void testRemoveMessage() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, null, false);
      ClientProducer producer = session.createProducer(address);

      // send 2 messages on queue
      producer.send(session.createClientMessage(false));
      producer.send(session.createClientMessage(false));

      QueueControlMBean queueControl = createManagementControl(address, queue);
      assertEquals(2, queueControl.getMessageCount());

      // the message IDs are set on the server
      Map<String, Object>[] messages = queueControl.listAllMessages(); 
      assertEquals(2, messages.length);
      long messageID = (Long)messages[0].get("MessageID");

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

      QueueControlMBean queueControl = createManagementControl(address, queue);
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

      QueueControlMBean queueControl = createManagementControl(address, queue);
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

      QueueControlMBean queueControl = createManagementControl(address, queue);
      QueueControlMBean expiryQueueControl = createManagementControl(expiryAddress, expiryQueue);
      assertEquals(1, queueControl.getMessageCount());
      assertEquals(0, expiryQueueControl.getMessageCount());

      // the message IDs are set on the server
      Map<String, Object>[] messages = queueControl.listAllMessages();       
      assertEquals(1, messages.length);
      long messageID = (Long)messages[0].get("MessageID");

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

      QueueControlMBean queueControl = createManagementControl(address, queue);
      QueueControlMBean deadLetterQueueControl = createManagementControl(deadLetterAddress, deadLetterQueue);
      assertEquals(2, queueControl.getMessageCount());

      // the message IDs are set on the server
      Map<String, Object>[] messages = queueControl.listAllMessages();       
      assertEquals(2, messages.length);
      long messageID = (Long)messages[0].get("MessageID");

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

      QueueControlMBean queueControl = createManagementControl(address, queue);
      assertEquals(1, queueControl.getMessageCount());

      // the message IDs are set on the server
      Map<String, Object>[] messages = queueControl.listAllMessages();       
      assertEquals(1, messages.length);
      long messageID = (Long)messages[0].get("MessageID");

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

      QueueControlMBean queueControl = createManagementControl(address, queue);
      assertEquals(1, queueControl.getMessageCount());

      // the message IDs are set on the server
      Map<String, Object>[] messages = queueControl.listAllMessages();       
      assertEquals(1, messages.length);
      long messageID = (Long)messages[0].get("MessageID");

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
      QueueControlMBean queueControl = createManagementControl(address, queue);
      
      MessagingServerControlMBean serverControl = createMessagingServerControl(mbeanServer);
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
      QueueControlMBean queueControl = createManagementControl(address, queue);
      
      MessagingServerControlMBean serverControl = createMessagingServerControl(mbeanServer);
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
      QueueControlMBean queueControl = createManagementControl(address, queue);
      
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
      QueueControlMBean queueControl = createManagementControl(address, queue);
      
      MessagingServerControlMBean serverControl = createMessagingServerControl(mbeanServer);
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
      QueueControlMBean queueControl = createManagementControl(address, queue);
      
      MessagingServerControlMBean serverControl = createMessagingServerControl(mbeanServer);
      serverControl.enableMessageCounters();
      serverControl.setMessageCounterSamplePeriod(counterPeriod);

      String history = queueControl.listMessageCounterHistoryAsHTML();
      assertNotNull(history);

      session.deleteQueue(queue);
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

      super.tearDown();
   }

   protected QueueControlMBean createManagementControl(SimpleString address, SimpleString queue) throws Exception
   {
      QueueControlMBean queueControl = createQueueControl(address, queue, mbeanServer);
      return queueControl;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
