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
import static org.jboss.messaging.tests.util.RandomUtil.randomBoolean;
import static org.jboss.messaging.tests.util.RandomUtil.randomInt;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;
import static org.jboss.messaging.tests.util.RandomUtil.randomSimpleString;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.DayCounterInfo;
import org.jboss.messaging.core.management.MessageCounterInfo;
import org.jboss.messaging.core.management.MessagingServerControlMBean;
import org.jboss.messaging.core.management.ResourceNames;
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
public class QueueControlUsingCoreTest extends ManagementTestBase
{

   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(QueueControlUsingCoreTest.class);

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

      CoreMessagingProxy proxy = createProxy(queue);
      assertEquals(queue.toString(), proxy.retrieveAttributeValue("Name"));
      assertEquals(address.toString(), proxy.retrieveAttributeValue("Address"));
      assertEquals(filter.toString(), proxy.retrieveAttributeValue("Filter"));
      assertEquals(durable, proxy.retrieveAttributeValue("Durable"));
      assertEquals(false, proxy.retrieveAttributeValue("Temporary"));
      assertEquals(false, proxy.retrieveAttributeValue("Backup"));

      session.deleteQueue(queue);
   }

   public void testGetNullFilter() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, null, false);

      CoreMessagingProxy proxy = createProxy(queue);
      assertEquals(queue.toString(), proxy.retrieveAttributeValue("Name"));
      assertEquals(null, proxy.retrieveAttributeValue("Filter"));

      session.deleteQueue(queue);
   }

   public void testGetDeadLetterAddress() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      final SimpleString deadLetterAddress = randomSimpleString();

      session.createQueue(address, queue, null, false);

      CoreMessagingProxy proxy = createProxy(queue);
      assertNull(proxy.retrieveAttributeValue("DeadLetterAddress"));

      server.getAddressSettingsRepository().addMatch(address.toString(), new AddressSettings()
      {
         @Override
         public SimpleString getDeadLetterAddress()
         {
            return deadLetterAddress;
         }
      });

      assertEquals(deadLetterAddress.toString(), proxy.retrieveAttributeValue("DeadLetterAddress"));

      session.deleteQueue(queue);
   }

   public void testSetDeadLetterAddress() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      String deadLetterAddress = randomString();

      session.createQueue(address, queue, null, false);

      CoreMessagingProxy proxy = createProxy(queue);
      proxy.invokeOperation("setDeadLetterAddress", deadLetterAddress);

      assertEquals(deadLetterAddress, proxy.retrieveAttributeValue("DeadLetterAddress"));

      session.deleteQueue(queue);
   }

   public void testGetExpiryAddress() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      final SimpleString expiryAddress = randomSimpleString();

      session.createQueue(address, queue, null, false);

      CoreMessagingProxy proxy = createProxy(queue);
      assertNull(proxy.retrieveAttributeValue("ExpiryAddress"));

      server.getAddressSettingsRepository().addMatch(address.toString(), new AddressSettings()
      {
         @Override
         public SimpleString getExpiryAddress()
         {
            return expiryAddress;
         }
      });

      assertEquals(expiryAddress.toString(), proxy.retrieveAttributeValue("ExpiryAddress"));

      session.deleteQueue(queue);
   }

   public void testSetExpiryAddress() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      String expiryAddress = randomString();

      session.createQueue(address, queue, null, false);

      CoreMessagingProxy proxy = createProxy(queue);
      proxy.invokeOperation("setExpiryAddress", expiryAddress);

      assertEquals(expiryAddress, proxy.retrieveAttributeValue("ExpiryAddress"));

      session.deleteQueue(queue);
   }

   public void testGetConsumerCount() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, null, false);

      CoreMessagingProxy proxy = createProxy(queue);

      assertEquals(0, proxy.retrieveAttributeValue("ConsumerCount"));

      ClientConsumer consumer = session.createConsumer(queue);
      assertEquals(1, proxy.retrieveAttributeValue("ConsumerCount"));

      consumer.close();
      assertEquals(0, proxy.retrieveAttributeValue("ConsumerCount"));

      session.deleteQueue(queue);
   }

   public void testGetMessageCount() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, null, false);

      CoreMessagingProxy proxy = createProxy(queue);
      assertEquals(0, proxy.retrieveAttributeValue("MessageCount"));

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createClientMessage(false));
      assertEquals(1, proxy.retrieveAttributeValue("MessageCount"));

      consumeMessages(1, session, queue);

      assertEquals(0, proxy.retrieveAttributeValue("MessageCount"));

      session.deleteQueue(queue);
   }

   public void testGetMessagesAdded() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, null, false);

      CoreMessagingProxy proxy = createProxy(queue);
      assertEquals(0, proxy.retrieveAttributeValue("MessagesAdded"));

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createClientMessage(false));
      assertEquals(1, proxy.retrieveAttributeValue("MessagesAdded"));
      producer.send(session.createClientMessage(false));
      assertEquals(2, proxy.retrieveAttributeValue("MessagesAdded"));

      consumeMessages(2, session, queue);

      assertEquals(2, proxy.retrieveAttributeValue("MessagesAdded"));

      session.deleteQueue(queue);
   }

   public void testGetScheduledCount() throws Exception
   {
      long delay = 2000;
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, null, false);

      CoreMessagingProxy proxy = createProxy(queue);
      assertEquals(0, proxy.retrieveAttributeValue("ScheduledCount"));

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createClientMessage(false);
      message.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, System.currentTimeMillis() + delay);
      producer.send(message);

      assertEquals(1, proxy.retrieveAttributeValue("ScheduledCount"));
      consumeMessages(0, session, queue);

      Thread.sleep(delay);

      assertEquals(0, proxy.retrieveAttributeValue("ScheduledCount"));
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

      CoreMessagingProxy proxy = createProxy(queue);

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createClientMessage(false);
      message.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, System.currentTimeMillis() + delay);
      message.putIntProperty(new SimpleString("key"), intValue);
      producer.send(message);
      // unscheduled message
      producer.send(session.createClientMessage(false));

      Object[] data = (Object[])proxy.invokeOperation("listScheduledMessages");
      assertEquals(1, data.length);     
      Map messageReceived = (Map)data[0];
      Map properties = (Map)messageReceived.get("properties");
      
      Set entries = properties.entrySet();      
      Iterator iter = entries.iterator();
      while (iter.hasNext())
      {
         Map.Entry entry = (Map.Entry)iter.next();
         
         log.info("key: " + entry.getKey() + " value: " + entry.getValue());
      }
      
      
      assertEquals(intValue, properties.get("key"));

      Thread.sleep(delay);

      data = (Object[])proxy.invokeOperation("listScheduledMessages");
      assertEquals(0, data.length);

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

      CoreMessagingProxy proxy = createProxy(queue);
      assertEquals(0, proxy.retrieveAttributeValue("DeliveringCount"));

      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage message = consumer.receive(500);
      assertNotNull(message);
      assertEquals(1, proxy.retrieveAttributeValue("DeliveringCount"));

      message.acknowledge();
      session.commit();
      assertEquals(0, proxy.retrieveAttributeValue("DeliveringCount"));

      consumer.close();
      session.deleteQueue(queue);
   }

   public void testListAllMessages() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      int intValue = randomInt();
      session.createQueue(address, queue, null, false);

      CoreMessagingProxy proxy = createProxy(queue);

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createClientMessage(false);
      message.putIntProperty(new SimpleString("key"), intValue);
      producer.send(message);

      Object[] data = (Object[])proxy.invokeOperation("listAllMessages", null) ;
      assertEquals(1, data.length);
      //MessageInfo[] messageInfos = MessageInfo.from(data);
      Map messageReceived = (Map)data[0];
      Map properties = (Map)messageReceived.get("properties");
      assertEquals(intValue, properties.get("key"));

      consumeMessages(1, session, queue);

      data = (Object[])proxy.invokeOperation("listAllMessages", null) ;
      assertEquals(0, data.length);

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
      CoreMessagingProxy proxy = createProxy(queue);

      ClientProducer producer = session.createProducer(address);
      ClientMessage matchingMessage = session.createClientMessage(false);
      matchingMessage.putLongProperty(key, matchingValue);
      producer.send(matchingMessage);
      ClientMessage unmatchingMessage = session.createClientMessage(false);
      unmatchingMessage.putLongProperty(key, unmatchingValue);
      producer.send(unmatchingMessage);

      Object[] data = (Object[])proxy.invokeOperation("listMessages", filter);
      assertEquals(1, data.length);
     // MessageInfo[] messageInfos = MessageInfo.from(data);
      Map messageReceived = (Map)data[0];
      Map properties = (Map)messageReceived.get("properties");
      assertEquals(matchingValue, properties.get("key"));

      consumeMessages(2, session, queue);

      data = (Object[])proxy.invokeOperation("listMessages", filter);
      assertEquals(0, data.length);

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

      CoreMessagingProxy proxy = createProxy(queue);
      assertEquals(1, proxy.retrieveAttributeValue("MessageCount"));

      // moved all messages to otherQueue
      int movedMessagesCount = (Integer)proxy.invokeOperation("moveAllMessages", otherQueue.toString());
      assertEquals(1, movedMessagesCount);
      assertEquals(0, proxy.retrieveAttributeValue("MessageCount"));

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

      CoreMessagingProxy proxy = createProxy(queue);
      assertEquals(1, proxy.retrieveAttributeValue("MessageCount"));

      // moved all messages to unknown queue
      try
      {
         proxy.invokeOperation("moveAllMessages", unknownQueue.toString());
         fail("operation must fail if the other queue does not exist");
      }
      catch (Exception e)
      {
      }
      assertEquals(1, proxy.retrieveAttributeValue("MessageCount"));

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

      CoreMessagingProxy proxy = createProxy(queue);
      assertEquals(2, proxy.retrieveAttributeValue("MessageCount"));

      // moved matching messages to otherQueue
      int movedMatchedMessagesCount = (Integer)proxy.invokeOperation("moveMatchingMessages",
                                                                     key + " =" + matchingValue,
                                                                        otherQueue.toString());
      assertEquals(1, movedMatchedMessagesCount);
      assertEquals(1, proxy.retrieveAttributeValue("MessageCount"));

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

      CoreMessagingProxy proxy = createProxy(queue);
      CoreMessagingProxy otherproxy = createProxy(otherQueue);
      assertEquals(2, proxy.retrieveAttributeValue("MessageCount"));
      assertEquals(0, otherproxy.retrieveAttributeValue("MessageCount"));

      // the message IDs are set on the server
      Object[] data = (Object[])proxy.invokeOperation("listAllMessages");
      Map messageReceived = (Map)data[0];
      assertEquals(2, data.length);
      long messageID = (Long)messageReceived.get("MessageID");

      boolean moved = (Boolean)proxy.invokeOperation("moveMessage", messageID, otherQueue.toString());
      assertTrue(moved);
      assertEquals(1, proxy.retrieveAttributeValue("MessageCount"));
      assertEquals(1, otherproxy.retrieveAttributeValue("MessageCount"));

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

      CoreMessagingProxy proxy = createProxy(queue);
      assertEquals(1, proxy.retrieveAttributeValue("MessageCount"));

      // the message IDs are set on the server
      Object[] data = (Object[])proxy.invokeOperation("listAllMessages");
      assertEquals(1, data.length);
      Map messageReceived = (Map)data[0];
      long messageID = (Long)messageReceived.get("MessageID");

      // moved all messages to unknown queue
      try
      {
         proxy.invokeOperation("moveMessage", messageID, unknownQueue.toString());
         fail("operation must fail if the other queue does not exist");
      }
      catch (Exception e)
      {
      }
      assertEquals(1, proxy.retrieveAttributeValue("MessageCount"));

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

      CoreMessagingProxy proxy = createProxy(queue);
      assertEquals(2, proxy.retrieveAttributeValue("MessageCount"));

      // delete all messages
      int deletedMessagesCount = (Integer)proxy.invokeOperation("removeAllMessages");
      assertEquals(2, deletedMessagesCount);
      assertEquals(0, proxy.retrieveAttributeValue("MessageCount"));

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

      CoreMessagingProxy proxy = createProxy(queue);
      assertEquals(2, proxy.retrieveAttributeValue("MessageCount"));

      // removed matching messages to otherQueue
      int removedMatchedMessagesCount = (Integer)proxy.invokeOperation("removeMatchingMessages", key + " =" + matchingValue);
      assertEquals(1, removedMatchedMessagesCount);
      assertEquals(1, proxy.retrieveAttributeValue("MessageCount"));

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
      SimpleString address = randomSimpleString();      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, null, false);
      ClientProducer producer = session.createProducer(address);

      // send 2 messages on queue
      producer.send(session.createClientMessage(false));
      producer.send(session.createClientMessage(false));

      CoreMessagingProxy proxy = createProxy(queue);
      assertEquals(2, proxy.retrieveAttributeValue("MessageCount"));

      // the message IDs are set on the server
      Object[] data = (Object[])proxy.invokeOperation("listAllMessages", null);    
      assertEquals(2, data.length);
      Map message = (Map)data[0];
      long messageID = (Long)message.get("MessageID");

      // delete 1st message
      boolean deleted = (Boolean)proxy.invokeOperation("removeMessage", messageID);
      assertTrue(deleted);
      assertEquals(1, proxy.retrieveAttributeValue("MessageCount"));

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

      CoreMessagingProxy proxy = createProxy(queue);
      assertEquals(3, proxy.retrieveAttributeValue("MessageCount"));

      assertEquals(2, proxy.invokeOperation("countMessages", key + " =" + matchingValue));
      assertEquals(1, proxy.invokeOperation("countMessages", key + " =" + unmatchingValue));

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

      CoreMessagingProxy proxy = createProxy(queue);
      assertEquals(2, proxy.retrieveAttributeValue("MessageCount"));

      int expiredMessagesCount = (Integer)proxy.invokeOperation("expireMessages", key + " =" + matchingValue);
      assertEquals(1, expiredMessagesCount);
      assertEquals(1, proxy.retrieveAttributeValue("MessageCount"));

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

      CoreMessagingProxy proxy = createProxy(queue);
 
      CoreMessagingProxy expiryproxy = createProxy(expiryQueue);
      
      assertEquals(1, proxy.retrieveAttributeValue("MessageCount"));
      assertEquals(0, expiryproxy.retrieveAttributeValue("MessageCount"));

      // the message IDs are set on the server
      Object[] data = (Object[])proxy.invokeOperation("listAllMessages");    
      assertEquals(1, data.length);
      Map messageReceived = (Map)data[0];
      long messageID = (Long)messageReceived.get("MessageID");

      proxy.invokeOperation("setExpiryAddress", expiryAddress.toString());
      boolean expired = (Boolean)proxy.invokeOperation("expireMessage", messageID);
      assertTrue(expired);
      assertEquals(0, proxy.retrieveAttributeValue("MessageCount"));
      assertEquals(1, expiryproxy.retrieveAttributeValue("MessageCount"));

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

      CoreMessagingProxy proxy = createProxy(queue);
      CoreMessagingProxy deadLetterproxy = createProxy(deadLetterQueue);
      
      assertEquals(2, proxy.retrieveAttributeValue("MessageCount"));

      // the message IDs are set on the server
      Object[] data = (Object[])proxy.invokeOperation("listAllMessages");    
      assertEquals(2, data.length);
      Map message = (Map)data[0];
      long messageID = (Long)message.get("MessageID");

      proxy.invokeOperation("setDeadLetterAddress", deadLetterAddress.toString());

      assertEquals(0, deadLetterproxy.retrieveAttributeValue("MessageCount"));
      boolean movedToDeadLetterAddress = (Boolean)proxy.invokeOperation("sendMessageToDeadLetterAddress", messageID);
      assertTrue(movedToDeadLetterAddress);
      assertEquals(1, proxy.retrieveAttributeValue("MessageCount"));
      assertEquals(1, deadLetterproxy.retrieveAttributeValue("MessageCount"));

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

      CoreMessagingProxy proxy = createProxy(queue);
      assertEquals(1, proxy.retrieveAttributeValue("MessageCount"));

      // the message IDs are set on the server
      Object[] messages = (Object[])proxy.invokeOperation("listAllMessages");
      assertEquals(1, messages.length);
      long messageID = (Long)((Map)messages[0]).get("MessageID");

      boolean priorityChanged = (Boolean)proxy.invokeOperation("changeMessagePriority", messageID, newPriority);
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

      CoreMessagingProxy proxy = createProxy(queue);
      assertEquals(1, proxy.retrieveAttributeValue("MessageCount"));

      // the message IDs are set on the server
      Object[] data = (Object[])proxy.invokeOperation("listAllMessages");    
      assertEquals(1, data.length);
      Map messageReceived = (Map)data[0];
      long messageID = (Long)messageReceived.get("MessageID");

      try
      {
         proxy.invokeOperation("changeMessagePriority", messageID, invalidPriority);
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
      CoreMessagingProxy proxy = createProxy(queue);

      MessagingServerControlMBean serverControl = createMessagingServerControl(mbeanServer);
      serverControl.enableMessageCounters();
      serverControl.setMessageCounterSamplePeriod(MessageCounterManagerImpl.MIN_SAMPLE_PERIOD);

      String jsonString = (String)proxy.invokeOperation("listMessageCounter");     
      MessageCounterInfo info = MessageCounterInfo.fromJSON(jsonString);
      assertEquals(0, info.getDepth());
      assertEquals(0, info.getCount());

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createClientMessage(false));

      Thread.sleep(MessageCounterManagerImpl.MIN_SAMPLE_PERIOD * 2);
      jsonString = (String)proxy.invokeOperation("listMessageCounter");     
      info = MessageCounterInfo.fromJSON(jsonString);
      assertEquals(1, info.getDepth());
      assertEquals(1, info.getDepthDelta());
      assertEquals(1, info.getCount());
      assertEquals(1, info.getCountDelta());

      producer.send(session.createClientMessage(false));

      Thread.sleep(MessageCounterManagerImpl.MIN_SAMPLE_PERIOD * 2);
      jsonString = (String)proxy.invokeOperation("listMessageCounter");     
      info = MessageCounterInfo.fromJSON(jsonString);
      assertEquals(2, info.getDepth());
      assertEquals(1, info.getDepthDelta());
      assertEquals(2, info.getCount());
      assertEquals(1, info.getCountDelta());

      consumeMessages(2, session, queue);

      Thread.sleep(MessageCounterManagerImpl.MIN_SAMPLE_PERIOD * 2);
      jsonString = (String)proxy.invokeOperation("listMessageCounter");     
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
      CoreMessagingProxy proxy = createProxy(queue);

      MessagingServerControlMBean serverControl = createMessagingServerControl(mbeanServer);
      serverControl.enableMessageCounters();
      serverControl.setMessageCounterSamplePeriod(MessageCounterManagerImpl.MIN_SAMPLE_PERIOD);

      String jsonString = (String)proxy.invokeOperation("listMessageCounter");     
      MessageCounterInfo info = MessageCounterInfo.fromJSON(jsonString);
      assertEquals(0, info.getDepth());
      assertEquals(0, info.getCount());

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createClientMessage(false));

      Thread.sleep(MessageCounterManagerImpl.MIN_SAMPLE_PERIOD * 2);
      jsonString = (String)proxy.invokeOperation("listMessageCounter");     
      info = MessageCounterInfo.fromJSON(jsonString);
      assertEquals(1, info.getDepth());
      assertEquals(1, info.getDepthDelta());
      assertEquals(1, info.getCount());
      assertEquals(1, info.getCountDelta());

      consumeMessages(1, session, queue);

      Thread.sleep(MessageCounterManagerImpl.MIN_SAMPLE_PERIOD * 2);
      jsonString = (String)proxy.invokeOperation("listMessageCounter");     
      info = MessageCounterInfo.fromJSON(jsonString);
      assertEquals(0, info.getDepth());
      assertEquals(-1, info.getDepthDelta());
      assertEquals(1, info.getCount());
      assertEquals(0, info.getCountDelta());

      proxy.invokeOperation("resetMessageCounter") ;

      Thread.sleep(MessageCounterManagerImpl.MIN_SAMPLE_PERIOD * 2);
      jsonString = (String)proxy.invokeOperation("listMessageCounter");     
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
      CoreMessagingProxy proxy = createProxy(queue);

      String history = (String)proxy.invokeOperation("listMessageCounterAsHTML");
      assertNotNull(history);

      session.deleteQueue(queue);
   }

   public void testListMessageCounterHistory() throws Exception
   {
      long counterPeriod = 1000;
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, null, false);
      CoreMessagingProxy proxy = createProxy(queue);

      MessagingServerControlMBean serverControl = createMessagingServerControl(mbeanServer);
      serverControl.enableMessageCounters();
      serverControl.setMessageCounterSamplePeriod(counterPeriod);

      String jsonString = (String)proxy.invokeOperation("listMessageCounterHistory");
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
      CoreMessagingProxy proxy = createProxy(queue);

      MessagingServerControlMBean serverControl = createMessagingServerControl(mbeanServer);
      serverControl.enableMessageCounters();
      serverControl.setMessageCounterSamplePeriod(counterPeriod);

      String history = (String)proxy.invokeOperation("listMessageCounterHistoryAsHTML");
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

   protected CoreMessagingProxy createProxy(final SimpleString queue) throws Exception
   {
      CoreMessagingProxy proxy = new CoreMessagingProxy(session, ResourceNames.CORE_QUEUE + queue);

      return proxy;
   }
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
