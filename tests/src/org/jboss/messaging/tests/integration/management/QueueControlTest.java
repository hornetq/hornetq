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

import static org.jboss.messaging.tests.integration.management.ManagementControlHelper.createQueueControl;
import static org.jboss.messaging.tests.util.RandomUtil.randomBoolean;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;
import static org.jboss.messaging.tests.util.RandomUtil.randomSimpleString;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.management.MessageInfo;
import org.jboss.messaging.core.management.QueueControlMBean;
import org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.utils.SimpleString;

/**
 * A QueueControlTest
 *
 * @author jmesnil
 * 
 * Created 26 nov. 2008 14:18:48
 *
 *
 */
public class QueueControlTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private MessagingService service;
   
   private MBeanServer mbeanServer;

   private ClientSession session;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testAttributes() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      SimpleString filter = new SimpleString("color = 'blue'");
      boolean durable = randomBoolean();
      boolean temporary = false;
      
      session.createQueue(address, queue, filter, durable, temporary);

      QueueControlMBean queueControl = createQueueControl(address, queue, mbeanServer);
      assertEquals(queue.toString(), queueControl.getName());
      assertEquals(filter.toString(), queueControl.getFilter());
      assertEquals(durable, queueControl.isDurable());
      assertEquals(temporary, queueControl.isTemporary());

      session.deleteQueue(queue);
   }
   
   public void testGetNullFilter() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      
      session.createQueue(address, queue, null, false, false);

      QueueControlMBean queueControl = createQueueControl(address, queue, mbeanServer);
      assertEquals(queue.toString(), queueControl.getName());
      assertEquals(null, queueControl.getFilter());

      session.deleteQueue(queue);
   }
   
   public void testGetDeadLetterAddress() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      final SimpleString deadLetterAddress = randomSimpleString();

      session.createQueue(address, queue, null, false, false);

      QueueControlMBean queueControl = createQueueControl(address, queue, mbeanServer);
      assertNull(queueControl.getDeadLetterAddress());

      service.getServer().getAddressSettingsRepository().addMatch(address.toString(), new AddressSettings()
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

      session.createQueue(address, queue, null, false, false);

      QueueControlMBean queueControl = createQueueControl(address, queue, mbeanServer);
      queueControl.setDeadLetterAddress(deadLetterAddress);

      assertEquals(deadLetterAddress, queueControl.getDeadLetterAddress());
      
      session.deleteQueue(queue);
   }

   public void testGetExpiryAddress() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      final SimpleString expiryAddress = randomSimpleString();

      session.createQueue(address, queue, null, false, false);

      QueueControlMBean queueControl = createQueueControl(address, queue, mbeanServer);
      assertNull(queueControl.getExpiryAddress());

      service.getServer().getAddressSettingsRepository().addMatch(address.toString(), new AddressSettings()
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

      session.createQueue(address, queue, null, false, false);

      QueueControlMBean queueControl = createQueueControl(address, queue, mbeanServer);
      queueControl.setExpiryAddress(expiryAddress);

      assertEquals(expiryAddress, queueControl.getExpiryAddress());
      
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

      session.createQueue(address, queue, null, false, true);
      session.createQueue(otherAddress, otherQueue, null, false, true);
      ClientProducer producer = session.createProducer(address);
      session.start();

      // send on queue
      ClientMessage message = session.createClientMessage(false);
      SimpleString key = randomSimpleString();
      long value = randomLong();
      message.putLongProperty(key, value);
      producer.send(message);

      QueueControlMBean queueControl = createQueueControl(address, queue, mbeanServer);
      assertEquals(1, queueControl.getMessageCount());

      // moved all messages to otherQueue
      int movedMessagesCount = queueControl.moveAllMessages(otherQueue.toString());
      assertEquals(1, movedMessagesCount);
      assertEquals(0, queueControl.getMessageCount());

      // check there is no message to consume from queue
      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage m = consumer.receive(500);
      assertNull(m);

      // consume the message from otherQueue
      ClientConsumer otherConsumer = session.createConsumer(otherQueue);
      m = otherConsumer.receive(500);
      assertEquals(value, m.getProperty(key));

      m.acknowledge();

      consumer.close();
      session.deleteQueue(queue);
      otherConsumer.close();
      session.deleteQueue(otherQueue);
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

      session.createQueue(address, queue, null, false, true);
      session.createQueue(otherAddress, otherQueue, null, false, true);
      ClientProducer producer = session.createProducer(address);
      session.start();

      // send on queue
      ClientMessage matchingMessage = session.createClientMessage(false);
      matchingMessage.putLongProperty(key, matchingValue);
      producer.send(matchingMessage);
      ClientMessage unmatchingMessage = session.createClientMessage(false);
      unmatchingMessage.putLongProperty(key, unmatchingValue);
      producer.send(unmatchingMessage);

      QueueControlMBean queueControl = createQueueControl(address, queue, mbeanServer);
      assertEquals(2, queueControl.getMessageCount());

      // moved matching messages to otherQueue
      int movedMatchedMessagesCount = queueControl.moveMatchingMessages(key + " =" + matchingValue, otherQueue.toString());
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

      session.createQueue(address, queue, null, false, true);
      ClientProducer producer = session.createProducer(address);
      session.start();

      // send 2 messages on queue
      producer.send(session.createClientMessage(false));
      producer.send(session.createClientMessage(false));

      QueueControlMBean queueControl = createQueueControl(address, queue, mbeanServer);
      assertEquals(2, queueControl.getMessageCount());

      // delete all messages
      int deletedMessagesCount = queueControl.removeAllMessages();
      assertEquals(2, deletedMessagesCount);
      assertEquals(0, queueControl.getMessageCount());

      // check there is no message to consume from queue
      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage m = consumer.receive(500);
      assertNull(m);

      consumer.close();
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

      session.createQueue(address, queue, null, false, true);
      ClientProducer producer = session.createProducer(address);
      session.start();

      // send on queue
      ClientMessage matchingMessage = session.createClientMessage(false);
      matchingMessage.putLongProperty(key, matchingValue);
      producer.send(matchingMessage);
      ClientMessage unmatchingMessage = session.createClientMessage(false);
      unmatchingMessage.putLongProperty(key, unmatchingValue);
      producer.send(unmatchingMessage);

      QueueControlMBean queueControl = createQueueControl(address, queue, mbeanServer);
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

      session.createQueue(address, queue, null, false, true);
      ClientProducer producer = session.createProducer(address);
      session.start();

      // send 2 messages on queue
      producer.send(session.createClientMessage(false));
      producer.send(session.createClientMessage(false));

      QueueControlMBean queueControl = createQueueControl(address, queue, mbeanServer);
      assertEquals(2, queueControl.getMessageCount());

      // the message IDs are set on the server
      MessageInfo[] messageInfos = MessageInfo.from(queueControl.listAllMessages());
      assertEquals(2, messageInfos.length);
      long messageID = messageInfos[0].getID();

      // delete 1st message
      boolean deleted = queueControl.removeMessage(messageID);
      assertTrue(deleted);
      assertEquals(1, queueControl.getMessageCount());

      // check there is a single message to consume from queue
      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage m = consumer.receive(500);
      assertNotNull(m);
      m = consumer.receive(500);
      assertNull(m);

      consumer.close();
      session.deleteQueue(queue);
   }
   
   public void testCountMessagesWithFilter() throws Exception
   {
      SimpleString key = new SimpleString("key");
      long matchingValue = randomLong();
      long unmatchingValue = matchingValue + 1;

      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, null, false, true);
      ClientProducer producer = session.createProducer(address);
      session.start();

      // send on queue
      ClientMessage matchingMessage = session.createClientMessage(false);
      matchingMessage.putLongProperty(key, matchingValue);
      ClientMessage unmatchingMessage = session.createClientMessage(false);
      unmatchingMessage.putLongProperty(key, unmatchingValue);
      producer.send(matchingMessage);
      producer.send(unmatchingMessage);
      producer.send(matchingMessage);

      QueueControlMBean queueControl = createQueueControl(address, queue, mbeanServer);
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

      session.createQueue(address, queue, null, false, true);
      ClientProducer producer = session.createProducer(address);
      session.start();

      // send on queue
      ClientMessage matchingMessage = session.createClientMessage(false);
      matchingMessage.putLongProperty(key, matchingValue);
      producer.send(matchingMessage);
      ClientMessage unmatchingMessage = session.createClientMessage(false);
      unmatchingMessage.putLongProperty(key, unmatchingValue);
      producer.send(unmatchingMessage);

      QueueControlMBean queueControl = createQueueControl(address, queue, mbeanServer);
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
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      
      mbeanServer = MBeanServerFactory.createMBeanServer();
      
      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      service = Messaging.newNullStorageMessagingService(conf, mbeanServer);
      service.start();
      
      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnNonPersistentSend(true);
      session = sf.createSession(false, true, true);
   }

   @Override
   protected void tearDown() throws Exception
   {
      session.close();
      
      service.stop();

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
