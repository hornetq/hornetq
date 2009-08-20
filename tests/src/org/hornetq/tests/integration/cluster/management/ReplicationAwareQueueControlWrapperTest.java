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

package org.hornetq.tests.integration.cluster.management;

import static org.hornetq.tests.integration.management.ManagementControlHelper.createQueueControl;
import static org.hornetq.tests.util.RandomUtil.randomLong;
import static org.hornetq.tests.util.RandomUtil.randomSimpleString;
import static org.hornetq.tests.util.RandomUtil.randomString;

import java.util.Map;

import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.management.QueueControl;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.utils.SimpleString;

/**
 * A ReplicationAwareQueueControlWrapperTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ReplicationAwareQueueControlWrapperTest extends ReplicationAwareTestBase
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ReplicationAwareQueueControlWrapperTest.class);

   // Attributes ----------------------------------------------------

   private ClientSession session;

   private SimpleString address;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testChangeMessagePriority() throws Exception
   {
      byte oldPriority = (byte)1;
      byte newPriority = (byte)8;

      QueueControl liveQueueControl = createQueueControl(address, address, liveMBeanServer);
      QueueControl backupQueueControl = createQueueControl(address, address, backupMBeanServer);

      assertFalse(liveQueueControl.isBackup());
      assertTrue(backupQueueControl.isBackup());

      // send 1 message
      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createClientMessage(false);
      message.setPriority(oldPriority);
      producer.send(message);

      // check it is on both live & backup nodes
      assertEquals(1, liveQueueControl.getMessageCount());
      assertEquals(1, backupQueueControl.getMessageCount());

      Map<String, Object>[] messages = liveQueueControl.listMessages(null);
      assertEquals(1, messages.length);
      long messageID = (Long)messages[0].get("messageID");
      assertEquals(oldPriority, messages[0].get("priority"));

      messages = backupQueueControl.listMessages(null);
      assertEquals(1, messages.length);
      assertEquals(oldPriority, messages[0].get("priority"));

      assertTrue(liveQueueControl.changeMessagePriority(messageID, newPriority));

      // check the priority is changed on both live & backup nodes
      messages = liveQueueControl.listMessages(null);
      assertEquals(1, messages.length);
      assertEquals(newPriority, messages[0].get("priority"));

      messages = backupQueueControl.listMessages(null);
      assertEquals(1, messages.length);
      assertEquals(newPriority, messages[0].get("priority"));
   }

   public void testExpireMessage() throws Exception
   {
      QueueControl liveQueueControl = createQueueControl(address, address, liveMBeanServer);
      QueueControl backupQueueControl = createQueueControl(address, address, backupMBeanServer);

      assertFalse(liveQueueControl.isBackup());
      assertTrue(backupQueueControl.isBackup());

      // send 1 message
      ClientProducer producer = session.createProducer(address);
      producer.send(session.createClientMessage(false));

      // check it is on both live & backup nodes
      assertEquals(1, liveQueueControl.getMessageCount());
      assertEquals(1, backupQueueControl.getMessageCount());

      Map<String, Object>[] messages = liveQueueControl.listMessages(null);
      assertEquals(1, messages.length);
      long messageID = (Long)messages[0].get("messageID");

      assertTrue(liveQueueControl.expireMessage(messageID));

      // check the message is no longer in the queue on both live & backup nodes
      assertEquals(0, liveQueueControl.getMessageCount());
      assertEquals(0, backupQueueControl.getMessageCount());
   }

   public void testExpireMessagesWithFilter() throws Exception
   {
      SimpleString key = new SimpleString("key");
      long matchingValue = randomLong();
      long unmatchingValue = matchingValue + 1;

      QueueControl liveQueueControl = createQueueControl(address, address, liveMBeanServer);
      QueueControl backupQueueControl = createQueueControl(address, address, backupMBeanServer);

      assertFalse(liveQueueControl.isBackup());
      assertTrue(backupQueueControl.isBackup());

      // send 1 message
      ClientProducer producer = session.createProducer(address);
      ClientMessage unmatchingMessage = session.createClientMessage(false);
      unmatchingMessage.putLongProperty(key, unmatchingValue);
      producer.send(unmatchingMessage);
      ClientMessage matchingMessage = session.createClientMessage(false);
      matchingMessage.putLongProperty(key, matchingValue);
      producer.send(matchingMessage);

      // check messages are on both live & backup nodes
      assertEquals(2, liveQueueControl.getMessageCount());
      assertEquals(2, backupQueueControl.getMessageCount());

      assertEquals(1, liveQueueControl.expireMessages(key + " =" + matchingValue));

      // check there is only 1 message in the queue on both live & backup nodes
      assertEquals(1, liveQueueControl.getMessageCount());
      assertEquals(1, backupQueueControl.getMessageCount());
   }

   public void testMoveAllMessages() throws Exception
   {
      SimpleString otherQueue = randomSimpleString();

      session.createQueue(otherQueue, otherQueue, null, false);
      ClientProducer producer = session.createProducer(address);

      QueueControl liveQueueControl = createQueueControl(address, address, liveMBeanServer);
      QueueControl backupQueueControl = createQueueControl(address, address, backupMBeanServer);
      QueueControl liveOtherQueueControl = createQueueControl(otherQueue, otherQueue, liveMBeanServer);
      QueueControl backupOtherQueueControl = createQueueControl(otherQueue, otherQueue, backupMBeanServer);

      assertFalse(liveQueueControl.isBackup());
      assertTrue(backupQueueControl.isBackup());
      assertFalse(liveOtherQueueControl.isBackup());
      assertTrue(backupOtherQueueControl.isBackup());

      // send on queue
      ClientMessage message = session.createClientMessage(false);
      SimpleString key = randomSimpleString();
      long value = randomLong();
      message.putLongProperty(key, value);
      producer.send(message);

      assertEquals(1, liveQueueControl.getMessageCount());
      assertEquals(1, backupQueueControl.getMessageCount());
      assertEquals(0, liveOtherQueueControl.getMessageCount());
      assertEquals(0, backupOtherQueueControl.getMessageCount());

      // moved all messages to otherQueue
      int movedMessagesCount = liveQueueControl.moveMessages(null, otherQueue.toString());
      assertEquals(1, movedMessagesCount);

      assertEquals(0, liveQueueControl.getMessageCount());
      assertEquals(0, backupQueueControl.getMessageCount());
      assertEquals(1, liveOtherQueueControl.getMessageCount());
      assertEquals(1, backupOtherQueueControl.getMessageCount());

      session.deleteQueue(otherQueue);
   }

   public void testMoveMatchingMessages() throws Exception
   {
      SimpleString key = new SimpleString("key");
      long matchingValue = randomLong();
      long unmatchingValue = matchingValue + 1;

      SimpleString otherQueue = randomSimpleString();

      session.createQueue(otherQueue, otherQueue, null, false);

      QueueControl liveQueueControl = createQueueControl(address, address, liveMBeanServer);
      QueueControl backupQueueControl = createQueueControl(address, address, backupMBeanServer);
      QueueControl liveOtherQueueControl = createQueueControl(otherQueue, otherQueue, liveMBeanServer);
      QueueControl backupOtherQueueControl = createQueueControl(otherQueue, otherQueue, backupMBeanServer);

      // send on queue
      ClientProducer producer = session.createProducer(address);
      ClientMessage unmatchingMessage = session.createClientMessage(false);
      unmatchingMessage.putLongProperty(key, unmatchingValue);
      producer.send(unmatchingMessage);
      ClientMessage matchingMessage = session.createClientMessage(false);
      matchingMessage.putLongProperty(key, matchingValue);
      producer.send(matchingMessage);

      assertEquals(2, liveQueueControl.getMessageCount());
      assertEquals(2, backupQueueControl.getMessageCount());
      assertEquals(0, liveOtherQueueControl.getMessageCount());
      assertEquals(0, backupOtherQueueControl.getMessageCount());

      // moved matching messages to otherQueue
      int movedMatchedMessagesCount = liveQueueControl.moveMessages(key + " =" + matchingValue,
                                                                            otherQueue.toString());
      assertEquals(1, movedMatchedMessagesCount);

      assertEquals(1, liveQueueControl.getMessageCount());
      assertEquals(1, backupQueueControl.getMessageCount());
      assertEquals(1, liveOtherQueueControl.getMessageCount());
      assertEquals(1, backupOtherQueueControl.getMessageCount());

      session.deleteQueue(otherQueue);
   }

   public void testMoveMessage() throws Exception
   {
      SimpleString otherQueue = randomSimpleString();

      session.createQueue(otherQueue, otherQueue, null, false);

      QueueControl liveQueueControl = createQueueControl(address, address, liveMBeanServer);
      QueueControl backupQueueControl = createQueueControl(address, address, backupMBeanServer);
      QueueControl liveOtherQueueControl = createQueueControl(otherQueue, otherQueue, liveMBeanServer);
      QueueControl backupOtherQueueControl = createQueueControl(otherQueue, otherQueue, backupMBeanServer);

      assertFalse(liveQueueControl.isBackup());
      assertTrue(backupQueueControl.isBackup());

      // send 1 message
      ClientProducer producer = session.createProducer(address);
      producer.send(session.createClientMessage(false));

      // check it is on both live & backup nodes
      assertEquals(1, liveQueueControl.getMessageCount());
      assertEquals(1, backupQueueControl.getMessageCount());
      assertEquals(0, liveOtherQueueControl.getMessageCount());
      assertEquals(0, backupOtherQueueControl.getMessageCount());

      Map<String, Object>[] messages = liveQueueControl.listMessages(null);
      assertEquals(1, messages.length);
      long messageID = (Long)messages[0].get("messageID");

      assertTrue(liveQueueControl.moveMessage(messageID, otherQueue.toString()));

      // check the message is no longer in the queue on both live & backup nodes
      assertEquals(0, liveQueueControl.getMessageCount());
      assertEquals(0, backupQueueControl.getMessageCount());
      assertEquals(1, liveOtherQueueControl.getMessageCount());
      assertEquals(1, backupOtherQueueControl.getMessageCount());

      session.deleteQueue(otherQueue);
   }

   public void testRemoveAllMessages() throws Exception
   {
      QueueControl liveQueueControl = createQueueControl(address, address, liveMBeanServer);
      QueueControl backupQueueControl = createQueueControl(address, address, backupMBeanServer);

      assertFalse(liveQueueControl.isBackup());
      assertTrue(backupQueueControl.isBackup());

      // send 1 message
      ClientProducer producer = session.createProducer(address);
      producer.send(session.createClientMessage(false));

      // check it is on both live & backup nodes
      assertEquals(1, liveQueueControl.getMessageCount());
      assertEquals(1, backupQueueControl.getMessageCount());

      // remove all messages
      int count = liveQueueControl.removeMessages(null);
      assertEquals(1, count);

      // check there are no messages on both live & backup nodes
      assertEquals(0, liveQueueControl.getMessageCount());
      assertEquals(0, backupQueueControl.getMessageCount());
   }

   public void testRemoveMatchingMessages() throws Exception
   {
      SimpleString key = new SimpleString("key");
      long matchingValue = randomLong();
      long unmatchingValue = matchingValue + 1;

      QueueControl liveQueueControl = createQueueControl(address, address, liveMBeanServer);
      QueueControl backupQueueControl = createQueueControl(address, address, backupMBeanServer);

      // send on queue
      ClientProducer producer = session.createProducer(address);
      ClientMessage unmatchingMessage = session.createClientMessage(false);
      unmatchingMessage.putLongProperty(key, unmatchingValue);
      producer.send(unmatchingMessage);
      ClientMessage matchingMessage = session.createClientMessage(false);
      matchingMessage.putLongProperty(key, matchingValue);
      producer.send(matchingMessage);

      assertEquals(2, liveQueueControl.getMessageCount());
      assertEquals(2, backupQueueControl.getMessageCount());

      // removed matching messages
      int removedMatchedMessagesCount = liveQueueControl.removeMessages(key + " =" + matchingValue);
      assertEquals(1, removedMatchedMessagesCount);

      assertEquals(1, liveQueueControl.getMessageCount());
      assertEquals(1, backupQueueControl.getMessageCount());
   }

   public void testRemoveMessage() throws Exception
   {
      QueueControl liveQueueControl = createQueueControl(address, address, liveMBeanServer);
      QueueControl backupQueueControl = createQueueControl(address, address, backupMBeanServer);

      assertFalse(liveQueueControl.isBackup());
      assertTrue(backupQueueControl.isBackup());

      // send 1 message
      ClientProducer producer = session.createProducer(address);
      producer.send(session.createClientMessage(false));

      // check it is on both live & backup nodes
      assertEquals(1, liveQueueControl.getMessageCount());
      assertEquals(1, backupQueueControl.getMessageCount());

      Map<String, Object>[] messages = liveQueueControl.listMessages(null);
      assertEquals(1, messages.length);
      long messageID = (Long)messages[0].get("messageID");

      assertTrue(liveQueueControl.removeMessage(messageID));

      // check the message is no longer in the queue on both live & backup nodes
      assertEquals(0, liveQueueControl.getMessageCount());
      assertEquals(0, backupQueueControl.getMessageCount());
   }

   public void testSendMessageToDeadLetterAddress() throws Exception
   {
      QueueControl liveQueueControl = createQueueControl(address, address, liveMBeanServer);
      QueueControl backupQueueControl = createQueueControl(address, address, backupMBeanServer);

      assertFalse(liveQueueControl.isBackup());
      assertTrue(backupQueueControl.isBackup());

      // send 1 message
      ClientProducer producer = session.createProducer(address);
      producer.send(session.createClientMessage(false));

      // check it is on both live & backup nodes
      assertEquals(1, liveQueueControl.getMessageCount());
      assertEquals(1, backupQueueControl.getMessageCount());

      Map<String, Object>[] messages = liveQueueControl.listMessages(null);
      assertEquals(1, messages.length);
      long messageID = (Long)messages[0].get("messageID");

      assertTrue(liveQueueControl.sendMessageToDeadLetterAddress(messageID));

      // check the message is no longer in the queue on both live & backup nodes
      assertEquals(0, liveQueueControl.getMessageCount());
      assertEquals(0, backupQueueControl.getMessageCount());
   }

   public void testSetDeadLetterAddress() throws Exception
   {
      String deadLetterAddress = randomString();

      QueueControl liveQueueControl = createQueueControl(address, address, liveMBeanServer);
      QueueControl backupQueueControl = createQueueControl(address, address, backupMBeanServer);

      assertFalse(liveQueueControl.isBackup());
      assertTrue(backupQueueControl.isBackup());

      assertNull(liveQueueControl.getDeadLetterAddress());
      assertNull(backupQueueControl.getDeadLetterAddress());

      liveQueueControl.setDeadLetterAddress(deadLetterAddress);

      assertEquals(deadLetterAddress, liveQueueControl.getDeadLetterAddress());
      assertEquals(deadLetterAddress, backupQueueControl.getDeadLetterAddress());
   }

   public void testSetExpiryAddress() throws Exception
   {
      String expiryAddress = randomString();

      QueueControl liveQueueControl = createQueueControl(address, address, liveMBeanServer);
      QueueControl backupQueueControl = createQueueControl(address, address, backupMBeanServer);

      assertFalse(liveQueueControl.isBackup());
      assertTrue(backupQueueControl.isBackup());

      assertNull(liveQueueControl.getExpiryAddress());
      assertNull(backupQueueControl.getExpiryAddress());

      liveQueueControl.setExpiryAddress(expiryAddress);

      assertEquals(expiryAddress, liveQueueControl.getExpiryAddress());
      assertEquals(expiryAddress, backupQueueControl.getExpiryAddress());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      address = RandomUtil.randomSimpleString();

      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()),
                                                                     new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                                                                backupParams));
      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      session = sf.createSession(false, true, true);

      session.createQueue(address, address, null, false);
   }

   @Override
   protected void tearDown() throws Exception
   {
      session.close();
      
      session = null;
      
      address = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
