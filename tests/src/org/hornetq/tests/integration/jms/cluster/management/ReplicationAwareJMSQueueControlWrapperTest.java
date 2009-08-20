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

package org.hornetq.tests.integration.jms.cluster.management;

import static org.hornetq.tests.integration.management.ManagementControlHelper.createJMSQueueControl;
import static org.hornetq.tests.util.RandomUtil.randomLong;
import static org.hornetq.tests.util.RandomUtil.randomString;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.jms.HornetQQueue;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.jms.server.management.JMSQueueControl;
import org.hornetq.tests.integration.cluster.management.ReplicationAwareTestBase;
import org.hornetq.tests.integration.jms.server.management.JMSUtil;
import org.hornetq.tests.integration.jms.server.management.NullInitialContext;

/**
 * A ReplicationAwareQueueControlWrapperTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ReplicationAwareJMSQueueControlWrapperTest extends ReplicationAwareTestBase
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ReplicationAwareJMSQueueControlWrapperTest.class);

   
   // Attributes ----------------------------------------------------

   private JMSServerManagerImpl liveServerManager;

   private JMSServerManagerImpl backupServerManager;

   private HornetQQueue queue;

   private HornetQQueue otherQueue;

   private Connection connection;
   
   private Session session;

   private JMSQueueControl liveQueueControl;

   private JMSQueueControl backupQueueControl;

   private JMSQueueControl liveOtherQueueControl;

   private JMSQueueControl backupOtherQueueControl;


   // Static --------------------------------------------------------

   // Public --------------------------------------------------------
   
   public void testChangeMessagePriority() throws Exception
   {
      byte oldPriority = (byte)1;
      byte newPriority = (byte)8;

      // send 1 message
      MessageProducer producer = session.createProducer(queue);
      TextMessage message = session.createTextMessage(randomString());
      message.setJMSPriority(oldPriority);
      producer.send(message);

      // check it is on both live & backup nodes
      assertEquals(1, liveQueueControl.getMessageCount());
      assertEquals(1, backupQueueControl.getMessageCount());

      assertTrue(liveQueueControl.changeMessagePriority(message.getJMSMessageID(), newPriority));
   }

   public void testExpireMessage() throws Exception
   {
      // send 1 message
      MessageProducer producer = session.createProducer(queue);
      TextMessage message = session.createTextMessage(randomString());
      producer.send(message);

      // check it is on both live & backup nodes
      assertEquals(1, liveQueueControl.getMessageCount());
      assertEquals(1, backupQueueControl.getMessageCount());

      assertTrue(liveQueueControl.expireMessage(message.getJMSMessageID()));

      // check it is on both live & backup nodes
      assertEquals(0, liveQueueControl.getMessageCount());
      assertEquals(0, backupQueueControl.getMessageCount());
   }

   public void testExpireMessagesWithFilter() throws Exception
   {
      String key = "key";
      long matchingValue = randomLong();
      long unmatchingValue = matchingValue + 1;

      // send 1 message
      JMSUtil.sendMessageWithProperty(session, queue, key, unmatchingValue);
      JMSUtil.sendMessageWithProperty(session, queue, key, matchingValue);

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
      // send on queue
      MessageProducer producer = session.createProducer(queue);
      producer.send(session.createMessage());

      assertEquals(1, liveQueueControl.getMessageCount());
      assertEquals(1, backupQueueControl.getMessageCount());
      assertEquals(0, liveOtherQueueControl.getMessageCount());
      assertEquals(0, backupOtherQueueControl.getMessageCount());

      // moved all messages to otherQueue
      int movedMessagesCount = liveQueueControl.moveMessages(null, otherQueue.getName());
      assertEquals(1, movedMessagesCount);
      
      assertEquals(0, liveQueueControl.getMessageCount());
      assertEquals(0, backupQueueControl.getMessageCount());
      assertEquals(1, liveOtherQueueControl.getMessageCount());
      assertEquals(1, backupOtherQueueControl.getMessageCount());
   }

   public void testMoveMatchingMessages() throws Exception
   {
      String key = new String("key");
      long matchingValue = randomLong();
      long unmatchingValue = matchingValue + 1;

      // send on queue
      JMSUtil.sendMessageWithProperty(session, queue, key, unmatchingValue);
      JMSUtil.sendMessageWithProperty(session, queue, key, matchingValue);

      assertEquals(2, liveQueueControl.getMessageCount());
      assertEquals(2, backupQueueControl.getMessageCount());
      assertEquals(0, liveOtherQueueControl.getMessageCount());
      assertEquals(0, backupOtherQueueControl.getMessageCount());

      // moved matching messages to otherQueue
      int movedMatchedMessagesCount = liveQueueControl.moveMessages(key + " =" + matchingValue, otherQueue.getName());
      assertEquals(1, movedMatchedMessagesCount);

      assertEquals(1, liveQueueControl.getMessageCount());
      assertEquals(1, backupQueueControl.getMessageCount());
      assertEquals(1, liveOtherQueueControl.getMessageCount());
      assertEquals(1, backupOtherQueueControl.getMessageCount());
   }

   public void testMoveMessage() throws Exception
   {
      // send on queue
      MessageProducer producer = session.createProducer(queue);
      Message message = session.createMessage();
      producer.send(message);
      
      // check it is on both live & backup nodes
      assertEquals(1, liveQueueControl.getMessageCount());
      assertEquals(1, backupQueueControl.getMessageCount());
      assertEquals(0, liveOtherQueueControl.getMessageCount());
      assertEquals(0, backupOtherQueueControl.getMessageCount());

      assertTrue(liveQueueControl.moveMessage(message.getJMSMessageID(), otherQueue.getName()));
      
      // check the message is no longer in the queue on both live & backup nodes
      assertEquals(0, liveQueueControl.getMessageCount());
      assertEquals(0, backupQueueControl.getMessageCount());
      assertEquals(1, liveOtherQueueControl.getMessageCount());
      assertEquals(1, backupOtherQueueControl.getMessageCount());
   }
   
   public void testRemoveAllMessages() throws Exception
   {
      // send 1 message
      MessageProducer producer = session.createProducer(queue);
      producer.send(session.createMessage());

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
      String key = "key";
      long matchingValue = randomLong();
      long unmatchingValue = matchingValue + 1;

      // send on queue
      JMSUtil.sendMessageWithProperty(session, queue, key, unmatchingValue);
      JMSUtil.sendMessageWithProperty(session, queue, key, matchingValue);

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
      // send 1 message
      MessageProducer producer = session.createProducer(queue);
      Message message = session.createMessage();
      producer.send(message);
      
      // check it is on both live & backup nodes
      assertEquals(1, liveQueueControl.getMessageCount());
      assertEquals(1, backupQueueControl.getMessageCount());

      assertTrue(liveQueueControl.removeMessage(message.getJMSMessageID()));
      
      // check the message is no longer in the queue on both live & backup nodes
      assertEquals(0, liveQueueControl.getMessageCount());
      assertEquals(0, backupQueueControl.getMessageCount());
   }
   
   public void testSendMessageToDeadLetterAddress() throws Exception
   {
      // send 1 message
      MessageProducer producer = session.createProducer(queue);
      Message message = session.createMessage();
      producer.send(message);
      
      // check it is on both live & backup nodes
      assertEquals(1, liveQueueControl.getMessageCount());
      assertEquals(1, backupQueueControl.getMessageCount());

      assertTrue(liveQueueControl.sendMessageToDeadLetterAddress(message.getJMSMessageID()));
      
      // check the message is no longer in the queue on both live & backup nodes
      assertEquals(0, liveQueueControl.getMessageCount());
      assertEquals(0, backupQueueControl.getMessageCount());
   }
   
   public void testSetDeadLetterAddress() throws Exception
   {
      String deadLetterAddress = randomString();
      
      assertNull(liveQueueControl.getDeadLetterAddress());
      assertNull(backupQueueControl.getDeadLetterAddress());
      
      liveQueueControl.setDeadLetterAddress(deadLetterAddress);
      
      assertEquals(deadLetterAddress, liveQueueControl.getDeadLetterAddress());
      assertEquals(deadLetterAddress, backupQueueControl.getDeadLetterAddress());
   }
   
   public void testSetExpiryAddress() throws Exception
   {
      String expiryAddress = randomString();
      
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
      doSetup(false);

      backupServerManager = new JMSServerManagerImpl(backupServer);
      backupServerManager.setContext(new NullInitialContext());
      backupServerManager.start();
      
      liveServerManager = new JMSServerManagerImpl(liveServer);
      liveServerManager.setContext(new NullInitialContext());
      liveServerManager.start();
                  
      String queueName = randomString();
      liveServerManager.createQueue(queueName, queueName, null, true);
      backupServerManager.createQueue(queueName, queueName, null, true);
      queue = new HornetQQueue(queueName);
      
      String otherQueueName = randomString();     
      liveServerManager.createQueue(otherQueueName, otherQueueName, null, true);
      backupServerManager.createQueue(otherQueueName, otherQueueName, null, true);
      otherQueue = new HornetQQueue(otherQueueName);
      
      connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      liveQueueControl = createJMSQueueControl(queue.getQueueName(), liveMBeanServer);
      backupQueueControl = createJMSQueueControl(queue.getQueueName(), backupMBeanServer);
      liveOtherQueueControl = createJMSQueueControl(otherQueue.getQueueName(), liveMBeanServer);
      backupOtherQueueControl = createJMSQueueControl(otherQueue.getQueueName(), backupMBeanServer);
   }

   @Override
   protected void tearDown() throws Exception
   {

      
      backupServerManager.stop();
 
      liveServerManager.stop();
      
      connection.close();     
      
      session.close();
      
      liveServerManager = null;
      
      backupServerManager = null;
      
      liveQueueControl = null;
      
      backupQueueControl = null;
      
      liveOtherQueueControl = null;
      
      backupOtherQueueControl = null;
      
      session = null;
      
      queue = otherQueue = null;

      super.tearDown();
   }
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
