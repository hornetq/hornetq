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

package org.jboss.messaging.tests.integration.jms.cluster.management;

import static org.jboss.messaging.tests.integration.management.ManagementControlHelper.createJMSQueueControl;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.server.impl.JMSServerManagerImpl;
import org.jboss.messaging.jms.server.management.JMSQueueControlMBean;
import org.jboss.messaging.tests.integration.cluster.management.ReplicationAwareTestBase;
import org.jboss.messaging.tests.integration.jms.management.JMSUtil;
import org.jboss.messaging.tests.integration.jms.management.NullInitialContext;

/**
 * A ReplicationAwareQueueControlWrapperTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ReplicationAwareJMSQueueControlWrapperTest extends ReplicationAwareTestBase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private JMSServerManagerImpl liveServerManager;

   private JMSServerManagerImpl backupServerManager;

   private JBossQueue queue;

   private JBossQueue otherQueue;

   private Session session;

   private JMSQueueControlMBean liveQueueControl;

   private JMSQueueControlMBean backupQueueControl;

   private JMSQueueControlMBean liveOtherQueueControl;

   private JMSQueueControlMBean backupOtherQueueControl;


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
      int movedMessagesCount = liveQueueControl.moveAllMessages(otherQueue.getName());
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
      int movedMatchedMessagesCount = liveQueueControl.moveMatchingMessages(key + " =" + matchingValue, otherQueue.getName());
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
      int count = liveQueueControl.removeAllMessages();
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
      int removedMatchedMessagesCount = liveQueueControl.removeMatchingMessages(key + " =" + matchingValue);
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

      assertTrue(liveQueueControl.sendMessageToDLQ(message.getJMSMessageID()));
      
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
      super.setUp();

      liveServerManager = JMSServerManagerImpl.newJMSServerManagerImpl(liveServer);
      liveServerManager.start();
      liveServerManager.setContext(new NullInitialContext());

      backupServerManager = JMSServerManagerImpl.newJMSServerManagerImpl(backupServer);
      backupServerManager.start();
      backupServerManager.setContext(new NullInitialContext());

      String queueName = randomString();
      liveServerManager.createQueue(queueName, queueName);
      backupServerManager.createQueue(queueName, queueName);
      queue = new JBossQueue(queueName);

      String otherQueueName = randomString();     
      liveServerManager.createQueue(otherQueueName, otherQueueName);
      backupServerManager.createQueue(otherQueueName, otherQueueName);
      otherQueue = new JBossQueue(otherQueueName);
      
      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      liveQueueControl = createJMSQueueControl(queue.getQueueName(), liveMBeanServer);
      backupQueueControl = createJMSQueueControl(queue.getQueueName(), backupMBeanServer);
      liveOtherQueueControl = createJMSQueueControl(otherQueue.getQueueName(), liveMBeanServer);
      backupOtherQueueControl = createJMSQueueControl(otherQueue.getQueueName(), backupMBeanServer);
   }

   @Override
   protected void tearDown() throws Exception
   {
      session.close();

      super.tearDown();
   }
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
