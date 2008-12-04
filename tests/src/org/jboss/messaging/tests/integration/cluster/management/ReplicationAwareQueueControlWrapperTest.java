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

package org.jboss.messaging.tests.integration.cluster.management;

import static org.jboss.messaging.tests.util.RandomUtil.randomLong;
import static org.jboss.messaging.tests.util.RandomUtil.randomSimpleString;

import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.openmbean.TabularData;

import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryInternal;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.management.MessageInfo;
import org.jboss.messaging.core.management.QueueControlMBean;
import org.jboss.messaging.core.management.impl.ManagementServiceImpl;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.tests.util.RandomUtil;
import org.jboss.messaging.util.SimpleString;

/**
 * A ReplicationAwareQueueControlWrapperTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ReplicationAwareQueueControlWrapperTest extends ReplicationAwareTestBase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private ClientSession session;

   private SimpleString address;

   private final long timeToSleep = 100;

   // Static --------------------------------------------------------

   private static QueueControlMBean createQueueControl(SimpleString address, SimpleString name, MBeanServer mbeanServer) throws Exception
   {
      QueueControlMBean queueControl = (QueueControlMBean)MBeanServerInvocationHandler.newProxyInstance(mbeanServer,
                                                                                                        ManagementServiceImpl.getQueueObjectName(address,
                                                                                                                                                 name),
                                                                                                        QueueControlMBean.class,
                                                                                                        false);
      return queueControl;
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testChangeMessagePriority() throws Exception
   {
      byte oldPriority = (byte)1;
      byte newPriority = (byte)8;
      
      QueueControlMBean liveQueueControl = createQueueControl(address, address, liveMBeanServer);
      QueueControlMBean backupQueueControl = createQueueControl(address, address, backupMBeanServer);

      assertFalse(liveQueueControl.isBackup());
      assertTrue(backupQueueControl.isBackup());

      // send 1 message
      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createClientMessage(false);
      message.setPriority(oldPriority);
      producer.send(message);
      
      // wiat a little bit to give time for the message to be handled by the server
      Thread.sleep(timeToSleep);

      // check it is on both live & backup nodes
      assertEquals(1, liveQueueControl.getMessageCount());
      assertEquals(1, backupQueueControl.getMessageCount());

      TabularData messages = liveQueueControl.listAllMessages();
      MessageInfo[] messageInfos = MessageInfo.from(messages);
      assertEquals(1, messageInfos.length);
      long messageID = messageInfos[0].getID();
      assertEquals(oldPriority, messageInfos[0].getPriority());
      
       messages = backupQueueControl.listAllMessages();
       messageInfos = MessageInfo.from(messages);
      assertEquals(1, messageInfos.length);
      assertEquals(oldPriority, messageInfos[0].getPriority());
      
      assertTrue(liveQueueControl.changeMessagePriority(messageID, newPriority));

      // check the priority is changed on both live & backup nodes
      messages = liveQueueControl.listAllMessages();
      messageInfos = MessageInfo.from(messages);
      assertEquals(1, messageInfos.length);
      assertEquals(newPriority, messageInfos[0].getPriority());

      messages = backupQueueControl.listAllMessages();
      messageInfos = MessageInfo.from(messages);
      assertEquals(1, messageInfos.length);
      assertEquals(newPriority, messageInfos[0].getPriority());
   }
   
   public void testExpireMessage() throws Exception
   {
      QueueControlMBean liveQueueControl = createQueueControl(address, address, liveMBeanServer);
      QueueControlMBean backupQueueControl = createQueueControl(address, address, backupMBeanServer);

      assertFalse(liveQueueControl.isBackup());
      assertTrue(backupQueueControl.isBackup());

      // send 1 message
      ClientProducer producer = session.createProducer(address);
      producer.send(session.createClientMessage(false));
      
      // wiat a little bit to give time for the message to be handled by the server
      Thread.sleep(timeToSleep);

      // check it is on both live & backup nodes
      assertEquals(1, liveQueueControl.getMessageCount());
      assertEquals(1, backupQueueControl.getMessageCount());

      TabularData messages = liveQueueControl.listAllMessages();
      MessageInfo[] messageInfos = MessageInfo.from(messages);
      assertEquals(1, messageInfos.length);
      long messageID = messageInfos[0].getID();
      
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

      QueueControlMBean liveQueueControl = createQueueControl(address, address, liveMBeanServer);
      QueueControlMBean backupQueueControl = createQueueControl(address, address, backupMBeanServer);

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
      
      // wiat a little bit to give time for the message to be handled by the server
      Thread.sleep(timeToSleep);

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

      session.createQueue(otherQueue, otherQueue, null, false, true, true);
      ClientProducer producer = session.createProducer(address);

      QueueControlMBean liveQueueControl = createQueueControl(address, address, liveMBeanServer);
      QueueControlMBean backupQueueControl = createQueueControl(address, address, backupMBeanServer);
      QueueControlMBean liveOtherQueueControl = createQueueControl(otherQueue, otherQueue, liveMBeanServer);
      QueueControlMBean backupOtherQueueControl = createQueueControl(otherQueue, otherQueue, backupMBeanServer);

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

      // wait a little bit to ensure the message is handled by the server
      Thread.sleep(timeToSleep);

      assertEquals(1, liveQueueControl.getMessageCount());
      assertEquals(1, backupQueueControl.getMessageCount());
      assertEquals(0, liveOtherQueueControl.getMessageCount());
      assertEquals(0, backupOtherQueueControl.getMessageCount());

      // moved all messages to otherQueue
      int movedMessagesCount = liveQueueControl.moveAllMessages(otherQueue.toString());
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

      session.createQueue(otherQueue, otherQueue, null, false, true, true);

      QueueControlMBean liveQueueControl = createQueueControl(address, address, liveMBeanServer);
      QueueControlMBean backupQueueControl = createQueueControl(address, address, backupMBeanServer);
      QueueControlMBean liveOtherQueueControl = createQueueControl(otherQueue, otherQueue, liveMBeanServer);
      QueueControlMBean backupOtherQueueControl = createQueueControl(otherQueue, otherQueue, backupMBeanServer);

      // send on queue
      ClientProducer producer = session.createProducer(address);
      ClientMessage unmatchingMessage = session.createClientMessage(false);
      unmatchingMessage.putLongProperty(key, unmatchingValue);
      producer.send(unmatchingMessage);
      ClientMessage matchingMessage = session.createClientMessage(false);
      matchingMessage.putLongProperty(key, matchingValue);
      producer.send(matchingMessage);

      // wait a little bit to ensure the message is handled by the server
      Thread.sleep(timeToSleep);
      
      assertEquals(2, liveQueueControl.getMessageCount());
      assertEquals(2, backupQueueControl.getMessageCount());
      assertEquals(0, liveOtherQueueControl.getMessageCount());
      assertEquals(0, backupOtherQueueControl.getMessageCount());

      // moved matching messages to otherQueue
      int movedMatchedMessagesCount = liveQueueControl.moveMatchingMessages(key + " =" + matchingValue, otherQueue.toString());
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

      session.createQueue(otherQueue, otherQueue, null, false, true, true);

      QueueControlMBean liveQueueControl = createQueueControl(address, address, liveMBeanServer);
      QueueControlMBean backupQueueControl = createQueueControl(address, address, backupMBeanServer);
      QueueControlMBean liveOtherQueueControl = createQueueControl(otherQueue, otherQueue, liveMBeanServer);
      QueueControlMBean backupOtherQueueControl = createQueueControl(otherQueue, otherQueue, backupMBeanServer);

      assertFalse(liveQueueControl.isBackup());
      assertTrue(backupQueueControl.isBackup());

      // send 1 message
      ClientProducer producer = session.createProducer(address);
      producer.send(session.createClientMessage(false));
      
      // wait a little bit to give time for the message to be handled by the server
      Thread.sleep(timeToSleep);

      // check it is on both live & backup nodes
      assertEquals(1, liveQueueControl.getMessageCount());
      assertEquals(1, backupQueueControl.getMessageCount());
      assertEquals(0, liveOtherQueueControl.getMessageCount());
      assertEquals(0, backupOtherQueueControl.getMessageCount());

      TabularData messages = liveQueueControl.listAllMessages();
      MessageInfo[] messageInfos = MessageInfo.from(messages);
      assertEquals(1, messageInfos.length);
      long messageID = messageInfos[0].getID();
      
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
      QueueControlMBean liveQueueControl = createQueueControl(address, address, liveMBeanServer);
      QueueControlMBean backupQueueControl = createQueueControl(address, address, backupMBeanServer);

      assertFalse(liveQueueControl.isBackup());
      assertTrue(backupQueueControl.isBackup());

      // send 1 message
      ClientProducer producer = session.createProducer(address);
      producer.send(session.createClientMessage(false));

      // wiat a little bit to give time for the message to be handled by the server
      Thread.sleep(timeToSleep);

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
      SimpleString key = new SimpleString("key");
      long matchingValue = randomLong();
      long unmatchingValue = matchingValue + 1;

      QueueControlMBean liveQueueControl = createQueueControl(address, address, liveMBeanServer);
      QueueControlMBean backupQueueControl = createQueueControl(address, address, backupMBeanServer);

      // send on queue
      ClientProducer producer = session.createProducer(address);
      ClientMessage unmatchingMessage = session.createClientMessage(false);
      unmatchingMessage.putLongProperty(key, unmatchingValue);
      producer.send(unmatchingMessage);
      ClientMessage matchingMessage = session.createClientMessage(false);
      matchingMessage.putLongProperty(key, matchingValue);
      producer.send(matchingMessage);

      // wait a little bit to ensure the message is handled by the server
      Thread.sleep(timeToSleep );
      
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
      QueueControlMBean liveQueueControl = createQueueControl(address, address, liveMBeanServer);
      QueueControlMBean backupQueueControl = createQueueControl(address, address, backupMBeanServer);

      assertFalse(liveQueueControl.isBackup());
      assertTrue(backupQueueControl.isBackup());

      // send 1 message
      ClientProducer producer = session.createProducer(address);
      producer.send(session.createClientMessage(false));
      
      // wait a little bit to give time for the message to be handled by the server
      Thread.sleep(timeToSleep);

      // check it is on both live & backup nodes
      assertEquals(1, liveQueueControl.getMessageCount());
      assertEquals(1, backupQueueControl.getMessageCount());

      TabularData messages = liveQueueControl.listAllMessages();
      MessageInfo[] messageInfos = MessageInfo.from(messages);
      assertEquals(1, messageInfos.length);
      long messageID = messageInfos[0].getID();
      
      assertTrue(liveQueueControl.removeMessage(messageID));
      
      // check the message is no longer in the queue on both live & backup nodes
      assertEquals(0, liveQueueControl.getMessageCount());
      assertEquals(0, backupQueueControl.getMessageCount());
   }
   
   public void testSendMessageToDLQ() throws Exception
   {
      QueueControlMBean liveQueueControl = createQueueControl(address, address, liveMBeanServer);
      QueueControlMBean backupQueueControl = createQueueControl(address, address, backupMBeanServer);

      assertFalse(liveQueueControl.isBackup());
      assertTrue(backupQueueControl.isBackup());

      // send 1 message
      ClientProducer producer = session.createProducer(address);
      producer.send(session.createClientMessage(false));
      
      // wait a little bit to give time for the message to be handled by the server
      Thread.sleep(timeToSleep);

      // check it is on both live & backup nodes
      assertEquals(1, liveQueueControl.getMessageCount());
      assertEquals(1, backupQueueControl.getMessageCount());

      TabularData messages = liveQueueControl.listAllMessages();
      MessageInfo[] messageInfos = MessageInfo.from(messages);
      assertEquals(1, messageInfos.length);
      long messageID = messageInfos[0].getID();
      
      assertTrue(liveQueueControl.sendMessageToDeadLetterAddress(messageID));
      
      // check the message is no longer in the queue on both live & backup nodes
      assertEquals(0, liveQueueControl.getMessageCount());
      assertEquals(0, backupQueueControl.getMessageCount());
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

      session = sf.createSession(false, true, true);

      session.createQueue(address, address, null, false, false, true);
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
