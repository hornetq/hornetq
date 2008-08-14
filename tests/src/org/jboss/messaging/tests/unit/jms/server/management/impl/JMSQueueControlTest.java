/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.unit.jms.server.management.impl;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.jboss.messaging.tests.util.RandomUtil.randomBoolean;
import static org.jboss.messaging.tests.util.RandomUtil.randomByte;
import static org.jboss.messaging.tests.util.RandomUtil.randomInt;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;
import static org.jboss.messaging.tests.util.RandomUtil.randomSimpleString;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import junit.framework.TestCase;

import org.easymock.EasyMock;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.server.management.impl.JMSQueueControl;
import org.jboss.messaging.util.SimpleString;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class JMSQueueControlTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testGetName() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      PostOffice postOffice = createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);
      HierarchicalRepository<QueueSettings> queueSettingsRepository = createMock(HierarchicalRepository.class);

      replay(coreQueue, postOffice, storageManager, queueSettingsRepository);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, postOffice, storageManager, queueSettingsRepository);
      assertEquals(name, control.getName());

      verify(coreQueue, postOffice, storageManager, queueSettingsRepository);
   }

   public void testGetAddress() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      PostOffice postOffice = createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);
      HierarchicalRepository<QueueSettings> queueSettingsRepository = createMock(HierarchicalRepository.class);

      replay(coreQueue, postOffice, storageManager, queueSettingsRepository);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, postOffice, storageManager, queueSettingsRepository);
      assertEquals(queue.getAddress(), control.getAddress());

      verify(coreQueue, postOffice, storageManager, queueSettingsRepository);
   }

   public void testGetJNDIBinding() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      PostOffice postOffice = createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);
      HierarchicalRepository<QueueSettings> queueSettingsRepository = createMock(HierarchicalRepository.class);

      replay(coreQueue, postOffice, storageManager, queueSettingsRepository);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, postOffice, storageManager, queueSettingsRepository);
      assertEquals(jndiBinding, control.getJNDIBinding());

      verify(coreQueue, postOffice, storageManager, queueSettingsRepository);
   }

   public void testIsTemporary() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      PostOffice postOffice = createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);
      HierarchicalRepository<QueueSettings> queueSettingsRepository = createMock(HierarchicalRepository.class);

      replay(coreQueue, postOffice, storageManager, queueSettingsRepository);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, postOffice, storageManager, queueSettingsRepository);
      assertEquals(queue.isTemporary(), control.isTemporary());

      verify(coreQueue, postOffice, storageManager, queueSettingsRepository);
   }

   public void testIsClustered() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();
      boolean clustered = randomBoolean();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      expect(coreQueue.isClustered()).andReturn(clustered);
      PostOffice postOffice = createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);
      HierarchicalRepository<QueueSettings> queueSettingsRepository = createMock(HierarchicalRepository.class);

      replay(coreQueue, postOffice, storageManager, queueSettingsRepository);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, postOffice, storageManager, queueSettingsRepository);
      assertEquals(clustered, control.isClustered());

      verify(coreQueue, postOffice, storageManager, queueSettingsRepository);
   }

   public void testIsDurabled() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();
      boolean durable = randomBoolean();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      expect(coreQueue.isDurable()).andReturn(durable);
      PostOffice postOffice = createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);
      HierarchicalRepository<QueueSettings> queueSettingsRepository = createMock(HierarchicalRepository.class);

      replay(coreQueue, postOffice, storageManager, queueSettingsRepository);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, postOffice, storageManager, queueSettingsRepository);
      assertEquals(durable, control.isDurable());

      verify(coreQueue, postOffice, storageManager, queueSettingsRepository);
   }

   public void testGetMessageCount() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();
      int count = randomInt();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      expect(coreQueue.getMessageCount()).andReturn(count);
      PostOffice postOffice = createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);
      HierarchicalRepository<QueueSettings> queueSettingsRepository = createMock(HierarchicalRepository.class);

      replay(coreQueue, postOffice, storageManager, queueSettingsRepository);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, postOffice, storageManager, queueSettingsRepository);
      assertEquals(count, control.getMessageCount());

      verify(coreQueue, postOffice, storageManager, queueSettingsRepository);
   }

   public void testGetMessagesAdded() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();
      int count = randomInt();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      expect(coreQueue.getMessagesAdded()).andReturn(count);
      PostOffice postOffice = createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);
      HierarchicalRepository<QueueSettings> queueSettingsRepository = createMock(HierarchicalRepository.class);

      replay(coreQueue, postOffice, storageManager, queueSettingsRepository);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, postOffice, storageManager, queueSettingsRepository);
      assertEquals(count, control.getMessagesAdded());

      verify(coreQueue, postOffice, storageManager, queueSettingsRepository);
   }

   public void testGetConsumerCount() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();
      int count = randomInt();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      expect(coreQueue.getConsumerCount()).andReturn(count);
      PostOffice postOffice = createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);
      HierarchicalRepository<QueueSettings> queueSettingsRepository = createMock(HierarchicalRepository.class);

      replay(coreQueue, postOffice, storageManager, queueSettingsRepository);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, postOffice, storageManager, queueSettingsRepository);
      assertEquals(count, control.getConsumerCount());

      verify(coreQueue, postOffice, storageManager, queueSettingsRepository);
   }

   public void testGetDeliveringCount() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();
      int count = randomInt();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      expect(coreQueue.getDeliveringCount()).andReturn(count);
      PostOffice postOffice = createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);
      HierarchicalRepository<QueueSettings> queueSettingsRepository = createMock(HierarchicalRepository.class);

      replay(coreQueue, postOffice, storageManager, queueSettingsRepository);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, postOffice, storageManager, queueSettingsRepository);
      assertEquals(count, control.getDeliveringCount());

      verify(coreQueue, postOffice, storageManager, queueSettingsRepository);
   }

   public void testGetMaxSizeBytes() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();
      int size = randomInt();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      expect(coreQueue.getMaxSizeBytes()).andReturn(size);
      PostOffice postOffice = createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);
      HierarchicalRepository<QueueSettings> queueSettingsRepository = createMock(HierarchicalRepository.class);

      replay(coreQueue, postOffice, storageManager, queueSettingsRepository);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, postOffice, storageManager, queueSettingsRepository);
      assertEquals(size, control.getMaxSizeBytes());

      verify(coreQueue, postOffice, storageManager, queueSettingsRepository);
   }

   public void testGetSizeBytes() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();
      int size = randomInt();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      expect(coreQueue.getSizeBytes()).andReturn(size);
      PostOffice postOffice = createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);
      HierarchicalRepository<QueueSettings> queueSettingsRepository = createMock(HierarchicalRepository.class);

      replay(coreQueue, postOffice, storageManager, queueSettingsRepository);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, postOffice, storageManager, queueSettingsRepository);
      assertEquals(size, control.getSizeBytes());

      verify(coreQueue, postOffice, storageManager, queueSettingsRepository);
   }

   public void testGetScheduledCount() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();
      int count = randomInt();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      expect(coreQueue.getScheduledCount()).andReturn(count);
      PostOffice postOffice = createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);
      HierarchicalRepository<QueueSettings> queueSettingsRepository = createMock(HierarchicalRepository.class);

      replay(coreQueue, postOffice, storageManager, queueSettingsRepository);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, postOffice, storageManager, queueSettingsRepository);
      assertEquals(count, control.getScheduledCount());

      verify(coreQueue, postOffice, storageManager, queueSettingsRepository);
   }

   public void testGetDLQ() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();
      final String dlq = randomString();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      PostOffice postOffice = createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);
      HierarchicalRepository<QueueSettings> queueSettingsRepository = createMock(HierarchicalRepository.class);

      QueueSettings settings = new QueueSettings()
      {
         @Override
         public SimpleString getDLQ()
         {
            return new SimpleString(JBossQueue.JMS_QUEUE_ADDRESS_PREFIX + dlq);
         }
      };
      expect(queueSettingsRepository.getMatch(name)).andReturn(settings);

      replay(coreQueue, postOffice, storageManager, queueSettingsRepository);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, postOffice, storageManager, queueSettingsRepository);
      assertEquals(dlq, control.getDLQ());

      verify(coreQueue, postOffice, storageManager, queueSettingsRepository);
   }

   public void testGetExpiryQueue() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();
      final String expiryQueue = randomString();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      PostOffice postOffice = createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);
      HierarchicalRepository<QueueSettings> queueSettingsRepository = createMock(HierarchicalRepository.class);

      QueueSettings settings = new QueueSettings()
      {
         @Override
         public SimpleString getExpiryQueue()
         {
            return new SimpleString(JBossQueue.JMS_QUEUE_ADDRESS_PREFIX
                  + expiryQueue);
         }
      };
      expect(queueSettingsRepository.getMatch(name)).andReturn(settings);

      replay(coreQueue, postOffice, storageManager, queueSettingsRepository);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, postOffice, storageManager, queueSettingsRepository);
      assertEquals(expiryQueue, control.getExpiryQueue());

      verify(coreQueue, postOffice, storageManager, queueSettingsRepository);
   }

   public void testRemoveMessage() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();
      String jmsMessageID = randomString();
      long messageID = randomLong();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      PostOffice postOffice = createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);
      HierarchicalRepository<QueueSettings> queueSettingsRepository = createMock(HierarchicalRepository.class);

      List<MessageReference> refs = new ArrayList<MessageReference>();
      MessageReference ref = createMock(MessageReference.class);
      ServerMessage message = createMock(ServerMessage.class);
      expect(message.getMessageID()).andReturn(messageID);
      expect(ref.getMessage()).andReturn(message);
      refs.add(ref);
      expect(coreQueue.list(EasyMock.isA(Filter.class))).andReturn(refs);
      expect(coreQueue.deleteReference(messageID, storageManager)).andReturn(
            true);

      replay(coreQueue, postOffice, storageManager, queueSettingsRepository,
            ref, message);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, postOffice, storageManager, queueSettingsRepository);
      assertTrue(control.removeMessage(jmsMessageID));

      verify(coreQueue, postOffice, storageManager, queueSettingsRepository,
            ref, message);
   }

   public void testRemoveAllMessages() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      PostOffice postOffice = createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);
      HierarchicalRepository<QueueSettings> queueSettingsRepository = createMock(HierarchicalRepository.class);
      coreQueue.deleteAllReferences(storageManager);

      replay(coreQueue, postOffice, storageManager, queueSettingsRepository);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, postOffice, storageManager, queueSettingsRepository);
      control.removeAllMessages();

      verify(coreQueue, postOffice, storageManager, queueSettingsRepository);
   }

   public void testListMessages() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();

      JBossQueue queue = new JBossQueue(name);
      String filterStr = "color = 'green'";
      List<MessageReference> refs = new ArrayList<MessageReference>();
      MessageReference ref = createMock(MessageReference.class);
      ServerMessage message = createMock(ServerMessage.class);
      expect(message.getProperty(new SimpleString("JMSMessageID")))
            .andStubReturn(randomSimpleString());
      expect(message.getProperty(new SimpleString("JMSCorrelationID")))
            .andStubReturn(randomSimpleString());
      expect(message.getProperty(new SimpleString("JMSType"))).andStubReturn(
            randomSimpleString());
      expect(message.isDurable()).andStubReturn(randomBoolean());
      expect(message.getPriority()).andStubReturn(randomByte());
      expect(message.getProperty(new SimpleString("JMSReplyTo")))
            .andStubReturn(randomSimpleString());
      expect(message.getTimestamp()).andStubReturn(randomLong());
      expect(message.getExpiration()).andStubReturn(randomLong());
      expect(message.getPropertyNames()).andStubReturn(
            new HashSet<SimpleString>());
      expect(ref.getMessage()).andReturn(message);
      refs.add(ref);
      Queue coreQueue = createMock(Queue.class);
      expect(coreQueue.list(isA(Filter.class))).andReturn(refs);
      PostOffice postOffice = createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);
      HierarchicalRepository<QueueSettings> queueSettingsRepository = createMock(HierarchicalRepository.class);

      replay(coreQueue, postOffice, storageManager, queueSettingsRepository,
            ref, message);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, postOffice, storageManager, queueSettingsRepository);
      TabularData data = control.listMessages(filterStr);
      assertEquals(1, data.size());
      CompositeData info = data.get(new Object[] { message.getProperty(
            new SimpleString("JMSMessageID")).toString() });
      assertNotNull(info);

      verify(coreQueue, postOffice, storageManager, queueSettingsRepository,
            ref, message);
   }

   public void testListMessagesThrowsException() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();
      String invalidFilterStr = "this is not a valid filter";

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      PostOffice postOffice = createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);
      HierarchicalRepository<QueueSettings> queueSettingsRepository = createMock(HierarchicalRepository.class);

      replay(coreQueue, postOffice, storageManager, queueSettingsRepository);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, postOffice, storageManager, queueSettingsRepository);
      try
      {
         control.listMessages(invalidFilterStr);
         fail("IllegalStateException");
      } catch (IllegalStateException e)
      {

      }

      verify(coreQueue, postOffice, storageManager, queueSettingsRepository);
   }

   public void testExpireMessage() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();
      String jmsMessageID = randomString();
      long messageID = randomLong();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      PostOffice postOffice = createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);
      HierarchicalRepository<QueueSettings> queueSettingsRepository = createMock(HierarchicalRepository.class);

      List<MessageReference> refs = new ArrayList<MessageReference>();
      MessageReference ref = createMock(MessageReference.class);
      ServerMessage serverMessage = createMock(ServerMessage.class);
      expect(serverMessage.getMessageID()).andReturn(messageID);
      expect(ref.getMessage()).andReturn(serverMessage);
      refs.add(ref);
      expect(coreQueue.list(EasyMock.isA(Filter.class))).andReturn(refs);
      expect(
            coreQueue.expireMessage(messageID, storageManager, postOffice,
                  queueSettingsRepository)).andReturn(true);

      replay(coreQueue, postOffice, storageManager, queueSettingsRepository,
            ref, serverMessage);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, postOffice, storageManager, queueSettingsRepository);
      assertTrue(control.expireMessage(jmsMessageID));

      verify(coreQueue, postOffice, storageManager, queueSettingsRepository,
            ref, serverMessage);
   }

   public void testExpireMessageWithNoJMSMesageID() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();
      String jmsMessageID = randomString();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      PostOffice postOffice = createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);
      HierarchicalRepository<QueueSettings> queueSettingsRepository = createMock(HierarchicalRepository.class);
      expect(coreQueue.list(isA(Filter.class))).andReturn(
            new ArrayList<MessageReference>());

      replay(coreQueue, postOffice, storageManager, queueSettingsRepository);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, postOffice, storageManager, queueSettingsRepository);
      try
      {
         control.expireMessage(jmsMessageID);
         fail("IllegalArgumentException");
      } catch (IllegalArgumentException e)
      {
      }

      verify(coreQueue, postOffice, storageManager, queueSettingsRepository);
   }

   public void testExpireMessages() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();
      long messageID = randomLong();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      PostOffice postOffice = createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);
      HierarchicalRepository<QueueSettings> queueSettingsRepository = createMock(HierarchicalRepository.class);
      List<MessageReference> refs = new ArrayList<MessageReference>();
      MessageReference ref = createMock(MessageReference.class);
      ServerMessage serverMessage = createMock(ServerMessage.class);
      expect(serverMessage.getMessageID()).andReturn(messageID);
      expect(ref.getMessage()).andReturn(serverMessage);
      refs.add(ref);
      expect(coreQueue.list(isA(Filter.class))).andReturn(refs);
      expect(
            coreQueue.expireMessage(messageID, storageManager, postOffice,
                  queueSettingsRepository)).andReturn(true);

      replay(coreQueue, postOffice, storageManager, queueSettingsRepository, ref, serverMessage);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, postOffice, storageManager, queueSettingsRepository);
      assertEquals(1, control.expireMessages("color = 'green'"));

      verify(coreQueue, postOffice, storageManager, queueSettingsRepository, ref, serverMessage);
   }

   public void testSendMessageToDLQ() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();
      String jmsMessageID = randomString();
      long messageID = randomLong();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      PostOffice postOffice = createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);
      HierarchicalRepository<QueueSettings> queueSettingsRepository = createMock(HierarchicalRepository.class);
      List<MessageReference> refs = new ArrayList<MessageReference>();
      MessageReference ref = createMock(MessageReference.class);
      ServerMessage serverMessage = createMock(ServerMessage.class);
      expect(serverMessage.getMessageID()).andReturn(messageID);
      expect(ref.getMessage()).andReturn(serverMessage);
      refs.add(ref);
      expect(coreQueue.list(isA(Filter.class))).andReturn(refs);
      expect(coreQueue.sendMessageToDLQ(messageID, storageManager, postOffice, queueSettingsRepository)).andReturn(true);
      
      replay(coreQueue, postOffice, storageManager, queueSettingsRepository, ref, serverMessage);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, postOffice, storageManager, queueSettingsRepository);
      assertTrue(control.sendMessageToDLQ(jmsMessageID));

      verify(coreQueue, postOffice, storageManager, queueSettingsRepository, ref, serverMessage);
   }

   public void testSendMessageToDLQWithNoJMSMessageID() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();
      String jmsMessageID = randomString();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      PostOffice postOffice = createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);
      HierarchicalRepository<QueueSettings> queueSettingsRepository = createMock(HierarchicalRepository.class);
      expect(coreQueue.list(isA(Filter.class))).andReturn(
            new ArrayList<MessageReference>());

      replay(coreQueue, postOffice, storageManager, queueSettingsRepository);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, postOffice, storageManager, queueSettingsRepository);
      try
      {
         control.sendMessageToDLQ(jmsMessageID);
         fail("IllegalArgumentException");
      } catch (IllegalArgumentException e)
      {
      }

      verify(coreQueue, postOffice, storageManager, queueSettingsRepository);
   }

   public void testChangeMessagePriority() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();
      byte newPriority = 5;
      String jmsMessageID = randomString();
      long messageID = randomLong();
      
      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      PostOffice postOffice = createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);
      HierarchicalRepository<QueueSettings> queueSettingsRepository = createMock(HierarchicalRepository.class);
      List<MessageReference> refs = new ArrayList<MessageReference>();
      MessageReference ref = createMock(MessageReference.class);
      ServerMessage serverMessage = createMock(ServerMessage.class);
      expect(serverMessage.getMessageID()).andReturn(messageID);
      expect(ref.getMessage()).andReturn(serverMessage);
      refs.add(ref);
      expect(coreQueue.list(isA(Filter.class))).andReturn(refs);
      expect(coreQueue.changeMessagePriority(messageID, newPriority, storageManager, postOffice, queueSettingsRepository)).andReturn(true);

      replay(coreQueue, postOffice, storageManager, queueSettingsRepository, ref, serverMessage);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, postOffice, storageManager, queueSettingsRepository);
      assertTrue(control.changeMessagePriority(jmsMessageID, newPriority));

      verify(coreQueue, postOffice, storageManager, queueSettingsRepository, ref, serverMessage);
   }

   public void testChangeMessagePriorityWithInvalidPriorityValues()
         throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();
      String jmsMessageID = randomString();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      PostOffice postOffice = createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);
      HierarchicalRepository<QueueSettings> queueSettingsRepository = createMock(HierarchicalRepository.class);

      replay(coreQueue, postOffice, storageManager, queueSettingsRepository);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, postOffice, storageManager, queueSettingsRepository);
      try
      {
         control.changeMessagePriority(jmsMessageID, -1);
         fail("IllegalArgumentException");
      } catch (IllegalArgumentException e)
      {
      }

      try
      {
         control.changeMessagePriority(jmsMessageID, 10);
         fail("IllegalArgumentException");
      } catch (IllegalArgumentException e)
      {
      }

      verify(coreQueue, postOffice, storageManager, queueSettingsRepository);
   }

   public void testChangeMessagePriorityWithNoJMSMessageID() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();
      byte newPriority = 5;
      String jmsMessageID = randomString();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      PostOffice postOffice = createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);
      HierarchicalRepository<QueueSettings> queueSettingsRepository = createMock(HierarchicalRepository.class);
      expect(coreQueue.list(isA(Filter.class))).andReturn(
            new ArrayList<MessageReference>());

      replay(coreQueue, postOffice, storageManager, queueSettingsRepository);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, postOffice, storageManager, queueSettingsRepository);
      try
      {
         control.changeMessagePriority(jmsMessageID, newPriority);
         fail("IllegalArgumentException");
      } catch (IllegalArgumentException e)
      {
      }

      verify(coreQueue, postOffice, storageManager, queueSettingsRepository);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
