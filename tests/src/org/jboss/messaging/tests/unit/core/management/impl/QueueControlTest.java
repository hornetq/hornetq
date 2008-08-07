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

package org.jboss.messaging.tests.unit.core.management.impl;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
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

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.management.QueueControlMBean;
import org.jboss.messaging.core.management.impl.QueueControl;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.HandleStatus;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.util.SimpleString;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class QueueControlTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testGetName() throws Exception
   {
      String name = randomString();

      Queue queue = createMock(Queue.class);
      expect(queue.getName()).andReturn(new SimpleString(name));
      StorageManager storageManager = createMock(StorageManager.class);
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> repository = createMock(HierarchicalRepository.class);

      replay(queue, storageManager, postOffice, repository);

      QueueControlMBean control = new QueueControl(queue, storageManager,
            postOffice, repository);
      assertEquals(name, control.getName());

      verify(queue, storageManager, postOffice, repository);
   }

   public void testGetFilter() throws Exception
   {
      String filterStr = "color = 'green'";
      Queue queue = createMock(Queue.class);
      Filter filter = createMock(Filter.class);
      expect(filter.getFilterString()).andReturn(new SimpleString(filterStr));
      expect(queue.getFilter()).andReturn(filter);
      StorageManager storageManager = createMock(StorageManager.class);
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> repository = createMock(HierarchicalRepository.class);

      replay(queue, storageManager, postOffice, repository, filter);

      QueueControlMBean control = new QueueControl(queue, storageManager,
            postOffice, repository);
      assertEquals(filterStr, control.getFilter());

      verify(queue, storageManager, postOffice, repository, filter);
   }

   public void testGetFilterWithNull() throws Exception
   {
      Queue queue = createMock(Queue.class);
      expect(queue.getFilter()).andReturn(null);
      StorageManager storageManager = createMock(StorageManager.class);
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> repository = createMock(HierarchicalRepository.class);

      replay(queue, storageManager, postOffice, repository);

      QueueControlMBean control = new QueueControl(queue, storageManager,
            postOffice, repository);
      assertNull(control.getFilter());

      verify(queue, storageManager, postOffice, repository);
   }

   public void testIsClustered() throws Exception
   {
      boolean clustered = randomBoolean();

      Queue queue = createMock(Queue.class);
      expect(queue.isClustered()).andReturn(clustered);
      StorageManager storageManager = createMock(StorageManager.class);
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> repository = createMock(HierarchicalRepository.class);

      replay(queue, storageManager, postOffice, repository);

      QueueControlMBean control = new QueueControl(queue, storageManager,
            postOffice, repository);
      assertEquals(clustered, control.isClustered());

      verify(queue, storageManager, postOffice, repository);
   }

   public void testIsDurable() throws Exception
   {
      boolean durable = randomBoolean();

      Queue queue = createMock(Queue.class);
      expect(queue.isDurable()).andReturn(durable);
      StorageManager storageManager = createMock(StorageManager.class);
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> repository = createMock(HierarchicalRepository.class);

      replay(queue, storageManager, postOffice, repository);

      QueueControlMBean control = new QueueControl(queue, storageManager,
            postOffice, repository);
      assertEquals(durable, control.isDurable());

      verify(queue, storageManager, postOffice, repository);
   }

   public void testIsTemporary() throws Exception
   {
      boolean temp = randomBoolean();

      Queue queue = createMock(Queue.class);
      expect(queue.isTemporary()).andReturn(temp);
      StorageManager storageManager = createMock(StorageManager.class);
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> repository = createMock(HierarchicalRepository.class);

      replay(queue, storageManager, postOffice, repository);

      QueueControlMBean control = new QueueControl(queue, storageManager,
            postOffice, repository);
      assertEquals(temp, control.isTemporary());

      verify(queue, storageManager, postOffice, repository);
   }

   public void testGetMessageCount() throws Exception
   {
      int count = randomInt();

      Queue queue = createMock(Queue.class);
      expect(queue.getMessageCount()).andReturn(count);
      StorageManager storageManager = createMock(StorageManager.class);
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> repository = createMock(HierarchicalRepository.class);

      replay(queue, storageManager, postOffice, repository);

      QueueControlMBean control = new QueueControl(queue, storageManager,
            postOffice, repository);
      assertEquals(count, control.getMessageCount());

      verify(queue, storageManager, postOffice, repository);
   }

   public void testGetMessagesAdded() throws Exception
   {
      int count = randomInt();

      Queue queue = createMock(Queue.class);
      expect(queue.getMessagesAdded()).andReturn(count);
      StorageManager storageManager = createMock(StorageManager.class);
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> repository = createMock(HierarchicalRepository.class);

      replay(queue, storageManager, postOffice, repository);

      QueueControlMBean control = new QueueControl(queue, storageManager,
            postOffice, repository);
      assertEquals(count, control.getMessagesAdded());

      verify(queue, storageManager, postOffice, repository);
   }

   public void testGetMaxSizeBytes() throws Exception
   {
      int size = randomInt();

      Queue queue = createMock(Queue.class);
      expect(queue.getMaxSizeBytes()).andReturn(size);
      StorageManager storageManager = createMock(StorageManager.class);
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> repository = createMock(HierarchicalRepository.class);

      replay(queue, storageManager, postOffice, repository);

      QueueControlMBean control = new QueueControl(queue, storageManager,
            postOffice, repository);
      assertEquals(size, control.getMaxSizeBytes());

      verify(queue, storageManager, postOffice, repository);
   }

   public void testGetSizeBytes() throws Exception
   {
      int size = randomInt();

      Queue queue = createMock(Queue.class);
      expect(queue.getSizeBytes()).andReturn(size);
      StorageManager storageManager = createMock(StorageManager.class);
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> repository = createMock(HierarchicalRepository.class);

      replay(queue, storageManager, postOffice, repository);

      QueueControlMBean control = new QueueControl(queue, storageManager,
            postOffice, repository);
      assertEquals(size, control.getSizeBytes());

      verify(queue, storageManager, postOffice, repository);
   }

   public void testGetScheduledCount() throws Exception
   {
      int count = randomInt();

      Queue queue = createMock(Queue.class);
      expect(queue.getScheduledCount()).andReturn(count);
      StorageManager storageManager = createMock(StorageManager.class);
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> repository = createMock(HierarchicalRepository.class);

      replay(queue, storageManager, postOffice, repository);

      QueueControlMBean control = new QueueControl(queue, storageManager,
            postOffice, repository);
      assertEquals(count, control.getScheduledCount());

      verify(queue, storageManager, postOffice, repository);
   }

   public void testGetConsumerCount() throws Exception
   {
      int count = randomInt();

      Queue queue = createMock(Queue.class);
      expect(queue.getConsumerCount()).andReturn(count);
      StorageManager storageManager = createMock(StorageManager.class);
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> repository = createMock(HierarchicalRepository.class);

      replay(queue, storageManager, postOffice, repository);

      QueueControlMBean control = new QueueControl(queue, storageManager,
            postOffice, repository);
      assertEquals(count, control.getConsumerCount());

      verify(queue, storageManager, postOffice, repository);
   }

   public void testGetDeliveringCount() throws Exception
   {
      int count = randomInt();

      Queue queue = createMock(Queue.class);
      expect(queue.getDeliveringCount()).andReturn(count);
      StorageManager storageManager = createMock(StorageManager.class);
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> repository = createMock(HierarchicalRepository.class);

      replay(queue, storageManager, postOffice, repository);

      QueueControlMBean control = new QueueControl(queue, storageManager,
            postOffice, repository);
      assertEquals(count, control.getDeliveringCount());

      verify(queue, storageManager, postOffice, repository);
   }

   public void testGetPersistenceID() throws Exception
   {
      long id = randomLong();

      Queue queue = createMock(Queue.class);
      expect(queue.getPersistenceID()).andReturn(id);
      StorageManager storageManager = createMock(StorageManager.class);
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> repository = createMock(HierarchicalRepository.class);

      replay(queue, storageManager, postOffice, repository);

      QueueControlMBean control = new QueueControl(queue, storageManager,
            postOffice, repository);
      assertEquals(id, control.getPersistenceID());

      verify(queue, storageManager, postOffice, repository);
   }

   public void testGetDLQ() throws Exception
   {
      String queueName = randomString();
      final String dlqName = randomString();

      Queue queue = createMock(Queue.class);
      expect(queue.getName()).andReturn(new SimpleString(queueName));
      StorageManager storageManager = createMock(StorageManager.class);
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> repository = createMock(HierarchicalRepository.class);
      QueueSettings queueSettings = new QueueSettings()
      {
         @Override
         public SimpleString getDLQ()
         {
            return new SimpleString(dlqName);
         }
      };
      expect(repository.getMatch(queueName)).andReturn(queueSettings);

      replay(queue, storageManager, postOffice, repository);

      QueueControlMBean control = new QueueControl(queue, storageManager,
            postOffice, repository);
      assertEquals(dlqName, control.getDLQ());

      verify(queue, storageManager, postOffice, repository);
   }

   public void testGetExpiryQueue() throws Exception
   {
      String queueName = randomString();
      final String expiryQueueName = randomString();

      Queue queue = createMock(Queue.class);
      expect(queue.getName()).andReturn(new SimpleString(queueName));
      StorageManager storageManager = createMock(StorageManager.class);
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> repository = createMock(HierarchicalRepository.class);
      QueueSettings queueSettings = new QueueSettings()
      {
         @Override
         public SimpleString getExpiryQueue()
         {
            return new SimpleString(expiryQueueName);
         }
      };
      expect(repository.getMatch(queueName)).andReturn(queueSettings);

      replay(queue, storageManager, postOffice, repository);

      QueueControlMBean control = new QueueControl(queue, storageManager,
            postOffice, repository);
      assertEquals(expiryQueueName, control.getExpiryQueue());

      verify(queue, storageManager, postOffice, repository);
   }

   public void testRemoveAllMessages() throws Exception
   {
      Queue queue = createMock(Queue.class);
      StorageManager storageManager = createMock(StorageManager.class);
      queue.deleteAllReferences(storageManager);
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> repository = createMock(HierarchicalRepository.class);

      replay(queue, storageManager, postOffice, repository);

      QueueControlMBean control = new QueueControl(queue, storageManager,
            postOffice, repository);
      control.removeAllMessages();

      verify(queue, storageManager, postOffice, repository);
   }

   public void testRemoveAllMessagesThrowsException() throws Exception
   {
      Queue queue = createMock(Queue.class);
      StorageManager storageManager = createMock(StorageManager.class);
      queue.deleteAllReferences(storageManager);
      expectLastCall().andThrow(new MessagingException());
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> repository = createMock(HierarchicalRepository.class);

      replay(queue, storageManager, postOffice, repository);

      QueueControlMBean control = new QueueControl(queue, storageManager,
            postOffice, repository);
      try
      {
         control.removeAllMessages();
         fail("IllegalStateException");
      } catch (IllegalStateException e)
      {

      }

      verify(queue, storageManager, postOffice, repository);
   }

   public void testRemoveMessage() throws Exception
   {
      long messageID = randomLong();
      boolean deleted = randomBoolean();

      Queue queue = createMock(Queue.class);
      StorageManager storageManager = createMock(StorageManager.class);
      expect(queue.deleteReference(messageID, storageManager)).andReturn(
            deleted);
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> repository = createMock(HierarchicalRepository.class);

      replay(queue, storageManager, postOffice, repository);

      QueueControlMBean control = new QueueControl(queue, storageManager,
            postOffice, repository);
      assertEquals(deleted, control.removeMessage(messageID));

      verify(queue, storageManager, postOffice, repository);
   }

   public void testRemoveMessageThrowsException() throws Exception
   {
      long messageID = randomLong();

      Queue queue = createMock(Queue.class);
      StorageManager storageManager = createMock(StorageManager.class);
      expect(queue.deleteReference(messageID, storageManager)).andThrow(
            new MessagingException());
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> repository = createMock(HierarchicalRepository.class);

      replay(queue, storageManager, postOffice, repository);

      QueueControlMBean control = new QueueControl(queue, storageManager,
            postOffice, repository);
      try
      {
         control.removeMessage(messageID);
         fail("IllegalStateException");
      } catch (IllegalStateException e)
      {

      }

      verify(queue, storageManager, postOffice, repository);
   }

   public void testListMessages() throws Exception
   {
      String filterStr = "color = 'green'";
      List<MessageReference> refs = new ArrayList<MessageReference>();

      MessageReference ref = createMock(MessageReference.class);
      ServerMessage message = createMock(ServerMessage.class);
      expect(message.getMessageID()).andStubReturn(randomLong());
      expect(message.getDestination()).andStubReturn(randomSimpleString());
      expect(message.isDurable()).andStubReturn(randomBoolean());
      expect(message.getTimestamp()).andStubReturn(randomLong());
      expect(message.getType()).andStubReturn(randomByte());
      expect(message.getEncodeSize()).andStubReturn(randomInt());
      expect(message.getPriority()).andStubReturn(randomByte());
      expect(message.isExpired()).andStubReturn(randomBoolean());
      expect(message.getExpiration()).andStubReturn(randomLong());
      expect(message.getPropertyNames()).andReturn(new HashSet<SimpleString>());
      expect(ref.getMessage()).andReturn(message);
      refs.add(ref);
      Queue queue = createMock(Queue.class);
      StorageManager storageManager = createMock(StorageManager.class);
      expect(queue.list(isA(Filter.class))).andReturn(refs);
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> repository = createMock(HierarchicalRepository.class);

      replay(queue, storageManager, postOffice, repository, ref, message);

      QueueControlMBean control = new QueueControl(queue, storageManager,
            postOffice, repository);
      TabularData data = control.listMessages(filterStr);
      assertEquals(1, data.size());
      CompositeData info = data.get(new Object[] { message.getMessageID() });
      assertNotNull(info);
      assertEquals(message.getMessageID(), info.get("id"));
      assertEquals(message.getDestination().toString(), info.get("destination"));
      assertEquals(message.isDurable(), info.get("durable"));
      assertEquals(message.getTimestamp(), info.get("timestamp"));
      assertEquals(message.getType(), info.get("type"));
      assertEquals(message.getEncodeSize(), info.get("size"));
      assertEquals(message.getPriority(), info.get("priority"));
      assertEquals(message.isExpired(), info.get("expired"));
      assertEquals(message.getExpiration(), info.get("expiration"));

      verify(queue, storageManager, postOffice, repository, ref, message);
   }

   public void testExpireMessageWithMessageID() throws Exception
   {
      long messageID = randomLong();

      Queue queue = createMock(Queue.class);
      StorageManager storageManager = createMock(StorageManager.class);
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> repository = createMock(HierarchicalRepository.class);
      expect(
            queue.expireMessage(messageID, storageManager, postOffice,
                  repository)).andReturn(true);

      replay(queue, storageManager, postOffice, repository);

      QueueControlMBean control = new QueueControl(queue, storageManager,
            postOffice, repository);
      assertTrue(control.expireMessage(messageID));

      verify(queue, storageManager, postOffice, repository);
   }

   public void testExpireMessageWithNoMatch() throws Exception
   {
      long messageID = randomLong();

      Queue queue = createMock(Queue.class);
      StorageManager storageManager = createMock(StorageManager.class);
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> repository = createMock(HierarchicalRepository.class);
      expect(
            queue.expireMessage(messageID, storageManager, postOffice,
                  repository)).andReturn(false);
      replay(queue, storageManager, postOffice, repository);

      QueueControlMBean control = new QueueControl(queue, storageManager,
            postOffice, repository);
      assertFalse(control.expireMessage(messageID));

      verify(queue, storageManager, postOffice, repository);
   }

   public void testExpireMessagesWithFilter() throws Exception
   {
      long messageID_1 = randomLong();
      long messageID_2 = randomLong();

      List<MessageReference> refs = new ArrayList<MessageReference>();
      MessageReference ref_1 = createMock(MessageReference.class);
      ServerMessage message_1 = createMock(ServerMessage.class);
      expect(message_1.getMessageID()).andStubReturn(messageID_1);
      expect(ref_1.getMessage()).andReturn(message_1);
      MessageReference ref_2 = createMock(MessageReference.class);
      ServerMessage message_2 = createMock(ServerMessage.class);
      expect(message_2.getMessageID()).andStubReturn(messageID_2);
      expect(ref_2.getMessage()).andReturn(message_2);
      refs.add(ref_1);
      refs.add(ref_2);
      Queue queue = createMock(Queue.class);
      StorageManager storageManager = createMock(StorageManager.class);
      expect(queue.list(isA(Filter.class))).andReturn(refs);
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> repository = createMock(HierarchicalRepository.class);
      expect(
            queue.expireMessage(messageID_1, storageManager, postOffice,
                  repository)).andReturn(true);
      expect(
            queue.expireMessage(messageID_2, storageManager, postOffice,
                  repository)).andReturn(true);

      replay(queue, storageManager, postOffice, repository, ref_1, ref_2,
            message_1, message_2);

      QueueControlMBean control = new QueueControl(queue, storageManager,
            postOffice, repository);
      assertEquals(2, control.expireMessages("foo = true"));

      verify(queue, storageManager, postOffice, repository, ref_1, ref_2,
            message_1, message_2);
   }

   public void testMoveMessage() throws Exception
   {
      long messageID = randomLong();
      SimpleString otherQueueName = randomSimpleString();
      Queue queue = createMock(Queue.class);
      Binding otherBinding = createMock(Binding.class);
      StorageManager storageManager = createMock(StorageManager.class);
      PostOffice postOffice = createMock(PostOffice.class);
      expect(postOffice.getBinding(otherQueueName)).andReturn(otherBinding);
      HierarchicalRepository<QueueSettings> repository = createMock(HierarchicalRepository.class);
      expect(
            queue.moveMessage(messageID, otherBinding, storageManager,
                  postOffice)).andReturn(true);

      replay(queue, storageManager, postOffice, repository, otherBinding);

      QueueControlMBean control = new QueueControl(queue, storageManager,
            postOffice, repository);
      assertTrue(control.moveMessage(messageID, otherQueueName.toString()));

      verify(queue, storageManager, postOffice, repository, otherBinding);
   }

   public void testMoveMessageWithNoQueue() throws Exception
   {
      long messageID = randomLong();
      SimpleString otherQueueName = randomSimpleString();

      Queue queue = createMock(Queue.class);
      StorageManager storageManager = createMock(StorageManager.class);
      PostOffice postOffice = createMock(PostOffice.class);
      expect(postOffice.getBinding(otherQueueName)).andReturn(null);
      HierarchicalRepository<QueueSettings> repository = createMock(HierarchicalRepository.class);

      replay(queue, storageManager, postOffice, repository);

      QueueControl control = new QueueControl(queue, storageManager,
            postOffice, repository);
      try
      {
         control.moveMessage(messageID, otherQueueName.toString());
         fail("IllegalArgumentException");
      } catch (IllegalArgumentException e)
      {

      }
      verify(queue, storageManager, postOffice, repository);
   }

   public void testMoveMessageWithNoMessageID() throws Exception
   {
      long messageID = randomLong();
      SimpleString otherQueueName = randomSimpleString();
      Queue queue = createMock(Queue.class);
      Binding otherBinding = createMock(Binding.class);
      StorageManager storageManager = createMock(StorageManager.class);
      PostOffice postOffice = createMock(PostOffice.class);
      expect(postOffice.getBinding(otherQueueName)).andReturn(otherBinding);
      HierarchicalRepository<QueueSettings> repository = createMock(HierarchicalRepository.class);
      expect(queue.moveMessage(messageID, otherBinding, storageManager, postOffice)).andReturn(false);
      
      replay(queue, storageManager, postOffice, repository, otherBinding);

      QueueControl control = new QueueControl(queue, storageManager,
            postOffice, repository);
      assertFalse(control.moveMessage(messageID, otherQueueName.toString()));

      verify(queue, storageManager, postOffice, repository, otherBinding);
   }

   public void testChangeMessagePriority() throws Exception
   {
      long messageID = randomLong();
      byte newPriority = 5;
      List<MessageReference> refs = new ArrayList<MessageReference>();
      MessageReference ref = createMock(MessageReference.class);
      refs.add(ref);
      StorageManager storageManager = createMock(StorageManager.class);
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> repository = createMock(HierarchicalRepository.class);
      Queue queue = createMock(Queue.class);
      expect(queue.changeMessagePriority(messageID, newPriority, storageManager, postOffice, repository)).andReturn(true);

      replay(queue, storageManager, postOffice, repository, ref);

      QueueControl control = new QueueControl(queue, storageManager,
            postOffice, repository);
      assertTrue(control.changeMessagePriority(messageID, newPriority));

      verify(queue, storageManager, postOffice, repository, ref);
   }

   public void testChangeMessagePriorityWithInvalidPriorityValues()
         throws Exception
   {
      long messageID = randomLong();
      Queue queue = createMock(Queue.class);
      StorageManager storageManager = createMock(StorageManager.class);
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> repository = createMock(HierarchicalRepository.class);

      replay(queue, storageManager, postOffice, repository);

      QueueControl control = new QueueControl(queue, storageManager,
            postOffice, repository);

      try
      {
         control.changeMessagePriority(messageID, -1);
         fail("IllegalArgumentException");
      } catch (IllegalArgumentException e)
      {
      }

      try
      {
         control.changeMessagePriority(messageID, 10);
         fail("IllegalArgumentException");
      } catch (IllegalArgumentException e)
      {
      }

      verify(queue, storageManager, postOffice, repository);
   }

   public void testChangeMessagePriorityWithNoMessageID() throws Exception
   {
      long messageID = randomLong();
      byte newPriority = 5;
      Queue queue = createMock(Queue.class);
      StorageManager storageManager = createMock(StorageManager.class);
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> repository = createMock(HierarchicalRepository.class);
      expect(queue.changeMessagePriority(messageID, newPriority, storageManager, postOffice, repository)).andReturn(false);
      
      replay(queue, storageManager, postOffice, repository);

      QueueControl control = new QueueControl(queue, storageManager,
            postOffice, repository);
      assertFalse(control.changeMessagePriority(messageID, newPriority));

      verify(queue, storageManager, postOffice, repository);
   }

   public void testSendMessageToDLQ() throws Exception
   {
      long messageID = randomLong();

      Queue queue = createMock(Queue.class);
      StorageManager storageManager = createMock(StorageManager.class);
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> repository = createMock(HierarchicalRepository.class);
      expect(
            queue.sendMessageToDLQ(messageID, storageManager, postOffice,
                  repository)).andReturn(true);

      replay(queue, storageManager, postOffice, repository);

      QueueControlMBean control = new QueueControl(queue, storageManager,
            postOffice, repository);
      assertTrue(control.sendMessageToDLQ(messageID));

      verify(queue, storageManager, postOffice, repository);
   }

   public void testSendMessageToDLQWithNoMessageID() throws Exception
   {
      long messageID = randomLong();

      Queue queue = createMock(Queue.class);
      StorageManager storageManager = createMock(StorageManager.class);
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> repository = createMock(HierarchicalRepository.class);
      expect(
            queue.sendMessageToDLQ(messageID, storageManager, postOffice,
                  repository)).andReturn(false);

      replay(queue, storageManager, postOffice, repository);

      QueueControlMBean control = new QueueControl(queue, storageManager,
            postOffice, repository);
      assertFalse(control.sendMessageToDLQ(messageID));

      verify(queue, storageManager, postOffice, repository);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
