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
import static org.jboss.messaging.tests.util.RandomUtil.randomPositiveInt;
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
import org.jboss.messaging.core.messagecounter.MessageCounter;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.AddressSettings;
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

   private Queue queue;

   private StorageManager storageManager;

   private PostOffice postOffice;

   private HierarchicalRepository<AddressSettings> repository;

   private MessageCounter messageCounter;

   private SimpleString queueName;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testGetName() throws Exception
   {
      expect(queue.getName()).andReturn(queueName);

      replayMockedAttributes();

      QueueControlMBean control = createControl();
      assertEquals(queueName.toString(), control.getName());

      verifyMockedAttributes();
   }

   public void testGetFilter() throws Exception
   {
      String filterStr = "color = 'green'";
      Filter filter = createMock(Filter.class);
      expect(filter.getFilterString()).andReturn(new SimpleString(filterStr));
      expect(queue.getFilter()).andReturn(filter);

      replayMockedAttributes();
      replay(filter);

      QueueControlMBean control = createControl();
      assertEquals(filterStr, control.getFilter());

      verifyMockedAttributes();
      verify(filter);
   }

   public void testGetFilterWithNull() throws Exception
   {
      expect(queue.getFilter()).andReturn(null);

      replayMockedAttributes();

      QueueControlMBean control = createControl();
      assertNull(control.getFilter());

      verifyMockedAttributes();
   }

   public void testIsDurable() throws Exception
   {
      boolean durable = randomBoolean();
      expect(queue.isDurable()).andReturn(durable);

      replayMockedAttributes();

      QueueControlMBean control = createControl();
      assertEquals(durable, control.isDurable());

      verifyMockedAttributes();
   }

   public void testIsTemporary() throws Exception
   {
      boolean temp = randomBoolean();
      expect(queue.isTemporary()).andReturn(temp);

      replayMockedAttributes();

      QueueControlMBean control = createControl();
      assertEquals(temp, control.isTemporary());

      verify(queue, storageManager, postOffice, repository);
   }

   public void testIsBackup() throws Exception
   {
      boolean backup = randomBoolean();
      expect(queue.isBackup()).andReturn(backup);

      replayMockedAttributes();

      QueueControlMBean control = createControl();
      assertEquals(backup, control.isBackup());

      verifyMockedAttributes();
   }

   public void testGetMessageCount() throws Exception
   {
      int count = randomInt();
      expect(queue.getMessageCount()).andReturn(count);

      replayMockedAttributes();

      QueueControlMBean control = createControl();
      assertEquals(count, control.getMessageCount());

      verifyMockedAttributes();
   }

   public void testGetMessagesAdded() throws Exception
   {
      int count = randomInt();
      expect(queue.getMessagesAdded()).andReturn(count);

      replayMockedAttributes();

      QueueControlMBean control = createControl();
      assertEquals(count, control.getMessagesAdded());

      verifyMockedAttributes();
   }

   public void testGetScheduledCount() throws Exception
   {
      int count = randomInt();
      expect(queue.getScheduledCount()).andReturn(count);

      replayMockedAttributes();

      QueueControlMBean control = createControl();
      assertEquals(count, control.getScheduledCount());

      verifyMockedAttributes();
   }

   public void testGetConsumerCount() throws Exception
   {
      int count = randomInt();
      expect(queue.getConsumerCount()).andReturn(count);

      replayMockedAttributes();

      QueueControlMBean control = createControl();
      assertEquals(count, control.getConsumerCount());

      verifyMockedAttributes();
   }

   public void testGetDeliveringCount() throws Exception
   {
      int count = randomInt();
      expect(queue.getDeliveringCount()).andReturn(count);

      replayMockedAttributes();

      QueueControlMBean control = createControl();
      assertEquals(count, control.getDeliveringCount());

      verifyMockedAttributes();
   }

   public void testGetPersistenceID() throws Exception
   {
      long id = randomLong();
      expect(queue.getPersistenceID()).andReturn(id);

      replayMockedAttributes();

      QueueControlMBean control = createControl();
      assertEquals(id, control.getPersistenceID());

      verifyMockedAttributes();
   }

   public void testGetDLQ() throws Exception
   {
      final String dlqName = randomString();

      expect(queue.getName()).andReturn(queueName);
      AddressSettings addressSettings = new AddressSettings()
      {
         @Override
         public SimpleString getDeadLetterAddress()
         {
            return new SimpleString(dlqName);
         }
      };
      expect(repository.getMatch(queueName.toString())).andReturn(addressSettings);

      replayMockedAttributes();

      QueueControlMBean control = createControl();
      assertEquals(dlqName, control.getDeadLetterAddress());

      verifyMockedAttributes();
   }

   public void testGetExpiryQueue() throws Exception
   {
      final String expiryQueueName = randomString();

      expect(queue.getName()).andReturn(queueName);
      AddressSettings addressSettings = new AddressSettings()
      {
         @Override
         public SimpleString getExpiryAddress()
         {
            return new SimpleString(expiryQueueName);
         }
      };
      expect(repository.getMatch(queueName.toString())).andReturn(addressSettings);

      replayMockedAttributes();

      QueueControlMBean control = createControl();
      assertEquals(expiryQueueName, control.getExpiryAddress());

      verifyMockedAttributes();
   }

   public void testRemoveAllMessages() throws Exception
   {
      int messageRemoved = randomPositiveInt();
      expect(queue.deleteAllReferences()).andReturn(messageRemoved);

      replayMockedAttributes();

      QueueControlMBean control = createControl();
      assertEquals(messageRemoved, control.removeAllMessages());

      verifyMockedAttributes();
   }

   public void testRemoveAllMessagesThrowsException() throws Exception
   {
      queue.deleteAllReferences();
      expectLastCall().andThrow(new MessagingException());

      replayMockedAttributes();

      QueueControlMBean control = createControl();
      try
      {
         control.removeAllMessages();
         fail("IllegalStateException");
      }
      catch (IllegalStateException e)
      {

      }

      verifyMockedAttributes();
   }

   public void testRemoveMessage() throws Exception
   {
      long messageID = randomLong();
      boolean deleted = randomBoolean();
      expect(queue.deleteReference(messageID)).andReturn(deleted);

      replayMockedAttributes();

      QueueControlMBean control = createControl();
      assertEquals(deleted, control.removeMessage(messageID));

      verifyMockedAttributes();
   }

   public void testRemoveMessageThrowsException() throws Exception
   {
      long messageID = randomLong();
      expect(queue.deleteReference(messageID)).andThrow(new MessagingException());

      replayMockedAttributes();

      QueueControlMBean control = createControl();
      try
      {
         control.removeMessage(messageID);
         fail("IllegalStateException");
      }
      catch (IllegalStateException e)
      {

      }

      verifyMockedAttributes();
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
      expect(queue.list(isA(Filter.class))).andReturn(refs);

      replayMockedAttributes();
      replay(ref, message);

      QueueControlMBean control = createControl();
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

      verifyMockedAttributes();
      verify(ref, message);
   }

   public void testExpireMessageWithMessageID() throws Exception
   {
      long messageID = randomLong();
      expect(queue.expireMessage(messageID)).andReturn(true);

      replayMockedAttributes();

      QueueControlMBean control = createControl();
      assertTrue(control.expireMessage(messageID));

      verifyMockedAttributes();
   }

   public void testExpireMessageWithNoMatch() throws Exception
   {
      long messageID = randomLong();
      expect(queue.expireMessage(messageID)).andReturn(false);
      replayMockedAttributes();

      QueueControlMBean control = createControl();
      assertFalse(control.expireMessage(messageID));

      verifyMockedAttributes();
   }

   public void testExpireMessagesWithFilter() throws Exception
   {
      int expiredMessagesCount = randomPositiveInt();

      expect(queue.expireMessages(isA(Filter.class))).andReturn(expiredMessagesCount);
      replayMockedAttributes();

      QueueControlMBean control = createControl();
      assertEquals(expiredMessagesCount, control.expireMessages("foo = true"));

      verifyMockedAttributes();
   }

   public void testMoveMessage() throws Exception
   {
      long messageID = randomLong();
      SimpleString otherQueueName = randomSimpleString();
      SimpleString otherAddress = randomSimpleString();
      Binding otherBinding = createMock(Binding.class);
      expect(otherBinding.getAddress()).andReturn(otherAddress);
      expect(postOffice.getBinding(otherQueueName)).andReturn(otherBinding);
      expect(queue.moveMessage(messageID, otherAddress)).andReturn(true);

      replayMockedAttributes();
      replay(otherBinding);

      QueueControlMBean control = createControl();
      assertTrue(control.moveMessage(messageID, otherQueueName.toString()));

      verifyMockedAttributes();
      verify(otherBinding);
   }

   public void testMoveMessageWithNoQueue() throws Exception
   {
      long messageID = randomLong();
      SimpleString otherQueueName = randomSimpleString();
      expect(postOffice.getBinding(otherQueueName)).andReturn(null);

      replayMockedAttributes();

      QueueControl control = createControl();
      try
      {
         control.moveMessage(messageID, otherQueueName.toString());
         fail("IllegalArgumentException");
      }
      catch (IllegalArgumentException e)
      {

      }
      verifyMockedAttributes();
   }

   public void testMoveMessageWithNoMessageID() throws Exception
   {
      long messageID = randomLong();
      SimpleString otherQueueName = randomSimpleString();
      SimpleString otherAddress = randomSimpleString();
      Binding otherBinding = createMock(Binding.class);
      expect(otherBinding.getAddress()).andReturn(otherAddress);
      expect(postOffice.getBinding(otherQueueName)).andReturn(otherBinding);
      expect(queue.moveMessage(messageID, otherAddress)).andReturn(false);

      replayMockedAttributes();
      replay(otherBinding);

      QueueControl control = createControl();
      assertFalse(control.moveMessage(messageID, otherQueueName.toString()));

      verifyMockedAttributes();
      verify(otherBinding);
   }

   public void testChangeMessagePriority() throws Exception
   {
      long messageID = randomLong();
      byte newPriority = 5;
      List<MessageReference> refs = new ArrayList<MessageReference>();
      MessageReference ref = createMock(MessageReference.class);
      refs.add(ref);
      expect(queue.changeMessagePriority(messageID, newPriority)).andReturn(true);

      replayMockedAttributes();
      replay(ref);

      QueueControl control = createControl();
      assertTrue(control.changeMessagePriority(messageID, newPriority));

      verifyMockedAttributes();
      verify(ref);
   }

   public void testChangeMessagePriorityWithInvalidPriorityValues() throws Exception
   {
      long messageID = randomLong();

      replayMockedAttributes();

      QueueControl control = createControl();

      try
      {
         control.changeMessagePriority(messageID, -1);
         fail("IllegalArgumentException");
      }
      catch (IllegalArgumentException e)
      {
      }

      try
      {
         control.changeMessagePriority(messageID, 10);
         fail("IllegalArgumentException");
      }
      catch (IllegalArgumentException e)
      {
      }

      verifyMockedAttributes();
   }

   public void testChangeMessagePriorityWithNoMessageID() throws Exception
   {
      long messageID = randomLong();
      byte newPriority = 5;
      expect(queue.changeMessagePriority(messageID, newPriority)).andReturn(false);

      replayMockedAttributes();

      QueueControl control = createControl();
      assertFalse(control.changeMessagePriority(messageID, newPriority));

      verifyMockedAttributes();
   }

   public void testSendMessageToDeadLetterAddress() throws Exception
   {
      long messageID = randomLong();
      expect(queue.sendMessageToDeadLetterAddress(messageID)).andReturn(true);

      replayMockedAttributes();

      QueueControlMBean control = createControl();
      assertTrue(control.sendMessageToDeadLetterAddress(messageID));

      verifyMockedAttributes();
   }

   public void testSendMessageToDeadLetterAddressWithNoMessageID() throws Exception
   {
      long messageID = randomLong();
      expect(queue.sendMessageToDeadLetterAddress(messageID)).andReturn(false);

      replayMockedAttributes();

      QueueControlMBean control = createControl();
      assertFalse(control.sendMessageToDeadLetterAddress(messageID));

      verifyMockedAttributes();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      queueName = randomSimpleString();
      queue = createMock(Queue.class);
      storageManager = createMock(StorageManager.class);
      postOffice = createMock(PostOffice.class);
      repository = createMock(HierarchicalRepository.class);
      messageCounter = new MessageCounter(queueName.toString(), null, queue, false, false, 10);
   }

   @Override
   protected void tearDown() throws Exception
   {
      queue = null;
      storageManager = null;
      postOffice = null;
      repository = null;
      messageCounter = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   private void replayMockedAttributes()
   {
      replay(queue, storageManager, postOffice, repository);
   }

   private void verifyMockedAttributes()
   {
      verify(queue, storageManager, postOffice, repository);
   }

   private QueueControl createControl() throws Exception
   {
      return new QueueControl(queue, postOffice, repository, messageCounter);
   }

   // Inner classes -------------------------------------------------
}
