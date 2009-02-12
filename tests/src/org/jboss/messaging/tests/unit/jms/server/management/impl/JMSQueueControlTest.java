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
import static org.jboss.messaging.tests.util.RandomUtil.randomPositiveInt;
import static org.jboss.messaging.tests.util.RandomUtil.randomSimpleString;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import javax.management.NotCompliantMBeanException;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import junit.framework.TestCase;

import org.easymock.EasyMock;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.messagecounter.MessageCounter;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.AddressSettings;
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
   private String jndiBinding;

   private String name;

   private JBossQueue queue;

   private Queue coreQueue;

   private PostOffice postOffice;

   private StorageManager storageManager;

   private HierarchicalRepository<AddressSettings> addressSettingsRepository;

   private MessageCounter counter;

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testGetName() throws Exception
   {
      replayMockedAttributes();

      JMSQueueControl control = createControl();
      assertEquals(name, control.getName());

      verifyMockedAttributes();
   }

   public void testGetAddress() throws Exception
   {
      replayMockedAttributes();

      JMSQueueControl control = createControl();
      assertEquals(queue.getAddress(), control.getAddress());

      verifyMockedAttributes();
   }

   public void testGetJNDIBinding() throws Exception
   {
      replayMockedAttributes();

      JMSQueueControl control = createControl();
      assertEquals(jndiBinding, control.getJNDIBinding());

      verifyMockedAttributes();
   }

   public void testIsTemporary() throws Exception
   {
      replayMockedAttributes();

      JMSQueueControl control = createControl();
      assertEquals(queue.isTemporary(), control.isTemporary());

      verifyMockedAttributes();
   }

   public void testIsDurabled() throws Exception
   {
      boolean durable = randomBoolean();

      expect(coreQueue.isDurable()).andReturn(durable);
      replayMockedAttributes();

      JMSQueueControl control = createControl();
      assertEquals(durable, control.isDurable());

      verifyMockedAttributes();
   }

   public void testGetMessageCount() throws Exception
   {
      int count = randomInt();

      expect(coreQueue.getMessageCount()).andReturn(count);
      replayMockedAttributes();

      JMSQueueControl control = createControl();
      assertEquals(count, control.getMessageCount());

      verifyMockedAttributes();
   }

   public void testGetMessagesAdded() throws Exception
   {
      int count = randomInt();

      expect(coreQueue.getMessagesAdded()).andReturn(count);
      replayMockedAttributes();

      JMSQueueControl control = createControl();
      assertEquals(count, control.getMessagesAdded());

      verifyMockedAttributes();
   }

   public void testGetConsumerCount() throws Exception
   {
      int count = randomInt();

      expect(coreQueue.getConsumerCount()).andReturn(count);
      replayMockedAttributes();

      JMSQueueControl control = createControl();
      assertEquals(count, control.getConsumerCount());

      verifyMockedAttributes();
   }

   public void testGetDeliveringCount() throws Exception
   {
      int count = randomInt();

      expect(coreQueue.getDeliveringCount()).andReturn(count);
      replayMockedAttributes();

      JMSQueueControl control = createControl();
      assertEquals(count, control.getDeliveringCount());

      verifyMockedAttributes();
   }

   public void testGetScheduledCount() throws Exception
   {
      int count = randomInt();

      expect(coreQueue.getScheduledCount()).andReturn(count);
      replayMockedAttributes();

      JMSQueueControl control = createControl();
      assertEquals(count, control.getScheduledCount());

      verifyMockedAttributes();
   }

   public void testGetDeadLetterAddress() throws Exception
   {
      final String deadLetterAddress = randomString();

      AddressSettings settings = new AddressSettings()
      {
         @Override
         public SimpleString getDeadLetterAddress()
         {
            return new SimpleString(JBossQueue.JMS_QUEUE_ADDRESS_PREFIX + deadLetterAddress);
         }
      };
      expect(addressSettingsRepository.getMatch(JBossQueue.JMS_QUEUE_ADDRESS_PREFIX + name)).andReturn(settings);

      replayMockedAttributes();

      JMSQueueControl control = createControl();
      assertEquals(settings.getDeadLetterAddress().toString(), control.getDeadLetterAddress());

      verifyMockedAttributes();
   }

   public void testGetExpiryAddress() throws Exception
   {
      final String expiryQueue = randomString();

      AddressSettings settings = new AddressSettings()
      {
         @Override
         public SimpleString getExpiryAddress()
         {
            return new SimpleString(JBossQueue.JMS_QUEUE_ADDRESS_PREFIX + expiryQueue);
         }
      };
      expect(addressSettingsRepository.getMatch(JBossQueue.JMS_QUEUE_ADDRESS_PREFIX + name)).andReturn(settings);

      replayMockedAttributes();

      JMSQueueControl control = createControl();
      assertEquals(settings.getExpiryAddress().toString(), control.getExpiryAddress());

      verifyMockedAttributes();
   }

   public void testRemoveMessage() throws Exception
   {
      String jmsMessageID = randomString();
      long messageID = randomLong();

      List<MessageReference> refs = new ArrayList<MessageReference>();
      MessageReference ref = createMock(MessageReference.class);
      ServerMessage message = createMock(ServerMessage.class);
      expect(message.getMessageID()).andReturn(messageID);
      expect(ref.getMessage()).andReturn(message);
      refs.add(ref);
      expect(coreQueue.list(EasyMock.isA(Filter.class))).andReturn(refs);
      expect(coreQueue.deleteReference(messageID)).andReturn(true);

      replayMockedAttributes();
      replay(ref, message);

      JMSQueueControl control = createControl();
      assertTrue(control.removeMessage(jmsMessageID));

      verifyMockedAttributes();
      verify(ref, message);
   }

   public void testRemoveAllMessages() throws Exception
   {
      int removedMessagesCount = randomPositiveInt();
      expect(coreQueue.deleteAllReferences()).andReturn(removedMessagesCount);

      replayMockedAttributes();

      JMSQueueControl control = createControl();
      assertEquals(removedMessagesCount, control.removeAllMessages());

      verifyMockedAttributes();
   }

   public void testListMessages() throws Exception
   {
      String filterStr = "color = 'green'";
      List<MessageReference> refs = new ArrayList<MessageReference>();
      MessageReference ref = createMock(MessageReference.class);
      ServerMessage message = createMock(ServerMessage.class);
      expect(message.getProperty(new SimpleString("JMSMessageID"))).andStubReturn(randomSimpleString());
      expect(message.getProperty(new SimpleString("JMSCorrelationID"))).andStubReturn(randomSimpleString());
      expect(message.getProperty(new SimpleString("JMSType"))).andStubReturn(randomSimpleString());
      expect(message.isDurable()).andStubReturn(randomBoolean());
      expect(message.getPriority()).andStubReturn(randomByte());
      expect(message.getProperty(new SimpleString("JMSReplyTo"))).andStubReturn(randomSimpleString());
      expect(message.getTimestamp()).andStubReturn(randomLong());
      expect(message.getExpiration()).andStubReturn(randomLong());
      expect(message.getPropertyNames()).andStubReturn(new HashSet<SimpleString>());
      expect(ref.getMessage()).andReturn(message);
      refs.add(ref);
      expect(coreQueue.list(isA(Filter.class))).andReturn(refs);

      replayMockedAttributes();
      replay(ref, message);

      JMSQueueControl control = createControl();
      TabularData data = control.listMessages(filterStr);
      assertEquals(1, data.size());
      CompositeData info = data.get(new Object[] { message.getProperty(new SimpleString("JMSMessageID")).toString() });
      assertNotNull(info);

      verifyMockedAttributes();
      verify(ref, message);
   }

   public void testListMessagesThrowsException() throws Exception
   {
      String invalidFilterStr = "this is not a valid filter";

      replayMockedAttributes();

      JMSQueueControl control = createControl();
      try
      {
         control.listMessages(invalidFilterStr);
         fail("IllegalStateException");
      }
      catch (IllegalStateException e)
      {

      }

      verifyMockedAttributes();
   }

   public void testExpireMessage() throws Exception
   {
      String jmsMessageID = randomString();
      long messageID = randomLong();

      List<MessageReference> refs = new ArrayList<MessageReference>();
      MessageReference ref = createMock(MessageReference.class);
      ServerMessage serverMessage = createMock(ServerMessage.class);
      expect(serverMessage.getMessageID()).andReturn(messageID);
      expect(ref.getMessage()).andReturn(serverMessage);
      refs.add(ref);
      expect(coreQueue.list(EasyMock.isA(Filter.class))).andReturn(refs);
      expect(coreQueue.expireMessage(messageID)).andReturn(true);

      replayMockedAttributes();
      replay(ref, serverMessage);

      JMSQueueControl control = createControl();
      assertTrue(control.expireMessage(jmsMessageID));

      verifyMockedAttributes();
      verify(ref, serverMessage);
   }

   public void testExpireMessageWithNoJMSMesageID() throws Exception
   {
      String jmsMessageID = randomString();

      expect(coreQueue.list(isA(Filter.class))).andReturn(new ArrayList<MessageReference>());

      replayMockedAttributes();

      JMSQueueControl control = createControl();
      try
      {
         control.expireMessage(jmsMessageID);
         fail("IllegalArgumentException");
      }
      catch (IllegalArgumentException e)
      {
      }

      verifyMockedAttributes();
   }

   public void testExpireMessages() throws Exception
   {
      int expiredMessages = randomInt();

      expect(coreQueue.expireMessages(isA(Filter.class))).andReturn(expiredMessages);

      replayMockedAttributes();

      JMSQueueControl control = createControl();
      assertEquals(expiredMessages, control.expireMessages("color = 'green'"));

      verifyMockedAttributes();
   }

   public void testSendMessageToDLQ() throws Exception
   {
      String jmsMessageID = randomString();
      long messageID = randomLong();

      List<MessageReference> refs = new ArrayList<MessageReference>();
      MessageReference ref = createMock(MessageReference.class);
      ServerMessage serverMessage = createMock(ServerMessage.class);
      expect(serverMessage.getMessageID()).andReturn(messageID);
      expect(ref.getMessage()).andReturn(serverMessage);
      refs.add(ref);
      expect(coreQueue.list(isA(Filter.class))).andReturn(refs);
      expect(coreQueue.sendMessageToDeadLetterAddress(messageID)).andReturn(true);

      replayMockedAttributes();
      replay(ref, serverMessage);

      JMSQueueControl control = createControl();
      assertTrue(control.sendMessageToDLQ(jmsMessageID));

      verifyMockedAttributes();
      verify(ref, serverMessage);
   }

   public void testSendMessageToDLQWithNoJMSMessageID() throws Exception
   {
      String jmsMessageID = randomString();

      expect(coreQueue.list(isA(Filter.class))).andReturn(new ArrayList<MessageReference>());
      replayMockedAttributes();

      JMSQueueControl control = createControl();
      try
      {
         control.sendMessageToDLQ(jmsMessageID);
         fail("IllegalArgumentException");
      }
      catch (IllegalArgumentException e)
      {
      }

      verifyMockedAttributes();
   }

   public void testChangeMessagePriority() throws Exception
   {
      byte newPriority = 5;
      String jmsMessageID = randomString();
      long messageID = randomLong();

      List<MessageReference> refs = new ArrayList<MessageReference>();
      MessageReference ref = createMock(MessageReference.class);
      ServerMessage serverMessage = createMock(ServerMessage.class);
      expect(serverMessage.getMessageID()).andReturn(messageID);
      expect(ref.getMessage()).andReturn(serverMessage);
      refs.add(ref);
      expect(coreQueue.list(isA(Filter.class))).andReturn(refs);
      expect(coreQueue.changeMessagePriority(messageID,
                                             newPriority)).andReturn(true);

      replayMockedAttributes();
      replay(ref, serverMessage);

      JMSQueueControl control = createControl();
      assertTrue(control.changeMessagePriority(jmsMessageID, newPriority));

      verifyMockedAttributes();
      verify(ref, serverMessage);
   }

   public void testChangeMessagePriorityWithInvalidPriorityValues() throws Exception
   {
      String jmsMessageID = randomString();

      replayMockedAttributes();

      JMSQueueControl control = createControl();
      try
      {
         control.changeMessagePriority(jmsMessageID, -1);
         fail("IllegalArgumentException");
      }
      catch (IllegalArgumentException e)
      {
      }

      try
      {
         control.changeMessagePriority(jmsMessageID, 10);
         fail("IllegalArgumentException");
      }
      catch (IllegalArgumentException e)
      {
      }

      verifyMockedAttributes();
   }

   public void testChangeMessagePriorityWithNoJMSMessageID() throws Exception
   {
      byte newPriority = 5;
      String jmsMessageID = randomString();

      expect(coreQueue.list(isA(Filter.class))).andReturn(new ArrayList<MessageReference>());

      replayMockedAttributes();

      JMSQueueControl control = createControl();
      try
      {
         control.changeMessagePriority(jmsMessageID, newPriority);
         fail("IllegalArgumentException");
      }
      catch (IllegalArgumentException e)
      {
      }

      verifyMockedAttributes();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      jndiBinding = randomString();
      name = randomString();

      queue = new JBossQueue(name);
      coreQueue = createMock(Queue.class);
      postOffice = createMock(PostOffice.class);
      storageManager = createMock(StorageManager.class);
      addressSettingsRepository = createMock(HierarchicalRepository.class);
      counter = new MessageCounter(name, null, coreQueue, false, true, 10);
   }

   @Override
   protected void tearDown() throws Exception
   {
      jndiBinding = null;
      name = null;
      queue = null;
      coreQueue = null;
      postOffice = null;
      storageManager = null;
      addressSettingsRepository = null;
      counter = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   private JMSQueueControl createControl() throws NotCompliantMBeanException
   {
      return new JMSQueueControl(queue,
                                 coreQueue,
                                 jndiBinding,
                                 postOffice,                               
                                 addressSettingsRepository,
                                 counter);
   }

   private void replayMockedAttributes()
   {
      replay(coreQueue, postOffice, storageManager, addressSettingsRepository);
   }

   private void verifyMockedAttributes()
   {
      verify(coreQueue, postOffice, storageManager, addressSettingsRepository);
   }

   // Inner classes -------------------------------------------------
}
