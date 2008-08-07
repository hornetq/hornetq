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
import static org.easymock.EasyMock.eq;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import junit.framework.TestCase;

import org.easymock.EasyMock;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.server.JMSServerManager;
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
      JMSServerManager serverManager = createMock(JMSServerManager.class);

      replay(coreQueue, serverManager);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, serverManager);
      assertEquals(name, control.getName());

      verify(coreQueue, serverManager);
   }

   public void testGetAddress() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      JMSServerManager serverManager = createMock(JMSServerManager.class);

      replay(coreQueue, serverManager);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, serverManager);
      assertEquals(queue.getAddress(), control.getAddress());

      verify(coreQueue, serverManager);
   }

   public void testGetJNDIBinding() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      JMSServerManager serverManager = createMock(JMSServerManager.class);

      replay(coreQueue, serverManager);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, serverManager);
      assertEquals(jndiBinding, control.getJNDIBinding());

      verify(coreQueue, serverManager);
   }

   public void testIsTemporary() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      JMSServerManager serverManager = createMock(JMSServerManager.class);

      replay(coreQueue, serverManager);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, serverManager);
      assertEquals(queue.isTemporary(), control.isTemporary());

      verify(coreQueue, serverManager);
   }

   public void testGetMessageCount() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();
      int count = randomInt();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      expect(coreQueue.getMessageCount()).andReturn(count);
      JMSServerManager serverManager = createMock(JMSServerManager.class);

      replay(coreQueue, serverManager);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, serverManager);
      assertEquals(count, control.getMessageCount());

      verify(coreQueue, serverManager);
   }

   public void testGetDLQ() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();
      final String dlq = randomString();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      JMSServerManager serverManager = createMock(JMSServerManager.class);
      QueueSettings settings = new QueueSettings()
      {
         @Override
         public SimpleString getDLQ()
         {
            return new SimpleString(JBossQueue.JMS_QUEUE_ADDRESS_PREFIX + dlq);
         }
      };
      expect(serverManager.getSettings(queue)).andReturn(settings);

      replay(coreQueue, serverManager);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, serverManager);
      assertEquals(dlq, control.getDLQ());

      verify(coreQueue, serverManager);
   }

   public void testGetExpiryQueue() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();
      final String expiryQueue = randomString();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      JMSServerManager serverManager = createMock(JMSServerManager.class);
      QueueSettings settings = new QueueSettings()
      {
         @Override
         public SimpleString getExpiryQueue()
         {
            return new SimpleString(JBossQueue.JMS_QUEUE_ADDRESS_PREFIX
                  + expiryQueue);
         }
      };
      expect(serverManager.getSettings(queue)).andReturn(settings);

      replay(coreQueue, serverManager);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, serverManager);
      assertEquals(expiryQueue, control.getExpiryQueue());

      verify(coreQueue, serverManager);
   }

   public void testRemoveMessage() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();
      String jmsMessageID = randomString();
      long messageID = randomLong();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      JMSServerManager serverManager = createMock(JMSServerManager.class);
      List<MessageReference> refs = new ArrayList<MessageReference>();
      MessageReference ref = createMock(MessageReference.class);
      ServerMessage message = createMock(ServerMessage.class);
      expect(message.getMessageID()).andReturn(messageID);
      expect(ref.getMessage()).andReturn(message);
      refs.add(ref);
      expect(coreQueue.list(EasyMock.isA(Filter.class))).andReturn(refs);
      expect(serverManager.removeMessage(messageID, queue)).andReturn(true);

      replay(coreQueue, serverManager, ref, message);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, serverManager);
      assertTrue(control.removeMessage(jmsMessageID));

      verify(coreQueue, serverManager, ref, message);
   }

   public void testRemoveAllMessages() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      JMSServerManager serverManager = createMock(JMSServerManager.class);
      serverManager.removeAllMessages(queue);

      replay(coreQueue, serverManager);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, serverManager);
      control.removeAllMessages();

      verify(coreQueue, serverManager);
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
      JMSServerManager serverManager = createMock(JMSServerManager.class);

      replay(coreQueue, serverManager, ref, message);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, serverManager);
      TabularData data = control.listMessages(filterStr);
      assertEquals(1, data.size());
      CompositeData info = data.get(new Object[] { message.getProperty(
            new SimpleString("JMSMessageID")).toString() });
      assertNotNull(info);

      verify(coreQueue, serverManager, ref, message);
   }

   public void testListMessagesThrowsException() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();
      String invalidFilterStr = "this is not a valid filter";

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      JMSServerManager serverManager = createMock(JMSServerManager.class);

      replay(coreQueue, serverManager);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, serverManager);
      try
      {
         control.listMessages(invalidFilterStr);
         fail("IllegalStateException");
      } catch (IllegalStateException e)
      {

      }
      verify(coreQueue, serverManager);
   }

   public void testExpireMessage() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();
      String jmsMessageID = randomString();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      JMSServerManager serverManager = createMock(JMSServerManager.class);
      List<MessageReference> refs = new ArrayList<MessageReference>();
      MessageReference ref = createMock(MessageReference.class);
      refs.add(ref);
      expect(coreQueue.list(EasyMock.isA(Filter.class))).andReturn(refs);
      expect(serverManager.expireMessages(isA(Filter.class), eq(queue)))
            .andReturn(1);

      replay(coreQueue, serverManager, ref);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, serverManager);
      assertTrue(control.expireMessage(jmsMessageID));

      verify(coreQueue, serverManager, ref);
   }

   public void testExpireMessageWithNoJMSMesageID() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();
      String jmsMessageID = randomString();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      JMSServerManager serverManager = createMock(JMSServerManager.class);
      expect(coreQueue.list(isA(Filter.class))).andReturn(
            new ArrayList<MessageReference>());

      replay(coreQueue, serverManager);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, serverManager);
      try
      {
         control.expireMessage(jmsMessageID);
         fail("IllegalArgumentException");
      } catch (IllegalArgumentException e)
      {
      }

      verify(coreQueue, serverManager);
   }
   
   public void testExpireMessages() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();
      int expiredMessage = randomInt();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      JMSServerManager serverManager = createMock(JMSServerManager.class);
      expect(serverManager.expireMessages(isA(Filter.class), eq(queue)))
            .andReturn(expiredMessage);

      replay(coreQueue, serverManager);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, serverManager);
      assertEquals(expiredMessage, control.expireMessages("color = 'green'"));

      verify(coreQueue, serverManager);
   }

   public void testSendMessageToDLQ() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();
      String jmsMessageID = randomString();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      JMSServerManager serverManager = createMock(JMSServerManager.class);
      List<MessageReference> refs = new ArrayList<MessageReference>();
      MessageReference ref = createMock(MessageReference.class);
      refs.add(ref);
      expect(coreQueue.list(isA(Filter.class))).andReturn(refs);
      expect(serverManager.sendMessagesToDLQ(isA(Filter.class), eq(queue)))
            .andReturn(1);

      replay(coreQueue, serverManager, ref);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, serverManager);
      assertTrue(control.sendMessageTDLQ(jmsMessageID));

      verify(coreQueue, serverManager, ref);
   }

   public void testSendMessageToDLQWithNoJMSMesageID() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();
      String jmsMessageID = randomString();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      JMSServerManager serverManager = createMock(JMSServerManager.class);
      expect(coreQueue.list(isA(Filter.class))).andReturn(
            new ArrayList<MessageReference>());

      replay(coreQueue, serverManager);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, serverManager);
      try
      {
         control.sendMessageTDLQ(jmsMessageID);
         fail("IllegalArgumentException");
      } catch (IllegalArgumentException e)
      {
      }

      verify(coreQueue, serverManager);
   }
   
   public void testChangeMessagePriority() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();
      byte newPriority = 5;
      String jmsMessageID = randomString();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      JMSServerManager serverManager = createMock(JMSServerManager.class);
      List<MessageReference> refs = new ArrayList<MessageReference>();
      MessageReference ref = createMock(MessageReference.class);
      refs.add(ref);
      expect(coreQueue.list(isA(Filter.class))).andReturn(refs);
      expect(
            serverManager.changeMessagesPriority(isA(Filter.class),
                  eq(newPriority), eq(queue))).andReturn(1);

      replay(coreQueue, serverManager, ref);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, serverManager);
      assertTrue(control.changeMessagePriority(jmsMessageID, newPriority));

      verify(coreQueue, serverManager, ref);
   }

   public void testChangeMessagePriorityWithInvalidPriorityValues()
         throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();
      String jmsMessageID = randomString();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      JMSServerManager serverManager = createMock(JMSServerManager.class);

      replay(coreQueue, serverManager);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, serverManager);
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

      verify(coreQueue, serverManager);
   }

   public void testChangeMessagePriorityWithNoJMSMesageID() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();
      byte newPriority = 5;
      String jmsMessageID = randomString();

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      JMSServerManager serverManager = createMock(JMSServerManager.class);
      expect(coreQueue.list(isA(Filter.class))).andReturn(
            new ArrayList<MessageReference>());

      replay(coreQueue, serverManager);

      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, serverManager);
      try
      {
         control.changeMessagePriority(jmsMessageID, newPriority);
         fail("IllegalArgumentException");
      } catch (IllegalArgumentException e)
      {
      }

      verify(coreQueue, serverManager);
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
