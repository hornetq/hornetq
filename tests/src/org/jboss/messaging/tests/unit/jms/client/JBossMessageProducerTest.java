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

package org.jboss.messaging.tests.unit.jms.client;

import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.anyLong;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.isNull;
import static org.easymock.EasyMock.startsWith;
import static org.easymock.classextension.EasyMock.createStrictMock;
import static org.easymock.classextension.EasyMock.replay;
import static org.easymock.classextension.EasyMock.verify;
import static org.jboss.messaging.tests.util.RandomUtil.randomBoolean;
import static org.jboss.messaging.tests.util.RandomUtil.randomBytes;
import static org.jboss.messaging.tests.util.RandomUtil.randomSimpleString;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.util.Vector;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.Topic;

import org.easymock.EasyMock;
import org.jboss.messaging.core.buffers.ChannelBuffers;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.impl.ClientMessageImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.jms.JBossDestination;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.JBossTopic;
import org.jboss.messaging.jms.client.JBossConnection;
import org.jboss.messaging.jms.client.JBossMessage;
import org.jboss.messaging.jms.client.JBossMessageProducer;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.utils.SimpleString;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class JBossMessageProducerTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testClose() throws Exception
   {
      ClientProducer clientProducer = createStrictMock(ClientProducer.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      JBossConnection jbConn = createStrictMock(JBossConnection.class);
      EasyMock.expect(jbConn.getUID()).andReturn(randomSimpleString());
      clientProducer.close();

      replay(clientProducer, clientSession, jbConn);

      JBossDestination destination = new JBossQueue(randomString());
      JBossMessageProducer producer = new JBossMessageProducer(jbConn, clientProducer,
            destination, clientSession);

      producer.close();

      verify(clientProducer, clientSession, jbConn);
   }

   public void testCloseThrowsException() throws Exception
   {
      ClientProducer clientProducer = createStrictMock(ClientProducer.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      JBossConnection jbConn = createStrictMock(JBossConnection.class);
      EasyMock.expect(jbConn.getUID()).andReturn(randomSimpleString());
      clientProducer.close();
      expectLastCall().andThrow(new MessagingException());

      replay(clientProducer, clientSession, jbConn);

      JBossDestination destination = new JBossQueue(randomString());
      JBossMessageProducer producer = new JBossMessageProducer(jbConn, clientProducer,
            destination, clientSession);

      try
      {
         producer.close();
         fail("JMSException");
      } catch (JMSException e)
      {
      }

      verify(clientProducer, clientSession, jbConn);
   }

   public void testCheckClosed() throws Exception
   {
      JBossDestination destination = new JBossQueue(randomString());
      ClientProducer clientProducer = createStrictMock(ClientProducer.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      JBossConnection jbConn = createStrictMock(JBossConnection.class);
      EasyMock.expect(jbConn.getUID()).andReturn(randomSimpleString());
      expect(clientProducer.isClosed()).andReturn(true);
      replay(clientProducer, clientSession, jbConn);

      JBossMessageProducer producer = new JBossMessageProducer(jbConn, clientProducer,
            destination, clientSession);

      try
      {
         producer.getDeliveryMode();
         fail("IllegalStateException");
      } catch (IllegalStateException e)
      {
      }

      verify(clientProducer, clientSession, jbConn);
   }

   public void testDisabledMessageID() throws Exception
   {
      ClientProducer clientProducer = createStrictMock(ClientProducer.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      EasyMock.expect(clientProducer.isClosed()).andStubReturn(false);
      JBossConnection jbConn = createStrictMock(JBossConnection.class);
      EasyMock.expect(jbConn.getUID()).andReturn(randomSimpleString());
      replay(clientProducer, clientSession, jbConn);

      JBossDestination destination = new JBossQueue(randomString());
      JBossMessageProducer producer = new JBossMessageProducer(jbConn, clientProducer,
            destination, clientSession);
      boolean disabledMessageID = randomBoolean();
      producer.setDisableMessageID(disabledMessageID);
      assertEquals(disabledMessageID, producer.getDisableMessageID());

      verify(clientProducer, clientSession, jbConn);
   }

   public void testDisableMessageTimestamp() throws Exception
   {
      ClientProducer clientProducer = createStrictMock(ClientProducer.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      JBossConnection jbConn = createStrictMock(JBossConnection.class);
      EasyMock.expect(jbConn.getUID()).andReturn(randomSimpleString());
      EasyMock.expect(clientProducer.isClosed()).andStubReturn(false);

      replay(clientProducer, clientSession, jbConn);

      JBossDestination destination = new JBossQueue(randomString());
      JBossMessageProducer producer = new JBossMessageProducer(jbConn, clientProducer,
            destination, clientSession);;
      boolean disabledTimestamp = randomBoolean();
      producer.setDisableMessageTimestamp(disabledTimestamp);
      assertEquals(disabledTimestamp, producer.getDisableMessageTimestamp());

      verify(clientProducer, clientSession, jbConn);
   }

   public void testDeliveryMode() throws Exception
   {
      ClientProducer clientProducer = createStrictMock(ClientProducer.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      JBossConnection jbConn = createStrictMock(JBossConnection.class);
      EasyMock.expect(jbConn.getUID()).andReturn(randomSimpleString());
      EasyMock.expect(clientProducer.isClosed()).andStubReturn(false);
      replay(clientProducer, clientSession, jbConn);

      JBossDestination destination = new JBossQueue(randomString());
      JBossMessageProducer producer = new JBossMessageProducer(jbConn, clientProducer,
            destination, clientSession);
      int deliveryMode = DeliveryMode.PERSISTENT;
      producer.setDeliveryMode(deliveryMode);
      assertEquals(deliveryMode, producer.getDeliveryMode());

      verify(clientProducer, clientSession, jbConn);
   }

   public void testPriority() throws Exception
   {
      ClientProducer clientProducer = createStrictMock(ClientProducer.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      JBossConnection jbConn = createStrictMock(JBossConnection.class);
      EasyMock.expect(jbConn.getUID()).andReturn(randomSimpleString());
      EasyMock.expect(clientProducer.isClosed()).andStubReturn(false);
      replay(clientProducer, clientSession, jbConn);

      JBossDestination destination = new JBossQueue(randomString());
      JBossMessageProducer producer = new JBossMessageProducer(jbConn, clientProducer,
            destination, clientSession);
      int priority = 7;
      producer.setPriority(priority);
      assertEquals(priority, producer.getPriority());

      verify(clientProducer, clientSession, jbConn);
   }

   public void testTimeToLive() throws Exception
   {
      ClientProducer clientProducer = createStrictMock(ClientProducer.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      JBossConnection jbConn = createStrictMock(JBossConnection.class);
      EasyMock.expect(jbConn.getUID()).andReturn(randomSimpleString());
      EasyMock.expect(clientProducer.isClosed()).andStubReturn(false);
      replay(clientProducer, clientSession, jbConn);

      JBossDestination destination = new JBossQueue(randomString());
      JBossMessageProducer producer = new JBossMessageProducer(jbConn, clientProducer,
            destination, clientSession);
      long ttl = System.currentTimeMillis();
      producer.setTimeToLive(ttl);
      assertEquals(ttl, producer.getTimeToLive());

      verify(clientProducer, clientSession, jbConn);
   }

   public void testGetDestination() throws Exception
   {
      ClientProducer clientProducer = createStrictMock(ClientProducer.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      JBossConnection jbConn = createStrictMock(JBossConnection.class);
      EasyMock.expect(jbConn.getUID()).andReturn(randomSimpleString());
      EasyMock.expect(clientProducer.isClosed()).andStubReturn(false);
      replay(clientProducer, clientSession, jbConn);

      JBossDestination destination = new JBossQueue(randomString());
      JBossMessageProducer producer = new JBossMessageProducer(jbConn, clientProducer,
            destination, clientSession);
      assertEquals(destination, producer.getDestination());

      verify(clientProducer, clientSession, jbConn);
   }

   public void testGetDelegate() throws Exception
   {
      ClientProducer clientProducer = createStrictMock(ClientProducer.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      JBossConnection jbConn = createStrictMock(JBossConnection.class);
      EasyMock.expect(jbConn.getUID()).andReturn(randomSimpleString());
      EasyMock.expect(clientProducer.isClosed()).andStubReturn(false);
      EasyMock.expect(clientProducer.isClosed()).andStubReturn(false);
      replay(clientProducer, clientSession, jbConn);

      JBossDestination destination = new JBossQueue(randomString());
      JBossMessageProducer producer = new JBossMessageProducer(jbConn, clientProducer,
            destination, clientSession);
      assertEquals(destination, producer.getDestination());
      verify(clientProducer, clientSession, jbConn);
   }

   public void testGetTopic() throws Exception
   {
      ClientProducer clientProducer = createStrictMock(ClientProducer.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      JBossConnection jbConn = createStrictMock(JBossConnection.class);
      EasyMock.expect(jbConn.getUID()).andReturn(randomSimpleString());
      EasyMock.expect(clientProducer.isClosed()).andStubReturn(false);
      replay(clientProducer, clientSession, jbConn);

      JBossDestination destination = new JBossTopic(randomString());
      JBossMessageProducer producer = new JBossMessageProducer(jbConn, clientProducer,
            destination, clientSession);
      assertEquals(destination, producer.getTopic());

      verify(clientProducer, clientSession, jbConn);
   }

   public void testGetQueue() throws Exception
   {
      ClientProducer clientProducer = createStrictMock(ClientProducer.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      JBossConnection jbConn = createStrictMock(JBossConnection.class);
      EasyMock.expect(jbConn.getUID()).andReturn(randomSimpleString());
      EasyMock.expect(clientProducer.isClosed()).andStubReturn(false);
      replay(clientProducer, clientSession, jbConn);

      JBossDestination destination = new JBossQueue(randomString());
      JBossMessageProducer producer = new JBossMessageProducer(jbConn, clientProducer,
            destination, clientSession);
      assertEquals(destination, producer.getQueue());

      verify(clientProducer, clientSession, jbConn);
   }

   public void testSend() throws Exception
   {
      doProduce(new MessageProduction()
      {
         public void produce(JBossMessageProducer producer, Message message,
               Destination destination) throws Exception
         {
            producer.send(message);
         }
      });
   }

   public void testSendWithDestination() throws Exception
   {
      doProduceWithDestination(new JBossQueue(randomString()),
            new MessageProduction()
            {
               public void produce(JBossMessageProducer producer,
                     Message message, Destination destination) throws Exception
               {
                  producer.send(destination, message);
               }
            });
   }

   public void testSendWithQueue() throws Exception
   {
      doProduceWithDestination(new JBossQueue(randomString()),
            new MessageProduction()
            {
               public void produce(JBossMessageProducer producer,
                     Message message, Destination destination) throws Exception
               {
                  assertTrue(destination instanceof Queue);
                  producer.send((Queue)destination, message);
               }
            });
   }

   public void testPublish() throws Exception
   {
      doProduce(new MessageProduction()
      {
         public void produce(JBossMessageProducer producer, Message message,
               Destination destination) throws Exception
         {
            producer.publish(message);
         }
      });
   }

   public void testPublishWithDestination() throws Exception
   {
      doProduceWithDestination(new JBossTopic(randomString()), new MessageProduction()
      {
         public void produce(JBossMessageProducer producer, Message message,
               Destination destination) throws Exception
         {
            assertTrue(destination instanceof Topic);
            producer.publish((Topic) destination, message);
         }
      });
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void doProduce(MessageProduction production) throws Exception
   {
      JBossDestination destination = new JBossQueue(randomString());
      JBossDestination replyTo = new JBossQueue(randomString());
      ClientProducer clientProducer = createStrictMock(ClientProducer.class);
      JBossConnection jbConn = createStrictMock(JBossConnection.class);
      EasyMock.expect(jbConn.getUID()).andReturn(randomSimpleString());
      EasyMock.expect(jbConn.hasNoLocal()).andStubReturn(false);
      Message message = createStrictMock(Message.class);
      ClientSession session = EasyMock.createNiceMock(ClientSession.class);
      MessagingBuffer body = ChannelBuffers.wrappedBuffer(new byte[1024]);
      ClientMessage clientMessage = new ClientMessageImpl(JBossMessage.TYPE, true, 0, System.currentTimeMillis(), (byte)4, body);
      expect(session.createClientMessage(EasyMock.anyByte(), EasyMock.anyBoolean(), EasyMock.anyInt(), EasyMock.anyLong(), EasyMock.anyByte())).andReturn(clientMessage);
      expect(clientProducer.isClosed()).andStubReturn(false);
      message.setJMSDeliveryMode(anyInt());
      message.setJMSPriority(anyInt());
      message.setJMSExpiration(0);
      message.setJMSTimestamp(anyLong());
      expect(message.getJMSTimestamp()).andReturn(0L);
      expect(message.getJMSCorrelationIDAsBytes()).andReturn(randomBytes());
      expect(message.getJMSReplyTo()).andReturn(replyTo);
      expect(message.getJMSDestination()).andReturn(destination);
      expect(message.getJMSDeliveryMode()).andReturn(
            DeliveryMode.NON_PERSISTENT);
      expect(message.getJMSExpiration()).andReturn(0L);
      expect(message.getJMSPriority()).andReturn(4);
      expect(message.getJMSType()).andReturn(null);
      expect(message.getPropertyNames()).andReturn(
            (new Vector<String>()).elements());
      message.setJMSDestination(destination);
      message.setJMSMessageID(startsWith("ID:"));
      clientProducer.send((SimpleString) isNull(), isA(ClientMessage.class));
      replay(clientProducer, message, session, jbConn);

      JBossMessageProducer producer = new JBossMessageProducer(jbConn, clientProducer,
            destination, session);
      production.produce(producer, message, destination);

      verify(clientProducer, message, session, jbConn);
   }

   private void doProduceWithDestination(JBossDestination destination,
         MessageProduction production) throws Exception
   {
      JBossDestination replyTo = new JBossQueue(randomString());
      ClientProducer clientProducer = createStrictMock(ClientProducer.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      JBossConnection jbConn = createStrictMock(JBossConnection.class);
      EasyMock.expect(jbConn.getUID()).andReturn(randomSimpleString());
      EasyMock.expect(jbConn.hasNoLocal()).andStubReturn(false);
      MessagingBuffer body = ChannelBuffers.wrappedBuffer(new byte[1024]);
      ClientMessage clientMessage = new ClientMessageImpl(JBossMessage.TYPE, true, 0, System.currentTimeMillis(), (byte)4, body);
      expect(clientSession.createClientMessage(EasyMock.anyByte(), EasyMock.anyBoolean(), EasyMock.anyInt(), EasyMock.anyLong(), EasyMock.anyByte())).andReturn(clientMessage);
      Message message = createStrictMock(Message.class);
      expect(clientProducer.isClosed()).andStubReturn(false);
      message.setJMSDeliveryMode(anyInt());
      message.setJMSPriority(anyInt());
      message.setJMSExpiration(0);
      message.setJMSTimestamp(anyLong());
      expect(message.getJMSTimestamp()).andReturn(0L);
      expect(message.getJMSCorrelationIDAsBytes()).andReturn(randomBytes());
      expect(message.getJMSReplyTo()).andReturn(replyTo);
      expect(message.getJMSDestination()).andReturn(destination);
      expect(message.getJMSDeliveryMode()).andReturn(
            DeliveryMode.NON_PERSISTENT);
      expect(message.getJMSExpiration()).andReturn(0L);
      expect(message.getJMSPriority()).andReturn(4);
      expect(message.getJMSType()).andReturn(null);
      expect(message.getPropertyNames()).andReturn(
            (new Vector<String>()).elements());
      message.setJMSDestination(destination);
      message.setJMSMessageID(startsWith("ID:"));
      clientProducer.send(eq(destination.getSimpleAddress()), isA(ClientMessage.class));
      replay(clientProducer, message, clientSession, jbConn);

      JBossMessageProducer producer = new JBossMessageProducer(jbConn, clientProducer,
            destination, clientSession);
      production.produce(producer, message, destination);

      verify(clientProducer, message, clientSession, jbConn);
   }

   // Inner classes -------------------------------------------------

   private interface MessageProduction
   {
      void produce(JBossMessageProducer producer, Message message,
            Destination destination) throws Exception;
   }
}
