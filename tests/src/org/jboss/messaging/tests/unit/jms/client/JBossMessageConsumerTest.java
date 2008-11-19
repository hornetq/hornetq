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

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.easymock.classextension.EasyMock.createStrictMock;
import static org.easymock.classextension.EasyMock.replay;
import static org.easymock.classextension.EasyMock.reset;
import static org.easymock.classextension.EasyMock.verify;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;

import junit.framework.TestCase;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.MessageHandler;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.jms.JBossDestination;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.JBossTopic;
import org.jboss.messaging.jms.client.JBossMessage;
import org.jboss.messaging.jms.client.JBossMessageConsumer;
import org.jboss.messaging.jms.client.JBossSession;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class JBossMessageConsumerTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testClose() throws Exception
   {
      JBossSession session = createStrictMock(JBossSession.class);
      
      ClientConsumer clientConsumer = createStrictMock(ClientConsumer.class);
      
      expect(session.getAcknowledgeMode()).andStubReturn(Session.AUTO_ACKNOWLEDGE);
                                      
      replay(session, clientConsumer);
                  
      JBossDestination destination = new JBossQueue(randomString());
      
      JBossMessageConsumer consumer = new JBossMessageConsumer(session,
               clientConsumer, false, destination, null, null);
      
      verify(session, clientConsumer);
      
      reset(session, clientConsumer);
            
      clientConsumer.close();
      
      session.removeConsumer(consumer);

      replay(session, clientConsumer);

      consumer.close();

      verify(session, clientConsumer);
   }

   public void testCloseThrowsException() throws Exception
   {
      JBossSession session = createStrictMock(JBossSession.class);
      JBossDestination destination = new JBossQueue(randomString());
      expect(session.getAcknowledgeMode()).andReturn(Session.AUTO_ACKNOWLEDGE);
      ClientConsumer clientConsumer = createStrictMock(ClientConsumer.class);
      clientConsumer.close();
      expectLastCall().andThrow(new MessagingException());

      replay(session, clientConsumer);

      JBossMessageConsumer consumer = new JBossMessageConsumer(session,
            clientConsumer, false, destination, null, null);

      try
      {
         consumer.close();
         fail("JMSException");
      } catch (JMSException e)
      {
      }

      verify(session, clientConsumer);
   }

   public void testCheckClosed() throws Exception
   {
      JBossDestination destination = new JBossQueue(randomString());
      JBossSession session = createStrictMock(JBossSession.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      expect(session.getAcknowledgeMode()).andReturn(Session.AUTO_ACKNOWLEDGE);

      expect(clientSession.isClosed()).andReturn(true);

      expect(session.getCoreSession()).andReturn(clientSession);
      ClientConsumer clientConsumer = createStrictMock(ClientConsumer.class);

      replay(session, clientSession, clientConsumer);

      JBossMessageConsumer consumer = new JBossMessageConsumer(session,
            clientConsumer, false, destination, null, null);

      try
      {
         consumer.getMessageSelector();
         fail("IllegalStateException");
      } catch (IllegalStateException e)
      {
      }

      verify(session, clientSession, clientConsumer);
   }

   public void testGetMessageSelector() throws Exception
   {
      String messageSelector = "color = 'green'";
      JBossDestination destination = new JBossQueue(randomString());
      JBossSession session = createStrictMock(JBossSession.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      expect(session.getAcknowledgeMode()).andReturn(Session.AUTO_ACKNOWLEDGE);
      expect(clientSession.isClosed()).andReturn(false);
      expect(session.getCoreSession()).andReturn(clientSession);
      ClientConsumer clientConsumer = createStrictMock(ClientConsumer.class);

      replay(session, clientSession, clientConsumer);

      JBossMessageConsumer consumer = new JBossMessageConsumer(session,
            clientConsumer, false, destination, messageSelector, null);

      assertEquals(messageSelector, consumer.getMessageSelector());

      verify(session, clientSession, clientConsumer);
   }

   public void testGetNoLocal() throws Exception
   {
      boolean noLocal = true;
      JBossDestination destination = new JBossQueue(randomString());
      JBossSession session = createStrictMock(JBossSession.class);
      expect(session.getAcknowledgeMode()).andReturn(Session.AUTO_ACKNOWLEDGE);
      ClientConsumer clientConsumer = createStrictMock(ClientConsumer.class);

      replay(session, clientConsumer);

      JBossMessageConsumer consumer = new JBossMessageConsumer(session,
            clientConsumer, noLocal, destination, null, null);

      assertEquals(noLocal, consumer.getNoLocal());

      verify(session, clientConsumer);
   }

   public void testGetQueue() throws Exception
   {
      JBossDestination destination = new JBossQueue(randomString());
      JBossSession session = createStrictMock(JBossSession.class);
      expect(session.getAcknowledgeMode()).andReturn(Session.AUTO_ACKNOWLEDGE);
      ClientConsumer clientConsumer = createStrictMock(ClientConsumer.class);

      replay(session, clientConsumer);

      JBossMessageConsumer consumer = new JBossMessageConsumer(session,
            clientConsumer, false, destination, null, null);

      assertEquals(destination, consumer.getQueue());

      verify(session, clientConsumer);
   }

   public void testGetTopic() throws Exception
   {
      JBossDestination destination = new JBossTopic(randomString());
      JBossSession session = createStrictMock(JBossSession.class);
      expect(session.getAcknowledgeMode()).andReturn(Session.AUTO_ACKNOWLEDGE);
      ClientConsumer clientConsumer = createStrictMock(ClientConsumer.class);

      replay(session, clientConsumer);

      JBossMessageConsumer consumer = new JBossMessageConsumer(session,
            clientConsumer, false, destination, null, null);

      assertEquals(destination, consumer.getTopic());

      verify(session, clientConsumer);
   }

   public void testGetMessageListenerIsNull() throws Exception
   {
      JBossDestination destination = new JBossQueue(randomString());
      JBossSession session = createStrictMock(JBossSession.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      expect(session.getAcknowledgeMode()).andReturn(Session.AUTO_ACKNOWLEDGE);
      expect(clientSession.isClosed()).andReturn(false);
      expect(session.getCoreSession()).andReturn(clientSession);
      ClientConsumer clientConsumer = createStrictMock(ClientConsumer.class);

      replay(session, clientSession, clientConsumer);

      JBossMessageConsumer consumer = new JBossMessageConsumer(session,
            clientConsumer, false, destination, null, null);
      assertNull(consumer.getMessageListener());

      verify(session, clientSession, clientConsumer);
   }

   public void testSetMessageListener() throws Exception
   {
      JBossDestination destination = new JBossQueue(randomString());
      JBossSession session = createStrictMock(JBossSession.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      expect(session.getAcknowledgeMode()).andReturn(Session.AUTO_ACKNOWLEDGE);
      expect(clientSession.isClosed()).andReturn(false);
      expect(session.getCoreSession()).andReturn(clientSession);
      ClientConsumer clientConsumer = createStrictMock(ClientConsumer.class);
      clientConsumer.setMessageHandler(isA(MessageHandler.class));
      MessageListener listener = createStrictMock(MessageListener.class);

      replay(session, clientSession, clientConsumer, listener);

      JBossMessageConsumer consumer = new JBossMessageConsumer(session,
            clientConsumer, false, destination, null, null);
      consumer.setMessageListener(listener);
      assertEquals(listener, consumer.getMessageListener());

      verify(session, clientSession, clientConsumer, listener);
   }

   public void testSetMessageListenerThrowsException() throws Exception
   {
      JBossDestination destination = new JBossQueue(randomString());
      JBossSession session = createStrictMock(JBossSession.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      expect(session.getAcknowledgeMode()).andReturn(Session.AUTO_ACKNOWLEDGE);
      ClientConsumer clientConsumer = createStrictMock(ClientConsumer.class);
      clientConsumer.setMessageHandler(isA(MessageHandler.class));
      expectLastCall().andThrow(new MessagingException());
      MessageListener listener = createStrictMock(MessageListener.class);

      replay(session, clientSession, clientConsumer, listener);

      JBossMessageConsumer consumer = new JBossMessageConsumer(session,
            clientConsumer, false, destination, null, null);
      try
      {
         consumer.setMessageListener(listener);
         fail("JMSException");
      } catch (JMSException e)
      {
      }

      verify(session, clientSession, clientConsumer, listener);
   }

   public void testReceiveWithNoMessage() throws Exception
   {
      doReceiveWithNoMessage(0, new MessageReceiver()
      {
         public Message receive(MessageConsumer consumer) throws Exception
         {
            return consumer.receive();
         }
      });
   }

   public void testReceiveNoWaitWithNoMessage() throws Exception
   {
      doReceiveWithNoMessage(-1, new MessageReceiver()
      {
         public Message receive(MessageConsumer consumer) throws Exception
         {
            return consumer.receiveNoWait();
         }
      });
   }

   public void testReceiveWithTimeoutWithNoMessage() throws Exception
   {
      final long timeout = 1000;
      doReceiveWithNoMessage(timeout, new MessageReceiver()
      {
         public Message receive(MessageConsumer consumer) throws Exception
         {
            return consumer.receive(timeout);
         }
      });
   }

   public void testReceiveThrowsException() throws Exception
   {
      JBossDestination destination = new JBossQueue(randomString());
      JBossSession session = createStrictMock(JBossSession.class);
      expect(session.getAcknowledgeMode()).andReturn(Session.AUTO_ACKNOWLEDGE);
      ClientConsumer clientConsumer = createStrictMock(ClientConsumer.class);
      expect(clientConsumer.receive(0)).andThrow(new MessagingException());

      replay(session, clientConsumer);

      JBossMessageConsumer consumer = new JBossMessageConsumer(session,
            clientConsumer, false, destination, null, null);

      try
      {
         consumer.receive();
         fail("JMSException");
      } catch (JMSException e)
      {
      }

      verify(session, clientConsumer);
   }

   public void testReceive() throws Exception
   {
      JBossDestination destination = new JBossQueue(randomString());
      JBossSession session = createStrictMock(JBossSession.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      expect(session.getAcknowledgeMode()).andReturn(Session.AUTO_ACKNOWLEDGE);
      expect(session.getCoreSession()).andStubReturn(clientSession);
      ClientConsumer clientConsumer = createStrictMock(ClientConsumer.class);
      ClientMessage clientMessage = createStrictMock(ClientMessage.class);
      clientMessage.acknowledge();
      expect(clientMessage.getType()).andReturn(JBossMessage.TYPE);
      MessagingBuffer body = createStrictMock(MessagingBuffer.class);
      expect(clientMessage.getBody()).andStubReturn(body );
      expect(clientConsumer.receive(0)).andReturn(clientMessage );
      body.rewind();

      replay(session, clientSession, clientConsumer, clientMessage, body);

      JBossMessageConsumer consumer = new JBossMessageConsumer(session,
            clientConsumer, false, destination, null, null);

      Message message = consumer.receive();
      assertNotNull(message);
      
      verify(session, clientSession, clientConsumer, clientMessage, body);
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   public void doReceiveWithNoMessage(long expectedTimeout,
         MessageReceiver receiver) throws Exception
   {
      JBossDestination destination = new JBossQueue(randomString());
      JBossSession session = createStrictMock(JBossSession.class);
      expect(session.getAcknowledgeMode()).andReturn(Session.AUTO_ACKNOWLEDGE);
      ClientConsumer clientConsumer = createStrictMock(ClientConsumer.class);
      expect(clientConsumer.receive(expectedTimeout)).andReturn(null);

      replay(session, clientConsumer);

      JBossMessageConsumer consumer = new JBossMessageConsumer(session,
            clientConsumer, false, destination, null, null);

      Message message = receiver.receive(consumer);
      assertNull(message);

      verify(session, clientConsumer);
   }

   // Inner classes -------------------------------------------------

   private interface MessageReceiver
   {
      Message receive(MessageConsumer consumer) throws Exception;
   }
}
