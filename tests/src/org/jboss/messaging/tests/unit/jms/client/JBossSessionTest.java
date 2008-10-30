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

package org.jboss.messaging.tests.unit.jms.client;

import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.isNull;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.jboss.messaging.tests.util.RandomUtil.randomSimpleString;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.InvalidClientIDException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TransactionInProgressException;
import javax.transaction.xa.XAResource;

import junit.framework.TestCase;

import org.easymock.EasyMock;
import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientMessageImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.impl.ByteBufferWrapper;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryResponseMessage;
import org.jboss.messaging.jms.JBossDestination;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.JBossTemporaryQueue;
import org.jboss.messaging.jms.JBossTemporaryTopic;
import org.jboss.messaging.jms.JBossTopic;
import org.jboss.messaging.jms.client.JBossConnection;
import org.jboss.messaging.jms.client.JBossMessage;
import org.jboss.messaging.jms.client.JBossSession;
import org.jboss.messaging.util.SimpleString;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class JBossSessionTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   private ClientSessionFactory sf;
   private ClientSession mockClientSession;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      sf = createStrictMock(ClientSessionFactory.class);      
      mockClientSession = createStrictMock(ClientSession.class);
   }

   @Override
   protected void tearDown() throws Exception
   {
      verify(sf, mockClientSession);

      super.tearDown();
   }

   public void testClose() throws Exception
   {
      mockClientSession.close();

      replay(sf, mockClientSession);
      
      JBossConnection connection = new JBossConnection(null, null, JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      Session session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      session.close();
   }
      
   public void testCloseThrowsException() throws Exception
   {
      mockClientSession.close();
      expectLastCall().andThrow(new MessagingException());
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null, JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      Session session = new JBossSession(connection, true, false,
            Session.DUPS_OK_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      try
      {
         session.close();
         fail("should throw a JMSException");
      } catch (JMSException e)
      {
      }
   }

   public void testClosedSession() throws Exception
   {
      mockClientSession.close();
      expect(mockClientSession.isClosed()).andReturn(true);

      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null, JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      Session session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      session.close();

      try
      {
         session.getTransacted();
         fail("once a session is closed, this must throw a IllegalStateException");
      } catch (IllegalStateException e)
      {
      }
   }

   public void testGetTransacted() throws Exception
   {
      expect(mockClientSession.isClosed()).andReturn(false);

      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
               JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      
      Session session = new JBossSession(connection, true, false, 0,
            mockClientSession, JBossSession.TYPE_GENERIC_SESSION);

      assertEquals(true, session.getTransacted());
   }

   public void testGetAcknowledgeMode() throws Exception
   {
      expect(mockClientSession.isClosed()).andReturn(false);

      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      Session session = new JBossSession(connection, false, false,
            Session.DUPS_OK_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      assertEquals(Session.DUPS_OK_ACKNOWLEDGE, session.getAcknowledgeMode());
   }

   public void testCommitOnTransactedSession() throws Exception
   {
      mockClientSession.commit();

      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      Session session = new JBossSession(connection, true, false,
            Session.DUPS_OK_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      session.commit();
   }

   public void testCommitThrowsException() throws Exception
   {
      mockClientSession.commit();
      expectLastCall().andThrow(new MessagingException());
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      Session session = new JBossSession(connection, true, false,
            Session.DUPS_OK_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      try
      {
         session.commit();
         fail("should throw a JMSException");
      } catch (JMSException e)
      {
      }
   }

   public void testCommitOnNonTransactedSession() throws Exception
   {
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      Session session = new JBossSession(connection, false, false,
            Session.DUPS_OK_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      try
      {
         session.commit();
         fail("commit() is not allowed on a non-transacted session");
      } catch (IllegalStateException e)
      {
      }
   }

   public void testCommitOnXASession() throws Exception
   {
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      Session session = new JBossSession(connection, true, true, 0,
            mockClientSession, JBossSession.TYPE_GENERIC_SESSION);

      try
      {
         session.commit();
         fail("commit() is not allowed on a XA session");
      } catch (TransactionInProgressException e)
      {
      }
   }

   public void testRollbackOnTransactedSession() throws Exception
   {
      mockClientSession.rollback();

      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      Session session = new JBossSession(connection, true, false,
            Session.DUPS_OK_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      session.rollback();
   }

   public void testRollbackThrowsException() throws Exception
   {
      mockClientSession.rollback();
      expectLastCall().andThrow(new MessagingException());
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      Session session = new JBossSession(connection, true, false,
            Session.DUPS_OK_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      try
      {
         session.rollback();
         fail("should throw a JMSException");
      } catch (JMSException e)
      {
      }
   }

   public void testRollbackOnNonTransactedSession() throws Exception
   {
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      Session session = new JBossSession(connection, false, false,
            Session.DUPS_OK_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      try
      {
         session.rollback();
         fail("rollback() is not allowed on a non-transacted session");
      } catch (IllegalStateException e)
      {
      }
   }

   public void testRollbackOnXASession() throws Exception
   {
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      Session session = new JBossSession(connection, true, true, 0,
            mockClientSession, JBossSession.TYPE_GENERIC_SESSION);

      try
      {
         session.rollback();
         fail("rollback() is not allowed on a XA session");
      } catch (TransactionInProgressException e)
      {
      }
   }

   public void testRecoverOnTransactedSession() throws Exception
   {
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      Session session = new JBossSession(connection, true, false,
            Session.DUPS_OK_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      try
      {
         session.recover();
         fail("recover() is not allowed on a non-transacted session");
      } catch (IllegalStateException e)
      {
      }
   }

   public void testRecoverOnNonTransactedSession() throws Exception
   {
      mockClientSession.rollback();
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      Session session = new JBossSession(connection, false, false,
            Session.DUPS_OK_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      session.recover();
   }

   public void testRecoverThrowsException() throws Exception
   {
      mockClientSession.rollback();
      expectLastCall().andThrow(new MessagingException());
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      Session session = new JBossSession(connection, false, false,
            Session.DUPS_OK_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      try
      {
         session.recover();
         fail("should throw a JMSException");
      } catch (JMSException e)
      {
      }
   }

   public void testMessageListener() throws Exception
   {
      expect(mockClientSession.isClosed()).andStubReturn(false);
      MessageListener listener = createStrictMock(MessageListener.class);
      replay(sf, mockClientSession, listener);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      Session session = new JBossSession(connection, false, false,
            Session.DUPS_OK_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_GENERIC_SESSION);
      assertNull(session.getMessageListener());
      session.setMessageListener(listener);
      //Note we don't implement ASF so always return null
      assertNull(session.getMessageListener());

      verify(listener);
   }

   public void testCreateMessage() throws Exception
   {
      doTestCreateMessage(new MessageCreation()
      {
         public Message createMessage(Session session) throws JMSException
         {
            Message message = session.createMessage();
            return message;
         }
      });

   }

   public void testCreateMapMessage() throws Exception
   {
      doTestCreateMessage(new MessageCreation()
      {
         public Message createMessage(Session session) throws JMSException
         {
            MapMessage message = session.createMapMessage();
            return message;
         }
      });

   }

   public void testCreateBytesMessage() throws Exception
   {
      doTestCreateMessage(new MessageCreation()
      {
         public Message createMessage(Session session) throws JMSException
         {
            BytesMessage message = session.createBytesMessage();
            return message;
         }
      });

   }

   public void testCreateTextMessage() throws Exception
   {
      doTestCreateMessage(new MessageCreation()
      {
         public Message createMessage(Session session) throws JMSException
         {
            TextMessage message = session.createTextMessage();
            return message;
         }
      });

   }

   public void testCreateTextMessageWithString() throws Exception
   {
      doTestCreateMessage(new MessageCreation()
      {
         public Message createMessage(Session session) throws JMSException
         {
            TextMessage message = session.createTextMessage(randomString());
            return message;
         }
      });

   }

   public void testCreateObjectMessage() throws Exception
   {
      doTestCreateMessage(new MessageCreation()
      {
         public Message createMessage(Session session) throws JMSException
         {
            ObjectMessage message = session.createObjectMessage();
            return message;
         }
      });

   }

   public void testCreateObjectMessageWithSerializable() throws Exception
   {
      doTestCreateMessage(new MessageCreation()
      {
         public Message createMessage(Session session) throws JMSException
         {
            ObjectMessage message = session.createObjectMessage(randomString());
            return message;
         }
      });

   }

   public void testCreateStreamMessage() throws Exception
   {
      doTestCreateMessage(new MessageCreation()
      {
         public Message createMessage(Session session) throws JMSException
         {
            StreamMessage message = session.createStreamMessage();
            return message;
         }
      });
   }

   public void testCreateProducer() throws Exception
   {
      JBossDestination destination = new JBossQueue(randomString());
      ClientProducer clientProducer = createStrictMock(ClientProducer.class);
      expect(mockClientSession.createProducer(destination.getSimpleAddress()))
            .andReturn(clientProducer);

      replay(sf, mockClientSession, clientProducer);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      Session session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      MessageProducer producer = session.createProducer(destination);
      assertNotNull(producer);

      verify(clientProducer);
   }

   public void testCreateProducerThrowsException() throws Exception
   {
      JBossDestination destination = new JBossQueue(randomString());
      ClientProducer clientProducer = createStrictMock(ClientProducer.class);
      expect(mockClientSession.createProducer(destination.getSimpleAddress()))
            .andThrow(new MessagingException());

      replay(sf, mockClientSession, clientProducer);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      Session session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      try
      {
         session.createProducer(destination);
         fail("should throw a JMSException");
      } catch (JMSException e)
      {
      }

      verify(clientProducer);
   }

   public void testCreateProducerWithInvalidDestination() throws Exception
   {
      Destination destination = createStrictMock(Destination.class);

      replay(sf, mockClientSession, destination);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      Session session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      try
      {
         session.createProducer(destination);
         fail("only instances of JBossDestination are allowed as destination");
      } catch (InvalidDestinationException e)
      {
      }

      verify(destination);
   }

   public void testCreateProducerWithNullDestination() throws Exception
   {
      ClientProducer clientProducer = createStrictMock(ClientProducer.class);
      expect(mockClientSession.createProducer(null)).andReturn(clientProducer);

      replay(sf, mockClientSession, clientProducer);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      Session session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      session.createProducer(null);

      verify(clientProducer);
   }

   public void testCreatePublisher() throws Exception
   {
      JBossTopic topic = new JBossTopic(randomString());
      ClientProducer clientProducer = createStrictMock(ClientProducer.class);
      expect(mockClientSession.createProducer(topic.getSimpleAddress()))
            .andReturn(clientProducer);

      replay(sf, mockClientSession, clientProducer);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_TOPIC_CONNECTION, null, -1, sf);
      TopicSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_TOPIC_SESSION);

      TopicPublisher publisher = session.createPublisher(topic);
      assertNotNull(publisher);

      verify(clientProducer);
   }

   public void testCreateSender() throws Exception
   {
      JBossQueue queue = new JBossQueue(randomString());
      ClientProducer clientProducer = createStrictMock(ClientProducer.class);
      expect(mockClientSession.createProducer(queue.getSimpleAddress()))
            .andReturn(clientProducer);

      replay(sf, mockClientSession, clientProducer);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_QUEUE_SESSION);

      QueueSender sender = session.createSender(queue);
      assertNotNull(sender);

      verify(clientProducer);
   }

   public void testCreateConsumer() throws Exception
   {
      JBossDestination destination = new JBossQueue(randomString());
      ClientConsumer clientConsumer = createStrictMock(ClientConsumer.class);

      // isExists() will return true
      SessionQueueQueryResponseMessage resp = new SessionQueueQueryResponseMessage(
            false, 0, 1, null, destination.getSimpleAddress());
      expect(mockClientSession.queueQuery(destination.getSimpleAddress()))
            .andReturn(resp);
      expect(
            mockClientSession.createConsumer(destination.getSimpleAddress(),
                  null, false)).andReturn(clientConsumer);
      expect(mockClientSession.isClosed()).andReturn(false);

      replay(sf, mockClientSession, clientConsumer);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      Session session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      MessageConsumer consumer = session.createConsumer(destination);
      assertNotNull(consumer);

      verify(clientConsumer);
   }

   public void testCreateConsumerWithMessageSelector() throws Exception
   {
      String selector = "color = 'red";
      doTestCreateConsumerWithSelector(selector, new SimpleString(selector));
   }

   public void testCreateConsumerWithEmptyMessageSelector() throws Exception
   {
      doTestCreateConsumerWithSelector("", null);
   }

   public void testCreateConsumerThrowsException() throws Exception
   {
      JBossDestination destination = new JBossQueue(randomString());
      expect(mockClientSession.queueQuery(destination.getSimpleAddress())).andThrow(new MessagingException());
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      Session session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      try
      {
         session.createConsumer(destination);
         fail("must throw an JMSException");
      } catch (JMSException e)
      {
      }
   }
   
   public void testCreateConsumerWithNullDestination() throws Exception
   {
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      Session session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      try
      {
         session.createConsumer(null);
         fail("must throw an InvalidDestinationException");
      } catch (InvalidDestinationException e)
      {
      }
   }

   public void testCreateConsumerWithInvalidDestination() throws Exception
   {
      Destination invalidDestination = createStrictMock(Destination.class);
      replay(sf, mockClientSession, invalidDestination);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      Session session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      try
      {
         session.createConsumer(invalidDestination);
         fail("only instances of JBossDestination are allowed");
      } catch (InvalidDestinationException e)
      {
      }

      verify(invalidDestination);
   }

   public void testCreateConsumerWithUnknownQueue() throws Exception
   {
      JBossDestination destination = new JBossQueue(randomString());

      // isExists() will return false
      SessionQueueQueryResponseMessage resp = new SessionQueueQueryResponseMessage();
      expect(mockClientSession.queueQuery(destination.getSimpleAddress()))
            .andReturn(resp);

      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      Session session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      try
      {
         session.createConsumer(destination);
         fail("should throw an InvalidDestinationException");
      } catch (InvalidDestinationException e)
      {
      }
   }

   public void testCreateConsumerWithUnknownTopic() throws Exception
   {
      JBossDestination destination = new JBossTopic(randomString());

      // isExists() will return false
      SessionBindingQueryResponseMessage resp = new SessionBindingQueryResponseMessage();
      expect(mockClientSession.bindingQuery(destination.getSimpleAddress()))
      .andReturn(resp);

      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_TOPIC_CONNECTION, null, -1, sf);
      Session session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_TOPIC_SESSION);

      try
      {
         session.createConsumer(destination);
         fail("should throw an InvalidDestinationException");
      } catch (InvalidDestinationException e)
      {
      }
   }
   
   public void testCreateConsumerForTopic() throws Exception
   {
      JBossDestination destination = new JBossTopic(randomString());
      ClientConsumer clientConsumer = createStrictMock(ClientConsumer.class);

      // isExists() will return true
      SessionBindingQueryResponseMessage resp = new SessionBindingQueryResponseMessage(
            true, new ArrayList<SimpleString>());
      expect(mockClientSession.bindingQuery(destination.getSimpleAddress()))
            .andReturn(resp);
      mockClientSession.createQueue(eq(destination.getSimpleAddress()),
            isA(SimpleString.class), (SimpleString) isNull(), eq(false),
            eq(true));
      expect(
            mockClientSession.createConsumer(isA(SimpleString.class),
                  (SimpleString) isNull(), eq(false)))
            .andReturn(clientConsumer);
      expect(mockClientSession.isClosed()).andReturn(false);

      replay(sf, mockClientSession, clientConsumer);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_TOPIC_CONNECTION, null, -1, sf);
      Session session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_TOPIC_SESSION);

      MessageConsumer consumer = session.createConsumer(destination);
      assertNotNull(consumer);

      verify(clientConsumer);
   }


   public void testCreateDurableSubscriberFromQueueSession() throws Exception
   {
      JBossTopic topic = new JBossTopic(randomString());
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_TOPIC_CONNECTION, null, -1, sf);
      Session session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_QUEUE_SESSION);

      try
      {
         session.createDurableSubscriber(topic, randomString());
         fail("cannot create a durable subscriber on a QueueSession");
      } catch (IllegalStateException e)
      {
      }
   }
   
   public void testCreateDurableSubscriberForNullTopic() throws Exception
   {
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_TOPIC_CONNECTION, null, -1, sf);
      Session session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_TOPIC_SESSION);

      try
      {
         session.createDurableSubscriber(null, randomString());
         fail("cannot create a durable subscriber on a null topict");
      } catch (InvalidDestinationException e)
      {
      }
   }
   
   public void testCreateDurableSubscriberForInvalidTopic() throws Exception
   {
      Topic invalidTopic = createStrictMock(Topic.class);
      replay(sf, mockClientSession, invalidTopic);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_TOPIC_CONNECTION, null, -1, sf);
      Session session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_TOPIC_SESSION);

      try
      {
         session.createDurableSubscriber(invalidTopic, randomString());
         fail("only instances of JBossTopic are allowed");
      } catch (InvalidDestinationException e)
      {
      }
      
      verify(invalidTopic);
   }
   
   public void testCreateDurableSubscriber() throws Exception
   {
      String subscriptionName = randomString();
      String clientID = randomString();
      JBossTopic topic = new JBossTopic(randomString());
      ClientConsumer clientConsumer = createStrictMock(ClientConsumer.class);
    
      expect(mockClientSession.isClosed()).andStubReturn(false);

      // isExists() will return true
      SessionBindingQueryResponseMessage bindingResp = new SessionBindingQueryResponseMessage(
            true, new ArrayList<SimpleString>());
      expect(mockClientSession.bindingQuery(topic.getSimpleAddress()))
            .andReturn(bindingResp);
      SessionQueueQueryResponseMessage queryResp = new SessionQueueQueryResponseMessage();
      expect(mockClientSession.queueQuery(isA(SimpleString.class))).andReturn(queryResp);
      mockClientSession.createQueue(eq(topic.getSimpleAddress()),
            isA(SimpleString.class), (SimpleString) isNull(), eq(true),
            eq(false));
      expect(
            mockClientSession.createConsumer(isA(SimpleString.class),
                  (SimpleString) isNull(), eq(false)))
            .andReturn(clientConsumer);

      replay(sf, mockClientSession, clientConsumer);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_TOPIC_CONNECTION, clientID, -1, sf);
      Session session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_TOPIC_SESSION);

      MessageConsumer consumer = session.createDurableSubscriber(topic, subscriptionName);
      assertNotNull(consumer);

      verify(clientConsumer);
   }
   
   public void testCreateDurableSubscriberWithNullClientID() throws Exception
   {
      String subscriptionName = randomString();
      String clientID = null;
      JBossTopic topic = new JBossTopic(randomString());

      
      expect(mockClientSession.isClosed()).andStubReturn(false);

      // isExists() will return true
      SessionBindingQueryResponseMessage bindingResp = new SessionBindingQueryResponseMessage(
            true, new ArrayList<SimpleString>());
      expect(mockClientSession.bindingQuery(topic.getSimpleAddress()))
            .andReturn(bindingResp);
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_TOPIC_CONNECTION, clientID, -1, sf);
      Session session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_TOPIC_SESSION);

      try {
         session.createDurableSubscriber(topic, subscriptionName);
         fail("clientID must be set to create a durable subscriber");
      } catch (InvalidClientIDException e)
      {
      }
   }
   
   public void testCreateDurableSubscriberWithTemporaryTopic() throws Exception
   {
      String topicName = randomString();
      SimpleString topicAddress = new SimpleString(
            JBossTemporaryTopic.JMS_TEMP_TOPIC_ADDRESS_PREFIX + topicName);
      
      String subscriptionName = randomString();
      String clientID = randomString();
      
      expect(mockClientSession.isClosed()).andStubReturn(false);

      // isExists() will return true
      SessionBindingQueryResponseMessage bindingResp = new SessionBindingQueryResponseMessage(
            true, new ArrayList<SimpleString>());
      expect(mockClientSession.bindingQuery(topicAddress))
            .andReturn(bindingResp);
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_TOPIC_CONNECTION, clientID, -1, sf);
      JBossSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_TOPIC_SESSION);
      JBossTopic tempTopic = new JBossTemporaryTopic(session, topicName);


      try {
         session.createDurableSubscriber(tempTopic, subscriptionName);
         fail("can not create a durable subscriber for a temporary topic");
      } catch (InvalidDestinationException e)
      {
      }
   }
   
   public void testCreateDurableSubscriberWithAlreadyReigsteredSubscriber() throws Exception
   {
      String subscriptionName = randomString();
      String clientID = randomString();
      JBossTopic topic = new JBossTopic(randomString());

      
      expect(mockClientSession.isClosed()).andStubReturn(false);

      // isExists() will return true
      SessionBindingQueryResponseMessage bindingResp = new SessionBindingQueryResponseMessage(
            true, new ArrayList<SimpleString>());
      expect(mockClientSession.bindingQuery(topic.getSimpleAddress()))
            .andReturn(bindingResp);
      // already 1 durable subscriber
      SessionQueueQueryResponseMessage queryResp =
         new SessionQueueQueryResponseMessage(true, 1, 0, null, topic.getSimpleAddress());
      expect(mockClientSession.queueQuery(isA(SimpleString.class))).andReturn(queryResp);

      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_TOPIC_CONNECTION, clientID, -1, sf);
      Session session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_TOPIC_SESSION);

      try
      {
         session.createDurableSubscriber(topic, subscriptionName);         
         fail("can not create a durable subscriber when another is already registered");
      } catch (IllegalStateException e)
      {
      }
   }
   
   public void testCreateDurableSubscriberWithEmptyMessageSelector() throws Exception
   {
      String subscriptionName = randomString();
      String clientID = randomString();
      String selector = "";
      JBossTopic topic = new JBossTopic(randomString());
      ClientConsumer clientConsumer = createStrictMock(ClientConsumer.class);

      
      expect(mockClientSession.isClosed()).andStubReturn(false);

      // isExists() will return true
      SessionBindingQueryResponseMessage bindingResp = new SessionBindingQueryResponseMessage(
            true, new ArrayList<SimpleString>());
      expect(mockClientSession.bindingQuery(topic.getSimpleAddress()))
            .andReturn(bindingResp);
      SessionQueueQueryResponseMessage queryResp = new SessionQueueQueryResponseMessage();
      expect(mockClientSession.queueQuery(isA(SimpleString.class))).andReturn(queryResp);
      mockClientSession.createQueue(eq(topic.getSimpleAddress()),
            isA(SimpleString.class), (SimpleString) isNull(), eq(true),
            eq(false));
      expect(
            mockClientSession.createConsumer(isA(SimpleString.class),
                  (SimpleString) isNull(), eq(false)))
            .andReturn(clientConsumer);

      replay(sf, mockClientSession, clientConsumer);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_TOPIC_CONNECTION, clientID, -1, sf);
      Session session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_TOPIC_SESSION);

      MessageConsumer consumer = session.createDurableSubscriber(topic, subscriptionName, selector, false);
      assertNotNull(consumer);

      verify(clientConsumer);
   }
   
   public void testCreateDurableSubscriberWhichWasAlreadyRegistered() throws Exception
   {
      String subscriptionName = randomString();
      String clientID = randomString();
      JBossTopic topic = new JBossTopic(randomString());
      ClientConsumer clientConsumer = createStrictMock(ClientConsumer.class);

      
      expect(mockClientSession.isClosed()).andStubReturn(false);

      // isExists() will return true
      SessionBindingQueryResponseMessage bindingResp = new SessionBindingQueryResponseMessage(
            true, new ArrayList<SimpleString>());
      expect(mockClientSession.bindingQuery(topic.getSimpleAddress()))
            .andReturn(bindingResp);
      // isExists will return true
      SessionQueueQueryResponseMessage queryResp =
         new SessionQueueQueryResponseMessage(true, 0, 0, null, topic.getSimpleAddress());
      expect(mockClientSession.queueQuery(isA(SimpleString.class))).andReturn(queryResp);
      expect(
            mockClientSession.createConsumer(isA(SimpleString.class),
                  (SimpleString) isNull(), eq(false)))
            .andReturn(clientConsumer);

      replay(sf, mockClientSession, clientConsumer);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_TOPIC_CONNECTION, clientID, -1, sf);
      Session session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_TOPIC_SESSION);

      MessageConsumer consumer = session.createDurableSubscriber(topic, subscriptionName);
      assertNotNull(consumer);

      verify(clientConsumer);
   }
   
   public void testCreateDurableSubscriberWhichWasAlreadyRegisteredWithAnotherTopic() throws Exception
   {
      String subscriptionName = randomString();
      String clientID = randomString();
      JBossTopic oldTopic = new JBossTopic(randomString());
      JBossTopic newTopic = new JBossTopic(randomString());
      ClientConsumer clientConsumer = createStrictMock(ClientConsumer.class);

      
      expect(mockClientSession.isClosed()).andStubReturn(false);

      // isExists() will return true
      SessionBindingQueryResponseMessage bindingResp = new SessionBindingQueryResponseMessage(
            true, new ArrayList<SimpleString>());
      expect(mockClientSession.bindingQuery(newTopic.getSimpleAddress()))
            .andReturn(bindingResp);
      // isExists will return true
      SessionQueueQueryResponseMessage queryResp =
         new SessionQueueQueryResponseMessage(true, 0, 0, null, oldTopic.getSimpleAddress());
      expect(mockClientSession.queueQuery(isA(SimpleString.class))).andReturn(queryResp);
      // queue address of the old topic
      mockClientSession.deleteQueue(isA(SimpleString.class));
      mockClientSession.createQueue(eq(newTopic.getSimpleAddress()), isA(SimpleString.class), (SimpleString) isNull(), eq(true), eq(false));
      expect(
            mockClientSession.createConsumer(isA(SimpleString.class),
                  (SimpleString) isNull(), eq(false)))
            .andReturn(clientConsumer);

      replay(sf, mockClientSession, clientConsumer);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_TOPIC_CONNECTION, clientID, -1, sf);
      Session session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_TOPIC_SESSION);

      MessageConsumer consumer = session.createDurableSubscriber(newTopic, subscriptionName);
      assertNotNull(consumer);

      verify(clientConsumer);
   }
   
   public void testCreateDurableSubscriberWhichWasAlreadyRegisteredWithAnotherMessageSelector() throws Exception
   {
      String subscriptionName = randomString();
      String clientID = randomString();
      JBossTopic topic = new JBossTopic(randomString());
      SimpleString oldSelector = new SimpleString("color = 'red'");
      SimpleString newSelector = new SimpleString("color = 'blue'");
      ClientConsumer clientConsumer = createStrictMock(ClientConsumer.class);

      
      expect(mockClientSession.isClosed()).andStubReturn(false);

      // isExists() will return true
      SessionBindingQueryResponseMessage bindingResp = new SessionBindingQueryResponseMessage(
            true, new ArrayList<SimpleString>());
      expect(mockClientSession.bindingQuery(topic.getSimpleAddress()))
            .andReturn(bindingResp);
      // isExists will return true
      SessionQueueQueryResponseMessage queryResp =
         new SessionQueueQueryResponseMessage(true, 0, 0, oldSelector, topic.getSimpleAddress());
      expect(mockClientSession.queueQuery(isA(SimpleString.class))).andReturn(queryResp);
      // queue address of the old topic
      mockClientSession.deleteQueue(isA(SimpleString.class));
      mockClientSession.createQueue(eq(topic.getSimpleAddress()), isA(SimpleString.class), eq(newSelector), eq(true), eq(false));
      expect(
            mockClientSession.createConsumer(isA(SimpleString.class),
                  (SimpleString) isNull(), eq(false)))
            .andReturn(clientConsumer);

      replay(sf, mockClientSession, clientConsumer);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_TOPIC_CONNECTION, clientID, -1, sf);
      Session session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_TOPIC_SESSION);

      MessageConsumer consumer = session.createDurableSubscriber(topic, subscriptionName, newSelector.toString(), false);
      assertNotNull(consumer);

      verify(clientConsumer);
   }


   /*public void testCreateBrowser() throws Exception
   {
      JBossQueue queue = new JBossQueue(randomString());
      ClientBrowser clientBrowser = createStrictMock(ClientBrowser.class);
      expect(mockClientSession.createBrowser(queue.getSimpleAddress(), null))
            .andReturn(clientBrowser);

      replay(sf, mockClientSession, clientBrowser);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_QUEUE_SESSION);

      QueueBrowser browser = session.createBrowser(queue);
      assertNotNull(browser);

      verify(clientBrowser);
   }

   public void testCreateBrowserThrowsException() throws Exception
   {
      JBossQueue queue = new JBossQueue(randomString());
      ClientBrowser clientBrowser = createStrictMock(ClientBrowser.class);
      expect(mockClientSession.createBrowser(queue.getSimpleAddress(), null))
            .andThrow(new MessagingException());

      replay(sf, mockClientSession, clientBrowser);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_QUEUE_SESSION);

      try
      {
         session.createBrowser(queue);
         fail("should throw a JMSException");
      } catch (JMSException e)
      {
      }

      verify(clientBrowser);
   }

   public void testCreateBrowserWithEmptyFilter() throws Exception
   {
      JBossQueue queue = new JBossQueue(randomString());
      ClientBrowser clientBrowser = createStrictMock(ClientBrowser.class);
      expect(mockClientSession.createBrowser(queue.getSimpleAddress(), null))
            .andReturn(clientBrowser);

      replay(sf, mockClientSession, clientBrowser);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_QUEUE_SESSION);

      QueueBrowser browser = session.createBrowser(queue, "");
      assertNotNull(browser);

      verify(clientBrowser);
   }

   public void testCreateBrowserWithFilter() throws Exception
   {
      String filter = "color = 'red'";
      JBossQueue queue = new JBossQueue(randomString());
      ClientBrowser clientBrowser = createStrictMock(ClientBrowser.class);
      expect(
            mockClientSession.createBrowser(queue.getSimpleAddress(),
                  new SimpleString(filter))).andReturn(clientBrowser);

      replay(sf, mockClientSession, clientBrowser);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_QUEUE_SESSION);

      QueueBrowser browser = session.createBrowser(queue, filter);
      assertNotNull(browser);

      verify(clientBrowser);
   }*/

   public void testCreateBrowserFromTopicSession() throws Exception
   {
      Queue queue = new JBossQueue(randomString());

      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_TOPIC_SESSION);

      try
      {
         session.createBrowser(queue);
         fail("browser can not be created from topic session");
      } catch (IllegalStateException e)
      {
      }
   }

   public void testCreateBrowserForNullQueue() throws Exception
   {
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_QUEUE_SESSION);

      try
      {
         session.createBrowser(null);
         fail("browser can not be created for a null destination");
      } catch (InvalidDestinationException e)
      {
      }
   }

   public void testCreateBrowserForInvalidQueue() throws Exception
   {
      Queue queue = createStrictMock(Queue.class);
      replay(sf, mockClientSession, queue);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_QUEUE_SESSION);

      try
      {
         session.createBrowser(queue);
         fail("browser can not be created for queues which are not instances of JBossQueue");
      } catch (InvalidDestinationException e)
      {
      }

      verify(queue);
   }

   public void testCreateQueue() throws Exception
   {
      String queueName = randomString();
      SimpleString queueAddress = new SimpleString(
            JBossQueue.JMS_QUEUE_ADDRESS_PREFIX + queueName);

      // isExists() will return true
      SessionQueueQueryResponseMessage resp = new SessionQueueQueryResponseMessage(
            false, -1, 1, null, queueAddress);
      expect(mockClientSession.queueQuery(queueAddress)).andReturn(resp);

      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_QUEUE_SESSION);

      Queue queue = session.createQueue(queueName);
      assertNotNull(queue);
   }

   public void testCreateQueueThrowsException() throws Exception
   {
      String queueName = randomString();
      SimpleString queueAddress = new SimpleString(
            JBossQueue.JMS_QUEUE_ADDRESS_PREFIX + queueName);

      expect(mockClientSession.queueQuery(queueAddress)).andThrow(
            new MessagingException());

      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_QUEUE_SESSION);

      try
      {
         session.createQueue(queueName);
         fail("should throw a JMSException");
      } catch (JMSException e)
      {
      }
   }

   public void testCreateQueueWithUnknownName() throws Exception
   {
      String queueName = randomString();
      SimpleString queueAddress = new SimpleString(
            JBossQueue.JMS_QUEUE_ADDRESS_PREFIX + queueName);

      // isExists() will return false
      SessionQueueQueryResponseMessage resp = new SessionQueueQueryResponseMessage();
      expect(mockClientSession.queueQuery(queueAddress)).andReturn(resp);

      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_QUEUE_SESSION);

      try
      {
         session.createQueue(queueName);
         fail("creating a queue with an unknown name must throw a JMSException");
      } catch (JMSException e)
      {
      }
   }

   public void testCreateQueueFromTopicSession() throws Exception
   {
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_TOPIC_SESSION);

      try
      {
         session.createQueue(randomString());
         fail("creating a queue from a topic session must throw a IllegalStateException");
      } catch (IllegalStateException e)
      {
      }
   }

   public void testCreateTopic() throws Exception
   {
      String topicName = randomString();
      SimpleString topicAddress = new SimpleString(
            JBossTopic.JMS_TOPIC_ADDRESS_PREFIX + topicName);

      SessionBindingQueryResponseMessage resp = new SessionBindingQueryResponseMessage(
            true, new ArrayList<SimpleString>());
      expect(mockClientSession.bindingQuery(topicAddress)).andReturn(resp);

      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_TOPIC_SESSION);

      Topic topic = session.createTopic(topicName);
      assertNotNull(topic);
   }

   public void testCreateTopicThrowsException() throws Exception
   {
      String topicName = randomString();
      SimpleString topicAddress = new SimpleString(
            JBossTopic.JMS_TOPIC_ADDRESS_PREFIX + topicName);

      expect(mockClientSession.bindingQuery(topicAddress)).andThrow(
            new MessagingException());

      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_TOPIC_SESSION);

      try
      {
         session.createTopic(topicName);
         fail("should throw a JMSException");
      } catch (JMSException e)
      {
      }
   }

   public void testCreateTopicWithUnknownName() throws Exception
   {
      String topicName = randomString();
      SimpleString topicAddress = new SimpleString(
            JBossTopic.JMS_TOPIC_ADDRESS_PREFIX + topicName);

      SessionBindingQueryResponseMessage resp = new SessionBindingQueryResponseMessage(
            false, new ArrayList<SimpleString>());
      expect(mockClientSession.bindingQuery(topicAddress)).andReturn(resp);

      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_TOPIC_SESSION);

      try
      {
         session.createTopic(topicName);
         fail("creating a topic with an unknown name must throw a JMSException");
      } catch (JMSException e)
      {
      }
   }

   public void testCreateTopicFromQueueSession() throws Exception
   {
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_QUEUE_SESSION);

      try
      {
         session.createTopic(randomString());
         fail("creating a topic from a queue session must throw a IllegalStateException");
      } catch (IllegalStateException e)
      {
      }
   }
   
   public void testCreateTemporaryQueue() throws Exception
   {
      mockClientSession.createQueue(isA(SimpleString.class), isA(SimpleString.class), (SimpleString) isNull(), eq(false), eq(true));
      mockClientSession.addDestination(isA(SimpleString.class), eq(false), eq(true));
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_QUEUE_SESSION);

      TemporaryQueue topic = session.createTemporaryQueue();
      assertNotNull(topic);
   }
   
   public void testCreateTemporaryQueueThrowsException() throws Exception
   {
      mockClientSession.createQueue(isA(SimpleString.class), isA(SimpleString.class), (SimpleString) isNull(), eq(false), eq(true));
      expectLastCall().andThrow(new MessagingException());
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_QUEUE_SESSION);
      try
      {
         session.createTemporaryQueue();
         fail("must throw a JMSException");
      } catch (JMSException e)
      {
      }
   }
   
   public void testCreateTemporaryQueueFromTopicSession() throws Exception
   {
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_TOPIC_CONNECTION, null, -1, sf);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_TOPIC_SESSION);
      try
      {
         session.createTemporaryQueue();
         fail("must throw a JMSException");
      } catch (JMSException e)
      {
      }
   }
   
   public void testCreateTemporaryTopic() throws Exception
   {
      mockClientSession.addDestination(isA(SimpleString.class), eq(false), eq(true));
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_TOPIC_CONNECTION, null, -1, sf);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_TOPIC_SESSION);

      TemporaryTopic topic = session.createTemporaryTopic();
      assertNotNull(topic);
   }
   
   public void testCreateTemporaryTopicThrowsException() throws Exception
   {
      mockClientSession.addDestination(isA(SimpleString.class), eq(false), eq(true));
      expectLastCall().andThrow(new MessagingException());
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_TOPIC_CONNECTION, null, -1, sf);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_TOPIC_SESSION);

      try
      {
         session.createTemporaryTopic();
         fail("must throw a JMSException");
      } catch (JMSException e)
      {
      }
   }

   public void testCreateTemporaryTopicFromQueueSession() throws Exception
   {
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_QUEUE_SESSION);

      try
      {
         session.createTemporaryTopic();
         fail("must throw a JMSException");
      } catch (JMSException e)
      {
      }
   }
   
   public void testDeleteTemporaryQueue() throws Exception
   {
      String queueName = randomString();
      SimpleString queueAddress = new SimpleString(JBossTemporaryQueue.JMS_TEMP_QUEUE_ADDRESS_PREFIX + queueName);
      
      // isExists() will return true
      SessionQueueQueryResponseMessage resp = new SessionQueueQueryResponseMessage(
            false, -1, 1, null, queueAddress);
      expect(mockClientSession.queueQuery(queueAddress)).andReturn(resp);      
      mockClientSession.removeDestination(queueAddress, false);
      mockClientSession.deleteQueue(queueAddress);
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      JBossSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_QUEUE_SESSION);
      JBossTemporaryQueue tempQueue = new JBossTemporaryQueue(session, queueName);

      session.deleteTemporaryQueue(tempQueue);
   }
   
   public void testDeleteTemporaryQueueWithUnknownQueue() throws Exception
   {
      String queueName = randomString();
      SimpleString queueAddress = new SimpleString(JBossTemporaryQueue.JMS_TEMP_QUEUE_ADDRESS_PREFIX + queueName);
      
      // isExists() will return false
      SessionQueueQueryResponseMessage resp = new SessionQueueQueryResponseMessage();
      expect(mockClientSession.queueQuery(queueAddress)).andReturn(resp);
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      JBossSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_QUEUE_SESSION);
      JBossTemporaryQueue tempQueue = new JBossTemporaryQueue(session, queueName);

      try
      {
         session.deleteTemporaryQueue(tempQueue);   
         fail("can not delete a temp queue which does not exist");
      } catch (InvalidDestinationException e)
      {
      }
   }
   
   public void testDeleteTemporaryQueueWithConsumers() throws Exception
   {
      String queueName = randomString();
      int consumerCount = 1;
      SimpleString queueAddress = new SimpleString(JBossTemporaryQueue.JMS_TEMP_QUEUE_ADDRESS_PREFIX + queueName);
      
      
      SessionQueueQueryResponseMessage resp =
         new SessionQueueQueryResponseMessage(false, consumerCount, 0, null, queueAddress);
      expect(mockClientSession.queueQuery(queueAddress)).andReturn(resp);
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      JBossSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_QUEUE_SESSION);
      JBossTemporaryQueue tempQueue = new JBossTemporaryQueue(session, queueName);

      try
      {
         session.deleteTemporaryQueue(tempQueue);   
         fail("can not delete a temp queue which has consumers");
      } catch (IllegalStateException e)
      {
      }
   }
   
   public void testDeleteTemporaryQueueThrowsException() throws Exception
   {
      expect(mockClientSession.queueQuery(isA(SimpleString.class))).andThrow(new MessagingException());
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      JBossSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_QUEUE_SESSION);
      JBossTemporaryQueue tempQueue = new JBossTemporaryQueue(session, randomString());

      try
      {
         session.deleteTemporaryQueue(tempQueue);   
         fail("must throw a JMSException");
      } catch (JMSException e)
      {
      }
   } 
   
   public void testDeleteTemporaryTopic() throws Exception
   {
      String topicName = randomString();
      SimpleString topicAddress = new SimpleString(JBossTemporaryTopic.JMS_TEMP_TOPIC_ADDRESS_PREFIX + topicName);
      
      SessionBindingQueryResponseMessage resp = new SessionBindingQueryResponseMessage(true, new ArrayList<SimpleString>());
      expect(mockClientSession.bindingQuery(topicAddress)).andReturn(resp);
      mockClientSession.removeDestination(topicAddress, false);
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      JBossSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_QUEUE_SESSION);
      JBossTemporaryTopic tempTopic = new JBossTemporaryTopic(session, topicName);

      session.deleteTemporaryTopic(tempTopic);
   }
 
   public void testDeleteTemporaryTopicWithUnknownTopic() throws Exception
   {
      String topicName = randomString();
      SimpleString topicAddress = new SimpleString(JBossTemporaryTopic.JMS_TEMP_TOPIC_ADDRESS_PREFIX + topicName);
      
      SessionBindingQueryResponseMessage resp = new SessionBindingQueryResponseMessage(false, new ArrayList<SimpleString>());
      expect(mockClientSession.bindingQuery(topicAddress)).andReturn(resp);
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      JBossSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_QUEUE_SESSION);
      JBossTemporaryTopic tempTopic = new JBossTemporaryTopic(session, topicName);

      try
      {
         session.deleteTemporaryTopic(tempTopic);
         fail("can not delete a temp topic which does not exist");
      } catch (InvalidDestinationException e)
      {
      }
   }
   
   public void testDeleteTemporaryTopicWhichHasSubscribers() throws Exception
   {
      String topicName = randomString();
      SimpleString topicAddress = new SimpleString(JBossTemporaryTopic.JMS_TEMP_TOPIC_ADDRESS_PREFIX + topicName);
      List<SimpleString> queueNames = new ArrayList<SimpleString>();
      queueNames.add(randomSimpleString());
      
      SessionBindingQueryResponseMessage resp = new SessionBindingQueryResponseMessage(true, queueNames);
      expect(mockClientSession.bindingQuery(topicAddress)).andReturn(resp);
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      JBossSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_QUEUE_SESSION);
      JBossTemporaryTopic tempTopic = new JBossTemporaryTopic(session, topicName);

      try
      {
         session.deleteTemporaryTopic(tempTopic);
         fail("can not delete a temp topic which has subscribers");
      } catch (IllegalStateException e)
      {
      }
   }
   
   public void testDeleteTemporaryTopicWhichThrowsException() throws Exception
   {
      String topicName = randomString();

      expect(mockClientSession.bindingQuery(isA(SimpleString.class))).andThrow(new MessagingException());
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      JBossSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_QUEUE_SESSION);
      JBossTemporaryTopic tempTopic = new JBossTemporaryTopic(session, topicName);

      try
      {
         session.deleteTemporaryTopic(tempTopic);
         fail("must throw a JMSException");
      } catch (JMSException e)
      {
      }
   }
   
   public void testGetSessionOnXASession() throws Exception
   {
      boolean isXA = true;
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      JBossSession session = new JBossSession(connection, false, isXA,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_QUEUE_SESSION);

      Session sess = session.getSession();
      assertNotNull(sess);
   }

   public void testGetSessionOnNonXASession() throws Exception
   {
      boolean isXA = false;
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      JBossSession session = new JBossSession(connection, false, isXA,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_QUEUE_SESSION);

      try
      {
         session.getSession();
         fail("can not get the session on a non-XA session");
      } catch (IllegalStateException e)
      {
      }
   }
   
   public void testGetXAResource() throws Exception
   {
      expect(mockClientSession.getXAResource()).andReturn(mockClientSession);
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      JBossSession session = new JBossSession(connection, false, true,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_QUEUE_SESSION);

      XAResource xares = session.getXAResource();
      assertNotNull(xares);
      assertSame(mockClientSession, xares);
   }
   
   public void testGetQueueSession() throws Exception
   {
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      JBossSession session = new JBossSession(connection, false, true,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_QUEUE_SESSION);

      QueueSession queueSess = session.getQueueSession();
      assertNotNull(queueSess);
      assertSame(session, queueSess);
   }
   
   public void testGetCoreSession() throws Exception
   {
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      JBossSession session = new JBossSession(connection, false, true,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_QUEUE_SESSION);

      ClientSession clientSession = session.getCoreSession();
      assertNotNull(clientSession);
      assertSame(mockClientSession, clientSession);
   }
   
   public void testUnsubscribe() throws Exception
   {
      String subName = randomString();
      String clientID = randomString();
      SimpleString queueAddres = new SimpleString(JBossTopic.createQueueNameForDurableSubscription(clientID, subName));      
      SessionQueueQueryResponseMessage resp = new SessionQueueQueryResponseMessage(false, 0, 0, null, queueAddres);
      expect(mockClientSession.queueQuery(queueAddres)).andReturn(resp );
      mockClientSession.deleteQueue(queueAddres);
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_TOPIC_CONNECTION, clientID, -1, sf);
      JBossSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_TOPIC_SESSION);

      session.unsubscribe(subName);
   }
   
   public void testUnsubscribeWithUnknownSubscription() throws Exception
   {
      String clientID = randomString();     
      // isExists() will return false
      SessionQueueQueryResponseMessage resp = new SessionQueueQueryResponseMessage();
      expect(mockClientSession.queueQuery(isA(SimpleString.class))).andReturn(resp );
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_TOPIC_CONNECTION, clientID, -1, sf);
      JBossSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_TOPIC_SESSION);

      try
      {
         session.unsubscribe(randomString());
         fail("can not unsubscribe from an unknown subscription");
      } catch (InvalidDestinationException e)
      {
      }
   }
   
   public void testUnsubscribeWithActiveSubscribers() throws Exception
   {
      String clientID = randomString();
      String subName = randomString();
      SimpleString queueAddres = new SimpleString(JBossTopic.createQueueNameForDurableSubscription(clientID, subName));
      int consumerCount = 1;      

      SessionQueueQueryResponseMessage resp =
         new SessionQueueQueryResponseMessage(true, consumerCount, 0, null, queueAddres);
      expect(mockClientSession.queueQuery(isA(SimpleString.class))).andReturn(resp );
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_TOPIC_CONNECTION, clientID, -1, sf);
      JBossSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_TOPIC_SESSION);

      try
      {
         session.unsubscribe(randomString());
         fail("can not unsubscribe when there are active subscribers");
      } catch (IllegalStateException e)
      {
      }
   }
   
   public void testUnsubscribeThrowsException() throws Exception
   {
      String clientID = randomString();
      expect(mockClientSession.queueQuery(isA(SimpleString.class))).andThrow(new MessagingException());
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_TOPIC_CONNECTION, clientID, -1, sf);
      JBossSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_TOPIC_SESSION);

      try
      {
         session.unsubscribe(randomString());
         fail("must throw a JMSException");
      } catch (JMSException e)
      {
      }
   }
   
   public void testUnsubscribeFromQueueSession() throws Exception
   {
      String subName = randomString();
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      JBossSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_QUEUE_SESSION);

      try
      {
         session.unsubscribe(subName);
         fail("can not unsubscribe from a queue session");
      } catch (IllegalStateException e)
      {
      }
   }

      // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void doTestCreateMessage(MessageCreation creation)
         throws JMSException
   {
      ByteBufferWrapper body = new ByteBufferWrapper(ByteBuffer.allocate(1024));
      ClientMessage clientMessage = new ClientMessageImpl(JBossMessage.TYPE, true, 0, System.currentTimeMillis(), (byte)4, body);
      expect(mockClientSession.isClosed()).andReturn(false);
      expect(mockClientSession.createClientMessage(EasyMock.anyByte(), EasyMock.anyBoolean(), EasyMock.anyInt(), EasyMock.anyLong(), EasyMock.anyByte())).andReturn(clientMessage);
      replay(sf, mockClientSession);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      Session session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_GENERIC_SESSION);
      Message message = creation.createMessage(session);
      assertNotNull(message);
   }

   private void doTestCreateConsumerWithSelector(String selector,
         SimpleString expectedSelector) throws Exception
   {
      JBossDestination destination = new JBossQueue(randomString());
      ClientConsumer clientConsumer = createStrictMock(ClientConsumer.class);

      // isExists() will return true
      SessionQueueQueryResponseMessage resp = new SessionQueueQueryResponseMessage(
            false, 0, 1, null, destination.getSimpleAddress());
      expect(mockClientSession.queueQuery(destination.getSimpleAddress()))
            .andReturn(resp);
      expect(
            mockClientSession.createConsumer(destination.getSimpleAddress(),
                  expectedSelector, false)).andReturn(
            clientConsumer);
      expect(mockClientSession.isClosed()).andReturn(false);

      replay(sf, mockClientSession, clientConsumer);

      JBossConnection connection = new JBossConnection(null, null,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
      Session session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, mockClientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      MessageConsumer consumer = session.createConsumer(destination, selector);
      assertNotNull(consumer);

      verify(clientConsumer);
   }

   // Inner classes -------------------------------------------------

   interface MessageCreation
   {
      Message createMessage(Session session) throws JMSException;
   }
}
