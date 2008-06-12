/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.unit.jms.client;

import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.util.ArrayList;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TransactionInProgressException;

import junit.framework.TestCase;

import org.easymock.EasyMock;
import org.jboss.messaging.core.client.ClientBrowser;
import org.jboss.messaging.core.client.ClientConnection;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryResponseMessage;
import org.jboss.messaging.jms.JBossDestination;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.JBossTopic;
import org.jboss.messaging.jms.client.JBossConnection;
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

   public void testClose() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      clientSession.close();

      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      Session session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      session.close();

      verify(clientConn, clientSession);
   }

   public void testCloseThrowsException() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      clientSession.close();
      expectLastCall().andThrow(new MessagingException());
      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      Session session = new JBossSession(connection, true, false,
            Session.DUPS_OK_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      try
      {
         session.close();
         fail("should throw a JMSException");
      } catch (JMSException e)
      {
      }

      verify(clientConn, clientSession);
   }

   public void testClosedSession() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      clientSession.close();
      expect(clientSession.isClosed()).andReturn(true);

      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      Session session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      session.close();

      try
      {
         session.getTransacted();
         fail("once a session is closed, this must throw a IllegalStateException");
      } catch (IllegalStateException e)
      {
      }

      verify(clientConn, clientSession);
   }

   public void testGetTransacted() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      expect(clientSession.isClosed()).andReturn(false);

      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      Session session = new JBossSession(connection, true, false, 0,
            clientSession, JBossSession.TYPE_GENERIC_SESSION);

      assertEquals(true, session.getTransacted());

      verify(clientConn, clientSession);
   }

   public void testGetAcknowledgeMode() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      expect(clientSession.isClosed()).andReturn(false);

      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      Session session = new JBossSession(connection, false, false,
            Session.DUPS_OK_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      assertEquals(Session.DUPS_OK_ACKNOWLEDGE, session.getAcknowledgeMode());

      verify(clientConn, clientSession);
   }

   public void testCommitOnTransactedSession() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      clientSession.commit();

      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      Session session = new JBossSession(connection, true, false,
            Session.DUPS_OK_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      session.commit();

      verify(clientConn, clientSession);
   }

   public void testCommitThrowsException() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      clientSession.commit();
      expectLastCall().andThrow(new MessagingException());
      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      Session session = new JBossSession(connection, true, false,
            Session.DUPS_OK_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      try
      {
         session.commit();
         fail("should throw a JMSException");
      } catch (JMSException e)
      {
      }

      verify(clientConn, clientSession);
   }

   public void testCommitOnNonTransactedSession() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);

      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      Session session = new JBossSession(connection, false, false,
            Session.DUPS_OK_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      try
      {
         session.commit();
         fail("commit() is not allowed on a non-transacted session");
      } catch (IllegalStateException e)
      {
      }

      verify(clientConn, clientSession);
   }

   public void testCommitOnXASession() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);

      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      Session session = new JBossSession(connection, true, true, 0,
            clientSession, JBossSession.TYPE_GENERIC_SESSION);

      try
      {
         session.commit();
         fail("commit() is not allowed on a XA session");
      } catch (TransactionInProgressException e)
      {
      }

      verify(clientConn, clientSession);
   }

   public void testRollbackOnTransactedSession() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      clientSession.rollback();

      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      Session session = new JBossSession(connection, true, false,
            Session.DUPS_OK_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      session.rollback();

      verify(clientConn, clientSession);
   }

   public void testRollbackThrowsException() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      clientSession.rollback();
      expectLastCall().andThrow(new MessagingException());
      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      Session session = new JBossSession(connection, true, false,
            Session.DUPS_OK_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      try
      {
         session.rollback();
         fail("should throw a JMSException");
      } catch (JMSException e)
      {
      }

      verify(clientConn, clientSession);
   }

   public void testRollbackOnNonTransactedSession() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);

      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      Session session = new JBossSession(connection, false, false,
            Session.DUPS_OK_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      try
      {
         session.rollback();
         fail("rollback() is not allowed on a non-transacted session");
      } catch (IllegalStateException e)
      {
      }

      verify(clientConn, clientSession);
   }

   public void testRollbackOnXASession() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);

      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      Session session = new JBossSession(connection, true, true, 0,
            clientSession, JBossSession.TYPE_GENERIC_SESSION);

      try
      {
         session.rollback();
         fail("rollback() is not allowed on a XA session");
      } catch (TransactionInProgressException e)
      {
      }

      verify(clientConn, clientSession);
   }

   public void testRecoverOnTransactedSession() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      Session session = new JBossSession(connection, true, false,
            Session.DUPS_OK_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      try
      {
         session.recover();
         fail("recover() is not allowed on a non-transacted session");
      } catch (IllegalStateException e)
      {
      }

      verify(clientConn, clientSession);
   }

   public void testRecoverOnNonTransactedSession() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      clientSession.rollback();
      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      Session session = new JBossSession(connection, false, false,
            Session.DUPS_OK_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      session.recover();

      verify(clientConn, clientSession);
   }

   public void testRecoverThrowsException() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      clientSession.rollback();
      expectLastCall().andThrow(new MessagingException());
      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      Session session = new JBossSession(connection, false, false,
            Session.DUPS_OK_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      try
      {
         session.recover();
         fail("should throw a JMSException");
      } catch (JMSException e)
      {
      }

      verify(clientConn, clientSession);
   }

   public void testMessageListener() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      expect(clientSession.isClosed()).andStubReturn(false);
      MessageListener listener = createStrictMock(MessageListener.class);
      replay(clientConn, clientSession, listener);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      Session session = new JBossSession(connection, false, false,
            Session.DUPS_OK_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_GENERIC_SESSION);
      assertNull(session.getMessageListener());
      session.setMessageListener(listener);
      assertSame(listener, session.getMessageListener());

      verify(clientConn, clientSession, listener);
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
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      ClientProducer clientProducer = createStrictMock(ClientProducer.class);
      expect(clientSession.createProducer(destination.getSimpleAddress()))
            .andReturn(clientProducer);

      replay(clientConn, clientSession, clientProducer);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      Session session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      MessageProducer producer = session.createProducer(destination);
      assertNotNull(producer);

      EasyMock.verify(clientConn, clientSession, clientProducer);
   }

   public void testCreateProducerThrowsException() throws Exception
   {
      JBossDestination destination = new JBossQueue(randomString());
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      ClientProducer clientProducer = createStrictMock(ClientProducer.class);
      expect(clientSession.createProducer(destination.getSimpleAddress()))
            .andThrow(new MessagingException());

      replay(clientConn, clientSession, clientProducer);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      Session session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      try
      {
         session.createProducer(destination);
         fail("should throw a JMSException");
      } catch (JMSException e)
      {
      }

      EasyMock.verify(clientConn, clientSession, clientProducer);
   }

   public void testCreateProducerWithInvalidDestination() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      Destination destination = createStrictMock(Destination.class);

      replay(clientConn, clientSession, destination);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      Session session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      try
      {
         session.createProducer(destination);
         fail("only instances of JBossDestination are allowed as destination");
      } catch (InvalidDestinationException e)
      {
      }

      EasyMock.verify(clientConn, clientSession, destination);
   }

   public void testCreateProducerWithNullDestination() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      ClientProducer clientProducer = createStrictMock(ClientProducer.class);
      expect(clientSession.createProducer(null)).andReturn(clientProducer);

      replay(clientConn, clientSession, clientProducer);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      Session session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_GENERIC_SESSION);

      session.createProducer(null);

      EasyMock.verify(clientConn, clientSession, clientProducer);
   }

   public void testCreatePublisher() throws Exception
   {
      JBossTopic topic = new JBossTopic(randomString());
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      ClientProducer clientProducer = createStrictMock(ClientProducer.class);
      expect(clientSession.createProducer(topic.getSimpleAddress())).andReturn(
            clientProducer);

      replay(clientConn, clientSession, clientProducer);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_TOPIC_CONNECTION, null, -1);
      TopicSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_TOPIC_SESSION);

      TopicPublisher publisher = session.createPublisher(topic);
      assertNotNull(publisher);

      EasyMock.verify(clientConn, clientSession, clientProducer);
   }

   public void testCreateSender() throws Exception
   {
      JBossQueue queue = new JBossQueue(randomString());
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      ClientProducer clientProducer = createStrictMock(ClientProducer.class);
      expect(clientSession.createProducer(queue.getSimpleAddress())).andReturn(
            clientProducer);

      replay(clientConn, clientSession, clientProducer);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_QUEUE_SESSION);

      QueueSender sender = session.createSender(queue);
      assertNotNull(sender);

      EasyMock.verify(clientConn, clientSession, clientProducer);
   }

   public void testCreateBrowser() throws Exception
   {
      JBossQueue queue = new JBossQueue(randomString());
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      ClientBrowser clientBrowser = createStrictMock(ClientBrowser.class);
      expect(clientSession.createBrowser(queue.getSimpleAddress(), null))
            .andReturn(clientBrowser);

      replay(clientConn, clientSession, clientBrowser);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_QUEUE_SESSION);

      QueueBrowser browser = session.createBrowser(queue);
      assertNotNull(browser);

      EasyMock.verify(clientConn, clientSession, clientBrowser);
   }
   
   public void testCreateBrowserThrowsException() throws Exception
   {
      JBossQueue queue = new JBossQueue(randomString());
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      ClientBrowser clientBrowser = createStrictMock(ClientBrowser.class);
      expect(clientSession.createBrowser(queue.getSimpleAddress(), null))
            .andThrow(new MessagingException());
      
      replay(clientConn, clientSession, clientBrowser);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_QUEUE_SESSION);

      try
      {
         session.createBrowser(queue);
         fail("should throw a JMSException");
      } catch (JMSException e)
      {
      }

      EasyMock.verify(clientConn, clientSession, clientBrowser);
   }

   public void testCreateBrowserWithEmptyFilter() throws Exception
   {
      JBossQueue queue = new JBossQueue(randomString());
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      ClientBrowser clientBrowser = createStrictMock(ClientBrowser.class);
      expect(clientSession.createBrowser(queue.getSimpleAddress(), null))
            .andReturn(clientBrowser);

      replay(clientConn, clientSession, clientBrowser);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_QUEUE_SESSION);

      QueueBrowser browser = session.createBrowser(queue, "");
      assertNotNull(browser);

      EasyMock.verify(clientConn, clientSession, clientBrowser);
   }

   public void testCreateBrowserWithFilter() throws Exception
   {
      String filter = "color = 'red'";
      JBossQueue queue = new JBossQueue(randomString());
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      ClientBrowser clientBrowser = createStrictMock(ClientBrowser.class);
      expect(
            clientSession.createBrowser(queue.getSimpleAddress(),
                  new SimpleString(filter))).andReturn(clientBrowser);

      replay(clientConn, clientSession, clientBrowser);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_QUEUE_SESSION);

      QueueBrowser browser = session.createBrowser(queue, filter);
      assertNotNull(browser);

      EasyMock.verify(clientConn, clientSession, clientBrowser);
   }

   public void testCreateBrowserFromTopicSession() throws Exception
   {
      Queue queue = new JBossQueue(randomString());
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);

      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_TOPIC_SESSION);

      try
      {
         session.createBrowser(queue);
         fail("browser can not be created from topic session");
      } catch (IllegalStateException e)
      {
      }

      EasyMock.verify(clientConn, clientSession);
   }

   public void testCreateBrowserForNullQueue() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);

      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_QUEUE_SESSION);

      try
      {
         session.createBrowser(null);
         fail("browser can not be created for a null destination");
      } catch (InvalidDestinationException e)
      {
      }

      EasyMock.verify(clientConn, clientSession);
   }

   public void testCreateBrowserForInvalidQueue() throws Exception
   {
      Queue queue = createStrictMock(Queue.class);
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);

      replay(clientConn, clientSession, queue);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_QUEUE_SESSION);

      try
      {
         session.createBrowser(queue);
         fail("browser can not be created for queues which are not instances of JBossQueue");
      } catch (InvalidDestinationException e)
      {
      }

      EasyMock.verify(clientConn, clientSession, queue);
   }

   public void testCreateQueue() throws Exception
   {
      // FIXME need to clean up use of queue address/simple address/name
      JBossQueue tempQueue = new JBossQueue(randomString());
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      // isExists() will return true
      SessionQueueQueryResponseMessage resp = new SessionQueueQueryResponseMessage(
            false, false, -1, -1, 1, null, tempQueue.getSimpleAddress());
      expect(clientSession.queueQuery(tempQueue.getSimpleAddress())).andReturn(
            resp);

      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_QUEUE_SESSION);

      Queue queue = session.createQueue(tempQueue.getName());
      assertNotNull(queue);

      EasyMock.verify(clientConn, clientSession);
   }

   public void testCreateQueueThrowsException() throws Exception
   {
      // FIXME need to clean up use of queue address/simple address/name
      JBossQueue tempQueue = new JBossQueue(randomString());
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      expect(clientSession.queueQuery(tempQueue.getSimpleAddress())).andThrow(new MessagingException());
      
      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_QUEUE_SESSION);

      try
      {
         session.createQueue(tempQueue.getName());
         fail("should throw a JMSException");
      } catch (JMSException e)
      {
      }

      EasyMock.verify(clientConn, clientSession);
   }
   public void testCreateQueueWithUnknownName() throws Exception
   {
      // FIXME need to clean up use of queue address/simple address/name
      JBossQueue tempQueue = new JBossQueue(randomString());
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      // isExists() will return false
      SessionQueueQueryResponseMessage resp = new SessionQueueQueryResponseMessage();
      expect(clientSession.queueQuery(tempQueue.getSimpleAddress())).andReturn(
            resp);

      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_QUEUE_SESSION);

      try
      {
         session.createQueue(tempQueue.getName());
         fail("creating a queue with an unknown name must throw a JMSException");
      } catch (JMSException e)
      {
      }

      EasyMock.verify(clientConn, clientSession);
   }

   public void testCreateQueueFromTopicSession() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);

      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_TOPIC_SESSION);

      try
      {
         session.createQueue(randomString());
         fail("creating a queue from a topic session must throw a IllegalStateException");
      } catch (IllegalStateException e)
      {
      }

      EasyMock.verify(clientConn, clientSession);
   }

   public void testCreateTopic() throws Exception
   {
      // FIXME need to clean up use of topic address/simple address/name
      JBossTopic tempTopic = new JBossTopic(randomString());
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      SessionBindingQueryResponseMessage resp = new SessionBindingQueryResponseMessage(
            true, new ArrayList<SimpleString>());
      expect(clientSession.bindingQuery(tempTopic.getSimpleAddress()))
            .andReturn(resp);

      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_TOPIC_SESSION);

      Topic topic = session.createTopic(tempTopic.getName());
      assertNotNull(topic);
      
      EasyMock.verify(clientConn, clientSession);
   }
   
   public void testCreateTopicThrowsException() throws Exception
   {
      // FIXME need to clean up use of topic address/simple address/name
      JBossTopic tempTopic = new JBossTopic(randomString());
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      expect(clientSession.bindingQuery(tempTopic.getSimpleAddress()))
            .andThrow(new MessagingException());

      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_TOPIC_SESSION);

      try
      {
         session.createTopic(tempTopic.getName());
         fail("should throw a JMSException");
      } catch (JMSException e)
      {
      }

      EasyMock.verify(clientConn, clientSession);
   }


   public void testCreateTopicWithUnknownName() throws Exception
   {
      // FIXME need to clean up use of topic address/simple address/name
      JBossTopic tempTopic = new JBossTopic(randomString());
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      SessionBindingQueryResponseMessage resp = new SessionBindingQueryResponseMessage(
            false, new ArrayList<SimpleString>());
      expect(clientSession.bindingQuery(tempTopic.getSimpleAddress()))
            .andReturn(resp);

      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_TOPIC_SESSION);

      try
      {
         session.createTopic(tempTopic.getName());
         fail("creating a topic with an unknown name must throw a JMSException");
      } catch (JMSException e)
      {
      }

      EasyMock.verify(clientConn, clientSession);
   }

   public void testCreateTopicFromQueueSession() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);

      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      QueueSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_QUEUE_SESSION);

      try
      {
         session.createTopic(randomString());
         fail("creating a topic from a queue session must throw a IllegalStateException");
      } catch (IllegalStateException e)
      {
      }

      EasyMock.verify(clientConn, clientSession);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void doTestCreateMessage(MessageCreation creation)
         throws JMSException
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      expect(clientSession.isClosed()).andReturn(false);
      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      Session session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_GENERIC_SESSION);
      Message message = creation.createMessage(session);
      assertNotNull(message);

      EasyMock.verify(clientConn, clientSession);
   }

   // Inner classes -------------------------------------------------

   interface MessageCreation
   {
      Message createMessage(Session session) throws JMSException;
   }
}
