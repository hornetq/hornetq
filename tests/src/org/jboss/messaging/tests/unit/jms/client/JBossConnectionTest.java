/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.unit.jms.client;

import junit.framework.TestCase;
import org.easymock.EasyMock;
import static org.easymock.EasyMock.*;
import org.easymock.IArgumentMatcher;
import org.jboss.messaging.core.client.ClientConnection;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.RemotingSessionListener;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.version.Version;
import org.jboss.messaging.jms.client.JBossConnection;
import org.jboss.messaging.tests.util.RandomUtil;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import javax.jms.*;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class JBossConnectionTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testGetClientConnection() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.setRemotingSessionListener((RemotingSessionListener) EasyMock.anyObject());
      replay(clientConn);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);

      assertSame(clientConn, connection.getConnection());
      verify(clientConn);

   }
   
   public void testStart() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.setRemotingSessionListener((RemotingSessionListener) EasyMock.anyObject());
      clientConn.start();
      expectLastCall().once();

      replay(clientConn);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);

      connection.start();

      verify(clientConn);
   }

   public void testStartThrowsException() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.setRemotingSessionListener((RemotingSessionListener) EasyMock.anyObject());
      clientConn.start();
      expectLastCall().andThrow(new MessagingException());

      replay(clientConn);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);

      try
      {
         connection.start();
         fail("should throw a JMSException");
      } catch(JMSException e)
      {
      }

      verify(clientConn);
   }
   
   public void testStop() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.setRemotingSessionListener((RemotingSessionListener) EasyMock.anyObject());
      clientConn.stop();
      expectLastCall().once();

      replay(clientConn);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);

      connection.stop();

      verify(clientConn);
   }

   public void testStopThrowsException() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.setRemotingSessionListener((RemotingSessionListener) EasyMock.anyObject());
      clientConn.stop();
      expectLastCall().andThrow(new MessagingException());

      replay(clientConn);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);

      try
      {
         connection.stop();
         fail("should throw a JMSException");
      } catch(JMSException e)
      {
      }

      verify(clientConn);
   }
   
   public void testCloseThrowsException() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.setRemotingSessionListener((RemotingSessionListener) EasyMock.anyObject());
      clientConn.close();
      expectLastCall().andThrow(new MessagingException());

      replay(clientConn);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);

      try
      {
         connection.close();
         fail("should throw a JMSException");
      } catch(JMSException e)
      {
      }

      verify(clientConn);
   }
   
   public void testUsingClosedConnection() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.setRemotingSessionListener((RemotingSessionListener) EasyMock.anyObject());
      clientConn.close();
      expectLastCall().once();
      expect(clientConn.isClosed()).andReturn(true);
      
      replay(clientConn);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      connection.close();

      try
      {
         connection.getClientID();
         fail("should throw a JMSException");
      } catch (JMSException e)
      {
      }

      verify(clientConn);
   }

   public void testGetClientID() throws Exception
   {
      String clientID = randomString();
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.setRemotingSessionListener((RemotingSessionListener) EasyMock.anyObject());
      expect(clientConn.isClosed()).andReturn(false);
      replay(clientConn);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, clientID, -1);

      assertEquals(clientID, connection.getClientID());

      verify(clientConn);
   }

   public void testSetClientID() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.setRemotingSessionListener((RemotingSessionListener) EasyMock.anyObject());
      expect(clientConn.isClosed()).andStubReturn(false);

      replay(clientConn);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      String newClientID = randomString();
      connection.setClientID(newClientID);

      assertEquals(newClientID, connection.getClientID());

      verify(clientConn);
   }

   public void testSetClientIDFailsIfClientIDAlreadyExists() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.setRemotingSessionListener((RemotingSessionListener) EasyMock.anyObject());
      expect(clientConn.isClosed()).andStubReturn(false);

      replay(clientConn);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      String clientID = randomString();
      connection.setClientID(clientID);

      assertEquals(clientID, connection.getClientID());

      try
      {
         connection.setClientID(randomString());
         fail("should throw a JMS Exception");
      } catch (JMSException e)
      {
      }

      verify(clientConn);
   }

   public void testSetClientIDFailsIfConnectionAlreadyUsed() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.setRemotingSessionListener((RemotingSessionListener) EasyMock.anyObject());
      expect(clientConn.isClosed()).andStubReturn(false);
      clientConn.start();
      expectLastCall().once();

      replay(clientConn);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      connection.start();

      try
      {
         connection.setClientID(randomString());
         fail("should throw a JMS Exception");
      } catch (JMSException e)
      {
      }

      verify(clientConn);
   }
   
   public void testGetMetaData() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.setRemotingSessionListener((RemotingSessionListener) EasyMock.anyObject());
      expect(clientConn.isClosed()).andStubReturn(false);
      Version version = createStrictMock(Version.class);
      expect(clientConn.getServerVersion()).andReturn(version);
      replay(clientConn, version);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);

      ConnectionMetaData metadata = connection.getMetaData();
      assertNotNull(metadata);

      verify(clientConn, version);
   }
   
   public void testExceptionListener() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.setRemotingSessionListener((RemotingSessionListener) EasyMock.anyObject());
      expectLastCall().once();
      ExceptionListener listener = createStrictMock(ExceptionListener.class);
      replay(clientConn, listener);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);

      assertNull(connection.getExceptionListener());
      connection.setExceptionListener(listener);
      assertSame(listener, connection.getExceptionListener());
      
      verify(clientConn, listener);
   }
   
   public void testSetNullExceptionListener() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.setRemotingSessionListener((RemotingSessionListener) EasyMock.anyObject());
      expectLastCall().once();
      ExceptionListener listener = createStrictMock(ExceptionListener.class);
      replay(clientConn, listener);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);

      assertNull(connection.getExceptionListener());
      connection.setExceptionListener(null);
      assertNull(connection.getExceptionListener());
      
      verify(clientConn, listener);
   }

   public void testCreateConnectionConsumerFromDestination() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.setRemotingSessionListener((RemotingSessionListener) EasyMock.anyObject());
      Destination destination = createStrictMock(Destination.class);
      ServerSessionPool sessionPool = createStrictMock(ServerSessionPool.class);

      replay(clientConn, destination, sessionPool);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);

      ConnectionConsumer connConsumer = connection.createConnectionConsumer(destination, null, sessionPool, 10);
      assertNull(connConsumer); 

      verify(clientConn, destination, sessionPool);
   }
   
   public void testCreateConnectionConsumerFromQueue() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.setRemotingSessionListener((RemotingSessionListener) EasyMock.anyObject());
      Queue queue = createStrictMock(Queue.class);
      ServerSessionPool sessionPool = createStrictMock(ServerSessionPool.class);

      replay(clientConn, queue, sessionPool);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);

      ConnectionConsumer connConsumer = connection.createConnectionConsumer(queue, null, sessionPool, 10);
      assertNull(connConsumer); 

      verify(clientConn, queue, sessionPool);
   }
   
   public void testCreateConnectionConsumerFromTopic() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.setRemotingSessionListener((RemotingSessionListener) EasyMock.anyObject());
      Topic topic = createStrictMock(Topic.class);
      ServerSessionPool sessionPool = createStrictMock(ServerSessionPool.class);

      replay(clientConn, topic, sessionPool);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);

      ConnectionConsumer connConsumer = connection.createConnectionConsumer(topic, null, sessionPool, 10);
      assertNull(connConsumer); 

      verify(clientConn, topic, sessionPool);
   }
   
   public void testCreateDurableConnectionConsumerFromQueueConnection() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.setRemotingSessionListener((RemotingSessionListener) EasyMock.anyObject());
      Topic topic = createStrictMock(Topic.class);
      ServerSessionPool sessionPool = createStrictMock(ServerSessionPool.class);

      replay(clientConn, topic, sessionPool);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);

      try
      {
         connection.createDurableConnectionConsumer(topic, RandomUtil.randomString(), null, sessionPool, 10);
         connection.setClientID(randomString());
         fail("should throw a JMS Exception");
      } catch (JMSException e)
      {
      }

      verify(clientConn, topic, sessionPool);
   }
   
   public void testCreateSessionThrowsException() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.setRemotingSessionListener((RemotingSessionListener) EasyMock.anyObject());
      ClientSession clientSession = createStrictMock(ClientSession.class);
      expect(clientConn.createClientSession(false, false, false, -1, false, false)).andThrow(new MessagingException());
      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);

      try
      {
         connection.createQueueSession(true, 0);
         fail("should throw a JMSException");
      } catch(JMSException e)
      {
      }

      verify(clientConn, clientSession);
   }
   
   public void testCreateTransactedQueueSession() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.setRemotingSessionListener((RemotingSessionListener) EasyMock.anyObject());
      ClientSession clientSession = createStrictMock(ClientSession.class);
      expect(clientConn.createClientSession(false, false, false, -1, false, false)).andReturn(clientSession);
      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);

      QueueSession session = connection.createQueueSession(true, 0);
      assertNotNull(session);

      verify(clientConn, clientSession);
   }

   public void testCreateAutoAckQueueSession() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.setRemotingSessionListener((RemotingSessionListener) EasyMock.anyObject());
      ClientSession clientSession = createStrictMock(ClientSession.class);
      expect(clientConn.createClientSession(false, true, true, 1)).andReturn(clientSession);
      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);

      QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
      assertNotNull(session);

      verify(clientConn, clientSession);
   }
   
   public void testCreateDupsOKQueueSession() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.setRemotingSessionListener((RemotingSessionListener) EasyMock.anyObject());
      ClientSession clientSession = createStrictMock(ClientSession.class);
      expect(clientConn.createClientSession(false, true, true, -1)).andReturn(clientSession);
      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);

      QueueSession session = connection.createQueueSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      assertNotNull(session);

      verify(clientConn, clientSession);
   }
   
   public void testCreateClientAckQueueSession() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.setRemotingSessionListener((RemotingSessionListener) EasyMock.anyObject());
      ClientSession clientSession = createStrictMock(ClientSession.class);
      expect(clientConn.createClientSession(false, true, false, -1, false, false)).andReturn(clientSession);
      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);

      QueueSession session = connection.createQueueSession(false, Session.CLIENT_ACKNOWLEDGE);
      assertNotNull(session);

      verify(clientConn, clientSession);
   }
   
   public void testCreateQueueSessionWithInvalidAckMode() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.setRemotingSessionListener((RemotingSessionListener) EasyMock.anyObject());
      ClientSession clientSession = createStrictMock(ClientSession.class);
      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);

      try 
      {
         connection.createQueueSession(false, 12345);
         fail("must throw a IllegalArgumentException");
      } catch (IllegalArgumentException e)
      {         
      }

      verify(clientConn, clientSession);
   }

   public void testCreateTopicSession() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.setRemotingSessionListener((RemotingSessionListener) EasyMock.anyObject());
      ClientSession clientSession = createStrictMock(ClientSession.class);
      expect(clientConn.createClientSession(false, true, true, 1)).andReturn(clientSession);
      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_TOPIC_CONNECTION, null, -1);

      TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
      assertNotNull(session);

      verify(clientConn, clientSession);
   }
   
   public void testCreateTopicSessionWithCachedProducers() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.setRemotingSessionListener((RemotingSessionListener) EasyMock.anyObject());
      ClientSession clientSession = createStrictMock(ClientSession.class);
      expect(clientConn.createClientSession(false, true, true, 1)).andReturn(clientSession);
      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_TOPIC_CONNECTION, null, -1);

      TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE, true);
      assertNotNull(session);

      verify(clientConn, clientSession);
   }
   
   public void testCreateSession() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.setRemotingSessionListener((RemotingSessionListener) EasyMock.anyObject());
      ClientSession clientSession = createStrictMock(ClientSession.class);
      expect(clientConn.createClientSession(false, true, true, 1)).andReturn(clientSession);
      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_TOPIC_CONNECTION, null, -1);

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      assertNotNull(session);

      verify(clientConn, clientSession);
   }
   
   public void testCreateSessionWithCachedProducers() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.setRemotingSessionListener((RemotingSessionListener) EasyMock.anyObject());
      ClientSession clientSession = createStrictMock(ClientSession.class);
      expect(clientConn.createClientSession(false, true, true, 1)).andReturn(clientSession);
      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_TOPIC_CONNECTION, null, -1);

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE, true);
      assertNotNull(session);

      verify(clientConn, clientSession);
   }
   
   public void testCreateXASession() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.setRemotingSessionListener((RemotingSessionListener) EasyMock.anyObject());
      ClientSession clientSession = createStrictMock(ClientSession.class);
      expect(clientConn.createClientSession(true,false, false, -1, false, false)).andReturn(clientSession);
      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_TOPIC_CONNECTION, null, -1);

      XASession session = connection.createXASession();
      assertNotNull(session);

      verify(clientConn, clientSession);
   }
   
   public void testCreateXASessionWithCachedProducers() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.setRemotingSessionListener((RemotingSessionListener) EasyMock.anyObject());
      ClientSession clientSession = createStrictMock(ClientSession.class);
      expect(clientConn.createClientSession(true,false, false, -1, false, true)).andReturn(clientSession);
      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_TOPIC_CONNECTION, null, -1);

      XASession session = connection.createXASession(true);
      assertNotNull(session);

      verify(clientConn, clientSession);
   }
   
   public void testCreateXAQueueSession() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.setRemotingSessionListener((RemotingSessionListener) EasyMock.anyObject());
      ClientSession clientSession = createStrictMock(ClientSession.class);
      expect(clientConn.createClientSession(true,false, false, -1, false, false)).andReturn(clientSession);
      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_TOPIC_CONNECTION, null, -1);

      XAQueueSession session = connection.createXAQueueSession();
      assertNotNull(session);

      verify(clientConn, clientSession);
   }
   
   public void testCreateXAQueueSessionWithCachedProducers() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.setRemotingSessionListener((RemotingSessionListener) EasyMock.anyObject());
      ClientSession clientSession = createStrictMock(ClientSession.class);
      expect(clientConn.createClientSession(true,false, false, -1, false, true)).andReturn(clientSession);
      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_TOPIC_CONNECTION, null, -1);

      XAQueueSession session = connection.createXAQueueSession(true);
      assertNotNull(session);

      verify(clientConn, clientSession);
   }
   
   public void testCreateXATopicSession() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.setRemotingSessionListener((RemotingSessionListener) EasyMock.anyObject());
      ClientSession clientSession = createStrictMock(ClientSession.class);
      expect(clientConn.createClientSession(true,false, false, -1, false, false)).andReturn(clientSession);
      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_TOPIC_CONNECTION, null, -1);

      XATopicSession session = connection.createXATopicSession();
      assertNotNull(session);

      verify(clientConn, clientSession);
   }
   
   public void testCreateXATopicSessionWithCachedProducers() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.setRemotingSessionListener((RemotingSessionListener) EasyMock.anyObject());
      ClientSession clientSession = createStrictMock(ClientSession.class);
      expect(clientConn.createClientSession(true,false, false, -1, false, true)).andReturn(clientSession);
      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_TOPIC_CONNECTION, null, -1);

      XATopicSession session = connection.createXATopicSession(true);
      assertNotNull(session);

      verify(clientConn, clientSession);
   }

   public void testResourcesCleanedUp() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      FailureListenerMatcher failureListenerMatcher = new FailureListenerMatcher();
      EasyMock.reportMatcher(failureListenerMatcher);
      clientConn.setRemotingSessionListener(null);
      clientConn.cleanUp();
      replay(clientConn);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);
      failureListenerMatcher.listener.sessionDestroyed(0, new MessagingException());

      verify(clientConn);
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
   class FailureListenerMatcher implements IArgumentMatcher
   {
      RemotingSessionListener listener = null;
      public boolean matches(Object o)
      {
         listener = (RemotingSessionListener) o;
         return true;
      }

      public void appendTo(StringBuffer stringBuffer)
      {
         //we dont need this
      }
   }
}
