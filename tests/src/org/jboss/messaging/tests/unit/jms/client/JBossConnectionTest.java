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

import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSession;
import javax.jms.XAQueueSession;
import javax.jms.XASession;
import javax.jms.XATopicSession;

import junit.framework.TestCase;

import org.easymock.EasyMock;
import org.jboss.messaging.core.client.ClientConnection;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.RemotingSessionListener;
import org.jboss.messaging.core.version.Version;
import org.jboss.messaging.jms.client.JBossConnection;
import org.jboss.messaging.tests.util.RandomUtil;

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

      replay(clientConn);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);

      assertSame(clientConn, connection.getConnection());
      verify(clientConn);

   }
   
   public void testStart() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.start();
      expectLastCall().once();

      replay(clientConn);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);

      connection.start();

      verify(clientConn);
   }

   public void testStop() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      clientConn.stop();
      expectLastCall().once();

      replay(clientConn);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1);

      connection.stop();

      verify(clientConn);
   }

   public void testUsingClosedConnection() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
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
      clientConn.setRemotingSessionListener(null);
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
   
   public void testCreateTransactedQueueSession() throws Exception
   {
      ClientConnection clientConn = createStrictMock(ClientConnection.class);
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
      ClientSession clientSession = createStrictMock(ClientSession.class);
      expect(clientConn.createClientSession(true,false, false, -1, false, true)).andReturn(clientSession);
      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_TOPIC_CONNECTION, null, -1);

      XATopicSession session = connection.createXATopicSession(true);
      assertNotNull(session);

      verify(clientConn, clientSession);
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
