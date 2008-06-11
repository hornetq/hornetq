/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.unit.jms.client;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import javax.jms.JMSException;

import junit.framework.TestCase;

import org.easymock.EasyMock;
import org.jboss.messaging.core.client.ClientConnection;
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

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
