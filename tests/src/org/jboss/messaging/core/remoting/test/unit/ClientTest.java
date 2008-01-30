/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.test.unit;

import static java.util.UUID.randomUUID;
import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.jboss.messaging.core.remoting.TransportType.TCP;
import static org.jboss.messaging.core.remoting.impl.mina.integration.test.TestSupport.PORT;

import java.io.IOException;

import javax.jms.IllegalStateException;

import junit.framework.TestCase;

import org.jboss.messaging.core.remoting.Client;
import org.jboss.messaging.core.remoting.NIOConnector;
import org.jboss.messaging.core.remoting.NIOSession;
import org.jboss.messaging.core.remoting.RemotingConfiguration;
import org.jboss.messaging.core.remoting.impl.ClientImpl;
import org.jboss.messaging.core.remoting.wireformat.NullPacket;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class ClientTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   private RemotingConfiguration remotingConfig;

   public void testConnected() throws Exception
   {
      NIOConnector connector = createStrictMock(NIOConnector.class);
      NIOSession session1 = createStrictMock(NIOSession.class);
      NIOSession session2 = createStrictMock(NIOSession.class);
      
      expect(connector.connect()).andReturn(session1).andReturn(session2);
      expect(session1.isConnected()).andReturn(true);
      expect(session2.isConnected()).andReturn(true);
      
      replay(connector, session1, session2);

      Client client = new ClientImpl(connector, remotingConfig);
      client.connect();
      assertTrue(client.isConnected());
      assertTrue(client.disconnect());
      assertFalse(client.isConnected());

      client.connect();
      assertTrue(client.isConnected());

      assertTrue(client.disconnect());
      assertFalse(client.isConnected());
      assertFalse(client.disconnect());
      
      verify(connector, session1, session2);
   }

   public void testConnectionFailure() throws Exception
   {
      NIOConnector connector = createStrictMock(NIOConnector.class);
      expect(connector.connect()).andThrow(new IOException("connection exception"));

      replay(connector);

      Client client = new ClientImpl(connector, remotingConfig);
      
      try
      {
         client.connect();
         fail("connection must fail");
      } catch (IOException e)
      {
      }
      
      verify(connector);
   }

   public void testSessionID() throws Exception
   {
      String sessionID = randomUUID().toString();
      
      NIOConnector connector = createStrictMock(NIOConnector.class);
      NIOSession session = createStrictMock(NIOSession.class);
      
      expect(connector.connect()).andReturn(session);
      expect(session.isConnected()).andReturn(true);
      expect(session.getID()).andReturn(sessionID);
      
      replay(connector, session);
      
      Client client = new ClientImpl(connector, remotingConfig);
      
      assertNull(client.getSessionID());
      client.connect();

      String actualSessionID = client.getSessionID();
      
      assertNotNull(actualSessionID);
      assertEquals(sessionID, actualSessionID);
      client.disconnect();
      assertNull(client.getSessionID());
      
      verify(connector, session);
   }

   public void testURI() throws Exception
   {
      NIOConnector connector = createStrictMock(NIOConnector.class);
      NIOSession session = createStrictMock(NIOSession.class);
      
      expect(connector.getServerURI()).andReturn(null);
      expect(connector.connect()).andReturn(session);
      expect(connector.getServerURI()).andReturn("tcp://localhost:" + PORT);
      expect(connector.getServerURI()).andReturn(null);
      // no expectation for the session
      
      replay(connector, session);
      
      Client client = new ClientImpl(connector, remotingConfig);
      
      assertNull(client.getURI());
      client.connect();
      assertNotNull(client.getURI());
      client.disconnect();
      assertNull(client.getURI());

      verify(connector, session);
   }

   public void testCanNotSendPacketIfNotConnected() throws Exception
   {
      NIOConnector connector = createStrictMock(NIOConnector.class);
      
      // connector is not expected to be called at all;
      replay(connector);
      
      Client client = new ClientImpl(connector, remotingConfig);
      try
      {
         client.send(new NullPacket(), true);
         fail("can not send a packet if the dispatcher is not connected");
      } catch (IllegalStateException e)
      {

      }
      
      verify(connector);
   }
   
   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      
      this.remotingConfig = new RemotingConfiguration(TCP, "localhost", PORT);
   }
   
   @Override
   protected void tearDown() throws Exception
   {
      remotingConfig = null;

      super.tearDown();
   }
}
