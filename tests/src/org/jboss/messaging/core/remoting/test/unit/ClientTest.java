/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.test.unit;

import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.jboss.messaging.core.remoting.TransportType.TCP;
import static org.jboss.messaging.core.remoting.integration.test.TestSupport.PORT;

import java.io.IOException;

import javax.jms.IllegalStateException;

import junit.framework.TestCase;

import org.jboss.messaging.core.remoting.Client;
import org.jboss.messaging.core.remoting.NIOConnector;
import org.jboss.messaging.core.remoting.NIOSession;
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

   public void testConnected() throws Exception
   {
      NIOConnector connector = createStrictMock(NIOConnector.class);
      NIOSession session1 = createStrictMock(NIOSession.class);
      NIOSession session2 = createStrictMock(NIOSession.class);
      
      expect(connector.connect("localhost", PORT, TCP)).andReturn(session1);
      expect(connector.disconnect()).andReturn(true);
      
      expect(connector.connect("localhost", PORT, TCP)).andReturn(session2);
      expect(session2.isConnected()).andReturn(true);
      
      expect(connector.disconnect()).andReturn(true);
      expect(connector.disconnect()).andReturn(false);

      replay(connector, session1, session2);

      Client client = new Client(connector);
      connector.connect("localhost", PORT, TCP);
      assertTrue(client.disconnect());
      assertFalse(client.isConnected());

      client.connect("localhost", PORT, TCP);
      assertTrue(client.isConnected());

      assertTrue(client.disconnect());
      assertFalse(client.isConnected());
      assertFalse(client.disconnect());
      
      verify(connector, session1, session2);
   }

   public void testConnectionFailure() throws Exception
   {
      NIOConnector connector = createStrictMock(NIOConnector.class);
      expect(connector.connect("localhost", PORT, TCP)).andThrow(new IOException("connection exception"));

      replay(connector);

      Client client = new Client(connector);
      
      try
      {
         client.connect("localhost", PORT, TCP);
         fail("connection must fail");
      } catch (IOException e)
      {
      }
      
      verify(connector);
   }

   public void testSessionID() throws Exception
   {
      long sessionID = System.currentTimeMillis();
      
      NIOConnector connector = createStrictMock(NIOConnector.class);
      NIOSession session = createStrictMock(NIOSession.class);
      
      expect(connector.connect("localhost", PORT, TCP)).andReturn(session);
      expect(session.isConnected()).andReturn(true);
      expect(session.getID()).andReturn(sessionID);
      expect(connector.disconnect()).andReturn(true);
      
      replay(connector, session);
      
      Client client = new Client(connector);

      assertNull(client.getSessionID());
      client.connect("localhost", PORT, TCP);

      String actualSessionID = client.getSessionID();
      
      assertNotNull(actualSessionID);
      assertEquals(Long.toString(sessionID), actualSessionID);
      client.disconnect();
      assertNull(client.getSessionID());
      
      verify(connector, session);
   }

   public void testURI() throws Exception
   {
      NIOConnector connector = createStrictMock(NIOConnector.class);
      NIOSession session = createStrictMock(NIOSession.class);
      
      expect(connector.getServerURI()).andReturn(null);
      expect(connector.connect("localhost", PORT, TCP)).andReturn(session);
      expect(connector.getServerURI()).andReturn("tcp://localhost:" + PORT);
      expect(connector.disconnect()).andReturn(true);
      expect(connector.getServerURI()).andReturn(null);
      // no expectation for the session
      
      replay(connector, session);
      
      Client client = new Client(connector);
      
      assertNull(client.getURI());
      client.connect("localhost", PORT, TCP);
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
      
      Client client = new Client(connector);
      try
      {
         client.sendOneWay(new NullPacket());
         fail("can not send a packet if the dispatcher is not connected");
      } catch (IllegalStateException e)
      {

      }
      
      verify(connector);
   }
}
