/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core.remoting.integration;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.List;

import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.wireformat.TextPacket;
import org.jboss.test.messaging.core.remoting.TestPacketHandler;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class TargetHandlerTest extends TestSupport
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private ReversePacketHandler serverPacketHandler;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testClientHandlePacketSentByServer() throws Exception
   {
      TestPacketHandler clientHandler = new TestPacketHandler();
      PacketDispatcher.client.register(clientHandler);

      serverPacketHandler.expectMessage(1);
      clientHandler.expectMessage(1);

      TextPacket packet = new TextPacket(
            "testClientHandlePacketSentByServer from client");
      packet.setVersion((byte) 1);
      packet.setTargetID(serverPacketHandler.getID());
      // send a packet to create a sender when the server
      // handles the packet
      client.sendOneWay(packet);

      assertTrue(serverPacketHandler.await(2, SECONDS));

      assertNotNull(serverPacketHandler.getLastSender());
      PacketSender sender = serverPacketHandler.getLastSender();
      TextPacket packetFromServer = new TextPacket(
            "testClientHandlePacketSentByServer from server");
      packetFromServer.setVersion((byte) 1);
      packetFromServer.setTargetID(clientHandler.getID());
      sender.send(packetFromServer);

      assertTrue(clientHandler.await(2, SECONDS));

      List<TextPacket> packets = clientHandler.getPackets();
      assertEquals(1, packets.size());
      TextPacket packetReceivedByClient = (TextPacket) packets.get(0);
      assertEquals(packetFromServer.getText(), packetReceivedByClient.getText());
   }

   // TestCase overrides --------------------------------------------

   public void setUp() throws Exception
   {
      startServer(PORT, TRANSPORT);
      startClient(PORT, TRANSPORT);
      
      serverPacketHandler = new ReversePacketHandler();
      PacketDispatcher.server.register(serverPacketHandler);
   }

   public void tearDown() throws Exception
   {
      PacketDispatcher.server.unregister(serverPacketHandler.getID());

      client.disconnect();
      stopServer();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
