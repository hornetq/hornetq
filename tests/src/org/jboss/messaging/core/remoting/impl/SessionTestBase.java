/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.jboss.messaging.core.remoting.impl.mina.integration.test.TestSupport.MANY_MESSAGES;
import static org.jboss.messaging.core.remoting.impl.mina.integration.test.TestSupport.REQRES_TIMEOUT;
import static org.jboss.messaging.core.remoting.impl.mina.integration.test.TestSupport.reverse;
import static org.jboss.messaging.test.unit.RandomUtil.randomString;

import java.util.List;

import junit.framework.TestCase;

import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.NIOConnector;
import org.jboss.messaging.core.remoting.NIOSession;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.impl.mina.integration.test.ReversePacketHandler;
import org.jboss.messaging.core.remoting.impl.wireformat.AbstractPacket;
import org.jboss.messaging.core.remoting.impl.wireformat.Packet;
import org.jboss.messaging.core.remoting.impl.wireformat.TextPacket;
import org.jboss.messaging.core.remoting.test.unit.TestPacketHandler;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public abstract class SessionTestBase extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   protected ReversePacketHandler serverPacketHandler;

   protected PacketDispatcher serverDispatcher;
   protected PacketDispatcher clientDispatcher;

   protected NIOConnector connector;

   protected NIOSession session;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testConnected() throws Exception
   {
      NIOConnector connector = createNIOConnector(new PacketDispatcherImpl());
      NIOSession session = connector.connect();

      assertTrue(session.isConnected());
      
      assertTrue(connector.disconnect());
      assertFalse(session.isConnected());
      
   }    
      
   public void testWrite() throws Exception
   {
      serverPacketHandler.expectMessage(1);

      TextPacket packet = new TextPacket("testSendOneWay");
      packet.setTargetID(serverPacketHandler.getID());
      
      session.write(packet);

      assertTrue(serverPacketHandler.await(2, SECONDS));

      List<TextPacket> messages = serverPacketHandler.getPackets();
      assertEquals(1, messages.size());
      String response = ((TextPacket) messages.get(0)).getText();
      assertEquals(packet.getText(), response);
   }

   public void testWriteMany() throws Exception
   {
      serverPacketHandler.expectMessage(MANY_MESSAGES);

      TextPacket[] packets = new TextPacket[MANY_MESSAGES];
      for (int i = 0; i < MANY_MESSAGES; i++)
      {
         packets[i] = new TextPacket("testSendManyOneWay " + i);
         packets[i].setTargetID(serverPacketHandler.getID());
         session.write(packets[i]);
      }

      assertTrue(serverPacketHandler.await(10, SECONDS));

      List<TextPacket> receivedPackets = serverPacketHandler.getPackets();
      assertEquals(MANY_MESSAGES, receivedPackets.size());
      for (int i = 0; i < MANY_MESSAGES; i++)
      {
         TextPacket receivedPacket = (TextPacket) receivedPackets.get(i);
         assertEquals(packets[i].getText(), receivedPacket.getText());
      }
   }

   public void testWriteWithCallbackHandler() throws Exception
   {
      TestPacketHandler callbackHandler = new TestPacketHandler();
      callbackHandler.expectMessage(1);

      clientDispatcher.register(callbackHandler);

      TextPacket packet = new TextPacket("testSendOneWayWithCallbackHandler");
      packet.setTargetID(serverPacketHandler.getID());
      packet.setCallbackID(callbackHandler.getID());

      session.write(packet);

      assertTrue(callbackHandler.await(5, SECONDS));

      assertEquals(1, callbackHandler.getPackets().size());
      String response = callbackHandler.getPackets().get(0).getText();
      assertEquals(reverse(packet.getText()), response);
   }
   
   public void testWriteAndBlockWithOneCallbackLater() throws Exception
   {
      final PacketSender[] serverSender = new PacketSender[1];
      PacketHandler serverHandler = new PacketHandler() {
         private final String id = randomString();
         
         public String getID()
         {
            return id;
         }

         public void handle(Packet packet, PacketSender sender)
         {
            serverSender[0] = sender;
            // immediate reply
            TextPacket response = new TextPacket("blockingResponse");
            response.normalize(packet);
            try
            {
               sender.send(packet);
            } catch (Exception e)
            {
               fail(e.getMessage());
            }
         }
      };
      serverDispatcher.register(serverHandler);
      
      TestPacketHandler callbackHandler = new TestPacketHandler();
      callbackHandler.expectMessage(1);
      clientDispatcher.register(callbackHandler);

      TextPacket packet = new TextPacket("testSendOneWayWith2Callbacks");
      packet.setTargetID(serverHandler.getID());
      packet.setCallbackID(callbackHandler.getID());

      AbstractPacket blockingResponse = (AbstractPacket) session.writeAndBlock(packet, REQRES_TIMEOUT, SECONDS);
      assertNotNull(blockingResponse);
      
      assertEquals(0, callbackHandler.getPackets().size());
      
      assertNotNull(serverSender[0]);
      TextPacket callbackResponse = new TextPacket("callbackResponse");
      callbackResponse.setTargetID(callbackHandler.getID());
      serverSender[0].send(callbackResponse);

      assertTrue(callbackHandler.await(REQRES_TIMEOUT, SECONDS));
      
      assertEquals(1, callbackHandler.getPackets().size());
   }

   public void testWriteAndBlock() throws Exception
   {
      TextPacket request = new TextPacket("testSendBlocking");
      request.setTargetID(serverPacketHandler.getID());

      AbstractPacket receivedPacket = (AbstractPacket) session.writeAndBlock(request, REQRES_TIMEOUT, SECONDS);

      assertNotNull(receivedPacket);
      assertTrue(receivedPacket instanceof TextPacket);
      TextPacket response = (TextPacket) receivedPacket;
      assertEquals(reverse(request.getText()), response.getText());
   }
   
   public void testCorrelationCounter() throws Exception
   {
      TextPacket request = new TextPacket("testSendBlocking");
      request.setTargetID(serverPacketHandler.getID());

      AbstractPacket receivedPacket = (AbstractPacket) session.writeAndBlock(request, REQRES_TIMEOUT, SECONDS);
      long correlationID = request.getCorrelationID();
      
      assertNotNull(receivedPacket);      
      assertEquals(request.getCorrelationID(), receivedPacket.getCorrelationID());
      
      receivedPacket = (AbstractPacket) session.writeAndBlock(request, REQRES_TIMEOUT, SECONDS);
      assertEquals(correlationID + 1, request.getCorrelationID());
      assertEquals(correlationID + 1, receivedPacket.getCorrelationID());      
   }

   
   public void testClientHandlePacketSentByServer() throws Exception
   {
      TestPacketHandler clientHandler = new TestPacketHandler();
      clientDispatcher.register(clientHandler);

      serverPacketHandler.expectMessage(1);
      clientHandler.expectMessage(1);

      TextPacket packet = new TextPacket(
            "testClientHandlePacketSentByServer from client");
      packet.setTargetID(serverPacketHandler.getID());
      // send a packet to create a sender when the server
      // handles the packet
      session.write(packet);

      assertTrue(serverPacketHandler.await(2, SECONDS));

      assertNotNull(serverPacketHandler.getLastSender());
      PacketSender sender = serverPacketHandler.getLastSender();
      TextPacket packetFromServer = new TextPacket(
            "testClientHandlePacketSentByServer from server");
      packetFromServer.setTargetID(clientHandler.getID());
      sender.send(packetFromServer);

      assertTrue(clientHandler.await(2, SECONDS));

      List<TextPacket> packets = clientHandler.getPackets();
      assertEquals(1, packets.size());
      TextPacket packetReceivedByClient = (TextPacket) packets.get(0);
      assertEquals(packetFromServer.getText(), packetReceivedByClient.getText());
   }
   
   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      serverDispatcher = startServer();
      
      clientDispatcher = new PacketDispatcherImpl();

      connector = createNIOConnector(clientDispatcher);
      session = connector.connect();
      
      serverPacketHandler = new ReversePacketHandler();
      serverDispatcher.register(serverPacketHandler);
      
   }

   @Override
   protected void tearDown() throws Exception
   {
      serverDispatcher.unregister(serverPacketHandler.getID());

      connector.disconnect();
      stopServer();
      
      connector = null;
      session = null;
      serverDispatcher = null;
   }
   
   protected abstract RemotingConfiguration createRemotingConfiguration();
   
   protected abstract NIOConnector createNIOConnector(PacketDispatcher dispatcher);

   protected abstract PacketDispatcher startServer() throws Exception;
   
   protected abstract void stopServer();
}
