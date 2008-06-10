/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.integration.core.remoting.mina;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import junit.framework.TestCase;

import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.impl.PacketDispatcherImpl;
import org.jboss.messaging.core.remoting.impl.mina.MinaHandler;
import org.jboss.messaging.core.remoting.impl.wireformat.ConnectionCreateSessionResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.Ping;
import org.jboss.messaging.tests.unit.core.remoting.TestPacketHandler;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class MinaHandlerTest extends TestCase
{

   private MinaHandler handler;
   private ExecutorService threadPool;
   private TestPacketHandler packetHandler;
   private PacketDispatcher clientDispatcher;

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testReceiveUnhandledAbstractPacket() throws Exception
   {
      Packet packet = new Ping(randomLong());
      packet.setExecutorID(packetHandler.getID());
      
      handler.messageReceived(null, packet);

      assertEquals(0, packetHandler.getPackets().size());
   }

   public void testReceiveHandledAbstractPacket() throws Exception
   {
      packetHandler.expectMessage(1);

      ConnectionCreateSessionResponseMessage packet = new ConnectionCreateSessionResponseMessage(randomLong());
      packet.setTargetID(packetHandler.getID());
      packet.setExecutorID(packetHandler.getID());

      handler.messageReceived(null, packet);

      assertTrue(packetHandler.await(500, MILLISECONDS));
      assertEquals(1, packetHandler.getPackets().size());
      ConnectionCreateSessionResponseMessage receivedPacket = (ConnectionCreateSessionResponseMessage) packetHandler.getPackets().get(0);
      assertEquals(packet.getSessionID(), receivedPacket.getSessionID());
   }

   // TestCase overrides --------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      clientDispatcher = new PacketDispatcherImpl(null);
      threadPool = Executors.newCachedThreadPool();
      handler = new MinaHandler(clientDispatcher, threadPool, null, true, true);

      packetHandler = new TestPacketHandler(23);
      clientDispatcher.register(packetHandler);
   }

   @Override
   protected void tearDown() throws Exception
   {
      clientDispatcher.unregister(packetHandler.getID());
      threadPool.shutdown();
      packetHandler = null;
      clientDispatcher = null;
      handler = null;
      threadPool = null;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
