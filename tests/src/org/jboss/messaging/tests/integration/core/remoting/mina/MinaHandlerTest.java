/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.integration.core.remoting.mina;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import junit.framework.TestCase;

import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.impl.PacketDispatcherImpl;
import org.jboss.messaging.core.remoting.impl.mina.MinaHandler;
import org.jboss.messaging.core.remoting.impl.wireformat.TextPacket;
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
      TextPacket packet = new TextPacket("testReceiveUnhandledAbstractPacket");
      packet.setExecutorID(packetHandler.getID());
      
      handler.messageReceived(null, packet);

      assertEquals(0, packetHandler.getPackets().size());
   }

   public void testReceiveHandledAbstractPacket() throws Exception
   {
      packetHandler.expectMessage(1);

      TextPacket packet = new TextPacket("testReceiveHandledAbstractPacket");
      packet.setTargetID(packetHandler.getID());
      packet.setExecutorID(packetHandler.getID());

      handler.messageReceived(null, packet);

      assertTrue(packetHandler.await(500, MILLISECONDS));
      assertEquals(1, packetHandler.getPackets().size());
      assertEquals(packet.getText(), packetHandler.getPackets().get(0)
            .getText());
   }

   // TestCase overrides --------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      clientDispatcher = new PacketDispatcherImpl(null);
      threadPool = Executors.newCachedThreadPool();
      handler = new MinaHandler(clientDispatcher, threadPool, null, true);

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
