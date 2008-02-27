/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina.integration.test;

import junit.framework.TestCase;

import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.impl.PacketDispatcherImpl;
import org.jboss.messaging.core.remoting.impl.mina.MinaHandler;
import org.jboss.messaging.core.remoting.impl.wireformat.TextPacket;
import org.jboss.messaging.core.remoting.test.unit.TestPacketHandler;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class MinaHandlerTest extends TestCase
{

   private MinaHandler handler;
   private TestPacketHandler packetHandler;
   private PacketDispatcher clientDispatcher;

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testReceiveNotAbstractPacket() throws Exception
   {
      try
      {
         handler.messageReceived(null, new Object());
         fail();
      } catch (IllegalArgumentException e)
      {
      }
   }

   public void testReceiveUnhandledAbstractPacket() throws Exception
   {
      TextPacket packet = new TextPacket("testReceiveUnhandledAbstractPacket");

      handler.messageReceived(null, packet);

      assertEquals(0, packetHandler.getPackets().size());
   }

   public void testReceiveHandledAbstractPacket() throws Exception
   {

      TextPacket packet = new TextPacket("testReceiveHandledAbstractPacket");
      packet.setTargetID(packetHandler.getID());

      handler.messageReceived(null, packet);

      assertEquals(1, packetHandler.getPackets().size());
      assertEquals(packet.getText(), packetHandler.getPackets().get(0)
            .getText());
   }

   // TestCase overrides --------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      clientDispatcher = new PacketDispatcherImpl();
      handler = new MinaHandler(clientDispatcher, null, true);

      packetHandler = new TestPacketHandler();
      clientDispatcher.register(packetHandler);
   }

   @Override
   protected void tearDown() throws Exception
   {
      clientDispatcher.unregister(packetHandler.getID());
      packetHandler = null;
      clientDispatcher = null;
      handler = null;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
