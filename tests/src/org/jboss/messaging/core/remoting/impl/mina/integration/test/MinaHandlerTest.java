/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina.integration.test;

import junit.framework.TestCase;

import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.impl.mina.MinaHandler;
import org.jboss.messaging.core.remoting.test.unit.TestPacketHandler;
import org.jboss.messaging.core.remoting.wireformat.TextPacket;

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
      handler = new MinaHandler(PacketDispatcher.client);

      packetHandler = new TestPacketHandler();
      PacketDispatcher.client.register(packetHandler);
   }

   @Override
   protected void tearDown() throws Exception
   {
      PacketDispatcher.client.unregister(packetHandler.getID());
      packetHandler = null;

      handler = null;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
