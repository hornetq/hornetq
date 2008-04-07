/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina.integration.test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import junit.framework.TestCase;

import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.impl.PacketDispatcherImpl;
import org.jboss.messaging.core.remoting.impl.mina.MinaHandler;
import org.jboss.messaging.core.remoting.impl.wireformat.Packet;
import org.jboss.messaging.core.remoting.impl.wireformat.TextPacket;
import org.jboss.messaging.core.remoting.test.unit.TestPacketHandler;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class MinaHandlerOrderingTest extends TestCase
{

   private MinaHandler handler;
   private ExecutorService threadPool;
   
   private TestPacketHandler handler_1;
   private TestPacketHandler handler_2;
   private PacketDispatcher clientDispatcher;

   // Constants -----------------------------------------------------

   private static final int MANY_MESSAGES = 50;
   
   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   /**
    * Test that when messages are sent to 2 different targetIDs,
    * the messages are handled concurrently by the 2 PacketHandlers
    */
   public void testSerializationOrder() throws Exception
   {
      handler_1.expectMessage(2);
      handler_2.expectMessage(MANY_MESSAGES);

      TextPacket packet_1 = new TextPacket("testSerializationOrder handled by handle_1");
      packet_1.setTargetID(handler_1.getID());
      packet_1.setExecutorID(handler_1.getID());

      // we send 1 packet to handler_1
      // then many packets to handler_2
      // and again 1 packet to handler_1
      handler.messageReceived(null, packet_1);
      for (int i = 0; i < MANY_MESSAGES; i++)
      {
         TextPacket packet_2 = new TextPacket(Integer.toString(i));
         packet_2.setTargetID(handler_2.getID());
         packet_2.setExecutorID(handler_2.getID());
         handler.messageReceived(null, packet_2);
      }
      handler.messageReceived(null, packet_1);

      // we expect to receive the 2 packets on handler_1
      // *before* handler_2 received all its packets 
      assertTrue(handler_1.await(50, MILLISECONDS));
      int size = handler_2.getPackets().size();
      assertTrue("handler_2 should not have received all its message (size:" + size + ")", size < MANY_MESSAGES);

      assertTrue(handler_2.await(2, SECONDS));
      List<TextPacket> packetsReceivedByHandler_2 = handler_2.getPackets();
      assertEquals(MANY_MESSAGES, packetsReceivedByHandler_2.size());      
      // we check that handler_2 receives all its messages in order:
      for (int i = 0; i < MANY_MESSAGES; i++)
      {
         TextPacket p = packetsReceivedByHandler_2.get(i);
         assertEquals(Integer.toString(i), p.getText());
      }      
   }

   // TestCase overrides --------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      clientDispatcher = new PacketDispatcherImpl();
      threadPool = Executors.newCachedThreadPool();
      handler = new MinaHandler(clientDispatcher, threadPool, null, true);

      handler_1 = new TestPacketHandler();
      clientDispatcher.register(handler_1);
      handler_2 = new TestPacketHandler() {
        @Override
         public void handle(Packet packet, PacketSender sender)
         {
           // slow down the 2nd handler
           try
           {
              Thread.sleep(10);
           } catch (InterruptedException e)
           {
              e.printStackTrace();
           }           
           super.handle(packet, sender);
         } 
      };
      clientDispatcher.register(handler_2);
   }

   @Override
   protected void tearDown() throws Exception
   {
      clientDispatcher.unregister(handler_1.getID());
      clientDispatcher.unregister(handler_2.getID());
      threadPool.shutdown();
      handler_1 = null;
      handler_2 = null;
      clientDispatcher = null;
      handler = null;
      threadPool = null;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
