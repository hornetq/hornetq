/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.unit.core.remoting;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.impl.wireformat.TextPacket;

public class TestPacketHandler implements PacketHandler
{
   private final long id;
   private final List<TextPacket> packets;
   private CountDownLatch latch;
   
   public TestPacketHandler(final long id)
   {
      this.id = id;
      packets = new ArrayList<TextPacket>();
   }

   public long getID()
   {
      return id;
   }
   
   public boolean await(long time, TimeUnit timeUnit) throws InterruptedException
   {
     if (latch == null)
        return false;
     return latch.await(time, timeUnit);
   }

   public void expectMessage(int count)
   {
      this.latch = new CountDownLatch(count);
   }

   public void handle(Packet packet, PacketSender sender)
   {
      packets.add((TextPacket) packet);
      
      doHandle(packet, sender);

      if (latch != null)
         latch.countDown();
   }
   
   protected void doHandle(Packet packet, PacketSender sender)
   {
   }

   public List<TextPacket> getPackets()
   {
      return packets;
   }
}