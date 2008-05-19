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
import org.jboss.messaging.core.remoting.PacketReturner;

public class TestPacketHandler implements PacketHandler
{
   private final long id;
   private final List<Packet> packets;
   private CountDownLatch latch;
   
   public TestPacketHandler(final long id)
   {
      this.id = id;
      packets = new ArrayList<Packet>();
   }

   public long getID()
   {
      return id;
   }
   
   public boolean await(long time, TimeUnit timeUnit) throws InterruptedException
   {
     if (latch == null)
        return false;
     boolean receivedAll = latch.await(time, timeUnit);
     if (!receivedAll)
        System.out.println("Still expecting to receive " + latch.getCount() + " packets");
     return receivedAll;
   }

   public void expectMessage(int count)
   {
      this.latch = new CountDownLatch(count);
   }

   public void handle(Packet packet, PacketReturner sender)
   {
      packets.add(packet);
      
      doHandle(packet, sender);

      if (latch != null)
         latch.countDown();
   }
   
   protected void doHandle(Packet packet, PacketReturner sender)
   {
   }

   public List<Packet> getPackets()
   {
      return packets;
   }
}