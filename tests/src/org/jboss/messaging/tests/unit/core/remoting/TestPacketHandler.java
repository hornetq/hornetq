/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
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