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
package org.jboss.messaging.tests.unit.core.server.impl;

import org.easymock.EasyMock;
import static org.easymock.EasyMock.*;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketReturner;
import org.jboss.messaging.core.remoting.impl.wireformat.ConsumerFlowCreditMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.server.ServerConsumer;
import org.jboss.messaging.core.server.impl.ServerConsumerPacketHandler;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class ServerConsumerPacketHandlerTest extends UnitTestCase
{
   public void testGetId()
   {
      ServerConsumer consumer = createStrictMock(ServerConsumer.class);
      ServerConsumerPacketHandler handler = new ServerConsumerPacketHandler(consumer);
      expect(consumer.getID()).andReturn(9999l);
      replay(consumer);
      assertEquals(9999l, handler.getID());
      verify(consumer);
   }

   public void testConsumerFlowHandle() throws Exception
   {
      ServerConsumer consumer = createStrictMock(ServerConsumer.class);
      PacketReturner returner = createStrictMock(PacketReturner.class);
      ServerConsumerPacketHandler handler = new ServerConsumerPacketHandler(consumer);
      ConsumerFlowCreditMessage message = new ConsumerFlowCreditMessage(100);
      consumer.receiveCredits(100);
      replay(consumer, returner);
      handler.doHandle(message, returner);
      verify(consumer, returner);
   }

   public void testConsumerClose() throws Exception
   {
      ServerConsumer consumer = createStrictMock(ServerConsumer.class);
      PacketReturner returner = createStrictMock(PacketReturner.class);
      ServerConsumerPacketHandler handler = new ServerConsumerPacketHandler(consumer);
      Packet message = new PacketImpl(PacketImpl.CLOSE);
      message.setResponseTargetID(123);
      consumer.close();
      replay(consumer, returner);
      assertNotNull(handler.doHandle(message, returner));
      verify(consumer, returner);
   }

   public void testUnsupportedPacket() throws Exception
   {
      ServerConsumer consumer = createStrictMock(ServerConsumer.class);
      PacketReturner returner = createStrictMock(PacketReturner.class);
      ServerConsumerPacketHandler handler = new ServerConsumerPacketHandler(consumer);
      Packet packet = EasyMock.createStrictMock(Packet.class);
      expect(packet.getType()).andReturn(Byte.MAX_VALUE);
      replay(consumer, returner);

      try
      {
         handler.doHandle(packet, returner);
         fail("should throw exception");
      }
      catch (Exception e)
      {
         MessagingException messagingException = (MessagingException) e;
         assertEquals(messagingException.getCode(), MessagingException.UNSUPPORTED_PACKET);
      }

      verify(consumer, returner);
   }
}
