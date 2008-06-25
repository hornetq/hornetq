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
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.ProducerSendMessage;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.ServerProducer;
import org.jboss.messaging.core.server.impl.ServerProducerPacketHandler;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class ServerProducerPacketHandlerTest extends UnitTestCase
{
   public void testGetId()
   {
      ServerProducer producer = createStrictMock(ServerProducer.class);
      ServerProducerPacketHandler handler = new ServerProducerPacketHandler(producer);
      expect(producer.getID()).andReturn(999l);
      replay(producer);
      assertEquals(handler.getID(), 999);
      verify(producer);
   }

   public void testSend() throws Exception
   {
      ServerMessage serverMessage = createStrictMock(ServerMessage.class);
      PacketReturner returner = createStrictMock(PacketReturner.class);
      ServerProducer producer = createStrictMock(ServerProducer.class);
      ServerProducerPacketHandler handler = new ServerProducerPacketHandler(producer);
      producer.send(serverMessage);
      //expect(producer.getID()).andReturn(999l);
      ProducerSendMessage message = new ProducerSendMessage(serverMessage);
      replay(producer, returner, serverMessage);
      handler.doHandle(message, returner);
      verify(producer, returner, serverMessage);
   }

    public void testClose() throws Exception
   {
      ServerMessage serverMessage = createStrictMock(ServerMessage.class);
      PacketReturner returner = createStrictMock(PacketReturner.class);
      ServerProducer producer = createStrictMock(ServerProducer.class);
      ServerProducerPacketHandler handler = new ServerProducerPacketHandler(producer);
      producer.close();
      PacketImpl message = new PacketImpl(PacketImpl.CLOSE);
      message.setResponseTargetID(1);
      replay(producer, returner, serverMessage);
      assertNotNull(handler.doHandle(message, returner));
      verify(producer, returner, serverMessage);
   }

   public void testUnsupportedPacket() throws Exception
   {
      ServerMessage serverMessage = createStrictMock(ServerMessage.class);
      PacketReturner returner = createStrictMock(PacketReturner.class);
      ServerProducer producer = createStrictMock(ServerProducer.class);
      ServerProducerPacketHandler handler = new ServerProducerPacketHandler(producer);
      Packet packet = EasyMock.createStrictMock(Packet.class);
      expect(packet.getType()).andReturn(Byte.MAX_VALUE);
      replay(producer, returner, serverMessage);

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

      verify(producer, returner, serverMessage);
   }
}
