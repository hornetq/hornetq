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

import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class ServerProducerPacketHandlerTest extends UnitTestCase
{
   public void testDummy()
   {      
   }
//   public void testGetId()
//   {
//      ServerProducer producer = createStrictMock(ServerProducer.class);
//      CommandManager cm = createStrictMock(CommandManager.class);  
//      ServerProducerPacketHandler handler = new ServerProducerPacketHandler(producer, cm);
//      expect(producer.getID()).andReturn(999l);
//      replay(producer, cm);
//      assertEquals(handler.getID(), 999);
//      verify(producer, cm);
//   }
//
//   public void testSend() throws Exception
//   {
//      ServerMessage serverMessage = createStrictMock(ServerMessage.class);
//      CommandManager cm = createStrictMock(CommandManager.class);  
//      ServerProducer producer = createStrictMock(ServerProducer.class);
//      ServerProducerPacketHandler handler = new ServerProducerPacketHandler(producer, cm);
//      producer.send(serverMessage);
//      SessionSendMessage message = new SessionSendMessage(serverMessage);
//      cm.packetProcessed(message);      
//      replay(producer, cm, serverMessage);
//      handler.handle(1212, message);
//      verify(producer, cm, serverMessage);
//   }
//
//   public void testClose() throws Exception
//   {
//      CommandManager cm = createStrictMock(CommandManager.class);  
//      ServerProducer producer = createStrictMock(ServerProducer.class);
//      ServerProducerPacketHandler handler = new ServerProducerPacketHandler(producer, cm);
//      producer.close();
//      PacketImpl message = new PacketImpl(PacketImpl.CLOSE);
//      final long responseTargetID = 1212;
//      message.setResponseTargetID(responseTargetID);      
//      cm.sendCommandOneway(responseTargetID, new PacketImpl(PacketImpl.NULL));
//      EasyMock.expectLastCall().andAnswer(new IAnswer<Object>()
//      {
//         public Object answer() throws Throwable
//         {
//            Packet packet = (Packet) getCurrentArguments()[1];
//            assertEquals(PacketImpl.NULL, packet.getType());
//            return null;
//         }
//      });
//      cm.packetProcessed(message);            
//      replay(producer, cm);
//      handler.handle(1726172, message);
//      verify(producer, cm);
//   }
//
//   public void testUnsupportedPacket() throws Exception
//   {
//      CommandManager cm = createStrictMock(CommandManager.class);       
//      ServerProducer producer = createStrictMock(ServerProducer.class);
//      ServerProducerPacketHandler handler = new ServerProducerPacketHandler(producer, cm);
//      Packet packet = EasyMock.createStrictMock(Packet.class);
//      expect(packet.getType()).andReturn(Byte.MAX_VALUE);
//      final long responseTargetID = 283782374;
//      EasyMock.expect(packet.getResponseTargetID()).andReturn(responseTargetID);
//      cm.sendCommandOneway(EasyMock.eq(responseTargetID), EasyMock.isA(PacketImpl.class));
//      EasyMock.expectLastCall().andAnswer(new IAnswer<Object>()
//      {
//         public Object answer() throws Throwable
//         {
//            MessagingExceptionMessage me = (MessagingExceptionMessage)EasyMock.getCurrentArguments()[1];
//            assertEquals(MessagingException.UNSUPPORTED_PACKET, me.getException().getCode());
//            return null;
//         }
//      });
//      cm.packetProcessed(packet);
//      replay(producer, cm, packet);
//      handler.handle(1212, packet);
//      verify(producer, cm, packet);
//   }
}
