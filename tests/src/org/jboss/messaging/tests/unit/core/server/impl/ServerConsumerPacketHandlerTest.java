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
public class ServerConsumerPacketHandlerTest extends UnitTestCase
{
   public void testDummy()
   {      
   }
   
//   public void testGetId()
//   {
//      ServerConsumer consumer = createStrictMock(ServerConsumer.class);
//      CommandManager cm = createStrictMock(CommandManager.class);      
//      ServerConsumerPacketHandler handler = new ServerConsumerPacketHandler(consumer, cm);
//      expect(consumer.getID()).andReturn(9999l);
//      replay(consumer);
//      assertEquals(9999l, handler.getID());
//      verify(consumer);
//   }
//
//   public void testConsumerFlowHandle() throws Exception
//   {
//      ServerConsumer consumer = createStrictMock(ServerConsumer.class);
//      CommandManager cm = createStrictMock(CommandManager.class);     
//      ServerConsumerPacketHandler handler = new ServerConsumerPacketHandler(consumer, cm);
//      SessionFlowCreditMessage message = new SessionFlowCreditMessage(100);
//      consumer.receiveCredits(100);
//      cm.packetProcessed(message);
//      replay(consumer, cm);
//      handler.handle(1212, message);
//      verify(consumer, cm);
//   }
//
//   public void testConsumerClose() throws Exception
//   {
//      ServerConsumer consumer = createStrictMock(ServerConsumer.class);
//      CommandManager cm = createStrictMock(CommandManager.class);     
//      ServerConsumerPacketHandler handler = new ServerConsumerPacketHandler(consumer, cm);
//      Packet message = new PacketImpl(PacketImpl.CLOSE);
//      message.setResponseTargetID(123L);
//      consumer.close();
//      cm.sendCommandOneway(EasyMock.eq(123l), EasyMock.isA(PacketImpl.class));
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
//      replay(consumer, cm);
//      handler.handle(1212, message);
//      verify(consumer, cm);
//   }
//
//   public void testUnsupportedPacket() throws Exception
//   {
//      ServerConsumer consumer = createStrictMock(ServerConsumer.class);
//      CommandManager cm = createStrictMock(CommandManager.class);     
//      ServerConsumerPacketHandler handler = new ServerConsumerPacketHandler(consumer, cm);
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
//      replay(consumer, cm, packet);
//      handler.handle(1212, packet);
//      verify(consumer, cm, packet);
//   }
}
