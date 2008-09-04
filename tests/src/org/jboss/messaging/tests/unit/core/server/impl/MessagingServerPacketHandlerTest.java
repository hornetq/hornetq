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
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public class MessagingServerPacketHandlerTest extends UnitTestCase
{
   public void testDummy()
   {      
   }
   
//   public void testIdIsZero()
//   {
//      MessagingServer server = EasyMock.createStrictMock(MessagingServer.class);
//      RemotingService rs = EasyMock.createStrictMock(RemotingService.class);
//      MessagingServerPacketHandler messagingServerPacketHandler = new MessagingServerPacketHandler(server, rs);
//      EasyMock.replay(server, rs);
//      assertEquals(0, messagingServerPacketHandler.getID());
//      EasyMock.verify(server, rs);
//   }
//
//   public void testHandleCreateSession() throws Exception
//   {
//      MessagingServer server = EasyMock.createStrictMock(MessagingServer.class);
//      RemotingService rs = EasyMock.createStrictMock(RemotingService.class);
//      Version serverVersion = EasyMock.createStrictMock(Version.class);
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
//      CreateSessionMessage packet = new CreateSessionMessage("blah", 123, "andy", "taylor",
//                                                             false, false, false, 123456);
//      CreateSessionResponseMessage createConnectionResponse =
//         new CreateSessionResponseMessage(16256152, 98365, 123, 1234);
//      MessagingServerPacketHandler messagingServerPacketHandler = new MessagingServerPacketHandler(server, rs);
//      final long connectionID = 23982893;
//      EasyMock.expect(rs.getConnection(connectionID)).andReturn(rc);      
//      EasyMock.expect(server.createSession("blah", "andy", "taylor", 123, rc, false, false, false, 123456)).andReturn(createConnectionResponse);
//      rc.sendOneWay(createConnectionResponse);
//      EasyMock.replay(server, rc, rs, serverVersion);
//      messagingServerPacketHandler.handle(connectionID, packet);
//      EasyMock.verify(server, rc, rs, serverVersion);
//   }
//
//   public void testHandlePing() throws Exception
//   {
//      MessagingServer server = EasyMock.createStrictMock(MessagingServer.class);
//      RemotingService rs = EasyMock.createStrictMock(RemotingService.class);
//      final Packet ping = new PacketImpl(PacketImpl.PING);
//      final long responseTargetID = 282828;
//      ping.setResponseTargetID(responseTargetID);
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
//      MessagingServerPacketHandler messagingServerPacketHandler = new MessagingServerPacketHandler(server, rs);
//      final long connectionID = 23982893;
//      EasyMock.expect(rs.getConnection(connectionID)).andReturn(rc);
//      rc.sendOneWay((Packet) EasyMock.anyObject());
//      EasyMock.expectLastCall().andAnswer(new IAnswer<Object>()
//      {
//         public Object answer() throws Throwable
//         {
//            Packet decodedPong = (PacketImpl) EasyMock.getCurrentArguments()[0];
//            assertEquals(PacketImpl.PONG, decodedPong.getType()); 
//            assertEquals(decodedPong.getTargetID(), responseTargetID);
//            return null;
//         }
//      });
//      EasyMock.replay(server, rs, rc);
//      messagingServerPacketHandler.handle(connectionID, ping);
//      EasyMock.verify(server, rs, rc);
//   }
//
//   public void testUnsupportedPacket() throws Exception
//   {
//      MessagingServer server = EasyMock.createStrictMock(MessagingServer.class);
//      RemotingService rs = EasyMock.createStrictMock(RemotingService.class);          
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
//      MessagingServerPacketHandler messagingServerPacketHandler = new MessagingServerPacketHandler(server, rs);
//      final long connectionID = 23982893;
//      EasyMock.expect(rs.getConnection(connectionID)).andReturn(rc);     
//      Packet packet = EasyMock.createStrictMock(Packet.class);
//     
//      EasyMock.expect(packet.getType()).andReturn(Byte.MAX_VALUE);
//      final long responseTargetID = 283782374;
//      EasyMock.expect(packet.getResponseTargetID()).andReturn(responseTargetID);
//      rc.sendOneWay(EasyMock.isA(MessagingExceptionMessage.class));
//      EasyMock.expectLastCall().andAnswer(new IAnswer<Object>()
//      {
//         public Object answer() throws Throwable
//         {
//            MessagingExceptionMessage me = (MessagingExceptionMessage) EasyMock.getCurrentArguments()[0];
//            assertEquals(MessagingException.UNSUPPORTED_PACKET, me.getException().getCode());
//            return null;
//         }
//      });
//      EasyMock.replay(server, packet, rs, rc);
//  
//      messagingServerPacketHandler.handle(connectionID, packet);
//      
//      EasyMock.verify(server, packet, rs, rc);
//   }
}
