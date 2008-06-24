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
import org.easymock.IAnswer;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketReturner;
import org.jboss.messaging.core.remoting.RemotingService;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateConnectionRequest;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateConnectionResponse;
import org.jboss.messaging.core.remoting.impl.wireformat.Ping;
import org.jboss.messaging.core.remoting.impl.wireformat.Pong;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.impl.MessagingServerPacketHandler;
import org.jboss.messaging.core.version.Version;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class MessagingServerPacketHandlerTest extends UnitTestCase
{
   public void testIdIsZero()
   {
      MessagingServer server = EasyMock.createStrictMock(MessagingServer.class);
      MessagingServerPacketHandler messagingServerPacketHandler = new MessagingServerPacketHandler(server);
      EasyMock.replay(server);
      assertEquals(0, messagingServerPacketHandler.getID());
      EasyMock.verify(server);
   }

   public void testHandleCreateConnection() throws Exception
   {
      MessagingServer server = EasyMock.createStrictMock(MessagingServer.class);
      Version serverVersion = EasyMock.createStrictMock(Version.class);
      CreateConnectionRequest packet = new CreateConnectionRequest(123, "andy", "taylor");
      CreateConnectionResponse createConnectionResponse = new CreateConnectionResponse(1, serverVersion );
      PacketReturner sender = EasyMock.createStrictMock(PacketReturner.class);
      MessagingServerPacketHandler messagingServerPacketHandler = new MessagingServerPacketHandler(server);
      EasyMock.expect(server.createConnection("andy", "taylor", 123, sender)).andReturn(createConnectionResponse);
      EasyMock.replay(server, sender);
      assertEquals(createConnectionResponse, messagingServerPacketHandler.doHandle(packet, sender));
      EasyMock.verify(server, sender);
   }

   public void testHandlePing() throws Exception
   {
      MessagingServer server = EasyMock.createStrictMock(MessagingServer.class);
      RemotingService remotingService = EasyMock.createStrictMock(RemotingService.class);
      final Ping packet = new Ping(456);
      PacketReturner sender = EasyMock.createStrictMock(PacketReturner.class);
      MessagingServerPacketHandler messagingServerPacketHandler = new MessagingServerPacketHandler(server);
      EasyMock.expect(server.getRemotingService()).andStubReturn(remotingService);
      EasyMock.expect(remotingService.isSession(789l)).andReturn(true);
      EasyMock.expect(sender.getSessionID()).andStubReturn(789);
      sender.send((Packet) EasyMock.anyObject());
      EasyMock.expectLastCall().andAnswer(new IAnswer<Object>()
      {
         public Object answer() throws Throwable
         {
            Pong decodedPong = (Pong) EasyMock.getCurrentArguments()[0];
            assertEquals(decodedPong.getSessionID(), 456);
            assertEquals(decodedPong.getTargetID(), packet.getResponseTargetID());
            assertEquals(decodedPong.isSessionFailed(), false);
            return null;
         }
      });
      EasyMock.replay(server, sender, remotingService);
      messagingServerPacketHandler.doHandle(packet, sender);
      EasyMock.verify(server, sender, remotingService);
   }

   public void testHandlePingNoSession() throws Exception
   {
      MessagingServer server = EasyMock.createStrictMock(MessagingServer.class);
      RemotingService remotingService = EasyMock.createStrictMock(RemotingService.class);
      final Ping packet = new Ping(456);
      PacketReturner sender = EasyMock.createStrictMock(PacketReturner.class);
      MessagingServerPacketHandler messagingServerPacketHandler = new MessagingServerPacketHandler(server);
      EasyMock.expect(server.getRemotingService()).andStubReturn(remotingService);
      EasyMock.expect(remotingService.isSession(789l)).andReturn(false);
      EasyMock.expect(sender.getSessionID()).andStubReturn(789);
      sender.send((Packet) EasyMock.anyObject());
      EasyMock.expectLastCall().andAnswer(new IAnswer<Object>()
      {
         public Object answer() throws Throwable
         {
            Pong decodedPong = (Pong) EasyMock.getCurrentArguments()[0];
            assertEquals(decodedPong.getSessionID(), 456);
            assertEquals(decodedPong.getTargetID(), packet.getResponseTargetID());
            assertEquals(decodedPong.isSessionFailed(), true);
            return null;
         }
      });
      EasyMock.replay(server, sender, remotingService);
      messagingServerPacketHandler.doHandle(packet, sender);
      EasyMock.verify(server, sender, remotingService);
   }

   public void testUnsupportedPacket() throws Exception
   {
      MessagingServer server = EasyMock.createStrictMock(MessagingServer.class);
      Packet packet = EasyMock.createStrictMock(Packet.class);
      PacketReturner sender = EasyMock.createStrictMock(PacketReturner.class);
      MessagingServerPacketHandler messagingServerPacketHandler = new MessagingServerPacketHandler(server);
      EasyMock.expect(packet.getType()).andReturn(Byte.MAX_VALUE);
      EasyMock.replay(server, packet, sender);
      try
      {
         messagingServerPacketHandler.doHandle(packet, sender);
         fail("should throw exception");
      }
      catch (Exception e)
      {
         MessagingException messagingException = (MessagingException) e;
         assertEquals(messagingException.getCode(), MessagingException.UNSUPPORTED_PACKET);
      }
      EasyMock.verify(server, packet, sender);
   }
}
