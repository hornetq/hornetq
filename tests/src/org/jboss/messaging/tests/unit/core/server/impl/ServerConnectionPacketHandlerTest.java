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
import org.easymock.IAnswer;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketReturner;
import org.jboss.messaging.core.remoting.impl.wireformat.ConnectionCreateSessionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ConnectionCreateSessionResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.server.ServerConnection;
import org.jboss.messaging.core.server.impl.ServerConnectionPacketHandler;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class ServerConnectionPacketHandlerTest extends UnitTestCase
{
   public void testGetId()
   {
      ServerConnection connection = createStrictMock(ServerConnection.class);
      ServerConnectionPacketHandler handler = new ServerConnectionPacketHandler(connection);
      expect(connection.getID()).andReturn(12345l);
      replay(connection);
      assertEquals(handler.getID(), 12345l);
      verify(connection);
   }

   public void testCreateSession() throws Exception
   {
      final PacketReturner returner = createStrictMock(PacketReturner.class);
      ServerConnection connection = createStrictMock(ServerConnection.class);
      ServerConnectionPacketHandler handler = new ServerConnectionPacketHandler(connection);
      ConnectionCreateSessionMessage request = new ConnectionCreateSessionMessage(true, true, true);

      expect(connection.createSession(true, true, true, returner)).andAnswer(new IAnswer<ConnectionCreateSessionResponseMessage>()
      {
         public ConnectionCreateSessionResponseMessage answer() throws Throwable
         {
            boolean isXa = (Boolean) getCurrentArguments()[0];
            boolean isSends = (Boolean) getCurrentArguments()[1];
            boolean isAcks = (Boolean) getCurrentArguments()[2];
            PacketReturner packetReturner = (PacketReturner) getCurrentArguments()[3];
            assertTrue(isXa);
            assertTrue(isSends);
            assertTrue(isAcks);
            assertEquals(returner, packetReturner);
            return new ConnectionCreateSessionResponseMessage(12345);
         }
      });
      replay(connection, returner);
      handler.doHandle(request, returner);
      verify(connection, returner);
   }

   public void testCreateSession2() throws Exception
   {
      final PacketReturner returner = createStrictMock(PacketReturner.class);
      ServerConnection connection = createStrictMock(ServerConnection.class);
      ServerConnectionPacketHandler handler = new ServerConnectionPacketHandler(connection);
      ConnectionCreateSessionMessage request = new ConnectionCreateSessionMessage(false, false, false);

      expect(connection.createSession(false, false, false, returner)).andAnswer(new IAnswer<ConnectionCreateSessionResponseMessage>()
      {
         public ConnectionCreateSessionResponseMessage answer() throws Throwable
         {
            boolean isXa = (Boolean) getCurrentArguments()[0];
            boolean isSends = (Boolean) getCurrentArguments()[1];
            boolean isAcks = (Boolean) getCurrentArguments()[2];
            PacketReturner packetReturner = (PacketReturner) getCurrentArguments()[3];
            assertFalse(isXa);
            assertFalse(isSends);
            assertFalse(isAcks);
            assertEquals(returner, packetReturner);
            return new ConnectionCreateSessionResponseMessage(12345);
         }
      });
      replay(connection, returner);
      handler.doHandle(request, returner);
      verify(connection, returner);
   }

   public void testConnectionStart() throws Exception
   {
      final PacketReturner returner = createStrictMock(PacketReturner.class);
      ServerConnection connection = createStrictMock(ServerConnection.class);
      ServerConnectionPacketHandler handler = new ServerConnectionPacketHandler(connection);
      Packet packet = new PacketImpl(PacketImpl.CONN_START);
      connection.start();
      replay(connection, returner);
      handler.doHandle(packet, returner);
      verify(connection, returner);
   }

   public void testConnectionStop() throws Exception
   {
      final PacketReturner returner = createStrictMock(PacketReturner.class);
      ServerConnection connection = createStrictMock(ServerConnection.class);
      ServerConnectionPacketHandler handler = new ServerConnectionPacketHandler(connection);
      Packet packet = new PacketImpl(PacketImpl.CONN_STOP);
      connection.stop();
      replay(connection, returner);
      handler.doHandle(packet, returner);
      verify(connection, returner);
   }

   public void testConnectionClose() throws Exception
   {
      final PacketReturner returner = createStrictMock(PacketReturner.class);
      ServerConnection connection = createStrictMock(ServerConnection.class);
      ServerConnectionPacketHandler handler = new ServerConnectionPacketHandler(connection);
      Packet packet = new PacketImpl(PacketImpl.CLOSE);
      packet.setResponseTargetID(123);
      connection.close();
      replay(connection, returner);
      assertNotNull(handler.doHandle(packet, returner));
      verify(connection, returner);
   }

   public void testUnsupportedPacket() throws Exception
   {
      final PacketReturner returner = createStrictMock(PacketReturner.class);
      ServerConnection connection = createStrictMock(ServerConnection.class);
      ServerConnectionPacketHandler handler = new ServerConnectionPacketHandler(connection);
      Packet packet = EasyMock.createStrictMock(Packet.class);
      expect(packet.getType()).andReturn(Byte.MAX_VALUE);
      replay(connection, returner);
      
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

      verify(connection, returner);
   }
}
