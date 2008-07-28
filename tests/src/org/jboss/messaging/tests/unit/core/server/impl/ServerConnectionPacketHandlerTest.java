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

import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.getCurrentArguments;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.wireformat.ConnectionCreateSessionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ConnectionCreateSessionResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.MessagingExceptionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.server.ServerConnection;
import org.jboss.messaging.core.server.impl.ServerConnectionPacketHandler;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class ServerConnectionPacketHandlerTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(ServerConnectionPacketHandlerTest.class);
   
   
   public void testGetId()
   {
      ServerConnection connection = createStrictMock(ServerConnection.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ServerConnectionPacketHandler handler = new ServerConnectionPacketHandler(connection, rc);
      expect(connection.getID()).andReturn(12345l);
      replay(connection);
      assertEquals(handler.getID(), 12345l);
      verify(connection);
   }

   public void testCreateSession() throws Exception
   {      
      ServerConnection connection = createStrictMock(ServerConnection.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ServerConnectionPacketHandler handler = new ServerConnectionPacketHandler(connection, rc);
      ConnectionCreateSessionMessage request = new ConnectionCreateSessionMessage(true, true, true);

      final ConnectionCreateSessionResponseMessage response = new ConnectionCreateSessionResponseMessage(12345);
      
      expect(connection.createSession(true, true, true)).andAnswer(new IAnswer<ConnectionCreateSessionResponseMessage>()
      {
         public ConnectionCreateSessionResponseMessage answer() throws Throwable
         {
            boolean isXa = (Boolean) getCurrentArguments()[0];
            boolean isSends = (Boolean) getCurrentArguments()[1];
            boolean isAcks = (Boolean) getCurrentArguments()[2];           
            assertTrue(isXa);
            assertTrue(isSends);
            assertTrue(isAcks);           
            return response;
         }
      });
      rc.sendOneWay(response);      
      replay(connection, rc);
      handler.handle(1254, request);
      verify(connection, rc);
   }

   public void testCreateSession2() throws Exception
   {      
      ServerConnection connection = createStrictMock(ServerConnection.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ServerConnectionPacketHandler handler = new ServerConnectionPacketHandler(connection, rc);
      ConnectionCreateSessionMessage request = new ConnectionCreateSessionMessage(false, false, false);

      final ConnectionCreateSessionResponseMessage response = new ConnectionCreateSessionResponseMessage(12345);
            
      expect(connection.createSession(false, false, false)).andAnswer(new IAnswer<ConnectionCreateSessionResponseMessage>()
      {
         public ConnectionCreateSessionResponseMessage answer() throws Throwable
         {
            boolean isXa = (Boolean) getCurrentArguments()[0];
            boolean isSends = (Boolean) getCurrentArguments()[1];
            boolean isAcks = (Boolean) getCurrentArguments()[2];           
            assertFalse(isXa);
            assertFalse(isSends);
            assertFalse(isAcks);
            return response;
         }
      });
      rc.sendOneWay(response);  
      replay(connection, rc);
      handler.handle(82712, request);
      verify(connection, rc);
   }

   public void testConnectionStart() throws Exception
   {
      ServerConnection connection = createStrictMock(ServerConnection.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ServerConnectionPacketHandler handler = new ServerConnectionPacketHandler(connection, rc);
      Packet packet = new PacketImpl(PacketImpl.CONN_START);
      connection.start();      
      replay(connection, rc);
      handler.handle(17261, packet);
      verify(connection, rc);
   }

   public void testConnectionStop() throws Exception
   {      
      ServerConnection connection = createStrictMock(ServerConnection.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ServerConnectionPacketHandler handler = new ServerConnectionPacketHandler(connection, rc);
      Packet packet = new PacketImpl(PacketImpl.CONN_STOP);
      connection.stop();
      rc.sendOneWay(EasyMock.isA(PacketImpl.class));
      EasyMock.expectLastCall().andAnswer(new IAnswer<Object>()
      {
         public Object answer() throws Throwable
         {
            Packet packet = (Packet) getCurrentArguments()[0];
            assertEquals(PacketImpl.NULL, packet.getType());
            return null;
         }
      });
      replay(connection, rc);
      handler.handle(71626, packet);
      verify(connection, rc);
   }

   public void testConnectionClose() throws Exception
   {      
      ServerConnection connection = createStrictMock(ServerConnection.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ServerConnectionPacketHandler handler = new ServerConnectionPacketHandler(connection, rc);
      Packet packet = new PacketImpl(PacketImpl.CLOSE);
      packet.setResponseTargetID(123);
      connection.close();
      rc.sendOneWay(EasyMock.isA(PacketImpl.class));
      EasyMock.expectLastCall().andAnswer(new IAnswer<Object>()
      {
         public Object answer() throws Throwable
         {
            Packet packet = (Packet) getCurrentArguments()[0];
            assertEquals(PacketImpl.NULL, packet.getType());
            return null;
         }
      });
      replay(connection, rc);
      handler.handle(1212, packet);
      verify(connection, rc);
   }

   public void testUnsupportedPacket() throws Exception
   {      
      ServerConnection connection = createStrictMock(ServerConnection.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ServerConnectionPacketHandler handler = new ServerConnectionPacketHandler(connection, rc);
      Packet packet = EasyMock.createStrictMock(Packet.class);
      expect(packet.getType()).andReturn(Byte.MAX_VALUE);
      final long responseTargetID = 283782374;
      EasyMock.expect(packet.getResponseTargetID()).andReturn(responseTargetID);
      rc.sendOneWay(EasyMock.isA(PacketImpl.class));
      EasyMock.expectLastCall().andAnswer(new IAnswer<Object>()
      {
         public Object answer() throws Throwable
         {
            MessagingExceptionMessage me = (MessagingExceptionMessage)EasyMock.getCurrentArguments()[0];
            assertEquals(MessagingException.UNSUPPORTED_PACKET, me.getException().getCode());
            return null;
         }
      });
      replay(connection, rc, packet);
      
      handler.handle(1212, packet);
      
      verify(connection, rc, packet);
   }
}
