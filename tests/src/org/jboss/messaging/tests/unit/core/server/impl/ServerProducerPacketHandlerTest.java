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
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.wireformat.MessagingExceptionMessage;
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
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ServerProducerPacketHandler handler = new ServerProducerPacketHandler(producer, rc);
      expect(producer.getID()).andReturn(999l);
      replay(producer, rc);
      assertEquals(handler.getID(), 999);
      verify(producer, rc);
   }

   public void testSend() throws Exception
   {
      ServerMessage serverMessage = createStrictMock(ServerMessage.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ServerProducer producer = createStrictMock(ServerProducer.class);
      ServerProducerPacketHandler handler = new ServerProducerPacketHandler(producer, rc);
      producer.send(serverMessage);
      ProducerSendMessage message = new ProducerSendMessage(serverMessage);
      replay(producer, rc, serverMessage);
      handler.handle(1212, message);
      verify(producer, rc, serverMessage);
   }

   public void testClose() throws Exception
   {
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ServerProducer producer = createStrictMock(ServerProducer.class);
      ServerProducerPacketHandler handler = new ServerProducerPacketHandler(producer, rc);
      producer.close();
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
      PacketImpl message = new PacketImpl(PacketImpl.CLOSE);
      message.setResponseTargetID(1);
      replay(producer, rc);
      handler.handle(1726172, message);
      verify(producer, rc);
   }

   public void testUnsupportedPacket() throws Exception
   {
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
      ServerProducer producer = createStrictMock(ServerProducer.class);
      ServerProducerPacketHandler handler = new ServerProducerPacketHandler(producer, rc);
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
      replay(producer, rc, packet);
      handler.handle(1212, packet);
      verify(producer, rc, packet);
   }
}
