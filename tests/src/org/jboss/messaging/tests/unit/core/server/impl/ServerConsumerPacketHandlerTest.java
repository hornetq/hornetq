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
import org.jboss.messaging.core.remoting.impl.wireformat.ConsumerFlowCreditMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.MessagingExceptionMessage;
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
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ServerConsumerPacketHandler handler = new ServerConsumerPacketHandler(consumer, rc);
      expect(consumer.getID()).andReturn(9999l);
      replay(consumer);
      assertEquals(9999l, handler.getID());
      verify(consumer);
   }

   public void testConsumerFlowHandle() throws Exception
   {
      ServerConsumer consumer = createStrictMock(ServerConsumer.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ServerConsumerPacketHandler handler = new ServerConsumerPacketHandler(consumer, rc);
      ConsumerFlowCreditMessage message = new ConsumerFlowCreditMessage(100);
      consumer.receiveCredits(100);
      replay(consumer, rc);
      handler.handle(1212, message);
      verify(consumer, rc);
   }

   public void testConsumerClose() throws Exception
   {
      ServerConsumer consumer = createStrictMock(ServerConsumer.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ServerConsumerPacketHandler handler = new ServerConsumerPacketHandler(consumer, rc);
      Packet message = new PacketImpl(PacketImpl.CLOSE);
      message.setResponseTargetID(123);
      consumer.close();
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
      replay(consumer, rc);
      handler.handle(1212, message);
      verify(consumer, rc);
   }

   public void testUnsupportedPacket() throws Exception
   {
      ServerConsumer consumer = createStrictMock(ServerConsumer.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ServerConsumerPacketHandler handler = new ServerConsumerPacketHandler(consumer, rc);
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
      replay(consumer, rc, packet);
      handler.handle(1212, packet);
      verify(consumer, rc, packet);
   }
}
