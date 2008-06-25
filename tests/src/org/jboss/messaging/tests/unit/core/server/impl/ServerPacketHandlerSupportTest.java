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

import static org.easymock.EasyMock.*;
import org.easymock.IAnswer;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketReturner;
import org.jboss.messaging.core.remoting.impl.wireformat.MessagingExceptionMessage;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.NO_ID_SET;
import org.jboss.messaging.core.server.impl.ServerPacketHandlerSupport;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class ServerPacketHandlerSupportTest extends UnitTestCase
{
   public void testCreateNullResponseNiIdSet()
   {
      Packet message = createStrictMock(Packet.class);
      PacketReturner returner = createStrictMock(PacketReturner.class);
      ServerPacketHandlerSupport handler = new ServerPacketHandlerSupport()
      {
         protected Packet doHandle(Packet packet, PacketReturner sender) throws Exception
         {
            return null;
         }

         public long getID()
         {
            return 0;
         }
      };

      replay(message, returner);
      handler.handle(message, returner);
      verify(message, returner);
   }

   public void testCreateResponseNiIdSet()
   {
      Packet message = createStrictMock(Packet.class);
      final Packet response = createStrictMock(Packet.class);
      PacketReturner returner = createStrictMock(PacketReturner.class);
      ServerPacketHandlerSupport handler = new ServerPacketHandlerSupport()
      {
         protected Packet doHandle(Packet packet, PacketReturner sender) throws Exception
         {
            return response;
         }

         public long getID()
         {
            return 0;
         }
      };

      expect(message.getResponseTargetID()).andReturn(NO_ID_SET);
      replay(message, response, returner);
      handler.handle(message, returner);
      verify(message, response, returner);
   }

   public void testCreateResponseIdSet() throws Exception
   {
      Packet message = createStrictMock(Packet.class);
      final Packet response = createStrictMock(Packet.class);
      PacketReturner returner = createStrictMock(PacketReturner.class);
      ServerPacketHandlerSupport handler = new ServerPacketHandlerSupport()
      {
         protected Packet doHandle(Packet packet, PacketReturner sender) throws Exception
         {
            return response;
         }

         public long getID()
         {
            return 0;
         }
      };

      expect(message.getResponseTargetID()).andReturn(1l);
      response.normalize(message);
      returner.send(response);
      replay(message, response, returner);
      handler.handle(message, returner);
      verify(message, response, returner);
   }

   public void testCreateResponseIdSetMessagingException() throws Exception
   {
      Packet message = createStrictMock(Packet.class);
      PacketReturner returner = createStrictMock(PacketReturner.class);
      ServerPacketHandlerSupport handler = new ServerPacketHandlerSupport()
      {
         protected Packet doHandle(Packet packet, PacketReturner sender) throws Exception
         {
            throw new MessagingException(99999);
         }

         public long getID()
         {
            return 0;
         }
      };

      expect(message.getResponseTargetID()).andStubReturn(1l);
      returner.send((Packet) anyObject());
      expectLastCall().andAnswer(new IAnswer<Object>()
      {
         public Object answer() throws Throwable
         {
            assertEquals(MessagingExceptionMessage.class, getCurrentArguments()[0].getClass());
            MessagingExceptionMessage m = (MessagingExceptionMessage) getCurrentArguments()[0];
            assertEquals(m.getException().getCode(), 99999);
            return null;
         }
      });
      replay(message, returner);
      handler.handle(message, returner);
      verify(message, returner);
   }

   public void testCreateResponseIdSetException() throws Exception
   {
      Packet message = createStrictMock(Packet.class);
      PacketReturner returner = createStrictMock(PacketReturner.class);
      ServerPacketHandlerSupport handler = new ServerPacketHandlerSupport()
      {
         protected Packet doHandle(Packet packet, PacketReturner sender) throws Exception
         {
            throw new Exception();
         }

         public long getID()
         {
            return 0;
         }
      };

      expect(message.getResponseTargetID()).andStubReturn(1l);
      returner.send((Packet) anyObject());
      expectLastCall().andAnswer(new IAnswer<Object>()
      {
         public Object answer() throws Throwable
         {
            assertEquals(MessagingExceptionMessage.class, getCurrentArguments()[0].getClass());
            MessagingExceptionMessage m = (MessagingExceptionMessage) getCurrentArguments()[0];
            assertEquals(m.getException().getCode(), MessagingException.INTERNAL_ERROR);
            return null;
         }
      });
      replay(message, returner);
      handler.handle(message, returner);
      verify(message, returner);
   }
}
