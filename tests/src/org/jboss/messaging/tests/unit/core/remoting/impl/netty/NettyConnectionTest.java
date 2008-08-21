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
package org.jboss.messaging.tests.unit.core.remoting.impl.netty;

import java.util.UUID;

import org.easymock.EasyMock;
import org.jboss.messaging.core.remoting.impl.netty.NettyConnection;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.netty.channel.Channel;

/**
 *
 * A NettyConnectionTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class NettyConnectionTest extends UnitTestCase
{
   public void testGetID() throws Exception
   {
      Channel channel = EasyMock.createStrictMock(Channel.class);

      final UUID id = UUID.randomUUID();

      EasyMock.expect(channel.getId()).andReturn(id);

      NettyConnection conn = new NettyConnection(channel);

      EasyMock.replay(channel);

      assertEquals(id, conn.getID());

      EasyMock.verify(channel);
   }

   public void testWrite() throws Exception
   {
      Channel channel = EasyMock.createStrictMock(Channel.class);

      final Object underlying = new Object();

      MessagingBuffer buff = EasyMock.createStrictMock(MessagingBuffer.class);

      EasyMock.expect(buff.getUnderlyingBuffer()).andReturn(underlying);

      EasyMock.expect(channel.write(underlying)).andReturn(null);

      NettyConnection conn = new NettyConnection(channel);

      EasyMock.replay(channel, buff);

      conn.write(buff);

      EasyMock.verify(channel, buff);
   }

   public void testCreateBuffer() throws Exception
   {
      Channel channel = EasyMock.createStrictMock(Channel.class);

      NettyConnection conn = new NettyConnection(channel);

      EasyMock.replay(channel);

      final int size = 1234;

      MessagingBuffer buff = conn.createBuffer(size);
      buff.putByte((byte) 0x00); // Netty buffer does lazy initialization.
      assertEquals(size, buff.capacity());

      EasyMock.verify(channel);
   }

}
