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
package org.jboss.messaging.tests.timing.core.remoting.impl;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.easymock.EasyMock;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.impl.ByteBufferWrapper;
import org.jboss.messaging.core.remoting.impl.RemotingHandlerImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 *
 * A RemotingHandlerImplTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class RemotingHandlerImplTest extends UnitTestCase
{
   private RemotingHandlerImpl handler;
   private MessagingBuffer buff;
   private PacketDispatcher dispatcher;
   private ExecutorService executorService;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      dispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      executorService = EasyMock.createStrictMock(ExecutorService.class);
      handler = new RemotingHandlerImpl(dispatcher, executorService);
      buff = new ByteBufferWrapper(ByteBuffer.allocate(1024));
   }

   @Override
   protected void tearDown() throws Exception
   {
      super.tearDown();
      handler = null;
      buff = null;
   }

   public void testScanForFailedConnections() throws Exception
   {
      final long expirePeriod = 100;

      MessagingBuffer buff1 = new ByteBufferWrapper(ByteBuffer.allocate(1024));
      Packet ping1 = new PacketImpl(PacketImpl.PING);
      ping1.encode(buff1);
      buff1.getInt();
      final long connectionID1 = 120912;

      MessagingBuffer buff2 = new ByteBufferWrapper(ByteBuffer.allocate(1024));
      Packet ping2 = new PacketImpl(PacketImpl.PING);
      ping2.encode(buff2);
      buff2.getInt();
      final long connectionID2 = 12023;

      MessagingBuffer buff3 = new ByteBufferWrapper(ByteBuffer.allocate(1024));
      Packet ping3 = new PacketImpl(PacketImpl.PING);
      ping3.encode(buff3);
      buff3.getInt();
      final long connectionID3 = 1123128;

      MessagingBuffer buff4 = new ByteBufferWrapper(ByteBuffer.allocate(1024));
      Packet ping4 = new PacketImpl(PacketImpl.PING);
      ping4.encode(buff4);
      buff4.getInt();
      final long connectionID4 = 127987;

      Set<Object> failed = handler.scanForFailedConnections(expirePeriod);

      assertEquals(0, failed.size());

      handler.bufferReceived(connectionID1, buff1);
      handler.bufferReceived(connectionID2, buff2);
      handler.bufferReceived(connectionID3, buff3);
      handler.bufferReceived(connectionID4, buff4);

      failed = handler.scanForFailedConnections(expirePeriod);
      assertEquals(0, failed.size());

      Thread.sleep(expirePeriod + 10);

      failed = handler.scanForFailedConnections(expirePeriod);
      assertEquals(4, failed.size());

      handler.removeLastPing(connectionID1);
      handler.removeLastPing(connectionID2);
      handler.removeLastPing(connectionID3);
      handler.removeLastPing(connectionID4);

      failed = handler.scanForFailedConnections(expirePeriod);
      assertEquals(0, failed.size());
   }


}

