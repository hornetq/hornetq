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

package org.jboss.messaging.tests.unit.core.remoting.impl.invm;

import org.easymock.EasyMock;
import org.jboss.messaging.core.client.ConnectionParams;
import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.RemotingSession;
import org.jboss.messaging.core.remoting.impl.invm.INVMConnector;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.ByteBufferWrapper;
import org.jboss.messaging.util.MessagingBuffer;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class INVMConnectorTest extends UnitTestCase
{
   public void testConnect() throws Exception
   {
      Location location = EasyMock.createStrictMock(Location.class);
      ConnectionParams connectionParams = EasyMock.createStrictMock(ConnectionParams.class);
      PacketDispatcher clientPacketDispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      PacketDispatcher serverPacketDispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      INVMConnector invmConnector = new INVMConnector(location, connectionParams, 0, clientPacketDispatcher, serverPacketDispatcher);
      EasyMock.replay(location, connectionParams, clientPacketDispatcher, serverPacketDispatcher);
      RemotingSession remotingSession = invmConnector.connect();
      assertNotNull(remotingSession);
      assertEquals(invmConnector.getConnectionParams(), connectionParams);
      assertEquals(invmConnector.getDispatcher(), clientPacketDispatcher);
      assertEquals(invmConnector.getLocation(), location);
      EasyMock.verify(location, connectionParams, clientPacketDispatcher, serverPacketDispatcher);
   }

   public void testDisconnect() throws Exception
   {
      Location location = EasyMock.createStrictMock(Location.class);
      ConnectionParams connectionParams = EasyMock.createStrictMock(ConnectionParams.class);
      PacketDispatcher clientPacketDispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      PacketDispatcher serverPacketDispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      INVMConnector invmConnector = new INVMConnector(location, connectionParams, 0, clientPacketDispatcher, serverPacketDispatcher);
      EasyMock.replay(location, connectionParams, clientPacketDispatcher, serverPacketDispatcher);
      RemotingSession remotingSession = invmConnector.connect();
      assertNotNull(remotingSession);
      assertEquals(invmConnector.getConnectionParams(), connectionParams);
      assertEquals(invmConnector.getDispatcher(), clientPacketDispatcher);
      assertEquals(invmConnector.getLocation(), location);
      EasyMock.verify(location, connectionParams, clientPacketDispatcher, serverPacketDispatcher);

      EasyMock.reset(location, connectionParams, clientPacketDispatcher, serverPacketDispatcher);
      invmConnector.disconnect();
      EasyMock.replay(location, connectionParams, clientPacketDispatcher, serverPacketDispatcher);
      EasyMock.verify(location, connectionParams, clientPacketDispatcher, serverPacketDispatcher);
   }
    
    public void testCreateBuffer() throws Exception
    {
       Location location = EasyMock.createStrictMock(Location.class);
       ConnectionParams connectionParams = EasyMock.createStrictMock(ConnectionParams.class);
       PacketDispatcher clientPacketDispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
       PacketDispatcher serverPacketDispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
       INVMConnector invmConnector = new INVMConnector(location, connectionParams, 0, clientPacketDispatcher, serverPacketDispatcher);
       
       final int size = 120912;
       
       MessagingBuffer buff = invmConnector.createBuffer(size);
       
       assertTrue(buff instanceof ByteBufferWrapper);
       
       assertEquals(size, buff.capacity());
    }
}
