/*
   * JBoss, Home of Professional Open Source
   * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.messaging.tests.unit.core.remoting.impl.mina;

import org.apache.mina.common.IoSession;
import org.easymock.EasyMock;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.impl.mina.MinaSession;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class MinaSessionTest extends UnitTestCase
{
   public void testGetSessionId()
   {
      IoSession ioSession = EasyMock.createStrictMock(IoSession.class);

      EasyMock.expect(ioSession.getId()).andReturn(12345l);
      EasyMock.replay(ioSession);
      MinaSession minaSession = new MinaSession(ioSession);
      assertEquals(minaSession.getID(), 12345l);
      EasyMock.verify(ioSession);
   }

   public void testIsConnected()
   {
      IoSession ioSession = EasyMock.createStrictMock(IoSession.class);

      EasyMock.expect(ioSession.isConnected()).andReturn(true);
      EasyMock.replay(ioSession);
      MinaSession minaSession = new MinaSession(ioSession);
      assertEquals(minaSession.isConnected(), true);
      EasyMock.verify(ioSession);
      EasyMock.reset(ioSession);
      EasyMock.expect(ioSession.isConnected()).andReturn(false);
      EasyMock.replay(ioSession);
      minaSession = new MinaSession(ioSession);
      assertEquals(minaSession.isConnected(), false);
      EasyMock.verify(ioSession);
   }

   public void testWritePacket()
   {
      IoSession ioSession = EasyMock.createStrictMock(IoSession.class);
      Packet packet = EasyMock.createNiceMock(Packet.class);
      EasyMock.expect(ioSession.write(packet)).andReturn(null);
      EasyMock.replay(ioSession);
      MinaSession minaSession = new MinaSession(ioSession);
      minaSession.write(packet);
      EasyMock.verify(ioSession);
   }
}
