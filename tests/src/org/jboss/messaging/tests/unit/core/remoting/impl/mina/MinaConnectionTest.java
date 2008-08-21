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
package org.jboss.messaging.tests.unit.core.remoting.impl.mina;

import org.apache.mina.core.session.IoSession;
import org.easymock.EasyMock;
import org.jboss.messaging.core.remoting.impl.mina.MinaConnection;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 *
 * A MinaConnectionTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class MinaConnectionTest extends UnitTestCase
{
   public void testGetID() throws Exception
   {
      IoSession session = EasyMock.createStrictMock(IoSession.class);

      final long id = 192812;

      EasyMock.expect(session.getId()).andReturn(id);

      MinaConnection conn = new MinaConnection(session);

      EasyMock.replay(session);

      assertEquals(id, conn.getID());

      EasyMock.verify(session);
   }

   public void testWrite() throws Exception
   {
      IoSession session = EasyMock.createStrictMock(IoSession.class);

      final Object underlying = new Object();

      MessagingBuffer buff = EasyMock.createStrictMock(MessagingBuffer.class);

      EasyMock.expect(buff.getUnderlyingBuffer()).andReturn(underlying);

      EasyMock.expect(session.write(underlying)).andReturn(null);

      MinaConnection conn = new MinaConnection(session);

      EasyMock.replay(session, buff);

      conn.write(buff);

      EasyMock.verify(session, buff);
   }

   public void testCreateBuffer() throws Exception
   {
      IoSession session = EasyMock.createStrictMock(IoSession.class);

      MinaConnection conn = new MinaConnection(session);

      EasyMock.replay(session);

      final int size = 1234;

      MessagingBuffer buff = conn.createBuffer(size);

      assertEquals(size, buff.capacity());

      EasyMock.verify(session);
   }

}
