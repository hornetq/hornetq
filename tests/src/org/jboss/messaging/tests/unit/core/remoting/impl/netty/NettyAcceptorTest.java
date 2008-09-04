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

import java.util.HashMap;
import java.util.Map;

import org.easymock.EasyMock;
import org.jboss.messaging.core.remoting.impl.netty.NettyAcceptor;
import org.jboss.messaging.core.remoting.spi.BufferHandler;
import org.jboss.messaging.core.remoting.spi.ConnectionLifeCycleListener;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 *
 * A MinaAcceptorTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class NettyAcceptorTest extends UnitTestCase
{
   public void testStartStop() throws Exception
   {
      BufferHandler handler = EasyMock.createStrictMock(BufferHandler.class);
      Map<String, Object> params = new HashMap<String, Object>();
      ConnectionLifeCycleListener listener = EasyMock.createStrictMock(ConnectionLifeCycleListener.class);
      NettyAcceptor acceptor = new NettyAcceptor(params, handler, listener);

      acceptor.start();
      acceptor.stop();
      acceptor.start();
      acceptor.stop();

   }
}
