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

import org.easymock.EasyMock;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.remoting.RemotingHandler;
import org.jboss.messaging.core.remoting.impl.netty.NettyAcceptor;
import org.jboss.messaging.core.remoting.impl.netty.NettyAcceptorFactory;
import org.jboss.messaging.core.remoting.spi.Acceptor;
import org.jboss.messaging.core.remoting.spi.ConnectionLifeCycleListener;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 *
 * A MinaAcceptorFactoryTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class NettyAcceptorFactoryTest extends UnitTestCase
{
   public void testCreateAcceptor() throws Exception
   {
      NettyAcceptorFactory factory = new NettyAcceptorFactory();

      RemotingHandler handler = EasyMock.createStrictMock(RemotingHandler.class);
      Configuration config = new ConfigurationImpl();
      ConnectionLifeCycleListener listener = EasyMock.createStrictMock(ConnectionLifeCycleListener.class);

      Acceptor acceptor = factory.createAcceptor(config, handler, listener);

      assertTrue(acceptor instanceof NettyAcceptor);
   }
}
