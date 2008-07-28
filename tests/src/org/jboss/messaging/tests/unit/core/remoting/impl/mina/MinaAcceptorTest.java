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

import org.easymock.EasyMock;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.remoting.ConnectionLifeCycleListener;
import org.jboss.messaging.core.remoting.RemotingHandler;
import org.jboss.messaging.core.remoting.impl.mina.MinaAcceptor;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * 
 * A MinaAcceptorTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class MinaAcceptorTest extends UnitTestCase
{
   public void testStartStop() throws Exception
   {
      RemotingHandler handler = EasyMock.createStrictMock(RemotingHandler.class);
      Configuration config = new ConfigurationImpl();
      ConnectionLifeCycleListener listener = EasyMock.createStrictMock(ConnectionLifeCycleListener.class);
      MinaAcceptor acceptor = new MinaAcceptor(config, handler, listener);
      
      acceptor.start();
      acceptor.stop();
      acceptor.start();
      acceptor.stop();
      
   }
}
