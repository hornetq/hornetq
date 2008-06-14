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
package org.jboss.messaging.tests.unit.core.remoting.impl;

import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.remoting.Acceptor;
import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.remoting.impl.AcceptorFactoryImpl;
import org.jboss.messaging.core.remoting.impl.invm.INVMAcceptor;
import org.jboss.messaging.core.remoting.impl.mina.MinaAcceptor;
import org.jboss.messaging.tests.util.UnitTestCase;

import java.util.List;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class AcceptorFactoryImplTest extends UnitTestCase
{
   public void testTCPINVMEnabled()
   {
      AcceptorFactoryImpl acceptorFactory = new AcceptorFactoryImpl();
      ConfigurationImpl config = new ConfigurationImpl();
      config.setTransport(TransportType.TCP);
      config.getConnectionParams().setInVMDisabled(false);
      List<Acceptor> acceptors = acceptorFactory.createAcceptors(config);
      assertEquals(2, acceptors.size());
      assertEquals(MinaAcceptor.class, acceptors.get(0).getClass());
      assertEquals(INVMAcceptor.class, acceptors.get(1).getClass());
   }

   public void testTCPINVMDisabled()
   {
      AcceptorFactoryImpl acceptorFactory = new AcceptorFactoryImpl();
      ConfigurationImpl config = new ConfigurationImpl();
      config.setTransport(TransportType.TCP);
      config.getConnectionParams().setInVMDisabled(true);
      List<Acceptor> acceptors = acceptorFactory.createAcceptors(config);
      assertEquals(1, acceptors.size());
      assertEquals(MinaAcceptor.class, acceptors.get(0).getClass());
   }

   public void testINVOnly()
   {
      AcceptorFactoryImpl acceptorFactory = new AcceptorFactoryImpl();
      ConfigurationImpl config = new ConfigurationImpl();
      config.setTransport(TransportType.INVM);
      config.getConnectionParams().setInVMDisabled(false);
      List<Acceptor> acceptors = acceptorFactory.createAcceptors(config);
      assertEquals(1, acceptors.size());
      assertEquals(INVMAcceptor.class, acceptors.get(0).getClass());
   }
}
