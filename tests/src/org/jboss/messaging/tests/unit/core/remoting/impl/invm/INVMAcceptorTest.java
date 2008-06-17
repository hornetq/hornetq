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
package org.jboss.messaging.tests.unit.core.remoting.impl.invm;

import org.easymock.EasyMock;
import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.remoting.*;
import org.jboss.messaging.core.remoting.impl.invm.INVMAcceptor;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class INVMAcceptorTest extends UnitTestCase
{
   public void testStart() throws Exception
   {
      try
      {
         RemotingService remotingService = EasyMock.createStrictMock(RemotingService.class);
         CleanUpNotifier cleanUpNotifier = EasyMock.createStrictMock(CleanUpNotifier.class);
         final ConnectorRegistry connectorRegistry = EasyMock.createStrictMock(ConnectorRegistry.class);
         ConnectorRegistryFactory.setRegisteryLocator(new ConnectorRegistryLocator()
         {
            public ConnectorRegistry locate()
            {
               return connectorRegistry;
            }
         });
         Configuration conf = EasyMock.createStrictMock(Configuration.class);
         Location location = EasyMock.createStrictMock(Location.class);

         EasyMock.expect(remotingService.getConfiguration()).andReturn(conf).anyTimes();
         EasyMock.expect(conf.getLocation()).andReturn(location).anyTimes();
         PacketDispatcher packetDispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
         EasyMock.expect(remotingService.getDispatcher()).andReturn(packetDispatcher);
         EasyMock.expect(connectorRegistry.register(location, packetDispatcher)).andReturn(true);
         EasyMock.replay(remotingService, cleanUpNotifier, packetDispatcher, connectorRegistry, conf, location);

         INVMAcceptor invmAcceptor = new INVMAcceptor();
         invmAcceptor.startAccepting(remotingService, cleanUpNotifier);
         
         EasyMock.verify(remotingService, cleanUpNotifier, packetDispatcher, connectorRegistry, conf, location);
      }
      finally
      {
         ConnectorRegistryFactory.setRegisteryLocator(null);
      }
   }

   public void testStop() throws Exception
   {
      try
      {
         RemotingService remotingService = EasyMock.createStrictMock(RemotingService.class);
         CleanUpNotifier cleanUpNotifier = EasyMock.createStrictMock(CleanUpNotifier.class);
         final ConnectorRegistry connectorRegistry = EasyMock.createStrictMock(ConnectorRegistry.class);
         ConnectorRegistryFactory.setRegisteryLocator(new ConnectorRegistryLocator()
         {
            public ConnectorRegistry locate()
            {
               return connectorRegistry;
            }
         });
         Configuration conf = EasyMock.createStrictMock(Configuration.class);
         Location location = EasyMock.createStrictMock(Location.class);

         EasyMock.expect(remotingService.getConfiguration()).andReturn(conf).anyTimes();
         EasyMock.expect(conf.getLocation()).andReturn(location).anyTimes();
         PacketDispatcher packetDispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
         EasyMock.expect(remotingService.getDispatcher()).andReturn(packetDispatcher);
         EasyMock.expect(connectorRegistry.register(location, packetDispatcher)).andReturn(true);
         EasyMock.replay(remotingService, cleanUpNotifier, packetDispatcher, connectorRegistry, conf, location);

         INVMAcceptor invmAcceptor = new INVMAcceptor();
         invmAcceptor.startAccepting(remotingService, cleanUpNotifier);

         EasyMock.verify(remotingService, cleanUpNotifier, packetDispatcher, connectorRegistry, conf, location);

         EasyMock.reset(remotingService, cleanUpNotifier, packetDispatcher, connectorRegistry, conf, location);
         EasyMock.expect(remotingService.getConfiguration()).andReturn(conf).anyTimes();
         EasyMock.expect(conf.getLocation()).andReturn(location).anyTimes();
         EasyMock.expect(connectorRegistry.unregister(location)).andReturn(true);
         EasyMock.replay(remotingService, cleanUpNotifier, packetDispatcher, connectorRegistry, conf, location);
         invmAcceptor.stopAccepting();

         EasyMock.verify(remotingService, cleanUpNotifier, packetDispatcher, connectorRegistry, conf, location);
      }
      finally
      {
         ConnectorRegistryFactory.setRegisteryLocator(null);
      }
   }
}
