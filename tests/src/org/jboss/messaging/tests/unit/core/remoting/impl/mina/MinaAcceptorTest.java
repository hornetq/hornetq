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

import org.easymock.EasyMock;
import org.jboss.messaging.core.client.ConnectionParams;
import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.client.impl.ConnectionParamsImpl;
import org.jboss.messaging.core.client.impl.LocationImpl;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.*;
import org.jboss.messaging.core.remoting.impl.PacketDispatcherImpl;
import org.jboss.messaging.core.remoting.impl.mina.MinaAcceptor;
import org.jboss.messaging.core.remoting.impl.mina.MinaConnector;
import org.jboss.messaging.core.remoting.impl.wireformat.Ping;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class MinaAcceptorTest extends UnitTestCase
{
   public void testStartAccepting() throws Exception
   {
      MinaAcceptor acceptor = new MinaAcceptor();
      ConfigurationImpl conf = new ConfigurationImpl();
      conf.setTransport(TransportType.TCP);
      conf.setPort(5400);
      conf.setHost("localhost");
      try
      {
         RemotingService remotingService = EasyMock.createStrictMock(RemotingService.class);
         CleanUpNotifier cleanUpNotifier = EasyMock.createStrictMock(CleanUpNotifier.class);
         PacketDispatcher packetDispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
         EasyMock.expect(remotingService.getConfiguration()).andReturn(conf).anyTimes();
         EasyMock.expect(remotingService.getDispatcher()).andReturn(packetDispatcher);
         EasyMock.expect(remotingService.getConfiguration()).andReturn(conf).anyTimes();
         remotingService.registerPinger((RemotingSession) EasyMock.anyObject());
         remotingService.unregisterPinger(EasyMock.anyLong());
         cleanUpNotifier.fireCleanup(EasyMock.anyLong(), (MessagingException) EasyMock.isNull());
         EasyMock.replay(remotingService, cleanUpNotifier);
         acceptor.startAccepting(remotingService, cleanUpNotifier);
         Location location = new LocationImpl(TransportType.TCP, "localhost", 5400);
         MinaConnector minaConnector = new MinaConnector(location, new PacketDispatcherImpl(null));
         minaConnector.connect();
         minaConnector.disconnect();
         EasyMock.verify(remotingService, cleanUpNotifier);
      }
      finally
      {
         acceptor.stopAccepting();
      }
   }

   public void testStartAcceptingUsingSSL() throws Exception
   {
      MinaAcceptor acceptor = new MinaAcceptor();
      ConfigurationImpl conf = new ConfigurationImpl();
      conf.setTransport(TransportType.TCP);
      conf.setPort(5402);
      conf.setHost("localhost");
      conf.setSSLEnabled(true);
      conf.setKeyStorePath("messaging.keystore");
      conf.setKeyStorePassword("secureexample");
      conf.setTrustStorePath("messaging.truststore");
      conf.setTrustStorePassword("secureexample");
      try
      {
         RemotingService remotingService = EasyMock.createStrictMock(RemotingService.class);
         CleanUpNotifier cleanUpNotifier = EasyMock.createStrictMock(CleanUpNotifier.class);
         PacketDispatcher packetDispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
         EasyMock.expect(remotingService.getConfiguration()).andReturn(conf).anyTimes();
         EasyMock.expect(remotingService.getDispatcher()).andReturn(packetDispatcher);
         EasyMock.expect(remotingService.getConfiguration()).andReturn(conf).anyTimes();
         remotingService.registerPinger((RemotingSession) EasyMock.anyObject());
         remotingService.unregisterPinger(EasyMock.anyLong());
         cleanUpNotifier.fireCleanup(EasyMock.anyLong(), (MessagingException) EasyMock.isNull());
         EasyMock.replay(remotingService, cleanUpNotifier);
         acceptor.startAccepting(remotingService, cleanUpNotifier);
         ConnectionParams connectionParams = new ConnectionParamsImpl();
         connectionParams.setSSLEnabled(true);
         connectionParams.setKeyStorePath("messaging.keystore");
         connectionParams.setKeyStorePassword("secureexample");
         connectionParams.setTrustStorePath("messaging.truststore");
         connectionParams.setTrustStorePassword("secureexample");
         MinaConnector minaConnector = new MinaConnector(conf.getLocation(), connectionParams, new PacketDispatcherImpl(null));
         minaConnector.connect();
         minaConnector.getDispatcher().dispatch(new Ping(), null);
         // TODO investigate why we need to wait a little bit before disconnecting to avoid a SSL exception
         Thread.sleep(500);
         minaConnector.disconnect();
         EasyMock.verify(remotingService, cleanUpNotifier);
      }
      catch(Exception e)
      {
         e.printStackTrace();
      }
      finally
      {
         acceptor.stopAccepting();
      }
   }
}
