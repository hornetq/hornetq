/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.tests.integration.client;
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.remoting.impl.netty.NettyConnector;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;
import org.jboss.netty.bootstrap.ClientBootstrap;

/**
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 */
public class NettyConnectorTest extends ServiceTestBase
{
   private HornetQServer server;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      Configuration config = this.createDefaultConfig(true);
      server = this.createServer(false, config);
      server.start();
   }

   //make sure the 'connect-timeout' passed to netty.
   @Test
   public void testConnectionTimeoutConfig() throws Exception
   {
      final int timeout = 23456;
      TransportConfiguration transport = new TransportConfiguration(NETTY_CONNECTOR_FACTORY);
      transport.getParams().put(TransportConstants.NETTY_CONNECT_TIMEOUT, timeout);
      
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(transport);

      ClientSessionFactoryImpl factory = (ClientSessionFactoryImpl) locator.createSessionFactory();
      NettyConnector connector = (NettyConnector) factory.getConnector();
      
      ClientBootstrap bootstrap = connector.getBootStrap();
      
      assertEquals(timeout, bootstrap.getOption("connectTimeoutMillis"));
      
      factory.close();
      locator.close();
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      super.tearDown();
      server.stop();
   }
}
