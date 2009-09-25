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

package org.hornetq.tests.integration.cluster.failover;

import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.remoting.impl.invm.InVMConnector;
import org.hornetq.core.remoting.impl.invm.InVMRegistry;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.SimpleString;

/**
 * A FailoverTestBase
 *
 * @author tim
 *
 *
 */
public abstract class FailoverTestBase extends ServiceTestBase
{
   // Constants -----------------------------------------------------
   
   protected static final SimpleString ADDRESS = new SimpleString("FailoverTestAddress");

   // Attributes ----------------------------------------------------
   
   protected HornetQServer server0Service;

   protected HornetQServer server1Service;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
   protected void setUp() throws Exception
   {
      super.setUp();
      
      Configuration config1 = super.createDefaultConfig();
      config1.getAcceptorConfigurations().clear();
      config1.getAcceptorConfigurations()
             .add(getAcceptorTransportConfiguration(false));
      config1.setSecurityEnabled(false);
      config1.setSharedStore(true);
      config1.setBackup(true);
      server1Service = super.createServer(true, config1);

      Configuration config0 = super.createDefaultConfig();
      config0.getAcceptorConfigurations().clear();
      config0.getAcceptorConfigurations()
             .add(getAcceptorTransportConfiguration(true));
      config0.setSecurityEnabled(false);
      config0.setSharedStore(true);
      server0Service = super.createServer(true, config0);

      server1Service.start();
      server0Service.start();
   }
   
   
   protected void tearDown() throws Exception
   {
      server1Service.stop();

      server0Service.stop();

      assertEquals(0, InVMRegistry.instance.size());

      server1Service = null;

      server0Service = null;
      
      InVMConnector.failOnCreateConnection = false;

      super.tearDown();
   }
   
   protected abstract TransportConfiguration getAcceptorTransportConfiguration(boolean live);
   
   protected abstract TransportConfiguration getConnectorTransportConfiguration(final boolean live);
   
   protected ClientSessionFactoryInternal getSessionFactory()
   {
      return new ClientSessionFactoryImpl(getConnectorTransportConfiguration(true), getConnectorTransportConfiguration(false));
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
