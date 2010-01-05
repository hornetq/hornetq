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

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.hornetq.SimpleString;
import org.hornetq.core.client.ClientSessionFactoryImpl;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.remoting.impl.invm.InVMConnector;
import org.hornetq.core.remoting.impl.invm.InVMRegistry;
import org.hornetq.core.remoting.impl.invm.TransportConstants;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * A FailoverTestBase
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
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

   /**
    * @param name
    */
   public FailoverTestBase(final String name)
   {
      super(name);
   }

   public FailoverTestBase()
   {
   }

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      createConfigs();

      if (server1Service != null)
      {
         server1Service.start();
      }

      server0Service.start();
   }

   /**
    * @throws Exception
    */
   protected void createConfigs() throws Exception
   {
      Configuration config1 = super.createDefaultConfig();
      config1.getAcceptorConfigurations().clear();
      config1.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(false));
      config1.setSecurityEnabled(false);
      config1.setSharedStore(true);
      config1.setBackup(true);
      server1Service = createServer(true, config1);

      Configuration config0 = super.createDefaultConfig();
      config0.getAcceptorConfigurations().clear();
      config0.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(true));
      config0.setSecurityEnabled(false);
      config0.setSharedStore(true);
      server0Service = createServer(true, config0);

   }

   protected void createReplicatedConfigs() throws Exception
   {
      Configuration config1 = super.createDefaultConfig();
      config1.setBindingsDirectory(config1.getBindingsDirectory() + "_backup");
      config1.setJournalDirectory(config1.getJournalDirectory() + "_backup");
      config1.setPagingDirectory(config1.getPagingDirectory() + "_backup");
      config1.setLargeMessagesDirectory(config1.getLargeMessagesDirectory() + "_backup");
      config1.getAcceptorConfigurations().clear();
      config1.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(false));
      config1.setSecurityEnabled(false);
      config1.setSharedStore(false);
      config1.setBackup(true);
      server1Service = super.createServer(true, config1);

      Configuration config0 = super.createDefaultConfig();
      config0.getAcceptorConfigurations().clear();
      config0.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(true));

      config0.getConnectorConfigurations().put("toBackup", getConnectorTransportConfiguration(false));
      config0.setBackupConnectorName("toBackup");
      config0.setSecurityEnabled(false);
      config0.setSharedStore(false);
      server0Service = super.createServer(true, config0);

      server1Service.start();
      server0Service.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      server1Service.stop();

      server0Service.stop();

      Assert.assertEquals(0, InVMRegistry.instance.size());

      server1Service = null;

      server0Service = null;

      InVMConnector.failOnCreateConnection = false;

      super.tearDown();
   }

   protected TransportConfiguration getInVMConnectorTransportConfiguration(final boolean live)
   {
      if (live)
      {
         return new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory");
      }
      else
      {
         Map<String, Object> server1Params = new HashMap<String, Object>();

         server1Params.put(TransportConstants.SERVER_ID_PROP_NAME, 1);

         return new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory", server1Params);
      }
   }

   protected TransportConfiguration getInVMTransportAcceptorConfiguration(final boolean live)
   {
      if (live)
      {
         return new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory");
      }
      else
      {
         Map<String, Object> server1Params = new HashMap<String, Object>();

         server1Params.put(TransportConstants.SERVER_ID_PROP_NAME, 1);

         return new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory", server1Params);
      }
   }

   protected TransportConfiguration getNettyAcceptorTransportConfiguration(final boolean live)
   {
      if (live)
      {
         return new TransportConfiguration("org.hornetq.integration.transports.netty.NettyAcceptorFactory");
      }
      else
      {
         Map<String, Object> server1Params = new HashMap<String, Object>();

         server1Params.put(org.hornetq.integration.transports.netty.TransportConstants.PORT_PROP_NAME,
                           org.hornetq.integration.transports.netty.TransportConstants.DEFAULT_PORT + 1);

         return new TransportConfiguration("org.hornetq.integration.transports.netty.NettyAcceptorFactory",
                                           server1Params);
      }
   }

   protected TransportConfiguration getNettyConnectorTransportConfiguration(final boolean live)
   {
      if (live)
      {
         return new TransportConfiguration("org.hornetq.integration.transports.netty.NettyConnectorFactory");
      }
      else
      {
         Map<String, Object> server1Params = new HashMap<String, Object>();

         server1Params.put(org.hornetq.integration.transports.netty.TransportConstants.PORT_PROP_NAME,
                           org.hornetq.integration.transports.netty.TransportConstants.DEFAULT_PORT + 1);

         return new TransportConfiguration("org.hornetq.integration.transports.netty.NettyConnectorFactory",
                                           server1Params);
      }
   }

   protected abstract TransportConfiguration getAcceptorTransportConfiguration(boolean live);

   protected abstract TransportConfiguration getConnectorTransportConfiguration(final boolean live);

   protected ClientSessionFactoryInternal getSessionFactory()
   {
      return new ClientSessionFactoryImpl(getConnectorTransportConfiguration(true),
                                          getConnectorTransportConfiguration(false));
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
