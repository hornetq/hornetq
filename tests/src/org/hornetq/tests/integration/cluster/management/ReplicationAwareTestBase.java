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

package org.hornetq.tests.integration.cluster.management;

import java.util.HashMap;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.remoting.impl.invm.TransportConstants;
import org.hornetq.core.server.Messaging;
import org.hornetq.core.server.MessagingServer;
import org.hornetq.tests.util.UnitTestCase;

/**
 * A ReplicationAwareAddressControlWrapperTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public abstract class ReplicationAwareTestBase extends UnitTestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   protected MessagingServer liveServer;

   protected MessagingServer backupServer;

   protected Map<String, Object> backupParams = new HashMap<String, Object>();

   protected MBeanServer liveMBeanServer;

   protected MBeanServer backupMBeanServer;

   // Static --------------------------------------------------------

   protected static void assertResourceExists(MBeanServer mbeanServer, ObjectName objectName)
   {
      boolean registered = mbeanServer.isRegistered(objectName);
      if (!registered)
      {
         fail("Resource does not exist: " + objectName);
      }
   }

   protected static void assertResourceNotExists(MBeanServer mbeanServer, ObjectName objectName)
   {
      boolean registered = mbeanServer.isRegistered(objectName);
      if (registered)
      {
         fail("Resource exists: " + objectName);
      }
   }
   
   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      
      doSetup(true);
   }
   
   protected void doSetup(boolean startServers) throws Exception
   {
      super.setUp();
      
      backupMBeanServer = MBeanServerFactory.createMBeanServer();
      liveMBeanServer = MBeanServerFactory.createMBeanServer();

      assertTrue(backupMBeanServer != liveMBeanServer);
      
      Configuration backupConf = new ConfigurationImpl();
      backupConf.setPersistenceEnabled(false);
      backupConf.setSecurityEnabled(false);
      backupParams.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      backupConf.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName(),
                                                                            backupParams));
      backupConf.setBackup(true);
      backupConf.setJMXManagementEnabled(true);
      backupServer = Messaging.newMessagingServer(backupConf, backupMBeanServer, false);
      if (startServers)
      {
         backupServer.start();
      }

      Configuration liveConf = new ConfigurationImpl();
      liveConf.setPersistenceEnabled(false);
      liveConf.setSecurityEnabled(false);
      liveConf.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();
      TransportConfiguration backupTC = new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                                   backupParams,
                                                                   "backup-connector");
      connectors.put(backupTC.getName(), backupTC);      
      liveConf.setConnectorConfigurations(connectors);
      liveConf.setBackupConnectorName(backupTC.getName());
      liveConf.setJMXManagementEnabled(true);
      liveServer = Messaging.newMessagingServer(liveConf, liveMBeanServer, false);
      if (startServers)
      {
         liveServer.start();
      }
   }
   
   

   @Override
   protected void tearDown() throws Exception
   {
      liveServer.stop();

      backupServer.stop();
      
      MBeanServerFactory.releaseMBeanServer(backupMBeanServer);
      
      MBeanServerFactory.releaseMBeanServer(liveMBeanServer);
      
      backupServer = null;
      
      liveServer = null;
      
      backupParams = null;
      
      liveMBeanServer = null;
      
      backupMBeanServer = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

}
