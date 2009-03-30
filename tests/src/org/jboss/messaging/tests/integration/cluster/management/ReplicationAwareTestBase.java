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

package org.jboss.messaging.tests.integration.cluster.management;

import java.util.HashMap;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.core.remoting.impl.invm.InVMRegistry;
import org.jboss.messaging.core.remoting.impl.invm.TransportConstants;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * A ReplicationAwareAddressControlWrapperTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ReplicationAwareTestBase extends UnitTestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   protected MessagingServer liveServer;

   protected MessagingServer backupServer;

   protected final Map<String, Object> backupParams = new HashMap<String, Object>();

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
      
      backupMBeanServer = MBeanServerFactory.createMBeanServer();
      liveMBeanServer = MBeanServerFactory.createMBeanServer();

      assertTrue(backupMBeanServer != liveMBeanServer);
      
      Configuration backupConf = new ConfigurationImpl();
      backupConf.setSecurityEnabled(false);
      backupParams.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      backupConf.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName(),
                                                                            backupParams));
      backupConf.setBackup(true);
      backupConf.setJMXManagementEnabled(true);
      backupServer = Messaging.newNullStorageMessagingServer(backupConf, backupMBeanServer);
      backupServer.start();

      Configuration liveConf = new ConfigurationImpl();
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
      liveServer = Messaging.newNullStorageMessagingServer(liveConf, liveMBeanServer);
      liveServer.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      backupServer.stop();

      liveServer.stop();

      super.tearDown();
   }

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

}
