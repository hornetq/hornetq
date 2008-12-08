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

import junit.framework.TestCase;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.management.ManagementService;
import org.jboss.messaging.core.management.impl.ManagementServiceImpl;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.persistence.impl.nullpm.NullStorageManager;
import org.jboss.messaging.core.remoting.RemotingService;
import org.jboss.messaging.core.remoting.impl.RemotingServiceImpl;
import org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.core.remoting.impl.invm.InVMRegistry;
import org.jboss.messaging.core.remoting.impl.invm.TransportConstants;
import org.jboss.messaging.core.security.JBMSecurityManager;
import org.jboss.messaging.core.security.impl.JBMSecurityManagerImpl;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServerImpl;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;

/**
 * A ReplicationAwareAddressControlWrapperTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ReplicationAwareTestBase extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   protected MessagingService liveService;

   protected MessagingService backupService;

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

   protected static MessagingService createNullStorageMessagingServer(final Configuration config, MBeanServer mbeanServer)
   {
      StorageManager storageManager = new NullStorageManager();
      
      RemotingService remotingService = new RemotingServiceImpl(config);

      JBMSecurityManager securityManager = new JBMSecurityManagerImpl(true);
      
      ManagementService managementService = new ManagementServiceImpl(mbeanServer, config.isJMXManagementEnabled());
      
      MessagingServer server = new MessagingServerImpl();
      
      server.setConfiguration(config);
      
      server.setStorageManager(storageManager);
      
      server.setRemotingService(remotingService);
      
      server.setSecurityManager(securityManager);
      
      server.setManagementService(managementService);
      
      return new MessagingServiceImpl(server, storageManager, remotingService);
   }
   
   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      backupMBeanServer = MBeanServerFactory.createMBeanServer();
      Configuration backupConf = new ConfigurationImpl();
      backupConf.setSecurityEnabled(false);
      backupParams.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      backupConf.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName(),
                                                                            backupParams));
      backupConf.setBackup(true);
      backupConf.setJMXManagementEnabled(true);
      backupService = createNullStorageMessagingServer(backupConf, backupMBeanServer);
      backupService.start();

      liveMBeanServer = MBeanServerFactory.createMBeanServer();
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
      liveService = createNullStorageMessagingServer(liveConf, liveMBeanServer);
      liveService.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      assertEquals(0, backupService.getServer().getRemotingService().getConnections().size());

      backupService.stop();

      assertEquals(0, liveService.getServer().getRemotingService().getConnections().size());

      liveService.stop();

      assertEquals(0, InVMRegistry.instance.size());
   }

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

}
