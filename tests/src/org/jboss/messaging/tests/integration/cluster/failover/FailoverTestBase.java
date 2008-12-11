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

package org.jboss.messaging.tests.integration.cluster.failover;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory;
import org.jboss.messaging.core.remoting.impl.invm.InVMRegistry;
import org.jboss.messaging.core.remoting.impl.invm.TransportConstants;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;
import org.jboss.messaging.tests.util.ServiceTestBase;

/**
 * A FailoverTestBase
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Dec 8, 2008 6:59:53 PM
 *
 *
 */
public class FailoverTestBase extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   protected final Map<String, Object> backupParams = new HashMap<String, Object>();

   private MessagingService liveService;

   private MessagingService backupService;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   protected ClientSessionFactory createFailoverFactory()
   {
      return new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                          new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                     backupParams));
   }

   protected void setUpFileBased() throws Exception
   {

      deleteDirectory(new File(getTestDir()));

      Configuration backupConf = new ConfigurationImpl();

      backupConf.setJournalDirectory(getJournalDir(getTestDir() + "/backup"));
      backupConf.setLargeMessagesDirectory(getLargeMessagesDir(getTestDir() + "/backup"));
      backupConf.setBindingsDirectory(getBindingsDir(getTestDir() + "/backup"));
      backupConf.setPagingDirectory(getPageDir(getTestDir() + "/backup"));
      backupConf.setJournalFileSize(100 * 1024);

      backupConf.setPagingMaxGlobalSizeBytes(30 * 1024);
      backupConf.setPagingDefaultSize(10 * 1024);

      backupConf.setSecurityEnabled(false);
      backupParams.put(TransportConstants.SERVER_ID_PROP_NAME, 1);

      backupConf.getAcceptorConfigurations()
                .add(new TransportConfiguration(InVMAcceptorFactory.class.getCanonicalName(), backupParams));
      backupConf.setBackup(true);

      clearData(getTestDir() + "/backup");

      backupService = MessagingServiceImpl.newMessagingService(backupConf);
      backupService.start();

      Configuration liveConf = new ConfigurationImpl();

      liveConf.setJournalDirectory(getJournalDir(getTestDir() + "/live"));
      liveConf.setLargeMessagesDirectory(getLargeMessagesDir(getTestDir() + "/live"));
      liveConf.setBindingsDirectory(getBindingsDir(getTestDir() + "/live"));
      liveConf.setPagingDirectory(getPageDir(getTestDir() + "/live"));

      liveConf.setPagingMaxGlobalSizeBytes(30 * 1024);
      liveConf.setPagingDefaultSize(10 * 1024);
      liveConf.setJournalFileSize(100 * 1024);

      liveConf.setSecurityEnabled(false);
      liveConf.getAcceptorConfigurations()
              .add(new TransportConfiguration(InVMAcceptorFactory.class.getCanonicalName()));

      Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();

      TransportConfiguration backupTC = new TransportConfiguration(INVM_CONNECTOR_FACTORY,
                                                                   backupParams,
                                                                   "backup-connector");
      connectors.put(backupTC.getName(), backupTC);
      liveConf.setConnectorConfigurations(connectors);
      liveConf.setBackupConnectorName(backupTC.getName());
      liveService = MessagingServiceImpl.newMessagingService(liveConf);

      clearData(getTestDir() + "/live");

      liveService.start();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void tearDown() throws Exception
   {
      assertEquals(0, backupService.getServer().getRemotingService().getConnections().size());

      backupService.stop();

      assertEquals(0, liveService.getServer().getRemotingService().getConnections().size());

      liveService.stop();

      assertEquals(0, InVMRegistry.instance.size());
      
      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
