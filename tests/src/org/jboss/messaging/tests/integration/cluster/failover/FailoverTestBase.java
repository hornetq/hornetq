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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.cluster.BroadcastGroupConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory;
import org.jboss.messaging.core.remoting.impl.invm.InVMRegistry;
import org.jboss.messaging.core.remoting.impl.invm.TransportConstants;
import org.jboss.messaging.core.server.JournalType;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.utils.Pair;

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

   protected MessagingService liveService;

   protected MessagingService backupService;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   protected ClientSessionFactory createFailoverFactory()
   {
      return new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                          new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                     backupParams));
   }

   protected ClientSessionFactory createBackupFactory()
   {
      return new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                     backupParams));
   }

   protected void setUpFileBased(final long maxGlobalSize) throws Exception
   {
      setUpFileBased(maxGlobalSize, 20 * 1024);
   }

   protected void setUpFileBased(final long maxGlobalSize, final int pageSize) throws Exception
   {
      setUpFailoverServers(true, maxGlobalSize, pageSize);
   }

   /*
    * 

    */

   protected void setUpFailoverServers(boolean fileBased,
                                       final long maxGlobalSize,
                                       final int pageSize) throws Exception
   {
      deleteDirectory(new File(getTestDir()));

      Configuration backupConf = new ConfigurationImpl();
      backupConf.setSecurityEnabled(false);
      backupConf.setClustered(true);
      backupConf.setBackup(true);
      backupParams.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      backupConf.getAcceptorConfigurations()
                .add(new TransportConfiguration(InVMAcceptorFactory.class.getCanonicalName(), backupParams));

      if (fileBased)
      {
         clearData(getTestDir() + "/backup");

         backupConf.setJournalDirectory(getJournalDir(getTestDir() + "/backup"));
         backupConf.setLargeMessagesDirectory(getLargeMessagesDir(getTestDir() + "/backup"));
         backupConf.setBindingsDirectory(getBindingsDir(getTestDir() + "/backup"));
         backupConf.setPagingDirectory(getPageDir(getTestDir() + "/backup"));
         backupConf.setJournalFileSize(100 * 1024);

         backupConf.setJournalType(JournalType.NIO);

         backupConf.setPagingMaxGlobalSizeBytes(maxGlobalSize);
         backupConf.setPagingGlobalWatermarkSize(pageSize);
         backupService = Messaging.newMessagingService(backupConf);
      }
      else
      {
         backupService = Messaging.newNullStorageMessagingService(backupConf);
      }

      backupService.start();

      Configuration liveConf = new ConfigurationImpl();
      liveConf.setSecurityEnabled(false);
      liveConf.setClustered(true);

      TransportConfiguration liveTC = new TransportConfiguration(InVMAcceptorFactory.class.getCanonicalName());
      liveConf.getAcceptorConfigurations().add(liveTC);

      Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();

      TransportConfiguration backupTC = new TransportConfiguration(INVM_CONNECTOR_FACTORY,
                                                                   backupParams,
                                                                   "backup-connector");
      connectors.put(backupTC.getName(), backupTC);
      liveConf.setConnectorConfigurations(connectors);
      liveConf.setBackupConnectorName(backupTC.getName());

      if (fileBased)
      {
         liveConf.setJournalDirectory(getJournalDir(getTestDir() + "/live"));
         liveConf.setLargeMessagesDirectory(getLargeMessagesDir(getTestDir() + "/live"));
         liveConf.setBindingsDirectory(getBindingsDir(getTestDir() + "/live"));
         liveConf.setPagingDirectory(getPageDir(getTestDir() + "/live"));

         liveConf.setPagingMaxGlobalSizeBytes(maxGlobalSize);
         liveConf.setPagingGlobalWatermarkSize(pageSize);
         liveConf.setJournalFileSize(100 * 1024);

         liveConf.setJournalType(JournalType.NIO);
      }

      if (fileBased)
      {
         liveService = Messaging.newMessagingService(liveConf);
      }
      else
      {
         liveService = Messaging.newNullStorageMessagingService(liveConf);
      }

      AddressSettings settings = new AddressSettings();
      settings.setPageSizeBytes(pageSize);

      liveService.getServer().getAddressSettingsRepository().addMatch("#", settings);
      backupService.getServer().getAddressSettingsRepository().addMatch("#", settings);

      clearData(getTestDir() + "/live");

      liveService.start();
   }
   
   protected void setupGroupServers(boolean fileBased, String bcGroupName, int localBindPort, String groupAddress, int groupPort) throws Exception
   {
      Configuration backupConf = new ConfigurationImpl();
      backupConf.setSecurityEnabled(false);
      backupConf.setClustered(true);
      backupParams.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      backupConf.getAcceptorConfigurations()
                .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory",
                                                backupParams));
      backupConf.setBackup(true);
      
      if (fileBased)
      {
         clearData(getTestDir() + "/backup");

         backupConf.setJournalDirectory(getJournalDir(getTestDir() + "/backup"));
         backupConf.setLargeMessagesDirectory(getLargeMessagesDir(getTestDir() + "/backup"));
         backupConf.setBindingsDirectory(getBindingsDir(getTestDir() + "/backup"));
         backupConf.setPagingDirectory(getPageDir(getTestDir() + "/backup"));
         backupConf.setJournalFileSize(100 * 1024);

         backupConf.setJournalType(JournalType.NIO);

         backupConf.setPagingMaxGlobalSizeBytes(-1);
         backupConf.setPagingGlobalWatermarkSize(-1);
         backupService = Messaging.newMessagingService(backupConf);
      }
      else
      {
         backupService = Messaging.newNullStorageMessagingService(backupConf);
      }
      
      backupService.start();

      Configuration liveConf = new ConfigurationImpl();
      liveConf.setSecurityEnabled(false);
      TransportConfiguration liveTC = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory");
      liveConf.getAcceptorConfigurations()
              .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory"));
      Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();
      TransportConfiguration backupTC = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                   backupParams);
      connectors.put(backupTC.getName(), backupTC);
      connectors.put(liveTC.getName(), liveTC);
      liveConf.setConnectorConfigurations(connectors);
      liveConf.setBackupConnectorName(backupTC.getName());
      liveConf.setClustered(true);

      List<Pair<String, String>> connectorNames = new ArrayList<Pair<String, String>>();
      connectorNames.add(new Pair<String, String>(liveTC.getName(), backupTC.getName()));

      final long broadcastPeriod = 250;

      BroadcastGroupConfiguration bcConfig1 = new BroadcastGroupConfiguration(bcGroupName,
                                                                              localBindPort,
                                                                              groupAddress,
                                                                              groupPort,
                                                                              broadcastPeriod,
                                                                              connectorNames);

      List<BroadcastGroupConfiguration> bcConfigs1 = new ArrayList<BroadcastGroupConfiguration>();
      bcConfigs1.add(bcConfig1);
      liveConf.setBroadcastGroupConfigurations(bcConfigs1);

      if (fileBased)
      {
         liveConf.setJournalDirectory(getJournalDir(getTestDir() + "/live"));
         liveConf.setLargeMessagesDirectory(getLargeMessagesDir(getTestDir() + "/live"));
         liveConf.setBindingsDirectory(getBindingsDir(getTestDir() + "/live"));
         liveConf.setPagingDirectory(getPageDir(getTestDir() + "/live"));

         liveConf.setPagingMaxGlobalSizeBytes(-1);
         liveConf.setPagingGlobalWatermarkSize(-1);
         liveConf.setJournalFileSize(100 * 1024);

         liveConf.setJournalType(JournalType.NIO);
         liveService = Messaging.newMessagingService(liveConf);
      }
      else
      {
         liveService = Messaging.newNullStorageMessagingService(liveConf);
      }

      liveService = Messaging.newNullStorageMessagingService(liveConf);
      liveService.start();

   }
   

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void tearDown() throws Exception
   {
      stopServers();

      super.tearDown();
   }

   protected void stopServers() throws Exception
   {
      if (backupService != null && backupService.isStarted())
      {
         backupService.stop();

         backupService = null;
      }

      if (liveService != null && liveService.isStarted())
      {
         liveService.stop();

         liveService = null;

      }

      assertEquals(0, InVMRegistry.instance.size());
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
