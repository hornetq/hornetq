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

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.cluster.BroadcastGroupConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMRegistry;
import org.hornetq.core.remoting.impl.invm.TransportConstants;
import org.hornetq.core.server.JournalType;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.Pair;

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

   protected Map<String, Object> backupParams = new HashMap<String, Object>();

   protected HornetQServer liveServer;

   protected HornetQServer backupServer;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   protected ClientSessionFactory createFailoverFactory()
   {
      return new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                          new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                     backupParams));
   }

   protected ClientSessionFactory createBackupFactory()
   {
      return new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                     backupParams));
   }

   protected void setUpFileBased(final int maxGlobalSize) throws Exception
   {
      setUpFileBased(maxGlobalSize, 20 * 1024);
   }

   protected void setUpFileBased(final int maxGlobalSize, final int pageSize) throws Exception
   {
      setUpFailoverServers(true, maxGlobalSize, pageSize);
   }

   /*
    * 

    */

   protected void setUpFailoverServers(boolean fileBased,
                                       final int maxAddressSize,
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
         backupConf.setJournalCompactPercentage(0);

         backupConf.setJournalType(JournalType.ASYNCIO);
         
         backupServer = HornetQ.newHornetQServer(backupConf);
      }
      else
      {
         backupServer = HornetQ.newHornetQServer(backupConf, false);
      }

      backupServer.start();
      
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
         liveConf.setJournalCompactMinFiles(0);
         
         liveConf.setJournalFileSize(100 * 1024);

         liveConf.setJournalType(JournalType.ASYNCIO);
      }

      if (fileBased)
      {
         liveServer = HornetQ.newHornetQServer(liveConf);
      }
      else
      {
         liveServer = HornetQ.newHornetQServer(liveConf, false);
      }

      AddressSettings settings = new AddressSettings();
      settings.setPageSizeBytes(pageSize);
      settings.setMaxSizeBytes(maxAddressSize);
      settings.setPageSizeBytes(pageSize);

      liveServer.getAddressSettingsRepository().addMatch("#", settings);
      backupServer.getAddressSettingsRepository().addMatch("#", settings);

      clearData(getTestDir() + "/live");

      liveServer.start();
   }
   
   protected void setupGroupServers(boolean fileBased, String bcGroupName, int localBindPort, String groupAddress, int groupPort) throws Exception
   {
      Configuration backupConf = new ConfigurationImpl();
      backupConf.setSecurityEnabled(false);
      backupConf.setClustered(true);
      backupParams.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      backupConf.getAcceptorConfigurations()
                .add(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory",
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

         backupConf.setJournalType(JournalType.ASYNCIO);

         backupServer = HornetQ.newHornetQServer(backupConf);
         
      }
      else
      {
         backupServer = HornetQ.newHornetQServer(backupConf, false);
      }
      
      backupServer.start();

      Configuration liveConf = new ConfigurationImpl();
      liveConf.setSecurityEnabled(false);
      TransportConfiguration liveTC = new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory");
      liveConf.getAcceptorConfigurations()
              .add(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory"));
      Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();
      TransportConfiguration backupTC = new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
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
                                                                              null,
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

         liveConf.setJournalFileSize(100 * 1024);

         liveConf.setJournalType(JournalType.ASYNCIO);
         liveServer = HornetQ.newHornetQServer(liveConf);
      }
      else
      {
         liveServer = HornetQ.newHornetQServer(liveConf, false);
      }

      liveServer = HornetQ.newHornetQServer(liveConf, false);
      liveServer.start();

   }
   

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void tearDown() throws Exception
   {
      stopServers();
      
      backupServer = null;
      
      liveServer = null;
      
      backupParams = null;

      super.tearDown();
   }

   protected void stopServers() throws Exception
   {
      if (backupServer != null && backupServer.isStarted())
      {
         backupServer.stop();        
      }

      if (liveServer != null && liveServer.isStarted())
      {
         liveServer.stop();        
      }

      assertEquals(0, InVMRegistry.instance.size());
      
      backupServer = null;
      
      liveServer = null;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
