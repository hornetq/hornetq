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

package org.hornetq.tests.util;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.MBeanServer;

import junit.framework.Assert;

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.client.impl.Topology;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.journal.PreparedTransactionInfo;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.journal.impl.JournalFile;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.core.journal.impl.JournalReaderCallback;
import org.hornetq.core.journal.impl.NIOSequentialFileFactory;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.Bindings;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.postoffice.QueueBinding;
import org.hornetq.core.postoffice.impl.LocalQueueBinding;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.remoting.impl.invm.InVMRegistry;
import org.hornetq.core.remoting.impl.invm.TransportConstants;
import org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.core.server.NodeManager;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.cluster.ClusterConnection;
import org.hornetq.core.server.cluster.RemoteQueueBinding;
import org.hornetq.core.server.impl.HornetQServerImpl;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.jms.client.HornetQBytesMessage;
import org.hornetq.spi.core.security.HornetQSecurityManager;
import org.hornetq.spi.core.security.HornetQSecurityManagerImpl;
import org.hornetq.utils.UUIDGenerator;

/**
 *
 * Base class with basic utilities on starting up a basic server
 *
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public abstract class ServiceTestBase extends UnitTestCase
{

   // Constants -----------------------------------------------------

   protected static final long WAIT_TIMEOUT = 10000;


   // Attributes ----------------------------------------------------

   protected static final String INVM_ACCEPTOR_FACTORY = InVMAcceptorFactory.class.getCanonicalName();

   public static final String INVM_CONNECTOR_FACTORY = InVMConnectorFactory.class.getCanonicalName();

   protected static final String NETTY_ACCEPTOR_FACTORY = NettyAcceptorFactory.class.getCanonicalName();

   protected static final String NETTY_CONNECTOR_FACTORY = NettyConnectorFactory.class.getCanonicalName();

   private List<ServerLocator> locators = new ArrayList<ServerLocator>();

   @Override
   protected void tearDown() throws Exception
   {
      for (ServerLocator locator : locators)
      {
         try
         {
            locator.close();
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }
      }
      locators.clear();
      super.tearDown();
      if (InVMRegistry.instance.size() > 0)
      {
         fail("InVMREgistry size > 0");
      }
   }

   /**
    * @param queue
    * @throws InterruptedException
    */
   protected void waitForNotPaging(Queue queue) throws InterruptedException
   {
      waitForNotPaging(queue.getPageSubscription().getPagingStore());
   }

   protected void waitForNotPaging(PagingStore store) throws InterruptedException
   {
      long timeout = System.currentTimeMillis() + 10000;
      while (timeout > System.currentTimeMillis() && store.isPaging())
      {
         Thread.sleep(100);
      }
      assertFalse(store.isPaging());
   }

   protected void waitForTopology(final HornetQServer server, final int nodes) throws Exception
   {
      waitForTopology(server, nodes, WAIT_TIMEOUT);
   }

   protected void waitForTopology(final HornetQServer server, final int nodes, final long timeout) throws Exception
   {
      log.debug("waiting for " + nodes + " on the topology for server = " + server);

      long start = System.currentTimeMillis();

      Set<ClusterConnection> ccs = server.getClusterManager().getClusterConnections();

      if (ccs.size() != 1)
      {
         throw new IllegalStateException("You need a single cluster connection on this version of waitForTopology on ServiceTestBase, it had " + ccs.size());
      }

      Topology topology = ccs.iterator().next().getTopology();

      do
      {
         if (nodes == topology.getMembers().size())
         {
            return;
         }

         Thread.sleep(10);
      }
      while (System.currentTimeMillis() - start < timeout);

      String msg = "Timed out waiting for cluster topology of " + nodes +
                   " (received " +
                   topology.getMembers().size() +
                   ") topology = " +
                   topology +
                   ")";

      log.error(msg);

      throw new Exception(msg);
   }


   protected void waitForTopology(final HornetQServer server, String clusterConnectionName, final int nodes, final long timeout) throws Exception
   {
      log.debug("waiting for " + nodes + " on the topology for server = " + server);

      long start = System.currentTimeMillis();

      ClusterConnection clusterConnection = server.getClusterManager().getClusterConnection(clusterConnectionName);


      Topology topology = clusterConnection.getTopology();

      do
      {
         if (nodes == topology.getMembers().size())
         {
            return;
         }

         Thread.sleep(10);
      }
      while (System.currentTimeMillis() - start < timeout);

      String msg = "Timed out waiting for cluster topology of " + nodes +
                   " (received " +
                   topology.getMembers().size() +
                   ") topology = " +
                   topology +
                   ")";

      log.error(msg);

      throw new Exception(msg);
   }


   protected static Map<String, Object> generateParams(final int node, final boolean netty)
   {
      Map<String, Object> params = new HashMap<String, Object>();

      if (netty)
      {
         params.put(org.hornetq.core.remoting.impl.netty.TransportConstants.PORT_PROP_NAME,
                    org.hornetq.core.remoting.impl.netty.TransportConstants.DEFAULT_PORT + node);
      }
      else
      {
         params.put(org.hornetq.core.remoting.impl.invm.TransportConstants.SERVER_ID_PROP_NAME, node);
      }

      return params;
   }

   protected static TransportConfiguration createTransportConfiguration(boolean netty,
                                                                        boolean acceptor,
                                                                        Map<String, Object> params)
   {
      String className;
      if (netty)
      {
         if (acceptor)
         {
            className = NettyAcceptorFactory.class.getName();
         }
         else
         {
            className = NettyConnectorFactory.class.getName();
         }
      }
      else
      {
         if (acceptor)
         {
            className = InVMAcceptorFactory.class.getName();
         }
         else
         {
            className = InVMConnectorFactory.class.getName();
         }
      }
      return new TransportConfiguration(className, params);
   }

   // Static --------------------------------------------------------
   private final Logger log = Logger.getLogger(this.getClass());

   // Constructors --------------------------------------------------

   public ServiceTestBase()
   {
      super();
   }

   public ServiceTestBase(final String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void waitForServer(HornetQServer server) throws InterruptedException
   {
      long timetowait = System.currentTimeMillis() + 5000;
      while (!server.isStarted() && System.currentTimeMillis() < timetowait)
      {
         Thread.sleep(100);
      }

      if (!server.isStarted())
      {
         log.info(threadDump("Server didn't start"));
         fail("server didnt start");
      }

      if (!server.getConfiguration().isBackup())
      {
         timetowait = System.currentTimeMillis() + 5000;
         while (!server.isInitialised() && System.currentTimeMillis() < timetowait)
         {
            Thread.sleep(100);
         }

         if (!server.isInitialised())
         {
            fail("Server didn't initialize");
         }
      }
   }

   protected void waitForBackupInitialize(HornetQServer server) throws InterruptedException
   {
      long timetowait = System.currentTimeMillis() + 5000;
      while (!server.isStarted() && System.currentTimeMillis() < timetowait)
      {
         Thread.sleep(100);
      }

      if (!server.isStarted())
      {
         log.info(threadDump("Server didn't start"));
         fail("server didnt start");
      }

      timetowait = System.currentTimeMillis() + 5000;
      while (!server.isInitialised() && System.currentTimeMillis() < timetowait)
      {
         Thread.sleep(100);
      }

      if (!server.isInitialised())
      {
         fail("Server didn't initialize");
      }
   }


   protected HornetQServer createServer(final boolean realFiles,
                                        final Configuration configuration,
                                        final int pageSize,
                                        final int maxAddressSize,
                                        final Map<String, AddressSettings> settings,
                                        final MBeanServer mbeanServer)
   {
      HornetQServer server;

      if (realFiles)
      {
         server = HornetQServers.newHornetQServer(configuration, mbeanServer, true);
      }
      else
      {
         server = HornetQServers.newHornetQServer(configuration, mbeanServer, false);
      }

      for (Map.Entry<String, AddressSettings> setting : settings.entrySet())
      {
         server.getAddressSettingsRepository().addMatch(setting.getKey(), setting.getValue());
      }

      AddressSettings defaultSetting = new AddressSettings();
      defaultSetting.setPageSizeBytes(pageSize);
      defaultSetting.setMaxSizeBytes(maxAddressSize);

      server.getAddressSettingsRepository().addMatch("#", defaultSetting);

      return server;
   }

   protected HornetQServer createServer(final boolean realFiles,
                                        final Configuration configuration,
                                        final int pageSize,
                                        final int maxAddressSize,
                                        final Map<String, AddressSettings> settings)
   {
      return createServer(realFiles, configuration, pageSize, maxAddressSize, AddressFullMessagePolicy.PAGE, settings);
   }

   protected HornetQServer createServer(final boolean realFiles,
                                        final Configuration configuration,
                                        final int pageSize,
                                        final int maxAddressSize,
                                        final AddressFullMessagePolicy fullPolicy,
                                        final Map<String, AddressSettings> settings)
   {
      HornetQServer server;

      if (realFiles)
      {
         server = HornetQServers.newHornetQServer(configuration);
      }
      else
      {
         server = HornetQServers.newHornetQServer(configuration, false);
      }

      if (settings != null)
      {
         for (Map.Entry<String, AddressSettings> setting : settings.entrySet())
         {
            server.getAddressSettingsRepository().addMatch(setting.getKey(), setting.getValue());
         }
      }

      AddressSettings defaultSetting = new AddressSettings();
      defaultSetting.setPageSizeBytes(pageSize);
      defaultSetting.setMaxSizeBytes(maxAddressSize);
      defaultSetting.setAddressFullMessagePolicy(fullPolicy);

      server.getAddressSettingsRepository().addMatch("#", defaultSetting);

      return server;
   }

   protected HornetQServer createServer(final boolean realFiles,
                                        final Configuration configuration,
                                        final MBeanServer mbeanServer,
                                        final Map<String, AddressSettings> settings)
   {
      HornetQServer server;

      if (realFiles)
      {
         server = HornetQServers.newHornetQServer(configuration, mbeanServer);
      }
      else
      {
         server = HornetQServers.newHornetQServer(configuration, mbeanServer, false);
      }

      for (Map.Entry<String, AddressSettings> setting : settings.entrySet())
      {
         server.getAddressSettingsRepository().addMatch(setting.getKey(), setting.getValue());
      }

      AddressSettings defaultSetting = new AddressSettings();
      server.getAddressSettingsRepository().addMatch("#", defaultSetting);

      return server;
   }

   protected HornetQServer createServer(final boolean realFiles)
   {
      return createServer(realFiles, false);
   }

   protected HornetQServer createServer(final boolean realFiles, final boolean netty)
   {
      return createServer(realFiles, createDefaultConfig(netty), -1, -1, new HashMap<String, AddressSettings>());
   }

   protected HornetQServer createServer(final boolean realFiles, final Configuration configuration)
   {
      return createServer(realFiles, configuration, -1, -1, new HashMap<String, AddressSettings>());
   }

   protected HornetQServer createInVMFailoverServer(final boolean realFiles,
                                                    final Configuration configuration,
                                                    final NodeManager nodeManager,
                                                    final int id)
   {
      return createInVMFailoverServer(realFiles,
                                      configuration,
                                      -1,
                                      -1,
                                      new HashMap<String, AddressSettings>(),
                                      nodeManager,
                                      id);
   }

   protected HornetQServer createInVMFailoverServer(final boolean realFiles,
                                                    final Configuration configuration,
                                                    final int pageSize,
                                                    final int maxAddressSize,
                                                    final Map<String, AddressSettings> settings,
                                                    NodeManager nodeManager,
                                                    final int id)
   {
      HornetQServer server;
      HornetQSecurityManager securityManager = new HornetQSecurityManagerImpl();
      configuration.setPersistenceEnabled(realFiles);
      server = new InVMNodeManagerServer(configuration,
                                         ManagementFactory.getPlatformMBeanServer(),
                                         securityManager,
                                         nodeManager);

      server.setIdentity("Server " + id);

      for (Map.Entry<String, AddressSettings> setting : settings.entrySet())
      {
         server.getAddressSettingsRepository().addMatch(setting.getKey(), setting.getValue());
      }

      AddressSettings defaultSetting = new AddressSettings();
      defaultSetting.setPageSizeBytes(pageSize);
      defaultSetting.setMaxSizeBytes(maxAddressSize);

      server.getAddressSettingsRepository().addMatch("#", defaultSetting);

      return server;
   }

   protected HornetQServer createServer(final boolean realFiles,
                                        final Configuration configuration,
                                        final HornetQSecurityManager securityManager)
   {
      HornetQServer server;

      if (realFiles)
      {
         server = HornetQServers.newHornetQServer(configuration,
                                                  ManagementFactory.getPlatformMBeanServer(),
                                                  securityManager);
      }
      else
      {
         server = HornetQServers.newHornetQServer(configuration,
                                                  ManagementFactory.getPlatformMBeanServer(),
                                                  securityManager,
                                                  false);
      }

      Map<String, AddressSettings> settings = new HashMap<String, AddressSettings>();

      for (Map.Entry<String, AddressSettings> setting : settings.entrySet())
      {
         server.getAddressSettingsRepository().addMatch(setting.getKey(), setting.getValue());
      }

      AddressSettings defaultSetting = new AddressSettings();

      server.getAddressSettingsRepository().addMatch("#", defaultSetting);

      return server;
   }

   protected HornetQServer createClusteredServerWithParams(final boolean isNetty,
                                                           final int index,
                                                           final boolean realFiles,
                                                           final Map<String, Object> params)
   {
      if (isNetty)
      {
         return createServer(realFiles,
                             createClusteredDefaultConfig(index, params, NETTY_ACCEPTOR_FACTORY),
                             -1,
                             -1,
                             new HashMap<String, AddressSettings>());
      }
      else
      {
         return createServer(realFiles,
                             createClusteredDefaultConfig(index, params, INVM_ACCEPTOR_FACTORY),
                             -1,
                             -1,
                             new HashMap<String, AddressSettings>());
      }
   }

   protected HornetQServer createClusteredServerWithParams(final boolean isNetty,
                                                           final int index,
                                                           final boolean realFiles,
                                                           final int pageSize,
                                                           final int maxAddressSize,
                                                           final Map<String, Object> params)
   {
      if (isNetty)
      {
         return createServer(realFiles,
                             createClusteredDefaultConfig(index, params, NETTY_ACCEPTOR_FACTORY),
                             pageSize,
                             maxAddressSize,
                             new HashMap<String, AddressSettings>());
      }
      else
      {
         return createServer(realFiles,
                             createClusteredDefaultConfig(index, params, INVM_ACCEPTOR_FACTORY),
                             -1,
                             -1,
                             new HashMap<String, AddressSettings>());
      }
   }

   protected ServerLocator createFactory(final boolean isNetty) throws Exception
   {
      if (isNetty)
      {
         return createNettyNonHALocator();
      }
      else
      {
         return createInVMNonHALocator();
      }
   }

   protected void createQueue(String address, String queue) throws Exception
   {
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory sf = locator.createSessionFactory();
      ClientSession session = sf.createSession();
      session.createQueue(address, queue);
      session.close();
      sf.close();
      locator.close();
   }

   protected ServerLocator createInVMNonHALocator()
   {
      return createNonHALocator(false);
   }

   protected ServerLocator createNettyNonHALocator()
   {
      return createNonHALocator(true);
   }

   protected ServerLocator createNonHALocator(final boolean isNetty)
   {
      ServerLocator locatorWithoutHA = isNetty ? HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(NETTY_CONNECTOR_FACTORY))
                                              : HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      locators.add(locatorWithoutHA);
      return locatorWithoutHA;
   }

   protected ServerLocator createInVMLocator(final int serverID)
   {
      TransportConfiguration tnspConfig = createInVMTransportConnectorConfig(serverID, UUIDGenerator.getInstance().generateStringUUID());

      return HornetQClient.createServerLocatorWithHA(tnspConfig);
   }

   /**
    * @param serverID
    * @return
    */
   protected TransportConfiguration createInVMTransportConnectorConfig(final int serverID, String name)
   {
      Map<String, Object> server1Params = new HashMap<String, Object>();

      if (serverID != 0)
      {
         server1Params.put(TransportConstants.SERVER_ID_PROP_NAME, serverID);
      }

      TransportConfiguration tnspConfig = new TransportConfiguration(INVM_CONNECTOR_FACTORY, server1Params, name);
      return tnspConfig;
   }

   protected ClientSessionFactoryImpl createFactory(final String connectorClass) throws Exception
   {
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(connectorClass));
      return (ClientSessionFactoryImpl)locator.createSessionFactory();

   }
   public String getTextMessage(final ClientMessage m)
   {
      m.getBodyBuffer().resetReaderIndex();
      return m.getBodyBuffer().readString();
   }

   protected ClientMessage createBytesMessage(final ClientSession session, final byte[] b, final boolean durable)
   {
      ClientMessage message = session.createMessage(HornetQBytesMessage.TYPE,
                                                    durable,
                                                    0,
                                                    System.currentTimeMillis(),
                                                    (byte)1);
      message.getBodyBuffer().writeBytes(b);
      return message;
   }



   /**
    * Reads a journal system and returns a Map<Integer,AtomicInteger> of recordTypes and the number of records per type,
    * independent of being deleted or not
    * @param config
    * @return
    * @throws Exception
    */
   protected Pair<List<RecordInfo>, List<PreparedTransactionInfo>> loadMessageJournal(Configuration config) throws Exception
   {
      JournalImpl messagesJournal = null;
      try
      {
         SequentialFileFactory messagesFF = new NIOSequentialFileFactory(getJournalDir(), null);

         messagesJournal = new JournalImpl(config.getJournalFileSize(),
                                           config.getJournalMinFiles(),
                                           0,
                                           0,
                                           messagesFF,
                                           "hornetq-data",
                                           "hq",
                                           1);
         final List<RecordInfo> committedRecords = new LinkedList<RecordInfo>();
         final List<PreparedTransactionInfo> preparedTransactions = new LinkedList<PreparedTransactionInfo>();

         messagesJournal.start();

         messagesJournal.load(committedRecords, preparedTransactions, null, false);

         return new Pair<List<RecordInfo>, List<PreparedTransactionInfo>>(committedRecords, preparedTransactions);
      }
      finally
      {
         try
         {
            if (messagesJournal != null)
            {
               messagesJournal.stop();
            }
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   /**
    * Reads a journal system and returns a Map<Integer,AtomicInteger> of recordTypes and the number of records per type,
    * independent of being deleted or not
    * @param config
    * @return
    * @throws Exception
    */
   protected HashMap<Integer, AtomicInteger> countJournal(Configuration config) throws Exception
   {
      final HashMap<Integer, AtomicInteger> recordsType = new HashMap<Integer, AtomicInteger>();
      SequentialFileFactory messagesFF = new NIOSequentialFileFactory(getJournalDir(), null);

      JournalImpl messagesJournal = new JournalImpl(config.getJournalFileSize(),
                                                    config.getJournalMinFiles(),
                                                    0,
                                                    0,
                                                    messagesFF,
                                                    "hornetq-data",
                                                    "hq",
                                                    1);
      List<JournalFile> filesToRead = messagesJournal.orderFiles();

      for (JournalFile file : filesToRead)
      {
         JournalImpl.readJournalFile(messagesFF, file, new JournalReaderCallback()
         {

            AtomicInteger getType(byte key)
            {
               Integer ikey = new Integer(key);
               AtomicInteger value = recordsType.get(ikey);
               if (value == null)
               {
                  value = new AtomicInteger();
                  recordsType.put(ikey, value);
               }
               return value;
            }

            public void onReadUpdateRecordTX(long transactionID, RecordInfo recordInfo) throws Exception
            {
               getType(recordInfo.getUserRecordType()).incrementAndGet();
            }

            public void onReadUpdateRecord(RecordInfo recordInfo) throws Exception
            {
               getType(recordInfo.getUserRecordType()).incrementAndGet();
            }

            public void onReadRollbackRecord(long transactionID) throws Exception
            {
            }

            public void onReadPrepareRecord(long transactionID, byte[] extraData, int numberOfRecords) throws Exception
            {
            }

            public void onReadDeleteRecordTX(long transactionID, RecordInfo recordInfo) throws Exception
            {
            }

            public void onReadDeleteRecord(long recordID) throws Exception
            {
            }

            public void onReadCommitRecord(long transactionID, int numberOfRecords) throws Exception
            {
            }

            public void onReadAddRecordTX(long transactionID, RecordInfo recordInfo) throws Exception
            {
               getType(recordInfo.getUserRecordType()).incrementAndGet();
            }

            public void onReadAddRecord(RecordInfo recordInfo) throws Exception
            {
               getType(recordInfo.getUserRecordType()).incrementAndGet();
            }

            public void markAsDataFile(JournalFile file)
            {
            }
         });
      }
      return recordsType;
   }

   /**
    * This method will load a journal and count the living records
    * @param config
    * @return
    * @throws Exception
    */
   protected HashMap<Integer, AtomicInteger> countJournalLivingRecords(Configuration config) throws Exception
   {
      final HashMap<Integer, AtomicInteger> recordsType = new HashMap<Integer, AtomicInteger>();
      SequentialFileFactory messagesFF = new NIOSequentialFileFactory(getJournalDir(), null);

      JournalImpl messagesJournal = new JournalImpl(config.getJournalFileSize(),
                                                    config.getJournalMinFiles(),
                                                    0,
                                                    0,
                                                    messagesFF,
                                                    "hornetq-data",
                                                    "hq",
                                                    1);
      messagesJournal.start();


      final List<RecordInfo> committedRecords = new LinkedList<RecordInfo>();
      final List<PreparedTransactionInfo> preparedTransactions = new LinkedList<PreparedTransactionInfo>();


      messagesJournal.load(committedRecords, preparedTransactions, null, false);

      for (RecordInfo info: committedRecords)
      {
         Integer ikey = new Integer(info.getUserRecordType());
         AtomicInteger value = recordsType.get(ikey);
         if (value == null)
         {
            value = new AtomicInteger();
            recordsType.put(ikey, value);
         }
         value.incrementAndGet();

      }

      messagesJournal.stop();
      return recordsType;
   }


   /**
    * Deleting a file on LargeDire is an asynchronous process. Wee need to keep looking for a while if the file hasn't been deleted yet
    */
   protected void validateNoFilesOnLargeDir(final int expect) throws Exception
   {
      File largeMessagesFileDir = new File(getLargeMessagesDir());

      // Deleting the file is async... we keep looking for a period of the time until the file is really gone
      long timeout = System.currentTimeMillis() + 5000;
      while (timeout > System.currentTimeMillis() && largeMessagesFileDir.listFiles().length != expect)
      {
         Thread.sleep(100);
      }


      if (expect != largeMessagesFileDir.listFiles().length)
      {
         for (File file : largeMessagesFileDir.listFiles())
         {
            System.out.println("File " + file + " still on ");
         }
      }

      Assert.assertEquals(expect, largeMessagesFileDir.listFiles().length);
   }

   /**
    * @param server the server where's being checked
    * @param address the name of the address being checked
    * @param local if true we are looking for local bindings, false we are looking for remoting servers
    * @param expectedBindingCount the expected number of counts
    * @param expectedConsumerCount the expected number of consumers
    * @param timeout the timeout used on the check
    * @return
    * @throws Exception
    * @throws InterruptedException
    */
   protected boolean waitForBindings(final HornetQServer server,
                                    final String address,
                                    final boolean local,
                                    final int expectedBindingCount,
                                    final int expectedConsumerCount,
                                    long timeout) throws Exception, InterruptedException
   {
      final PostOffice po = server.getPostOffice();
      
      long start = System.currentTimeMillis();

      int bindingCount = 0;

      int totConsumers = 0;

      do
      {
         bindingCount = 0;

         totConsumers = 0;

         Bindings bindings = po.getBindingsForAddress(new SimpleString(address));

         for (Binding binding : bindings.getBindings())
         {
            if (binding instanceof LocalQueueBinding && local || binding instanceof RemoteQueueBinding && !local)
            {
               QueueBinding qBinding = (QueueBinding)binding;

               bindingCount++;

               totConsumers += qBinding.consumerCount();
            }
         }

         if (bindingCount == expectedBindingCount && totConsumers == expectedConsumerCount)
         {
            return true;
         }

         Thread.sleep(10);
      }
      while (System.currentTimeMillis() - start < timeout);

      String msg = "Timed out waiting for bindings (bindingCount = " + bindingCount +
                   " (expecting " +
                   expectedBindingCount +
                   ") " +
                   ", totConsumers = " +
                   totConsumers +
                   " (expecting " +
                   expectedConsumerCount +
                   ")" +
                   ")";

      log.error(msg);
      return false;
   }

   /**
    * Deleting a file on LargeDire is an asynchronous process. Wee need to keep looking for a while if the file hasn't been deleted yet
    */
   protected void validateNoFilesOnLargeDir() throws Exception
   {
      validateNoFilesOnLargeDir(0);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
   class InVMNodeManagerServer extends HornetQServerImpl
   {
      final NodeManager nodeManager;

      public InVMNodeManagerServer(NodeManager nodeManager)
      {
         super();
         this.nodeManager = nodeManager;
      }

      public InVMNodeManagerServer(Configuration configuration, NodeManager nodeManager)
      {
         super(configuration);
         this.nodeManager = nodeManager;
      }

      public InVMNodeManagerServer(Configuration configuration, MBeanServer mbeanServer, NodeManager nodeManager)
      {
         super(configuration, mbeanServer);
         this.nodeManager = nodeManager;
      }

      public InVMNodeManagerServer(Configuration configuration,
                                   HornetQSecurityManager securityManager,
                                   NodeManager nodeManager)
      {
         super(configuration, securityManager);
         this.nodeManager = nodeManager;
      }

      public InVMNodeManagerServer(Configuration configuration,
                                   MBeanServer mbeanServer,
                                   HornetQSecurityManager securityManager,
                                   NodeManager nodeManager)
      {
         super(configuration, mbeanServer, securityManager);
         this.nodeManager = nodeManager;
      }

      @Override
      protected NodeManager createNodeManager(String directory)
      {
         return nodeManager;
      }

   }
}
