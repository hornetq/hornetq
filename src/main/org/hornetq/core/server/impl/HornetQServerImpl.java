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

package org.hornetq.core.server.impl;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;

import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.client.impl.FailoverManager;
import org.hornetq.core.client.impl.FailoverManagerImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.cluster.DivertConfiguration;
import org.hornetq.core.config.cluster.QueueConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.deployers.Deployer;
import org.hornetq.core.deployers.DeploymentManager;
import org.hornetq.core.deployers.impl.AddressSettingsDeployer;
import org.hornetq.core.deployers.impl.BasicUserCredentialsDeployer;
import org.hornetq.core.deployers.impl.FileDeploymentManager;
import org.hornetq.core.deployers.impl.QueueDeployer;
import org.hornetq.core.deployers.impl.SecurityDeployer;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.filter.impl.FilterImpl;
import org.hornetq.core.logging.LogDelegateFactory;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.management.ManagementService;
import org.hornetq.core.management.impl.HornetQServerControlImpl;
import org.hornetq.core.management.impl.ManagementServiceImpl;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.paging.impl.PagingManagerImpl;
import org.hornetq.core.paging.impl.PagingStoreFactoryNIO;
import org.hornetq.core.persistence.QueueBindingInfo;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager;
import org.hornetq.core.persistence.impl.nullpm.NullStorageManager;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.DuplicateIDCache;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.postoffice.impl.DivertBinding;
import org.hornetq.core.postoffice.impl.LocalQueueBinding;
import org.hornetq.core.postoffice.impl.PostOfficeImpl;
import org.hornetq.core.remoting.Channel;
import org.hornetq.core.remoting.Interceptor;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.wireformat.CreateSessionResponseMessage;
import org.hornetq.core.remoting.impl.wireformat.ReattachSessionResponseMessage;
import org.hornetq.core.remoting.server.RemotingService;
import org.hornetq.core.remoting.server.impl.RemotingServiceImpl;
import org.hornetq.core.replication.ReplicationEndpoint;
import org.hornetq.core.replication.ReplicationManager;
import org.hornetq.core.replication.impl.ReplicationEndpointImpl;
import org.hornetq.core.replication.impl.ReplicationManagerImpl;
import org.hornetq.core.security.CheckType;
import org.hornetq.core.security.HornetQSecurityManager;
import org.hornetq.core.security.Role;
import org.hornetq.core.security.SecurityStore;
import org.hornetq.core.security.impl.SecurityStoreImpl;
import org.hornetq.core.server.ActivateCallback;
import org.hornetq.core.server.Divert;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.MemoryManager;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.QueueFactory;
import org.hornetq.core.server.ServerSession;
import org.hornetq.core.server.cluster.ClusterManager;
import org.hornetq.core.server.cluster.Transformer;
import org.hornetq.core.server.cluster.impl.ClusterManagerImpl;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.settings.impl.HierarchicalObjectRepository;
import org.hornetq.core.transaction.ResourceManager;
import org.hornetq.core.transaction.impl.ResourceManagerImpl;
import org.hornetq.core.version.Version;
import org.hornetq.utils.ExecutorFactory;
import org.hornetq.utils.HornetQThreadFactory;
import org.hornetq.utils.OrderedExecutorFactory;
import org.hornetq.utils.Pair;
import org.hornetq.utils.SimpleString;
import org.hornetq.utils.UUID;
import org.hornetq.utils.UUIDGenerator;
import org.hornetq.utils.VersionLoader;

/**
 * The HornetQ server implementation
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ataylor@redhat.com>Andy Taylor</a>
 * @version <tt>$Revision: 3543 $</tt> <p/> $Id: ServerPeer.java 3543 2008-01-07 22:31:58Z clebert.suconic@jboss.com $
 */
public class HornetQServerImpl implements HornetQServer
{
   // Constants
   // ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(HornetQServerImpl.class);

   // Static
   // ---------------------------------------------------------------------------------------

   // Attributes
   // -----------------------------------------------------------------------------------

   private SimpleString nodeID;

   private UUID uuid;

   private final Version version;

   private final HornetQSecurityManager securityManager;

   private final Configuration configuration;

   private final MBeanServer mbeanServer;

   private volatile boolean started;

   private SecurityStore securityStore;

   private final HierarchicalRepository<AddressSettings> addressSettingsRepository;

   private QueueFactory queueFactory;

   private PagingManager pagingManager;

   private PostOffice postOffice;

   private ExecutorService threadPool;

   private ScheduledExecutorService scheduledPool;

   private ExecutorFactory executorFactory;

   private HierarchicalRepository<Set<Role>> securityRepository;

   private ResourceManager resourceManager;

   private HornetQServerControlImpl messagingServerControl;

   private ClusterManager clusterManager;

   private StorageManager storageManager;

   private RemotingService remotingService;

   private ManagementService managementService;

   private MemoryManager memoryManager;

   private DeploymentManager deploymentManager;

   private Deployer basicUserCredentialsDeployer;

   private Deployer addressSettingsDeployer;

   private Deployer queueDeployer;

   private Deployer securityDeployer;

   private final Map<String, ServerSession> sessions = new ConcurrentHashMap<String, ServerSession>();

   private final Object initialiseLock = new Object();

   private boolean initialised;

   private FailoverManager replicationFailoverManager;
   
   private ReplicationManager replicationManager;
   
   private ReplicationEndpoint replicationEndpoint;

   private final Set<ActivateCallback> activateCallbacks = new HashSet<ActivateCallback>();

   // Constructors
   // ---------------------------------------------------------------------------------

   public HornetQServerImpl()
   {
      this(null, null, null);
   }

   public HornetQServerImpl(final Configuration configuration)
   {
      this(configuration, null, null);
   }

   public HornetQServerImpl(final Configuration configuration, MBeanServer mbeanServer)
   {
      this(configuration, mbeanServer, null);
   }

   public HornetQServerImpl(final Configuration configuration, final HornetQSecurityManager securityManager)
   {
      this(configuration, null, securityManager);
   }

   public HornetQServerImpl(Configuration configuration,
                            MBeanServer mbeanServer,
                            final HornetQSecurityManager securityManager)
   {
      if (configuration == null)
      {
         configuration = new ConfigurationImpl();
      }

      if (mbeanServer == null)
      {
         // Just use JVM mbean server
         mbeanServer = ManagementFactory.getPlatformMBeanServer();
      }

      // We need to hard code the version information into a source file

      version = VersionLoader.getVersion();

      this.configuration = configuration;

      this.mbeanServer = mbeanServer;

      this.securityManager = securityManager;

      this.addressSettingsRepository = new HierarchicalObjectRepository<AddressSettings>();

      addressSettingsRepository.setDefault(new AddressSettings());

     // this.managementConnectorID = managementConnectorSequence.decrementAndGet();
   }

   // lifecycle methods
   // ----------------------------------------------------------------

   public synchronized void start() throws Exception
   {
      initialiseLogging();

      log.info((configuration.isBackup() ? "backup" : "live") + " server is starting..");

      if (started)
      {
         return;
      }

      initialisePart1();

      if (configuration.isBackup())
      {
         // We defer actually initialisation until the live node has contacted the backup
         log.info("Backup server initialised");
      }
      else
      {
         initialisePart2();
      }

      // We start the remoting service here - if the server is a backup remoting service needs to be started
      // so it can be initialised by the live node
      remotingService.start();

      started = true;

      log.info("HornetQ Server version " + getVersion().getFullVersion() + " started");
   }

   @Override
   protected void finalize() throws Throwable
   {
      if (started)
      {
         log.warn("HornetQServer is being finalized and has not been stopped. Please remember to stop the " + "server before letting it go out of scope");

         stop();
      }

      super.finalize();
   }

   public synchronized void stop() throws Exception
   {
      if (!started)
      {
         return;
      }

      if (clusterManager != null)
      {
         clusterManager.stop();
      }

      // Need to flush all sessions to make sure all confirmations get sent back to client

      for (ServerSession session : sessions.values())
      {
         session.getChannel().flushConfirmations();
      }

      remotingService.stop();

      // Stop the deployers
      if (configuration.isFileDeploymentEnabled())
      {
         basicUserCredentialsDeployer.stop();

         addressSettingsDeployer.stop();

         if (queueDeployer != null)
         {
            queueDeployer.stop();
         }

         if (securityDeployer != null)
         {
            securityDeployer.stop();
         }

         deploymentManager.stop();
      }

      managementService.unregisterServer();

      managementService.stop();

      if (storageManager != null)
      {
         storageManager.stop();
      }
      
      if (replicationEndpoint != null)
      {
         replicationEndpoint.stop();
         replicationEndpoint = null;
      }

      if (securityManager != null)
      {
         securityManager.stop();
      }

      if (resourceManager != null)
      {
         resourceManager.stop();
      }

      if (postOffice != null)
      {
         postOffice.stop();
      }

      // Need to shutdown pools before shutting down paging manager to make sure everything is written ok

      List<Runnable> tasks = scheduledPool.shutdownNow();

      for (Runnable task : tasks)
      {
         log.debug("Waiting for " + task);
      }

      threadPool.shutdown();
      try
      {
         if (!threadPool.awaitTermination(30000, TimeUnit.MILLISECONDS))
         {
            log.warn("Timed out waiting for pool to terminate");
         }
      }
      catch (InterruptedException e)
      {
         // Ignore
      }

      scheduledPool = null;
      threadPool = null;

      if (pagingManager != null)
      {
         pagingManager.stop();
      }

      if (memoryManager != null)
      {
         memoryManager.stop();
      }
      
      pagingManager = null;
      securityStore = null;
      resourceManager = null;
      postOffice = null;
      securityRepository = null;
      securityStore = null;
      queueFactory = null;
      resourceManager = null;
      messagingServerControl = null;
      memoryManager = null;

      sessions.clear();

      started = false;
      initialised = false;
      uuid = null;
      nodeID = null;

      log.info("HornetQ Server version " + getVersion().getFullVersion() + " stopped");

      Logger.reset();
   }

   // HornetQServer implementation
   // -----------------------------------------------------------

   public Configuration getConfiguration()
   {
      return configuration;
   }

   public MBeanServer getMBeanServer()
   {
      return mbeanServer;
   }

   public RemotingService getRemotingService()
   {
      return remotingService;
   }

   public StorageManager getStorageManager()
   {
      return storageManager;
   }

   public HornetQSecurityManager getSecurityManager()
   {
      return securityManager;
   }

   public ManagementService getManagementService()
   {
      return managementService;
   }

   public HierarchicalRepository<Set<Role>> getSecurityRepository()
   {
      return securityRepository;
   }

   public HierarchicalRepository<AddressSettings> getAddressSettingsRepository()
   {
      return addressSettingsRepository;
   }

   public DeploymentManager getDeploymentManager()
   {
      return deploymentManager;
   }

   public ResourceManager getResourceManager()
   {
      return resourceManager;
   }

   public Version getVersion()
   {
      return version;
   }

   public boolean isStarted()
   {
      return started;
   }

   public ClusterManager getClusterManager()
   {
      return clusterManager;
   }

   public ReattachSessionResponseMessage reattachSession(final RemotingConnection connection,
                                                         final String name,
                                                         final int lastReceivedCommandID) throws Exception
   {
      ServerSession session = sessions.get(name);

      if (!checkActivate())
      {
         return new ReattachSessionResponseMessage(-1, false);
      }

      if (session == null)
      {
         return new ReattachSessionResponseMessage(-1, false);
      }
      else
      {
         // Reconnect the channel to the new connection
         int serverLastReceivedCommandID = session.transferConnection(connection, lastReceivedCommandID);

         return new ReattachSessionResponseMessage(serverLastReceivedCommandID, true);
      }
   }

   public CreateSessionResponseMessage createSession(final String name,
                                                     final long channelID,
                                                     final String username,
                                                     final String password,
                                                     final int minLargeMessageSize,
                                                     final int incrementingVersion,
                                                     final RemotingConnection connection,
                                                     final boolean autoCommitSends,
                                                     final boolean autoCommitAcks,
                                                     final boolean preAcknowledge,
                                                     final boolean xa,
                                                     final int sendWindowSize) throws Exception
   {
      if (version.getIncrementingVersion() != incrementingVersion)
      {
         log.warn("Client with version " + incrementingVersion +
                  " and address " +
                  connection.getRemoteAddress() +
                  " is not compatible with server version " +
                  version.getFullVersion() +
                  ". " +
                  "Please ensure all clients and servers are upgraded to the same version for them to " +
                  "interoperate properly");
         return null;
      }

      if (!checkActivate())
      {
         // Backup server is not ready to accept connections

         return new CreateSessionResponseMessage(version.getIncrementingVersion());
      }

      if (securityStore != null)
      {
         securityStore.authenticate(username, password);
      }

      ServerSession currentSession = sessions.remove(name);

      if (currentSession != null)
      {
         // This session may well be on a different connection and different channel id, so we must get rid
         // of it and create another
         currentSession.getChannel().close();
      }

      Channel channel = connection.getChannel(channelID, sendWindowSize, false);

      final ServerSessionImpl session = new ServerSessionImpl(name,
                                                              username,
                                                              password,
                                                              minLargeMessageSize,
                                                              autoCommitSends,
                                                              autoCommitAcks,
                                                              preAcknowledge,
                                                              configuration.isPersistDeliveryCountBeforeDelivery(),
                                                              xa,
                                                              connection,
                                                              storageManager,
                                                              postOffice,
                                                              resourceManager,
                                                              securityStore,
                                                              executorFactory.getExecutor(),
                                                              channel,
                                                              managementService,
                                                              // queueFactory,
                                                              this,
                                                              configuration.getManagementAddress());

      sessions.put(name, session);

      ServerSessionPacketHandler handler = new ServerSessionPacketHandler(session);

      session.setHandler(handler);

      channel.setHandler(handler);

      return new CreateSessionResponseMessage(version.getIncrementingVersion());
   }
   
   public synchronized ReplicationEndpoint createReplicationEndpoint(final Channel channel) throws Exception
   {
      if (!configuration.isBackup())
      {
         throw new HornetQException(HornetQException.ILLEGAL_STATE, "Connected server is not a backup server");
      }
      
      if (replicationEndpoint == null)
      {
         replicationEndpoint = new ReplicationEndpointImpl(this);
         replicationEndpoint.setChannel(channel);
         replicationEndpoint.start();
      }
      
      
      return replicationEndpoint;
   }

   public void removeSession(final String name) throws Exception
   {
      sessions.remove(name);
   }

   public ServerSession getSession(final String name)
   {
      return sessions.get(name);
   }

   public List<ServerSession> getSessions(final String connectionID)
   {
      Set<Entry<String, ServerSession>> sessionEntries = sessions.entrySet();
      List<ServerSession> matchingSessions = new ArrayList<ServerSession>();
      for (Entry<String, ServerSession> sessionEntry : sessionEntries)
      {
         ServerSession serverSession = sessionEntry.getValue();
         if (serverSession.getConnectionID().toString().equals(connectionID))
         {
            matchingSessions.add(serverSession);
         }
      }
      return matchingSessions;
   }

   public Set<ServerSession> getSessions()
   {
      return new HashSet<ServerSession>(sessions.values());
   }

   public boolean isInitialised()
   {
      synchronized (initialiseLock)
      {
         return initialised;
      }
   }

   // public void initialiseBackup(final UUID theUUID, final long liveUniqueID) throws Exception
   // {
   // if (theUUID == null)
   // {
   // throw new IllegalArgumentException("node id is null");
   // }
   //
   // synchronized (initialiseLock)
   // {
   // if (initialised)
   // {
   // throw new IllegalStateException("Server is already initialised");
   // }
   //
   // this.uuid = theUUID;
   //
   // this.nodeID = new SimpleString(uuid.toString());
   //
   // initialisePart2();
   //
   // long backupID = storageManager.getCurrentUniqueID();
   //
   // if (liveUniqueID != backupID)
   // {
   // initialised = false;
   //
   // throw new IllegalStateException("Live and backup unique ids different (" + liveUniqueID +
   // ":" +
   // backupID +
   // "). You're probably trying to restart a live backup pair after a crash");
   // }
   //
   // log.info("Backup server is now operational");
   // }
   // }

   private boolean startReplication() throws Exception
   {
      String backupConnectorName = configuration.getBackupConnectorName();

      if (backupConnectorName != null)
      {
         TransportConfiguration backupConnector = configuration.getConnectorConfigurations().get(backupConnectorName);

         if (backupConnector == null)
         {
            log.warn("connector with name '" + backupConnectorName + "' is not defined in the configuration.");
         }
         else
         {
            
            replicationFailoverManager = new FailoverManagerImpl((ClientSessionFactory)null,
                                                          backupConnector,
                                                          null,
                                                          false,
                                                          ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT,
                                                          ClientSessionFactoryImpl.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                                          ClientSessionFactoryImpl.DEFAULT_CONNECTION_TTL,
                                                          0,
                                                          1.0d,
                                                          0,
                                                          1,
                                                          threadPool,
                                                          scheduledPool,
                                                          null);
  
            
            this.replicationManager = new ReplicationManagerImpl(replicationFailoverManager, this.executorFactory.getExecutor());
            replicationManager.start();
         }
      }

      return true;
   }

   public HornetQServerControlImpl getHornetQServerControl()
   {
      return messagingServerControl;
   }

   public int getConnectionCount()
   {
      return remotingService.getConnections().size();
   }

   public PostOffice getPostOffice()
   {
      return postOffice;
   }

   public QueueFactory getQueueFactory()
   {
      return queueFactory;
   }

   public SimpleString getNodeID()
   {
      return nodeID;
   }

   public Queue createQueue(final SimpleString address,
                            final SimpleString queueName,
                            final SimpleString filterString,
                            final boolean durable,
                            final boolean temporary) throws Exception
   {
      return createQueue(address, queueName, filterString, durable, temporary, false);
   }

   public Queue deployQueue(final SimpleString address,
                            final SimpleString queueName,
                            final SimpleString filterString,
                            final boolean durable,
                            final boolean temporary) throws Exception
   {
      return createQueue(address, queueName, filterString, durable, temporary, true);
   }

   public void destroyQueue(final SimpleString queueName, final ServerSession session) throws Exception
   {
      Binding binding = postOffice.getBinding(queueName);

      if (binding == null)
      {
         throw new HornetQException(HornetQException.QUEUE_DOES_NOT_EXIST, "No such queue " + queueName);
      }

      Queue queue = (Queue)binding.getBindable();

      if (queue.getConsumerCount() != 0)
      {
         throw new HornetQException(HornetQException.ILLEGAL_STATE, "Cannot delete queue - it has consumers");
      }

      if (session != null)
      {
         if (queue.isDurable())
         {
            // make sure the user has privileges to delete this queue
            securityStore.check(binding.getAddress(), CheckType.DELETE_DURABLE_QUEUE, session);
         }
         else
         {
            securityStore.check(binding.getAddress(), CheckType.DELETE_NON_DURABLE_QUEUE, session);
         }
      }

      queue.deleteAllReferences();

      if (queue.isDurable())
      {
         storageManager.deleteQueueBinding(queue.getID());
      }

      postOffice.removeBinding(queueName);
   }

   public synchronized void registerActivateCallback(final ActivateCallback callback)
   {
      activateCallbacks.add(callback);
   }

   public synchronized void unregisterActivateCallback(final ActivateCallback callback)
   {
      activateCallbacks.remove(callback);
   }

   public ExecutorFactory getExecutorFactory()
   {
      return executorFactory;
   }

   // Public
   // ---------------------------------------------------------------------------------------

   // Package protected
   // ----------------------------------------------------------------------------

   // Protected
   // ------------------------------------------------------------------------------------

   protected PagingManager createPagingManager()
   {
      return new PagingManagerImpl(new PagingStoreFactoryNIO(configuration.getPagingDirectory(), executorFactory),
                                   storageManager,
                                   addressSettingsRepository,
                                   configuration.isJournalSyncNonTransactional());
   }

   /** for use on sub-classes */
   protected ExecutorService getExecutor()
   {
      return threadPool;
   }

   /** 
    * This method is protected as it may be used as a hook for creating a custom storage manager (on tests for instance) 
    * @return
    */
   protected StorageManager createStorageManager()
   {
      if (configuration.isPersistenceEnabled())
      {
         return new JournalStorageManager(configuration, threadPool, replicationManager);
      }
      else
      {
         return new NullStorageManager();
      }
   }

   // Private
   // --------------------------------------------------------------------------------------

   private synchronized void callActivateCallbacks()
   {
      for (ActivateCallback callback : activateCallbacks)
      {
         callback.activated();
      }
   }

   private synchronized boolean checkActivate() throws Exception
   {
      if (configuration.isBackup())
      {
         // Handle backup server activation

         if (!configuration.isSharedStore())
         {
            if (replicationEndpoint == null)
            {
               log.warn("There is no replication endpoint, can't activate this backup server");
               throw new HornetQException(HornetQException.INTERNAL_ERROR, "Can't activate the server");
            }
            
            replicationEndpoint.stop();
         }
         
         // Complete the startup procedure

         log.info("Activating server");

         configuration.setBackup(false);

         initialisePart2();
      }

      return true;
   }

   private void initialisePart1() throws Exception
   {
      // Create the pools - we have two pools - one for non scheduled - and another for scheduled

      ThreadFactory tFactory = new HornetQThreadFactory("HornetQ-server-threads" + System.identityHashCode(this), false);

      if (configuration.getThreadPoolMaxSize() == -1)
      {
         threadPool = Executors.newCachedThreadPool(tFactory);
      }
      else
      {
         threadPool = Executors.newFixedThreadPool(configuration.getThreadPoolMaxSize(), tFactory);
      }

      executorFactory = new OrderedExecutorFactory(threadPool);

      scheduledPool = new ScheduledThreadPoolExecutor(configuration.getScheduledThreadPoolMaxSize(),
                                                      new HornetQThreadFactory("HornetQ-scheduled-threads", false));

      managementService = new ManagementServiceImpl(mbeanServer, configuration);

      remotingService = new RemotingServiceImpl(configuration,
                                                this,
                                                managementService,
                                                threadPool,
                                                scheduledPool);

      if (configuration.getMemoryMeasureInterval() != -1)
      {
         memoryManager = new MemoryManagerImpl(configuration.getMemoryWarningThreshold(),
                                               configuration.getMemoryMeasureInterval());

         memoryManager.start();
      }
   }

   private void initialiseLogging()
   {
      LogDelegateFactory logDelegateFactory = (LogDelegateFactory)instantiateInstance(configuration.getLogDelegateFactoryClassName());

      Logger.setDelegateFactory(logDelegateFactory);
   }

   private void initialisePart2() throws Exception
   {
      // Create the hard-wired components

      if (configuration.isFileDeploymentEnabled())
      {
         deploymentManager = new FileDeploymentManager(configuration.getFileDeployerScanPeriod());
      }

      
      startReplication();

      this.storageManager = createStorageManager();

      securityRepository = new HierarchicalObjectRepository<Set<Role>>();
      securityRepository.setDefault(new HashSet<Role>());

      securityStore = new SecurityStoreImpl(securityRepository,
                                            securityManager,
                                            configuration.getSecurityInvalidationInterval(),
                                            configuration.isSecurityEnabled(),
                                            configuration.getManagementClusterUser(),
                                            configuration.getManagementClusterPassword(),
                                            managementService);

      queueFactory = new QueueFactoryImpl(scheduledPool, addressSettingsRepository, storageManager);

      pagingManager = createPagingManager();

      resourceManager = new ResourceManagerImpl((int)(configuration.getTransactionTimeout() / 1000),
                                                configuration.getTransactionTimeoutScanPeriod(),
                                                scheduledPool);
      postOffice = new PostOfficeImpl(storageManager,
                                      pagingManager,
                                      queueFactory,
                                      managementService,
                                      configuration.getMessageExpiryScanPeriod(),
                                      configuration.getMessageExpiryThreadPriority(),
                                      configuration.isWildcardRoutingEnabled(),
                                      configuration.getIDCacheSize(),
                                      configuration.isPersistIDCache(),
                                      executorFactory,
                                      addressSettingsRepository);

      messagingServerControl = managementService.registerServer(postOffice,
                                                                storageManager,
                                                                configuration,
                                                                addressSettingsRepository,
                                                                securityRepository,
                                                                resourceManager,
                                                                remotingService,
                                                                this,
                                                                queueFactory,
                                                                scheduledPool,
                                                                pagingManager, 
                                                                configuration.isBackup());

      // Address settings need to deployed initially, since they're require on paging manager.start()

      if (configuration.isFileDeploymentEnabled())
      {
         addressSettingsDeployer = new AddressSettingsDeployer(deploymentManager, addressSettingsRepository);

         addressSettingsDeployer.start();
      }

      storageManager.start();

      if (securityManager != null)
      {
         securityManager.start();
      }

      postOffice.start();

      pagingManager.start();

      managementService.start();

      resourceManager.start();

      // Deploy all security related config
      if (configuration.isFileDeploymentEnabled())
      {
         basicUserCredentialsDeployer = new BasicUserCredentialsDeployer(deploymentManager, securityManager);

         basicUserCredentialsDeployer.start();

         if (securityManager != null)
         {
            securityDeployer = new SecurityDeployer(deploymentManager, securityRepository);

            securityDeployer.start();
         }
      }

      // Load the journal and populate queues, transactions and caches in memory
      loadJournal();

      // Deploy any queues in the Configuration class - if there's no file deployment we still need
      // to load those
      deployQueuesFromConfiguration();

      // Deploy any predefined queues

      if (configuration.isFileDeploymentEnabled())
      {
         queueDeployer = new QueueDeployer(deploymentManager, messagingServerControl);

         queueDeployer.start();
      }

      // We need to call this here, this gives any dependent server a chance to deploy its own destinations
      // this needs to be done before clustering is initialised
      callActivateCallbacks();

      // Deply any pre-defined diverts
      deployDiverts();

      if (configuration.isClustered())
      {
         // This can't be created until node id is set
         clusterManager = new ClusterManagerImpl(executorFactory,
                                                 this,
                                                 postOffice,
                                                 scheduledPool,
                                                 managementService,
                                                 configuration,
                                                 uuid,
                                                 configuration.isBackup());

         clusterManager.start();
      }

      if (deploymentManager != null)
      {
         deploymentManager.start();
      }

      pagingManager.resumeDepages();

      final ServerInfo dumper = new ServerInfo(this, pagingManager);

      long dumpInfoInterval = configuration.getServerDumpInterval();

      if (dumpInfoInterval > 0)
      {
         scheduledPool.scheduleWithFixedDelay(new Runnable()
         {
            public void run()
            {
               log.info(dumper.dump());
            }
         }, 0, dumpInfoInterval, TimeUnit.MILLISECONDS);
      }

      initialised = true;
   }

   private void deployQueuesFromConfiguration() throws Exception
   {
      for (QueueConfiguration config : configuration.getQueueConfigurations())
      {
         messagingServerControl.deployQueue(config.getAddress(),
                                            config.getName(),
                                            config.getFilterString(),
                                            config.isDurable());
      }
   }

   private void loadJournal() throws Exception
   {
      List<QueueBindingInfo> queueBindingInfos = new ArrayList<QueueBindingInfo>();

      storageManager.loadBindingJournal(queueBindingInfos);

      // Set the node id - must be before we load the queues into the postoffice, but after we load the journal
      setNodeID();

      Map<Long, Queue> queues = new HashMap<Long, Queue>();

      for (QueueBindingInfo queueBindingInfo : queueBindingInfos)
      {
         Filter filter = FilterImpl.createFilter(queueBindingInfo.getFilterString());

         Queue queue = queueFactory.createQueue(queueBindingInfo.getId(),
                                                queueBindingInfo.getAddress(),
                                                queueBindingInfo.getQueueName(),
                                                filter,
                                                true,
                                                false);

         Binding binding = new LocalQueueBinding(queueBindingInfo.getAddress(), queue, nodeID);

         queues.put(queueBindingInfo.getId(), queue);

         postOffice.addBinding(binding);

         managementService.registerAddress(queueBindingInfo.getAddress());
         managementService.registerQueue(queue, queueBindingInfo.getAddress(), storageManager);
      }

      Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap = new HashMap<SimpleString, List<Pair<byte[], Long>>>();

      storageManager.loadMessageJournal(postOffice, pagingManager, resourceManager, queues, duplicateIDMap);

      for (Map.Entry<SimpleString, List<Pair<byte[], Long>>> entry : duplicateIDMap.entrySet())
      {
         SimpleString address = entry.getKey();

         DuplicateIDCache cache = postOffice.getDuplicateIDCache(address);

         if (configuration.isPersistIDCache())
         {
            cache.load(entry.getValue());
         }
      }
   }

   private void setNodeID() throws Exception
   {
      if (!configuration.isBackup())
      {
         if (uuid == null)
         {
            uuid = storageManager.getPersistentID();

            if (uuid == null)
            {
               uuid = UUIDGenerator.getInstance().generateUUID();

               storageManager.setPersistentID(uuid);
            }

            nodeID = new SimpleString(uuid.toString());
         }
      }
      else
      {
         UUID currentUUID = storageManager.getPersistentID();

         if (currentUUID != null)
         {
            if (!currentUUID.equals(uuid))
            {
               throw new IllegalStateException("Backup server already has an id but it's not the same as live");
            }
         }
         else
         {
            storageManager.setPersistentID(uuid);
         }
      }
   }

   private Queue createQueue(final SimpleString address,
                             final SimpleString queueName,
                             final SimpleString filterString,
                             final boolean durable,
                             final boolean temporary,
                             final boolean ignoreIfExists) throws Exception
   {
      Binding binding = postOffice.getBinding(queueName);

      if (binding != null)
      {
         if (ignoreIfExists)
         {
            return null;
         }
         else
         {
            throw new HornetQException(HornetQException.QUEUE_EXISTS, "Queue " + queueName + " already exists");
         }
      }

      Filter filter = FilterImpl.createFilter(filterString);

      final Queue queue = queueFactory.createQueue(storageManager.generateUniqueID(),
                                                   address,
                                                   queueName,
                                                   filter,
                                                   durable,
                                                   temporary);

      binding = new LocalQueueBinding(address, queue, nodeID);

      if (durable)
      {
         storageManager.addQueueBinding(binding);
      }

      postOffice.addBinding(binding);

      managementService.registerAddress(address);
      managementService.registerQueue(queue, address, storageManager);

      return queue;
   }

   private void deployDiverts() throws Exception
   {
      for (DivertConfiguration config : configuration.getDivertConfigurations())
      {
         if (config.getName() == null)
         {
            log.warn("Must specify a name for each divert. This one will not be deployed.");

            return;
         }

         if (config.getAddress() == null)
         {
            log.warn("Must specify an address for each divert. This one will not be deployed.");

            return;
         }

         if (config.getForwardingAddress() == null)
         {
            log.warn("Must specify an forwarding address for each divert. This one will not be deployed.");

            return;
         }

         SimpleString sName = new SimpleString(config.getName());

         if (postOffice.getBinding(sName) != null)
         {
            log.warn("Binding already exists with name " + sName + ", divert will not be deployed");

            continue;
         }

         SimpleString sAddress = new SimpleString(config.getAddress());

         Transformer transformer = instantiateTransformer(config.getTransformerClassName());

         Filter filter = FilterImpl.createFilter(config.getFilterString());

         Divert divert = new DivertImpl(new SimpleString(config.getForwardingAddress()),
                                        sName,
                                        new SimpleString(config.getRoutingName()),
                                        config.isExclusive(),
                                        filter,
                                        transformer,
                                        postOffice);
         // pagingManager,
         // storageManager);

         Binding binding = new DivertBinding(storageManager.generateUniqueID(), sAddress, divert);

         postOffice.addBinding(binding);

         managementService.registerDivert(divert, config);
      }
   }

   private Transformer instantiateTransformer(final String transformerClassName)
   {
      Transformer transformer = null;

      if (transformerClassName != null)
      {
         transformer = (Transformer)instantiateInstance(transformerClassName);
      }

      return transformer;
   }

   private Object instantiateInstance(final String className)
   {
      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      try
      {
         Class<?> clz = loader.loadClass(className);
         Object object = clz.newInstance();

         return object;
      }
      catch (Exception e)
      {
         throw new IllegalArgumentException("Error instantiating transformer class \"" + className + "\"", e);
      }
   }

   // Inner classes
   // --------------------------------------------------------------------------------
}
