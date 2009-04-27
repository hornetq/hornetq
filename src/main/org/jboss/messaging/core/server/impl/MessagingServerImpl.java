/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors by
 * the @authors tag. See the copyright.txt in the distribution for a full listing of individual contributors. This is
 * free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of the License, or (at your option) any later version.
 * This software is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public License along with this software; if not,
 * write to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.core.server.impl;

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
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;

import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.cluster.DivertConfiguration;
import org.jboss.messaging.core.config.cluster.QueueConfiguration;
import org.jboss.messaging.core.deployers.Deployer;
import org.jboss.messaging.core.deployers.DeploymentManager;
import org.jboss.messaging.core.deployers.impl.AddressSettingsDeployer;
import org.jboss.messaging.core.deployers.impl.BasicUserCredentialsDeployer;
import org.jboss.messaging.core.deployers.impl.FileDeploymentManager;
import org.jboss.messaging.core.deployers.impl.QueueDeployer;
import org.jboss.messaging.core.deployers.impl.SecurityDeployer;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.filter.impl.FilterImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.ManagementService;
import org.jboss.messaging.core.management.impl.ManagementServiceImpl;
import org.jboss.messaging.core.management.impl.MessagingServerControl;
import org.jboss.messaging.core.paging.PagingManager;
import org.jboss.messaging.core.paging.impl.PagingManagerImpl;
import org.jboss.messaging.core.paging.impl.PagingStoreFactoryNIO;
import org.jboss.messaging.core.persistence.QueueBindingInfo;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.persistence.impl.journal.JournalStorageManager;
import org.jboss.messaging.core.persistence.impl.nullpm.NullStorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.DuplicateIDCache;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.postoffice.impl.DivertBinding;
import org.jboss.messaging.core.postoffice.impl.LocalQueueBinding;
import org.jboss.messaging.core.postoffice.impl.PostOfficeImpl;
import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.remoting.FailureListener;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.RemotingConnectionImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateSessionResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ReattachSessionResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.replication.ReplicateStartupInfoMessage;
import org.jboss.messaging.core.remoting.server.RemotingService;
import org.jboss.messaging.core.remoting.server.impl.RemotingServiceImpl;
import org.jboss.messaging.core.remoting.spi.Connection;
import org.jboss.messaging.core.remoting.spi.ConnectionLifeCycleListener;
import org.jboss.messaging.core.remoting.spi.ConnectorFactory;
import org.jboss.messaging.core.security.CheckType;
import org.jboss.messaging.core.security.JBMSecurityManager;
import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.security.SecurityStore;
import org.jboss.messaging.core.security.impl.SecurityStoreImpl;
import org.jboss.messaging.core.server.ActivateCallback;
import org.jboss.messaging.core.server.Divert;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.QueueFactory;
import org.jboss.messaging.core.server.ServerSession;
import org.jboss.messaging.core.server.cluster.ClusterManager;
import org.jboss.messaging.core.server.cluster.Transformer;
import org.jboss.messaging.core.server.cluster.impl.ClusterManagerImpl;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.core.settings.impl.HierarchicalObjectRepository;
import org.jboss.messaging.core.transaction.ResourceManager;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.core.transaction.impl.ResourceManagerImpl;
import org.jboss.messaging.core.transaction.impl.TransactionImpl;
import org.jboss.messaging.core.version.Version;
import org.jboss.messaging.utils.Future;
import org.jboss.messaging.utils.Pair;
import org.jboss.messaging.utils.SimpleString;
import org.jboss.messaging.utils.UUID;
import org.jboss.messaging.utils.UUIDGenerator;
import org.jboss.messaging.utils.VersionLoader;

/**
 * The messaging server implementation
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ataylor@redhat.com>Andy Taylor</a>
 * @version <tt>$Revision: 3543 $</tt> <p/> $Id: ServerPeer.java 3543 2008-01-07 22:31:58Z clebert.suconic@jboss.com $
 */
public class MessagingServerImpl implements MessagingServer
{
   // Constants
   // ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(MessagingServerImpl.class);

   // Static
   // ---------------------------------------------------------------------------------------

   // Attributes
   // -----------------------------------------------------------------------------------

   private SimpleString nodeID;

   private UUID uuid;

   private final Version version;

   private final JBMSecurityManager securityManager;

   private final Configuration configuration;

   private final MBeanServer mbeanServer;

   private final Set<ActivateCallback> activateCallbacks = new HashSet<ActivateCallback>();

   private volatile boolean started;

   private SecurityStore securityStore;

   private final HierarchicalRepository<AddressSettings> addressSettingsRepository;

   private ScheduledExecutorService scheduledExecutor;

   private QueueFactory queueFactory;

   private PagingManager pagingManager;

   private PostOffice postOffice;

   private ExecutorService asyncDeliveryPool;

   private org.jboss.messaging.utils.ExecutorFactory executorFactory;

   private HierarchicalRepository<Set<Role>> securityRepository;

   private ResourceManager resourceManager;

   private MessagingServerControl messagingServerControl;

   private ClusterManager clusterManager;

   private StorageManager storageManager;

   private RemotingService remotingService;

   private ManagementService managementService;

   private DeploymentManager deploymentManager;

   private Deployer basicUserCredentialsDeployer;

   private Deployer addressSettingsDeployer;

   private Deployer queueDeployer;

   private Deployer securityDeployer;

   private final Map<String, ServerSession> sessions = new ConcurrentHashMap<String, ServerSession>();

   private ConnectorFactory backupConnectorFactory;

   private Map<String, Object> backupConnectorParams;

   private RemotingConnection replicatingConnection;

   private Channel replicatingChannel;

   private Object replicatingChannelLock = new Object();

   private final Object initialiseLock = new Object();

   private boolean initialised;

   // Constructors
   // ---------------------------------------------------------------------------------

   public MessagingServerImpl(final Configuration configuration,
                              final MBeanServer mbeanServer,
                              final JBMSecurityManager securityManager)
   {
      if (configuration == null)
      {
         throw new NullPointerException("Must inject Configuration into MessagingServer constructor");
      }

      if (mbeanServer == null)
      {
         throw new NullPointerException("Must inject MBeanServer into MessagingServer constructor");
      }

      if (securityManager == null)
      {
         throw new NullPointerException("Must inject SecurityManager into MessagingServer constructor");
      }

      // We need to hard code the version information into a source file

      version = VersionLoader.getVersion();

      this.configuration = configuration;

      this.mbeanServer = mbeanServer;

      this.securityManager = securityManager;

      this.addressSettingsRepository = new HierarchicalObjectRepository<AddressSettings>();

      addressSettingsRepository.setDefault(new AddressSettings());
   }

   // lifecycle methods
   // ----------------------------------------------------------------

   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }

      initialisePart1();

      if (configuration.isBackup())
      {
         // We defer actually initialisation until the live node has contacted the backup
         log.info("Backup server will await live server before becoming operational");
      }
      else
      {
         initialisePart2();
      }

      // We start the remoting service here - if the server is a backup remoting service needs to be started
      // so it can be initialised by the live node
      remotingService.start();
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

      remotingService.stop();

      // Stop the deployers
      if (configuration.isEnableFileDeployment())
      {
         basicUserCredentialsDeployer.stop();

         addressSettingsDeployer.stop();

         queueDeployer.stop();

         securityDeployer.stop();

         deploymentManager.stop();
      }

      managementService.stop();

      storageManager.stop();

      securityManager.stop();

      asyncDeliveryPool.shutdown();

      try
      {
         if (!asyncDeliveryPool.awaitTermination(10000, TimeUnit.MILLISECONDS))
         {
            log.warn("Timed out waiting for pool to terminate");
         }
      }
      catch (InterruptedException e)
      {
         // Ignore
      }

      if (replicatingConnection != null)
      {
         try
         {
            replicatingConnection.destroy();
         }
         catch (Exception ignore)
         {
         }

         replicatingConnection = null;
         replicatingChannel = null;
      }

      pagingManager.stop();
      pagingManager = null;
      securityStore = null;
      resourceManager.stop();
      resourceManager = null;
      postOffice.stop();
      postOffice = null;
      securityRepository = null;
      securityStore = null;
      scheduledExecutor.shutdown();
      queueFactory = null;
      resourceManager = null;
      messagingServerControl = null;

      sessions.clear();

      started = false;
      initialised = false;
      uuid = null;
      nodeID = null;
   }

   // MessagingServer implementation
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

   public JBMSecurityManager getSecurityManager()
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

      // Need to activate the connection even if session can't be found - since otherwise response
      // will never get back

      checkActivate(connection);

      if (session == null)
      {
         return new ReattachSessionResponseMessage(-1, true);
      }
      else
      {
         // Reconnect the channel to the new connection
         int serverLastReceivedCommandID = session.transferConnection(connection, lastReceivedCommandID);

         return new ReattachSessionResponseMessage(serverLastReceivedCommandID, false);
      }
   }

   public void replicateCreateSession(final String name,
                                      final long replicatedChannelID,
                                      final long originalChannelID,
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
      doCreateSession(name,
                      replicatedChannelID,
                      originalChannelID,
                      username,
                      password,
                      minLargeMessageSize,
                      incrementingVersion,
                      connection,
                      autoCommitSends,
                      autoCommitAcks,
                      preAcknowledge,
                      xa,
                      sendWindowSize,
                      true);
   }

   public CreateSessionResponseMessage createSession(final String name,
                                                     final long channelID,
                                                     final long replicatedChannelID,
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
      checkActivate(connection);

      return doCreateSession(name,
                             channelID,
                             replicatedChannelID,
                             username,
                             password,
                             minLargeMessageSize,
                             incrementingVersion,
                             connection,
                             autoCommitSends,
                             autoCommitAcks,
                             preAcknowledge,
                             xa,
                             sendWindowSize,
                             false);
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

   public void initialiseBackup(final UUID theUUID, final long liveUniqueID) throws Exception
   {
      if (theUUID == null)
      {
         throw new IllegalArgumentException("node id is null");
      }

      synchronized (initialiseLock)
      {
         if (initialised)
         {
            throw new IllegalStateException("Server is already initialised");
         }

         this.uuid = theUUID;

         this.nodeID = new SimpleString(uuid.toString());

         initialisePart2();
        
         long backupID = storageManager.getCurrentUniqueID();

         if (liveUniqueID != backupID)
         {
            initialised = false;
            
            throw new IllegalStateException("Live and backup unique ids different. You're probably trying to restart a live backup pair after a crash");            
         }

         log.info("Backup server is now operational");
      }
   }

   public Channel getReplicatingChannel()
   {
      synchronized (replicatingChannelLock)
      {
         if (replicatingChannel == null && backupConnectorFactory != null)
         {
            NoCacheConnectionLifeCycleListener listener = new NoCacheConnectionLifeCycleListener();

            replicatingConnection = (RemotingConnectionImpl)RemotingConnectionImpl.createConnection(backupConnectorFactory,
                                                                                                    backupConnectorParams,
                                                                                                    ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT,
                                                                                                    ClientSessionFactoryImpl.DEFAULT_PING_PERIOD,
                                                                                                    ClientSessionFactoryImpl.DEFAULT_CONNECTION_TTL,
                                                                                                    scheduledExecutor,
                                                                                                    listener);

            if (replicatingConnection == null)
            {
               return null;
            }

            listener.conn = replicatingConnection;

            replicatingChannel = replicatingConnection.getChannel(2, -1, false);

            replicatingConnection.addFailureListener(new FailureListener()
            {
               public boolean connectionFailed(MessagingException me)
               {
                  replicatingChannel.executeOutstandingDelayedResults();

                  return true;
               }
            });

            replicatingConnection.startPinger();

            // First time we get channel we send a message down it informing the backup of our node id -
            // backup and live must have the same node id

            Packet packet = new ReplicateStartupInfoMessage(uuid, storageManager.getCurrentUniqueID());

            final Future future = new Future();

            replicatingChannel.replicatePacket(packet, 1, new Runnable()
            {
               public void run()
               {
                  future.run();
               }
            });

            boolean ok = future.await(10000);

            if (!ok)
            {
               throw new IllegalStateException("Timed out waiting for response from backup for initialisation");
            }
         }
      }

      return replicatingChannel;
   }

   public MessagingServerControl getMessagingServerControl()
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

   public void handleReplicateRedistribution(final SimpleString queueName, final long messageID) throws Exception
   {
      Binding binding = postOffice.getBinding(queueName);

      if (binding == null)
      {
         throw new IllegalStateException("Cannot find queue " + queueName);
      }

      Queue queue = (Queue)binding.getBindable();

      MessageReference reference = queue.removeFirstReference(messageID);

      Transaction tx = new TransactionImpl(storageManager);

      boolean routed = postOffice.redistribute(reference.getMessage(), queue.getName(), tx);

      if (routed)
      {
         queue.acknowledge(tx, reference);

         tx.commit();
      }
      else
      {
         throw new IllegalStateException("Must be routed");
      }
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
         throw new MessagingException(MessagingException.QUEUE_DOES_NOT_EXIST, "No such queue " + queueName);
      }

      Queue queue = (Queue)binding.getBindable();

      if (queue.getConsumerCount() != 0)
      {
         throw new MessagingException(MessagingException.ILLEGAL_STATE, "Cannot delete queue - it has consumers");
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
         storageManager.deleteQueueBinding(queue.getPersistenceID());
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

   // Public
   // ---------------------------------------------------------------------------------------

   // Package protected
   // ----------------------------------------------------------------------------

   // Protected
   // ------------------------------------------------------------------------------------

   protected PagingManager createPagingManager()
   {
      return new PagingManagerImpl(new PagingStoreFactoryNIO(configuration.getPagingDirectory(),
                                                             configuration.getPagingMaxThreads()),
                                   storageManager,
                                   addressSettingsRepository,
                                   configuration.getPagingMaxGlobalSizeBytes(),
                                   configuration.getPagingGlobalWatermarkSize(),
                                   configuration.isJournalSyncNonTransactional(),
                                   configuration.isBackup());
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

   private void checkActivate(final RemotingConnection connection)
   {
      if (configuration.isBackup())
      {
         log.info("*** activating");
         
         synchronized (this)
         {
            freezeBackupConnection();

            List<Queue> toActivate = postOffice.activate();

            for (Queue queue : toActivate)
            {
               scheduledExecutor.schedule(new ActivateRunner(queue),
                                          configuration.getQueueActivationTimeout(),
                                          TimeUnit.MILLISECONDS);
            }

            configuration.setBackup(false);

            if (clusterManager != null)
            {
               clusterManager.activate();
            }
         }
      }

      connection.activate();
   }

   // We need to prevent any more packets being handled on replicating connection as soon as first live connection
   // is created or re-attaches, to prevent a situation like the following:
   // connection 1 create queue A
   // connection 2 fails over
   // A gets activated since no consumers
   // connection 1 create consumer on A
   // connection 1 delivery
   // connection 1 delivery gets replicated
   // can't find message in queue since active was delivered immediately
   private void freezeBackupConnection()
   {
      // Sanity check
      // All replicated sessions should be on the same connection
      RemotingConnection replConnection = null;

      for (ServerSession session : sessions.values())
      {
         RemotingConnection rc = session.getChannel().getConnection();

         if (replConnection == null)
         {
            replConnection = rc;
         }
         else if (replConnection != rc)
         {
            throw new IllegalStateException("More than one replicating connection!");
         }
      }

      if (replConnection != null)
      {
         replConnection.freeze();
      }
   }

   private void initialisePart1() throws Exception
   {
      managementService = new ManagementServiceImpl(mbeanServer, configuration);

      remotingService = new RemotingServiceImpl(configuration, this, managementService);
   }

   private void initialisePart2() throws Exception
   {
      // Create the pools and executor related objects
      asyncDeliveryPool = Executors.newCachedThreadPool(new org.jboss.messaging.utils.JBMThreadFactory("JBM-async-session-delivery-threads"));

      executorFactory = new org.jboss.messaging.utils.OrderedExecutorFactory(asyncDeliveryPool);

      scheduledExecutor = new ScheduledThreadPoolExecutor(configuration.getScheduledThreadPoolMaxSize(),
                                                          new org.jboss.messaging.utils.JBMThreadFactory("JBM-scheduled-threads"));

      // Create the hard-wired components

      if (configuration.isEnableFileDeployment())
      {
         deploymentManager = new FileDeploymentManager(configuration.getFileDeployerScanPeriod());
      }

      if (configuration.isEnablePersistence())
      {
         storageManager = new JournalStorageManager(configuration);
      }
      else
      {
         storageManager = new NullStorageManager();
      }

      securityRepository = new HierarchicalObjectRepository<Set<Role>>();
      securityRepository.setDefault(new HashSet<Role>());

      securityStore = new SecurityStoreImpl(securityRepository,
                                            securityManager,
                                            configuration.getSecurityInvalidationInterval(),
                                            configuration.isSecurityEnabled(),
                                            configuration.getManagementClusterPassword(),
                                            managementService);

      queueFactory = new QueueFactoryImpl(scheduledExecutor, addressSettingsRepository, storageManager);

      pagingManager = createPagingManager();

      resourceManager = new ResourceManagerImpl((int)(configuration.getTransactionTimeout() / 1000),
                                                configuration.getTransactionTimeoutScanPeriod());
      postOffice = new PostOfficeImpl(this,
                                      storageManager,
                                      pagingManager,
                                      queueFactory,
                                      managementService,
                                      configuration.getMessageExpiryScanPeriod(),
                                      configuration.getMessageExpiryThreadPriority(),
                                      configuration.isWildcardRoutingEnabled(),
                                      configuration.isBackup(),
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
                                                                configuration.isBackup());

      // Address settings need to deployed initially, since they're require on paging manager.start()

      if (configuration.isEnableFileDeployment())
      {
         addressSettingsDeployer = new AddressSettingsDeployer(deploymentManager, addressSettingsRepository);

         addressSettingsDeployer.start();
      }

      storageManager.start();

      securityManager.start();

      postOffice.start();

      pagingManager.start();

      log.info("starting management service");
      managementService.start();

      resourceManager.start();

      // Deploy all security related config
      if (configuration.isEnableFileDeployment())
      {
         basicUserCredentialsDeployer = new BasicUserCredentialsDeployer(deploymentManager, securityManager);

         securityDeployer = new SecurityDeployer(deploymentManager, securityRepository);

         basicUserCredentialsDeployer.start();

         securityDeployer.start();
      }

      // Load the journal and populate queues, transactions and caches in memory
      loadJournal();

      // Deploy any queues in the Configuration class - if there's no file deployment we still need
      // to load those
      deployQueuesFromConfiguration();

      // Deploy any predefined queues - on backup we don't start queue deployer - instead deployments
      // are replicated from live

      if (configuration.isEnableFileDeployment() && !configuration.isBackup())
      {
         queueDeployer = new QueueDeployer(deploymentManager, messagingServerControl);

         queueDeployer.start();
      }

      // We need to call this here, this gives any dependent server a chance to deploy its own destinations
      // this needs to be done before clustering is initialised, and in the same order on live and backup
      callActivateCallbacks();

      // Deply any pre-defined diverts
      deployDiverts();

      // Set-up the replicating connection
      if (!setupReplicatingConnection())
      {
         return;
      }

      if (configuration.isClustered())
      {
         // This can't be created until node id is set
         clusterManager = new ClusterManagerImpl(executorFactory,
                                                 this,
                                                 postOffice,
                                                 scheduledExecutor,
                                                 managementService,
                                                 configuration,
                                                 uuid,
                                                 replicatingChannel,
                                                 configuration.isBackup());

         clusterManager.start();
      }

      if (deploymentManager != null)
      {
         deploymentManager.start();
      }

      pagingManager.startGlobalDepage();

      initialised = true;

      started = true;
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

   private boolean setupReplicatingConnection() throws Exception
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

            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            try
            {
               Class<?> clz = loader.loadClass(backupConnector.getFactoryClassName());
               backupConnectorFactory = (ConnectorFactory)clz.newInstance();
            }
            catch (Exception e)
            {
               throw new IllegalArgumentException("Error instantiating interceptor \"" + backupConnector.getFactoryClassName() +
                                                           "\"",
                                                  e);
            }

            backupConnectorParams = backupConnector.getParams();
         }
      }

      Channel replicatingChannel = getReplicatingChannel();

      if (replicatingChannel == null && backupConnectorFactory != null)
      {
         log.warn("Backup server MUST be started before live server. Initialisation will proceed.");

         return false;
      }
      else
      {
         return true;
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
         Filter filter = null;

         if (queueBindingInfo.getFilterString() != null)
         {
            filter = new FilterImpl(queueBindingInfo.getFilterString());
         }

         Queue queue = queueFactory.createQueue(queueBindingInfo.getPersistenceID(),
                                                queueBindingInfo.getAddress(),
                                                queueBindingInfo.getQueueName(),
                                                filter,
                                                true,
                                                false);

         Binding binding = new LocalQueueBinding(queueBindingInfo.getAddress(), queue, nodeID);

         queues.put(queueBindingInfo.getPersistenceID(), queue);

         postOffice.addBinding(binding);
      }

      Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap = new HashMap<SimpleString, List<Pair<byte[], Long>>>();

      storageManager.loadMessageJournal(pagingManager, resourceManager, queues, duplicateIDMap);

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
            throw new MessagingException(MessagingException.QUEUE_EXISTS);
         }
      }

      Filter filter = null;

      if (filterString != null)
      {
         filter = new FilterImpl(filterString);
      }

      final Queue queue = queueFactory.createQueue(-1, address, queueName, filter, durable, temporary);

      binding = new LocalQueueBinding(address, queue, nodeID);

      if (durable)
      {
         storageManager.addQueueBinding(binding);
      }

      postOffice.addBinding(binding);

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

         Filter filter = null;

         if (config.getFilterString() != null)
         {
            filter = new FilterImpl(new SimpleString(config.getFilterString()));
         }

         Divert divert = new DivertImpl(new SimpleString(config.getForwardingAddress()),
                                        sName,
                                        new SimpleString(config.getRoutingName()),
                                        config.isExclusive(),
                                        filter,
                                        transformer,
                                        postOffice,
                                        pagingManager,
                                        storageManager);

         Binding binding = new DivertBinding(sAddress, divert);

         postOffice.addBinding(binding);

         managementService.registerDivert(divert, config);
      }
   }

   private CreateSessionResponseMessage doCreateSession(final String name,
                                                        final long channelID,
                                                        final long oppositeChannelID,
                                                        final String username,
                                                        final String password,
                                                        final int minLargeMessageSize,
                                                        final int incrementingVersion,
                                                        final RemotingConnection connection,
                                                        final boolean autoCommitSends,
                                                        final boolean autoCommitAcks,
                                                        final boolean preAcknowledge,
                                                        final boolean xa,
                                                        final int sendWindowSize,
                                                        final boolean backup) throws Exception
   {
      if (version.getIncrementingVersion() < incrementingVersion)
      {
         throw new MessagingException(MessagingException.INCOMPATIBLE_CLIENT_SERVER_VERSIONS,
                                      "client not compatible with version: " + version.getFullVersion());
      }

      // Is this comment relevant any more ?

      // Authenticate. Successful autentication will place a new SubjectContext
      // on thread local,
      // which will be used in the authorization process. However, we need to
      // make sure we clean
      // up thread local immediately after we used the information, otherwise
      // some other people
      // security my be screwed up, on account of thread local security stack
      // being corrupted.

      securityStore.authenticate(username, password);

      ServerSession currentSession = sessions.remove(name);

      if (currentSession != null)
      {
         // This session may well be on a different connection and different channel id, so we must get rid
         // of it and create another
         currentSession.getChannel().close();
      }

      Channel channel = connection.getChannel(channelID, sendWindowSize, false);

      Channel replicatingChannel = getReplicatingChannel();

      final ServerSessionImpl session = new ServerSessionImpl(name,
                                                              oppositeChannelID,
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
                                                              queueFactory,
                                                              this,
                                                              configuration.getManagementAddress(),
                                                              replicatingChannel,
                                                              backup);

      sessions.put(name, session);

      ServerSessionPacketHandler handler = new ServerSessionPacketHandler(session);

      session.setHandler(handler);

      channel.setHandler(handler);

      connection.addFailureListener(session);

      return new CreateSessionResponseMessage(version.getIncrementingVersion());
   }

   private Transformer instantiateTransformer(final String transformerClassName)
   {
      Transformer transformer = null;

      if (transformerClassName != null)
      {
         ClassLoader loader = Thread.currentThread().getContextClassLoader();
         try
         {
            Class<?> clz = loader.loadClass(transformerClassName);
            transformer = (Transformer)clz.newInstance();
         }
         catch (Exception e)
         {
            throw new IllegalArgumentException("Error instantiating transformer class \"" + transformerClassName + "\"",
                                               e);
         }
      }
      return transformer;
   }

   // Inner classes
   // --------------------------------------------------------------------------------

   private class ActivateRunner implements Runnable
   {
      private Queue queue;

      ActivateRunner(final Queue queue)
      {
         this.queue = queue;
      }

      public void run()
      {
         queue.activateNow(asyncDeliveryPool);
      }
   }

   private class NoCacheConnectionLifeCycleListener implements ConnectionLifeCycleListener
   {
      private RemotingConnection conn;

      public void connectionCreated(final Connection connection)
      {
      }

      public void connectionDestroyed(final Object connectionID)
      {
         if (conn != null)
         {
            conn.destroy();
         }
      }

      public void connectionException(final Object connectionID, final MessagingException me)
      {
         backupConnectorFactory = null;

         if (conn != null)
         {
            // Execute on different thread to avoid deadlocks
            new Thread()
            {
               public void run()
               {
                  conn.fail(me);
               }
            }.start();
         }
      }
   }

}
