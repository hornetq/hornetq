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

import static org.hornetq.core.server.impl.QuorumManager.BACKUP_ACTIVATION.FAILURE_REPLICATING;
import static org.hornetq.core.server.impl.QuorumManager.BACKUP_ACTIVATION.FAIL_OVER;
import static org.hornetq.core.server.impl.QuorumManager.BACKUP_ACTIVATION.STOP;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Array;
import java.nio.channels.ClosedChannelException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;

import org.hornetq.api.config.HornetQDefaultConfiguration;
import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.HornetQAlreadyReplicatingException;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQIllegalStateException;
import org.hornetq.api.core.HornetQInternalErrorException;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.TopologyMember;
import org.hornetq.core.asyncio.impl.AsynchronousFileImpl;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.config.BridgeConfiguration;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.ConfigurationUtils;
import org.hornetq.core.config.CoreQueueConfiguration;
import org.hornetq.core.config.DivertConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.deployers.Deployer;
import org.hornetq.core.deployers.DeploymentManager;
import org.hornetq.core.deployers.impl.AddressSettingsDeployer;
import org.hornetq.core.deployers.impl.BasicUserCredentialsDeployer;
import org.hornetq.core.deployers.impl.FileDeploymentManager;
import org.hornetq.core.deployers.impl.QueueDeployer;
import org.hornetq.core.deployers.impl.SecurityDeployer;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.filter.impl.FilterImpl;
import org.hornetq.core.journal.IOCriticalErrorListener;
import org.hornetq.core.journal.JournalLoadInformation;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.journal.impl.SyncSpeedTest;
import org.hornetq.core.management.impl.HornetQServerControlImpl;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.paging.cursor.PageSubscription;
import org.hornetq.core.paging.impl.PagingManagerImpl;
import org.hornetq.core.paging.impl.PagingStoreFactoryNIO;
import org.hornetq.core.persistence.GroupingInfo;
import org.hornetq.core.persistence.OperationContext;
import org.hornetq.core.persistence.QueueBindingInfo;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.persistence.config.PersistedAddressSetting;
import org.hornetq.core.persistence.config.PersistedRoles;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager;
import org.hornetq.core.persistence.impl.journal.OperationContextImpl;
import org.hornetq.core.persistence.impl.nullpm.NullStorageManager;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.DuplicateIDCache;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.postoffice.QueueBinding;
import org.hornetq.core.postoffice.impl.DivertBinding;
import org.hornetq.core.postoffice.impl.LocalQueueBinding;
import org.hornetq.core.postoffice.impl.PostOfficeImpl;
import org.hornetq.core.protocol.ServerPacketDecoder;
import org.hornetq.core.protocol.core.Channel;
import org.hornetq.core.protocol.core.CoreRemotingConnection;
import org.hornetq.core.protocol.core.impl.ChannelImpl;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationLiveIsStoppingMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationLiveIsStoppingMessage.LiveStopping;
import org.hornetq.core.remoting.CloseListener;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.core.remoting.server.RemotingService;
import org.hornetq.core.remoting.server.impl.RemotingServiceImpl;
import org.hornetq.core.replication.ReplicationEndpoint;
import org.hornetq.core.replication.ReplicationManager;
import org.hornetq.core.security.CheckType;
import org.hornetq.core.security.Role;
import org.hornetq.core.security.SecurityStore;
import org.hornetq.core.security.impl.SecurityStoreImpl;
import org.hornetq.core.server.ActivateCallback;
import org.hornetq.core.server.Bindable;
import org.hornetq.core.server.Divert;
import org.hornetq.core.server.HornetQComponent;
import org.hornetq.core.server.HornetQMessageBundle;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.JournalType;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.LiveNodeLocator;
import org.hornetq.core.server.MemoryManager;
import org.hornetq.core.server.NodeManager;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.QueueFactory;
import org.hornetq.core.server.ServerSession;
import org.hornetq.core.server.cluster.ClusterConnection;
import org.hornetq.core.server.cluster.ClusterManager;
import org.hornetq.core.server.cluster.Transformer;
import org.hornetq.core.server.group.GroupingHandler;
import org.hornetq.core.server.group.impl.GroupBinding;
import org.hornetq.core.server.group.impl.GroupingHandlerConfiguration;
import org.hornetq.core.server.group.impl.LocalGroupingHandler;
import org.hornetq.core.server.group.impl.RemoteGroupingHandler;
import org.hornetq.core.server.impl.QuorumManager.BACKUP_ACTIVATION;
import org.hornetq.core.server.management.ManagementService;
import org.hornetq.core.server.management.impl.ManagementServiceImpl;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.settings.impl.HierarchicalObjectRepository;
import org.hornetq.core.transaction.ResourceManager;
import org.hornetq.core.transaction.impl.ResourceManagerImpl;
import org.hornetq.core.version.Version;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.protocol.SessionCallback;
import org.hornetq.spi.core.security.HornetQSecurityManager;
import org.hornetq.utils.ClassloadingUtil;
import org.hornetq.utils.ConcurrentHashSet;
import org.hornetq.utils.ExecutorFactory;
import org.hornetq.utils.HornetQThreadFactory;
import org.hornetq.utils.OrderedExecutorFactory;
import org.hornetq.utils.ReusableLatch;
import org.hornetq.utils.SecurityFormatter;
import org.hornetq.utils.VersionLoader;

/**
 * The HornetQ server implementation
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ataylor@redhat.com>Andy Taylor</a>
 */
public class HornetQServerImpl implements HornetQServer
{
   /**
    * JMS Topics (which are outside of the scope of the core API) will require a dumb subscription
    * with a dummy-filter at this current version as a way to keep its existence valid and TCK
    * tests. That subscription needs an invalid filter, however paging needs to ignore any
    * subscription with this filter. For that reason, this filter needs to be rejected on paging or
    * any other component on the system, and just be ignored for any purpose It's declared here as
    * this filter is considered a global ignore
    */
   public static final String GENERIC_IGNORED_FILTER = "__HQX=-1";

   enum SERVER_STATE
   {
      /**
       * start() has been called but components are not initialized. The whole point of this state,
       * is to be in a state which is different from {@link SERVER_STATE#STARTED} and
       * {@link SERVER_STATE#STOPPED}, so that methods testing for these two values such as
       * {@link #stop(boolean,boolean)} work as intended.
       */
      STARTING,
      /**
       * server is started. {@code server.isStarted()} returns {@code true}, and all assumptions
       * about it hold.
       */
      STARTED,
      /**
       * stop() was called but has not finished yet. Meant to avoids starting components while
       * stop() is executing.
       */
      STOPPING,
      /**
       * Stopped: either stop() has been called and has finished running, or start() has never been
       * called.
       */
      STOPPED;
   }
   private volatile SERVER_STATE state = SERVER_STATE.STOPPED;

   private final Version version;

   private final HornetQSecurityManager securityManager;

   private final Configuration configuration;

   private final MBeanServer mbeanServer;

   private volatile SecurityStore securityStore;

   private final HierarchicalRepository<AddressSettings> addressSettingsRepository;

   private volatile QueueFactory queueFactory;

   private volatile PagingManager pagingManager;

   private volatile PostOffice postOffice;

   private volatile ExecutorService threadPool;

   private volatile ScheduledExecutorService scheduledPool;

   private volatile ExecutorFactory executorFactory;

   private final HierarchicalRepository<Set<Role>> securityRepository;

   private volatile ResourceManager resourceManager;

   private volatile HornetQServerControlImpl messagingServerControl;

   private volatile ClusterManager clusterManager;

   private volatile StorageManager storageManager;

   private volatile RemotingService remotingService;

   private volatile ManagementService managementService;

   private volatile ConnectorsService connectorsService;

   private MemoryManager memoryManager;

   private volatile DeploymentManager deploymentManager;

   private Deployer basicUserCredentialsDeployer;
   private Deployer addressSettingsDeployer;
   private Deployer queueDeployer;
   private Deployer securityDeployer;

   private final Map<String, ServerSession> sessions = new ConcurrentHashMap<String, ServerSession>();

   /**
    * This class here has the same principle of CountDownLatch but you can reuse the counters.
    * It's based on the same super classes of {@code CountDownLatch}
    */
   private final ReusableLatch activationLatch = new ReusableLatch(0);

   private final ReusableLatch backupSyncLatch = new ReusableLatch(0);

   private final Object replicationLock = new Object();

   /**
    * Only applicable to 'remote backup servers'. If this flag is false the backup may not become
    * 'live'.
    */
   private volatile boolean backupUpToDate = true;

   private ReplicationManager replicationManager;

   private ReplicationEndpoint replicationEndpoint;

   private final Set<ActivateCallback> activateCallbacks = new ConcurrentHashSet<ActivateCallback>();

   private volatile GroupingHandler groupingHandler;

   private NodeManager nodeManager;

   // Used to identify the server on tests... useful on debugging testcases
   private String identity;

   private Thread backupActivationThread;

   private Activation activation;

   private final ShutdownOnCriticalErrorListener shutdownOnCriticalIO = new ShutdownOnCriticalErrorListener();

   private final Object failbackCheckerGuard = new Object();
   private boolean cancelFailBackChecker;

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

   public HornetQServerImpl(final Configuration configuration, final MBeanServer mbeanServer)
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

      addressSettingsRepository = new HierarchicalObjectRepository<AddressSettings>();

      addressSettingsRepository.setDefault(new AddressSettings());

      securityRepository = new HierarchicalObjectRepository<Set<Role>>();

      securityRepository.setDefault(new HashSet<Role>());

   }

   // life-cycle methods
   // ----------------------------------------------------------------

   /*
    * Can be overridden for tests
    */
   protected NodeManager createNodeManager(final String directory, String nodeGroupName, boolean replicatingBackup)
   {
      NodeManager manager;
      if (configuration.getJournalType() == JournalType.ASYNCIO && AsynchronousFileImpl.isLoaded())
      {
         manager = new AIOFileLockNodeManager(directory, replicatingBackup);
      }
      else
      {
         manager = new FileLockNodeManager(directory, replicatingBackup);
      }
      manager.setNodeGroupName(nodeGroupName);
      return manager;
   }

   public final synchronized void start() throws Exception
   {
      if (state != SERVER_STATE.STOPPED)
      {
         HornetQServerLogger.LOGGER.debug("Server already started!");
         return;
      }
      synchronized (failbackCheckerGuard)
      {
         cancelFailBackChecker = false;
      }
      state = SERVER_STATE.STARTING;

      activationLatch.setCount(1);

      HornetQServerLogger.LOGGER.debug("Starting server " + this);

      OperationContextImpl.clearContext();

      try
      {
         checkJournalDirectory();

         nodeManager =
                  createNodeManager(configuration.getJournalDirectory(), configuration.getBackupGroupName(), false);

         nodeManager.start();

         HornetQServerLogger.LOGGER.serverStarting((configuration.isBackup() ? "backup" : "live"),  configuration);

         if (configuration.isRunSyncSpeedTest())
         {
            SyncSpeedTest test = new SyncSpeedTest();

            test.run();
         }

         final boolean wasLive = !configuration.isBackup();
         if (!configuration.isBackup())
         {
            if (configuration.isSharedStore() && configuration.isPersistenceEnabled())
            {
               activation = new SharedStoreLiveActivation();
            }
            else
            {
               activation = new SharedNothingLiveActivation();
            }

            activation.run();
         }
         // The activation on fail-back may change the value of isBackup, for that reason we are
         // checking again here
         if (configuration.isBackup())
         {
            if (configuration.isSharedStore())
            {
               activation = new SharedStoreBackupActivation();
            }
            else
            {
               assert replicationEndpoint == null;
               nodeManager.stop();
               nodeManager =
                        createNodeManager(configuration.getJournalDirectory(), configuration.getBackupGroupName(), true);
               backupUpToDate = false;
               backupSyncLatch.setCount(1);
               replicationEndpoint = new ReplicationEndpoint(this, shutdownOnCriticalIO, wasLive);
               activation = new SharedNothingBackupActivation(wasLive);
            }

            backupActivationThread = new Thread(activation, HornetQMessageBundle.BUNDLE.activationForServer(this));
            backupActivationThread.start();
         }
         else
         {
            state = SERVER_STATE.STARTED;
            HornetQServerLogger.LOGGER.serverStarted(getVersion().getFullVersion(), nodeManager.getNodeId(),
               identity != null ? identity : "");
         }
         // start connector service
         connectorsService = new ConnectorsService(configuration, storageManager, scheduledPool, postOffice);
         connectorsService.start();
      }
      finally
      {
         // this avoids embedded applications using dirty contexts from startup
         OperationContextImpl.clearContext();
      }
   }

   @Override
   protected final void finalize() throws Throwable
   {
      if (state != SERVER_STATE.STOPPED)
      {
         HornetQServerLogger.LOGGER.serverFinalisedWIthoutBeingSTopped();

         stop();
      }

      super.finalize();
   }

   /**
    * Stops the server in a different thread.
    */
   public final void stopTheServer(final boolean criticalIOError)
   {
      ExecutorService executor = Executors.newSingleThreadExecutor();
      executor.submit(new Runnable()
      {
         @Override
         public void run()
         {
            try
            {
               stop(configuration.isFailoverOnServerShutdown(), criticalIOError, false);
            }
            catch (Exception e)
            {
               HornetQServerLogger.LOGGER.errorStoppingServer(e);
            }
         }
      });
   }

   public final void stop() throws Exception
   {
      stop(configuration.isFailoverOnServerShutdown());
   }

   public void threadDump(final String reason)
   {
      StringWriter str = new StringWriter();
      PrintWriter out = new PrintWriter(str);

      Map<Thread, StackTraceElement[]> stackTrace = Thread.getAllStackTraces();

      out.println(HornetQMessageBundle.BUNDLE.generatingThreadDump(reason));
      out.println("*******************************************************************************");

      for (Map.Entry<Thread, StackTraceElement[]> el : stackTrace.entrySet())
      {
         out.println("===============================================================================");
         out.println(HornetQMessageBundle.BUNDLE.threadDump(el.getKey(), el.getKey().getName(), el.getKey().getId(), el.getKey().getThreadGroup()));
         out.println();
         for (StackTraceElement traceEl : el.getValue())
         {
            out.println(traceEl);
         }
      }

      out.println("===============================================================================");
      out.println(HornetQMessageBundle.BUNDLE.endThreadDump());
      out.println("*******************************************************************************");

      HornetQServerLogger.LOGGER.warn(str.toString());
   }

   public final void stop(boolean failoverOnServerShutdown) throws Exception
   {
      stop(failoverOnServerShutdown, false, false);
   }
   /*
   * stops the server
   * @param failoverOnServerShutdown whether we will allow a backup server to become live when the server is stopped normally
   * @param criticalIOError whether we have encountered an IO error with the journal etc
   * @param failingBack if true don't set the flag to stop the failback checker
   */
   private void stop(boolean failoverOnServerShutdown, final boolean criticalIOError, boolean failingBack) throws Exception
   {
      if (!failingBack)
      {
         synchronized (failbackCheckerGuard)
         {
            cancelFailBackChecker = true;
         }
      }

      synchronized (this)
      {
         if (state == SERVER_STATE.STOPPED || state == SERVER_STATE.STOPPING)
         {
            return;
         }
         state = SERVER_STATE.STOPPING;

         final ReplicationManager localReplicationManager = getReplicationManager();

         if (localReplicationManager != null)
         {
            replicationManager.sendLiveIsStopping(LiveStopping.STOP_CALLED);
            // Schedule for 10 seconds
            // this pool gets a 'hard' shutdown, no need to manage the Future of this Runnable.
            scheduledPool.schedule(new Runnable()
            {
               @Override
               public void run()
               {
                  localReplicationManager.clearReplicationTokens();
               }
            }, 30, TimeUnit.SECONDS);
         }
         stopComponent(connectorsService);

         // we stop the groupingHandler before we stop the cluster manager so binding mappings
         // aren't removed in case of failover
         if (groupingHandler != null)
         {
            managementService.removeNotificationListener(groupingHandler);
            groupingHandler = null;
         }
         stopComponent(clusterManager);
         freezeConnections();
      }
      closeAllServerSessions(criticalIOError);

      // *************************************************************************************************************
      // There's no need to sync this part of the method, since the state stopped | stopping is checked within the sync
      //
      // we can't synchronized the whole method here as that would cause a deadlock
      // so stop is checking for stopped || stopping inside the lock
      // which will be already enough to guarantee that no other thread will be accessing this method here.
      //
      // *************************************************************************************************************

      if (storageManager != null)
         storageManager.clearContext();

      //before we stop any components deactivate any callbacks
      callDeActiveCallbacks();

      synchronized (this)
      {
         // Stop the deployers
         if (configuration.isFileDeploymentEnabled())
         {
            stopComponent(basicUserCredentialsDeployer);
            stopComponent(addressSettingsDeployer);
            stopComponent(queueDeployer);
            stopComponent(securityDeployer);
            stopComponent(deploymentManager);
         }

         if (managementService != null)
            managementService.unregisterServer();

         stopComponent(managementService);
         stopComponent(replicationEndpoint); // applies to a "backup" server
         stopComponent(pagingManager);

         if (storageManager != null)
            storageManager.stop(criticalIOError);

         // We stop remotingService before otherwise we may lock the system in case of a critical IO
         // error shutdown
         if (remotingService != null)
            remotingService.stop(criticalIOError);

         stopComponent(securityManager);
         stopComponent(resourceManager);

         stopComponent(postOffice);

         if (scheduledPool != null)
         {
            // we just interrupt all running tasks, these are supposed to be pings and the like.
            scheduledPool.shutdownNow();
         }

         stopComponent(memoryManager);

         if (threadPool != null)
         {
            threadPool.shutdown();
            try
            {
               if (!threadPool.awaitTermination(10, TimeUnit.SECONDS))
               {
                  HornetQServerLogger.LOGGER.timedOutStoppingThreadpool(threadPool);
                  for (Runnable r : threadPool.shutdownNow())
                  {
                     HornetQServerLogger.LOGGER.debug("Cancelled the execution of " + r);
                  }
               }
            }
            catch (InterruptedException e)
            {
               // Ignore
            }
         }

         scheduledPool = null;
         threadPool = null;

         if (securityStore != null)
            securityStore.stop();

         threadPool = null;

         scheduledPool = null;

         pagingManager = null;
         securityStore = null;
         resourceManager = null;
         replicationManager = null;
         replicationEndpoint = null;
         postOffice = null;
         queueFactory = null;
         resourceManager = null;
         messagingServerControl = null;
         memoryManager = null;

         sessions.clear();

         state = SERVER_STATE.STOPPED;

         activationLatch.setCount(1);

         // to display in the log message
         SimpleString tempNodeID = getNodeID();
         if (activation != null)
         {
            activation.close(failoverOnServerShutdown);
         }
         if (backupActivationThread != null)
         {

            backupActivationThread.join(30000);
            if (backupActivationThread.isAlive())
            {
               HornetQServerLogger.LOGGER.backupActivationDidntFinish(this);
               backupActivationThread.interrupt();
            }
         }

         stopComponent(nodeManager);

         nodeManager = null;

         addressSettingsRepository.clearListeners();

         addressSettingsRepository.clearCache();
         if (identity != null)
         {
            HornetQServerLogger.LOGGER.serverStopped("identity=" + identity + ",version=" + getVersion().getFullVersion(),
               tempNodeID);
         }
         else
         {
            HornetQServerLogger.LOGGER.serverStopped(getVersion().getFullVersion(), tempNodeID);
         }
      }
   }

   /**
    * Freeze all connections.
    * <p>
    * If replicating, avoid freezing the replication connection. Helper method for
    * {@link #stop(boolean, boolean, boolean)}.
    */
   private void freezeConnections()
   {
      if (state != SERVER_STATE.STOPPING)
      {
         throw new IllegalStateException();
      }
      ReplicationManager localReplicationManager = getReplicationManager();
      if (localReplicationManager != null)
      {
         remotingService.freeze(localReplicationManager.getBackupTransportConnection());
      }
      else
      {
         remotingService.freeze(null);
      }
   }

   /**
    * We close all the exception in an attempt to let any pending IO to finish to avoid scenarios
    * where the send or ACK got to disk but the response didn't get to the client It may still be
    * possible to have this scenario on a real failure (without the use of XA) But at least we will
    * do our best to avoid it on regular shutdowns
    */
   private void closeAllServerSessions(final boolean criticalIOError)
   {
      if (state != SERVER_STATE.STOPPING)
      {
         throw new IllegalStateException();
      }
      for (ServerSession session : sessions.values())
      {
         try
         {
            session.close(true);
         }
         catch (Exception e)
         {
            // If anything went wrong with closing sessions.. we should ignore it
            // such as transactions.. etc.
            HornetQServerLogger.LOGGER.errorClosingSessionsWhileStoppingServer(e);
         }
      }
      if (!criticalIOError)
      {
         for (ServerSession session : sessions.values())
         {
            try
            {
               session.waitContextCompletion();
            }
            catch (Exception e)
            {
               HornetQServerLogger.LOGGER.errorClosingSessionsWhileStoppingServer(e);
            }
         }
      }

   }

   private static void stopComponent(HornetQComponent component) throws Exception
   {
      if (component != null)
         component.stop();
   }

   // HornetQServer implementation
   // -----------------------------------------------------------

   public String describe()
   {
      StringWriter str = new StringWriter();
      PrintWriter out = new PrintWriter(str);

      out.println(HornetQMessageBundle.BUNDLE.serverDescribe(identity, getClusterManager().describe()));

      return str.toString();
   }

   public String destroyConnectionWithSessionMetadata(String metaKey, String parameterValue) throws Exception
   {
      StringBuffer operationsExecuted = new StringBuffer();

      try
      {
         operationsExecuted.append("**************************************************************************************************\n");
         operationsExecuted.append(HornetQMessageBundle.BUNDLE.destroyConnectionWithSessionMetadataHeader(metaKey, parameterValue) + "\n");

         Set<ServerSession> allSessions = getSessions();

         ServerSession sessionFound = null;
         for (ServerSession session : allSessions)
         {
            try
            {
               String value = session.getMetaData(metaKey);
               if (value != null && value.equals(parameterValue))
               {
                  sessionFound = session;
                  operationsExecuted.append(HornetQMessageBundle.BUNDLE.destroyConnectionWithSessionMetadataClosingConnection(sessionFound.toString()) + "\n");
                  RemotingConnection conn = session.getRemotingConnection();
                  if (conn != null)
                  {
                     conn.fail(HornetQMessageBundle.BUNDLE.destroyConnectionWithSessionMetadataSendException(metaKey, parameterValue));
                  }
                  session.close(true);
                  sessions.remove(session.getName());
               }
            }
            catch (Throwable e)
            {
               HornetQServerLogger.LOGGER.warn(e.getMessage(), e);
            }
         }

         if (sessionFound == null)
         {
            operationsExecuted.append(HornetQMessageBundle.BUNDLE.destroyConnectionWithSessionMetadataNoSessionFound(metaKey, parameterValue) + "\n");
         }

         operationsExecuted.append("**************************************************************************************************");

         return operationsExecuted.toString();
      }
      finally
      {
         // This operation is critical for the knowledge of the admin, so we need to add info logs for later knowledge
         HornetQServerLogger.LOGGER.info(operationsExecuted.toString());
      }

   }

   public void setIdentity(String identity)
   {
      this.identity = identity;
   }

   public String getIdentity()
   {
      return identity;
   }

   public ScheduledExecutorService getScheduledPool()
   {
      return scheduledPool;
   }

   public Configuration getConfiguration()
   {
      return configuration;
   }

   public PagingManager getPagingManager()
   {
      return pagingManager;
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

   public NodeManager getNodeManager()
   {
      return nodeManager;
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
      return state == SERVER_STATE.STARTED;
   }

   public ClusterManager getClusterManager()
   {
      return clusterManager;
   }

   public ServerSession createSession(final String name,
                                      final String username,
                                      final String password,
                                      final int minLargeMessageSize,
                                      final RemotingConnection connection,
                                      final boolean autoCommitSends,
                                      final boolean autoCommitAcks,
                                      final boolean preAcknowledge,
                                      final boolean xa,
                                      final String defaultAddress,
                                      final SessionCallback callback) throws Exception
   {

      if (securityStore != null)
      {
         securityStore.authenticate(username, password);
      }
      final OperationContext context = storageManager.newContext(getExecutorFactory().getExecutor());
      final ServerSessionImpl session = internalCreateSession(name, username, password, minLargeMessageSize, connection, autoCommitSends, autoCommitAcks, preAcknowledge, xa, defaultAddress, callback, context);

      sessions.put(name, session);

      return session;
   }

   protected ServerSessionImpl internalCreateSession(String name, String username, String password, int minLargeMessageSize, RemotingConnection connection, boolean autoCommitSends, boolean autoCommitAcks, boolean preAcknowledge, boolean xa, String defaultAddress, SessionCallback callback, OperationContext context) throws Exception
   {
      return new ServerSessionImpl(name,
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
            managementService,
            this,
            configuration.getManagementAddress(),
            defaultAddress == null ? null
               : new SimpleString(defaultAddress),
            callback,
            context);
   }

   protected SecurityStore getSecurityStore()
   {
      return securityStore;
   }

   public void removeSession(final String name) throws Exception
   {
      sessions.remove(name);
   }

   public ServerSession lookupSession(String key, String value)
   {
      // getSessions is called here in a try to minimize locking the Server while this check is being done
      Set<ServerSession> allSessions = getSessions();

      for (ServerSession session : allSessions)
      {
         String metaValue = session.getMetaData(key);
         if (metaValue != null && metaValue.equals(value))
         {
            return session;
         }
      }

      return null;
   }

   public synchronized List<ServerSession> getSessions(final String connectionID)
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

   public synchronized Set<ServerSession> getSessions()
   {
      return new HashSet<ServerSession>(sessions.values());
   }

   @Override
   public boolean isActive()
   {
      return activationLatch.getCount() < 1;
   }

   @Override
   public boolean waitForActivation(long timeout, TimeUnit unit) throws InterruptedException
   {
      return activationLatch.await(timeout, unit);
   }

   @Override
   public boolean waitForBackupSync(long timeout, TimeUnit unit) throws InterruptedException
   {
      if (configuration.isBackup() && !configuration.isSharedStore())
      {
         return backupSyncLatch.await(timeout, unit);
      }
      else
      {
         return true;
      }
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
      return nodeManager == null ? null : nodeManager.getNodeId();
   }

   public Queue createQueue(final SimpleString address,
                            final SimpleString queueName,
                            final SimpleString filterString,
                            final boolean durable,
                            final boolean temporary) throws Exception
   {
      return createQueue(address, queueName, filterString, durable, temporary, false);
   }

   public Queue locateQueue(SimpleString queueName) throws Exception
   {
      Binding binding = postOffice.getBinding(queueName);

      if (binding == null)
      {
         return null;
      }

      Bindable queue = binding.getBindable();

      if (!(queue instanceof Queue))
      {
         throw new IllegalStateException("locateQueue should only be used to locate queues");
      }

      return (Queue)binding.getBindable();
   }

   public Queue deployQueue(final SimpleString address,
                            final SimpleString queueName,
                            final SimpleString filterString,
                            final boolean durable,
                            final boolean temporary) throws Exception
   {
      HornetQServerLogger.LOGGER.deployQueue(queueName);

      return createQueue(address, queueName, filterString, durable, temporary, true);
   }

   public void destroyQueue(final SimpleString queueName) throws Exception
   {
      // The session is passed as an argument to verify if the user has authorization to delete the queue
      // in some cases (such as temporary queues) this should happen regardless of the authorization
      // since that will only happen during a session close, which will be used to cleanup on temporary queues
      destroyQueue(queueName, null, true);
   }

   public void destroyQueue(final SimpleString queueName, final ServerSession session, final boolean checkConsumerCount) throws Exception
    {
      addressSettingsRepository.clearCache();

      Binding binding = postOffice.getBinding(queueName);

      if (binding == null)
      {
         throw HornetQMessageBundle.BUNDLE.noSuchQueue(queueName);
      }

      Queue queue = (Queue)binding.getBindable();

      // This check is only valid if checkConsumerCount == true
      if (checkConsumerCount && queue.getConsumerCount() != 0)
      {
         throw HornetQMessageBundle.BUNDLE.cannotDeleteQueue(queue.getName(), queueName, binding.getClass().getName());
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

      queue.deleteQueue();
   }


   public void registerActivateCallback(final ActivateCallback callback)
   {
      activateCallbacks.add(callback);
   }

   public void unregisterActivateCallback(final ActivateCallback callback)
   {
      activateCallbacks.remove(callback);
   }

   public ExecutorFactory getExecutorFactory()
   {
      return executorFactory;
   }

   public void setGroupingHandler(final GroupingHandler groupingHandler)
   {
      this.groupingHandler = groupingHandler;
   }

   public GroupingHandler getGroupingHandler()
   {
      return groupingHandler;
   }

   public ReplicationEndpoint getReplicationEndpoint()
   {
      return replicationEndpoint;
   }

   public ReplicationManager getReplicationManager()
   {
      synchronized (replicationLock)
      {
      return replicationManager;
      }
   }

   public ConnectorsService getConnectorsService()
   {
      return connectorsService;
   }

   public void deployDivert(DivertConfiguration config) throws Exception
   {
      if (config.getName() == null)
      {
         HornetQServerLogger.LOGGER.divertWithNoName();

         return;
      }

      if (config.getAddress() == null)
      {
         HornetQServerLogger.LOGGER.divertWithNoAddress();

         return;
      }

      if (config.getForwardingAddress() == null)
      {
         HornetQServerLogger.LOGGER.divertWithNoForwardingAddress();

         return;
      }

      SimpleString sName = new SimpleString(config.getName());

      if (postOffice.getBinding(sName) != null)
      {
         HornetQServerLogger.LOGGER.divertBindingNotExists(sName);

         return;
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
         postOffice,
         storageManager);

      Binding binding = new DivertBinding(storageManager.generateUniqueID(), sAddress, divert);

      postOffice.addBinding(binding);

      managementService.registerDivert(divert, config);
   }

   public void destroyDivert(SimpleString name) throws Exception
   {
      Binding binding = postOffice.getBinding(name);
      if (binding == null)
      {
         throw HornetQMessageBundle.BUNDLE.noBindingForDivert(name);
      }
      if (!(binding instanceof DivertBinding))
      {
         throw HornetQMessageBundle.BUNDLE.bindingNotDivert(name);
      }

      postOffice.removeBinding(name, null);
   }

   public void deployBridge(BridgeConfiguration config) throws Exception
   {
      if (clusterManager != null)
      {
         clusterManager.deployBridge(config);
      }
   }

   public void destroyBridge(String name) throws Exception
   {
      if (clusterManager != null)
      {
         clusterManager.destroyBridge(name);
      }
   }

   public ServerSession getSessionByID(String sessionName)
   {
      return sessions.get(sessionName);
   }

   // PUBLIC -------

   @Override
   public String toString()
   {
      if (identity != null)
      {
         return "HornetQServerImpl::" + identity;
      }
      return "HornetQServerImpl::" + (nodeManager != null ? "serverUUID=" + nodeManager.getUUID() : "");
   }

   /**
    * For tests only, don't use this method as it's not part of the API
    * @param factory
    */
   public void replaceQueueFactory(QueueFactory factory)
   {
      this.queueFactory = factory;
   }


   private PagingManager createPagingManager()
   {

      return new PagingManagerImpl(new PagingStoreFactoryNIO(storageManager, configuration.getPagingDirectory(),
         configuration.getJournalBufferSize_NIO(),
         scheduledPool,
         executorFactory,
         configuration.isJournalSyncNonTransactional(),
         shutdownOnCriticalIO),
      addressSettingsRepository);
   }

   /**
    * This method is protected as it may be used as a hook for creating a custom storage manager (on tests for instance)
    */
   private StorageManager createStorageManager()
   {
      if (configuration.isPersistenceEnabled())
      {
         return new JournalStorageManager(configuration, executorFactory, shutdownOnCriticalIO);
      }
      return new NullStorageManager();
   }

   private void callActivateCallbacks()
   {
      for (ActivateCallback callback : activateCallbacks)
      {
         callback.activated();
      }
   }

   private void callPreActiveCallbacks()
   {
      for (ActivateCallback callback : activateCallbacks)
      {
         callback.preActivate();
      }
   }

   private void callDeActiveCallbacks()
   {
      for (ActivateCallback callback : activateCallbacks)
      {
         callback.deActivate();
      }
   }


   /**
    * Starts everything apart from RemotingService and loading the data.
    * <p>
    * After optional intermediary steps, Part 1 is meant to be followed by part 2
    * {@link #initialisePart2()}.
    */
   private synchronized boolean initialisePart1() throws Exception
   {
      if (state == SERVER_STATE.STOPPED)
         return false;
      // Create the pools - we have two pools - one for non scheduled - and another for scheduled

      ThreadFactory tFactory = new HornetQThreadFactory("HornetQ-server-" + this.toString(),
         false,
         getThisClassLoader());

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
         new HornetQThreadFactory("HornetQ-scheduled-threads",
            false,
            getThisClassLoader()));

      managementService = new ManagementServiceImpl(mbeanServer, configuration);

      if (configuration.getMemoryMeasureInterval() != -1)
      {
         memoryManager = new MemoryManager(configuration.getMemoryWarningThreshold(),
            configuration.getMemoryMeasureInterval());

         memoryManager.start();
      }

      // Create the hard-wired components

      if (configuration.isFileDeploymentEnabled())
      {
         deploymentManager = new FileDeploymentManager(configuration.getFileDeployerScanPeriod());
      }

      callPreActiveCallbacks();

      // startReplication();

      storageManager = createStorageManager();

      if (HornetQDefaultConfiguration.getDefaultClusterUser().equals(configuration.getClusterUser()) && HornetQDefaultConfiguration.getDefaultClusterPassword().equals(configuration.getClusterPassword()))
      {
         HornetQServerLogger.LOGGER.clusterSecurityRisk();
      }

      securityStore = new SecurityStoreImpl(securityRepository,
         securityManager,
         configuration.getSecurityInvalidationInterval(),
         configuration.isSecurityEnabled(),
         configuration.getClusterUser(),
         configuration.getClusterPassword(),
         managementService);

      queueFactory = new QueueFactoryImpl(executorFactory, scheduledPool, addressSettingsRepository, storageManager);

      pagingManager = createPagingManager();

      resourceManager = new ResourceManagerImpl((int)(configuration.getTransactionTimeout() / 1000),
         configuration.getTransactionTimeoutScanPeriod(),
         scheduledPool);
      postOffice = new PostOfficeImpl(this,
         storageManager,
         pagingManager,
         queueFactory,
         managementService,
         configuration.getMessageExpiryScanPeriod(),
         configuration.getMessageExpiryThreadPriority(),
         configuration.isWildcardRoutingEnabled(),
         configuration.getIDCacheSize(),
         configuration.isPersistIDCache(),
         addressSettingsRepository);

      // This can't be created until node id is set
      clusterManager =
               new ClusterManager(executorFactory, this, postOffice, scheduledPool, managementService, configuration,
                                  nodeManager, configuration.isBackup(), threadPool);

      clusterManager.deploy();

      remotingService = new RemotingServiceImpl(clusterManager, configuration, this, managementService, scheduledPool);

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

      deployAddressSettingsFromConfiguration();

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

      deploySecurityFromConfiguration();

      deployGroupingHandlerConfiguration(configuration.getGroupingHandlerConfiguration());
      return true;
   }

   /*
    * Load the data, and start remoting service so clients can connect
    */
   private synchronized void initialisePart2() throws Exception
   {
      // Load the journal and populate queues, transactions and caches in memory

      if (state == SERVER_STATE.STOPPED || state == SERVER_STATE.STOPPING)
      {
         return;
      }

      pagingManager.reloadStores();

      JournalLoadInformation[] journalInfo = loadJournals();

      final ServerInfo dumper = new ServerInfo(this, pagingManager);

      long dumpInfoInterval = configuration.getServerDumpInterval();

      if (dumpInfoInterval > 0)
      {
         scheduledPool.scheduleWithFixedDelay(new Runnable()
         {
            public void run()
            {
               HornetQServerLogger.LOGGER.dumpServerInfo(dumper.dump());
            }
         }, 0, dumpInfoInterval, TimeUnit.MILLISECONDS);
      }

      // Deploy the rest of the stuff

      // Deploy any predefined queues
      if (configuration.isFileDeploymentEnabled())
      {
         queueDeployer = new QueueDeployer(deploymentManager, this);

         queueDeployer.start();
      }
      else
      {
         deployQueuesFromConfiguration();
      }

      // We need to call this here, this gives any dependent server a chance to deploy its own addresses
      // this needs to be done before clustering is fully activated
      callActivateCallbacks();

      // Deploy any pre-defined diverts
      deployDiverts();

      if (deploymentManager != null)
      {
         deploymentManager.start();
      }

      // We do this at the end - we don't want things like MDBs or other connections connecting to a backup server until
      // it is activated

      remotingService.start();

      clusterManager.start();

      if (nodeManager.getNodeId() == null)
      {
         throw HornetQMessageBundle.BUNDLE.nodeIdNull();
      }
      activationLatch.countDown();

      // We can only do this after everything is started otherwise we may get nasty races with expired messages
      postOffice.startExpiryScanner();
   }

   private void deploySecurityFromConfiguration()
   {
      for (Map.Entry<String, Set<Role>> entry : configuration.getSecurityRoles().entrySet())
      {
         securityRepository.addMatch(entry.getKey(), entry.getValue(), true);
      }
   }

   private void deployQueuesFromConfiguration() throws Exception
   {
      for (CoreQueueConfiguration config : configuration.getQueueConfigurations())
      {
         deployQueue(SimpleString.toSimpleString(config.getAddress()),
            SimpleString.toSimpleString(config.getName()),
            SimpleString.toSimpleString(config.getFilterString()),
            config.isDurable(),
            false);
      }
   }

   private void deployAddressSettingsFromConfiguration()
   {
      for (Map.Entry<String, AddressSettings> entry : configuration.getAddressesSettings().entrySet())
      {
         addressSettingsRepository.addMatch(entry.getKey(), entry.getValue(), true);
      }
   }

   private JournalLoadInformation[] loadJournals() throws Exception
   {
      JournalLoadInformation[] journalInfo = new JournalLoadInformation[2];

      List<QueueBindingInfo> queueBindingInfos = new ArrayList<QueueBindingInfo>();

      List<GroupingInfo> groupingInfos = new ArrayList<GroupingInfo>();

      journalInfo[0] = storageManager.loadBindingJournal(queueBindingInfos, groupingInfos);

      recoverStoredConfigs();

      Map<Long, Queue> queues = new HashMap<Long, Queue>();
      Map<Long, QueueBindingInfo> queueBindingInfosMap = new HashMap<Long, QueueBindingInfo>();

      // Used to rename the queue in case there's a duplication
      int duplicateID = 0;
      for (QueueBindingInfo queueBindingInfo : queueBindingInfos)
      {
         queueBindingInfosMap.put(queueBindingInfo.getId(), queueBindingInfo);

         Filter filter = FilterImpl.createFilter(queueBindingInfo.getFilterString());

         boolean isTopicIdentification =
                  filter != null && filter.getFilterString() != null &&
                           filter.getFilterString().toString().equals(GENERIC_IGNORED_FILTER);

         if (postOffice.getBinding(queueBindingInfo.getQueueName()) != null)
         {

            if (isTopicIdentification)
            {
               long tx = storageManager.generateUniqueID();
               storageManager.deleteQueueBinding(tx, queueBindingInfo.getId());
               storageManager.commit(tx);
               continue;
            }
            else
            {

               SimpleString newName = queueBindingInfo.getQueueName().concat("-" + (duplicateID++));
               HornetQServerLogger.LOGGER.queueDuplicatedRenaming(queueBindingInfo.getQueueName().toString(), newName.toString());
               queueBindingInfo.replaceQueueName(newName);
            }
         }

         PageSubscription subscription = null;

         if (!isTopicIdentification)
         {
            subscription = pagingManager.getPageStore(queueBindingInfo.getAddress())
               .getCursorProvider()
               .createSubscription(queueBindingInfo.getId(), filter, true);
         }

         Queue queue = queueFactory.createQueue(queueBindingInfo.getId(),
            queueBindingInfo.getAddress(),
            queueBindingInfo.getQueueName(),
            filter,
            subscription,
            true,
            false);

         Binding binding = new LocalQueueBinding(queueBindingInfo.getAddress(), queue, nodeManager.getNodeId());

         queues.put(queueBindingInfo.getId(), queue);

         postOffice.addBinding(binding);

         managementService.registerAddress(queueBindingInfo.getAddress());
         managementService.registerQueue(queue, queueBindingInfo.getAddress(), storageManager);

      }

      for (GroupingInfo groupingInfo : groupingInfos)
      {
         if (groupingHandler != null)
         {
            groupingHandler.addGroupBinding(new GroupBinding(groupingInfo.getId(),
               groupingInfo.getGroupId(),
               groupingInfo.getClusterName()));
         }
      }

      Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap = new HashMap<SimpleString, List<Pair<byte[], Long>>>();

      HashSet<Pair<Long, Long>> pendingLargeMessages = new HashSet<Pair<Long, Long>>();

      journalInfo[1] = storageManager.loadMessageJournal(postOffice,
         pagingManager,
         resourceManager,
         queues,
         queueBindingInfosMap,
         duplicateIDMap,
         pendingLargeMessages);

      for (Map.Entry<SimpleString, List<Pair<byte[], Long>>> entry : duplicateIDMap.entrySet())
      {
         SimpleString address = entry.getKey();

         DuplicateIDCache cache = postOffice.getDuplicateIDCache(address);

         if (configuration.isPersistIDCache())
         {
            cache.load(entry.getValue());
         }
      }

      for (Pair<Long, Long> msgToDelete : pendingLargeMessages)
      {
         HornetQServerLogger.LOGGER.deletingPendingMessage(msgToDelete);
         LargeServerMessage msg = storageManager.createLargeMessage();
         msg.setMessageID(msgToDelete.getB());
         msg.setPendingRecordID(msgToDelete.getA());
         msg.setDurable(true);
         msg.deleteFile();
      }

      return journalInfo;
   }

   /**
    * @throws Exception
    */
   private void recoverStoredConfigs() throws Exception
   {
      List<PersistedAddressSetting> adsettings = storageManager.recoverAddressSettings();
      for (PersistedAddressSetting set : adsettings)
      {
         addressSettingsRepository.addMatch(set.getAddressMatch().toString(), set.getSetting());
      }

      List<PersistedRoles> roles = storageManager.recoverPersistedRoles();

      for (PersistedRoles roleItem : roles)
      {
         Set<Role> setRoles = SecurityFormatter.createSecurity(roleItem.getSendRoles(),
            roleItem.getConsumeRoles(),
            roleItem.getCreateDurableQueueRoles(),
            roleItem.getDeleteDurableQueueRoles(),
            roleItem.getCreateNonDurableQueueRoles(),
            roleItem.getDeleteNonDurableQueueRoles(),
            roleItem.getManageRoles());

         securityRepository.addMatch(roleItem.getAddressMatch().toString(), setRoles);
      }
   }

   private Queue createQueue(final SimpleString address,
                             final SimpleString queueName,
                             final SimpleString filterString,
                             final boolean durable,
                             final boolean temporary,
                             final boolean ignoreIfExists) throws Exception
   {
      QueueBinding binding = (QueueBinding)postOffice.getBinding(queueName);

      if (binding != null)
      {
         if (ignoreIfExists)
         {
            return binding.getQueue();
         }
         else
         {
            throw HornetQMessageBundle.BUNDLE.queueAlreadyExists(queueName);
         }
      }

      Filter filter = FilterImpl.createFilter(filterString);

      long txID = storageManager.generateUniqueID();;
      long queueID = storageManager.generateUniqueID();

      PageSubscription pageSubscription;

      if (filterString != null && filterString.toString().equals(GENERIC_IGNORED_FILTER))
      {
         pageSubscription = null;
      }
      else
      {
         pageSubscription = pagingManager.getPageStore(address)
            .getCursorProvider()
            .createSubscription(queueID, filter, durable);
      }

      final Queue queue = queueFactory.createQueue(queueID,
         address,
         queueName,
         filter,
         pageSubscription,
         durable,
         temporary);

      binding = new LocalQueueBinding(address, queue, nodeManager.getNodeId());

      if (durable)
      {
         storageManager.addQueueBinding(txID, binding);
      }

      try
      {
         postOffice.addBinding(binding);
         if (durable)
         {
            storageManager.commitBindings(txID);
         }
      }
      catch (Exception e)
      {
         try
         {
            if (durable)
            {
               storageManager.rollbackBindings(txID);
            }
            if (queue != null)
            {
               queue.close();
            }
            if (pageSubscription != null)
            {
               pageSubscription.destroy();
            }
         }
         catch (Throwable ignored)
         {
            HornetQServerLogger.LOGGER.debug(ignored.getMessage(), ignored);
         }
         throw e;
      }


      managementService.registerAddress(address);
      managementService.registerQueue(queue, address, storageManager);

      return queue;
   }

   private void deployDiverts() throws Exception
   {
      for (DivertConfiguration config : configuration.getDivertConfigurations())
      {
         deployDivert(config);
      }
   }

   private void deployGroupingHandlerConfiguration(final GroupingHandlerConfiguration config) throws Exception
   {
      if (config != null)
      {
         GroupingHandler groupingHandler1;
         if (config.getType() == GroupingHandlerConfiguration.TYPE.LOCAL)
         {
            groupingHandler1 =
                     new LocalGroupingHandler(managementService,
               config.getName(),
               config.getAddress(),
               getStorageManager(),
               config.getTimeout());
         }
         else
         {
            groupingHandler1 =
                     new RemoteGroupingHandler(managementService,
               config.getName(),
               config.getAddress(),
               config.getTimeout());
         }

         this.groupingHandler = groupingHandler1;

         managementService.addNotificationListener(groupingHandler1);
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
      return safeInitNewInstance(className);
   }

   private static ClassLoader getThisClassLoader()
   {
      return AccessController.doPrivileged(new PrivilegedAction<ClassLoader>()
      {
         public ClassLoader run()
         {
            return ClientSessionFactoryImpl.class.getClassLoader();
         }
      });

   }

   /**
    * Check if journal directory exists or create it (if configured to do so)
    */
   private void checkJournalDirectory()
   {
      File journalDir = new File(configuration.getJournalDirectory());

      if (!journalDir.exists())
      {
         if (configuration.isCreateJournalDir())
         {
            journalDir.mkdirs();
         }
         else
         {
            throw HornetQMessageBundle.BUNDLE.cannotCreateDir(journalDir.getAbsolutePath());
         }
      }
   }

   /**
    * To be called by backup trying to fail back the server
    */
   private void startFailbackChecker()
   {
      scheduledPool.scheduleAtFixedRate(new FailbackChecker(), 1000l, 1000l, TimeUnit.MILLISECONDS);
   }

   // Inner classes
   // --------------------------------------------------------------------------------

   private class FailbackChecker implements Runnable
   {
      private boolean restarting = false;

      public void run()
      {
         try
         {
            if (!restarting && nodeManager.isAwaitingFailback())
            {
               HornetQServerLogger.LOGGER.awaitFailBack();
               restarting = true;
               Thread t = new Thread(new Runnable()
               {
                  public void run()
                  {
                     try
                     {
                        HornetQServerLogger.LOGGER.debug(HornetQServerImpl.this + "::Stopping live node in favor of failback");
                        stop(true, false, true);
                        // We need to wait some time before we start the backup again
                        // otherwise we may eventually start before the live had a chance to get it
                        Thread.sleep(configuration.getFailbackDelay());
                        synchronized (failbackCheckerGuard)
                        {
                           if (cancelFailBackChecker)
                              return;
                           configuration.setBackup(true);
                           HornetQServerLogger.LOGGER.debug(HornetQServerImpl.this +
                                    "::Starting backup node now after failback");
                        start();
                     }
                     }
                     catch (Exception e)
                     {
                        HornetQServerLogger.LOGGER.serverRestartWarning();
                     }
                  }
               });
               t.start();
            }
         }
         catch (Exception e)
         {
            HornetQServerLogger.LOGGER.serverRestartWarning(e);
         }
      }
   }

   private final class SharedStoreLiveActivation implements Activation
   {
      public void run()
      {
         try
         {
            HornetQServerLogger.LOGGER.awaitingLiveLock();

            checkJournalDirectory();

            if (HornetQServerLogger.LOGGER.isDebugEnabled())
            {
               HornetQServerLogger.LOGGER.debug("First part initialization on " + this);
            }

            if (!initialisePart1())
               return;

            if (nodeManager.isBackupLive())
            {
               /*
                * looks like we've failed over at some point need to inform that we are the backup
                * so when the current live goes down they failover to us
                */
               if (HornetQServerLogger.LOGGER.isDebugEnabled())
               {
                  HornetQServerLogger.LOGGER.debug("announcing backup to the former live" + this);
               }

               clusterManager.announceBackup();
               Thread.sleep(configuration.getFailbackDelay());
            }

            nodeManager.startLiveNode();

            if (state == SERVER_STATE.STOPPED || state == SERVER_STATE.STOPPING)
            {
               return;
            }

            initialisePart2();

            HornetQServerLogger.LOGGER.serverIsLive();
         }
         catch (Exception e)
         {
            HornetQServerLogger.LOGGER.initializationError(e);
         }
      }

      public void close(boolean permanently) throws Exception
      {
         // TO avoid a NPE from stop
         NodeManager nodeManagerInUse = nodeManager;

         if (nodeManagerInUse != null)
         {
            if (permanently)
            {
               nodeManagerInUse.crashLiveServer();
            }
            else
            {
               nodeManagerInUse.pauseLiveServer();
            }
         }
      }
   }

   private final class SharedStoreBackupActivation implements Activation
   {
      public void run()
      {
         try
         {
            nodeManager.startBackup();

            if (!initialisePart1())
               return;

            clusterManager.start();

            state = SERVER_STATE.STARTED;

            HornetQServerLogger.LOGGER.backupServerStarted(version.getFullVersion(), nodeManager.getNodeId());

            nodeManager.awaitLiveNode();

            configuration.setBackup(false);

            if (state != SERVER_STATE.STARTED)
            {
               return;
            }

            initialisePart2();

            clusterManager.activate();

            HornetQServerLogger.LOGGER.backupServerIsLive();

            nodeManager.releaseBackup();
            if (configuration.isAllowAutoFailBack())
            {
               startFailbackChecker();
            }
         }
         catch (InterruptedException e)
         {
            // this is ok, we are being stopped
         }
         catch (ClosedChannelException e)
         {
            // this is ok too, we are being stopped
         }
         catch (Exception e)
         {
            if (!(e.getCause() instanceof InterruptedException))
            {
               HornetQServerLogger.LOGGER.initializationError(e);
            }
         }
         catch (Throwable e)
         {
            HornetQServerLogger.LOGGER.initializationError(e);
         }
      }

      public void close(boolean permanently) throws Exception
      {

         // To avoid a NPE cause by the stop
         NodeManager nodeManagerInUse = nodeManager;

         if (configuration.isBackup())
         {
            long timeout = 30000;

            long start = System.currentTimeMillis();

            while (backupActivationThread.isAlive() && System.currentTimeMillis() - start < timeout)
            {
               if (nodeManagerInUse != null)
               {
                  nodeManagerInUse.interrupt();
               }

               backupActivationThread.interrupt();

               backupActivationThread.join(1000);

            }

            if (System.currentTimeMillis() - start >= timeout)
            {
               threadDump("Timed out waiting for backup activation to exit");
            }

            if (nodeManagerInUse != null)
            {
               nodeManagerInUse.stopBackup();
            }
         }
         else
         {

            if (nodeManagerInUse != null)
            {
               // if we are now live, behave as live
               // We need to delete the file too, otherwise the backup will failover when we shutdown or if the backup is
               // started before the live
               if (permanently)
               {
                  nodeManagerInUse.crashLiveServer();
               }
               else
               {
                  nodeManagerInUse.pauseLiveServer();
               }
            }
         }
      }
   }

   private final class ShutdownOnCriticalErrorListener implements IOCriticalErrorListener
   {
      boolean failedAlready = false;

      public synchronized void onIOException(Exception cause, String message, SequentialFile file)
      {
         if (!failedAlready)
         {
            failedAlready = true;

            HornetQServerLogger.LOGGER.ioCriticalIOError(message, file.toString(), cause);

            stopTheServer(true);
         }
      }
   }

   private interface Activation extends Runnable
   {
      void close(boolean permanently) throws Exception;
   }

   private final class SharedNothingBackupActivation implements Activation
   {
      private volatile ServerLocatorInternal serverLocator0;
      private volatile QuorumManager quorumManager;
      private final boolean attemptFailBack;
      private String nodeID;
      ClientSessionFactoryInternal liveServerSessionFactory;
      private boolean closed;

      public SharedNothingBackupActivation(boolean attemptFailBack)
      {
         this.attemptFailBack = attemptFailBack;
      }

      public void run()
      {
         try
         {
            synchronized (HornetQServerImpl.this)
            {
               state = SERVER_STATE.STARTED;
            }
            // move all data away:
            nodeManager.stop();
            moveServerData();
            nodeManager.start();
            synchronized (this)
            {
               if (closed)
                  return;
               //we use the cluster connection configuration to connect to the cluster to find the live node we want to
               //connect to.
               ClusterConnectionConfiguration config =
                        ConfigurationUtils.getReplicationClusterConfiguration(configuration);
               serverLocator0 = getFailbackLocator(config);
            }
            //if the cluster isn't available we want to hang around until it is
            serverLocator0.setReconnectAttempts(-1);
            serverLocator0.setInitialConnectAttempts(-1);
            //this is used for replication so need to use the server packet decoder
            serverLocator0.setPacketDecoder(ServerPacketDecoder.INSTANCE);

            if (!initialisePart1())
               return;

            synchronized (this)
            {
               if (closed)
                  return;
               quorumManager = new QuorumManager(serverLocator0, threadPool, scheduledPool, getIdentity(), nodeManager);
               serverLocator0.addClusterTopologyListener(quorumManager);
            }

            //use a Node Locator to connect to the cluster
            LiveNodeLocator nodeLocator = configuration.getBackupGroupName() == null?
                  new AnyLiveNodeLocator(quorumManager):
                  new NamedLiveNodeLocator(configuration.getBackupGroupName(), quorumManager);
            serverLocator0.addClusterTopologyListener(nodeLocator);
            do
            {
               try
               {
                  nodeLocator.connectToCluster(serverLocator0);
                  break;
               }
               catch (HornetQException e)
               {
                  if(closed)
                     return;
                  Thread.sleep(serverLocator0.getRetryInterval());
               }
            } while (true);

            serverLocator0.addIncomingInterceptor(new ReplicationError(HornetQServerImpl.this, nodeLocator));

            // nodeManager.startBackup();

            clusterManager.start();

            replicationEndpoint.setQuorumManager(quorumManager);
            replicationEndpoint.setExecutor(executorFactory.getExecutor());
            EndpointConnector endpointConnector = new EndpointConnector();

            HornetQServerLogger.LOGGER.backupServerStarted(version.getFullVersion(), nodeManager.getNodeId());
            state = SERVER_STATE.STARTED;

            BACKUP_ACTIVATION signal;
            do
            {
               //locate the first live server to try to replicate
               nodeLocator.locateNode();
               if (closed)
               {
                  return;
               }
               Pair<TransportConfiguration, TransportConfiguration> possibleLive = nodeLocator.getLiveConfiguration();
               nodeID = nodeLocator.getNodeID();
               //in a normal (non failback) scenario if we couldn't find our live server we should fail
               if (!attemptFailBack)
               {
                  //this shouldn't happen
                  if (nodeID == null)
                     throw new RuntimeException("Could not establish the connection");
                  nodeManager.setNodeID(nodeID);
               }

               try
               {
                  liveServerSessionFactory
                       = (ClientSessionFactoryInternal) serverLocator0.createSessionFactory(possibleLive.getA(), 0, false);
               }
               catch (Exception e)
               {
                  if (possibleLive.getB() != null)
                  {
                     try
                     {
                        liveServerSessionFactory
                          = (ClientSessionFactoryInternal) serverLocator0.createSessionFactory(possibleLive.getB(), 0, false);
                     } catch (Exception e1)
                     {
                        liveServerSessionFactory = null;
                     }
                  }
               }
               if (liveServerSessionFactory == null)
               {
                  //its ok to retry here since we haven't started replication yet
                  //it may just be the server has gone since discovery
                  Thread.sleep(serverLocator0.getRetryInterval());
                  signal = BACKUP_ACTIVATION.ALREADY_REPLICATING;
                  continue;
               }

               threadPool.execute(endpointConnector);
               /**
               * Wait for a signal from the the quorum manager, at this point if replication has been successful we can
               * fail over or if there is an error trying to replicate (such as already replicating) we try the
               * process again on the next live server.  All the action happens inside
               * {@link QuorumManager}
               */
               signal = quorumManager.waitForStatusChange();
               /**
                * replicationEndpoint will be holding lots of open files. Make sure they get
                * closed/sync'ed.
                */
               stopComponent(replicationEndpoint);
               // time to give up
               if (!isStarted() || signal == STOP)
                  return;
               // time to fail over
               else if (signal == FAIL_OVER)
                  break;
               // something has gone badly run restart from scratch
               else if(signal == BACKUP_ACTIVATION.FAILURE_REPLICATING)
               {
                  Thread startThread = new Thread(new Runnable()
                  {
                     @Override
                     public void run()
                     {
                        try
                        {
                           stop();
                        }
                        catch (Exception e)
                        {
                           HornetQServerLogger.LOGGER.errorRestartingBackupServer(e, HornetQServerImpl.this);
                        }
                     }
                  });
                  startThread.start();
                  return;
               }
               //ok, this live is no good, lets reset and try again
               //close this session factory, we're done with it
               liveServerSessionFactory.close();
               quorumManager.reset();
               if (replicationEndpoint.getChannel() != null)
               {
                  replicationEndpoint.getChannel().close();
                  replicationEndpoint.setChannel(null);
               }
            }
            while (signal == BACKUP_ACTIVATION.ALREADY_REPLICATING);


            if (!isRemoteBackupUpToDate())
            {
               throw HornetQMessageBundle.BUNDLE.backupServerNotInSync();
            }

            configuration.setBackup(false);
            synchronized (HornetQServerImpl.this)
            {
               if (!isStarted())
                  return;
               HornetQServerLogger.LOGGER.becomingLive(HornetQServerImpl.this);
               nodeManager.stopBackup();
               storageManager.start();
               initialisePart2();
               clusterManager.activate();
            }
         }
         catch (Exception e)
         {
            if ((e instanceof InterruptedException || e instanceof IllegalStateException) && !isStarted())
               // do not log these errors if the server is being stopped.
               return;
            HornetQServerLogger.LOGGER.initializationError(e);
            e.printStackTrace();
         }
         finally
         {
            if (serverLocator0 != null)
               serverLocator0.close();
         }
      }

      public void close(final boolean permanently) throws Exception
      {
         synchronized (this)
         {
            if (quorumManager != null)
               quorumManager.causeExit(STOP);
            if (serverLocator0 != null)
            {
               serverLocator0.close();
            }
            closed = true;
         }

         if (configuration.isBackup())
         {
            long timeout = 30000;

            long start = System.currentTimeMillis();

            // To avoid a NPE cause by the stop
            NodeManager nodeManagerInUse = nodeManager;

            while (backupActivationThread.isAlive() && System.currentTimeMillis() - start < timeout)
            {

               if (nodeManagerInUse != null)
               {
                  nodeManagerInUse.interrupt();
               }

               backupActivationThread.interrupt();

               Thread.sleep(1000);
            }

            if (System.currentTimeMillis() - start >= timeout)
            {
               HornetQServerLogger.LOGGER.backupActivationProblem();
            }

            if (nodeManagerInUse != null)
            {
               nodeManagerInUse.stopBackup();
            }
         }
      }

      /**
       * Live has notified this server that it is going to stop.
       */
      public final void failOver(final LiveStopping finalMessage)
      {
         if (finalMessage == null)
         {
            quorumManager.causeExit(FAILURE_REPLICATING);
         }
         else
         {
            quorumManager.failOver(finalMessage);
         }
      }

      private class EndpointConnector implements Runnable
      {
         @Override
         public void run()
         {
            try
            {
               //we should only try once, if its not there we should move on.
               liveServerSessionFactory.setReconnectAttempts(1);
               quorumManager.setSessionFactory(liveServerSessionFactory);
               //get the connection and request replication to live
               CoreRemotingConnection liveConnection = liveServerSessionFactory.getConnection();
               quorumManager.addAsFailureListenerOf(liveConnection);
               Channel pingChannel = liveConnection.getChannel(ChannelImpl.CHANNEL_ID.PING.id, -1);
               Channel replicationChannel = liveConnection.getChannel(ChannelImpl.CHANNEL_ID.REPLICATION.id, -1);
               connectToReplicationEndpoint(replicationChannel);
               replicationEndpoint.start();
               clusterManager.announceReplicatingBackupToLive(pingChannel, attemptFailBack);
            }
            catch (Exception e)
            {
               //we shouldn't stop the server just mark the connector as tried and unavailable
               HornetQServerLogger.LOGGER.replicationStartProblem(e);
               quorumManager.causeExit(FAILURE_REPLICATING);
            }
         }

         private synchronized ReplicationEndpoint connectToReplicationEndpoint(final Channel channel) throws Exception
         {
            if (!isStarted())
               return null;
            if (!configuration.isBackup())
            {
               throw HornetQMessageBundle.BUNDLE.serverNotBackupServer();
            }

            channel.setHandler(replicationEndpoint);

            if (replicationEndpoint.getChannel() != null)
            {
               throw HornetQMessageBundle.BUNDLE.alreadyHaveReplicationServer();
            }

            replicationEndpoint.setChannel(channel);

            return replicationEndpoint;
         }
      }
   }


   private final class SharedNothingLiveActivation implements Activation
   {
      public void run()
      {
         try
         {
            if (configuration.isClustered() && configuration.isCheckForLiveServer() && isNodeIdUsed())
            {
               configuration.setBackup(true);
               return;
            }

            initialisePart1();

            initialisePart2();

            if (identity != null)
            {
               HornetQServerLogger.LOGGER.serverIsLive(identity);
            }
            else
            {
               HornetQServerLogger.LOGGER.serverIsLive();
            }
         }
         catch (Exception e)
         {
            HornetQServerLogger.LOGGER.initializationError(e);
         }
      }

      /**
       * Determines whether there is another server already running with this server's nodeID.
       * <p>
       * This can happen in case of a successful fail-over followed by the live's restart
       * (attempting a fail-back).
       * @throws Exception
       */
      private boolean isNodeIdUsed() throws Exception
      {
         if (configuration.getClusterConfigurations().isEmpty())
            return false;
         SimpleString nodeId0;
         try
         {
            nodeId0 = nodeManager.readNodeId();
         }
         catch (HornetQIllegalStateException e)
         {
            nodeId0 = null;
         }

         ServerLocatorInternal locator;

         ClusterConnectionConfiguration config = ConfigurationUtils.getReplicationClusterConfiguration(configuration);

         locator = getFailbackLocator(config);

         ClientSessionFactoryInternal factory = null;

         NodeIdListener listener=new NodeIdListener(nodeId0);

         locator.addClusterTopologyListener(listener);
         try
         {
            locator.setReconnectAttempts(0);
            try
            {
               locator.addClusterTopologyListener(listener);
               factory = locator.connectNoWarnings();
            }
            catch (Exception notConnected)
            {
               return false;
            }

            listener.latch.await(5, TimeUnit.SECONDS);

            return listener.isNodePresent;
         }
         finally
         {
            if (factory != null)
               factory.close();
            if (locator != null)
               locator.close();
         }
      }

      public void close(boolean permanently) throws Exception
      {
         // To avoid a NPE cause by the stop
         NodeManager nodeManagerInUse = nodeManager;

         if (nodeManagerInUse != null)
         {
            if (permanently)
            {
               nodeManagerInUse.crashLiveServer();
            }
            else
            {
               nodeManagerInUse.pauseLiveServer();
            }
         }
      }
   }

   static final class NodeIdListener implements ClusterTopologyListener
   {
      volatile boolean isNodePresent = false;

      private final SimpleString nodeId;
      private final CountDownLatch latch = new CountDownLatch(1);

      public NodeIdListener(SimpleString nodeId)
      {
         this.nodeId = nodeId;
      }

      @Override
      public void nodeUP(TopologyMember topologyMember, boolean last)
      {
         boolean isOurNodeId = nodeId != null && nodeId.toString().equals(topologyMember.getNodeId());
         if (isOurNodeId)
         {
            isNodePresent = true;
         }
         if (isOurNodeId || last)
         {
            latch.countDown();
         }
      }

      @Override
      public void nodeDown(long eventUID, String nodeID)
      {
         // no-op
      }
   }

   private TransportConfiguration[] connectorNameListToArray(final List<String> connectorNames)
   {
      TransportConfiguration[] tcConfigs = (TransportConfiguration[]) Array.newInstance(TransportConfiguration.class,
            connectorNames.size());
      int count = 0;
      for (String connectorName : connectorNames)
      {
         TransportConfiguration connector = configuration.getConnectorConfigurations().get(connectorName);

         if (connector == null)
         {
            HornetQServerLogger.LOGGER.bridgeNoConnector(connectorName);

            return null;
         }

         tcConfigs[count++] = connector;
      }

      return tcConfigs;
   }

   /** This seems duplicate code all over the place, but for security reasons we can't let something like this to be open in a
    *  utility class, as it would be a door to load anything you like in a safe VM.
    *  For that reason any class trying to do a privileged block should do with the AccessController directly.
    */
   private static Object safeInitNewInstance(final String className)
   {
      return AccessController.doPrivileged(new PrivilegedAction<Object>()
      {
         public Object run()
         {
            return ClassloadingUtil.newInstanceFromClassLoader(className);
         }
      });
   }

   @Override
   public void startReplication(CoreRemotingConnection rc, final ClusterConnection clusterConnection,
                               final Pair<TransportConfiguration, TransportConfiguration> pair, final boolean isFailBackRequest)
      throws HornetQException
   {
      if (replicationManager != null)
      {
         throw new HornetQAlreadyReplicatingException();
      }

      if (!isStarted())
      {
         throw new HornetQIllegalStateException();
      }

      synchronized (replicationLock)
      {

         if (replicationManager != null)
         {
            throw new HornetQAlreadyReplicatingException();
         }
         ReplicationFailureListener listener = new ReplicationFailureListener();
         rc.addCloseListener(listener);
         rc.addFailureListener(listener);
         replicationManager = new ReplicationManager(rc, executorFactory);
         replicationManager.start();
         Thread t = new Thread(new Runnable()
         {
            public void run()
            {
               try
               {
                  storageManager.startReplication(replicationManager, pagingManager, getNodeID().toString(),
                                                  isFailBackRequest && configuration.isAllowAutoFailBack());
                  clusterConnection.nodeAnnounced(System.currentTimeMillis(), getNodeID().toString(), configuration.getBackupGroupName(), pair, true);

                  if (isFailBackRequest && configuration.isAllowAutoFailBack())
                  {
                     BackupTopologyListener listener1 = new BackupTopologyListener(getNodeID().toString());
                     clusterConnection.addClusterTopologyListener(listener1);
                     if (listener1.waitForBackup())
                     {
                        try
                        {
                           Thread.sleep(configuration.getFailbackDelay());
                        } catch (InterruptedException e)
                        {
                           //
                        }
                        stop(true);
                        HornetQServerLogger.LOGGER.stopReplicatedBackupAfterFailback();
                     }
                     else
                     {
                        HornetQServerLogger.LOGGER.failbackMissedBackupAnnouncement();
                     }
                  }
               }
               catch (Exception e)
               {
                  if (state == HornetQServerImpl.SERVER_STATE.STARTED)
                  {
                  /*
                   * The reasoning here is that the exception was either caused by (1) the
                   * (interaction with) the backup, or (2) by an IO Error at the storage. If (1), we
                   * can swallow the exception and ignore the replication request. If (2) the live
                   * will crash shortly.
                   */
                  HornetQServerLogger.LOGGER.errorStartingReplication(e);
                  }
                  try
                  {
                     stopComponent(replicationManager);
                  }
                  catch (Exception hqe)
                  {
                     HornetQServerLogger.LOGGER.errorStoppingReplication(hqe);
                  }
                  finally
                  {
                     synchronized (replicationLock)
                     {
                        replicationManager = null;
                     }
                  }
               }
            }
         });

         t.start();
      }
   }

   /**
    * Whether a remote backup server was in sync with its live server. If it was not in sync, it may
    * not take over the live's functions.
    * <p>
    * A local backup server or a live server should always return {@code true}
    * @return whether the backup is up-to-date, if the server is not a backup it always returns
    *         {@code true}.
    */
   public boolean isRemoteBackupUpToDate()
   {
      return backupUpToDate;
   }

   public void setRemoteBackupUpToDate()
   {
      clusterManager.announceBackup();
      backupUpToDate = true;
      backupSyncLatch.countDown();
   }

   private final class ReplicationFailureListener implements FailureListener, CloseListener
   {

      @Override
      public void connectionFailed(HornetQException exception, boolean failedOver)
      {
         connectionClosed();
      }

      @Override
      public void connectionClosed()
      {
         threadPool.execute(new Runnable()
         {
            public void run()
            {
               synchronized (replicationLock)
               {
                  if (replicationManager != null)
                  {
                     storageManager.stopReplication();
                     replicationManager = null;
                  }
               }
            }
         });
      }
   }

   /**
    * @throws HornetQException
    */
   public void remoteFailOver(ReplicationLiveIsStoppingMessage.LiveStopping finalMessage) throws HornetQException
   {
      if (!configuration.isBackup() || configuration.isSharedStore())
      {
         throw new HornetQInternalErrorException();
      }
      if (activation instanceof SharedNothingBackupActivation)
      {
         final SharedNothingBackupActivation replicationActivation = ((SharedNothingBackupActivation)activation);

         if (!backupUpToDate)
         {
            replicationActivation.failOver(null);
         }
         else
         {
            replicationActivation.failOver(finalMessage);
         }
    }
   }


   private ServerLocatorInternal getFailbackLocator(ClusterConnectionConfiguration config) throws HornetQException
   {
      ServerLocatorInternal locator;
      if(config.getDiscoveryGroupName() != null)
      {
         DiscoveryGroupConfiguration dg = configuration.getDiscoveryGroupConfigurations().get(config.getDiscoveryGroupName());

         if (dg == null)
         {
            throw new HornetQException("foo");
         }
         locator = (ServerLocatorInternal) HornetQClient.createServerLocatorWithHA(dg);
      }
      else
      {
         TransportConfiguration[] tcConfigs = config.getStaticConnectors() != null ? connectorNameListToArray(config.getStaticConnectors())
                                                                               : null;

         locator = (ServerLocatorInternal) HornetQClient.createServerLocatorWithHA(tcConfigs);
      }
      return locator;
   }

   /**
    * Move data away before starting data synchronization for fail-back.
    * <p>
    * Use case is a server, upon restarting, finding a former backup running in its place. It will
    * move any older data away and log a warning about it.
    */
   private void moveServerData()
   {
      String[] dataDirs =
               new String[] { configuration.getBindingsDirectory(),
                             configuration.getJournalDirectory(),
                             configuration.getPagingDirectory(),
                             configuration.getLargeMessagesDirectory() };
      boolean allEmpty = true;
      int lowestSuffixForMovedData = 1;
      boolean redo = true;

      while (redo)
      {
         redo = false;
         for (String dir : dataDirs)
         {
            File fDir = new File(dir);
            if (fDir.exists())
            {
               if (!fDir.isDirectory())
               {
                  throw new IllegalStateException("Path " + fDir + " exists and it is not a directory");
               }

               if (fDir.list().length > 0)
                  allEmpty = false;
            }

            String sanitizedPath = fDir.getPath();
            while (new File(sanitizedPath + lowestSuffixForMovedData).exists())
            {
               lowestSuffixForMovedData++;
               redo = true;
            }
         }
      }
      if (allEmpty)
         return;

      for (String dir0 : dataDirs)
      {
         File dir = new File(dir0);
         File newPath = new File(dir.getPath() + lowestSuffixForMovedData);
         if (dir.exists())
         {
            if (!dir.renameTo(newPath))
            {
               throw new IllegalStateException("Could not move " + dir);
            }

            HornetQServerLogger.LOGGER.backupMovingDataAway(dir0, newPath.getPath());
         }
         dir.mkdir();
      }
   }
}
