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

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.asyncio.impl.AsynchronousFileImpl;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.BridgeConfiguration;
import org.hornetq.core.config.Configuration;
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
import org.hornetq.core.logging.Logger;
import org.hornetq.core.management.impl.HornetQServerControlImpl;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.paging.cursor.PageSubscription;
import org.hornetq.core.paging.impl.PagingManagerImpl;
import org.hornetq.core.paging.impl.PagingStoreFactoryNIO;
import org.hornetq.core.persistence.GroupingInfo;
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
import org.hornetq.core.protocol.core.Channel;
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
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.JournalType;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.MemoryManager;
import org.hornetq.core.server.NodeManager;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.QueueFactory;
import org.hornetq.core.server.ServerSession;
import org.hornetq.core.server.cluster.ClusterManager;
import org.hornetq.core.server.cluster.Transformer;
import org.hornetq.core.server.cluster.impl.ClusterManagerImpl;
import org.hornetq.core.server.group.GroupingHandler;
import org.hornetq.core.server.group.impl.GroupBinding;
import org.hornetq.core.server.group.impl.GroupingHandlerConfiguration;
import org.hornetq.core.server.group.impl.LocalGroupingHandler;
import org.hornetq.core.server.group.impl.RemoteGroupingHandler;
import org.hornetq.core.server.management.ManagementService;
import org.hornetq.core.server.management.impl.ManagementServiceImpl;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.settings.impl.HierarchicalObjectRepository;
import org.hornetq.core.transaction.ResourceManager;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.impl.BindingsTransactionImpl;
import org.hornetq.core.transaction.impl.ResourceManagerImpl;
import org.hornetq.core.transaction.impl.TransactionImpl;
import org.hornetq.core.version.Version;
import org.hornetq.spi.core.logging.LogDelegateFactory;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.protocol.SessionCallback;
import org.hornetq.spi.core.security.HornetQSecurityManager;
import org.hornetq.utils.ClassloadingUtil;
import org.hornetq.utils.ExecutorFactory;
import org.hornetq.utils.HornetQThreadFactory;
import org.hornetq.utils.OrderedExecutorFactory;
import org.hornetq.utils.SecurityFormatter;
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

   // JMS Topics (which are outside of the scope of the core API) will require a dumb subscription with a dummy-filter
   // at this current version
   // as a way to keep its existence valid and TCK tests
   // That subscription needs an invalid filter, however paging needs to ignore any subscription with this filter.
   // For that reason, this filter needs to be rejected on paging or any other component on the system, and just be
   // ignored for any purpose
   // It's declared here as this filter is considered a global ignore
   public static final String GENERIC_IGNORED_FILTER = "__HQX=-1";

   // Static
   // ---------------------------------------------------------------------------------------

   // Attributes
   // -----------------------------------------------------------------------------------

   private final Version version;

   private final HornetQSecurityManager securityManager;

   private final Configuration configuration;

   private final MBeanServer mbeanServer;

   private volatile boolean started;

   private volatile boolean stopped;

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

   private final Object initialiseLock = new Object();

   private boolean initialised;

   // private FailoverManager replicationFailoverManager;

   private ReplicationManager replicationManager;

   private ReplicationEndpoint replicationEndpoint;

   private final Set<ActivateCallback> activateCallbacks = new HashSet<ActivateCallback>();

   private volatile GroupingHandler groupingHandler;

   private NodeManager nodeManager;

   // Used to identify the server on tests... useful on debugging testcases
   private String identity;

   private Thread backupActivationThread;

   private Activation activation;

   private final ShutdownOnCriticalErrorListener shutdownOnCriticalIO = new ShutdownOnCriticalErrorListener();

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

   // lifecycle methods
   // ----------------------------------------------------------------

   /*
    * Can be overridden for tests
    */
   protected NodeManager createNodeManager(final String directory)
   {
      if (configuration.getJournalType() == JournalType.ASYNCIO && AsynchronousFileImpl.isLoaded())
      {
         return new AIOFileLockNodeManager(directory);
      }
      else
      {
         return new FileLockNodeManager(directory);
      }
   }

   public synchronized void start() throws Exception
   {
      if (started)
      {
         log.debug("Server already started!");
         return;
      }

      log.debug("Starting server " + this);
      OperationContextImpl.clearContext();

      try
      {
         stopped = false;

         initialiseLogging();

         checkJournalDirectory();

         nodeManager = createNodeManager(configuration.getJournalDirectory());

         nodeManager.start();

         if (started)
         {
            HornetQServerImpl.log.info((configuration.isBackup() ? "backup" : "live") + " is already started, ignoring the call to start..");
            return;
         }

         HornetQServerImpl.log.info((configuration.isBackup() ? "backup" : "live") + " server is starting with configuration " +
                                    configuration);

         if (configuration.isRunSyncSpeedTest())
         {
            SyncSpeedTest test = new SyncSpeedTest();

            test.run();
         }

         if (!configuration.isBackup())
         {
            if (configuration.isSharedStore() && configuration.isPersistenceEnabled())
            {
               activation = new SharedStoreLiveActivation();

               // This should block until the lock is got

               activation.run();
            }
            else
            {
               activation = new NoSharedStoreLiveActivation();

               activation.run();
            }
            started = true;

            HornetQServerImpl.log.info("HornetQ Server version " + getVersion().getFullVersion() +
                                       " [" +
                                       nodeManager.getNodeId() +
                                       "]" +
                                       (this.identity != null ? " (" + identity : ")") +
                                       " started");
         }

         // The activation on fail-back may change the value of isBackup, for that reason we are not using else here
         if (configuration.isBackup())
         {
            if (configuration.isSharedStore())
            {
               activation = new SharedStoreBackupActivation();
            }
            else
            {
               // Replicated

               activation = new SharedNothingBackupActivation();
            }

            backupActivationThread = new Thread(activation, "Activation for server " + this);
            backupActivationThread.start();
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
   protected void finalize() throws Throwable
   {
      if (started)
      {
         HornetQServerImpl.log.warn("HornetQServer is being finalized and has not been stopped. Please remember to stop the " + "server before letting it go out of scope");

         stop();
      }

      super.finalize();
   }

   public void stop() throws Exception
   {
      stopped = true;
      stop(configuration.isFailoverOnServerShutdown());
   }

   public void threadDump(final String reason)
   {
      StringWriter str = new StringWriter();
      PrintWriter out = new PrintWriter(str);

      Map<Thread, StackTraceElement[]> stackTrace = Thread.getAllStackTraces();

      out.println("Generating thread dump because - " + reason);
      out.println("*******************************************************************************");

      for (Map.Entry<Thread, StackTraceElement[]> el : stackTrace.entrySet())
      {
         out.println("===============================================================================");
         out.println("Thread " + el.getKey() +
                     " name = " +
                     el.getKey().getName() +
                     " id = " +
                     el.getKey().getId() +
                     " group = " +
                     el.getKey().getThreadGroup());
         out.println();
         for (StackTraceElement traceEl : el.getValue())
         {
            out.println(traceEl);
         }
      }

      out.println("===============================================================================");
      out.println("End Thread dump");
      out.println("*******************************************************************************");

      log.warn(str.toString());
   }

   public void stop(boolean failoverOnServerShutdown) throws Exception
   {
      stop(failoverOnServerShutdown, false);
   }

   protected void stop(boolean failoverOnServerShutdown, boolean criticalIOError) throws Exception
   {
      synchronized (this)
      {
         if (!started)
         {
            return;
         }

         connectorsService.stop();
         // we stop the groupinghandler before we stop te cluster manager so binding mappings aren't removed in case of
         // failover
         if (groupingHandler != null)
         {
            managementService.removeNotificationListener(groupingHandler);
            groupingHandler = null;
         }

         if (clusterManager != null)
         {
            clusterManager.stop();
         }

      }

      remotingService.stop(criticalIOError);

      // We close all the exception in an attempt to let any pending IO to finish
      // to avoid scenarios where the send or ACK got to disk but the response didn't get to the client
      // It may still be possible to have this scenario on a real failure (without the use of XA)
      // But at least we will do our best to avoid it on regular shutdowns
      for (ServerSession session : sessions.values())
      {
         try
         {
            storageManager.setContext(session.getSessionContext());
            session.close(true);
            if (!criticalIOError)
            {
               session.waitContextCompletion();
            }
         }
         catch (Exception e)
         {
            // If anything went wrong with closing sessions.. we should ignore it
            // such as transactions.. etc.
            log.warn(e.getMessage(), e);
         }
      }

      storageManager.clearContext();

      synchronized (this)
      {
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

         if (pagingManager != null)
         {
            pagingManager.stop();
         }

         if (!criticalIOError && storageManager != null)
         {
            storageManager.stop();
         }

         if (replicationManager != null)
         {
            replicationManager.stop();
            replicationManager = null;
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

         List<Runnable> tasks = scheduledPool.shutdownNow();

         for (Runnable task : tasks)
         {
            HornetQServerImpl.log.debug(this + "::Waiting for " + task);
         }

         if (memoryManager != null)
         {
            memoryManager.stop();
         }

         threadPool.shutdown();

         scheduledPool.shutdown();

         try
         {
            if (!threadPool.awaitTermination(10, TimeUnit.SECONDS))
            {
               HornetQServerImpl.log.warn("Timed out waiting for pool to terminate");
            }
         }
         catch (InterruptedException e)
         {
            // Ignore
         }
         threadPool = null;

         try
         {
            if (!scheduledPool.awaitTermination(10, TimeUnit.SECONDS))
            {
               HornetQServerImpl.log.warn("Timed out waiting for scheduled pool to terminate");
            }
         }
         catch (InterruptedException e)
         {
            // Ignore
         }

         securityStore.stop();

         threadPool = null;

         scheduledPool = null;

         pagingManager = null;
         securityStore = null;
         resourceManager = null;
         postOffice = null;
         securityStore = null;
         queueFactory = null;
         resourceManager = null;
         messagingServerControl = null;
         memoryManager = null;

         sessions.clear();

         started = false;
         initialised = false;
         // to display in the log message
         SimpleString tempNodeID = getNodeID();

         if (activation != null)
         {
            activation.close(failoverOnServerShutdown);
         }

         if (backupActivationThread != null)
         {
            backupActivationThread.join();
         }

         nodeManager.stop();

         nodeManager = null;

         addressSettingsRepository.clearListeners();

         addressSettingsRepository.clearCache();

         HornetQServerImpl.log.info("HornetQ Server version " + getVersion().getFullVersion() +
                                    " [" +
                                    tempNodeID +
                                    "] stopped");

         Logger.reset();
      }

   }

   // HornetQServer implementation
   // -----------------------------------------------------------

   public String describe()
   {
      StringWriter str = new StringWriter();
      PrintWriter out = new PrintWriter(str);

      out.println("Information about server " + this.identity);
      out.println("Cluster Connection:" + this.getClusterManager().describe());

      return str.toString();
   }

   public String destroyConnectionWithSessionMetadata(String metaKey, String parameterValue) throws Exception
   {
      StringBuffer operationsExecuted = new StringBuffer();

      try
      {
         operationsExecuted.append("**************************************************************************************************\n");
         operationsExecuted.append("Executing destroyConnection with " + metaKey + "='" + parameterValue + "' through management's request\n");

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
                  operationsExecuted.append("Closing connection " + sessionFound + "\n");
                  RemotingConnection conn = session.getRemotingConnection();
                  if (conn != null)
                  {
                     conn.fail(new HornetQException(HornetQException.DISCONNECTED, "Connection being disconnected per admin's request on destroyConnection(" + metaKey + "=" + parameterValue +")"));
                  }
                  session.close(true);
                  sessions.remove(session.getName());
               }
            }
            catch (Throwable e)
            {
               log.warn(e.getMessage(), e);
            }
         }

         if (sessionFound == null)
         {
            operationsExecuted.append("No session was found with " + metaKey + "=" + parameterValue + "\n");
         }

         return operationsExecuted.toString();
      }
      finally
      {
         // This operation is critical for the knowledge of the admin, so we need to add info logs for later knowledge
         log.info(operationsExecuted.toString());
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

   public ExecutorService getThreadPool()
   {
      return threadPool;
   }

   public Configuration getConfiguration()
   {
      return configuration;
   }

   public MBeanServer getMBeanServer()
   {
      return mbeanServer;
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
      return started;
   }

   public boolean isStopped()
   {
      return stopped;
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

      final ServerSessionImpl session = internalCreateSession(name,
                                                              username,
                                                              password,
                                                              minLargeMessageSize,
                                                              connection,
                                                              autoCommitSends,
                                                              autoCommitAcks,
                                                              preAcknowledge,
                                                              xa,
                                                              defaultAddress,
                                                              callback);

      sessions.put(name, session);

      return session;
   }

   /**
    * callback point for tests where a session is replaced
    * @param name
    * @param username
    * @param password
    * @param minLargeMessageSize
    * @param connection
    * @param autoCommitSends
    * @param autoCommitAcks
    * @param preAcknowledge
    * @param xa
    * @param defaultAddress
    * @param callback
    * @return
    * @throws Exception
    */
   protected ServerSessionImpl internalCreateSession(final String name,
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
                                                              managementService,
                                                              this,
                                                              configuration.getManagementAddress(),
                                                              defaultAddress == null ? null
                                                                                    : new SimpleString(defaultAddress),
                                                              callback);
      return session;
   }
   
   final protected SecurityStore getSecurityStore()
   {
      return securityStore;
   }

   public synchronized ReplicationEndpoint connectToReplicationEndpoint(final Channel channel) throws Exception
   {
      if (!configuration.isBackup())
      {
         throw new HornetQException(HornetQException.ILLEGAL_STATE, "Connected server is not a backup server");
      }

      if (replicationEndpoint.getChannel() != null)
      {
         throw new HornetQException(HornetQException.ILLEGAL_STATE,
                                    "Backup replication server is already connected to another server");
      }

      replicationEndpoint.setChannel(channel);

      return replicationEndpoint;
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

   public boolean isInitialised()
   {
      synchronized (initialiseLock)
      {
         return initialised;
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
      log.info("trying to deploy queue " + queueName);

      return createQueue(address, queueName, filterString, durable, temporary, true);
   }


   public void destroyQueue(final SimpleString queueName) throws Exception
   {
      // The session is passed as an argument to verify if the user has authorization to delete the queue
      // in some cases (such as temporary queues) this should happen regardless of the authorization
      // since that will only happen during a session close, which will be used to cleanup on temporary queues
      destroyQueue(queueName, null, true);
   }

   public void destroyQueue(final SimpleString queueName, final ServerSession session) throws Exception
   {
      destroyQueue(queueName, session, true);
   }

   public void destroyQueue(final SimpleString queueName, final ServerSession session, final boolean checkConsumerCount) throws Exception
   {
      addressSettingsRepository.clearCache();

      Binding binding = postOffice.getBinding(queueName);

      if (binding == null)
      {
         throw new HornetQException(HornetQException.QUEUE_DOES_NOT_EXIST, "No such queue " + queueName);
      }

      Queue queue = (Queue)binding.getBindable();

      // This check is only valid if checkConsumerCount == true
      if (checkConsumerCount && queue.getConsumerCount() != 0)
      {
         throw new HornetQException(HornetQException.ILLEGAL_STATE, "Cannot delete queue " + queue.getName() +
                                                                    " on binding " +
                                                                    queueName +
                                                                    " - it has consumers = " +
                                                                    binding.getClass().getName());
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
      return replicationManager;
   }

   public ConnectorsService getConnectorsService()
   {
      return connectorsService;
   }

   public synchronized boolean checkActivate() throws Exception
   {
      if (configuration.isBackup())
      {
         // Handle backup server activation

         if (!configuration.isSharedStore())
         {
            if (replicationEndpoint == null)
            {
               HornetQServerImpl.log.warn("There is no replication endpoint, can't activate this backup server");

               throw new HornetQException(HornetQException.INTERNAL_ERROR, "Can't activate the server");
            }

            replicationEndpoint.stop();
         }

         // Complete the startup procedure

         HornetQServerImpl.log.info("Activating backup server");

         configuration.setBackup(false);

         initialisePart2();
      }

      return true;
   }

   public void deployDivert(DivertConfiguration config) throws Exception
   {
      if (config.getName() == null)
      {
         HornetQServerImpl.log.warn("Must specify a name for each divert. This one will not be deployed.");

         return;
      }

      if (config.getAddress() == null)
      {
         HornetQServerImpl.log.warn("Must specify an address for each divert. This one will not be deployed.");

         return;
      }

      if (config.getForwardingAddress() == null)
      {
         HornetQServerImpl.log.warn("Must specify an forwarding address for each divert. This one will not be deployed.");

         return;
      }

      SimpleString sName = new SimpleString(config.getName());

      if (postOffice.getBinding(sName) != null)
      {
         HornetQServerImpl.log.warn("Binding already exists with name " + sName + ", divert will not be deployed");

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
         throw new HornetQException(HornetQException.INTERNAL_ERROR, "No binding for divert " + name);
      }
      if (!(binding instanceof DivertBinding))
      {
         throw new HornetQException(HornetQException.INTERNAL_ERROR, "Binding " + name + " is not a divert");
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

   public String toString()
   {
      if (identity != null)
      {
         return "HornetQServerImpl::" + identity;
      }
      else
      {
         return "HornetQServerImpl::" + (nodeManager != null ? "serverUUID=" + nodeManager.getUUID() : "");
      }
   }

   /**
    * For tests only, don't use this method as it's not part of the API
    * @param factory
    */
   public void replaceQueueFactory(QueueFactory factory)
   {
      this.queueFactory = factory;
   }

   // Package protected
   // ----------------------------------------------------------------------------

   // Protected
   // ------------------------------------------------------------------------------------

   protected Map<String, ServerSession> internalGetSessions()
   {
      return this.sessions;
   }
   /**
    * Protected so tests can change this behaviour
    * @param backupConnector
    */

   protected PagingManager createPagingManager()
   {

      return new PagingManagerImpl(new PagingStoreFactoryNIO(configuration.getPagingDirectory(),
                                                             (long)configuration.getJournalBufferSize_NIO(),
                                                             scheduledPool,
                                                             executorFactory,
                                                             configuration.isJournalSyncNonTransactional(),
                                                             shutdownOnCriticalIO),
                                   storageManager,
                                   addressSettingsRepository);
   }

   /**
    * This method is protected as it may be used as a hook for creating a custom storage manager (on tests for instance)
    */
   protected StorageManager createStorageManager()
   {
      if (configuration.isPersistenceEnabled())
      {
         return new JournalStorageManager(configuration, executorFactory, replicationManager, shutdownOnCriticalIO);
      }
      else
      {
         return new NullStorageManager();
      }
   }

   // Private
   // --------------------------------------------------------------------------------------

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

   private void initialiseLogging()
   {
      LogDelegateFactory logDelegateFactory = (LogDelegateFactory)instantiateInstance(configuration.getLogDelegateFactoryClassName());

      Logger.setDelegateFactory(logDelegateFactory);
   }

   /*
    * Start everything apart from RemotingService and loading the data
    */
   private void initialisePart1() throws Exception
   {
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
         memoryManager = new MemoryManagerImpl(configuration.getMemoryWarningThreshold(),
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

      if (ConfigurationImpl.DEFAULT_CLUSTER_USER.equals(configuration.getClusterUser()) && ConfigurationImpl.DEFAULT_CLUSTER_PASSWORD.equals(configuration.getClusterPassword()))
      {
         log.warn("Security risk! It has been detected that the cluster admin user and password " + "have not been changed from the installation default. "
                  + "Please see the HornetQ user guide, cluster chapter, for instructions on how to do this.");
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
      clusterManager = new ClusterManagerImpl(executorFactory,
                                              this,
                                              postOffice,
                                              scheduledPool,
                                              managementService,
                                              configuration,
                                              nodeManager.getUUID(),
                                              configuration.isBackup(),
                                              configuration.isClustered());

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
   }

   /*
    * Load the data, and start remoting service so clients can connect
    */
   private void initialisePart2() throws Exception
   {
      // Load the journal and populate queues, transactions and caches in memory

      if (stopped)
      {
         return;
      }

      pagingManager.reloadStores();

      JournalLoadInformation[] journalInfo = loadJournals();

      compareJournals(journalInfo);

      final ServerInfo dumper = new ServerInfo(this, pagingManager);

      long dumpInfoInterval = configuration.getServerDumpInterval();

      if (dumpInfoInterval > 0)
      {
         scheduledPool.scheduleWithFixedDelay(new Runnable()
         {
            public void run()
            {
               HornetQServerImpl.log.info(dumper.dump());
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

      // Deply any pre-defined diverts
      deployDiverts();

      if (deploymentManager != null)
      {
         deploymentManager.start();
      }

      // We do this at the end - we don't want things like MDBs or other connections connecting to a backup server until
      // it is activated

      clusterManager.start();

      remotingService.start();
      
      postOffice.startExpiryScanner();

      initialised = true;

   }

   /**
    * @param journalInfo
    */
   private void compareJournals(final JournalLoadInformation[] journalInfo) throws Exception
   {
      if (replicationManager != null)
      {
         replicationManager.compareJournals(journalInfo);
      }
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
         
         boolean isTopicIdentification = filter != null && filter.getFilterString() != null && filter.getFilterString().toString().equals(GENERIC_IGNORED_FILTER);
         
         if (postOffice.getBinding(queueBindingInfo.getQueueName()) != null)
         {
            
            
            if (isTopicIdentification)
            {
               long tx = storageManager.generateUniqueID();
               storageManager.deleteQueueBinding(tx, queueBindingInfo.getId());
               storageManager.commitBindings(tx);
               continue;
            }
            else
            {
               
               SimpleString newName = queueBindingInfo.getQueueName().concat("-" + (duplicateID++));
               log.warn("Queue " + queueBindingInfo.getQueueName() + " is duplicated during reload. This queue will be renamed as " + newName);
               queueBindingInfo.replaceQueueName(newName);
            }
         }

         PageSubscription subscription = null;
         
         if (!isTopicIdentification)
         {
             subscription = pagingManager.getPageStore(queueBindingInfo.getAddress())
                                                         .getCursorProvier()
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
         log.info("Deleting pending large message as it wasn't completed: LargeMessageID:" + msgToDelete.getB());
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
            throw new HornetQException(HornetQException.QUEUE_EXISTS, "Queue " + queueName + " already exists");
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
                                         .getCursorProvier()
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
         if (durable)
         {
            storageManager.rollbackBindings(txID);
         }
         try
         {
            if (queue!= null)
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
            log.error(ignored);
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

   private synchronized void deployGroupingHandlerConfiguration(final GroupingHandlerConfiguration config) throws Exception
   {
      if (config != null)
      {
         GroupingHandler groupingHandler;
         if (config.getType() == GroupingHandlerConfiguration.TYPE.LOCAL)
         {
            groupingHandler = new LocalGroupingHandler(managementService,
                                                       config.getName(),
                                                       config.getAddress(),
                                                       getStorageManager(),
                                                       config.getTimeout());
         }
         else
         {
            groupingHandler = new RemoteGroupingHandler(managementService,
                                                        config.getName(),
                                                        config.getAddress(),
                                                        config.getTimeout());
         }

         this.groupingHandler = groupingHandler;

         managementService.addNotificationListener(groupingHandler);
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
            throw new IllegalArgumentException("Directory " + journalDir + " does not exist and will not be created");
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

   class FailbackChecker implements Runnable
   {
      boolean restarting = false;

      public void run()
      {
         try
         {
            if (!restarting && nodeManager.isAwaitingFailback())
            {
               log.info("live server wants to restart, restarting server in backup");
               restarting = true;
               Thread t = new Thread(new Runnable()
               {
                  public void run()
                  {
                     try
                     {
                        log.debug(HornetQServerImpl.this + "::Stopping live node in favor of failback");
                        stop(true);
                        // We need to wait some time before we start the backup again
                        // otherwise we may eventually start before the live had a chance to get it
                        Thread.sleep(configuration.getFailbackDelay());
                        configuration.setBackup(true);
                        log.debug(HornetQServerImpl.this + "::Starting backup node now after failback");
                        start();
                     }
                     catch (Exception e)
                     {
                        log.warn("unable to restart server, please kill and restart manually", e);
                     }
                  }
               });
               t.start();
            }
         }
         catch (Exception e)
         {
            log.warn(e.getMessage(), e);
         }
      }
   }

   private class SharedStoreLiveActivation implements Activation
   {
      public void run()
      {
         try
         {
            log.info("Waiting to obtain live lock");

            checkJournalDirectory();

            if (log.isDebugEnabled())
            {
               log.debug("First part initialization on " + this);
            }

            initialisePart1();

            if (nodeManager.isBackupLive())
            {
               // looks like we've failed over at some point need to inform that we are the backup so when the current
               // live
               // goes down they failover to us
               if (log.isDebugEnabled())
               {
                  log.debug("announcing backup to the former live" + this);
               }

               clusterManager.announceBackup();
               Thread.sleep(configuration.getFailbackDelay());
            }

            nodeManager.startLiveNode();

            if (stopped)
            {
               return;
            }

            initialisePart2();

            log.info("Server is now live");
         }
         catch (Exception e)
         {
            log.error("Failure in initialisation", e);
         }
      }

      public void close(boolean permanently) throws Exception
      {
         if (permanently)
         {
            nodeManager.crashLiveServer();
         }
         else
         {
            nodeManager.pauseLiveServer();
         }
      }
   }

   private class SharedStoreBackupActivation implements Activation
   {
      public void run()
      {
         try
         {
            nodeManager.startBackup();

            initialisePart1();

            clusterManager.start();

            started = true;

            log.info("HornetQ Backup Server version " + getVersion().getFullVersion() +
                     " [" +
                     nodeManager.getNodeId() +
                     "] started, waiting live to fail before it gets active");

            nodeManager.awaitLiveNode();

            configuration.setBackup(false);

            if (stopped)
            {
               return;
            }

            initialisePart2();

            clusterManager.activate();

            log.info("Backup Server is now live");

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
               log.error("Failure in initialisation", e);
            }
         }
         catch (Throwable e)
         {
            log.error("Failure in initialisation", e);
         }
      }

      /**
       *
       */
      public void close(boolean permanently) throws Exception
      {
         if (configuration.isBackup())
         {
            long timeout = 30000;

            long start = System.currentTimeMillis();

            while (backupActivationThread.isAlive() && System.currentTimeMillis() - start < timeout)
            {
               nodeManager.interrupt();

               backupActivationThread.interrupt();

               backupActivationThread.join(1000);

            }

            if (System.currentTimeMillis() - start >= timeout)
            {
               threadDump("Timed out waiting for backup activation to exit");
            }

            nodeManager.stopBackup();
         }
         else
         {
            // if we are now live, behave as live
            // We need to delete the file too, otherwise the backup will failover when we shutdown or if the backup is
            // started before the live
            if (permanently)
            {
               nodeManager.crashLiveServer();
            }
            else
            {
               nodeManager.pauseLiveServer();
            }
         }
      }
   }

   private class ShutdownOnCriticalErrorListener implements IOCriticalErrorListener
   {
      boolean failedAlready = false;

      public synchronized void onIOException(int code, String message, SequentialFile file)
      {
         if (!failedAlready)
         {
            failedAlready = true;

            log.warn("Critical IO Error, shutting down the server. code=" + code + ", message=" + message);

            new Thread()
            {
               public void run()
               {
                  try
                  {
                     HornetQServerImpl.this.stop(true, true);
                  }
                  catch (Exception e)
                  {
                     log.warn(e.getMessage(), e);
                  }
               }
            }.run();
         }
      }
   }

   private interface Activation extends Runnable
   {
      void close(boolean permanently) throws Exception;
   }

   private class SharedNothingBackupActivation implements Activation
   {
      public void run()
      {
         try
         {
            // TODO

            // Try-Connect to live server using live-connector-ref

            // sit in loop and try and connect, if server is not live then it will return NOT_LIVE
         }
         catch (Exception e)
         {
            log.error("Failure in initialisation", e);
         }
      }

      public void close(boolean permanently) throws Exception
      {
      }
   }

   private class NoSharedStoreLiveActivation implements Activation
   {
      public void run()
      {
         try
         {
            initialisePart1();

            initialisePart2();

            if (identity != null)
            {
               log.info("Server " + identity + " is now live");
            }
            else
            {
               log.info("Server is now live");
            }
         }
         catch (Exception e)
         {
            log.error("Failure in initialisation", e);
         }
      }

      public void close(boolean permanently) throws Exception
      {

      }
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


}
