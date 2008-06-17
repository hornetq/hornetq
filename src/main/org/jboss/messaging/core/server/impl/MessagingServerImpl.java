/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.messaging.core.server.impl;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;

import org.jboss.messaging.core.client.RemotingSessionListener;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.deployers.Deployer;
import org.jboss.messaging.core.deployers.DeploymentManager;
import org.jboss.messaging.core.deployers.impl.FileDeploymentManager;
import org.jboss.messaging.core.deployers.impl.QueueSettingsDeployer;
import org.jboss.messaging.core.deployers.impl.SecurityDeployer;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.memory.MemoryManager;
import org.jboss.messaging.core.memory.impl.SimpleMemoryManager;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.persistence.impl.nullpm.NullStorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.postoffice.impl.PostOfficeImpl;
import org.jboss.messaging.core.remoting.ConnectorRegistryFactory;
import org.jboss.messaging.core.remoting.Interceptor;
import org.jboss.messaging.core.remoting.PacketReturner;
import org.jboss.messaging.core.remoting.RemotingService;
import org.jboss.messaging.core.remoting.impl.RemotingServiceImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateConnectionResponse;
import org.jboss.messaging.core.security.JBMSecurityManager;
import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.security.SecurityStore;
import org.jboss.messaging.core.security.impl.JBMSecurityManagerImpl;
import org.jboss.messaging.core.security.impl.SecurityStoreImpl;
import org.jboss.messaging.core.server.ConnectionManager;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.QueueFactory;
import org.jboss.messaging.core.server.ServerConnection;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.HierarchicalObjectRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.core.transaction.ResourceManager;
import org.jboss.messaging.core.transaction.impl.ResourceManagerImpl;
import org.jboss.messaging.core.version.Version;
import org.jboss.messaging.util.ExecutorFactory;
import org.jboss.messaging.util.OrderedExecutorFactory;
import org.jboss.messaging.util.VersionLoader;


/**
 * A Messaging Server
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:juha@jboss.org">Juha Lindfors</a>
 * @author <a href="mailto:aslak@conduct.no">Aslak Knutsen</a>
 * @author <a href="mailto:ataylor@redhat.com>Andy Taylor</a>
 * @version <tt>$Revision: 3543 $</tt>
 *          <p/>
 *          $Id: ServerPeer.java 3543 2008-01-07 22:31:58Z clebert.suconic@jboss.com $
 */
public class MessagingServerImpl implements MessagingServer
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(MessagingServerImpl.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private final Version version;

   private volatile boolean started;

   // wired components

   private SecurityStore securityStore;
   private ConnectionManager connectionManager;
   private RemotingSessionListener sessionListener;
   private MemoryManager memoryManager = new SimpleMemoryManager();
   private PostOffice postOffice;
   private Deployer securityDeployer;
   private Deployer queueSettingsDeployer;   
   private DeploymentManager deploymentManager = new FileDeploymentManager();
   private ExecutorFactory executorFactory;
   private ExecutorService threadPool;
   private HierarchicalRepository<Set<Role>> securityRepository = new HierarchicalObjectRepository<Set<Role>>();
   private HierarchicalRepository<QueueSettings> queueSettingsRepository = new HierarchicalObjectRepository<QueueSettings>();
   private QueueFactory queueFactory;
   private ResourceManager resourceManager = new ResourceManagerImpl(0);
   private ScheduledExecutorService scheduledExecutor;
   private MessagingServerPacketHandler serverPacketHandler;

   // plugins

   private StorageManager storageManager = new NullStorageManager();
   private RemotingService remotingService;
   private JBMSecurityManager securityManager = new JBMSecurityManagerImpl(true);   
   private boolean createTransport = false;
   private Configuration configuration;
   
     
   // Constructors ---------------------------------------------------------------------------------
   
   /**
    * typically called by the MC framework or embedded if the user want to create and start their own RemotingService
    */
   public MessagingServerImpl()
   {
      //We need to hard code the version information into a source file

      version = VersionLoader.load();
      
      //Default config
      configuration = new ConfigurationImpl();
   }

   /**
    * called when the usewr wants the MessagingServer to handle the creation of the RemotingTransport
    *
    * @param configuration the configuration
    */
   public MessagingServerImpl(final Configuration configuration)
   {
      version = VersionLoader.load();
                  
      this.configuration = configuration;
      createTransport = true;
      remotingService = new RemotingServiceImpl(configuration);
   }
   
   // lifecycle methods ----------------------------------------------------------------

   public synchronized void start() throws Exception
   {
      log.debug("starting MessagingServer");

      if (started)
      {
         return;
      }

      log.debug(this + " starting");

      // Create the wired components

      securityStore = new SecurityStoreImpl(configuration.getSecurityInvalidationInterval(), configuration.isSecurityEnabled());
      securityRepository.setDefault(new HashSet<Role>());
      securityStore.setSecurityRepository(securityRepository);
      securityStore.setSecurityManager(securityManager);
      securityDeployer = new SecurityDeployer(securityRepository);
      queueSettingsRepository.setDefault(new QueueSettings());
      scheduledExecutor = new ScheduledThreadPoolExecutor(configuration.getScheduledThreadPoolMaxSize(), new JBMThreadFactory("JBM-scheduled-threads"));
      queueFactory = new QueueFactoryImpl(scheduledExecutor, queueSettingsRepository);
      ConnectionManagerImpl cm = new ConnectionManagerImpl();
      this.connectionManager = cm;
      this.sessionListener = cm;
      memoryManager = new SimpleMemoryManager();
      postOffice = new PostOfficeImpl(storageManager, queueFactory, configuration.isRequireDestinations());
      queueSettingsDeployer = new QueueSettingsDeployer(queueSettingsRepository);
      threadPool = Executors.newFixedThreadPool(configuration.getThreadPoolMaxSize(), new JBMThreadFactory("JBM-session-threads"));
      executorFactory = new OrderedExecutorFactory(threadPool);

      if (createTransport)
      {
         remotingService.start();
      }
      // Start the wired components
      securityDeployer.start();
      remotingService.addRemotingSessionListener(sessionListener);
      memoryManager.start();
      deploymentManager.start(1);
      deploymentManager.registerDeployer(securityDeployer);
      deploymentManager.registerDeployer(queueSettingsDeployer);
      postOffice.start();
      deploymentManager.start(2);
      serverPacketHandler = new MessagingServerPacketHandler(this);
      getRemotingService().getDispatcher().register(serverPacketHandler);
      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      for (String interceptorClass : configuration.getInterceptorClassNames())
      {
         try
         {
            Class<?> clazz = loader.loadClass(interceptorClass);
            getRemotingService().addInterceptor((Interceptor) clazz.newInstance());
         }
         catch (Exception e)
         {
            log.warn("Error instantiating interceptor \"" + interceptorClass + "\"", e);
         }
      }

      started = true;
   }

   public synchronized void stop() throws Exception
   {
      if (!started)
      {
         return;
      }

      log.info(this + " is Stopping. NOTE! Stopping the server peer cleanly will NOT cause failover to occur");

      started = false;

      // Stop the wired components
      securityDeployer.stop();
      queueSettingsDeployer.stop();
      deploymentManager.stop();
      remotingService.removeRemotingSessionListener(sessionListener);
      connectionManager = null;
      memoryManager.stop();
      memoryManager = null;
      postOffice.stop();
      postOffice = null;
      scheduledExecutor.shutdown();
      scheduledExecutor = null;
      threadPool.shutdown();
      threadPool = null;
      executorFactory = null;
      if (createTransport)
      {
         remotingService.stop();
      }
      ConnectorRegistryFactory.getRegistry().clear();
   }

   // MessagingServer implementation -----------------------------------------------------------

   public Version getVersion()
   {
      return version;
   }

   public Configuration getConfiguration()
   {
      return configuration;
   }

   public boolean isStarted()
   {
      return started;
   }

   public void setConfiguration(Configuration configuration)
   {
      this.configuration = configuration;
   }

   public void setRemotingService(RemotingService remotingService)
   {
      this.remotingService = remotingService;
   }

   public RemotingService getRemotingService()
   {
      return remotingService;
   }

   public DeploymentManager getDeploymentManager()
   {
      return deploymentManager;
   }

   public ConnectionManager getConnectionManager()
   {
      return connectionManager;
   }

   public StorageManager getStorageManager()
   {
      return storageManager;
   }

   public void setStorageManager(StorageManager storageManager)
   {
      this.storageManager = storageManager;
   }

   public PostOffice getPostOffice()
   {
      return postOffice;
   }

   public void setPostOffice(PostOffice postOffice)
   {
      this.postOffice = postOffice;
   }

   public HierarchicalRepository<Set<Role>> getSecurityRepository()
   {
      return securityRepository;
   }

   public HierarchicalRepository<QueueSettings> getQueueSettingsRepository()
   {
      return queueSettingsRepository;
   }

   public SecurityStore getSecurityStore()
   {
      return securityStore;
   }

   public JBMSecurityManager getSecurityManager()
   {
      return securityManager;
   }

   public void setSecurityManager(JBMSecurityManager securityManager)
   {
      this.securityManager = securityManager;
   }

   public CreateConnectionResponse createConnection(final String username, final String password,
                                                    final long remotingClientSessionID, final String clientAddress,
                                                    final int incrementVersion,
                                                    final PacketReturner sender)
           throws Exception
   {
      log.trace("creating a new connection for user " + username);

      if (version.getIncrementingVersion() < incrementVersion)
      {
         throw new MessagingException(MessagingException.INCOMPATIBLE_CLIENT_SERVER_VERSIONS,
                 "client not compatible with version: " + version.getFullVersion());
      }
      // Authenticate. Successful autentication will place a new SubjectContext on thread local,
      // which will be used in the authorization process. However, we need to make sure we clean
      // up thread local immediately after we used the information, otherwise some other people
      // security my be screwed up, on account of thread local security stack being corrupted.

      securityStore.authenticate(username, password);

      final ServerConnection connection =
              new ServerConnectionImpl(this, username, password,
                      sender.getSessionID(), clientAddress);

      remotingService.getDispatcher().register(new ServerConnectionPacketHandler(connection));

      return new CreateConnectionResponse(connection.getID(), version);
   }

   public ExecutorFactory getExecutorFactory()
   {
      return executorFactory;
   }

   public ResourceManager getResourceManager()
   {
      return resourceManager;
   }
   
   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

   private static class JBMThreadFactory implements ThreadFactory
   {
      private ThreadGroup group;

      JBMThreadFactory(final String groupName)
      {
         this.group = new ThreadGroup(groupName);
      }

      public Thread newThread(Runnable command)
      {
         return new Thread(group, command);
      }
   }

}
