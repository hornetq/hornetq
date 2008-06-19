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
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.MessagingServerManagement;
import org.jboss.messaging.core.management.impl.MessagingServerManagementImpl;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.postoffice.impl.PostOfficeImpl;
import org.jboss.messaging.core.remoting.ConnectorRegistryFactory;
import org.jboss.messaging.core.remoting.Interceptor;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.PacketReturner;
import org.jboss.messaging.core.remoting.RemotingService;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateConnectionResponse;
import org.jboss.messaging.core.security.JBMSecurityManager;
import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.security.SecurityStore;
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
 * The messaging server implementation
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
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
   private HierarchicalRepository<QueueSettings> queueSettingsRepository;
   private ScheduledExecutorService scheduledExecutor;   
   private QueueFactory queueFactory;
   private PostOffice postOffice;
   private ExecutorService threadPool;
   private ExecutorFactory executorFactory;   
   private HierarchicalRepository<Set<Role>> securityRepository;
   private ResourceManager resourceManager;   
   private MessagingServerPacketHandler serverPacketHandler;
   private MessagingServerManagement serverManagement;
   private PacketDispatcher dispatcher;

   // plugins

   private StorageManager storageManager;
   private RemotingService remotingService;
   private JBMSecurityManager securityManager;  
   private Configuration configuration;
        
   // Constructors ---------------------------------------------------------------------------------
   
   public MessagingServerImpl()
   {
      //We need to hard code the version information into a source file

      version = VersionLoader.load();
   }
   
   // lifecycle methods ----------------------------------------------------------------

   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }

      /*
      The following components are pluggable on the messaging server:
      Configuration, StorageManager, RemotingService and SecurityManager
      They must already be injected by the time the messaging server starts
      It's up to the user to make sure the pluggable components are started - their
      lifecycle will not be controlled here
      */
      
      //We make sure the pluggable components have been injected
      if (configuration == null)
      {
         throw new IllegalStateException("Must inject Configuration before starting MessagingServer");
      }
      
      if (storageManager == null)
      {
         throw new IllegalStateException("Must inject StorageManager before starting MessagingServer");
      }
      
      if (remotingService == null)
      {
         throw new IllegalStateException("Must inject RemotingService before starting MessagingServer");
      }
      
      if (securityManager == null)
      {
         throw new IllegalStateException("Must inject SecurityManager before starting MessagingServer");
      }      
      
      if (!storageManager.isStarted())
      {
         throw new IllegalStateException("StorageManager must be started before MessagingServer is started");
      }
      
      if (!remotingService.isStarted())
      {
         throw new IllegalStateException("RemotingService must be started before MessagingServer is started");
      }
                 
      //The rest of the components are not pluggable and created and started here

      securityStore = new SecurityStoreImpl(configuration.getSecurityInvalidationInterval(), configuration.isSecurityEnabled());
      ConnectionManagerImpl cm = new ConnectionManagerImpl();
      this.connectionManager = cm;
      this.sessionListener = cm;   
      queueSettingsRepository = new HierarchicalObjectRepository<QueueSettings>();
      queueSettingsRepository.setDefault(new QueueSettings());
      scheduledExecutor = new ScheduledThreadPoolExecutor(configuration.getScheduledThreadPoolMaxSize(), new JBMThreadFactory("JBM-scheduled-threads"));                  
      queueFactory = new QueueFactoryImpl(scheduledExecutor, queueSettingsRepository);      
      postOffice = new PostOfficeImpl(storageManager, queueFactory, configuration.isRequireDestinations());
      threadPool = Executors.newFixedThreadPool(configuration.getThreadPoolMaxSize(), new JBMThreadFactory("JBM-session-threads"));
      executorFactory = new OrderedExecutorFactory(threadPool);                 
      securityRepository = new HierarchicalObjectRepository<Set<Role>>();
      securityRepository.setDefault(new HashSet<Role>());
      securityStore.setSecurityRepository(securityRepository);
      securityStore.setSecurityManager(securityManager);      
      scheduledExecutor = new ScheduledThreadPoolExecutor(configuration.getScheduledThreadPoolMaxSize(), new JBMThreadFactory("JBM-scheduled-threads"));            
      resourceManager = new ResourceManagerImpl(0);                           
      remotingService.addRemotingSessionListener(sessionListener);  
      dispatcher = remotingService.getDispatcher();
      postOffice.start();
      serverPacketHandler = new MessagingServerPacketHandler(this);          
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
      serverManagement = new MessagingServerManagementImpl(postOffice, storageManager, configuration,
                                                           connectionManager, securityRepository,
                                                           queueSettingsRepository, this);
      //Register the handler as the last thing - since after that users will be able to connect
      started = true;
      dispatcher.register(serverPacketHandler);      
   }

   public synchronized void stop() throws Exception
   {
      if (!started)
      {
         return;
      }
      
      dispatcher.unregister(serverPacketHandler.getID());       
      remotingService.removeRemotingSessionListener(sessionListener);
      
      securityStore = null;
      connectionManager = null;
      sessionListener = null;
      postOffice.stop();
      postOffice = null;
      threadPool.shutdown();
      executorFactory = null;
      securityRepository = null;
      securityStore = null;
      queueSettingsRepository = null;
      scheduledExecutor.shutdown();
      queueFactory = null;
      resourceManager = null;
      serverPacketHandler = null;
      serverManagement = null;
      ConnectorRegistryFactory.getRegistry().clear();
      
      started = false;
   }

   // MessagingServer implementation -----------------------------------------------------------

   
   // The plugabble components

   public void setConfiguration(Configuration configuration)
   {
      if (started)
      {
         throw new IllegalStateException("Cannot set configuration when started");
      }
      
      this.configuration = configuration;
   }
   
   public Configuration getConfiguration()
   {
      return configuration;
   }
   
   public void setRemotingService(RemotingService remotingService)
   {
      if (started)
      {
         throw new IllegalStateException("Cannot set remoting service when started");
      }
      this.remotingService = remotingService;
   }

   public RemotingService getRemotingService()
   {
      return remotingService;
   }

   public void setStorageManager(StorageManager storageManager)
   {
      if (started)
      {
         throw new IllegalStateException("Cannot set storage manager when started");
      }
      this.storageManager = storageManager;
   }
   
   public StorageManager getStorageManager()
   {
      return storageManager;
   }
   
   public void setSecurityManager(JBMSecurityManager securityManager)
   {
      if (started)
      {
         throw new IllegalStateException("Cannot set security Manager when started");
      }
      
      this.securityManager = securityManager;
   }
      
   public JBMSecurityManager getSecurityManager()
   {
      return securityManager;
   }
   
   //This is needed for the security deployer
   public HierarchicalRepository<Set<Role>> getSecurityRepository()
   {
      return securityRepository;
   }
   
   //This is needed for the queue settings deployer
   public HierarchicalRepository<QueueSettings> getQueueSettingsRepository()
   {
      return queueSettingsRepository;
   }

   public Version getVersion()
   {
      return version;
   }
   
   public boolean isStarted()
   {
      return started;
   }

   public CreateConnectionResponse createConnection(final String username, final String password,                                  
                                                    final int incrementingVersion,
                                                    final PacketReturner returner)
           throws Exception
   {
      if (version.getIncrementingVersion() < incrementingVersion)
      {
         throw new MessagingException(MessagingException.INCOMPATIBLE_CLIENT_SERVER_VERSIONS,
                 "client not compatible with version: " + version.getFullVersion());
      }
      
      // Authenticate. Successful autentication will place a new SubjectContext on thread local,
      // which will be used in the authorization process. However, we need to make sure we clean
      // up thread local immediately after we used the information, otherwise some other people
      // security my be screwed up, on account of thread local security stack being corrupted.

      securityStore.authenticate(username, password);

      long sessionID = returner.getSessionID();
      
      log.info("****Session id is " + sessionID);
      
      final ServerConnection connection =
              new ServerConnectionImpl(username, password, sessionID,
                                       postOffice, connectionManager,
                                       dispatcher, storageManager,
                                       queueSettingsRepository, resourceManager,
                                       securityStore, executorFactory);
      
      connectionManager.registerConnection(sessionID, connection);

      dispatcher.register(new ServerConnectionPacketHandler(connection));

      return new CreateConnectionResponse(connection.getID(), version);
   }
         
   public MessagingServerManagement getServerManagement()
   {
      return serverManagement;
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
