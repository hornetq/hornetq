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
package org.jboss.messaging.core.impl.server;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.jboss.aop.microcontainer.aspects.jmx.JMX;
import org.jboss.jms.server.ConnectionManager;
import org.jboss.jms.server.connectionmanager.SimpleConnectionManager;
import org.jboss.jms.server.endpoint.MessagingServerPacketHandler;
import org.jboss.jms.server.endpoint.ServerConnection;
import org.jboss.jms.server.endpoint.ServerConnectionEndpoint;
import org.jboss.jms.server.endpoint.ServerConnectionPacketHandler;
import org.jboss.jms.server.security.NullAuthenticationManager;
import org.jboss.jms.server.security.Role;
import org.jboss.jms.server.security.SecurityMetadataStore;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.Binding;
import org.jboss.messaging.core.Configuration;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.MemoryManager;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.MessagingServer;
import org.jboss.messaging.core.NullPersistenceManager;
import org.jboss.messaging.core.PersistenceManager;
import org.jboss.messaging.core.PostOffice;
import org.jboss.messaging.core.Queue;
import org.jboss.messaging.core.QueueFactory;
import org.jboss.messaging.core.QueueSettings;
import org.jboss.messaging.core.ResourceManager;
import org.jboss.messaging.core.impl.QueueFactoryImpl;
import org.jboss.messaging.core.impl.ResourceManagerImpl;
import org.jboss.messaging.core.impl.memory.SimpleMemoryManager;
import org.jboss.messaging.core.impl.messagecounter.MessageCounterManager;
import org.jboss.messaging.core.impl.postoffice.PostOfficeImpl;
import org.jboss.messaging.core.remoting.Interceptor;
import org.jboss.messaging.core.remoting.RemotingConfiguration;
import org.jboss.messaging.core.remoting.RemotingService;
import org.jboss.messaging.core.remoting.impl.mina.MinaService;
import org.jboss.messaging.core.remoting.wireformat.CreateConnectionResponse;
import org.jboss.messaging.deployers.queue.QueueSettingsDeployer;
import org.jboss.messaging.deployers.security.SecurityDeployer;
import org.jboss.messaging.util.ExceptionUtil;
import org.jboss.messaging.util.HierarchicalObjectRepository;
import org.jboss.messaging.util.HierarchicalRepository;
import org.jboss.messaging.util.Version;
import org.jboss.security.AuthenticationManager;

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
@JMX(name = "jboss.messaging:service=MessagingServer", exposedInterface = MessagingServer.class)
public class MessagingServerImpl implements MessagingServer
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(MessagingServerImpl.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private Version version;

   private volatile boolean started;

   // wired components

   private SecurityMetadataStore securityStore;
   private SimpleConnectionManager connectionManager;
   private MemoryManager memoryManager = new SimpleMemoryManager();
   private MessageCounterManager messageCounterManager;
   private PostOffice postOffice;
   private SecurityDeployer securityDeployer;
   private QueueSettingsDeployer queueSettingsDeployer;
   private AuthenticationManager authenticationManager = new NullAuthenticationManager();

   // plugins

   private PersistenceManager persistenceManager = new NullPersistenceManager();


   private RemotingService remotingService;
   private boolean createTransport = false;

   private Configuration configuration = new Configuration();
   private HierarchicalRepository<HashSet<Role>> securityRepository = new HierarchicalObjectRepository<HashSet<Role>>();
   private HierarchicalRepository<QueueSettings> queueSettingsRepository = new HierarchicalObjectRepository<QueueSettings>();
   private QueueFactory queueFactory;
   private ResourceManager resourceManager = new ResourceManagerImpl(0);
   private ScheduledExecutorService scheduledExecutor;

   // Constructors ---------------------------------------------------------------------------------
   /**
    * typically called by the MC framework or embedded if the user want to create and start their own RemotingService
    */
   public MessagingServerImpl()
   {
      // Some wired components need to be started here

      version = Version.instance();

      started = false;
   }

   /**
    * called when the usewr wants the MessagingServer to handle the creation of the RemotingTransport
    *
    * @param remotingConfiguration the RemotingConfiguration
    */
   public MessagingServerImpl(RemotingConfiguration remotingConfiguration)
   {
      this();
      createTransport = true;
      remotingService = new MinaService(remotingConfiguration);
   }
   // lifecycle methods ----------------------------------------------------------------

   public synchronized void start() throws Exception
   {
      try
      {
         log.debug("starting MessagingServer");

         if (started)
         {
            return;
         }

         if (configuration.getMessagingServerID() < 0)
         {
            throw new IllegalStateException("MessagingServer ID not set");
         }

         log.debug(this + " starting");

         // Create the wired components

         securityStore = new SecurityMetadataStore();
         securityRepository.setDefault(new HashSet<Role>());
         securityStore.setSecurityRepository(securityRepository);
         securityStore.setAuthenticationManager(authenticationManager);
         securityDeployer = new SecurityDeployer();
         securityDeployer.setSecurityRepository(securityRepository);
         queueSettingsDeployer = new QueueSettingsDeployer();
         queueSettingsRepository.setDefault(new QueueSettings());
         queueSettingsDeployer.setQueueSettingsRepository(queueSettingsRepository);
         scheduledExecutor = new ScheduledThreadPoolExecutor(configuration.getScheduledThreadPoolMaxSize());
         queueFactory = new QueueFactoryImpl(scheduledExecutor);
         queueFactory.setQueueSettingsRepository(queueSettingsRepository);
         connectionManager = new SimpleConnectionManager();
         memoryManager = new SimpleMemoryManager();
         messageCounterManager = new MessageCounterManager(configuration.getMessageCounterSamplePeriod());
         configuration.addPropertyChangeListener(new PropertyChangeListener()
         {
            public void propertyChange(PropertyChangeEvent evt)
            {
               if (evt.getPropertyName().equals("messageCounterSamplePeriod"))
                  messageCounterManager.reschedule(configuration.getMessageCounterSamplePeriod());
            }
         });
         postOffice = new PostOfficeImpl(configuration.getMessagingServerID(),
                 persistenceManager, queueFactory, configuration.isStrictTck());

         if (createTransport)
         {
            remotingService.start();
         }
         // Start the wired components
         securityDeployer.start();
         queueSettingsDeployer.start();
         connectionManager.start();
         remotingService.addFailureListener(connectionManager);
         memoryManager.start();
         postOffice.start();
         
         MessagingServerPacketHandler serverPacketHandler =	new MessagingServerPacketHandler(this);
                  
         getRemotingService().getDispatcher().register(serverPacketHandler);

         ClassLoader loader = Thread.currentThread().getContextClassLoader();
         for (String interceptorClass : configuration.getDefaultInterceptors())
         {
            try
            {
               Class clazz = loader.loadClass(interceptorClass);
               getRemotingService().addInterceptor((Interceptor) clazz.newInstance());
            }
            catch (Exception e)
            {
               log.warn("Error instantiating interceptor \"" + interceptorClass + "\"", e);
            }
         }

         started = true;
         log.info("JBoss Messaging " + getVersion().getProviderVersion() + " server [" +
                 configuration.getMessagingServerID() + "] started");
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " startService");
      }
   }

   public synchronized void stop() throws Exception
   {
      try
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
         connectionManager.stop();
         remotingService.removeFailureListener(connectionManager);
         connectionManager = null;
         memoryManager.stop();
         memoryManager = null;
         messageCounterManager.stop();
         messageCounterManager = null;
         postOffice.stop();
         postOffice = null;
         scheduledExecutor.shutdown();
         scheduledExecutor = null;
         if (createTransport)
         {
            remotingService.stop();
         }
         log.info("JMS " + this + " stopped");
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " stopService");
      }
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


   public void enableMessageCounters()
   {
      messageCounterManager.start();
   }

   public void disableMessageCounters()
   {
      messageCounterManager.stop();

      messageCounterManager.resetAllCounters();

      messageCounterManager.resetAllCounterHistories();
   }

   public void createQueue(String address, String name) throws Exception
   {
      if (postOffice.getBinding(name) == null)
      {
         postOffice.addBinding(address, name, null, true, false);
      }

      if (!postOffice.containsAllowableAddress(address))
      {
         postOffice.addAllowableAddress(address);
      }
   }

   public boolean destroyQueuesByAddress(String address) throws Exception
   {
      List<Binding> bindings = postOffice.getBindingsForAddress(address);

      boolean destroyed = false;

      for (Binding binding : bindings)
      {
         Queue queue = binding.getQueue();

         persistenceManager.deleteAllReferences(queue);

         queue.removeAllReferences();

         postOffice.removeBinding(queue.getName());

         destroyed = true;
      }

      postOffice.removeAllowableAddress(address);

      return destroyed;
   }

   public boolean destroyQueue(String name) throws Exception
   {
      Binding binding = postOffice.getBinding(name);

      boolean destroyed = false;

      if (binding != null)
      {
         Queue queue = binding.getQueue();

         persistenceManager.deleteAllReferences(queue);

         queue.removeAllReferences();

         postOffice.removeBinding(queue.getName());

         destroyed = true;
      }

      return destroyed;
   }

   public boolean addAddress(String address)
   {
      if (!postOffice.containsAllowableAddress(address))
      {
      	postOffice.addAllowableAddress(address);
         return true;
      }
      return false;
   }

   public boolean removeAddress(String address)
   {
      if (postOffice.containsAllowableAddress(address))
      {
      	postOffice.removeAllowableAddress(address);
         return true;
      }
      return false;
   }

   public void resetAllMessageCounters()
   {
      this.messageCounterManager.resetAllCounters();
   }

   public void resetAllMessageCounterHistories()
   {
      this.messageCounterManager.resetAllCounterHistories();
   }


   public void removeAllMessagesForAddress(String address) throws Exception
   {
      List<Binding> bindings = postOffice.getBindingsForAddress(address);

      for (Binding binding : bindings)
      {
         Queue queue = binding.getQueue();

         if (queue.isDurable())
         {
            persistenceManager.deleteAllReferences(queue);
         }

         queue.removeAllReferences();
      }
   }

   public void removeAllMessagesForBinding(String name) throws Exception
   {
      Binding binding = postOffice.getBinding(name);
      if (binding != null)
      {
         Queue queue = binding.getQueue();

         persistenceManager.deleteAllReferences(queue);

         queue.removeAllReferences();
      }
   }

   public void removeMessageForBinding(String name, Filter filter) throws Exception
   {
      Binding binding = postOffice.getBinding(name);
      if (binding != null)
      {
         Queue queue = binding.getQueue();
         List<MessageReference> allRefs = queue.list(filter);
         for (MessageReference messageReference : allRefs)
         {
            persistenceManager.deleteReference(messageReference);
            queue.removeReference(messageReference);
         }
      }
   }

   public ConnectionManager getConnectionManager()
   {
      return connectionManager;
   }

   public PersistenceManager getPersistenceManager()
   {
      return persistenceManager;
   }

   public void setPersistenceManager(PersistenceManager persistenceManager)
   {
      this.persistenceManager = persistenceManager;
   }

   public PostOffice getPostOffice()
   {
      return postOffice;
   }

   public void setPostOffice(PostOffice postOffice)
   {
      this.postOffice = postOffice;
   }

   public HierarchicalRepository<HashSet<Role>> getSecurityRepository()
   {
      return securityRepository;
   }

   public HierarchicalRepository<QueueSettings> getQueueSettingsRepository()
   {
      return queueSettingsRepository;
   }

   public void setAuthenticationManager(AuthenticationManager authenticationManager)
   {
      this.authenticationManager = authenticationManager;
   }


   public String toString()
   {
      return "MessagingServer[" + configuration.getMessagingServerID() + "]";
   }
   
   public CreateConnectionResponse createConnection(final String username, final String password,
                                                    final String remotingClientSessionID, final String clientVMID,
                                                    final int prefetchSize, final String clientAddress)
      throws Exception
   {
      log.trace("creating a new connection for user " + username);
      
      // Authenticate. Successful autentication will place a new SubjectContext on thread local,
      // which will be used in the authorization process. However, we need to make sure we clean
      // up thread local immediately after we used the information, otherwise some other people
      // security my be screwed up, on account of thread local security stack being corrupted.
      
      securityStore.authenticate(username, password);
      
      final ServerConnection connection =
         new ServerConnectionEndpoint(username, password,
                          remotingClientSessionID, clientVMID, clientAddress,
                          prefetchSize, remotingService.getDispatcher(), resourceManager, persistenceManager,
                          postOffice, securityStore, connectionManager);
      
      remotingService.getDispatcher().register(new ServerConnectionPacketHandler(connection));
      
      return new CreateConnectionResponse(connection.getID());
   }

   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   private boolean destroyDestination(String address) throws Exception
   {


      List<Binding> bindings = postOffice.getBindingsForAddress(address);

      boolean destroyed = false;

      for (Binding binding : bindings)
      {
         Queue queue = binding.getQueue();

         persistenceManager.deleteAllReferences(queue);

         queue.removeAllReferences();

         postOffice.removeBinding(queue.getName());

         destroyed = true;
      }

      postOffice.removeAllowableAddress(address);

      return destroyed;
   }
}
