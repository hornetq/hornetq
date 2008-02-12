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

import org.jboss.aop.microcontainer.aspects.jmx.JMX;
import org.jboss.jms.server.ConnectionManager;
import org.jboss.jms.server.MessagingTimeoutFactory;
import org.jboss.jms.server.SecurityStore;
import org.jboss.jms.server.connectionmanager.SimpleConnectionManager;
import org.jboss.jms.server.endpoint.MessagingServerPacketHandler;
import org.jboss.jms.server.security.NullAuthenticationManager;
import org.jboss.jms.server.security.Role;
import org.jboss.jms.server.security.SecurityMetadataStore;
import org.jboss.jms.server.security.CheckType;
import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.*;
import org.jboss.messaging.core.impl.QueueFactoryImpl;
import org.jboss.messaging.core.impl.ResourceManagerImpl;
import org.jboss.messaging.core.impl.memory.SimpleMemoryManager;
import org.jboss.messaging.core.impl.messagecounter.MessageCounterManager;
import org.jboss.messaging.core.impl.postoffice.PostOfficeImpl;
import org.jboss.messaging.core.remoting.Interceptor;
import org.jboss.messaging.core.remoting.RemotingConfiguration;
import org.jboss.messaging.core.remoting.RemotingService;
import org.jboss.messaging.core.remoting.impl.mina.MinaService;
import org.jboss.messaging.deployers.queue.QueueSettingsDeployer;
import org.jboss.messaging.deployers.security.SecurityDeployer;
import org.jboss.messaging.util.ExceptionUtil;
import org.jboss.messaging.util.HierarchicalObjectRepository;
import org.jboss.messaging.util.HierarchicalRepository;
import org.jboss.messaging.util.Version;
import org.jboss.security.AuthenticationManager;

import javax.jms.Destination;

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
   private QueueFactory queueFactory = new QueueFactoryImpl();
   private ResourceManager resourceManager = new ResourceManagerImpl(0);

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
         queueFactory.setQueueSettingsRepository(queueSettingsRepository);
         connectionManager = new SimpleConnectionManager();
         memoryManager = new SimpleMemoryManager();
         messageCounterManager = new MessageCounterManager(configuration.getMessageCounterSamplePeriod());
         configuration.addPropertyChangeListener(new PropertyChangeListener()
         {
            public void propertyChange(PropertyChangeEvent evt)
            {
               if(evt.getPropertyName().equals("messageCounterSamplePeriod"))
                  messageCounterManager.reschedule(configuration.getMessageCounterSamplePeriod());
            }
         });
         postOffice = new PostOfficeImpl(configuration.getMessagingServerID(),
                                         persistenceManager, queueFactory, configuration.isStrictTck());

         if(createTransport)
         {
            remotingService.start();
         }
         // Start the wired components
         securityDeployer.start();
         queueSettingsDeployer.start();
         connectionManager.start();
         remotingService.addConnectionExceptionListener(connectionManager);
         memoryManager.start();
         postOffice.start();
         MessagingServerPacketHandler serverPacketHandler =  new MessagingServerPacketHandler(this);
         getRemotingService().getDispatcher().register(serverPacketHandler);

         ClassLoader loader = Thread.currentThread().getContextClassLoader();
         for (String interceptorClass: configuration.getDefaultInterceptors())
         {
            try
            {
               Class clazz = loader.loadClass(interceptorClass);
               getRemotingService().addInterceptor((Interceptor)clazz.newInstance());
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
         remotingService.removeConnectionExceptionListener(connectionManager);
         connectionManager = null;
         memoryManager.stop();
         memoryManager = null;
         messageCounterManager.stop();
         messageCounterManager = null;
         postOffice.stop();
         postOffice = null;
         if(createTransport)
         {
            remotingService.stop();
         }
         MessagingTimeoutFactory.instance.reset();

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

   public void createQueue(String name) throws Exception
   {
      JBossQueue queue = new JBossQueue(name);

      if (getPostOffice().getBinding(queue.getAddress()) == null)
      {
         getPostOffice().addBinding(queue.getAddress(), queue.getAddress(), null, true, false);
      }

      if (!getPostOffice().containsAllowableAddress(queue.getAddress()))
      {
         getPostOffice().addAllowableAddress(queue.getAddress());
      }
   }

   public void destroyQueue(String name) throws Exception
   {
      destroyDestination(true, name);
   }

   public void createTopic(String name) throws Exception
   {
      JBossTopic topic = new JBossTopic(name);

      if (!getPostOffice().containsAllowableAddress(topic.getAddress()));
      {
         getPostOffice().addAllowableAddress(topic.getAddress());
      }
   }

   public void destroyTopic(String name) throws Exception
   {
      destroyDestination(false, name);
   }

   public void resetAllMessageCounters()
   {
      this.messageCounterManager.resetAllCounters();
   }

   public void resetAllMessageCounterHistories()
   {
      this.messageCounterManager.resetAllCounterHistories();
   }

   public void removeAllMessagesForQueue(String queueName) throws Exception
   {
      JBossQueue jbq = new JBossQueue(queueName);

      List<Binding> bindings = postOffice.getBindingsForAddress(jbq.getAddress());

      if (!bindings.isEmpty())
      {
         Queue queue = bindings.get(0).getQueue();

         persistenceManager.deleteAllReferences(queue);

         queue.removeAllReferences();
      }
   }

   public void removeAllMessagesForTopic(String queueName) throws Exception
   {
      JBossTopic jbt = new JBossTopic(queueName);

      List<Binding> bindings = postOffice.getBindingsForAddress(jbt.getAddress());

      for (Binding binding: bindings)
      {
         Queue queue = binding.getQueue();

         if (queue.isDurable())
         {
            persistenceManager.deleteAllReferences(queue);
         }

         queue.removeAllReferences();
      }
   }

   public SecurityStore getSecurityManager()
   {
      return securityStore;
   }

   public ConnectionManager getConnectionManager()
   {
      return connectionManager;
   }

   public MemoryManager getMemoryManager()
   {
      return memoryManager;
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

   public ResourceManager getResourceManager()
   {
      return resourceManager;
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

   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   private boolean destroyDestination(boolean isQueue, String name) throws Exception
   {
      JBossDestination dest;

      if (isQueue)
      {
         dest = new JBossQueue(name);
      }
      else
      {
         dest = new JBossTopic(name);
      }

      List<Binding> bindings = getPostOffice().getBindingsForAddress(dest.getAddress());

      boolean destroyed = false;

      for (Binding binding: bindings)
      {
         Queue queue = binding.getQueue();

         getPersistenceManager().deleteAllReferences(queue);

         queue.removeAllReferences();

         getPostOffice().removeBinding(queue.getName());

         destroyed = true;
      }

      getPostOffice().removeAllowableAddress(dest.getAddress());

      return destroyed;
   }
}
