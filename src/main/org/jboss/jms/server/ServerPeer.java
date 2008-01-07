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
package org.jboss.jms.server;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jboss.aop.microcontainer.aspects.jmx.JMX;
import org.jboss.jms.server.connectionfactory.ConnectionFactoryDeployer;
import org.jboss.jms.server.connectionfactory.ConnectionFactoryJNDIMapper;
import org.jboss.jms.server.connectionmanager.SimpleConnectionManager;
import org.jboss.jms.server.destination.DestinationDeployer;
import org.jboss.jms.server.destination.ManagedQueue;
import org.jboss.jms.server.destination.ManagedTopic;
import org.jboss.jms.server.endpoint.ServerSessionEndpoint;
import org.jboss.jms.server.messagecounter.MessageCounter;
import org.jboss.jms.server.messagecounter.MessageCounterManager;
import org.jboss.jms.server.plugin.contract.JMSUserManager;
import org.jboss.jms.server.security.SecurityMetadataStore;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.contract.Binding;
import org.jboss.messaging.core.contract.ClusterNotifier;
import org.jboss.messaging.core.contract.MemoryManager;
import org.jboss.messaging.core.contract.MessageStore;
import org.jboss.messaging.core.contract.PersistenceManager;
import org.jboss.messaging.core.contract.PostOffice;
import org.jboss.messaging.core.contract.Queue;
import org.jboss.messaging.core.contract.Replicator;
import org.jboss.messaging.core.impl.IDManager;
import org.jboss.messaging.core.impl.JDBCPersistenceManager;
import org.jboss.messaging.core.impl.memory.SimpleMemoryManager;
import org.jboss.messaging.core.impl.postoffice.MessagingPostOffice;
import org.jboss.messaging.core.impl.tx.TransactionRepository;
import org.jboss.messaging.core.remoting.impl.mina.MinaService;
import org.jboss.messaging.util.ExceptionUtil;
import org.jboss.messaging.util.Version;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;

/**
 * A JMS server peer.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:juha@jboss.org">Juha Lindfors</a>
 * @author <a href="mailto:aslak@conduct.no">Aslak Knutsen</a>
 * @author <a href="mailto:ataylor@redhat.com>Andy Taylor</a>
 * @version <tt>$Revision$</tt>
 *          <p/>
 *          $Id$
 */
@JMX(name = "jboss.messaging:service=ServerPeer", exposedInterface = JmsServer.class)
public class ServerPeer implements JmsServer
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerPeer.class);

   // The "subsystem" label this ServerPeer uses to register its ServerInvocationHandler with the
   // Remoting connector
   public static final String REMOTING_JMS_SUBSYSTEM = "JMS";

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private Version version;


   private boolean started;

   private boolean supportsFailover = true;

   private Map sessions;





   // wired components

   private DestinationJNDIMapper destinationJNDIMapper;
   private SecurityMetadataStore securityStore;
   private ConnectionFactoryJNDIMapper connFactoryJNDIMapper;
   private TransactionRepository txRepository;
   private SimpleConnectionManager connectionManager;
   private IDManager messageIDManager;
   private IDManager channelIDManager;
   private IDManager transactionIDManager;
   private MemoryManager memoryManager;
   private MessageCounterManager messageCounterManager;
   private ClusterNotifier clusterNotifier;

   // plugins

   protected PersistenceManager persistenceManager;

   protected PostOffice postOffice;

   protected JMSUserManager jmsUserManager;


   private DestinationDeployer destinationDeployer;

   private ConnectionFactoryDeployer connectionFactoryDeployer;

   private MinaService minaService;
   
   private Configuration configuration;


   // Constructors ---------------------------------------------------------------------------------
   public ServerPeer() throws Exception
   {
      // Some wired components need to be started here

      version = Version.instance();

      sessions = new ConcurrentReaderHashMap();

      started = false;
   }

   // lifecycle methods ----------------------------------------------------------------

   public synchronized void start() throws Exception
   {
      try
      {
         log.debug("starting ServerPeer");

         if (started)
         {
            return;
         }

         if (configuration.getServerPeerID() < 0)
         {
            throw new IllegalStateException("ServerPeerID not set");
         }

         log.debug(this + " starting");

         ((JDBCPersistenceManager) persistenceManager).injectNodeID(configuration.getServerPeerID());

         // We get references to some plugins lazily to avoid problems with circular MBean
         // dependencies

         // Create the wired components

         securityStore = new SecurityMetadataStore(this);
         messageIDManager = new IDManager("MESSAGE_ID", 4096, persistenceManager);
         channelIDManager = new IDManager("CHANNEL_ID", 10, persistenceManager);
         transactionIDManager = new IDManager("TRANSACTION_ID", 1024, persistenceManager);
         destinationJNDIMapper = new DestinationJNDIMapper(this);
         connFactoryJNDIMapper = new ConnectionFactoryJNDIMapper(this);
         connectionManager = new SimpleConnectionManager();
         memoryManager = new SimpleMemoryManager();
         destinationDeployer = new DestinationDeployer(this);
         connectionFactoryDeployer = new ConnectionFactoryDeployer(this, minaService);
         txRepository =
                 new TransactionRepository(persistenceManager, getPostOffice().getMessageStore(), transactionIDManager);
         messageCounterManager = new MessageCounterManager(configuration.getMessageCounterSamplePeriod());
         configuration.addPropertyChangeListener(new PropertyChangeListener()
         {
            public void propertyChange(PropertyChangeEvent evt)
            {
               if(evt.getPropertyName().equals("messageCounterSamplePeriod"))
                  messageCounterManager.reschedule(configuration.getMessageCounterSamplePeriod());
            }
         });
         //inverted dependancy, post office will now inject this
         //clusterNotifier = new DefaultClusterNotifier();

         clusterNotifier.registerListener(connectionManager);
         clusterNotifier.registerListener(connFactoryJNDIMapper);

         if (configuration.getSuckerPassword() == null)
         {
            configuration.setSuckerPassword(SecurityMetadataStore.DEFAULT_SUCKER_USER_PASSWORD);
         }

         // Start the wired components

         messageIDManager.start();
         channelIDManager.start();
         transactionIDManager.start();
         destinationJNDIMapper.start();
         connFactoryJNDIMapper.start();
         connectionManager.start();
         memoryManager.start();
         securityStore.setSuckerPassword(configuration.getSuckerPassword());
         securityStore.start();
         txRepository.start();

         // Note we do not start the message counter manager by default. This must be done
         // explicitly by the user by calling enableMessageCounters(). This is because message
         // counter history takes up growing memory to store the stats and could theoretically
         // eventually cause the server to run out of RAM

         txRepository.loadPreparedTransactions();

         if (configuration.isClustered())
         {
            Replicator rep = (Replicator) postOffice;

            connFactoryJNDIMapper.injectReplicator(rep);

            connectionManager.injectReplicator((Replicator) postOffice);
         }
         //we inject the server peer because the post office needs it for clustering and the tx repository.
         // This is crap and needs changing
         ((MessagingPostOffice) postOffice).injectServerPeer(this);
         // Also need to inject into txRepository
         txRepository.injectPostOffice(postOffice);


         connectionFactoryDeployer.start();
         destinationDeployer.start();
         
         started = true;
         log.info("JBoss Messaging " + getVersion().getProviderVersion() + " server [" +
                 configuration.getServerPeerID() + "] started");
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
         destinationDeployer.stop();
         destinationDeployer = null;
         connectionFactoryDeployer.stop();
         connectionFactoryDeployer = null;
         messageIDManager.stop();
         messageIDManager = null;
         channelIDManager.stop();
         channelIDManager = null;
         transactionIDManager.stop();
         transactionIDManager = null;
         destinationJNDIMapper.stop();
         destinationJNDIMapper = null;
         connFactoryJNDIMapper.stop();
         connFactoryJNDIMapper = null;
         connectionManager.stop();
         connectionManager = null;
         memoryManager.stop();
         memoryManager = null;
         securityStore.stop();
         //securityStore = null; - if securitySTore is set to null, The ServerPeer won't survive a restart of the service (stop/start)
         txRepository.stop();
         txRepository = null;
         messageCounterManager.stop();
         messageCounterManager = null;
         //postOffice = null;

         MessagingTimeoutFactory.instance.reset();

         log.info("JMS " + this + " stopped");
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " stopService");
      }
   }

   // JMX Attributes -------------------------------------------------------------------------------

   //read only JMX attributes

   public String getJMSVersion()
   {
      return version.getJMSVersion();
   }

   public int getJMSMajorVersion()
   {
      return version.getJMSMajorVersion();
   }

   public int getJMSMinorVersion()
   {
      return version.getJMSMinorVersion();
   }

   public String getJMSProviderName()
   {
      return version.getJMSProviderName();
   }

   public String getProviderVersion()
   {
      return version.getProviderVersion();
   }

   public int getProviderMajorVersion()
   {
      return version.getProviderMajorVersion();
   }

   public int getProviderMinorVersion()
   {
      return version.getProviderMinorVersion();
   }

   //Read - write attributes

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

   // JMX Operations -------------------------------------------------------------------------------

   public String deployQueue(String name, String jndiName) throws Exception
   {
      return destinationDeployer.deployQueue(name, jndiName);
   }

   public String deployQueue(String name, String jndiName, int fullSize, int pageSize, int downCacheSize) throws Exception
   {
      return destinationDeployer.deployQueue(name, jndiName, fullSize, pageSize, downCacheSize);
   }
   public boolean destroyQueue(String name) throws Exception
   {
      try
      {
         return destinationDeployer.destroyQueue(name);
      }
      catch (Throwable throwable)
      {
         throw new Exception(throwable);
      }
   }

   public boolean undeployQueue(String name) throws Exception
   {
      return destinationDeployer.undeployQueue(name);
   }

   public String deployTopic(String name, String jndiName) throws Exception
   {
      return destinationDeployer.deployTopic(name, jndiName);
   }

   public String deployTopic(String name, String jndiName, int fullSize, int pageSize, int downCacheSize) throws Exception
   {
      return destinationDeployer.deployTopic(name, jndiName, fullSize, pageSize, downCacheSize);
   }

   public boolean destroyTopic(String name) throws Exception
   {
      try
      {
         return destinationDeployer.destroyTopic(name);
      }
      catch (Throwable throwable)
      {
         throw new Exception(throwable);
      }
   }

   public boolean undeployTopic(String name) throws Exception
   {
      return destinationDeployer.undeployTopic(name);
   }

   public Set getDestinations() throws Exception
   {
      try
      {
         return destinationJNDIMapper.getDestinations();
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " getDestinations");
      }
   }

   public List getMessageCounters() throws Exception
   {
      Collection counters = messageCounterManager.getMessageCounters();

      return new ArrayList(counters);
   }

   public List getMessageStatistics() throws Exception
   {
      return MessageCounter.getMessageStatistics(getMessageCounters());
   }

   public void resetAllMessageCounters()
   {
      messageCounterManager.resetAllCounters();
   }

   public void resetAllMessageCounterHistories()
   {
      messageCounterManager.resetAllCounterHistories();
   }

   public List retrievePreparedTransactions()
   {
      return txRepository.getPreparedTransactions();
   }

   public void removeAllMessagesForQueue(String queueName) throws Exception
   {
      try
      {
         ((ManagedQueue)getDestinationManager().getDestination(queueName, true)).removeAllMessages();
      }
      catch (Throwable throwable)
      {
         throw new Exception(throwable);
      }
   }

   public void removeAllMessagesForTopic(String queueName) throws Exception
   {
      try
      {
         ((ManagedTopic)getDestinationManager().getDestination(queueName, false)).removeAllMessages();
      }
      catch (Throwable throwable)
      {
         throw new Exception(throwable);
      }
   }

   // Public ---------------------------------------------------------------------------------------

   public MessageCounterManager getMessageCounterManager()
   {
      return messageCounterManager;
   }

   public IDManager getMessageIDManager()
   {
      return messageIDManager;
   }

   public IDManager getChannelIDManager()
   {
      return channelIDManager;
   }

   public ServerSessionEndpoint getSession(String sessionID)
   {
      return (ServerSessionEndpoint) sessions.get(sessionID);
   }

   public Collection getSessions()
   {
      return sessions.values();
   }

   public void addSession(String id, ServerSessionEndpoint session)
   {
      sessions.put(id, session);
   }

   public void removeSession(String id)
   {
      if (sessions.remove(id) == null)
      {
         throw new IllegalStateException("Cannot find session with id " + id + " to remove");
      }
   }

   public synchronized Queue getDefaultDLQInstance() throws Exception
   {
      Queue dlq = null;

      if (configuration.getDefaultDLQ() != null)
      {

         Binding binding = postOffice.getBindingForQueueName(configuration.getDefaultDLQ());

         if (binding == null)
         {
            throw new IllegalStateException("Cannot find binding for queue " + configuration.getDefaultDLQ());
         }

         Queue queue = binding.queue;

         if (queue.isActive())
         {
            dlq = queue;
         }
      }

      return dlq;
   }

   public synchronized Queue getDefaultExpiryQueueInstance() throws Exception
   {
      Queue expiryQueue = null;

      if (configuration.getDefaultExpiryQueue() != null)
      {

         if (configuration.getDefaultDLQ() != null)
         {
            Binding binding = postOffice.getBindingForQueueName(configuration.getDefaultExpiryQueue());

            if (binding == null)
            {
               throw new IllegalStateException("Cannot find binding for queue " + configuration.getDefaultExpiryQueue());
            }

            Queue queue = binding.queue;

            if (queue.isActive())
            {
               expiryQueue = queue;
            }
         }
      }

      return expiryQueue;
   }

   public TransactionRepository getTxRepository()
   {
      return txRepository;
   }

   public synchronized boolean isStarted()
   {
      return started;
   }

   public Version getVersion()
   {
      return version;
   }

   // access to hard-wired server extensions

   public SecurityStore getSecurityManager()
   {
      return securityStore;
   }

   public DestinationManager getDestinationManager()
   {
      return destinationJNDIMapper;
   }

   public ConnectionFactoryManager getConnectionFactoryManager()
   {
      return connFactoryJNDIMapper;
   }

   public ConnectionManager getConnectionManager()
   {
      return connectionManager;
   }

   public MessageStore getMessageStore()
   {
      return getPostOffice().getMessageStore();
   }

   public MemoryManager getMemoryManager()
   {
      return memoryManager;
   }

   // access to plugin references

   public PersistenceManager getPersistenceManagerInstance()
   {
      return persistenceManager;
   }

   public void setPersistenceManager(PersistenceManager persistenceManager)
   {
      this.persistenceManager = persistenceManager;
   }

   public JMSUserManager getJmsUserManagerInstance()
   {
      return jmsUserManager;
   }


   public void setJmsUserManager(JMSUserManager jmsUserManager)
   {
      this.jmsUserManager = jmsUserManager;
   }

   public PostOffice getPostOffice()
   {
      return postOffice;
   }

   public void setPostOffice(PostOffice postOffice)
   {
      this.postOffice = postOffice;
   }

   public ClusterNotifier getClusterNotifier()
   {
      return clusterNotifier;
   }

   public void setClusterNotifier(ClusterNotifier clusterNotifier)
   {
      this.clusterNotifier = clusterNotifier;
   }

   public boolean isSupportsFailover()
   {
      return supportsFailover;
   }

   public void setSupportsFailover(boolean supportsFailover) throws Exception
   {
      if (started)
      {
         throw new IllegalAccessException("supportsFailover can only be changed when " +
                 "server peer is stopped");
      }
      this.supportsFailover = supportsFailover;
   }

   public String toString()
   {
      return "ServerPeer[" + configuration.getServerPeerID() + "]";
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   public List listAllMessages(String queueName) throws Exception
   {
      return ((ManagedQueue) getDestinationManager().getDestination(queueName, true)).listAllMessages(null);
   }


   public Configuration getConfiguration()
   {
      return configuration;
   }

   public void setConfiguration(Configuration configuration)
   {
      this.configuration = configuration;
   }
   
   public void setMinaService(MinaService minaService)
   {
      this.minaService = minaService;
   }
   
   public MinaService getMinaService()
   {
      return minaService;
   }
}
