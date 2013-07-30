/*
 * Copyright 2009 Red Hat, Inc.
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package org.hornetq.jms.server.impl;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.transaction.xa.Xid;

import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.management.AddressControl;
import org.hornetq.api.core.management.ResourceNames;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.deployers.DeploymentManager;
import org.hornetq.core.deployers.impl.FileDeploymentManager;
import org.hornetq.core.deployers.impl.XmlDeployer;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.BindingType;
import org.hornetq.core.registry.JndiBindingRegistry;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.core.security.Role;
import org.hornetq.core.server.ActivateCallback;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.impl.HornetQServerImpl;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.transaction.ResourceManager;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.TransactionDetail;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.client.HornetQDestination;
import org.hornetq.jms.client.HornetQQueue;
import org.hornetq.jms.client.HornetQTopic;
import org.hornetq.jms.client.SelectorTranslator;
import org.hornetq.jms.persistence.JMSStorageManager;
import org.hornetq.jms.persistence.config.PersistedConnectionFactory;
import org.hornetq.jms.persistence.config.PersistedDestination;
import org.hornetq.jms.persistence.config.PersistedJNDI;
import org.hornetq.jms.persistence.config.PersistedType;
import org.hornetq.jms.persistence.impl.journal.JMSJournalStorageManagerImpl;
import org.hornetq.jms.persistence.impl.nullpm.NullJMSStorageManagerImpl;
import org.hornetq.jms.server.JMSServerManager;
import org.hornetq.jms.server.config.ConnectionFactoryConfiguration;
import org.hornetq.jms.server.config.JMSConfiguration;
import org.hornetq.jms.server.config.JMSQueueConfiguration;
import org.hornetq.jms.server.config.TopicConfiguration;
import org.hornetq.jms.server.config.impl.ConnectionFactoryConfigurationImpl;
import org.hornetq.jms.server.management.JMSManagementService;
import org.hornetq.jms.server.management.impl.JMSManagementServiceImpl;
import org.hornetq.jms.transaction.JMSTransactionDetail;
import org.hornetq.spi.core.naming.BindingRegistry;
import org.hornetq.utils.TimeAndCounterIDGenerator;
import org.hornetq.utils.json.JSONArray;
import org.hornetq.utils.json.JSONObject;

/**
 * A Deployer used to create and add to JNDI queues, topics and connection
 * factories. Typically this would only be used in an app server env.
 * <p/>
 * JMS Connection Factories & Destinations can be configured either
 * using configuration files or using a JMSConfiguration object.
 * <p/>
 * If configuration files are used, JMS resources are redeployed if the
 * files content is changed.
 * If a JMSConfiguration object is used, the JMS resources can not be
 * redeployed.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public class JMSServerManagerImpl implements JMSServerManager, ActivateCallback
{
   private static final Logger log = Logger.getLogger(JMSServerManagerImpl.class);

   private static final String REJECT_FILTER = HornetQServerImpl.GENERIC_IGNORED_FILTER;

   private BindingRegistry registry;

   private Map<String, HornetQQueue> queues = new HashMap<String, HornetQQueue>();

   private Map<String, HornetQTopic> topics = new HashMap<String, HornetQTopic>();

   private final Map<String, HornetQConnectionFactory> connectionFactories = new HashMap<String, HornetQConnectionFactory>();

   private final Map<String, List<String>> queueJNDI = new HashMap<String, List<String>>();

   private final Map<String, List<String>> topicJNDI = new HashMap<String, List<String>>();

   private final Map<String, List<String>> connectionFactoryJNDI = new HashMap<String, List<String>>();

   // We keep things cached if objects are created while the JMS is not active
   private final List<Runnable> cachedCommands = new ArrayList<Runnable>();

   private final HornetQServer server;

   private JMSManagementService jmsManagementService;

   private XmlDeployer jmsDeployer;

   private boolean started;

   private boolean active;

   private DeploymentManager deploymentManager;

   private final String configFileName;

   private boolean contextSet;

   private JMSConfiguration config;

   private Configuration coreConfig;

   private JMSStorageManager storage;

   private Map<String, List<String>> unRecoveredJndi = new HashMap<String, List<String>>();

   public JMSServerManagerImpl(final HornetQServer server) throws Exception
   {
      this.server = server;

      this.coreConfig = server.getConfiguration();

      configFileName = null;
   }

   public JMSServerManagerImpl(final HornetQServer server, final String configFileName) throws Exception
   {
      this.server = server;

      this.coreConfig = server.getConfiguration();

      this.configFileName = configFileName;
   }

   public JMSServerManagerImpl(final HornetQServer server, final JMSConfiguration configuration) throws Exception
   {
      this.server = server;

      this.coreConfig = server.getConfiguration();

      configFileName = null;

      config = configuration;
   }

   public JMSServerManagerImpl(HornetQServer server, String configFilename, JMSStorageManager storageManager)
   {
      this.server = server;

      configFileName = null;

      storage = storageManager;
   }

   // ActivateCallback implementation -------------------------------------

   public void preActivate()
   {

   }

   public synchronized void activated()
   {
      if (!started)
      {
         return;
      }

      active = true;

      jmsManagementService = new JMSManagementServiceImpl(server.getManagementService(), server, this);

      try
      {
         jmsManagementService.registerJMSServer(this);

         initJournal();

         // start the JMS deployer only if the configuration is not done using the JMSConfiguration object
         if (config == null)
         {
            if (server.getConfiguration().isFileDeploymentEnabled())
            {
               jmsDeployer = new JMSServerDeployer(this, deploymentManager);

               if (configFileName != null)
               {
                  jmsDeployer.setConfigFileNames(new String[] { configFileName });
               }

               jmsDeployer.start();

               deploymentManager.start();
            }
         }
         else
         {
            deploy();
         }

         for (Runnable run : cachedCommands)
         {
            log.info("Running cached command for " + run);
            run.run();
         }

         // Do not clear cached commands - HORNETQ-1047 

         recoverJndiBindings();
      }
      catch (Exception e)
      {
         JMSServerManagerImpl.log.error("Failed to start jms deployer", e);
      }
   }

   public void recoverJndiBindings(String name, PersistedType type) throws NamingException
   {
      List<String> bindings = unRecoveredJndi.get(name);
      if ((bindings != null) && (bindings.size() > 0))
      {
         Map<String, List<String>> mapJNDI;
         Map<String, ?> objects;

         switch (type)
         {
            case Queue:
               mapJNDI = queueJNDI;
               objects = queues;
               break;
            case Topic:
               mapJNDI = topicJNDI;
               objects = topics;
               break;
            default:
            case ConnectionFactory:
               mapJNDI = connectionFactoryJNDI;
               objects = connectionFactories;
               break;
         }

         Object objectToBind = objects.get(name);

         List<String> jndiList = mapJNDI.get(name);

         if (objectToBind == null)
         {
            return;
         }

         if (jndiList == null)
         {
            jndiList = new ArrayList<String>();
            mapJNDI.put(name, jndiList);
         }

         for (String jndi : bindings)
         {
            jndiList.add(jndi);
            bindToJndi(jndi, objectToBind);
         }

         unRecoveredJndi.remove(name);
      }
   }

   private void recoverJndiBindings() throws Exception
   {
      // now its time to add journal recovered stuff
      List<PersistedJNDI> jndiSpace = storage.recoverPersistedJNDI();

      for (PersistedJNDI record : jndiSpace)
      {
         Map<String, List<String>> mapJNDI;
         Map<String, ?> objects;

         switch (record.getType())
         {
            case Queue:
               mapJNDI = queueJNDI;
               objects = queues;
               break;
            case Topic:
               mapJNDI = topicJNDI;
               objects = topics;
               break;
            default:
            case ConnectionFactory:
               mapJNDI = connectionFactoryJNDI;
               objects = connectionFactories;
               break;
         }

         Object objectToBind = objects.get(record.getName());

         List<String> jndiList = mapJNDI.get(record.getName());

         if (objectToBind == null)
         {
            unRecoveredJndi.put(record.getName(), record.getJndi());
            continue;
         }

         if (jndiList == null)
         {
            jndiList = new ArrayList<String>();
            mapJNDI.put(record.getName(), jndiList);
         }

         for (String jndi : record.getJndi())
         {
            jndiList.add(jndi);
            bindToJndi(jndi, objectToBind);
         }
      }
   }

   public void failback()
   {
      try
      {
         unbindJNDI();
         cleanupAdminObjects();
      }
      catch (Exception e)
      {
         JMSServerManagerImpl.log.error("Failed to clean up jms resources", e);
      }
   }

      // HornetQComponent implementation -----------------------------------

   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }

      if (registry == null)
      {
         if (!contextSet)
         {
            registry = new JndiBindingRegistry(new InitialContext());
         }
      }

      started = true;

      deploymentManager = new FileDeploymentManager(server.getConfiguration().getFileDeployerScanPeriod());

      server.registerActivateCallback(this);

      server.start();

      if (server.getReplicationEndpoint() != null)
      {
         createJournal();
         storage.installReplication(server.getReplicationEndpoint());
      }
   }

   public void stop() throws Exception
   {
      synchronized (this)
      {
         if (!started)
         {
            return;
         }

         if (jmsDeployer != null)
         {
            jmsDeployer.stop();
         }

         if (deploymentManager != null)
         {
            deploymentManager.stop();
         }

         // Storage could be null on a shared store backup server before initialization
         if (storage != null)
         {
            storage.stop();
         }

         unbindJNDI();

         cleanupAdminObjects();

         if (registry != null)
         {
            registry.close();
         }

         // it could be null if a backup
         if (jmsManagementService != null)
         {
            jmsManagementService.unregisterJMSServer();

            jmsManagementService.stop();
         }

         started = false;
      }

      server.stop();
   }

   private void unbindJNDI()
   {
      unbindJNDI(queueJNDI);

      unbindJNDI(topicJNDI);

      unbindJNDI(connectionFactoryJNDI);
   }

   private void cleanupAdminObjects() throws Exception
   {
      for (String connectionFactory : new HashSet<String>(connectionFactories.keySet()))
      {
         shutdownConnectionFactory(connectionFactory);
      }

      connectionFactories.clear();
      connectionFactoryJNDI.clear();

      queueJNDI.clear();
      queues.clear();

      topicJNDI.clear();
      topics.clear();
   }

   public boolean isStarted()
   {
      return server.isStarted();
   }

   // JMSServerManager implementation -------------------------------

   public BindingRegistry getRegistry()
   {
      return registry;
   }

   public void setRegistry(BindingRegistry registry)
   {
      this.registry = registry;
   }

   public HornetQServer getHornetQServer()
   {
      return server;
   }

   public void addAddressSettings(final String address, final AddressSettings addressSettings)
   {
      server.getAddressSettingsRepository().addMatch(address, addressSettings);
   }

   public AddressSettings getAddressSettings(final String address)
   {
      return server.getAddressSettingsRepository().getMatch(address);
   }

   public void addSecurity(final String addressMatch, final Set<Role> roles)
   {
      server.getSecurityRepository().addMatch(addressMatch, roles);
   }

   public Set<Role> getSecurity(final String addressMatch)
   {
      return server.getSecurityRepository().getMatch(addressMatch);
   }

   public synchronized void setContext(final Context context)
   {
      if (registry == null || registry instanceof JndiBindingRegistry)
      {
         registry = new JndiBindingRegistry(context);
         registry.setContext(context);
      }

      contextSet = true;
   }

   public synchronized String getVersion()
   {
      checkInitialised();

      return server.getVersion().getFullVersion();
   }

   public synchronized boolean createQueue(final boolean storeConfig,
                                           final String queueName,
                                           final String selectorString,
                                           final boolean durable,
                                           final String... jndi) throws Exception
   {

      if (active && queues.get(queueName) != null)
      {
         return false;
      }

      runAfterActive(new RunnableException()
      {
         public String toString()
         {
            return "createQueue for " + queueName;
         }

         public void runException() throws Exception
         {
            checkJNDI(jndi);

            if (internalCreateQueue(queueName, selectorString, durable))
            {

               HornetQDestination destination = queues.get(queueName);
               if (destination == null)
               {
                  // sanity check. internalCreateQueue should already have done this check
                  throw new IllegalArgumentException("Queue does not exist");
               }

               ArrayList<String> bindings = new ArrayList<String>();

               for (String jndiItem : jndi)
               {
                  if (bindToJndi(jndiItem, destination))
                  {
                     bindings.add(jndiItem);
                  }
               }

               String[] usedJNDI = bindings.toArray(new String[bindings.size()]);
               addToBindings(queueJNDI, queueName, usedJNDI);

               if (storeConfig && durable)
               {
                  storage.storeDestination(new PersistedDestination(PersistedType.Queue,
                                                                    queueName,
                                                                    selectorString,
                                                                    durable));
                  storage.addJNDI(PersistedType.Queue, queueName, usedJNDI);
               }
            }
         }
      });

      return true;
   }

   public synchronized boolean createTopic(final boolean storeConfig, final String topicName, final String... jndi) throws Exception
   {
      if (active && topics.get(topicName) != null)
      {
         return false;
      }

      runAfterActive(new RunnableException()
      {
         public String toString()
         {
            return "createTopic for " + topicName;
         }

         public void runException() throws Exception
         {
            checkJNDI(jndi);

            if (internalCreateTopic(topicName))
            {
               HornetQDestination destination = topics.get(topicName);

               if (destination == null)
               {
                  // sanity check. internalCreateQueue should already have done this check
                  throw new IllegalArgumentException("Queue does not exist");
               }

               ArrayList<String> bindings = new ArrayList<String>();

               for (String jndiItem : jndi)
               {
                  if (bindToJndi(jndiItem, destination))
                  {
                     bindings.add(jndiItem);
                  }
               }

               String[] usedJNDI = bindings.toArray(new String[bindings.size()]);
               addToBindings(topicJNDI, topicName, usedJNDI);

               if (storeConfig)
               {
                  storage.storeDestination(new PersistedDestination(PersistedType.Topic, topicName));
                  storage.addJNDI(PersistedType.Topic, topicName, usedJNDI);
               }
            }
         }
      });

      return true;

   }

   public boolean addTopicToJndi(final String topicName, final String jndiBinding) throws Exception
   {
      checkInitialised();

      checkJNDI(jndiBinding);

      HornetQTopic destination = topics.get(topicName);
      if (destination == null)
      {
         throw new IllegalArgumentException("Topic does not exist");
      }
      if (destination.getTopicName() == null)
      {
         throw new IllegalArgumentException(topicName + " is not a topic");
      }
      boolean added = bindToJndi(jndiBinding, destination);

      if (added)
      {
         addToBindings(topicJNDI, topicName, jndiBinding);
         storage.addJNDI(PersistedType.Topic, topicName, jndiBinding);
      }
      return added;
   }

   public String[] getJNDIOnQueue(String queue)
   {
      return getJNDIList(queueJNDI, queue);
   }

   public String[] getJNDIOnTopic(String topic)
   {
      return getJNDIList(topicJNDI, topic);
   }

   public String[] getJNDIOnConnectionFactory(String factoryName)
   {
      return getJNDIList(connectionFactoryJNDI, factoryName);
   }

   public boolean addQueueToJndi(final String queueName, final String jndiBinding) throws Exception
   {
      checkInitialised();

      checkJNDI(jndiBinding);

      HornetQQueue destination = queues.get(queueName);
      if (destination == null)
      {
         throw new IllegalArgumentException("Queue does not exist");
      }
      if (destination.getQueueName() == null)
      {
         throw new IllegalArgumentException(queueName + " is not a queue");
      }
      boolean added = bindToJndi(jndiBinding, destination);
      if (added)
      {
         addToBindings(queueJNDI, queueName, jndiBinding);
         storage.addJNDI(PersistedType.Queue, queueName, jndiBinding);
      }
      return added;
   }

   public boolean addConnectionFactoryToJNDI(final String name, final String jndiBinding) throws Exception
   {
      checkInitialised();

      checkJNDI(jndiBinding);

      HornetQConnectionFactory factory = connectionFactories.get(name);
      if (factory == null)
      {
         throw new IllegalArgumentException("Factory does not exist");
      }
      if (registry.lookup(jndiBinding) != null)
      {
         throw new HornetQException(HornetQException.ADDRESS_EXISTS, "JNDI " + name +
                                                                     " is already being used by another connection factory");
      }
      boolean added = bindToJndi(jndiBinding, factory);
      if (added)
      {
         addToBindings(connectionFactoryJNDI, name, jndiBinding);
         storage.addJNDI(PersistedType.ConnectionFactory, name, jndiBinding);
      }
      return added;
   }

   /* (non-Javadoc)
    * @see org.hornetq.jms.server.JMSServerManager#removeQueueFromJNDI(java.lang.String, java.lang.String)
    */

   public boolean removeQueueFromJNDI(String name, String jndi) throws Exception
   {
      checkInitialised();

      boolean removed = removeFromJNDI(queueJNDI, name, jndi);

      if (removed)
      {
         storage.deleteJNDI(PersistedType.Queue, name, jndi);
      }

      return removed;
   }

   /* (non-Javadoc)
    * @see org.hornetq.jms.server.JMSServerManager#removeQueueFromJNDI(java.lang.String, java.lang.String)
    */
   public boolean removeQueueFromJNDI(final String name) throws Exception
   {
      final AtomicBoolean valueReturn = new AtomicBoolean(false);

      // HORNETQ-911 - make this runAfterActive to prevent WARN messages on shutdown/undeployment when the backup was never activated
      runAfterActive(new RunnableException()
      {
         public String toString()
         {
            return "removeQueueFromJNDI for " + name;
         }

         public void runException() throws Exception
         {
            checkInitialised();

            if (removeFromJNDI(queues, queueJNDI, name))
            {
               storage.deleteDestination(PersistedType.Queue, name);
               valueReturn.set(true);
            }
         }
      });

      return valueReturn.get();
   }

   /* (non-Javadoc)
    * @see org.hornetq.jms.server.JMSServerManager#removeTopicFromJNDI(java.lang.String, java.lang.String)
    */

   public boolean removeTopicFromJNDI(String name, String jndi) throws Exception
   {
      checkInitialised();

      if (removeFromJNDI(topicJNDI, name, jndi))
      {
         storage.deleteJNDI(PersistedType.Topic, name, jndi);
         return true;
      }
      else
      {
         return false;
      }
   }

   /* (non-Javadoc)
   * @see org.hornetq.jms.server.JMSServerManager#removeTopicFromJNDI(java.lang.String, java.lang.String)
   */
   public boolean removeTopicFromJNDI(final String name) throws Exception
   {
      final AtomicBoolean valueReturn = new AtomicBoolean(false);

      // HORNETQ-911 - make this runAfterActive to prevent WARN messages on shutdown/undeployment when the backup was never activated
      runAfterActive(new RunnableException()
      {
         public String toString()
         {
            return "removeTopicFromJNDI for " + name;
         }

         public void runException() throws Exception
         {
            checkInitialised();

            if (removeFromJNDI(topics, topicJNDI, name))
            {
               storage.deleteDestination(PersistedType.Topic, name);
               valueReturn.set(true);
            }
         }
      });

      return valueReturn.get();
   }

   /* (non-Javadoc)
    * @see org.hornetq.jms.server.JMSServerManager#removeConnectionFactoryFromJNDI(java.lang.String, java.lang.String)
    */

   public boolean removeConnectionFactoryFromJNDI(String name, String jndi) throws Exception
   {
      checkInitialised();

      removeFromJNDI(connectionFactoryJNDI, name, jndi);

      storage.deleteJNDI(PersistedType.ConnectionFactory, name, jndi);

      return true;
   }

   /* (non-Javadoc)
    * @see org.hornetq.jms.server.JMSServerManager#removeConnectionFactoryFromJNDI(java.lang.String, java.lang.String)
    */

   public boolean removeConnectionFactoryFromJNDI(String name) throws Exception
   {
      checkInitialised();

      removeFromJNDI(connectionFactories, connectionFactoryJNDI, name);

      storage.deleteConnectionFactory(name);

      return true;
   }

   public synchronized boolean destroyQueue(final String name) throws Exception
   {
      checkInitialised();

      removeFromJNDI(queues, queueJNDI, name);

      queues.remove(name);
      queueJNDI.remove(name);

      jmsManagementService.unregisterQueue(name);

      server.destroyQueue(HornetQDestination.createQueueAddressFromName(name), null, true);

      storage.deleteDestination(PersistedType.Queue, name);

      return true;
   }

   public synchronized boolean destroyTopic(final String name) throws Exception
   {
      checkInitialised();

      removeFromJNDI(topics, topicJNDI, name);

      topics.remove(name);
      topicJNDI.remove(name);

      jmsManagementService.unregisterTopic(name);

      AddressControl addressControl = (AddressControl)server.getManagementService()
                                                            .getResource(ResourceNames.CORE_ADDRESS + HornetQDestination.createTopicAddressFromName(name));
      if (addressControl != null)
      {
         for (String queueName : addressControl.getQueueNames())
         {
            Binding binding = server.getPostOffice().getBinding(new SimpleString(queueName));
            if (binding == null)
            {
               log.warn("Queue " + queueName +
                        " doesn't exist on the topic " +
                        name +
                        ". It was deleted manually probably.");
               continue;
            }

            // We can't remove the remote binding. As this would be the bridge associated with the topic on this case
            if (binding.getType() != BindingType.REMOTE_QUEUE)
            {
               server.destroyQueue(SimpleString.toSimpleString(queueName), null, true);
            }
         }
      }
      storage.deleteDestination(PersistedType.Topic, name);
      return true;
   }

   public synchronized void createConnectionFactory(final String name,
                                                    final boolean ha,
                                                    final JMSFactoryType cfType,
                                                    final List<String> connectorNames,
                                                    String... jndiBindings) throws Exception
   {
      checkInitialised();
      HornetQConnectionFactory cf = connectionFactories.get(name);
      if (cf == null)
      {
         ConnectionFactoryConfiguration configuration = new ConnectionFactoryConfigurationImpl(name, ha, connectorNames);
         configuration.setFactoryType(cfType);
         createConnectionFactory(true, configuration, jndiBindings);
      }
   }

   public synchronized void createConnectionFactory(final String name,
                                                    final boolean ha,
                                                    JMSFactoryType cfType,
                                                    final List<String> connectorNames,
                                                    final String clientID,
                                                    final long clientFailureCheckPeriod,
                                                    final long connectionTTL,
                                                    final long callTimeout,
                                                    final boolean cacheLargeMessagesClient,
                                                    final int minLargeMessageSize,
                                                    final boolean compressLargeMessage,
                                                    final int consumerWindowSize,
                                                    final int consumerMaxRate,
                                                    final int confirmationWindowSize,
                                                    final int producerWindowSize,
                                                    final int producerMaxRate,
                                                    final boolean blockOnAcknowledge,
                                                    final boolean blockOnDurableSend,
                                                    final boolean blockOnNonDurableSend,
                                                    final boolean autoGroup,
                                                    final boolean preAcknowledge,
                                                    final String loadBalancingPolicyClassName,
                                                    final int transactionBatchSize,
                                                    final int dupsOKBatchSize,
                                                    final boolean useGlobalPools,
                                                    final int scheduledThreadPoolMaxSize,
                                                    final int threadPoolMaxSize,
                                                    final long retryInterval,
                                                    final double retryIntervalMultiplier,
                                                    final long maxRetryInterval,
                                                    final int reconnectAttempts,
                                                    final boolean failoverOnInitialConnection,
                                                    final String groupId,
                                                    String... jndiBindings) throws Exception
   {
      checkInitialised();
      HornetQConnectionFactory cf = connectionFactories.get(name);
      if (cf == null)
      {
         ConnectionFactoryConfiguration configuration = new ConnectionFactoryConfigurationImpl(name, ha, connectorNames);
         configuration.setClientID(clientID);
         configuration.setClientFailureCheckPeriod(clientFailureCheckPeriod);
         configuration.setConnectionTTL(connectionTTL);
         configuration.setFactoryType(cfType);
         configuration.setCallTimeout(callTimeout);
         configuration.setCacheLargeMessagesClient(cacheLargeMessagesClient);
         configuration.setMinLargeMessageSize(minLargeMessageSize);
         configuration.setCompressLargeMessages(compressLargeMessage);
         configuration.setConsumerWindowSize(consumerWindowSize);
         configuration.setConsumerMaxRate(consumerMaxRate);
         configuration.setConfirmationWindowSize(confirmationWindowSize);
         configuration.setProducerWindowSize(producerWindowSize);
         configuration.setProducerMaxRate(producerMaxRate);
         configuration.setBlockOnAcknowledge(blockOnAcknowledge);
         configuration.setBlockOnDurableSend(blockOnDurableSend);
         configuration.setBlockOnNonDurableSend(blockOnNonDurableSend);
         configuration.setAutoGroup(autoGroup);
         configuration.setPreAcknowledge(preAcknowledge);
         configuration.setLoadBalancingPolicyClassName(loadBalancingPolicyClassName);
         configuration.setTransactionBatchSize(transactionBatchSize);
         configuration.setDupsOKBatchSize(dupsOKBatchSize);
         configuration.setUseGlobalPools(useGlobalPools);
         configuration.setScheduledThreadPoolMaxSize(scheduledThreadPoolMaxSize);
         configuration.setThreadPoolMaxSize(threadPoolMaxSize);
         configuration.setRetryInterval(retryInterval);
         configuration.setRetryIntervalMultiplier(retryIntervalMultiplier);
         configuration.setMaxRetryInterval(maxRetryInterval);
         configuration.setReconnectAttempts(reconnectAttempts);
         configuration.setFailoverOnInitialConnection(failoverOnInitialConnection);
         configuration.setGroupID(groupId);
         createConnectionFactory(true, configuration, jndiBindings);
      }
   }

   public synchronized void createConnectionFactory(final String name,
                                                    final boolean ha,
                                                    final JMSFactoryType cfType,
                                                    final String discoveryGroupName,
                                                    final String clientID,
                                                    final long clientFailureCheckPeriod,
                                                    final long connectionTTL,
                                                    final long callTimeout,
                                                    final boolean cacheLargeMessagesClient,
                                                    final int minLargeMessageSize,
                                                    final boolean compressLargeMessages,
                                                    final int consumerWindowSize,
                                                    final int consumerMaxRate,
                                                    final int confirmationWindowSize,
                                                    final int producerWindowSize,
                                                    final int producerMaxRate,
                                                    final boolean blockOnAcknowledge,
                                                    final boolean blockOnDurableSend,
                                                    final boolean blockOnNonDurableSend,
                                                    final boolean autoGroup,
                                                    final boolean preAcknowledge,
                                                    final String loadBalancingPolicyClassName,
                                                    final int transactionBatchSize,
                                                    final int dupsOKBatchSize,
                                                    final boolean useGlobalPools,
                                                    final int scheduledThreadPoolMaxSize,
                                                    final int threadPoolMaxSize,
                                                    final long retryInterval,
                                                    final double retryIntervalMultiplier,
                                                    final long maxRetryInterval,
                                                    final int reconnectAttempts,
                                                    final boolean failoverOnInitialConnection,
                                                    final String groupId,
                                                    final String... jndiBindings) throws Exception
   {
      checkInitialised();
      HornetQConnectionFactory cf = connectionFactories.get(name);
      if (cf == null)
      {
         ConnectionFactoryConfiguration configuration = new ConnectionFactoryConfigurationImpl(name, ha, jndiBindings);
         configuration.setDiscoveryGroupName(discoveryGroupName);
         configuration.setFactoryType(cfType);
         configuration.setClientID(clientID);
         configuration.setClientFailureCheckPeriod(clientFailureCheckPeriod);
         configuration.setConnectionTTL(connectionTTL);
         configuration.setCallTimeout(callTimeout);
         configuration.setCacheLargeMessagesClient(cacheLargeMessagesClient);
         configuration.setMinLargeMessageSize(minLargeMessageSize);
         configuration.setCompressLargeMessages(compressLargeMessages);
         configuration.setConsumerWindowSize(consumerWindowSize);
         configuration.setConsumerMaxRate(consumerMaxRate);
         configuration.setConfirmationWindowSize(confirmationWindowSize);
         configuration.setProducerWindowSize(producerWindowSize);
         configuration.setProducerMaxRate(producerMaxRate);
         configuration.setBlockOnAcknowledge(blockOnAcknowledge);
         configuration.setBlockOnDurableSend(blockOnDurableSend);
         configuration.setBlockOnNonDurableSend(blockOnNonDurableSend);
         configuration.setAutoGroup(autoGroup);
         configuration.setPreAcknowledge(preAcknowledge);
         configuration.setLoadBalancingPolicyClassName(loadBalancingPolicyClassName);
         configuration.setTransactionBatchSize(transactionBatchSize);
         configuration.setDupsOKBatchSize(dupsOKBatchSize);
         configuration.setUseGlobalPools(useGlobalPools);
         configuration.setScheduledThreadPoolMaxSize(scheduledThreadPoolMaxSize);
         configuration.setThreadPoolMaxSize(threadPoolMaxSize);
         configuration.setRetryInterval(retryInterval);
         configuration.setRetryIntervalMultiplier(retryIntervalMultiplier);
         configuration.setMaxRetryInterval(maxRetryInterval);
         configuration.setReconnectAttempts(reconnectAttempts);
         configuration.setFailoverOnInitialConnection(failoverOnInitialConnection);
         createConnectionFactory(true, configuration, jndiBindings);
      }
   }

   public synchronized void createConnectionFactory(final String name,
                                                    final boolean ha,
                                                    final JMSFactoryType cfType,
                                                    final String discoveryGroupName,
                                                    final String... jndiBindings) throws Exception
   {
      checkInitialised();
      HornetQConnectionFactory cf = connectionFactories.get(name);
      if (cf == null)
      {
         ConnectionFactoryConfiguration configuration = new ConnectionFactoryConfigurationImpl(name, ha, jndiBindings);
         configuration.setDiscoveryGroupName(discoveryGroupName);
         createConnectionFactory(true, configuration, jndiBindings);
      }
   }

   public synchronized HornetQConnectionFactory recreateCF(String name, ConnectionFactoryConfiguration cf) throws Exception
   {
      List<String> jndi = connectionFactoryJNDI.get(name);

      if (jndi == null)
      {
         throw new HornetQException(HornetQException.INTERNAL_ERROR, "Connection Factory " + name + " doesn't exist");
      }

      String[] usedJNDI = jndi.toArray(new String[jndi.size()]);

      HornetQConnectionFactory realCF = internalCreateCFPOJO(cf);

      if (cf.isPersisted())
      {
         storage.storeConnectionFactory(new PersistedConnectionFactory(cf));
         storage.addJNDI(PersistedType.ConnectionFactory, cf.getName(), usedJNDI);
      }

      for (String jndiElement : usedJNDI)
      {
         this.bindToJndi(jndiElement, realCF);
      }

      return realCF;
   }

   public synchronized void createConnectionFactory(final boolean storeConfig,
                                                    final ConnectionFactoryConfiguration cfConfig,
                                                    final String... jndi) throws Exception
   {
      runAfterActive(new RunnableException()
      {

         public String toString()
         {
            return "createConnectionFactory for " + cfConfig.getName();
         }

         public void runException() throws Exception
         {
            checkJNDI(jndi);

            HornetQConnectionFactory cf = internalCreateCF(storeConfig, cfConfig);

            ArrayList<String> bindings = new ArrayList<String>();

            for (String jndiItem : jndi)
            {
               if (bindToJndi(jndiItem, cf))
               {
                  bindings.add(jndiItem);
               }
            }

            String[] usedJNDI = bindings.toArray(new String[bindings.size()]);
            addToBindings(connectionFactoryJNDI, cfConfig.getName(), usedJNDI);

            if (storeConfig)
            {
               storage.storeConnectionFactory(new PersistedConnectionFactory(cfConfig));
               storage.addJNDI(PersistedType.ConnectionFactory, cfConfig.getName(), usedJNDI);
            }

            JMSServerManagerImpl.this.recoverJndiBindings(cfConfig.getName(), PersistedType.ConnectionFactory);
         }
      });
   }

   public JMSStorageManager getJMSStorageManager()
   {
      return storage;
   }

   // used on tests only
   public void replaceStorageManager(JMSStorageManager newStorage)
   {
      this.storage = newStorage;
   }

   private String[] getJNDIList(final Map<String, List<String>> map, final String name)
   {
      List<String> result = map.get(name);
      if (result == null)
      {
         return new String[0];
      }
      else
      {
         String[] strings = new String[result.size()];
         result.toArray(strings);
         return strings;
      }
   }

   private boolean internalCreateQueue(final String queueName, final String selectorString, final boolean durable) throws Exception
   {
      if (queues.get(queueName) != null)
      {
         return false;
      }
      else
      {
         HornetQQueue hqQueue = HornetQDestination.createQueue(queueName);

         // Convert from JMS selector to core filter
         String coreFilterString = null;

         if (selectorString != null)
         {
            coreFilterString = SelectorTranslator.convertToHornetQFilterString(selectorString);
         }

         Queue queue = server.deployQueue(SimpleString.toSimpleString(hqQueue.getAddress()),
                            SimpleString.toSimpleString(hqQueue.getAddress()),
                            SimpleString.toSimpleString(coreFilterString),
                            durable,
                            false);

         queues.put(queueName, hqQueue);

         this.recoverJndiBindings(queueName, PersistedType.Queue);

         jmsManagementService.registerQueue(hqQueue, queue);

         return true;
      }
   }

   /**
    * Performs the internal creation without activating any storage.
    * The storage load will call this method
    *
    * @param topicName
    * @return
    * @throws Exception
    */
   private boolean internalCreateTopic(final String topicName) throws Exception
   {

      if (topics.get(topicName) != null)
      {
         return false;
      }
      else
      {
         HornetQTopic hqTopic = HornetQDestination.createTopic(topicName);
         // We create a dummy subscription on the topic, that never receives messages - this is so we can perform JMS
         // checks when routing messages to a topic that
         // does not exist - otherwise we would not be able to distinguish from a non existent topic and one with no
         // subscriptions - core has no notion of a topic
         server.deployQueue(SimpleString.toSimpleString(hqTopic.getAddress()),
                            SimpleString.toSimpleString(hqTopic.getAddress()),
                            SimpleString.toSimpleString(JMSServerManagerImpl.REJECT_FILTER),
                            true,
                            false);

         topics.put(topicName, hqTopic);

         this.recoverJndiBindings(topicName, PersistedType.Topic);

         jmsManagementService.registerTopic(hqTopic);

         return true;
      }
   }

   /**
    * @param cfConfig
    * @throws HornetQException
    * @throws Exception
    */
   private HornetQConnectionFactory internalCreateCF(final boolean persisted,
                                                     final ConnectionFactoryConfiguration cfConfig) throws HornetQException,
                                                                                                   Exception
   {
      checkInitialised();

      HornetQConnectionFactory cf = connectionFactories.get(cfConfig.getName());

      if (cf == null)
      {
         cf = internalCreateCFPOJO(cfConfig);
      }

      connectionFactories.put(cfConfig.getName(), cf);

      jmsManagementService.registerConnectionFactory(cfConfig.getName(), cfConfig, cf);

      return cf;
   }

   /**
    * @param cfConfig
    * @return
    * @throws HornetQException
    */
   protected HornetQConnectionFactory internalCreateCFPOJO(final ConnectionFactoryConfiguration cfConfig) throws HornetQException
   {
      HornetQConnectionFactory cf;
      if (cfConfig.getDiscoveryGroupName() != null)
      {
         DiscoveryGroupConfiguration groupConfig = server.getConfiguration()
                                                         .getDiscoveryGroupConfigurations()
                                                         .get(cfConfig.getDiscoveryGroupName());

         if (groupConfig == null)
         {
            throw new HornetQException(HornetQException.ILLEGAL_STATE,
                                       "Discovery Group '" + cfConfig.getDiscoveryGroupName() +
                                                "' doesn't exist on maing config");
         }

         if (cfConfig.isHA())
         {
            cf = HornetQJMSClient.createConnectionFactoryWithHA(groupConfig, cfConfig.getFactoryType());
         }
         else
         {
            cf = HornetQJMSClient.createConnectionFactoryWithoutHA(groupConfig, cfConfig.getFactoryType());
         }
      }
      else
      {
         if (cfConfig.getConnectorNames() == null || cfConfig.getConnectorNames().size() == 0)
         {
            throw new HornetQException(HornetQException.ILLEGAL_STATE,
                                       "Null Connector name passed to create ConnectionFactory");
         }

         TransportConfiguration[] configs = new TransportConfiguration[cfConfig.getConnectorNames().size()];

         int count = 0;
         for (String name : cfConfig.getConnectorNames())
         {
            TransportConfiguration connector = server.getConfiguration().getConnectorConfigurations().get(name);
            if (connector == null)
            {
               throw new HornetQException(HornetQException.ILLEGAL_STATE, "Connector '" + name +
                                                                          "' not found on the main configuration file");
            }
            correctInvalidNettyConnectorHost(connector);
            configs[count++] = connector;
         }

         if (cfConfig.isHA())
         {
            cf = HornetQJMSClient.createConnectionFactoryWithHA(cfConfig.getFactoryType(), configs);
         }
         else
         {
            cf = HornetQJMSClient.createConnectionFactoryWithoutHA(cfConfig.getFactoryType(), configs);
         }
      }

      cf.setClientID(cfConfig.getClientID());
      cf.setClientFailureCheckPeriod(cfConfig.getClientFailureCheckPeriod());
      cf.setConnectionTTL(cfConfig.getConnectionTTL());
      cf.setCallTimeout(cfConfig.getCallTimeout());
      cf.setCacheLargeMessagesClient(cfConfig.isCacheLargeMessagesClient());
      cf.setMinLargeMessageSize(cfConfig.getMinLargeMessageSize());
      cf.setConsumerWindowSize(cfConfig.getConsumerWindowSize());
      cf.setConsumerMaxRate(cfConfig.getConsumerMaxRate());
      cf.setConfirmationWindowSize(cfConfig.getConfirmationWindowSize());
      cf.setProducerWindowSize(cfConfig.getProducerWindowSize());
      cf.setProducerMaxRate(cfConfig.getProducerMaxRate());
      cf.setBlockOnAcknowledge(cfConfig.isBlockOnAcknowledge());
      cf.setBlockOnDurableSend(cfConfig.isBlockOnDurableSend());
      cf.setBlockOnNonDurableSend(cfConfig.isBlockOnNonDurableSend());
      cf.setAutoGroup(cfConfig.isAutoGroup());
      cf.setPreAcknowledge(cfConfig.isPreAcknowledge());
      cf.setConnectionLoadBalancingPolicyClassName(cfConfig.getLoadBalancingPolicyClassName());
      cf.setTransactionBatchSize(cfConfig.getTransactionBatchSize());
      cf.setDupsOKBatchSize(cfConfig.getDupsOKBatchSize());
      cf.setUseGlobalPools(cfConfig.isUseGlobalPools());
      cf.setScheduledThreadPoolMaxSize(cfConfig.getScheduledThreadPoolMaxSize());
      cf.setThreadPoolMaxSize(cfConfig.getThreadPoolMaxSize());
      cf.setRetryInterval(cfConfig.getRetryInterval());
      cf.setRetryIntervalMultiplier(cfConfig.getRetryIntervalMultiplier());
      cf.setMaxRetryInterval(cfConfig.getMaxRetryInterval());
      cf.setReconnectAttempts(cfConfig.getReconnectAttempts());
      cf.setFailoverOnInitialConnection(cfConfig.isFailoverOnInitialConnection());
      cf.setCompressLargeMessage(cfConfig.isCompressLargeMessages());
      cf.setGroupID(cfConfig.getGroupID());
      return cf;
   }

   public synchronized boolean destroyConnectionFactory(final String name) throws Exception
   {
      final AtomicBoolean valueReturn = new AtomicBoolean(false);

      // HORNETQ-911 - make this runAfterActive to prevent WARN messages on shutdown/undeployment when the backup was never activated
      runAfterActive(new RunnableException()
      {

         public String toString()
         {
            return "destroyConnectionFactory for " + name;
         }

         public void runException() throws Exception
         {
            shutdownConnectionFactory(name);

            storage.deleteConnectionFactory(name);
            valueReturn.set(true);
         }
      });

      return valueReturn.get();
   }

   /**
    * @param name
    * @throws Exception
    */
   protected boolean shutdownConnectionFactory(final String name) throws Exception
   {
      checkInitialised();
      List<String> jndiBindings = connectionFactoryJNDI.get(name);
      if (jndiBindings == null || jndiBindings.size() == 0)
      {
         return false;
      }

      if (registry != null)
      {
         for (String jndiBinding : jndiBindings)
         {
            registry.unbind(jndiBinding);
         }
      }

      connectionFactoryJNDI.remove(name);
      connectionFactories.remove(name);

      jmsManagementService.unregisterConnectionFactory(name);

      return true;
   }

   public String[] listRemoteAddresses() throws Exception
   {
      checkInitialised();
      return server.getHornetQServerControl().listRemoteAddresses();
   }

   public String[] listRemoteAddresses(final String ipAddress) throws Exception
   {
      checkInitialised();
      return server.getHornetQServerControl().listRemoteAddresses(ipAddress);
   }

   public boolean closeConnectionsForAddress(final String ipAddress) throws Exception
   {
      checkInitialised();
      return server.getHornetQServerControl().closeConnectionsForAddress(ipAddress);
   }

   public String[] listConnectionIDs() throws Exception
   {
      return server.getHornetQServerControl().listConnectionIDs();
   }

   public String[] listSessions(final String connectionID) throws Exception
   {
      checkInitialised();
      return server.getHornetQServerControl().listSessions(connectionID);
   }

   public String listPreparedTransactionDetailsAsJSON() throws Exception
   {
      ResourceManager resourceManager = server.getResourceManager();
      Map<Xid, Long> xids = resourceManager.getPreparedTransactionsWithCreationTime();
      if (xids == null || xids.size() == 0)
      {
         return "";
      }

      ArrayList<Entry<Xid, Long>> xidsSortedByCreationTime = new ArrayList<Map.Entry<Xid, Long>>(xids.entrySet());
      Collections.sort(xidsSortedByCreationTime, new Comparator<Entry<Xid, Long>>()
      {
         public int compare(final Entry<Xid, Long> entry1, final Entry<Xid, Long> entry2)
         {
            // sort by creation time, oldest first
            return (int)(entry1.getValue() - entry2.getValue());
         }
      });

      JSONArray txDetailListJson = new JSONArray();
      for (Map.Entry<Xid, Long> entry : xidsSortedByCreationTime)
      {
         Xid xid = entry.getKey();
         Transaction tx = resourceManager.getTransaction(xid);
         if (tx == null)
         {
            continue;
         }
         TransactionDetail detail = new JMSTransactionDetail(xid, tx, entry.getValue());
         txDetailListJson.put(detail.toJSON());
      }
      return txDetailListJson.toString();
   }

   public String listPreparedTransactionDetailsAsHTML() throws Exception
   {
      ResourceManager resourceManager = server.getResourceManager();
      Map<Xid, Long> xids = resourceManager.getPreparedTransactionsWithCreationTime();
      if (xids == null || xids.size() == 0)
      {
         return "<h3>*** Prepared Transaction Details ***</h3><p>No entry.</p>";
      }

      ArrayList<Entry<Xid, Long>> xidsSortedByCreationTime = new ArrayList<Map.Entry<Xid, Long>>(xids.entrySet());
      Collections.sort(xidsSortedByCreationTime, new Comparator<Entry<Xid, Long>>()
      {
         public int compare(final Entry<Xid, Long> entry1, final Entry<Xid, Long> entry2)
         {
            // sort by creation time, oldest first
            return (int)(entry1.getValue() - entry2.getValue());
         }
      });

      StringBuilder html = new StringBuilder();
      html.append("<h3>*** Prepared Transaction Details ***</h3>");

      for (Map.Entry<Xid, Long> entry : xidsSortedByCreationTime)
      {
         Xid xid = entry.getKey();
         Transaction tx = resourceManager.getTransaction(xid);
         if (tx == null)
         {
            continue;
         }
         TransactionDetail detail = new JMSTransactionDetail(xid, tx, entry.getValue());
         JSONObject txJson = detail.toJSON();

         html.append("<table border=\"1\">");
         html.append("<tr><th>creation_time</th>");
         html.append("<td>" + txJson.get(TransactionDetail.KEY_CREATION_TIME) + "</td>");
         html.append("<th>xid_as_base_64</th>");
         html.append("<td colspan=\"3\">" + txJson.get(TransactionDetail.KEY_XID_AS_BASE64) + "</td></tr>");
         html.append("<tr><th>xid_format_id</th>");
         html.append("<td>" + txJson.get(TransactionDetail.KEY_XID_FORMAT_ID) + "</td>");
         html.append("<th>xid_global_txid</th>");
         html.append("<td>" + txJson.get(TransactionDetail.KEY_XID_GLOBAL_TXID) + "</td>");
         html.append("<th>xid_branch_qual</th>");
         html.append("<td>" + txJson.get(TransactionDetail.KEY_XID_BRANCH_QUAL) + "</td></tr>");

         html.append("<tr><th colspan=\"6\">Message List</th></tr>");
         html.append("<tr><td colspan=\"6\">");
         html.append("<table border=\"1\" cellspacing=\"0\" cellpadding=\"0\">");

         JSONArray msgs = txJson.getJSONArray(TransactionDetail.KEY_TX_RELATED_MESSAGES);
         for (int i = 0; i < msgs.length(); i++)
         {
            JSONObject msgJson = msgs.getJSONObject(i);
            JSONObject props = msgJson.getJSONObject(TransactionDetail.KEY_MSG_PROPERTIES);
            StringBuilder propstr = new StringBuilder();
            @SuppressWarnings("unchecked")
            Iterator<String> propkeys = props.keys();
            while (propkeys.hasNext())
            {
               String key = propkeys.next();
               propstr.append(key);
               propstr.append("=");
               propstr.append(props.get(key));
               propstr.append(", ");
            }

            html.append("<th>operation_type</th>");
            html.append("<td>" + msgJson.get(TransactionDetail.KEY_MSG_OP_TYPE) + "</th>");
            html.append("<th>message_type</th>");
            html.append("<td>" + msgJson.get(TransactionDetail.KEY_MSG_TYPE) + "</td></tr>");
            html.append("<tr><th>properties</th>");
            html.append("<td colspan=\"3\">" + propstr.toString() + "</td></tr>");
         }
         html.append("</table></td></tr>");
         html.append("</table><br/>");
      }

      return html.toString();
   }

   // Public --------------------------------------------------------

   // Private -------------------------------------------------------

   private synchronized void checkInitialised()
   {
      if (!active)
      {
         throw new IllegalStateException("Cannot access JMS Server, core server is not yet active");
      }
   }

   private void addToBindings(Map<String, List<String>> map, String name, String... jndi)
   {
      List<String> list = map.get(name);
      if (list == null)
      {
         list = new ArrayList<String>();
         map.put(name, list);
      }
      for (String jndiItem : jndi)
      {
         list.add(jndiItem);
      }
   }

   private void checkJNDI(final String ... jndiNames) throws NamingException
   {

      for (String jndiName : jndiNames)
      {
         if (registry.lookup(jndiName) != null)
         {
            throw new NamingException(jndiName + " already has an object bound");
         }
      }
   }

   private boolean bindToJndi(final String jndiName, final Object objectToBind) throws NamingException
   {
      if (registry != null)
      {
         registry.unbind(jndiName);
         registry.bind(jndiName, objectToBind);
      }
      return true;
   }

   private void deploy() throws Exception
   {
      if (config == null)
      {
         return;
      }

      if (config.getContext() != null)
      {
         setContext(config.getContext());
      }

      List<ConnectionFactoryConfiguration> connectionFactoryConfigurations = config.getConnectionFactoryConfigurations();
      for (ConnectionFactoryConfiguration config : connectionFactoryConfigurations)
      {
         createConnectionFactory(false, config, config.getBindings());
      }

      List<JMSQueueConfiguration> queueConfigs = config.getQueueConfigurations();
      for (JMSQueueConfiguration config : queueConfigs)
      {
         String[] bindings = config.getBindings();
         createQueue(false, config.getName(), config.getSelector(), config.isDurable(), bindings);
      }

      List<TopicConfiguration> topicConfigs = config.getTopicConfigurations();
      for (TopicConfiguration config : topicConfigs)
      {
         String[] bindings = config.getBindings();
         createTopic(false, config.getName(), bindings);
      }
   }

   /**
    * @param param
    */
   private void unbindJNDI(Map<String, List<String>> param)
   {
      if (registry != null)
      {
         for (List<String> elementList : param.values())
         {
            for (String key : elementList)
            {
               try
               {
                  registry.unbind(key);
               }
               catch (Exception e)
               {
                  log.warn("Impossible to unbind key " + key + " from JNDI", e);
               }
            }
         }
      }
   }

   /**
    * @throws Exception
    */
   private void initJournal() throws Exception
   {
      this.coreConfig = server.getConfiguration();

      createJournal();

      storage.load();

      List<PersistedConnectionFactory> cfs = storage.recoverConnectionFactories();

      for (PersistedConnectionFactory cf : cfs)
      {
         internalCreateCF(true, cf.getConfig());
      }

      List<PersistedDestination> destinations = storage.recoverDestinations();

      for (PersistedDestination destination : destinations)
      {
         if (destination.getType() == PersistedType.Queue)
         {
            internalCreateQueue(destination.getName(), destination.getSelector(), destination.isDurable());
         }
         else if (destination.getType() == PersistedType.Topic)
         {
            internalCreateTopic(destination.getName());
         }
      }
   }

   /**
    * @throws Exception
    */
   private void createJournal() throws Exception
   {
      if (storage == null)
      {
         if (coreConfig.isPersistenceEnabled())
         {
            storage = new JMSJournalStorageManagerImpl(new TimeAndCounterIDGenerator(),
                                                       server.getConfiguration(),
                                                       server.getReplicationManager());
         }
         else
         {
            storage = new NullJMSStorageManagerImpl();
         }
      }
      else
      {
         if (storage.isStarted())
         {
            storage.stop();
         }
      }

      storage.start();
   }

   private synchronized boolean removeFromJNDI(final Map<String, ?> keys,
                                               final Map<String, List<String>> jndiMap,
                                               final String name) throws Exception
   {
      checkInitialised();
      List<String> jndiBindings = jndiMap.remove(name);
      if (jndiBindings == null || jndiBindings.size() == 0)
      {
         return false;
      }
      else
      {
         keys.remove(name);
      }
      if (registry != null)
      {
         Iterator<String> iter = jndiBindings.iterator();
         while (iter.hasNext())
         {
            String jndiBinding = iter.next();
            registry.unbind(jndiBinding);
            iter.remove();
         }
      }
      return true;
   }

   private synchronized boolean removeFromJNDI(final Map<String, List<String>> jndiMap,
                                               final String name,
                                               final String jndi) throws Exception
   {
      checkInitialised();
      List<String> jndiBindings = jndiMap.get(name);
      if (jndiBindings == null || jndiBindings.size() == 0)
      {
         return false;
      }

      if (jndiBindings.remove(jndi))
      {
         registry.unbind(jndi);
         return true;
      }
      else
      {
         return false;
      }
   }

   private boolean runAfterActive(RunnableException runnable) throws Exception
   {
      if (active)
      {
         runnable.runException();
         return true;
      }
      else
      {
         log.info("Caching command for " + runnable + " since the JMS Server is not active yet");
         cachedCommands.add(runnable);
         return false;
      }
   }

   private abstract class RunnableException implements Runnable
   {
      public void run()
      {
         try
         {
            runException();
         }
         catch (Exception e)
         {
            log.warn(e.getMessage(), e);
         }
      }

      public abstract void runException() throws Exception;
   }

   private void correctInvalidNettyConnectorHost(TransportConfiguration transportConfiguration)
   {
      Map<String, Object> params = transportConfiguration.getParams();

      if (transportConfiguration.getFactoryClassName().equals(NettyConnectorFactory.class.getCanonicalName()) &&
            params.containsKey(TransportConstants.HOST_PROP_NAME) &&
            params.get(TransportConstants.HOST_PROP_NAME).equals("0.0.0.0"))
      {
         try
         {
            String newHost = InetAddress.getLocalHost().getHostName();
            log.info("Invalid \"host\" value \"0.0.0.0\" detected for \"" + transportConfiguration.getName() +
                  "\" connector. Switching to \"" + newHost +
                  "\". If this new address is incorrect please manually configure the connector to use the proper one.");
            params.put(TransportConstants.HOST_PROP_NAME, newHost);
         }
         catch (UnknownHostException e)
         {
            log.warn("Tried to correct invalid \"host\" value \"0.0.0.0\" for \"" + transportConfiguration.getName() +
                  "\" connector, but received an exception.", e);
         }
      }
   }

}
