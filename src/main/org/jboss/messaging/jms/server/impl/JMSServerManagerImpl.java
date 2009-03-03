/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat
 * Middleware LLC, and individual contributors by the @authors tag. See the
 * copyright.txt in the distribution for a full listing of individual
 * contributors.
 * 
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 * 
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.jms.server.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.NotificationBroadcasterSupport;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.cluster.DiscoveryGroupConfiguration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.MessagingServerControlMBean;
import org.jboss.messaging.core.management.impl.MessagingServerControl;
import org.jboss.messaging.core.messagecounter.impl.MessageCounterManagerImpl;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.JBossTopic;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.jms.server.JMSServerManager;
import org.jboss.messaging.jms.server.management.JMSManagementService;
import org.jboss.messaging.jms.server.management.impl.JMSManagementServiceImpl;
import org.jboss.messaging.utils.Pair;

/**
 * A Deployer used to create and add to JNDI queues, topics and connection
 * factories. Typically this would only be used in an app server env.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class JMSServerManagerImpl implements JMSServerManager
{
   private static final Logger log = Logger.getLogger(JMSServerManagerImpl.class);
   
   private static final String REJECT_FILTER = "__JBMX=-1";

   /**
    * the context to bind to
    */
   private Context context;

   private final Map<String, List<String>> destinations = new HashMap<String, List<String>>();

   private final Map<String, JBossConnectionFactory> connectionFactories = new HashMap<String, JBossConnectionFactory>();

   private final Map<String, List<String>> connectionFactoryBindings = new HashMap<String, List<String>>();

   private final MessagingServerControlMBean messagingServer;

   private final PostOffice postOffice;

   private final StorageManager storageManager;

   private final HierarchicalRepository<AddressSettings> addressSettingsRepository;

   private final JMSManagementService managementService;

   public static JMSServerManagerImpl newJMSServerManagerImpl(final MessagingServer server) throws Exception
   {
      MessagingServerControlMBean control = new MessagingServerControl(server.getPostOffice(),
                                                                       server.getStorageManager(),
                                                                       server.getConfiguration(),                                                                    
                                                                       server.getResourceManager(),
                                                                       server.getRemotingService(),
                                                                       server,
                                                                       new MessageCounterManagerImpl(1000),
                                                                       new NotificationBroadcasterSupport(),
                                                                       server.getQueueFactory());
      JMSManagementService jmsManagementService = new JMSManagementServiceImpl(server.getManagementService());
      return new JMSServerManagerImpl(control,
                                      server.getPostOffice(),
                                      server.getStorageManager(),
                                      server.getAddressSettingsRepository(),
                                      jmsManagementService);
   }

   public JMSServerManagerImpl(final MessagingServerControlMBean server,
                               final PostOffice postOffice,
                               final StorageManager storageManager,
                               final HierarchicalRepository<AddressSettings> addressSettingsRepository,
                               final JMSManagementService managementService)
   {
      messagingServer = server;
      this.postOffice = postOffice;
      this.storageManager = storageManager;
      this.addressSettingsRepository = addressSettingsRepository;
      this.managementService = managementService;
   }

   public void start() throws Exception
   {
      try
      {
         context = new InitialContext();
      }
      catch (NamingException e)
      {
         log.error("Unable to create Initial Context", e);
      }
      managementService.registerJMSServer(this);
   }

   // JMSServerManager implementation -------------------------------

   public boolean isStarted()
   {
      return messagingServer.isStarted();
   }

   public String getVersion()
   {
      return messagingServer.getVersion();
   }

   public synchronized boolean createQueue(final String queueName, final String jndiBinding) throws Exception
   {
      JBossQueue jBossQueue = new JBossQueue(queueName);      
      messagingServer.createQueue(jBossQueue.getAddress(), jBossQueue.getAddress());
      boolean added = bindToJndi(jndiBinding, jBossQueue);
      if (added)
      {
         addToDestinationBindings(queueName, jndiBinding);
      }
      Binding binding = postOffice.getBinding(jBossQueue.getSimpleAddress());
      managementService.registerQueue(jBossQueue,
                                      (Queue)binding.getBindable(),
                                      jndiBinding,
                                      postOffice,
                                      storageManager,
                                      addressSettingsRepository);
      return added;
   }

   public synchronized boolean createTopic(final String topicName, final String jndiBinding) throws Exception
   {
      JBossTopic jBossTopic = new JBossTopic(topicName);
      //We create a dummy subscription on the topic, that never receives messages - this is so we can perform JMS checks when routing messages to a topic that
      //does not exist - otherwise we would not be able to distinguish from a non existent topic and one with no subscriptions - core has no notion of a topic      
      messagingServer.createQueue(jBossTopic.getAddress(), jBossTopic.getAddress(), REJECT_FILTER, true);
      boolean added = bindToJndi(jndiBinding, jBossTopic);
      if (added)
      {
         addToDestinationBindings(topicName, jndiBinding);
      }
      managementService.registerTopic(jBossTopic, jndiBinding, postOffice, storageManager, addressSettingsRepository);
      return added;
   }

   public synchronized boolean undeployDestination(final String name) throws Exception
   {
      List<String> jndiBindings = destinations.get(name);
      if (jndiBindings == null || jndiBindings.size() == 0)
      {
         return false;
      }
      for (String jndiBinding : jndiBindings)
      {
         context.unbind(jndiBinding);
      }
      return true;
   }

   public synchronized boolean destroyQueue(final String name) throws Exception
   {
      undeployDestination(name);

      destinations.remove(name);
      managementService.unregisterQueue(name);
      messagingServer.destroyQueue(JBossQueue.createAddressFromName(name).toString());

      return true;
   }

   public synchronized boolean destroyTopic(final String name) throws Exception
   {
      undeployDestination(name);

      destinations.remove(name);
      managementService.unregisterTopic(name);
      messagingServer.destroyQueue(JBossTopic.createAddressFromName(name).toString());

      return true;
   }

   public synchronized boolean createConnectionFactory(final String name,
                                          final List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs,
                                          final String connectionLoadBalancingPolicyClassName,
                                          final long pingPeriod,
                                          final long connectionTTL,
                                          final long callTimeout,
                                          final String clientID,
                                          final int dupsOKBatchSize,
                                          final int transactionBatchSize,
                                          final int consumerWindowSize,
                                          final int consumerMaxRate,
                                          final int sendWindowSize,
                                          final int producerMaxRate,
                                          final int minLargeMessageSize,
                                          final boolean blockOnAcknowledge,
                                          final boolean blockOnNonPersistentSend,
                                          final boolean blockOnPersistentSend,
                                          final boolean autoGroup,
                                          final int maxConnections,
                                          final boolean preAcknowledge,                                        
                                          final long retryInterval,
                                          final double retryIntervalMultiplier,                                          
                                          final int maxRetriesBeforeFailover,
                                          final int maxRetriesAfterFailover,
                                          final List<String> jndiBindings) throws Exception
   {
      JBossConnectionFactory cf = connectionFactories.get(name);
      if (cf == null)
      {
         cf = new JBossConnectionFactory(connectorConfigs,
                                         connectionLoadBalancingPolicyClassName,
                                         pingPeriod,
                                         connectionTTL,
                                         callTimeout,
                                         clientID,
                                         dupsOKBatchSize,
                                         transactionBatchSize,
                                         consumerWindowSize,
                                         consumerMaxRate,
                                         sendWindowSize,
                                         producerMaxRate,
                                         minLargeMessageSize,
                                         blockOnAcknowledge,
                                         blockOnNonPersistentSend,
                                         blockOnPersistentSend,
                                         autoGroup,
                                         maxConnections,
                                         preAcknowledge,                                     
                                         retryInterval,
                                         retryIntervalMultiplier,                                         
                                         maxRetriesBeforeFailover,
                                         maxRetriesAfterFailover);
      }

      bindConnectionFactory(cf, name, jndiBindings);

      return true;
   }

   public synchronized boolean createConnectionFactory(final String name,
                                          final DiscoveryGroupConfiguration discoveryGroupConfig,
                                          final long discoveryInitialWait,
                                          final String connectionLoadBalancingPolicyClassName,
                                          final long pingPeriod,
                                          final long connectionTTL,
                                          final long callTimeout,
                                          final String clientID,
                                          final int dupsOKBatchSize,
                                          final int transactionBatchSize,
                                          final int consumerWindowSize,
                                          final int consumerMaxRate,
                                          final int sendWindowSize,
                                          final int producerMaxRate,
                                          final int minLargeMessageSize,
                                          final boolean blockOnAcknowledge,
                                          final boolean blockOnNonPersistentSend,
                                          final boolean blockOnPersistentSend,
                                          final boolean autoGroup,
                                          final int maxConnections,
                                          final boolean preAcknowledge,                                 
                                          final long retryInterval,
                                          final double retryIntervalMultiplier,                                          
                                          final int maxRetriesBeforeFailover,
                                          final int maxRetriesAfterFailover,
                                          final List<String> jndiBindings) throws Exception
   {
      JBossConnectionFactory cf = connectionFactories.get(name);
      if (cf == null)
      {
         cf = new JBossConnectionFactory(discoveryGroupConfig.getGroupAddress(),
                                         discoveryGroupConfig.getGroupPort(),
                                         discoveryGroupConfig.getRefreshTimeout(),
                                         discoveryInitialWait,
                                         connectionLoadBalancingPolicyClassName,
                                         pingPeriod,
                                         connectionTTL,
                                         callTimeout,
                                         clientID,
                                         dupsOKBatchSize,
                                         transactionBatchSize,
                                         consumerWindowSize,
                                         consumerMaxRate,
                                         sendWindowSize,
                                         producerMaxRate,
                                         minLargeMessageSize,
                                         blockOnAcknowledge,
                                         blockOnNonPersistentSend,
                                         blockOnPersistentSend,
                                         autoGroup,
                                         maxConnections,
                                         preAcknowledge,                                   
                                         retryInterval,
                                         retryIntervalMultiplier,                                         
                                         maxRetriesBeforeFailover,
                                         maxRetriesAfterFailover);
      }

      bindConnectionFactory(cf, name, jndiBindings);

      return true;
   }

   public synchronized boolean destroyConnectionFactory(final String name) throws Exception
   {
      List<String> jndiBindings = connectionFactoryBindings.get(name);
      if (jndiBindings == null || jndiBindings.size() == 0)
      {
         return false;
      }
      for (String jndiBinding : jndiBindings)
      {
         context.unbind(jndiBinding);
      }
      connectionFactoryBindings.remove(name);
      connectionFactories.remove(name);

      managementService.unregisterConnectionFactory(name);

      return true;
   }

   public String[] listRemoteAddresses()
   {
      return messagingServer.listRemoteAddresses();
   }

   public String[] listRemoteAddresses(final String ipAddress)
   {
      return messagingServer.listRemoteAddresses(ipAddress);
   }

   public boolean closeConnectionsForAddress(final String ipAddress) throws Exception
   {
      return messagingServer.closeConnectionsForAddress(ipAddress);
   }

   public String[] listConnectionIDs()
   {
      return messagingServer.listConnectionIDs();
   }

   public String[] listSessions(final String connectionID)
   {
      return messagingServer.listSessions(connectionID);
   }

   // Public --------------------------------------------------------

   public void setContext(final Context context)
   {
      this.context = context;
   }

   // Private -------------------------------------------------------

   private void bindConnectionFactory(final JBossConnectionFactory cf,
                                      final String name,
                                      final List<String> jndiBindings) throws Exception
   {
      for (String jndiBinding : jndiBindings)
      {
         bindToJndi(jndiBinding, cf);

         if (connectionFactoryBindings.get(name) == null)
         {
            connectionFactoryBindings.put(name, new ArrayList<String>());
         }
         connectionFactoryBindings.get(name).add(jndiBinding);
      }

      managementService.registerConnectionFactory(name, cf, jndiBindings);
   }

   private boolean bindToJndi(final String jndiName, final Object objectToBind) throws NamingException
   {
      String parentContext;
      String jndiNameInContext;
      int sepIndex = jndiName.lastIndexOf('/');
      if (sepIndex == -1)
      {
         parentContext = "";
      }
      else
      {
         parentContext = jndiName.substring(0, sepIndex);
      }
      jndiNameInContext = jndiName.substring(sepIndex + 1);
      try
      {
         context.lookup(jndiName);

         log.warn("Binding for " + jndiName + " already exists");
         return false;
      }
      catch (Throwable e)
      {
         // OK
      }

      Context c = org.jboss.messaging.utils.JNDIUtil.createContext(context, parentContext);

      c.rebind(jndiNameInContext, objectToBind);
      return true;
   }

   private void addToDestinationBindings(final String destination, final String jndiBinding)
   {
      if (destinations.get(destination) == null)
      {
         destinations.put(destination, new ArrayList<String>());
      }
      destinations.get(destination).add(jndiBinding);
   }
}
