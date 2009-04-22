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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;
import javax.naming.NamingException;

import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.cluster.DiscoveryGroupConfiguration;
import org.jboss.messaging.core.deployers.Deployer;
import org.jboss.messaging.core.deployers.DeploymentManager;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.server.MessagingServer;
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
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
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

   private final MessagingServer server;

   private final JMSManagementService managementService;

   private Deployer jmsDeployer;

   private boolean started;

   public JMSServerManagerImpl(final MessagingServer server)
   {
      this.server = server;

      managementService = new JMSManagementServiceImpl(server.getManagementService());          
   }

   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }

      try
      {
         context = new InitialContext();
      }
      catch (NamingException e)
      {
         log.error("Unable to create Initial Context", e);
      }
      managementService.registerJMSServer(this);
      
      // The deployment manager is started in the core server - this is necessary since
      // we can't start any deployments until backup server is initialised
      DeploymentManager deploymentManager = server.getDeploymentManager();
      
      if (deploymentManager != null)
      {      
         jmsDeployer = new JMSServerDeployer(this, deploymentManager, server.getConfiguration());     
      
         jmsDeployer.start();
      }
      started = true;
   }

   // MessagingComponent implementation -----------------------------------

   public synchronized void stop() throws Exception
   {
      if (!started)
      {
         return;
      }
      if (jmsDeployer != null)
      {
         jmsDeployer.stop();
      }
      for (String destination : destinations.keySet())
      {
         undeployDestination(destination);
      }
      for (String connectionFactory : new HashSet<String>(connectionFactories.keySet()))
      {
         destroyConnectionFactory(connectionFactory);
      }
      destinations.clear();
      connectionFactories.clear();
      connectionFactoryBindings.clear();
      context.close();
      started = false;
   }

   public boolean isStarted()
   {
      return server.getMessagingServerControl().isStarted();
   }

   // JMSServerManager implementation -------------------------------

   public void setContext(final Context context)
   {
      this.context = context;
   }

   public String getVersion()
   {
      checkInitialised();
      return server.getMessagingServerControl().getVersion();
   }

   public synchronized boolean createQueue(final String queueName, final String jndiBinding) throws Exception
   {
      checkInitialised();
      JBossQueue jBossQueue = new JBossQueue(queueName);      
      server.getMessagingServerControl().createQueue(jBossQueue.getAddress(), jBossQueue.getAddress());
      boolean added = bindToJndi(jndiBinding, jBossQueue);
      if (added)
      {
         addToDestinationBindings(queueName, jndiBinding);
      }
      managementService.registerQueue(jBossQueue,
                                      jndiBinding);
      return added;
   }

   public synchronized boolean createTopic(final String topicName, final String jndiBinding) throws Exception
   {
      checkInitialised();
      JBossTopic jBossTopic = new JBossTopic(topicName);
      //We create a dummy subscription on the topic, that never receives messages - this is so we can perform JMS checks when routing messages to a topic that
      //does not exist - otherwise we would not be able to distinguish from a non existent topic and one with no subscriptions - core has no notion of a topic      
      server.getMessagingServerControl().createQueue(jBossTopic.getAddress(), jBossTopic.getAddress(), REJECT_FILTER, true);
      boolean added = bindToJndi(jndiBinding, jBossTopic);
      if (added)
      {
         addToDestinationBindings(topicName, jndiBinding);
      }
      managementService.registerTopic(jBossTopic, jndiBinding);
      return added;
   }

   public synchronized boolean undeployDestination(final String name) throws Exception
   {
      checkInitialised();
      List<String> jndiBindings = destinations.get(name);
      if (jndiBindings == null || jndiBindings.size() == 0)
      {
         return false;
      }
      Iterator<String> iter = jndiBindings.iterator();
      while (iter.hasNext())
      {
         String jndiBinding = (String)iter.next();
         context.unbind(jndiBinding);
         iter.remove();
      }
      return true;
   }

   public synchronized boolean destroyQueue(final String name) throws Exception
   {
      checkInitialised();
      undeployDestination(name);

      destinations.remove(name);
      managementService.unregisterQueue(name);
      server.getMessagingServerControl().destroyQueue(JBossQueue.createAddressFromName(name).toString());

      return true;
   }

   public synchronized boolean destroyTopic(final String name) throws Exception
   {
      checkInitialised();
      undeployDestination(name);

      destinations.remove(name);
      managementService.unregisterTopic(name);
      server.getMessagingServerControl().destroyQueue(JBossTopic.createAddressFromName(name).toString());

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
                                                       final int reconnectAttempts,
                                                       final boolean failoverOnNodeShutdown,
                                                       final List<String> jndiBindings) throws Exception
   {
      checkInitialised();
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
                                         reconnectAttempts,
                                         failoverOnNodeShutdown);
      }

      bindConnectionFactory(cf, name, jndiBindings);

      return true;
   }

   public synchronized boolean createConnectionFactory(final String name,
                                                       final List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs,
                                                       final List<String> jndiBindings) throws Exception
   {
      checkInitialised();
      JBossConnectionFactory cf = connectionFactories.get(name);
      if (cf == null)
      {
         cf = new JBossConnectionFactory(connectorConfigs);
      }

      bindConnectionFactory(cf, name, jndiBindings);

      return true;
   }

   public synchronized boolean createConnectionFactory(String name,
                                                       List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs,
                                                       boolean blockOnAcknowledge,
                                                       boolean blockOnNonPersistentSend,
                                                       boolean blockOnPersistentSend,
                                                       boolean preAcknowledge,
                                                       List<String> jndiBindings) throws Exception
   {
      checkInitialised();
      JBossConnectionFactory cf = connectionFactories.get(name);
      if (cf == null)
      {
         cf = new JBossConnectionFactory(connectorConfigs,
                                         blockOnAcknowledge,
                                         blockOnNonPersistentSend,
                                         blockOnPersistentSend,
                                         preAcknowledge);
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
                                                       final int reconnectAttempts,
                                                       final boolean failoverOnNodeShutdown,
                                                       final List<String> jndiBindings) throws Exception
   {
      checkInitialised();
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
                                         reconnectAttempts,
                                         failoverOnNodeShutdown);
      }

      bindConnectionFactory(cf, name, jndiBindings);

      return true;
   }

   public synchronized boolean destroyConnectionFactory(final String name) throws Exception
   {
      checkInitialised();
      List<String> jndiBindings = connectionFactoryBindings.get(name);
      if (jndiBindings == null || jndiBindings.size() == 0)
      {
         return false;
      }
      for (String jndiBinding : jndiBindings)
      {
         try
         {
            context.unbind(jndiBinding);
         }
         catch (NameNotFoundException e)
         {
            // this is ok.
         }
      }
      connectionFactoryBindings.remove(name);
      connectionFactories.remove(name);

      managementService.unregisterConnectionFactory(name);

      return true;
   }

   public String[] listRemoteAddresses() throws Exception
   {
      checkInitialised();
      return server.getMessagingServerControl().listRemoteAddresses();
   }

   public String[] listRemoteAddresses(final String ipAddress) throws Exception
   {
      checkInitialised();
      return server.getMessagingServerControl().listRemoteAddresses(ipAddress);
   }

   public boolean closeConnectionsForAddress(final String ipAddress) throws Exception
   {
      checkInitialised();
      return server.getMessagingServerControl().closeConnectionsForAddress(ipAddress);
   }

   public String[] listConnectionIDs() throws Exception
   {
      return server.getMessagingServerControl().listConnectionIDs();
   }

   public String[] listSessions(final String connectionID) throws Exception
   {
      checkInitialised();
      return server.getMessagingServerControl().listSessions(connectionID);
   }

   // Public --------------------------------------------------------

   // Private -------------------------------------------------------

   private void checkInitialised()
   {
      if (!server.isInitialised())
      {
         throw new IllegalStateException("Cannot access JMS Server, core server is not yet initialised");
      }
   }
   
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
