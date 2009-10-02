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

package org.hornetq.jms.server.management.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.ListenerNotFoundException;
import javax.management.MBeanNotificationInfo;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationEmitter;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.StandardMBean;

import org.hornetq.core.client.management.impl.ManagementHelper;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.jms.server.JMSServerManager;
import org.hornetq.jms.server.management.ConnectionFactoryControl;
import org.hornetq.jms.server.management.JMSQueueControl;
import org.hornetq.jms.server.management.JMSServerControl;
import org.hornetq.jms.server.management.TopicControl;
import org.hornetq.utils.Pair;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class JMSServerControlImpl extends StandardMBean implements JMSServerControl, NotificationEmitter
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final JMSServerManager server;

   private final NotificationBroadcasterSupport broadcaster;

   private final AtomicLong notifSeq = new AtomicLong(0);

   // Static --------------------------------------------------------

   private static List<String> convert(final Object[] jndiBindings)
   {
      List<String> bindings = new ArrayList<String>();
      for (Object object : jndiBindings)
      {
         bindings.add(object.toString().trim());
      }
      return bindings;
   }

   private static String[] toArray(String commaSeparatedString)
   {
      if (commaSeparatedString == null || commaSeparatedString.trim().length() == 0)
      {
         return new String[0];
      }
      String[] values = commaSeparatedString.split(",");
      String[] trimmed = new String[values.length];
      for (int i = 0; i < values.length; i++)
      {
         trimmed[i] = values[i].trim();
      }
      return trimmed;
   }

   private static List<Pair<TransportConfiguration, TransportConfiguration>> convertToConnectorPairs(final Object[] liveConnectorsTransportClassNames,
                                                                                                     final Object[] liveConnectorTransportParams,
                                                                                                     final Object[] backupConnectorsTransportClassNames,
                                                                                                     final Object[] backupConnectorTransportParams)
   {
      List<Pair<TransportConfiguration, TransportConfiguration>> pairs = new ArrayList<Pair<TransportConfiguration, TransportConfiguration>>();

      for (int i = 0; i < liveConnectorsTransportClassNames.length; i++)
      {
         Map<String, Object> liveParams = null;
         if (liveConnectorTransportParams.length > i)
         {
            liveParams = (Map<String, Object>)liveConnectorTransportParams[i];
         }

         TransportConfiguration tcLive = new TransportConfiguration(liveConnectorsTransportClassNames[i].toString(),
                                                                    liveParams);

         Map<String, Object> backupParams = null;
         if (backupConnectorTransportParams.length > i)
         {
            backupParams = (Map<String, Object>)backupConnectorTransportParams[i];
         }

         TransportConfiguration tcBackup = null;
         if (backupConnectorsTransportClassNames.length > i)
         {
            new TransportConfiguration(backupConnectorsTransportClassNames[i].toString(), backupParams);
         }
         Pair<TransportConfiguration, TransportConfiguration> pair = new Pair<TransportConfiguration, TransportConfiguration>(tcLive,
                                                                                                                              tcBackup);

         pairs.add(pair);
      }

      return pairs;
   }

   public static MBeanNotificationInfo[] getNotificationInfos()
   {
      NotificationType[] values = NotificationType.values();
      String[] names = new String[values.length];
      for (int i = 0; i < values.length; i++)
      {
         names[i] = values[i].toString();
      }
      return new MBeanNotificationInfo[] { new MBeanNotificationInfo(names,
                                                                     JMSServerControl.class.getName(),
                                                                     "Notifications emitted by a JMS Server") };
   }

   // Constructors --------------------------------------------------

   public JMSServerControlImpl(final JMSServerManager server) throws Exception
   {
      super(JMSServerControl.class);
      this.server = server;
      broadcaster = new NotificationBroadcasterSupport();
   }

   // Public --------------------------------------------------------

   // JMSServerControlMBean implementation --------------------------

   public void createConnectionFactory(final String name,
                                       final Object[] liveConnectorsTransportClassNames,
                                       final Object[] liveConnectorTransportParams,
                                       final Object[] backupConnectorsTransportClassNames,
                                       final Object[] backupConnectorTransportParams,
                                       final Object[] jndiBindings) throws Exception
   {
      List<Pair<TransportConfiguration, TransportConfiguration>> pairs = convertToConnectorPairs(liveConnectorsTransportClassNames,
                                                                                                 liveConnectorTransportParams,
                                                                                                 backupConnectorsTransportClassNames,
                                                                                                 backupConnectorTransportParams);
      List<String> jndiBindingsList = convert(jndiBindings);

      server.createConnectionFactory(name, pairs, jndiBindingsList);

      sendNotification(NotificationType.CONNECTION_FACTORY_CREATED, name);
   }

   public void createConnectionFactory(final String name,
                                       final String liveTransportClassNames,
                                       final String liveTransportParams,
                                       final String backupTransportClassNames,
                                       final String backupTransportParams,
                                       final String jndiBindings) throws Exception
   {
      Object[] liveClassNames = toArray(liveTransportClassNames);
      Object[] liveParams = ManagementHelper.fromCommaSeparatedArrayOfCommaSeparatedKeyValues(liveTransportParams);
      Object[] backupClassNames = toArray(backupTransportClassNames);
      Object[] backupParams = ManagementHelper.fromCommaSeparatedArrayOfCommaSeparatedKeyValues(backupTransportParams);;
      Object[] bindings = toArray(jndiBindings);
      createConnectionFactory(name, liveClassNames, liveParams, backupClassNames, backupParams, bindings);
   }

   public void createConnectionFactory(final String name,
                                       final Object[] liveConnectorsTransportClassNames,
                                       final Object[] liveConnectorTransportParams,
                                       final Object[] backupConnectorsTransportClassNames,
                                       final Object[] backupConnectorTransportParams,
                                       final String clientID,
                                       final Object[] jndiBindings) throws Exception
   {
      List<Pair<TransportConfiguration, TransportConfiguration>> pairs = convertToConnectorPairs(liveConnectorsTransportClassNames,
                                                                                                 liveConnectorTransportParams,
                                                                                                 backupConnectorsTransportClassNames,
                                                                                                 backupConnectorTransportParams);

      List<String> jndiBindingsList = convert(jndiBindings);

      server.createConnectionFactory(name, pairs, clientID, jndiBindingsList);

      sendNotification(NotificationType.CONNECTION_FACTORY_CREATED, name);
   }

   public void createConnectionFactory(String name,
                                       String liveTransportClassNames,
                                       String liveTransportParams,
                                       String backupTransportClassNames,
                                       String backupTransportParams,
                                       String clientID,
                                       String jndiBindings) throws Exception
   {
      Object[] liveClassNames = toArray(liveTransportClassNames);
      Object[] liveParams = ManagementHelper.fromCommaSeparatedArrayOfCommaSeparatedKeyValues(liveTransportParams);
      Object[] backupClassNames = toArray(backupTransportClassNames);
      Object[] backupParams = ManagementHelper.fromCommaSeparatedArrayOfCommaSeparatedKeyValues(backupTransportParams);;
      Object[] bindings = toArray(jndiBindings);

      createConnectionFactory(name, liveClassNames, liveParams, backupClassNames, backupParams, clientID, bindings);
   }

   public void createConnectionFactory(final String name,
                                       final Object[] liveConnectorsTransportClassNames,
                                       final Object[] liveConnectorTransportParams,
                                       final Object[] backupConnectorsTransportClassNames,
                                       final Object[] backupConnectorTransportParams,
                                       final String clientID,
                                       final long clientFailureCheckPeriod,
                                       final long connectionTTL,
                                       final long callTimeout,                                  
                                       final boolean cacheLargeMessageClient,
                                       final int minLargeMessageSize,
                                       final int consumerWindowSize,
                                       final int consumerMaxRate,
                                       final int producerWindowSize,
                                       final int producerMaxRate,
                                       final boolean blockOnAcknowledge,
                                       final boolean blockOnPersistentSend,
                                       final boolean blockOnNonPersistentSend,
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
                                       final boolean failoverOnServerShutdown,
                                       final Object[] jndiBindings) throws Exception
   {
      List<Pair<TransportConfiguration, TransportConfiguration>> pairs = convertToConnectorPairs(liveConnectorsTransportClassNames,
                                                                                                 liveConnectorTransportParams,
                                                                                                 backupConnectorsTransportClassNames,
                                                                                                 backupConnectorTransportParams);

      List<String> jndiBindingsList = convert(jndiBindings);

      server.createConnectionFactory(name,
                                     pairs,
                                     clientID,
                                     clientFailureCheckPeriod,
                                     connectionTTL,
                                     callTimeout,                                    
                                     cacheLargeMessageClient,
                                     minLargeMessageSize,
                                     consumerWindowSize,
                                     consumerMaxRate,
                                     producerWindowSize,
                                     producerMaxRate,
                                     blockOnAcknowledge,
                                     blockOnPersistentSend,
                                     blockOnNonPersistentSend,
                                     autoGroup,
                                     preAcknowledge,
                                     loadBalancingPolicyClassName,
                                     transactionBatchSize,
                                     dupsOKBatchSize,
                                     useGlobalPools,
                                     scheduledThreadPoolMaxSize,
                                     threadPoolMaxSize,
                                     retryInterval,
                                     retryIntervalMultiplier,
                                     maxRetryInterval,
                                     reconnectAttempts,
                                     failoverOnServerShutdown,
                                     jndiBindingsList);

      sendNotification(NotificationType.CONNECTION_FACTORY_CREATED, name);
   }

   public void createConnectionFactory(final String name,
                                       final String liveTransportClassNames,
                                       final String liveTransportParams,
                                       final String backupTransportClassNames,
                                       final String backupTransportParams,
                                       final String clientID,
                                       final long clientFailureCheckPeriod,
                                       final long connectionTTL,
                                       final long callTimeout,                                   
                                       final boolean cacheLargeMessageClient,
                                       final int minLargeMessageSize,
                                       final int consumerWindowSize,
                                       final int consumerMaxRate,
                                       final int producerWindowSize,
                                       final int producerMaxRate,
                                       final boolean blockOnAcknowledge,
                                       final boolean blockOnPersistentSend,
                                       final boolean blockOnNonPersistentSend,
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
                                       final boolean failoverOnServerShutdown,
                                       final String jndiBindings) throws Exception
   {
      Object[] liveClassNames = toArray(liveTransportClassNames);
      Object[] liveParams = ManagementHelper.fromCommaSeparatedArrayOfCommaSeparatedKeyValues(liveTransportParams);
      Object[] backupClassNames = toArray(backupTransportClassNames);
      Object[] backupParams = ManagementHelper.fromCommaSeparatedArrayOfCommaSeparatedKeyValues(backupTransportParams);
      Object[] bindings = toArray(jndiBindings);

      createConnectionFactory(name,
                              liveClassNames,
                              liveParams,
                              backupClassNames,
                              backupParams,
                              clientID,
                              clientFailureCheckPeriod,
                              connectionTTL,
                              callTimeout,                        
                              cacheLargeMessageClient,
                              minLargeMessageSize,
                              consumerWindowSize,
                              consumerMaxRate,
                              producerWindowSize,
                              producerMaxRate,
                              blockOnAcknowledge,
                              blockOnPersistentSend,
                              blockOnNonPersistentSend,
                              autoGroup,
                              preAcknowledge,
                              loadBalancingPolicyClassName,
                              transactionBatchSize,
                              dupsOKBatchSize,
                              useGlobalPools,
                              scheduledThreadPoolMaxSize,
                              threadPoolMaxSize,
                              retryInterval,
                              retryIntervalMultiplier,
                              maxRetryInterval,
                              reconnectAttempts,
                              failoverOnServerShutdown,
                              bindings);
   }

   public void createConnectionFactory(final String name,
                                       final String discoveryAddress,
                                       final int discoveryPort,
                                       final String clientID,
                                       final Object[] jndiBindings) throws Exception
   {
      List<String> jndiBindingsList = convert(jndiBindings);

      server.createConnectionFactory(name, discoveryAddress, discoveryPort, clientID, jndiBindingsList);

      sendNotification(NotificationType.CONNECTION_FACTORY_CREATED, name);
   }

   public void createConnectionFactory(String name,
                                       String discoveryAddress,
                                       int discoveryPort,
                                       String clientID,
                                       String jndiBindings) throws Exception
   {
      Object[] bindings = toArray(jndiBindings);

      createConnectionFactory(name, discoveryAddress, discoveryPort, clientID, bindings);
   }

   public void createConnectionFactory(final String name,
                                       final String discoveryAddress,
                                       final int discoveryPort,
                                       final String clientID,
                                       final long discoveryRefreshTimeout,
                                       final long clientFailureCheckPeriod,
                                       final long connectionTTL,
                                       final long callTimeout,                                   
                                       final boolean cacheLargeMessageClient,
                                       final int minLargeMessageSize,
                                       final int consumerWindowSize,
                                       final int consumerMaxRate,
                                       final int producerWindowSize,
                                       final int producerMaxRate,
                                       final boolean blockOnAcknowledge,
                                       final boolean blockOnPersistentSend,
                                       final boolean blockOnNonPersistentSend,
                                       final boolean autoGroup,
                                       final boolean preAcknowledge,
                                       final String loadBalancingPolicyClassName,
                                       final int transactionBatchSize,
                                       final int dupsOKBatchSize,
                                       final long initialWaitTimeout,
                                       final boolean useGlobalPools,
                                       final int scheduledThreadPoolMaxSize,
                                       final int threadPoolMaxSize,
                                       final long retryInterval,
                                       final double retryIntervalMultiplier,
                                       final long maxRetryInterval,
                                       final int reconnectAttempts,
                                       final boolean failoverOnServerShutdown,
                                       final Object[] jndiBindings) throws Exception
   {
      List<String> jndiBindingsList = convert(jndiBindings);

      server.createConnectionFactory(name,
                                     discoveryAddress,
                                     discoveryPort,
                                     clientID,
                                     discoveryRefreshTimeout,
                                     clientFailureCheckPeriod,
                                     connectionTTL,
                                     callTimeout,                                 
                                     cacheLargeMessageClient,
                                     minLargeMessageSize,
                                     consumerWindowSize,
                                     consumerMaxRate,
                                     producerWindowSize,
                                     producerMaxRate,
                                     blockOnAcknowledge,
                                     blockOnPersistentSend,
                                     blockOnNonPersistentSend,
                                     autoGroup,
                                     preAcknowledge,
                                     loadBalancingPolicyClassName,
                                     transactionBatchSize,
                                     dupsOKBatchSize,
                                     initialWaitTimeout,
                                     useGlobalPools,
                                     scheduledThreadPoolMaxSize,
                                     threadPoolMaxSize,
                                     retryInterval,
                                     retryIntervalMultiplier,
                                     maxRetryInterval,
                                     reconnectAttempts,
                                     failoverOnServerShutdown,
                                     jndiBindingsList);

      sendNotification(NotificationType.CONNECTION_FACTORY_CREATED, name);
   }

   public void createConnectionFactory(final String name,
                                       final String discoveryAddress,
                                       final int discoveryPort,
                                       final String clientID,
                                       final long discoveryRefreshTimeout,
                                       final long clientFailureCheckPeriod,
                                       final long connectionTTL,
                                       final long callTimeout,                                      
                                       final boolean cacheLargeMessageClient,
                                       final int minLargeMessageSize,
                                       final int consumerWindowSize,
                                       final int consumerMaxRate,
                                       final int producerWindowSize,
                                       final int producerMaxRate,
                                       final boolean blockOnAcknowledge,
                                       final boolean blockOnPersistentSend,
                                       final boolean blockOnNonPersistentSend,
                                       final boolean autoGroup,
                                       final boolean preAcknowledge,
                                       final String loadBalancingPolicyClassName,
                                       final int transactionBatchSize,
                                       final int dupsOKBatchSize,
                                       final long initialWaitTimeout,
                                       final boolean useGlobalPools,
                                       final int scheduledThreadPoolMaxSize,
                                       final int threadPoolMaxSize,
                                       final long retryInterval,
                                       final double retryIntervalMultiplier,
                                       final long maxRetryInterval,
                                       final int reconnectAttempts,
                                       final boolean failoverOnServerShutdown,
                                       final String jndiBindings) throws Exception
   {
      Object[] bindings = toArray(jndiBindings);

      createConnectionFactory(name,
                              discoveryAddress,
                              discoveryPort,
                              clientID,
                              discoveryRefreshTimeout,
                              clientFailureCheckPeriod,
                              connectionTTL,
                              callTimeout,                              
                              cacheLargeMessageClient,
                              minLargeMessageSize,
                              consumerWindowSize,
                              consumerMaxRate,
                              producerWindowSize,
                              producerMaxRate,
                              blockOnAcknowledge,
                              blockOnPersistentSend,
                              blockOnNonPersistentSend,
                              autoGroup,
                              preAcknowledge,
                              loadBalancingPolicyClassName,
                              transactionBatchSize,
                              dupsOKBatchSize,
                              initialWaitTimeout,
                              useGlobalPools,
                              scheduledThreadPoolMaxSize,
                              threadPoolMaxSize,
                              retryInterval,
                              retryIntervalMultiplier,
                              maxRetryInterval,
                              reconnectAttempts,
                              failoverOnServerShutdown,
                              bindings);
   }

   public void createConnectionFactory(String name,
                                       String liveTransportClassName,
                                       Map<String, Object> liveTransportParams,
                                       Object[] jndiBindings) throws Exception
   {
      List<String> jndiBindingsList = convert(jndiBindings);
      TransportConfiguration liveTC = new TransportConfiguration(liveTransportClassName, liveTransportParams);

      server.createConnectionFactory(name, liveTC, jndiBindingsList);

      sendNotification(NotificationType.CONNECTION_FACTORY_CREATED, name);
   }

   public void createConnectionFactory(String name,
                                       String liveTransportClassName,
                                       String liveTransportParams,
                                       String jndiBindings) throws Exception
   {
      Map<String, Object> params = ManagementHelper.fromCommaSeparatedKeyValues(liveTransportParams);
      String[] bindings = toArray(jndiBindings);

      createConnectionFactory(name, liveTransportClassName, params, bindings);
   }

   public void createConnectionFactory(final String name,
                                       final String liveTransportClassName,
                                       final Map<String, Object> liveTransportParams,
                                       final String clientID,
                                       final Object[] jndiBindings) throws Exception
   {
      List<String> jndiBindingsList = convert(jndiBindings);

      TransportConfiguration liveTC = new TransportConfiguration(liveTransportClassName, liveTransportParams);

      server.createConnectionFactory(name, liveTC, clientID, jndiBindingsList);

      sendNotification(NotificationType.CONNECTION_FACTORY_CREATED, name);
   }

   public void createConnectionFactory(String name,
                                       String liveTransportClassName,
                                       String liveTransportParams,
                                       String clientID,
                                       String jndiBindings) throws Exception
   {
      Map<String, Object> params = ManagementHelper.fromCommaSeparatedKeyValues(liveTransportParams);
      String[] bindings = toArray(jndiBindings);

      createConnectionFactory(name, liveTransportClassName, params, clientID, bindings);
   }

   public void createConnectionFactory(final String name,
                                       final String liveTransportClassName,
                                       final Map<String, Object> liveTransportParams,
                                       final String backupTransportClassName,
                                       final Map<String, Object> backupTransportParams,
                                       final Object[] jndiBindings) throws Exception
   {
      TransportConfiguration liveTC = new TransportConfiguration(liveTransportClassName, liveTransportParams);

      TransportConfiguration backupTC = new TransportConfiguration(backupTransportClassName, backupTransportParams);

      List<String> jndiBindingsList = convert(jndiBindings);

      server.createConnectionFactory(name, liveTC, backupTC, jndiBindingsList);

      sendNotification(NotificationType.CONNECTION_FACTORY_CREATED, name);
   }

   public void createConnectionFactory(final String name,
                                       final String liveTransportClassName,
                                       final Map<String, Object> liveTransportParams,
                                       final String backupTransportClassName,
                                       final Map<String, Object> backupTransportParams,
                                       final String clientID,
                                       final Object[] jndiBindings) throws Exception
   {
      TransportConfiguration liveTC = new TransportConfiguration(liveTransportClassName, liveTransportParams);

      TransportConfiguration backupTC = new TransportConfiguration(backupTransportClassName, backupTransportParams);

      List<String> jndiBindingsList = convert(jndiBindings);

      server.createConnectionFactory(name, liveTC, backupTC, clientID, jndiBindingsList);

      sendNotification(NotificationType.CONNECTION_FACTORY_CREATED, name);
   }

   public boolean createQueue(final String name, final String jndiBinding) throws Exception
   {
      boolean created = server.createQueue(name, jndiBinding, null, true);
      if (created)
      {
         sendNotification(NotificationType.QUEUE_CREATED, name);
      }
      return created;
   }

   public boolean destroyQueue(final String name) throws Exception
   {
      boolean destroyed = server.destroyQueue(name);
      if (destroyed)
      {
         sendNotification(NotificationType.QUEUE_DESTROYED, name);
      }
      return destroyed;
   }

   public boolean createTopic(final String topicName, final String jndiBinding) throws Exception
   {
      boolean created = server.createTopic(topicName, jndiBinding);
      if (created)
      {
         sendNotification(NotificationType.TOPIC_CREATED, topicName);
      }
      return created;
   }

   public boolean destroyTopic(final String name) throws Exception
   {
      boolean destroyed = server.destroyTopic(name);
      if (destroyed)
      {
         sendNotification(NotificationType.TOPIC_DESTROYED, name);
      }
      return destroyed;
   }

   public void destroyConnectionFactory(final String name) throws Exception
   {
      boolean destroyed = server.destroyConnectionFactory(name);
      if (destroyed)
      {
         sendNotification(NotificationType.CONNECTION_FACTORY_DESTROYED, name);
      }
   }

   public boolean isStarted()
   {
      return server.isStarted();
   }

   public String getVersion()
   {
      return server.getVersion();
   }

   public String[] getQueueNames()
   {
      Object[] queueControls = server.getHornetQServer().getManagementService().getResources(JMSQueueControl.class);
      String[] names = new String[queueControls.length];
      for (int i = 0; i < queueControls.length; i++)
      {
         JMSQueueControl queueControl = (JMSQueueControl)queueControls[i];
         names[i] = queueControl.getName();
      }
      return names;
   }
   
   public String[] getTopicNames()
   {
      Object[] topicControls = server.getHornetQServer().getManagementService().getResources(TopicControl.class);
      String[] names = new String[topicControls.length];
      for (int i = 0; i < topicControls.length; i++)
      {
         TopicControl topicControl = (TopicControl)topicControls[i];
         names[i] = topicControl.getName();
      }
      return names;
   }
   
   public String[] getConnectionFactoryNames()
   {
      Object[] cfControls = server.getHornetQServer().getManagementService().getResources(ConnectionFactoryControl.class);
      String[] names = new String[cfControls.length];
      for (int i = 0; i < cfControls.length; i++)
      {
         ConnectionFactoryControl cfControl = (ConnectionFactoryControl)cfControls[i];
         names[i] = cfControl.getName();
      }
      return names;
   }
   
   // NotificationEmitter implementation ----------------------------

   public void removeNotificationListener(final NotificationListener listener,
                                          final NotificationFilter filter,
                                          final Object handback) throws ListenerNotFoundException
   {
      broadcaster.removeNotificationListener(listener, filter, handback);
   }

   public void removeNotificationListener(final NotificationListener listener) throws ListenerNotFoundException
   {
      broadcaster.removeNotificationListener(listener);
   }

   public void addNotificationListener(final NotificationListener listener,
                                       final NotificationFilter filter,
                                       final Object handback) throws IllegalArgumentException
   {
      broadcaster.addNotificationListener(listener, filter, handback);
   }

   public MBeanNotificationInfo[] getNotificationInfo()
   {
      return getNotificationInfos();
   }

   public String[] listRemoteAddresses() throws Exception
   {
      return server.listRemoteAddresses();
   }

   public String[] listRemoteAddresses(final String ipAddress) throws Exception
   {
      return server.listRemoteAddresses(ipAddress);
   }

   public boolean closeConnectionsForAddress(final String ipAddress) throws Exception
   {
      return server.closeConnectionsForAddress(ipAddress);
   }

   public String[] listConnectionIDs() throws Exception
   {
      return server.listConnectionIDs();
   }

   public String[] listSessions(final String connectionID) throws Exception
   {
      return server.listSessions(connectionID);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void sendNotification(final NotificationType type, final String message)
   {
      Notification notif = new Notification(type.toString(), this, notifSeq.incrementAndGet(), message);
      broadcaster.sendNotification(notif);
   }

   // Inner classes -------------------------------------------------

   public static enum NotificationType
   {
      QUEUE_CREATED,
      QUEUE_DESTROYED,
      TOPIC_CREATED,
      TOPIC_DESTROYED,
      CONNECTION_FACTORY_CREATED,
      CONNECTION_FACTORY_DESTROYED;
   }

}
