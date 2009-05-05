/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.jms.server.management.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.ListenerNotFoundException;
import javax.management.MBeanInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.NotCompliantMBeanException;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationEmitter;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.StandardMBean;

import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.management.impl.MBeanInfoHelper;
import org.jboss.messaging.jms.server.JMSServerManager;
import org.jboss.messaging.jms.server.management.JMSServerControlMBean;
import org.jboss.messaging.utils.Pair;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class JMSServerControl extends StandardMBean implements JMSServerControlMBean, NotificationEmitter
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final JMSServerManager server;

   private final NotificationBroadcasterSupport broadcaster;

   private final AtomicLong notifSeq = new AtomicLong(0);

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public JMSServerControl(final JMSServerManager server) throws NotCompliantMBeanException
   {
      super(JMSServerControlMBean.class);
      this.server = server;
      broadcaster = new NotificationBroadcasterSupport();
   }

   // Public --------------------------------------------------------

   // JMSServerControlMBean implementation --------------------------

   public void createConnectionFactory(final String name,
                                       final String[] liveConnectorsTransportClassNames,
                                       final Map<String, Object>[] liveConnectorTransportParams,
                                       final String[] backupConnectorsTransportClassNames,
                                       final Map<String, Object>[] backupConnectorTransportParams,
                                       final String[] jndiBindings) throws Exception
   {
      List<Pair<TransportConfiguration, TransportConfiguration>> pairs = convertToConnectorPairs(liveConnectorsTransportClassNames,
                                                                                                 liveConnectorTransportParams,
                                                                                                 backupConnectorsTransportClassNames,
                                                                                                 backupConnectorTransportParams);

      List<String> jndiBindingsList = Arrays.asList(jndiBindings);

      server.createConnectionFactory(name, pairs, jndiBindingsList);

      sendNotification(NotificationType.CONNECTION_FACTORY_CREATED, name);
   }

   private List<Pair<TransportConfiguration, TransportConfiguration>> convertToConnectorPairs(final String[] liveConnectorsTransportClassNames,
                                                                                              final Map<String, Object>[] liveConnectorTransportParams,
                                                                                              final String[] backupConnectorsTransportClassNames,
                                                                                              final Map<String, Object>[] backupConnectorTransportParams)
   {
      List<Pair<TransportConfiguration, TransportConfiguration>> pairs = new ArrayList<Pair<TransportConfiguration, TransportConfiguration>>();

      for (int i = 0; i < liveConnectorsTransportClassNames.length; i++)
      {
         TransportConfiguration tcLive = new TransportConfiguration(liveConnectorsTransportClassNames[i],
                                                                    liveConnectorTransportParams[i]);
         TransportConfiguration tcBackup = new TransportConfiguration(liveConnectorsTransportClassNames[i],
                                                                      liveConnectorTransportParams[i]);
         Pair<TransportConfiguration, TransportConfiguration> pair = new Pair<TransportConfiguration, TransportConfiguration>(tcLive,
                                                                                                                              tcBackup);

         pairs.add(pair);
      }

      return pairs;
   }

   public void createConnectionFactory(final String name,
                                       final String[] liveConnectorsTransportClassNames,
                                       final Map<String, Object>[] liveConnectorTransportParams,
                                       final String[] backupConnectorsTransportClassNames,
                                       final Map<String, Object>[] backupConnectorTransportParams,
                                       final String clientID,
                                       final String[] jndiBindings) throws Exception
   {
      List<Pair<TransportConfiguration, TransportConfiguration>> pairs = convertToConnectorPairs(liveConnectorsTransportClassNames,
                                                                                                 liveConnectorTransportParams,
                                                                                                 backupConnectorsTransportClassNames,
                                                                                                 backupConnectorTransportParams);

      List<String> jndiBindingsList = Arrays.asList(jndiBindings);

      server.createConnectionFactory(name, pairs, clientID, jndiBindingsList);

      sendNotification(NotificationType.CONNECTION_FACTORY_CREATED, name);
   }

   public void createConnectionFactory(final String name,
                                       final String[] liveConnectorsTransportClassNames,
                                       final Map<String, Object>[] liveConnectorTransportParams,
                                       final String[] backupConnectorsTransportClassNames,
                                       final Map<String, Object>[] backupConnectorTransportParams,
                                       final String clientID,
                                       final long pingPeriod,
                                       final long connectionTTL,
                                       final long callTimeout,
                                       final int maxConnections,
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
                                       final int reconnectAttempts,
                                       final boolean failoverOnServerShutdown,
                                       final String[] jndiBindings) throws Exception
   {
      List<Pair<TransportConfiguration, TransportConfiguration>> pairs = convertToConnectorPairs(liveConnectorsTransportClassNames,
                                                                                                 liveConnectorTransportParams,
                                                                                                 backupConnectorsTransportClassNames,
                                                                                                 backupConnectorTransportParams);

      List<String> jndiBindingsList = Arrays.asList(jndiBindings);

      server.createConnectionFactory(name,
                                     pairs,
                                     clientID,
                                     pingPeriod,
                                     connectionTTL,
                                     callTimeout,
                                     maxConnections,
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
                                     reconnectAttempts,
                                     failoverOnServerShutdown,
                                     jndiBindingsList);

      sendNotification(NotificationType.CONNECTION_FACTORY_CREATED, name);
   }

   public void createConnectionFactory(final String name,
                                       final String discoveryAddress,
                                       final int discoveryPort,
                                       final String clientID,
                                       final String[] jndiBindings) throws Exception
   {
      List<String> jndiBindingsList = Arrays.asList(jndiBindings);

      server.createConnectionFactory(name, discoveryAddress, discoveryPort, clientID, jndiBindingsList);

      sendNotification(NotificationType.CONNECTION_FACTORY_CREATED, name);
   }

   public void createConnectionFactory(final String name,
                                       final String discoveryAddress,
                                       final int discoveryPort,
                                       final String clientID,
                                       final long discoveryRefreshTimeout,
                                       final long pingPeriod,
                                       final long connectionTTL,
                                       final long callTimeout,
                                       final int maxConnections,
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
                                       final int reconnectAttempts,
                                       final boolean failoverOnServerShutdown,
                                       final String[] jndiBindings) throws Exception
   {
      List<String> jndiBindingsList = Arrays.asList(jndiBindings);

      server.createConnectionFactory(name,
                                     discoveryAddress,
                                     discoveryPort,
                                     clientID,
                                     discoveryRefreshTimeout,
                                     pingPeriod,
                                     connectionTTL,
                                     callTimeout,
                                     maxConnections,
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
                                     reconnectAttempts,
                                     failoverOnServerShutdown,
                                     jndiBindingsList);

      sendNotification(NotificationType.CONNECTION_FACTORY_CREATED, name);
   }

   // FIXME
//   public void createConnectionFactory(final String name,
//                                       final String liveTransportClassName,
//                                       final Map<String, Object> liveTransportParams,
//                                       final String[] jndiBindings) throws Exception
//   {
//      TransportConfiguration liveTC = new TransportConfiguration(liveTransportClassName, liveTransportParams);
//
//      List<String> jndiBindingsList = Arrays.asList(jndiBindings);
//
//      server.createConnectionFactory(name, liveTC, jndiBindingsList);
//
//      sendNotification(NotificationType.CONNECTION_FACTORY_CREATED, name);
//   }

   public void createConnectionFactory(final String name,
                                       final String liveTransportClassName,
                                       final Map<String, Object> liveTransportParams,
                                       final Object[] jndiBindings) throws Exception
   {
      String[] bindings = new String[jndiBindings.length];
      for (int i = 0; i < jndiBindings.length; i++)
      {
         bindings[i] = jndiBindings[i].toString();
         
      }
      
      TransportConfiguration liveTC = new TransportConfiguration(liveTransportClassName, liveTransportParams);

      List<String> jndiBindingsList = Arrays.asList(bindings);

      server.createConnectionFactory(name, liveTC, jndiBindingsList);

      sendNotification(NotificationType.CONNECTION_FACTORY_CREATED, name);
   }
   
   public void createConnectionFactory(final String name,
                                       final String liveTransportClassName,
                                       final Map<String, Object> liveTransportParams,
                                       final String clientID,
                                       final String[] jndiBindings) throws Exception
   {
      TransportConfiguration liveTC = new TransportConfiguration(liveTransportClassName, liveTransportParams);

      List<String> jndiBindingsList = Arrays.asList(jndiBindings);

      server.createConnectionFactory(name, liveTC, clientID, jndiBindingsList);

      sendNotification(NotificationType.CONNECTION_FACTORY_CREATED, name);
   }

   public void createConnectionFactory(final String name,
                                       final String liveTransportClassName,
                                       final Map<String, Object> liveTransportParams,
                                       final String backupTransportClassName,
                                       final Map<String, Object> backupTransportParams,
                                       final String[] jndiBindings) throws Exception
   {
      TransportConfiguration liveTC = new TransportConfiguration(liveTransportClassName, liveTransportParams);

      TransportConfiguration backupTC = new TransportConfiguration(backupTransportClassName, backupTransportParams);

      List<String> jndiBindingsList = Arrays.asList(jndiBindings);

      server.createConnectionFactory(name, liveTC, backupTC, jndiBindingsList);

      sendNotification(NotificationType.CONNECTION_FACTORY_CREATED, name);
   }

   public void createConnectionFactory(final String name,
                                       final String liveTransportClassName,
                                       final Map<String, Object> liveTransportParams,
                                       final String backupTransportClassName,
                                       final Map<String, Object> backupTransportParams,
                                       final String clientID,
                                       final String[] jndiBindings) throws Exception
   {
      TransportConfiguration liveTC = new TransportConfiguration(liveTransportClassName, liveTransportParams);

      TransportConfiguration backupTC = new TransportConfiguration(backupTransportClassName, backupTransportParams);

      List<String> jndiBindingsList = Arrays.asList(jndiBindings);

      server.createConnectionFactory(name, liveTC, backupTC, clientID, jndiBindingsList);

      sendNotification(NotificationType.CONNECTION_FACTORY_CREATED, name);
   }

   public boolean createQueue(final String name, final String jndiBinding) throws Exception
   {
      boolean created = server.createQueue(name, jndiBinding);
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
      NotificationType[] values = NotificationType.values();
      String[] names = new String[values.length];
      for (int i = 0; i < values.length; i++)
      {
         names[i] = values[i].toString();
      }
      return new MBeanNotificationInfo[] { new MBeanNotificationInfo(names,
                                                                     this.getClass().getName(),
                                                                     "Notifications emitted by a JMS Server") };
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

   // StandardMBean overrides
   // ---------------------------------------------------

   /*
    * overrides getMBeanInfo to add operations info using annotations
    * 
    * @see Operation
    * 
    * @see Parameter
    */
   @Override
   public MBeanInfo getMBeanInfo()
   {
      MBeanInfo info = super.getMBeanInfo();
      return new MBeanInfo(info.getClassName(),
                           info.getDescription(),
                           info.getAttributes(),
                           info.getConstructors(),
                           MBeanInfoHelper.getMBeanOperationsInfo(JMSServerControlMBean.class),
                           getNotificationInfo());
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
