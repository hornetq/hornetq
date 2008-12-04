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
import java.util.List;
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
import org.jboss.messaging.core.config.cluster.DiscoveryGroupConfiguration;
import org.jboss.messaging.core.management.impl.MBeanInfoHelper;
import org.jboss.messaging.jms.server.JMSServerManager;
import org.jboss.messaging.jms.server.management.JMSServerControlMBean;
import org.jboss.messaging.util.Pair;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class JMSServerControl extends StandardMBean implements JMSServerControlMBean, NotificationEmitter
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final JMSServerManager server;

   private NotificationBroadcasterSupport broadcaster;

   private AtomicLong notifSeq = new AtomicLong(0);

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

   public void createConnectionFactory(String name,
                                       List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs,
                                       String connectionLoadBalancingPolicyClassName,
                                       long pingPeriod,   
                                       long connectionTTL,
                                       long callTimeout,
                                       String clientID,
                                       int dupsOKBatchSize,
                                       int transactionBatchSize,
                                       int consumerWindowSize,
                                       int consumerMaxRate,
                                       int producerWindowSize,
                                       int producerMaxRate,
                                       int minLargeMessageSize, 
                                       boolean blockOnAcknowledge,
                                       boolean blockOnNonPersistentSend,
                                       boolean blockOnPersistentSend,
                                       boolean autoGroup,
                                       int maxConnections,
                                       boolean preAcknowledge,                                   
                                       long retryInterval,
                                       double retryIntervalMultiplier,                                       
                                       int maxRetriesBeforeFailover,
                                       int maxRetriesAfterFailover,
                                       String jndiBinding) throws Exception
   {
      List<String> bindings = new ArrayList<String>();
      bindings.add(jndiBinding);

      boolean created = server.createConnectionFactory(name,
                                                       connectorConfigs,
                                                       connectionLoadBalancingPolicyClassName,
                                                       pingPeriod,                
                                                       connectionTTL,
                                                       callTimeout,
                                                       clientID,
                                                       dupsOKBatchSize,
                                                       transactionBatchSize,
                                                       consumerWindowSize,
                                                       consumerMaxRate,
                                                       producerWindowSize,
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
                                                       maxRetriesAfterFailover,
                                                       bindings);
      if (created)
      {
         sendNotification(NotificationType.CONNECTION_FACTORY_CREATED, name);
      }
   }
   
   public void createConnectionFactory(String name,
                                       DiscoveryGroupConfiguration discoveryGroupConfig,
                                       long discoveryInitialWait,
                                       String connectionLoadBalancingPolicyClassName,
                                       long pingPeriod,                        
                                       long connectionTTL,
                                       long callTimeout,
                                       String clientID,
                                       int dupsOKBatchSize,
                                       int transactionBatchSize,
                                       int consumerWindowSize,
                                       int consumerMaxRate,
                                       int producerWindowSize,
                                       int producerMaxRate,
                                       int minLargeMessageSize, 
                                       boolean blockOnAcknowledge,
                                       boolean blockOnNonPersistentSend,
                                       boolean blockOnPersistentSend,
                                       boolean autoGroup,
                                       int maxConnections,
                                       boolean preAcknowledge,                              
                                       final long retryInterval,
                                       final double retryIntervalMultiplier,                                       
                                       final int maxRetriesBeforeFailover,
                                       final int maxRetriesAfterFailover,
                                       String jndiBinding) throws Exception
   {
      List<String> bindings = new ArrayList<String>();
      bindings.add(jndiBinding);

      boolean created = server.createConnectionFactory(name,
                                                       discoveryGroupConfig,
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
                                                       producerWindowSize,
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
                                                       maxRetriesAfterFailover,
                                                       bindings);
      if (created)
      {
         sendNotification(NotificationType.CONNECTION_FACTORY_CREATED, name);
      }
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
   
   public String[] listRemoteAddresses()
   {
      return server.listRemoteAddresses();
   }

   public String[] listRemoteAddresses(final String ipAddress)
   {
      return server.listRemoteAddresses(ipAddress);
   }

   public boolean closeConnectionsForAddress(final String ipAddress) throws Exception
   {
      return server.closeConnectionsForAddress(ipAddress);
   }

   public String[] listConnectionIDs()
   {
      return server.listConnectionIDs();
   }

   public String[] listSessions(final String connectionID)
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
