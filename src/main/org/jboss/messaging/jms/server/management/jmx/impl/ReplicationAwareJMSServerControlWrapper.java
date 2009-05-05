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

package org.jboss.messaging.jms.server.management.jmx.impl;

import java.util.Map;

import javax.management.MBeanInfo;

import org.jboss.messaging.core.management.ReplicationOperationInvoker;
import org.jboss.messaging.core.management.ResourceNames;
import org.jboss.messaging.core.management.impl.MBeanInfoHelper;
import org.jboss.messaging.core.management.jmx.impl.ReplicationAwareStandardMBeanWrapper;
import org.jboss.messaging.jms.server.management.JMSServerControlMBean;
import org.jboss.messaging.jms.server.management.impl.JMSServerControl;

/**
 * A ReplicationAwareJMSServerControlWrapper
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class ReplicationAwareJMSServerControlWrapper extends ReplicationAwareStandardMBeanWrapper implements
         JMSServerControlMBean
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final JMSServerControl localControl;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReplicationAwareJMSServerControlWrapper(final JMSServerControl localControl,
                                                  final ReplicationOperationInvoker replicationInvoker) throws Exception
   {
      super(ResourceNames.JMS_SERVER, JMSServerControlMBean.class, replicationInvoker);
      this.localControl = localControl;
   }

   // JMSServerControlMBean implementation --------------------------

   public boolean closeConnectionsForAddress(final String ipAddress) throws Exception
   {
      return localControl.closeConnectionsForAddress(ipAddress);
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
      replicationAwareInvoke("createConnectionFactory",
                             discoveryAddress,
                             discoveryPort,
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
                             initialWaitTimeout,
                             useGlobalPools,
                             scheduledThreadPoolMaxSize,
                             threadPoolMaxSize,
                             retryInterval,
                             retryIntervalMultiplier,
                             reconnectAttempts,
                             failoverOnServerShutdown,
                             jndiBindings);

   }

   public void createConnectionFactory(final String name,
                                       final String discoveryAddress,
                                       final int discoveryPort,
                                       final String clientID,
                                       final String[] jndiBindings) throws Exception
   {
      replicationAwareInvoke("createConnectionFactory", discoveryAddress, discoveryPort, clientID, jndiBindings);

   }

   public void createConnectionFactory(final String name,
                                       final String liveTransportClassName,
                                       final Map<String, Object> liveTransportParams,
                                       final String backupTransportClassName,
                                       final Map<String, Object> backupTransportParams,
                                       final String clientID,
                                       final String[] jndiBindings) throws Exception
   {
      replicationAwareInvoke("createConnectionFactory",
                             liveTransportClassName,
                             liveTransportParams,
                             backupTransportClassName,
                             backupTransportParams,
                             clientID,
                             jndiBindings);
   }

   public void createConnectionFactory(final String name,
                                       final String liveTransportClassName,
                                       final Map<String, Object> liveTransportParams,
                                       final String backupTransportClassName,
                                       final Map<String, Object> backupTransportParams,
                                       final String[] jndiBindings) throws Exception
   {
      replicationAwareInvoke("createConnectionFactory",
                             liveTransportClassName,
                             liveTransportParams,
                             backupTransportClassName,
                             backupTransportParams,
                             jndiBindings);
   }

   public void createConnectionFactory(final String name,
                                       final String liveTransportClassName,
                                       final Map<String, Object> liveTransportParams,
                                       final String clientID,
                                       final String[] jndiBindings) throws Exception
   {
      replicationAwareInvoke("createConnectionFactory",
                             liveTransportClassName,
                             liveTransportParams,
                             clientID,
                             jndiBindings);
   }

   public void createConnectionFactory(final String name,
                                       final String liveTransportClassName,
                                       final Map<String, Object> liveTransportParams,
                                       final String[] jndiBindings) throws Exception
   {
      replicationAwareInvoke("createConnectionFactory", liveTransportClassName, liveTransportParams, jndiBindings);
   }

   public void createConnectionFactory(final String name,
                                       final String liveTransportClassName,
                                       final Map<String, Object> liveTransportParams,
                                       final Object[] jndiBindings) throws Exception
   {
      replicationAwareInvoke("createConnectionFactory", liveTransportClassName, liveTransportParams, jndiBindings);
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
      replicationAwareInvoke("createConnectionFactory",
                             liveConnectorsTransportClassNames,
                             liveConnectorTransportParams,
                             backupConnectorsTransportClassNames,
                             backupConnectorTransportParams,
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
                             jndiBindings);
   }

   public void createConnectionFactory(final String name,
                                       final String[] liveConnectorsTransportClassNames,
                                       final Map<String, Object>[] liveConnectorTransportParams,
                                       final String[] backupConnectorsTransportClassNames,
                                       final Map<String, Object>[] backupConnectorTransportParams,
                                       final String clientID,
                                       final String[] jndiBindings) throws Exception
   {
      replicationAwareInvoke("createConnectionFactory",
                             liveConnectorsTransportClassNames,
                             liveConnectorTransportParams,
                             backupConnectorsTransportClassNames,
                             backupConnectorTransportParams,
                             clientID,

                             jndiBindings);
   }

   public void createConnectionFactory(final String name,
                                       final String[] liveConnectorsTransportClassNames,
                                       final Map<String, Object>[] liveConnectorTransportParams,
                                       final String[] backupConnectorsTransportClassNames,
                                       final Map<String, Object>[] backupConnectorTransportParams,
                                       final String[] jndiBindings) throws Exception
   {
      replicationAwareInvoke("createConnectionFactory",
                             liveConnectorsTransportClassNames,
                             liveConnectorTransportParams,
                             backupConnectorsTransportClassNames,
                             backupConnectorTransportParams,

                             jndiBindings);
   }

   public boolean createQueue(final String name, final String jndiBinding) throws Exception
   {
      return (Boolean)replicationAwareInvoke("createQueue", name, jndiBinding);
   }

   public boolean createTopic(final String name, final String jndiBinding) throws Exception
   {
      return (Boolean)replicationAwareInvoke("createTopic", name, jndiBinding);
   }

   public void destroyConnectionFactory(final String name) throws Exception
   {
      replicationAwareInvoke("destroyConnectionFactory", name);
   }

   public boolean destroyQueue(final String name) throws Exception
   {
      return (Boolean)replicationAwareInvoke("destroyQueue", name);
   }

   public boolean destroyTopic(final String name) throws Exception
   {
      return (Boolean)replicationAwareInvoke("destroyTopic", name);
   }

   public String getVersion()
   {
      return localControl.getVersion();
   }

   public boolean isStarted()
   {
      return localControl.isStarted();
   }

   public String[] listConnectionIDs() throws Exception
   {
      return localControl.listConnectionIDs();
   }

   public String[] listRemoteAddresses() throws Exception
   {
      return localControl.listRemoteAddresses();
   }

   public String[] listRemoteAddresses(final String ipAddress) throws Exception
   {
      return localControl.listRemoteAddresses(ipAddress);
   }

   public String[] listSessions(final String connectionID) throws Exception
   {
      return localControl.listSessions(connectionID);
   }

   // StandardMBean overrides ---------------------------------------

   @Override
   public MBeanInfo getMBeanInfo()
   {
      MBeanInfo info = super.getMBeanInfo();
      return new MBeanInfo(info.getClassName(),
                           info.getDescription(),
                           info.getAttributes(),
                           info.getConstructors(),
                           MBeanInfoHelper.getMBeanOperationsInfo(JMSServerControlMBean.class),
                           info.getNotifications());
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
