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

import java.util.List;

import javax.management.MBeanInfo;
import javax.management.ObjectName;

import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.management.ReplicationOperationInvoker;
import org.jboss.messaging.core.management.impl.MBeanInfoHelper;
import org.jboss.messaging.core.management.jmx.impl.ReplicationAwareStandardMBeanWrapper;
import org.jboss.messaging.jms.server.management.JMSServerControlMBean;
import org.jboss.messaging.jms.server.management.impl.JMSServerControl;
import org.jboss.messaging.utils.Pair;

/**
 * A ReplicationAwareJMSServerControlWrapper
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ReplicationAwareJMSServerControlWrapper extends ReplicationAwareStandardMBeanWrapper implements
         JMSServerControlMBean
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final JMSServerControl localControl;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReplicationAwareJMSServerControlWrapper(final ObjectName objectName,
                                                  final JMSServerControl localControl,
                                                  final ReplicationOperationInvoker replicationInvoker) throws Exception
   {
      super(objectName, JMSServerControlMBean.class, replicationInvoker);
      this.localControl = localControl;
   }

   // JMSServerControlMBean implementation --------------------------

   public boolean closeConnectionsForAddress(final String ipAddress) throws Exception
   {
      return localControl.closeConnectionsForAddress(ipAddress);
   }

   public void createConnectionFactory(final String name,
                                       final String connectorFactoryClassName,
                                       final String jndiBinding) throws Exception
   {
      replicationAwareInvoke("createConnectionFactory", name, connectorFactoryClassName, jndiBinding);
   }

   public void createConnectionFactory(final String name,
                                       final String connectorFactoryClassName,
                                       final boolean blockOnAcknowledge,
                                       final boolean blockOnNonPersistentSend,
                                       final boolean blockOnPersistentSend,
                                       final boolean preAcknowledge,
                                       final String jndiBinding) throws Exception
   {
      replicationAwareInvoke("createConnectionFactory",
                             name,
                             connectorFactoryClassName,
                             blockOnAcknowledge,
                             blockOnNonPersistentSend,
                             blockOnPersistentSend,
                             preAcknowledge,
                             jndiBinding);
   }

   public void createSimpleConnectionFactory(final String name,
                                       final String connectorFactoryClassName,
                                       final String connectionLoadBalancingPolicyClassName,
                                       final long pingPeriod,
                                       final long connectionTTL,
                                       final long callTimeout,
                                       final String clientID,
                                       final int dupsOKBatchSize,
                                       final int transactionBatchSize,
                                       final int consumerWindowSize,
                                       final int consumerMaxRate,
                                       final int producerWindowSize,
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
                                       final String jndiBinding) throws Exception
   {
      replicationAwareInvoke("createSimpleConnectionFactory",
                             name,
                             connectorFactoryClassName,
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
                             jndiBinding);
   }

   public void createConnectionFactory(final String name,
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
                                       final int producerWindowSize,
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
                                       final String jndiBinding) throws Exception
   {
      // FIXME need to store correctly the connector configs
      replicationAwareInvoke("createConnectionFactory",
                             name,
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
                             jndiBinding);
   }

   public void createConnectionFactory(final String name,
                                       final String discoveryGroupName,
                                       final String discoveryGroupAddress,
                                       final int discoveryGroupPort,
                                       final long discoveryGroupRefreshTimeout,
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
                                       final int producerWindowSize,
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
                                       final String jndiBinding) throws Exception
   {
      replicationAwareInvoke("createConnectionFactory",
                             name,
                             discoveryGroupName,
                             discoveryGroupAddress,
                             discoveryGroupPort,
                             discoveryGroupRefreshTimeout,
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
                             jndiBinding);
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

   public String[] listConnectionIDs()
   {
      return localControl.listConnectionIDs();
   }

   public String[] listRemoteAddresses()
   {
      return localControl.listRemoteAddresses();
   }

   public String[] listRemoteAddresses(final String ipAddress)
   {
      return localControl.listRemoteAddresses(ipAddress);
   }

   public String[] listSessions(final String connectionID)
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
