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

import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.management.ReplicationOperationInvoker;
import org.jboss.messaging.core.management.ResourceNames;
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

   public void createConnectionFactory(String name,
                                       List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs,
                                       List<String> jndiBindings) throws Exception
   {
      replicationAwareInvoke("createConnectionFactory", connectorConfigs, jndiBindings);
   }

   public void createConnectionFactory(String name,
                                       List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs,
                                       String clientID,
                                       List<String> jndiBindings) throws Exception
   {
      replicationAwareInvoke("createConnectionFactory", connectorConfigs, clientID, jndiBindings);
   }

   public void createConnectionFactory(String name,
                                       List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs,
                                       String clientID,
                                       long pingPeriod,
                                       long connectionTTL,
                                       long callTimeout,
                                       int maxConnections,
                                       int minLargeMessageSize,
                                       int consumerWindowSize,
                                       int consumerMaxRate,
                                       int producerWindowSize,
                                       int producerMaxRate,
                                       boolean blockOnAcknowledge,
                                       boolean blockOnPersistentSend,
                                       boolean blockOnNonPersistentSend,
                                       boolean autoGroup,
                                       boolean preAcknowledge,
                                       String loadBalancingPolicyClassName,
                                       int transactionBatchSize,
                                       int dupsOKBatchSize,
                                       boolean useGlobalPools,
                                       int scheduledThreadPoolMaxSize,
                                       int threadPoolMaxSize,
                                       long retryInterval,
                                       double retryIntervalMultiplier,
                                       int reconnectAttempts,
                                       boolean failoverOnServerShutdown,
                                       List<String> jndiBindings) throws Exception
   {
      replicationAwareInvoke("createConnectionFactory",
                             connectorConfigs,
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

   public void createConnectionFactory(String name,
                                       String discoveryAddress,
                                       int discoveryPort,
                                       String clientID,
                                       List<String> jndiBindings) throws Exception
   {
      replicationAwareInvoke("createConnectionFactory", discoveryAddress, discoveryPort, clientID, jndiBindings);
   }

   public void createConnectionFactory(String name,
                                       String discoveryAddress,
                                       int discoveryPort,
                                       String clientID,
                                       long discoveryRefreshTimeout,
                                       long pingPeriod,
                                       long connectionTTL,
                                       long callTimeout,
                                       int maxConnections,
                                       int minLargeMessageSize,
                                       int consumerWindowSize,
                                       int consumerMaxRate,
                                       int producerWindowSize,
                                       int producerMaxRate,
                                       boolean blockOnAcknowledge,
                                       boolean blockOnPersistentSend,
                                       boolean blockOnNonPersistentSend,
                                       boolean autoGroup,
                                       boolean preAcknowledge,
                                       String loadBalancingPolicyClassName,
                                       int transactionBatchSize,
                                       int dupsOKBatchSize,
                                       long initialWaitTimeout,
                                       boolean useGlobalPools,
                                       int scheduledThreadPoolMaxSize,
                                       int threadPoolMaxSize,
                                       long retryInterval,
                                       double retryIntervalMultiplier,
                                       int reconnectAttempts,
                                       boolean failoverOnServerShutdown,
                                       List<String> jndiBindings) throws Exception
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

   public void createConnectionFactory(String name, TransportConfiguration liveTC, List<String> jndiBindings) throws Exception
   {
      replicationAwareInvoke("createConnectionFactory", liveTC, jndiBindings);
   }

   public void createConnectionFactory(String name,
                                       TransportConfiguration liveTC,
                                       String clientID,
                                       List<String> jndiBindings) throws Exception
   {
      replicationAwareInvoke("createConnectionFactory", liveTC, clientID, jndiBindings);
   }

   public void createConnectionFactory(String name,
                                       TransportConfiguration liveTC,
                                       TransportConfiguration backupTC,
                                       List<String> jndiBindings) throws Exception
   {
      replicationAwareInvoke("createConnectionFactory", liveTC, backupTC, jndiBindings);
   }

   public void createConnectionFactory(String name,
                                       TransportConfiguration liveTC,
                                       TransportConfiguration backupTC,
                                       String clientID,
                                       List<String> jndiBindings) throws Exception
   {
      replicationAwareInvoke("createConnectionFactory", liveTC, backupTC, clientID, jndiBindings);
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
