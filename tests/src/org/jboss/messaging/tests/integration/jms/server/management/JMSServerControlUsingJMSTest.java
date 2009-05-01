/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.integration.jms.server.management;

import static org.jboss.messaging.core.config.impl.ConfigurationImpl.DEFAULT_MANAGEMENT_ADDRESS;

import java.util.List;

import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.Session;

import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.management.ResourceNames;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.jms.server.impl.JMSServerManagerImpl;
import org.jboss.messaging.jms.server.management.JMSServerControlMBean;
import org.jboss.messaging.utils.Pair;

/**
 * A JMSServerControlUsingCoreTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class JMSServerControlUsingJMSTest extends JMSServerControlTest
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private QueueConnection connection;

   private QueueSession session;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // JMSServerControlTest overrides --------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      JMSServerManagerImpl serverManager = new JMSServerManagerImpl(server);
      serverManager.start();
      serverManager.setContext(context);

      JBossConnectionFactory cf = new JBossConnectionFactory(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      connection = cf.createQueueConnection();
      session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
      connection.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      connection.close();

      super.tearDown();
   }

   @Override
   protected JMSServerControlMBean createManagementControl() throws Exception
   {
      JBossQueue managementQueue = new JBossQueue(DEFAULT_MANAGEMENT_ADDRESS.toString(),
                                                  DEFAULT_MANAGEMENT_ADDRESS.toString());
      final JMSMessagingProxy proxy = new JMSMessagingProxy(session, managementQueue, ResourceNames.JMS_SERVER);

      return new JMSServerControlMBean()
      {

         public void createConnectionFactory(String name,
                                             List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs,
                                             List<String> jndiBindings) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory", name, connectorConfigs, jndiBindings);
         }

         public void createConnectionFactory(String name,
                                             List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs,
                                             String clientID,
                                             List<String> jndiBindings) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory", name, connectorConfigs, clientID, jndiBindings);
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
            proxy.invokeOperation("createConnectionFactory",
                                  name,
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
            proxy.invokeOperation("createConnectionFactory", name, discoveryAddress, clientID, jndiBindings);
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
            proxy.invokeOperation("createConnectionFactory",
                                  name,
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
            proxy.invokeOperation("createConnectionFactory", name, liveTC, jndiBindings);
         }

         public void createConnectionFactory(String name,
                                             TransportConfiguration liveTC,
                                             String clientID,
                                             List<String> jndiBindings) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory", name, liveTC, clientID, jndiBindings);
         }

         public void createConnectionFactory(String name,
                                             TransportConfiguration liveTC,
                                             TransportConfiguration backupTC,
                                             List<String> jndiBindings) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory", name, liveTC, backupTC, jndiBindings);
         }

         public void createConnectionFactory(String name,
                                             TransportConfiguration liveTC,
                                             TransportConfiguration backupTC,
                                             String clientID,
                                             List<String> jndiBindings) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory", name, liveTC, clientID, backupTC, jndiBindings);
         }

         public boolean closeConnectionsForAddress(final String ipAddress) throws Exception
         {
            return (Boolean)proxy.invokeOperation("closeConnectionsForAddress", ipAddress);
         }

         public boolean createQueue(final String name, final String jndiBinding) throws Exception
         {
            return (Boolean)proxy.invokeOperation("createQueue", name, jndiBinding);
         }

         public boolean createTopic(final String name, final String jndiBinding) throws Exception
         {
            return (Boolean)proxy.invokeOperation("createTopic", name, jndiBinding);
         }

         public void destroyConnectionFactory(final String name) throws Exception
         {
            proxy.invokeOperation("destroyConnectionFactory", name);
         }

         public boolean destroyQueue(final String name) throws Exception
         {
            return (Boolean)proxy.invokeOperation("destroyQueue", name);
         }

         public boolean destroyTopic(final String name) throws Exception
         {
            return (Boolean)proxy.invokeOperation("destroyTopic", name);
         }

         public String getVersion()
         {
            return (String)proxy.retrieveAttributeValue("Version");
         }

         public boolean isStarted()
         {
            return (Boolean)proxy.retrieveAttributeValue("Started");
         }

         public String[] listConnectionIDs() throws Exception
         {
            return (String[])proxy.invokeOperation("listConnectionIDs");
         }

         public String[] listRemoteAddresses() throws Exception
         {
            return (String[])proxy.invokeOperation("listRemoteAddresses");
         }

         public String[] listRemoteAddresses(final String ipAddress) throws Exception
         {
            return (String[])proxy.invokeOperation("listRemoteAddresses", ipAddress);
         }

         public String[] listSessions(final String connectionID) throws Exception
         {
            return (String[])proxy.invokeOperation("listSessions", connectionID);
         }

      };
   }
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
