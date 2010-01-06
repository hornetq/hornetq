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

package org.hornetq.tests.integration.jms.server.management;

import java.util.Map;

import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.Session;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.management.ResourceNames;
import org.hornetq.jms.HornetQConnectionFactory;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.jms.HornetQQueue;
import org.hornetq.api.jms.management.JMSServerControl;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;

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

   private static String[] toStringArray(final Object[] res)
   {
      String[] names = new String[res.length];
      for (int i = 0; i < res.length; i++)
      {
         names[i] = res[i].toString();
      }
      return names;
   }

   // Constructors --------------------------------------------------

   // JMSServerControlTest overrides --------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      HornetQConnectionFactory cf = (HornetQConnectionFactory) HornetQJMSClient.createConnectionFactory(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      connection = cf.createQueueConnection();
      session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
      connection.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      connection.close();

      connection = null;

      session = null;

      super.tearDown();
   }

   @Override
   protected JMSServerControl createManagementControl() throws Exception
   {
      HornetQQueue managementQueue = (HornetQQueue) HornetQJMSClient.createHornetQQueue(ConfigurationImpl.DEFAULT_MANAGEMENT_ADDRESS.toString(),
                                                      ConfigurationImpl.DEFAULT_MANAGEMENT_ADDRESS.toString());
      final JMSMessagingProxy proxy = new JMSMessagingProxy(session, managementQueue, ResourceNames.JMS_SERVER);

      return new JMSServerControl()
      {
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
                                             final long initialWaitTimeout,
                                             final boolean useGlobalPools,
                                             final int scheduledThreadPoolMaxSize,
                                             final int threadPoolMaxSize,
                                             final long retryInterval,
                                             final double retryIntervalMultiplier,
                                             final long maxRetryInterval,
                                             final int reconnectAttempts,
                                             final boolean failoverOnServerShutdown,
                                             final String groupId,
                                             final Object[] jndiBindings) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory",
                                  name,
                                  discoveryAddress,
                                  discoveryPort,
                                  clientID,
                                  clientFailureCheckPeriod,
                                  connectionTTL,
                                  callTimeout,
                                  cacheLargeMessageClient,
                                  minLargeMessageSize,
                                  consumerWindowSize,
                                  consumerMaxRate,
                                  confirmationWindowSize,
                                  producerWindowSize,
                                  producerMaxRate,
                                  blockOnAcknowledge,
                                  blockOnDurableSend,
                                  blockOnNonDurableSend,
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
                                  groupId,
                                  jndiBindings);
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
                                             final long initialWaitTimeout,
                                             final boolean useGlobalPools,
                                             final int scheduledThreadPoolMaxSize,
                                             final int threadPoolMaxSize,
                                             final long retryInterval,
                                             final double retryIntervalMultiplier,
                                             final long maxRetryInterval,
                                             final int reconnectAttempts,
                                             final boolean failoverOnServerShutdown,
                                             final String groupId,
                                             final String jndiBindings) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory",
                                  name,
                                  discoveryAddress,
                                  discoveryPort,
                                  clientID,
                                  clientFailureCheckPeriod,
                                  connectionTTL,
                                  callTimeout,
                                  cacheLargeMessageClient,
                                  minLargeMessageSize,
                                  consumerWindowSize,
                                  consumerMaxRate,
                                  confirmationWindowSize,
                                  producerWindowSize,
                                  producerMaxRate,
                                  blockOnAcknowledge,
                                  blockOnDurableSend,
                                  blockOnNonDurableSend,
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
                                  groupId,
                                  jndiBindings);
         }

         public void createConnectionFactory(final String name,
                                             final String discoveryAddress,
                                             final int discoveryPort,
                                             final String clientID,
                                             final Object[] jndiBindings) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory",
                                  name,
                                  discoveryAddress,
                                  discoveryPort,
                                  clientID,
                                  jndiBindings);
         }

         public void createConnectionFactory(final String name,
                                             final String discoveryAddress,
                                             final int discoveryPort,
                                             final String clientID,
                                             final String jndiBindings) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory",
                                  name,
                                  discoveryAddress,
                                  discoveryPort,
                                  clientID,
                                  jndiBindings);
         }

         public void createConnectionFactory(final String name,
                                             final String liveTransportClassName,
                                             final Map<String, Object> liveTransportParams,
                                             final String backupTransportClassName,
                                             final Map<String, Object> backupTransportParams,
                                             final String clientID,
                                             final Object[] jndiBindings) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory",
                                  name,
                                  liveTransportClassName,
                                  liveTransportParams,
                                  backupTransportClassName,
                                  backupTransportParams,
                                  clientID,
                                  jndiBindings);
         }

         public void createConnectionFactory(final String name,
                                             final String liveTransportClassNames,
                                             final String liveTransportParams,
                                             final String backupTransportClassNames,
                                             final String backupTransportParams,
                                             final String clientID,
                                             final String jndiBindings) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory",
                                  name,
                                  liveTransportClassNames,
                                  liveTransportParams,
                                  backupTransportClassNames,
                                  backupTransportParams,
                                  clientID,
                                  jndiBindings);
         }

         public void createConnectionFactory(final String name,
                                             final String liveTransportClassName,
                                             final Map<String, Object> liveTransportParams,
                                             final String backupTransportClassName,
                                             final Map<String, Object> backupTransportParams,
                                             final Object[] jndiBindings) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory",
                                  name,
                                  liveTransportClassName,
                                  liveTransportParams,
                                  backupTransportClassName,
                                  backupTransportParams,
                                  jndiBindings);
         }

         public void createConnectionFactory(final String name,
                                             final String liveTransportClassNames,
                                             final String liveTransportParams,
                                             final String backupTransportClassNames,
                                             final String backupTransportParams,
                                             final String jndiBindings) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory",
                                  name,
                                  liveTransportClassNames,
                                  liveTransportParams,
                                  backupTransportClassNames,
                                  backupTransportParams,
                                  jndiBindings);
         }

         public void createConnectionFactory(final String name,
                                             final String liveTransportClassName,
                                             final Map<String, Object> liveTransportParams,
                                             final String clientID,
                                             final Object[] jndiBindings) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory",
                                  name,
                                  liveTransportClassName,
                                  liveTransportParams,
                                  clientID,
                                  jndiBindings);
         }

         public void createConnectionFactory(final String name,
                                             final String liveTransportClassName,
                                             final String liveTransportParams,
                                             final String clientID,
                                             final String jndiBindings) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory",
                                  name,
                                  liveTransportClassName,
                                  liveTransportParams,
                                  clientID,
                                  jndiBindings);
         }

         public void createConnectionFactory(final String name,
                                             final String liveTransportClassName,
                                             final Map<String, Object> liveTransportParams,
                                             final Object[] jndiBindings) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory",
                                  name,
                                  liveTransportClassName,
                                  liveTransportParams,
                                  jndiBindings);
         }

         public void createConnectionFactory(final String name,
                                             final String liveTransportClassName,
                                             final String liveTransportParams,
                                             final String jndiBindings) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory",
                                  name,
                                  liveTransportClassName,
                                  liveTransportParams,
                                  jndiBindings);
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
                                             final boolean failoverOnServerShutdown,
                                             final String groupId,
                                             final Object[] jndiBindings) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory",
                                  name,
                                  liveConnectorsTransportClassNames,
                                  liveConnectorTransportParams,
                                  backupConnectorsTransportClassNames,
                                  backupConnectorTransportParams,
                                  clientID,
                                  clientFailureCheckPeriod,
                                  connectionTTL,
                                  callTimeout,
                                  cacheLargeMessageClient,
                                  minLargeMessageSize,
                                  consumerWindowSize,
                                  consumerMaxRate,
                                  confirmationWindowSize,
                                  producerWindowSize,
                                  producerMaxRate,
                                  blockOnAcknowledge,
                                  blockOnDurableSend,
                                  blockOnNonDurableSend,
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
                                  groupId,
                                  jndiBindings);

         }

         public void createConnectionFactory(final String name,
                                             final String liveConnectorsTransportClassNames,
                                             final String liveConnectorTransportParams,
                                             final String backupConnectorsTransportClassNames,
                                             final String backupConnectorTransportParams,
                                             final String clientID,
                                             final long clientFailureCheckPeriod,
                                             final long connectionTTL,
                                             final long callTimeout,
                                             final boolean cacheLargeMessageClient,
                                             final int minLargeMessageSize,
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
                                             final boolean failoverOnServerShutdown,
                                             final String groupId,
                                             final String jndiBindings) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory",
                                  name,
                                  liveConnectorsTransportClassNames,
                                  liveConnectorTransportParams,
                                  backupConnectorsTransportClassNames,
                                  backupConnectorTransportParams,
                                  clientID,
                                  clientFailureCheckPeriod,
                                  connectionTTL,
                                  callTimeout,
                                  cacheLargeMessageClient,
                                  minLargeMessageSize,
                                  consumerWindowSize,
                                  consumerMaxRate,
                                  confirmationWindowSize,
                                  producerWindowSize,
                                  producerMaxRate,
                                  blockOnAcknowledge,
                                  blockOnDurableSend,
                                  blockOnNonDurableSend,
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
                                  groupId,
                                  jndiBindings);

         }

         public void createConnectionFactory(final String name,
                                             final Object[] liveConnectorsTransportClassNames,
                                             final Object[] liveConnectorTransportParams,
                                             final Object[] backupConnectorsTransportClassNames,
                                             final Object[] backupConnectorTransportParams,
                                             final String clientID,
                                             final Object[] jndiBindings) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory",
                                  name,
                                  liveConnectorsTransportClassNames,
                                  liveConnectorTransportParams,
                                  backupConnectorsTransportClassNames,
                                  backupConnectorTransportParams,
                                  clientID,
                                  jndiBindings);
         }

         public void createConnectionFactory(final String name,
                                             final Object[] liveConnectorsTransportClassNames,
                                             final Object[] liveConnectorTransportParams,
                                             final Object[] backupConnectorsTransportClassNames,
                                             final Object[] backupConnectorTransportParams,
                                             final Object[] jndiBindings) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory",
                                  name,
                                  liveConnectorsTransportClassNames,
                                  liveConnectorTransportParams,
                                  backupConnectorsTransportClassNames,
                                  backupConnectorTransportParams,
                                  jndiBindings);
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
            return (String)proxy.retrieveAttributeValue("version");
         }

         public boolean isStarted()
         {
            return (Boolean)proxy.retrieveAttributeValue("started");
         }

         public String[] getQueueNames()
         {
            return JMSServerControlUsingJMSTest.toStringArray((Object[])proxy.retrieveAttributeValue("queueNames"));
         }

         public String[] getTopicNames()
         {
            return JMSServerControlUsingJMSTest.toStringArray((Object[])proxy.retrieveAttributeValue("topicNames"));
         }

         public String[] getConnectionFactoryNames()
         {
            return JMSServerControlUsingJMSTest.toStringArray((Object[])proxy.retrieveAttributeValue("connectionFactoryNames"));
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
