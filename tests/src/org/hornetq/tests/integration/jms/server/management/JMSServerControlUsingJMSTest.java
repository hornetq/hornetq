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

import static org.hornetq.core.config.impl.ConfigurationImpl.DEFAULT_MANAGEMENT_ADDRESS;

import java.util.Map;

import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.Session;

import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.management.ResourceNames;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.jms.HornetQQueue;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.server.management.JMSServerControl;

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

   private static String[] toStringArray(Object[] res)
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

      HornetQConnectionFactory cf = new HornetQConnectionFactory(new TransportConfiguration(InVMConnectorFactory.class.getName()));
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
      HornetQQueue managementQueue = new HornetQQueue(DEFAULT_MANAGEMENT_ADDRESS.toString(),
                                                  DEFAULT_MANAGEMENT_ADDRESS.toString());
      final JMSMessagingProxy proxy = new JMSMessagingProxy(session, managementQueue, ResourceNames.JMS_SERVER);

      return new JMSServerControl()
      {
         public void createConnectionFactory(String name,
                                             String discoveryAddress,
                                             int discoveryPort,
                                             String clientID,
                                             long discoveryRefreshTimeout,
                                             long clientFailureCheckPeriod,
                                             long connectionTTL,
                                             long callTimeout,                                            
                                             boolean cacheLargeMessageClient,
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
                                             long maxRetryInterval,
                                             int reconnectAttempts,
                                             boolean failoverOnServerShutdown,
                                             Object[] jndiBindings) throws Exception
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
                                  jndiBindings);
         }

         public void createConnectionFactory(String name,
                                             String discoveryAddress,
                                             int discoveryPort,
                                             String clientID,
                                             long discoveryRefreshTimeout,
                                             long clientFailureCheckPeriod,
                                             long connectionTTL,
                                             long callTimeout,                                           
                                             boolean cacheLargeMessageClient,
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
                                             long maxRetryInterval,
                                             int reconnectAttempts,
                                             boolean failoverOnServerShutdown,
                                             String jndiBindings) throws Exception
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
                                  jndiBindings);
         }

         public void createConnectionFactory(String name,
                                             String discoveryAddress,
                                             int discoveryPort,
                                             String clientID,
                                             Object[] jndiBindings) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory",
                                  name,
                                  discoveryAddress,
                                  discoveryPort,
                                  clientID,
                                  jndiBindings);
         }

         public void createConnectionFactory(String name,
                                             String discoveryAddress,
                                             int discoveryPort,
                                             String clientID,
                                             String jndiBindings) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory",
                                  name,
                                  discoveryAddress,
                                  discoveryPort,
                                  clientID,
                                  jndiBindings);
         }

         public void createConnectionFactory(String name,
                                             String liveTransportClassName,
                                             Map<String, Object> liveTransportParams,
                                             String backupTransportClassName,
                                             Map<String, Object> backupTransportParams,
                                             String clientID,
                                             Object[] jndiBindings) throws Exception
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

         public void createConnectionFactory(String name,
                                             String liveTransportClassNames,
                                             String liveTransportParams,
                                             String backupTransportClassNames,
                                             String backupTransportParams,
                                             String clientID,
                                             String jndiBindings) throws Exception
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

         public void createConnectionFactory(String name,
                                             String liveTransportClassName,
                                             Map<String, Object> liveTransportParams,
                                             String backupTransportClassName,
                                             Map<String, Object> backupTransportParams,
                                             Object[] jndiBindings) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory",
                                  name,
                                  liveTransportClassName,
                                  liveTransportParams,
                                  backupTransportClassName,
                                  backupTransportParams,
                                  jndiBindings);
         }

         public void createConnectionFactory(String name,
                                             String liveTransportClassNames,
                                             String liveTransportParams,
                                             String backupTransportClassNames,
                                             String backupTransportParams,
                                             String jndiBindings) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory",
                                  name,
                                  liveTransportClassNames,
                                  liveTransportParams,
                                  backupTransportClassNames,
                                  backupTransportParams,
                                  jndiBindings);
         }

         public void createConnectionFactory(String name,
                                             String liveTransportClassName,
                                             Map<String, Object> liveTransportParams,
                                             String clientID,
                                             Object[] jndiBindings) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory",
                                  name,
                                  liveTransportClassName,
                                  liveTransportParams,
                                  clientID,
                                  jndiBindings);
         }

         public void createConnectionFactory(String name,
                                             String liveTransportClassName,
                                             String liveTransportParams,
                                             String clientID,
                                             String jndiBindings) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory",
                                  name,
                                  liveTransportClassName,
                                  liveTransportParams,
                                  clientID,
                                  jndiBindings);
         }

         public void createConnectionFactory(String name,
                                             String liveTransportClassName,
                                             Map<String, Object> liveTransportParams,
                                             Object[] jndiBindings) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory",
                                  name,
                                  liveTransportClassName,
                                  liveTransportParams,
                                  jndiBindings);
         }

         public void createConnectionFactory(String name,
                                             String liveTransportClassName,
                                             String liveTransportParams,
                                             String jndiBindings) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory",
                                  name,
                                  liveTransportClassName,
                                  liveTransportParams,
                                  jndiBindings);
         }

         public void createConnectionFactory(String name,
                                             Object[] liveConnectorsTransportClassNames,
                                             Object[] liveConnectorTransportParams,
                                             Object[] backupConnectorsTransportClassNames,
                                             Object[] backupConnectorTransportParams,
                                             String clientID,
                                             long clientFailureCheckPeriod,
                                             long connectionTTL,
                                             long callTimeout,                                            
                                             boolean cacheLargeMessageClient,
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
                                             long maxRetryInterval,
                                             int reconnectAttempts,
                                             boolean failoverOnServerShutdown,
                                             Object[] jndiBindings) throws Exception
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
                                  jndiBindings);

         }

         public void createConnectionFactory(String name,
                                             String liveConnectorsTransportClassNames,
                                             String liveConnectorTransportParams,
                                             String backupConnectorsTransportClassNames,
                                             String backupConnectorTransportParams,
                                             String clientID,
                                             long clientFailureCheckPeriod,
                                             long connectionTTL,
                                             long callTimeout,                                            
                                             boolean cacheLargeMessageClient,
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
                                             long maxRetryInterval,
                                             int reconnectAttempts,
                                             boolean failoverOnServerShutdown,
                                             String jndiBindings) throws Exception
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
                                  jndiBindings);

         }

         public void createConnectionFactory(String name,
                                             Object[] liveConnectorsTransportClassNames,
                                             Object[] liveConnectorTransportParams,
                                             Object[] backupConnectorsTransportClassNames,
                                             Object[] backupConnectorTransportParams,
                                             String clientID,
                                             Object[] jndiBindings) throws Exception
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

         public void createConnectionFactory(String name,
                                             Object[] liveConnectorsTransportClassNames,
                                             Object[] liveConnectorTransportParams,
                                             Object[] backupConnectorsTransportClassNames,
                                             Object[] backupConnectorTransportParams,
                                             Object[] jndiBindings) throws Exception
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
            return toStringArray((Object[])proxy.retrieveAttributeValue("queueNames"));
         }

         public String[] getTopicNames()
         {
            return toStringArray((Object[])proxy.retrieveAttributeValue("topicNames"));
         }

         public String[] getConnectionFactoryNames()
         {
            return toStringArray((Object[])proxy.retrieveAttributeValue("connectionFactoryNames"));
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
