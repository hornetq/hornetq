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

package org.hornetq.tests.unit.ra;

import java.lang.reflect.Method;
import java.util.Timer;

import javax.jms.Connection;
import javax.resource.spi.BootstrapContext;
import javax.resource.spi.UnavailableException;
import javax.resource.spi.XATerminator;
import javax.resource.spi.endpoint.MessageEndpoint;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.resource.spi.work.ExecutionContext;
import javax.resource.spi.work.Work;
import javax.resource.spi.work.WorkException;
import javax.resource.spi.work.WorkListener;
import javax.resource.spi.work.WorkManager;
import javax.transaction.xa.XAResource;

import junit.framework.Assert;

import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.core.remoting.impl.invm.InVMConnector;
import org.hornetq.core.remoting.impl.netty.NettyConnector;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.client.HornetQDestination;
import org.hornetq.ra.ConnectionFactoryProperties;
import org.hornetq.ra.HornetQRAManagedConnectionFactory;
import org.hornetq.ra.HornetQResourceAdapter;
import org.hornetq.ra.inflow.HornetQActivation;
import org.hornetq.ra.inflow.HornetQActivationSpec;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * A ResourceAdapterTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ResourceAdapterTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testDefaultConnectionFactory() throws Exception
   {
      HornetQResourceAdapter ra = new HornetQResourceAdapter();
      ra.setConnectorClassName(InVMConnector.class.getName());
      HornetQConnectionFactory factory = ra.getDefaultHornetQConnectionFactory();
      Assert.assertEquals(factory.getCallTimeout(), HornetQClient.DEFAULT_CALL_TIMEOUT);
      Assert.assertEquals(factory.getClientFailureCheckPeriod(),
                          HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD);
      Assert.assertEquals(factory.getClientID(), null);
      Assert.assertEquals(factory.getConnectionLoadBalancingPolicyClassName(),
                          HornetQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME);
      Assert.assertEquals(factory.getConnectionTTL(), HornetQClient.DEFAULT_CONNECTION_TTL);
      Assert.assertEquals(factory.getConsumerMaxRate(), HornetQClient.DEFAULT_CONSUMER_MAX_RATE);
      Assert.assertEquals(factory.getConsumerWindowSize(), HornetQClient.DEFAULT_CONSUMER_WINDOW_SIZE);
      Assert.assertEquals(factory.getDiscoveryAddress(), null);
      Assert.assertEquals(factory.getDiscoveryInitialWaitTimeout(),
                          HornetQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT);
      Assert.assertEquals(factory.getDiscoveryPort(), 0);
      Assert.assertEquals(factory.getDiscoveryRefreshTimeout(),
                          HornetQClient.DEFAULT_DISCOVERY_REFRESH_TIMEOUT);
      Assert.assertEquals(factory.getDupsOKBatchSize(), HornetQClient.DEFAULT_ACK_BATCH_SIZE);
      Assert.assertEquals(factory.getMinLargeMessageSize(), HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);
      Assert.assertEquals(factory.getProducerMaxRate(), HornetQClient.DEFAULT_PRODUCER_MAX_RATE);
      Assert.assertEquals(factory.getConfirmationWindowSize(),
                          HornetQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE);
      // by default, reconnect attempts is set to -1
      Assert.assertEquals(-1, factory.getReconnectAttempts());
      Assert.assertEquals(factory.getRetryInterval(), HornetQClient.DEFAULT_RETRY_INTERVAL);
      Assert.assertEquals(factory.getRetryIntervalMultiplier(),
                          HornetQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER);
      Assert.assertEquals(factory.getScheduledThreadPoolMaxSize(),
                          HornetQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE);
      Assert.assertEquals(factory.getThreadPoolMaxSize(), HornetQClient.DEFAULT_THREAD_POOL_MAX_SIZE);
      Assert.assertEquals(factory.getTransactionBatchSize(), HornetQClient.DEFAULT_ACK_BATCH_SIZE);
      Assert.assertEquals(factory.isAutoGroup(), HornetQClient.DEFAULT_AUTO_GROUP);
      Assert.assertEquals(factory.isBlockOnAcknowledge(), HornetQClient.DEFAULT_BLOCK_ON_ACKNOWLEDGE);
      Assert.assertEquals(factory.isBlockOnNonDurableSend(),
                          HornetQClient.DEFAULT_BLOCK_ON_NON_DURABLE_SEND);
      Assert.assertEquals(factory.isBlockOnDurableSend(), HornetQClient.DEFAULT_BLOCK_ON_DURABLE_SEND);
      Assert.assertEquals(factory.isFailoverOnServerShutdown(),
                          HornetQClient.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN);
      Assert.assertEquals(factory.isPreAcknowledge(), HornetQClient.DEFAULT_PRE_ACKNOWLEDGE);
      Assert.assertEquals(factory.isUseGlobalPools(), HornetQClient.DEFAULT_USE_GLOBAL_POOLS);
   }

   public void test2DefaultConnectionFactorySame() throws Exception
   {
      HornetQResourceAdapter ra = new HornetQResourceAdapter();
      ra.setConnectorClassName(InVMConnector.class.getName());
      HornetQConnectionFactory factory = ra.getDefaultHornetQConnectionFactory();
      HornetQConnectionFactory factory2 = ra.getDefaultHornetQConnectionFactory();
      Assert.assertEquals(factory, factory2);
   }

   public void testCreateConnectionFactoryNoOverrides() throws Exception
   {
      HornetQResourceAdapter ra = new HornetQResourceAdapter();
      ra.setConnectorClassName(InVMConnector.class.getName());
      HornetQConnectionFactory factory = ra.createHornetQConnectionFactory(new ConnectionFactoryProperties());
      Assert.assertEquals(factory.getCallTimeout(), HornetQClient.DEFAULT_CALL_TIMEOUT);
      Assert.assertEquals(factory.getClientFailureCheckPeriod(),
                          HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD);
      Assert.assertEquals(factory.getClientID(), null);
      Assert.assertEquals(factory.getConnectionLoadBalancingPolicyClassName(),
                          HornetQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME);
      Assert.assertEquals(factory.getConnectionTTL(), HornetQClient.DEFAULT_CONNECTION_TTL);
      Assert.assertEquals(factory.getConsumerMaxRate(), HornetQClient.DEFAULT_CONSUMER_MAX_RATE);
      Assert.assertEquals(factory.getConsumerWindowSize(), HornetQClient.DEFAULT_CONSUMER_WINDOW_SIZE);
      Assert.assertEquals(factory.getDiscoveryAddress(), null);
      Assert.assertEquals(factory.getDiscoveryInitialWaitTimeout(),
                          HornetQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT);
      Assert.assertEquals(factory.getDiscoveryPort(), 0);
      Assert.assertEquals(factory.getDiscoveryRefreshTimeout(),
                          HornetQClient.DEFAULT_DISCOVERY_REFRESH_TIMEOUT);
      Assert.assertEquals(factory.getDupsOKBatchSize(), HornetQClient.DEFAULT_ACK_BATCH_SIZE);
      Assert.assertEquals(factory.getMinLargeMessageSize(), HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);
      Assert.assertEquals(factory.getProducerMaxRate(), HornetQClient.DEFAULT_PRODUCER_MAX_RATE);
      Assert.assertEquals(factory.getConfirmationWindowSize(),
                          HornetQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE);
      // by default, reconnect attempts is set to -1
      Assert.assertEquals(-1, factory.getReconnectAttempts());
      Assert.assertEquals(factory.getRetryInterval(), HornetQClient.DEFAULT_RETRY_INTERVAL);
      Assert.assertEquals(factory.getRetryIntervalMultiplier(),
                          HornetQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER);
      Assert.assertEquals(factory.getScheduledThreadPoolMaxSize(),
                          HornetQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE);
      Assert.assertEquals(factory.getThreadPoolMaxSize(), HornetQClient.DEFAULT_THREAD_POOL_MAX_SIZE);
      Assert.assertEquals(factory.getTransactionBatchSize(), HornetQClient.DEFAULT_ACK_BATCH_SIZE);
      Assert.assertEquals(factory.isAutoGroup(), HornetQClient.DEFAULT_AUTO_GROUP);
      Assert.assertEquals(factory.isBlockOnAcknowledge(), HornetQClient.DEFAULT_BLOCK_ON_ACKNOWLEDGE);
      Assert.assertEquals(factory.isBlockOnNonDurableSend(),
                          HornetQClient.DEFAULT_BLOCK_ON_NON_DURABLE_SEND);
      Assert.assertEquals(factory.isBlockOnDurableSend(), HornetQClient.DEFAULT_BLOCK_ON_DURABLE_SEND);
      Assert.assertEquals(factory.isFailoverOnServerShutdown(),
                          HornetQClient.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN);
      Assert.assertEquals(factory.isPreAcknowledge(), HornetQClient.DEFAULT_PRE_ACKNOWLEDGE);
      Assert.assertEquals(factory.isUseGlobalPools(), HornetQClient.DEFAULT_USE_GLOBAL_POOLS);
   }

   public void testDefaultConnectionFactoryOverrides() throws Exception
   {
      HornetQResourceAdapter ra = new HornetQResourceAdapter();
      ra.setConnectorClassName(InVMConnector.class.getName());
      ra.setAutoGroup(!HornetQClient.DEFAULT_AUTO_GROUP);
      ra.setBlockOnAcknowledge(!HornetQClient.DEFAULT_BLOCK_ON_ACKNOWLEDGE);
      ra.setBlockOnNonDurableSend(!HornetQClient.DEFAULT_BLOCK_ON_NON_DURABLE_SEND);
      ra.setBlockOnDurableSend(!HornetQClient.DEFAULT_BLOCK_ON_DURABLE_SEND);
      ra.setCallTimeout(1l);
      ra.setClientFailureCheckPeriod(2l);
      ra.setClientID("myid");
      ra.setConnectionLoadBalancingPolicyClassName("mlbcn");
      ra.setConnectionTTL(3l);
      ra.setConsumerMaxRate(4);
      ra.setConsumerWindowSize(5);
      ra.setDiscoveryInitialWaitTimeout(6l);
      ra.setDiscoveryRefreshTimeout(7l);
      ra.setDupsOKBatchSize(8);
      ra.setFailoverOnServerShutdown(!HornetQClient.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN);
      ra.setMinLargeMessageSize(10);
      ra.setPreAcknowledge(!HornetQClient.DEFAULT_PRE_ACKNOWLEDGE);
      ra.setProducerMaxRate(11);
      ra.setConfirmationWindowSize(12);
      ra.setReconnectAttempts(13);
      ra.setRetryInterval(14l);
      ra.setRetryIntervalMultiplier(15d);
      ra.setScheduledThreadPoolMaxSize(16);
      ra.setThreadPoolMaxSize(17);
      ra.setTransactionBatchSize(18);
      ra.setUseGlobalPools(!HornetQClient.DEFAULT_USE_GLOBAL_POOLS);
      HornetQConnectionFactory factory = ra.getDefaultHornetQConnectionFactory();
      Assert.assertEquals(factory.getCallTimeout(), 1);
      Assert.assertEquals(factory.getClientFailureCheckPeriod(), 2);
      Assert.assertEquals(factory.getClientID(), "myid");
      Assert.assertEquals(factory.getConnectionLoadBalancingPolicyClassName(), "mlbcn");
      Assert.assertEquals(factory.getConnectionTTL(), 3);
      Assert.assertEquals(factory.getConsumerMaxRate(), 4);
      Assert.assertEquals(factory.getConsumerWindowSize(), 5);
      Assert.assertEquals(factory.getDiscoveryAddress(), null);
      Assert.assertEquals(factory.getDiscoveryInitialWaitTimeout(), 6);
      Assert.assertEquals(factory.getDiscoveryPort(), 0);
      Assert.assertEquals(factory.getDiscoveryRefreshTimeout(), 7);
      Assert.assertEquals(factory.getDupsOKBatchSize(), 8);
      Assert.assertEquals(factory.getMinLargeMessageSize(), 10);
      Assert.assertEquals(factory.getProducerMaxRate(), 11);
      Assert.assertEquals(factory.getConfirmationWindowSize(), 12);
      Assert.assertEquals(factory.getReconnectAttempts(), 13);
      Assert.assertEquals(factory.getRetryInterval(), 14);
      Assert.assertEquals(factory.getRetryIntervalMultiplier(), 15d);
      Assert.assertEquals(factory.getScheduledThreadPoolMaxSize(), 16);
      Assert.assertEquals(factory.getThreadPoolMaxSize(), 17);
      Assert.assertEquals(factory.getTransactionBatchSize(), 18);
      Assert.assertEquals(factory.isAutoGroup(), !HornetQClient.DEFAULT_AUTO_GROUP);
      Assert.assertEquals(factory.isBlockOnAcknowledge(), !HornetQClient.DEFAULT_BLOCK_ON_ACKNOWLEDGE);
      Assert.assertEquals(factory.isBlockOnNonDurableSend(),
                          !HornetQClient.DEFAULT_BLOCK_ON_NON_DURABLE_SEND);
      Assert.assertEquals(factory.isBlockOnDurableSend(), !HornetQClient.DEFAULT_BLOCK_ON_DURABLE_SEND);
      Assert.assertEquals(factory.isFailoverOnServerShutdown(),
                          !HornetQClient.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN);
      Assert.assertEquals(factory.isPreAcknowledge(), !HornetQClient.DEFAULT_PRE_ACKNOWLEDGE);
      Assert.assertEquals(factory.isUseGlobalPools(), !HornetQClient.DEFAULT_USE_GLOBAL_POOLS);
   }

   public void testCreateConnectionFactoryOverrides() throws Exception
   {
      HornetQResourceAdapter ra = new HornetQResourceAdapter();
      ra.setConnectorClassName(InVMConnector.class.getName());
      ConnectionFactoryProperties connectionFactoryProperties = new ConnectionFactoryProperties();
      connectionFactoryProperties.setAutoGroup(!HornetQClient.DEFAULT_AUTO_GROUP);
      connectionFactoryProperties.setBlockOnAcknowledge(!HornetQClient.DEFAULT_BLOCK_ON_ACKNOWLEDGE);
      connectionFactoryProperties.setBlockOnNonDurableSend(!HornetQClient.DEFAULT_BLOCK_ON_NON_DURABLE_SEND);
      connectionFactoryProperties.setBlockOnDurableSend(!HornetQClient.DEFAULT_BLOCK_ON_DURABLE_SEND);
      connectionFactoryProperties.setCallTimeout(1l);
      connectionFactoryProperties.setClientFailureCheckPeriod(2l);
      connectionFactoryProperties.setClientID("myid");
      connectionFactoryProperties.setConnectionLoadBalancingPolicyClassName("mlbcn");
      connectionFactoryProperties.setConnectionTTL(3l);
      connectionFactoryProperties.setConsumerMaxRate(4);
      connectionFactoryProperties.setConsumerWindowSize(5);
      connectionFactoryProperties.setDiscoveryInitialWaitTimeout(6l);
      connectionFactoryProperties.setDiscoveryRefreshTimeout(7l);
      connectionFactoryProperties.setDupsOKBatchSize(8);
      connectionFactoryProperties.setFailoverOnServerShutdown(!HornetQClient.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN);
      connectionFactoryProperties.setMinLargeMessageSize(10);
      connectionFactoryProperties.setPreAcknowledge(!HornetQClient.DEFAULT_PRE_ACKNOWLEDGE);
      connectionFactoryProperties.setProducerMaxRate(11);
      connectionFactoryProperties.setConfirmationWindowSize(12);
      connectionFactoryProperties.setReconnectAttempts(13);
      connectionFactoryProperties.setRetryInterval(14l);
      connectionFactoryProperties.setRetryIntervalMultiplier(15d);
      connectionFactoryProperties.setScheduledThreadPoolMaxSize(16);
      connectionFactoryProperties.setThreadPoolMaxSize(17);
      connectionFactoryProperties.setTransactionBatchSize(18);
      connectionFactoryProperties.setUseGlobalPools(!HornetQClient.DEFAULT_USE_GLOBAL_POOLS);
      HornetQConnectionFactory factory = ra.createHornetQConnectionFactory(connectionFactoryProperties);
      Assert.assertEquals(factory.getCallTimeout(), 1);
      Assert.assertEquals(factory.getClientFailureCheckPeriod(), 2);
      Assert.assertEquals(factory.getClientID(), "myid");
      Assert.assertEquals(factory.getConnectionLoadBalancingPolicyClassName(), "mlbcn");
      Assert.assertEquals(factory.getConnectionTTL(), 3);
      Assert.assertEquals(factory.getConsumerMaxRate(), 4);
      Assert.assertEquals(factory.getConsumerWindowSize(), 5);
      Assert.assertEquals(factory.getDiscoveryAddress(), null);
      Assert.assertEquals(factory.getDiscoveryInitialWaitTimeout(), 6);
      Assert.assertEquals(factory.getDiscoveryPort(), 0);
      Assert.assertEquals(factory.getDiscoveryRefreshTimeout(), 7);
      Assert.assertEquals(factory.getDupsOKBatchSize(), 8);
      Assert.assertEquals(factory.getMinLargeMessageSize(), 10);
      Assert.assertEquals(factory.getProducerMaxRate(), 11);
      Assert.assertEquals(factory.getConfirmationWindowSize(), 12);
      Assert.assertEquals(factory.getReconnectAttempts(), 13);
      Assert.assertEquals(factory.getRetryInterval(), 14);
      Assert.assertEquals(factory.getRetryIntervalMultiplier(), 15d);
      Assert.assertEquals(factory.getScheduledThreadPoolMaxSize(), 16);
      Assert.assertEquals(factory.getThreadPoolMaxSize(), 17);
      Assert.assertEquals(factory.getTransactionBatchSize(), 18);
      Assert.assertEquals(factory.isAutoGroup(), !HornetQClient.DEFAULT_AUTO_GROUP);
      Assert.assertEquals(factory.isBlockOnAcknowledge(), !HornetQClient.DEFAULT_BLOCK_ON_ACKNOWLEDGE);
      Assert.assertEquals(factory.isBlockOnNonDurableSend(),
                          !HornetQClient.DEFAULT_BLOCK_ON_NON_DURABLE_SEND);
      Assert.assertEquals(factory.isBlockOnDurableSend(), !HornetQClient.DEFAULT_BLOCK_ON_DURABLE_SEND);
      Assert.assertEquals(factory.isFailoverOnServerShutdown(),
                          !HornetQClient.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN);
      Assert.assertEquals(factory.isPreAcknowledge(), !HornetQClient.DEFAULT_PRE_ACKNOWLEDGE);
      Assert.assertEquals(factory.isUseGlobalPools(), !HornetQClient.DEFAULT_USE_GLOBAL_POOLS);
   }

   public void testCreateConnectionFactoryOverrideConnector() throws Exception
   {
      HornetQResourceAdapter ra = new HornetQResourceAdapter();
      ra.setConnectorClassName(InVMConnector.class.getName());
      ConnectionFactoryProperties connectionFactoryProperties = new ConnectionFactoryProperties();
      connectionFactoryProperties.setConnectorClassName(NettyConnector.class.getName());
      HornetQConnectionFactory factory = ra.createHornetQConnectionFactory(connectionFactoryProperties);
      HornetQConnectionFactory defaultFactory = ra.getDefaultHornetQConnectionFactory();
      Assert.assertNotSame(factory, defaultFactory);
   }

   public void testCreateConnectionFactoryOverrideDiscovery() throws Exception
   {
      HornetQResourceAdapter ra = new HornetQResourceAdapter();
      ra.setConnectorClassName(InVMConnector.class.getName());
      ConnectionFactoryProperties connectionFactoryProperties = new ConnectionFactoryProperties();
      connectionFactoryProperties.setDiscoveryAddress("myhost");
      connectionFactoryProperties.setDiscoveryPort(5678);
      HornetQConnectionFactory factory = ra.createHornetQConnectionFactory(connectionFactoryProperties);
      HornetQConnectionFactory defaultFactory = ra.getDefaultHornetQConnectionFactory();
      Assert.assertNotSame(factory, defaultFactory);
   }

   public void testCreateConnectionFactoryThrowsException() throws Exception
   {
      HornetQResourceAdapter ra = new HornetQResourceAdapter();
      ConnectionFactoryProperties connectionFactoryProperties = new ConnectionFactoryProperties();
      try
      {
         HornetQConnectionFactory factory = ra.createHornetQConnectionFactory(connectionFactoryProperties);
         Assert.fail("should throw exception");
      }
      catch (IllegalArgumentException e)
      {
         // pass
      }
   }

   public void testValidateProperties() throws Exception
   {
      validateGettersAndSetters(new HornetQResourceAdapter(), "backupTransportConfiguration", "connectionParameters");
      validateGettersAndSetters(new HornetQRAManagedConnectionFactory(),
                                "connectionParameters",
                                "sessionDefaultType",
                                "backupConnectionParameters");
      validateGettersAndSetters(new HornetQActivationSpec(),
                                "connectionParameters",
                                "acknowledgeMode",
                                "subscriptionDurability");

      HornetQActivationSpec spec = new HornetQActivationSpec();

      spec.setAcknowledgeMode("DUPS_OK_ACKNOWLEDGE");
      Assert.assertEquals("Dups-ok-acknowledge", spec.getAcknowledgeMode());

      spec.setSubscriptionDurability("Durable");
      Assert.assertEquals("Durable", spec.getSubscriptionDurability());

      spec.setSubscriptionDurability("NonDurable");
      Assert.assertEquals("NonDurable", spec.getSubscriptionDurability());

      spec = new HornetQActivationSpec();
      HornetQResourceAdapter adapter = new HornetQResourceAdapter();

      adapter.setUserName("us1");
      adapter.setPassword("ps1");
      adapter.setClientID("cl1");

      spec.setResourceAdapter(adapter);

      Assert.assertEquals("us1", spec.getUser());
      Assert.assertEquals("ps1", spec.getPassword());

      spec.setUser("us2");
      spec.setPassword("ps2");
      spec.setClientID("cl2");

      Assert.assertEquals("us2", spec.getUser());
      Assert.assertEquals("ps2", spec.getPassword());
      Assert.assertEquals("cl2", spec.getClientID());

   }

   public void testStartActivation() throws Exception
   {
      HornetQServer server = createServer(false);

      try
      {

         server.start();

         ClientSessionFactory factory = createInVMFactory();
         ClientSession session = factory.createSession(false, false, false);
         HornetQDestination queue = (HornetQDestination) HornetQJMSClient.createQueue("test");
         session.createQueue(queue.getSimpleAddress(), queue.getSimpleAddress(), true);
         session.close();

         HornetQResourceAdapter ra = new HornetQResourceAdapter();

         ra.setConnectorClassName("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory");
         ra.setUserName("userGlobal");
         ra.setPassword("passwordGlobal");
         ra.start(fakeCTX);

         Connection conn = ra.getDefaultHornetQConnectionFactory().createConnection();

         conn.close();

         HornetQActivationSpec spec = new HornetQActivationSpec();

         spec.setResourceAdapter(ra);

         spec.setUseJNDI(false);

         spec.setUser("user");
         spec.setPassword("password");

         spec.setDestinationType("Topic");
         spec.setDestination("test");

         spec.setMinSession(1);
         spec.setMaxSession(1);

         HornetQActivation activation = new HornetQActivation(ra, new FakeMessageEndpointFactory(), spec);

         activation.start();
         activation.stop();

      }
      finally
      {
         server.stop();
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   class MockHornetQResourceAdapter extends HornetQResourceAdapter
   {

      /**
       * 
       */
      private static final long serialVersionUID = 2893126091158533715L;
      /*public HornetQRAConnectionFactory createRemoteFactory(String connectorClassName,
                                                        Map<String, Object> connectionParameters)
      {
         HornetQRAConnectionFactory factory = super.createHornetQConnectionFactory(connectionParameters);

         return factory;
      }*/
   }

   BootstrapContext fakeCTX = new BootstrapContext()
   {

      public Timer createTimer() throws UnavailableException
      {
         return null;
      }

      public WorkManager getWorkManager()
      {
         return new WorkManager()
         {
            public void doWork(final Work work) throws WorkException
            {
            }

            public void doWork(final Work work,
                               final long l,
                               final ExecutionContext executionContext,
                               final WorkListener workListener) throws WorkException
            {
            }

            public long startWork(final Work work) throws WorkException
            {
               return 0;
            }

            public long startWork(final Work work,
                                  final long l,
                                  final ExecutionContext executionContext,
                                  final WorkListener workListener) throws WorkException
            {
               return 0;
            }

            public void scheduleWork(final Work work) throws WorkException
            {
               work.run();
            }

            public void scheduleWork(final Work work,
                                     final long l,
                                     final ExecutionContext executionContext,
                                     final WorkListener workListener) throws WorkException
            {
            }
         };
      }

      public XATerminator getXATerminator()
      {
         return null;
      }

   };

   class FakeMessageEndpointFactory implements MessageEndpointFactory
   {

      /* (non-Javadoc)
       * @see javax.resource.spi.endpoint.MessageEndpointFactory#createEndpoint(javax.transaction.xa.XAResource)
       */
      public MessageEndpoint createEndpoint(final XAResource arg0) throws UnavailableException
      {
         return null;
      }

      /* (non-Javadoc)
       * @see javax.resource.spi.endpoint.MessageEndpointFactory#isDeliveryTransacted(java.lang.reflect.Method)
       */
      public boolean isDeliveryTransacted(final Method arg0) throws NoSuchMethodException
      {
         return false;
      }

   }
}
