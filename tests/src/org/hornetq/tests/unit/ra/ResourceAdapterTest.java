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

import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.remoting.impl.invm.InVMConnector;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.integration.transports.netty.NettyConnector;
import org.hornetq.jms.HornetQQueue;
import org.hornetq.jms.client.HornetQConnectionFactory;
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
      assertEquals(factory.getCallTimeout(), ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT);
      assertEquals(factory.getClientFailureCheckPeriod(), ClientSessionFactoryImpl.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD);
      assertEquals(factory.getClientID(), null);
      assertEquals(factory.getConnectionLoadBalancingPolicyClassName(), ClientSessionFactoryImpl.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME);
      assertEquals(factory.getConnectionTTL(), ClientSessionFactoryImpl.DEFAULT_CONNECTION_TTL);
      assertEquals(factory.getConsumerMaxRate(), ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE);
      assertEquals(factory.getConsumerWindowSize(), ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE);
      assertEquals(factory.getDiscoveryAddress(), null);
      assertEquals(factory.getDiscoveryInitialWaitTimeout(), ClientSessionFactoryImpl.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT);
      assertEquals(factory.getDiscoveryPort(), 0);
      assertEquals(factory.getDiscoveryRefreshTimeout(), ClientSessionFactoryImpl.DEFAULT_DISCOVERY_REFRESH_TIMEOUT);
      assertEquals(factory.getDupsOKBatchSize(), ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE);     
      assertEquals(factory.getMinLargeMessageSize(), ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE);
      assertEquals(factory.getProducerMaxRate(), ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE);
      assertEquals(factory.getProducerWindowSize(), ClientSessionFactoryImpl.DEFAULT_PRODUCER_WINDOW_SIZE);
      assertEquals(factory.getReconnectAttempts(), ClientSessionFactoryImpl.DEFAULT_RECONNECT_ATTEMPTS);
      assertEquals(factory.getRetryInterval(), ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL);
      assertEquals(factory.getRetryIntervalMultiplier(), ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL_MULTIPLIER);
      assertEquals(factory.getScheduledThreadPoolMaxSize(), ClientSessionFactoryImpl.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE);
      assertEquals(factory.getThreadPoolMaxSize(), ClientSessionFactoryImpl.DEFAULT_THREAD_POOL_MAX_SIZE);
      assertEquals(factory.getTransactionBatchSize(), ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE);
      assertEquals(factory.isAutoGroup(), ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP);
      assertEquals(factory.isBlockOnAcknowledge(), ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE);
      assertEquals(factory.isBlockOnNonPersistentSend(), ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND);
      assertEquals(factory.isBlockOnPersistentSend(), ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_PERSISTENT_SEND);
      assertEquals(factory.isFailoverOnServerShutdown(), ClientSessionFactoryImpl.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN);
      assertEquals(factory.isPreAcknowledge(), ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE);
      assertEquals(factory.isUseGlobalPools(), ClientSessionFactoryImpl.DEFAULT_USE_GLOBAL_POOLS);
   }

   public void test2DefaultConnectionFactorySame() throws Exception
   {
      HornetQResourceAdapter ra = new HornetQResourceAdapter();
      ra.setConnectorClassName(InVMConnector.class.getName());
      HornetQConnectionFactory factory = ra.getDefaultHornetQConnectionFactory();
      HornetQConnectionFactory factory2 = ra.getDefaultHornetQConnectionFactory();
      assertEquals(factory, factory2);
   }

   public void testCreateConnectionFactoryNoOverrides() throws Exception
   {
      HornetQResourceAdapter ra = new HornetQResourceAdapter();
      ra.setConnectorClassName(InVMConnector.class.getName());
      HornetQConnectionFactory factory = ra.createHornetQConnectionFactory(new ConnectionFactoryProperties());
      assertEquals(factory.getCallTimeout(), ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT);
      assertEquals(factory.getClientFailureCheckPeriod(), ClientSessionFactoryImpl.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD);
      assertEquals(factory.getClientID(), null);
      assertEquals(factory.getConnectionLoadBalancingPolicyClassName(), ClientSessionFactoryImpl.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME);
      assertEquals(factory.getConnectionTTL(), ClientSessionFactoryImpl.DEFAULT_CONNECTION_TTL);
      assertEquals(factory.getConsumerMaxRate(), ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE);
      assertEquals(factory.getConsumerWindowSize(), ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE);
      assertEquals(factory.getDiscoveryAddress(), null);
      assertEquals(factory.getDiscoveryInitialWaitTimeout(), ClientSessionFactoryImpl.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT);
      assertEquals(factory.getDiscoveryPort(), 0);
      assertEquals(factory.getDiscoveryRefreshTimeout(), ClientSessionFactoryImpl.DEFAULT_DISCOVERY_REFRESH_TIMEOUT);
      assertEquals(factory.getDupsOKBatchSize(), ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE);     
      assertEquals(factory.getMinLargeMessageSize(), ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE);
      assertEquals(factory.getProducerMaxRate(), ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE);
      assertEquals(factory.getProducerWindowSize(), ClientSessionFactoryImpl.DEFAULT_PRODUCER_WINDOW_SIZE);
      assertEquals(factory.getReconnectAttempts(), ClientSessionFactoryImpl.DEFAULT_RECONNECT_ATTEMPTS);
      assertEquals(factory.getRetryInterval(), ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL);
      assertEquals(factory.getRetryIntervalMultiplier(), ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL_MULTIPLIER);
      assertEquals(factory.getScheduledThreadPoolMaxSize(), ClientSessionFactoryImpl.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE);
      assertEquals(factory.getThreadPoolMaxSize(), ClientSessionFactoryImpl.DEFAULT_THREAD_POOL_MAX_SIZE);
      assertEquals(factory.getTransactionBatchSize(), ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE);
      assertEquals(factory.isAutoGroup(), ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP);
      assertEquals(factory.isBlockOnAcknowledge(), ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE);
      assertEquals(factory.isBlockOnNonPersistentSend(), ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND);
      assertEquals(factory.isBlockOnPersistentSend(), ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_PERSISTENT_SEND);
      assertEquals(factory.isFailoverOnServerShutdown(), ClientSessionFactoryImpl.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN);
      assertEquals(factory.isPreAcknowledge(), ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE);
      assertEquals(factory.isUseGlobalPools(), ClientSessionFactoryImpl.DEFAULT_USE_GLOBAL_POOLS);
   }

   public void testDefaultConnectionFactoryOverrides() throws Exception
   {
      HornetQResourceAdapter ra = new HornetQResourceAdapter();
      ra.setConnectorClassName(InVMConnector.class.getName());
      ra.setAutoGroup(!ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP);
      ra.setBlockOnAcknowledge(!ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE);
      ra.setBlockOnNonPersistentSend(!ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND);
      ra.setBlockOnPersistentSend(!ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_PERSISTENT_SEND);
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
      ra.setFailoverOnServerShutdown(!ClientSessionFactoryImpl.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN);    
      ra.setMinLargeMessageSize(10);
      ra.setPreAcknowledge(!ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE);
      ra.setProducerMaxRate(11);
      ra.setProducerWindowSize(12);
      ra.setReconnectAttempts(13);
      ra.setRetryInterval(14l);
      ra.setRetryIntervalMultiplier(15d);
      ra.setScheduledThreadPoolMaxSize(16);
      ra.setThreadPoolMaxSize(17);
      ra.setTransactionBatchSize(18);
      ra.setUseGlobalPools(!ClientSessionFactoryImpl.DEFAULT_USE_GLOBAL_POOLS);
      HornetQConnectionFactory factory = ra.getDefaultHornetQConnectionFactory();
      assertEquals(factory.getCallTimeout(), 1);
      assertEquals(factory.getClientFailureCheckPeriod(), 2);
      assertEquals(factory.getClientID(), "myid");
      assertEquals(factory.getConnectionLoadBalancingPolicyClassName(), "mlbcn");
      assertEquals(factory.getConnectionTTL(), 3);
      assertEquals(factory.getConsumerMaxRate(), 4);
      assertEquals(factory.getConsumerWindowSize(), 5);
      assertEquals(factory.getDiscoveryAddress(), null);
      assertEquals(factory.getDiscoveryInitialWaitTimeout(), 6);
      assertEquals(factory.getDiscoveryPort(), 0);
      assertEquals(factory.getDiscoveryRefreshTimeout(), 7);
      assertEquals(factory.getDupsOKBatchSize(), 8);     
      assertEquals(factory.getMinLargeMessageSize(), 10);
      assertEquals(factory.getProducerMaxRate(), 11);
      assertEquals(factory.getProducerWindowSize(), 12);
      assertEquals(factory.getReconnectAttempts(), 13);
      assertEquals(factory.getRetryInterval(), 14);
      assertEquals(factory.getRetryIntervalMultiplier(), 15d);
      assertEquals(factory.getScheduledThreadPoolMaxSize(), 16);
      assertEquals(factory.getThreadPoolMaxSize(), 17);
      assertEquals(factory.getTransactionBatchSize(), 18);
      assertEquals(factory.isAutoGroup(), !ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP);
      assertEquals(factory.isBlockOnAcknowledge(), !ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE);
      assertEquals(factory.isBlockOnNonPersistentSend(), !ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND);
      assertEquals(factory.isBlockOnPersistentSend(), !ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_PERSISTENT_SEND);
      assertEquals(factory.isFailoverOnServerShutdown(), !ClientSessionFactoryImpl.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN);
      assertEquals(factory.isPreAcknowledge(), !ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE);
      assertEquals(factory.isUseGlobalPools(), !ClientSessionFactoryImpl.DEFAULT_USE_GLOBAL_POOLS);
   }

   public void testCreateConnectionFactoryOverrides() throws Exception
   {
      HornetQResourceAdapter ra = new HornetQResourceAdapter();
      ra.setConnectorClassName(InVMConnector.class.getName());
      ConnectionFactoryProperties connectionFactoryProperties = new ConnectionFactoryProperties();
      connectionFactoryProperties.setAutoGroup(!ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP);
      connectionFactoryProperties.setBlockOnAcknowledge(!ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE);
      connectionFactoryProperties.setBlockOnNonPersistentSend(!ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND);
      connectionFactoryProperties.setBlockOnPersistentSend(!ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_PERSISTENT_SEND);
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
      connectionFactoryProperties.setFailoverOnServerShutdown(!ClientSessionFactoryImpl.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN);     
      connectionFactoryProperties.setMinLargeMessageSize(10);
      connectionFactoryProperties.setPreAcknowledge(!ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE);
      connectionFactoryProperties.setProducerMaxRate(11);
      connectionFactoryProperties.setProducerWindowSize(12);
      connectionFactoryProperties.setReconnectAttempts(13);
      connectionFactoryProperties.setRetryInterval(14l);
      connectionFactoryProperties.setRetryIntervalMultiplier(15d);
      connectionFactoryProperties.setScheduledThreadPoolMaxSize(16);
      connectionFactoryProperties.setThreadPoolMaxSize(17);
      connectionFactoryProperties.setTransactionBatchSize(18);
      connectionFactoryProperties.setUseGlobalPools(!ClientSessionFactoryImpl.DEFAULT_USE_GLOBAL_POOLS);
      HornetQConnectionFactory factory = ra.createHornetQConnectionFactory(connectionFactoryProperties);
      assertEquals(factory.getCallTimeout(), 1);
      assertEquals(factory.getClientFailureCheckPeriod(), 2);
      assertEquals(factory.getClientID(), "myid");
      assertEquals(factory.getConnectionLoadBalancingPolicyClassName(), "mlbcn");
      assertEquals(factory.getConnectionTTL(), 3);
      assertEquals(factory.getConsumerMaxRate(), 4);
      assertEquals(factory.getConsumerWindowSize(), 5);
      assertEquals(factory.getDiscoveryAddress(), null);
      assertEquals(factory.getDiscoveryInitialWaitTimeout(), 6);
      assertEquals(factory.getDiscoveryPort(), 0);
      assertEquals(factory.getDiscoveryRefreshTimeout(), 7);
      assertEquals(factory.getDupsOKBatchSize(), 8);      
      assertEquals(factory.getMinLargeMessageSize(), 10);
      assertEquals(factory.getProducerMaxRate(), 11);
      assertEquals(factory.getProducerWindowSize(), 12);
      assertEquals(factory.getReconnectAttempts(), 13);
      assertEquals(factory.getRetryInterval(), 14);
      assertEquals(factory.getRetryIntervalMultiplier(), 15d);
      assertEquals(factory.getScheduledThreadPoolMaxSize(), 16);
      assertEquals(factory.getThreadPoolMaxSize(), 17);
      assertEquals(factory.getTransactionBatchSize(), 18);
      assertEquals(factory.isAutoGroup(), !ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP);
      assertEquals(factory.isBlockOnAcknowledge(), !ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE);
      assertEquals(factory.isBlockOnNonPersistentSend(), !ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND);
      assertEquals(factory.isBlockOnPersistentSend(), !ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_PERSISTENT_SEND);
      assertEquals(factory.isFailoverOnServerShutdown(), !ClientSessionFactoryImpl.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN);
      assertEquals(factory.isPreAcknowledge(), !ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE);
      assertEquals(factory.isUseGlobalPools(), !ClientSessionFactoryImpl.DEFAULT_USE_GLOBAL_POOLS);
   }

   public void testCreateConnectionFactoryOverrideConnector() throws Exception
   {
      HornetQResourceAdapter ra = new HornetQResourceAdapter();
      ra.setConnectorClassName(InVMConnector.class.getName());
      ConnectionFactoryProperties connectionFactoryProperties = new ConnectionFactoryProperties();
      connectionFactoryProperties.setConnectorClassName(NettyConnector.class.getName());
      HornetQConnectionFactory factory = ra.createHornetQConnectionFactory(connectionFactoryProperties);
      HornetQConnectionFactory defaultFactory = ra.getDefaultHornetQConnectionFactory();
      assertNotSame(factory, defaultFactory);
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
      assertNotSame(factory, defaultFactory);
   }

    public void testCreateConnectionFactoryThrowsException() throws Exception
   {
      HornetQResourceAdapter ra = new HornetQResourceAdapter();
      ConnectionFactoryProperties connectionFactoryProperties = new ConnectionFactoryProperties();
      try
      {
         HornetQConnectionFactory factory = ra.createHornetQConnectionFactory(connectionFactoryProperties);
         fail("should throw exception");
      }
      catch (IllegalArgumentException e)
      {
         //pass
      }
   }

   public void testValidateProperties() throws Exception
   {
      validateGettersAndSetters(new HornetQResourceAdapter(), "backupTransportConfiguration");
      validateGettersAndSetters(new HornetQRAManagedConnectionFactory(), "connectionParameters", "sessionDefaultType", "backupConnectionParameters");
      validateGettersAndSetters(new HornetQActivationSpec(),
                                "connectionParameters",
                                "acknowledgeMode",
                                "subscriptionDurability");

      HornetQActivationSpec spec = new HornetQActivationSpec();

      spec.setAcknowledgeMode("DUPS_OK_ACKNOWLEDGE");
      assertEquals("Dups-ok-acknowledge", spec.getAcknowledgeMode());

      spec.setSubscriptionDurability("Durable");
      assertEquals("Durable", spec.getSubscriptionDurability());

      spec.setSubscriptionDurability("NonDurable");
      assertEquals("NonDurable", spec.getSubscriptionDurability());
      
      
      spec = new HornetQActivationSpec();
      HornetQResourceAdapter adapter = new HornetQResourceAdapter();

      adapter.setUserName("us1");
      adapter.setPassword("ps1");
      adapter.setClientID("cl1");
      
      spec.setResourceAdapter(adapter);
      
      assertEquals("us1", spec.getUser());
      assertEquals("ps1", spec.getPassword());
      
      spec.setUser("us2");
      spec.setPassword("ps2");
      spec.setClientID("cl2");

      
      assertEquals("us2", spec.getUser());
      assertEquals("ps2", spec.getPassword());
      assertEquals("cl2", spec.getClientID());
      
      
   }

   public void testStartActivation() throws Exception
   {
      HornetQServer server = createServer(false);

      try
      {

         server.start();
         
         ClientSessionFactory factory = createInVMFactory();
         ClientSession session = factory.createSession(false, false, false);
         HornetQQueue queue = new HornetQQueue("test");
         session.createQueue(queue.getSimpleAddress(), queue.getSimpleAddress(), true);
         session.close();
         
         HornetQResourceAdapter ra = new HornetQResourceAdapter();

         ra.setConnectorClassName("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory");
         ra.setConnectionParameters("bm.remoting.invm.serverid=0");
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
            public void doWork(Work work) throws WorkException
            {
            }

            public void doWork(Work work, long l, ExecutionContext executionContext, WorkListener workListener) throws WorkException
            {
            }

            public long startWork(Work work) throws WorkException
            {
               return 0;
            }

            public long startWork(Work work, long l, ExecutionContext executionContext, WorkListener workListener) throws WorkException
            {
               return 0;
            }

            public void scheduleWork(Work work) throws WorkException
            {
               work.run();
            }

            public void scheduleWork(Work work, long l, ExecutionContext executionContext, WorkListener workListener) throws WorkException
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
      public MessageEndpoint createEndpoint(XAResource arg0) throws UnavailableException
      {
         return null;
      }

      /* (non-Javadoc)
       * @see javax.resource.spi.endpoint.MessageEndpointFactory#isDeliveryTransacted(java.lang.reflect.Method)
       */
      public boolean isDeliveryTransacted(Method arg0) throws NoSuchMethodException
      {
         return false;
      }
      
   }
}
