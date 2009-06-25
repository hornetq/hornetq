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

package org.jboss.messaging.tests.unit.ra;

import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnector;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.integration.transports.netty.NettyConnector;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.ra.ConnectionFactoryProperties;
import org.jboss.messaging.ra.JBMManagedConnectionFactory;
import org.jboss.messaging.ra.JBMResourceAdapter;
import org.jboss.messaging.ra.inflow.JBMActivation;
import org.jboss.messaging.ra.inflow.JBMActivationSpec;
import org.jboss.messaging.tests.util.ServiceTestBase;

import javax.jms.Connection;
import javax.resource.spi.BootstrapContext;
import javax.resource.spi.UnavailableException;
import javax.resource.spi.XATerminator;
import javax.resource.spi.endpoint.MessageEndpoint;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.resource.spi.work.WorkManager;
import javax.transaction.xa.XAResource;
import java.lang.reflect.Method;
import java.util.Timer;

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
      JBMResourceAdapter ra = new JBMResourceAdapter();
      ra.setConnectorClassName(InVMConnector.class.getName());
      JBossConnectionFactory factory = ra.getDefaultJBossConnectionFactory();
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
      assertEquals(factory.getMaxConnections(), ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS);
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
      JBMResourceAdapter ra = new JBMResourceAdapter();
      ra.setConnectorClassName(InVMConnector.class.getName());
      JBossConnectionFactory factory = ra.getDefaultJBossConnectionFactory();
      JBossConnectionFactory factory2 = ra.getDefaultJBossConnectionFactory();
      assertEquals(factory, factory2);
   }

   public void testCreateConnectionFactoryNoOverrides() throws Exception
   {
      JBMResourceAdapter ra = new JBMResourceAdapter();
      ra.setConnectorClassName(InVMConnector.class.getName());
      JBossConnectionFactory factory = ra.createJBossConnectionFactory(new ConnectionFactoryProperties());
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
      assertEquals(factory.getMaxConnections(), ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS);
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
      JBMResourceAdapter ra = new JBMResourceAdapter();
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
      ra.setMaxConnections(9);
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
      JBossConnectionFactory factory = ra.getDefaultJBossConnectionFactory();
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
      assertEquals(factory.getMaxConnections(), 9);
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
      JBMResourceAdapter ra = new JBMResourceAdapter();
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
      connectionFactoryProperties.setMaxConnections(9);
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
      JBossConnectionFactory factory = ra.createJBossConnectionFactory(connectionFactoryProperties);
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
      assertEquals(factory.getMaxConnections(), 9);
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
      JBMResourceAdapter ra = new JBMResourceAdapter();
      ra.setConnectorClassName(InVMConnector.class.getName());
      ConnectionFactoryProperties connectionFactoryProperties = new ConnectionFactoryProperties();
      connectionFactoryProperties.setConnectorClassName(NettyConnector.class.getName());
      JBossConnectionFactory factory = ra.createJBossConnectionFactory(connectionFactoryProperties);
      JBossConnectionFactory defaultFactory = ra.getDefaultJBossConnectionFactory();
      assertNotSame(factory, defaultFactory);
   }

   public void testCreateConnectionFactoryOverrideDiscovery() throws Exception
   {
      JBMResourceAdapter ra = new JBMResourceAdapter();
      ra.setConnectorClassName(InVMConnector.class.getName());
      ConnectionFactoryProperties connectionFactoryProperties = new ConnectionFactoryProperties();
      connectionFactoryProperties.setDiscoveryAddress("myhost");
      connectionFactoryProperties.setDiscoveryPort(5678);
      JBossConnectionFactory factory = ra.createJBossConnectionFactory(connectionFactoryProperties);
      JBossConnectionFactory defaultFactory = ra.getDefaultJBossConnectionFactory();
      assertNotSame(factory, defaultFactory);
   }

    public void testCreateConnectionFactoryThrowsException() throws Exception
   {
      JBMResourceAdapter ra = new JBMResourceAdapter();
      ConnectionFactoryProperties connectionFactoryProperties = new ConnectionFactoryProperties();
      try
      {
         JBossConnectionFactory factory = ra.createJBossConnectionFactory(connectionFactoryProperties);
         fail("should throw exception");
      }
      catch (IllegalArgumentException e)
      {
         //pass
      }
   }

   public void testValidateProperties() throws Exception
   {
      validateGettersAndSetters(new JBMResourceAdapter(), "backupTransportConfiguration");
      validateGettersAndSetters(new JBMManagedConnectionFactory(), "connectionParameters", "sessionDefaultType", "backupConnectionParameters");
      validateGettersAndSetters(new JBMActivationSpec(),
                                "connectionParameters",
                                "acknowledgeMode",
                                "subscriptionDurability");

      JBMActivationSpec spec = new JBMActivationSpec();

      spec.setAcknowledgeMode("DUPS_OK_ACKNOWLEDGE");
      spec.setSessionTransacted(false);
      assertEquals("Dups-ok-acknowledge", spec.getAcknowledgeMode());

      spec.setSessionTransacted(true);

      assertEquals("Transacted", spec.getAcknowledgeMode());

      spec.setSubscriptionDurability("Durable");
      assertEquals("Durable", spec.getSubscriptionDurability());

      spec.setSubscriptionDurability("NonDurable");
      assertEquals("NonDurable", spec.getSubscriptionDurability());
      
      
      spec = new JBMActivationSpec();
      JBMResourceAdapter adapter = new JBMResourceAdapter();

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
      MessagingServer server = createServer(false);

      try
      {

         server.start();
         
         ClientSessionFactory factory = createInVMFactory();
         ClientSession session = factory.createSession(false, false, false);
         JBossQueue queue = new JBossQueue("test");
         session.createQueue(queue.getSimpleAddress(), queue.getSimpleAddress(), true);
         session.close();
         
         JBMResourceAdapter ra = new JBMResourceAdapter();
         ra.setConnectorClassName("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory");
         ra.setConnectionParameters("bm.remoting.invm.serverid=0");
         ra.setUseXA(true);
         ra.setUserName("userGlobal");
         ra.setPassword("passwordGlobal");
         ra.start(fakeCTX);

         Connection conn = ra.getDefaultJBossConnectionFactory().createConnection();
         
         conn.close();
         
         JBMActivationSpec spec = new JBMActivationSpec();
         
         spec.setResourceAdapter(ra);
         
         spec.setUseJNDI(false);
         
         spec.setUser("user");
         spec.setPassword("password");
         
         spec.setDestinationType("Topic");
         spec.setDestination("test");

         spec.setMinSession(1);
         spec.setMaxSession(1);
         
         JBMActivation activation = new JBMActivation(ra, new FakeMessageEndpointFactory(), spec);
         
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

   class MockJBMResourceAdapter extends JBMResourceAdapter
   {
      /*public JBossConnectionFactory createRemoteFactory(String connectorClassName,
                                                        Map<String, Object> connectionParameters)
      {
         JBossConnectionFactory factory = super.createJBossConnectionFactory(connectionParameters);

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
         return null;
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
