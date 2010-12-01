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
package org.hornetq.tests.integration.client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.BroadcastGroupConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * 
 * A ClientSessionFactoryTest
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class SessionFactoryTest extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(SessionFactoryTest.class);

   private final String groupAddress = getUDPDiscoveryAddress();

   private final int groupPort = getUDPDiscoveryPort();

   private HornetQServer liveService;

   private TransportConfiguration liveTC;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      startServer();
   }

   @Override
   protected void tearDown() throws Exception
   {
      if (liveService != null)
      {
         liveService.stop();
      }

      liveService = null;
      liveTC = null;

      super.tearDown();
   }

   public void testSerializable() throws Exception
   {
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(InVMConnectorFactory.class.getName()));

      ByteArrayOutputStream baos = new ByteArrayOutputStream();

      ObjectOutputStream oos = new ObjectOutputStream(baos);

      oos.writeObject(locator);

      oos.close();

      byte[] bytes = baos.toByteArray();

      ByteArrayInputStream bais = new ByteArrayInputStream(bytes);

      ObjectInputStream ois = new ObjectInputStream(bais);

      ServerLocator csi = (ServerLocator)ois.readObject();

      Assert.assertNotNull(csi);
   }

   public void testCloseUnusedClientSessionFactoryWithoutGlobalPools() throws Exception
   {
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(liveTC);

      ClientSessionFactory csf = locator.createSessionFactory();
      csf.close();
   }

   public void testDiscoveryConstructor() throws Exception
   {
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(groupAddress, groupPort);

      assertFactoryParams(locator,
                          null,
                          groupAddress,
                          groupPort,
                          0,
                          HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                          HornetQClient.DEFAULT_CONNECTION_TTL,
                          HornetQClient.DEFAULT_CALL_TIMEOUT,
                          HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                          HornetQClient.DEFAULT_CONSUMER_WINDOW_SIZE,
                          HornetQClient.DEFAULT_CONSUMER_MAX_RATE,
                          HornetQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE,
                          HornetQClient.DEFAULT_PRODUCER_MAX_RATE,
                          HornetQClient.DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                          HornetQClient.DEFAULT_BLOCK_ON_DURABLE_SEND,
                          HornetQClient.DEFAULT_BLOCK_ON_NON_DURABLE_SEND,
                          HornetQClient.DEFAULT_AUTO_GROUP,
                          HornetQClient.DEFAULT_PRE_ACKNOWLEDGE,
                          HornetQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                          HornetQClient.DEFAULT_ACK_BATCH_SIZE,
                          HornetQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT,
                          HornetQClient.DEFAULT_USE_GLOBAL_POOLS,
                          HornetQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,
                          HornetQClient.DEFAULT_THREAD_POOL_MAX_SIZE,
                          HornetQClient.DEFAULT_RETRY_INTERVAL,
                          HornetQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                          HornetQClient.DEFAULT_RECONNECT_ATTEMPTS);
      
      ClientSessionFactory cf = locator.createSessionFactory();
      ClientSession session = cf.createSession(false, true, true);
      Assert.assertNotNull(session);
      session.close();
      testSettersThrowException(cf);
      
      cf.close();
   }

   public void testStaticConnectorListConstructor() throws Exception
   {
      TransportConfiguration[] tc = new TransportConfiguration[] { liveTC };
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(tc);

      assertFactoryParams(locator,
                          tc,
                          null,
                          -1,
                          0,
                          HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                          HornetQClient.DEFAULT_CONNECTION_TTL,
                          HornetQClient.DEFAULT_CALL_TIMEOUT,
                          HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                          HornetQClient.DEFAULT_CONSUMER_WINDOW_SIZE,
                          HornetQClient.DEFAULT_CONSUMER_MAX_RATE,
                          HornetQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE,
                          HornetQClient.DEFAULT_PRODUCER_MAX_RATE,
                          HornetQClient.DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                          HornetQClient.DEFAULT_BLOCK_ON_DURABLE_SEND,
                          HornetQClient.DEFAULT_BLOCK_ON_NON_DURABLE_SEND,
                          HornetQClient.DEFAULT_AUTO_GROUP,
                          HornetQClient.DEFAULT_PRE_ACKNOWLEDGE,
                          HornetQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                          HornetQClient.DEFAULT_ACK_BATCH_SIZE,
                          HornetQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT,
                          HornetQClient.DEFAULT_USE_GLOBAL_POOLS,
                          HornetQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,
                          HornetQClient.DEFAULT_THREAD_POOL_MAX_SIZE,
                          HornetQClient.DEFAULT_RETRY_INTERVAL,
                          HornetQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                          HornetQClient.DEFAULT_RECONNECT_ATTEMPTS);
      
      ClientSessionFactory cf = locator.createSessionFactory();
      ClientSession session = cf.createSession(false, true, true);
      Assert.assertNotNull(session);
      session.close();
      testSettersThrowException(cf);
      
      cf.close();
   }

   public void testGettersAndSetters() throws Exception
   {

      TransportConfiguration[] tc = new TransportConfiguration[] { liveTC };
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(tc);

      long discoveryRefreshTimeout = RandomUtil.randomPositiveLong();
      long clientFailureCheckPeriod = RandomUtil.randomPositiveLong();
      long connectionTTL = RandomUtil.randomPositiveLong();
      long callTimeout = RandomUtil.randomPositiveLong();
      int minLargeMessageSize = RandomUtil.randomPositiveInt();
      int consumerWindowSize = RandomUtil.randomPositiveInt();
      int consumerMaxRate = RandomUtil.randomPositiveInt();
      int confirmationWindowSize = RandomUtil.randomPositiveInt();
      int producerMaxRate = RandomUtil.randomPositiveInt();
      boolean blockOnAcknowledge = RandomUtil.randomBoolean();
      boolean blockOnDurableSend = RandomUtil.randomBoolean();
      boolean blockOnNonDurableSend = RandomUtil.randomBoolean();
      boolean autoGroup = RandomUtil.randomBoolean();
      boolean preAcknowledge = RandomUtil.randomBoolean();
      String loadBalancingPolicyClassName = RandomUtil.randomString();
      int ackBatchSize = RandomUtil.randomPositiveInt();
      long initialWaitTimeout = RandomUtil.randomPositiveLong();
      boolean useGlobalPools = RandomUtil.randomBoolean();
      int scheduledThreadPoolMaxSize = RandomUtil.randomPositiveInt();
      int threadPoolMaxSize = RandomUtil.randomPositiveInt();
      long retryInterval = RandomUtil.randomPositiveLong();
      double retryIntervalMultiplier = RandomUtil.randomDouble();
      int reconnectAttempts = RandomUtil.randomPositiveInt();

      locator.setDiscoveryRefreshTimeout(discoveryRefreshTimeout);
      locator.setClientFailureCheckPeriod(clientFailureCheckPeriod);
      locator.setConnectionTTL(connectionTTL);
      locator.setCallTimeout(callTimeout);
      locator.setMinLargeMessageSize(minLargeMessageSize);
      locator.setConsumerWindowSize(consumerWindowSize);
      locator.setConsumerMaxRate(consumerMaxRate);
      locator.setConfirmationWindowSize(confirmationWindowSize);
      locator.setProducerMaxRate(producerMaxRate);
      locator.setBlockOnAcknowledge(blockOnAcknowledge);
      locator.setBlockOnDurableSend(blockOnDurableSend);
      locator.setBlockOnNonDurableSend(blockOnNonDurableSend);
      locator.setAutoGroup(autoGroup);
      locator.setPreAcknowledge(preAcknowledge);
      locator.setConnectionLoadBalancingPolicyClassName(loadBalancingPolicyClassName);
      locator.setAckBatchSize(ackBatchSize);
      locator.setDiscoveryInitialWaitTimeout(initialWaitTimeout);
      locator.setUseGlobalPools(useGlobalPools);
      locator.setScheduledThreadPoolMaxSize(scheduledThreadPoolMaxSize);
      locator.setThreadPoolMaxSize(threadPoolMaxSize);
      locator.setRetryInterval(retryInterval);
      locator.setRetryIntervalMultiplier(retryIntervalMultiplier);
      locator.setReconnectAttempts(reconnectAttempts);

      assertEqualsTransportConfigurations(tc, locator.getStaticTransportConfigurations());
      Assert.assertEquals(null, locator.getDiscoveryAddress());
      Assert.assertEquals(-1, locator.getDiscoveryPort());
      Assert.assertEquals(discoveryRefreshTimeout, locator.getDiscoveryRefreshTimeout());
      Assert.assertEquals(clientFailureCheckPeriod, locator.getClientFailureCheckPeriod());
      Assert.assertEquals(connectionTTL, locator.getConnectionTTL());
      Assert.assertEquals(callTimeout, locator.getCallTimeout());
      Assert.assertEquals(minLargeMessageSize, locator.getMinLargeMessageSize());
      Assert.assertEquals(consumerWindowSize, locator.getConsumerWindowSize());
      Assert.assertEquals(consumerMaxRate, locator.getConsumerMaxRate());
      Assert.assertEquals(confirmationWindowSize, locator.getConfirmationWindowSize());
      Assert.assertEquals(producerMaxRate, locator.getProducerMaxRate());
      Assert.assertEquals(blockOnAcknowledge, locator.isBlockOnAcknowledge());
      Assert.assertEquals(blockOnDurableSend, locator.isBlockOnDurableSend());
      Assert.assertEquals(blockOnNonDurableSend, locator.isBlockOnNonDurableSend());
      Assert.assertEquals(autoGroup, locator.isAutoGroup());
      Assert.assertEquals(preAcknowledge, locator.isPreAcknowledge());
      Assert.assertEquals(loadBalancingPolicyClassName, locator
                                                          .getConnectionLoadBalancingPolicyClassName());
      Assert.assertEquals(ackBatchSize, locator.getAckBatchSize());
      Assert.assertEquals(initialWaitTimeout, locator.getDiscoveryInitialWaitTimeout());
      Assert.assertEquals(useGlobalPools, locator.isUseGlobalPools());
      Assert.assertEquals(scheduledThreadPoolMaxSize, locator.getScheduledThreadPoolMaxSize());
      Assert.assertEquals(threadPoolMaxSize, locator.getThreadPoolMaxSize());
      Assert.assertEquals(retryInterval, locator.getRetryInterval());
      Assert.assertEquals(retryIntervalMultiplier, locator.getRetryIntervalMultiplier());
      Assert.assertEquals(reconnectAttempts, locator.getReconnectAttempts());

   }

   private void testSettersThrowException(final ClientSessionFactory cf)
   {
      long discoveryRefreshTimeout = RandomUtil.randomPositiveLong();
      long clientFailureCheckPeriod = RandomUtil.randomPositiveLong();
      long connectionTTL = RandomUtil.randomPositiveLong();
      long callTimeout = RandomUtil.randomPositiveLong();
      int minLargeMessageSize = RandomUtil.randomPositiveInt();
      int consumerWindowSize = RandomUtil.randomPositiveInt();
      int consumerMaxRate = RandomUtil.randomPositiveInt();
      int confirmationWindowSize = RandomUtil.randomPositiveInt();
      int producerMaxRate = RandomUtil.randomPositiveInt();
      boolean blockOnAcknowledge = RandomUtil.randomBoolean();
      boolean blockOnDurableSend = RandomUtil.randomBoolean();
      boolean blockOnNonDurableSend = RandomUtil.randomBoolean();
      boolean autoGroup = RandomUtil.randomBoolean();
      boolean preAcknowledge = RandomUtil.randomBoolean();
      String loadBalancingPolicyClassName = RandomUtil.randomString();
      int ackBatchSize = RandomUtil.randomPositiveInt();
      long initialWaitTimeout = RandomUtil.randomPositiveLong();
      boolean useGlobalPools = RandomUtil.randomBoolean();
      int scheduledThreadPoolMaxSize = RandomUtil.randomPositiveInt();
      int threadPoolMaxSize = RandomUtil.randomPositiveInt();
      long retryInterval = RandomUtil.randomPositiveLong();
      double retryIntervalMultiplier = RandomUtil.randomDouble();
      int reconnectAttempts = RandomUtil.randomPositiveInt();

      try
      {
         cf.getServerLocator().setDiscoveryRefreshTimeout(discoveryRefreshTimeout);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.getServerLocator().setClientFailureCheckPeriod(clientFailureCheckPeriod);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.getServerLocator().setConnectionTTL(connectionTTL);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.getServerLocator().setCallTimeout(callTimeout);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.getServerLocator().setMinLargeMessageSize(minLargeMessageSize);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.getServerLocator().setConsumerWindowSize(consumerWindowSize);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.getServerLocator().setConsumerMaxRate(consumerMaxRate);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.getServerLocator().setConfirmationWindowSize(confirmationWindowSize);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.getServerLocator().setProducerMaxRate(producerMaxRate);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.getServerLocator().setBlockOnAcknowledge(blockOnAcknowledge);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.getServerLocator().setBlockOnDurableSend(blockOnDurableSend);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.getServerLocator().setBlockOnNonDurableSend(blockOnNonDurableSend);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.getServerLocator().setAutoGroup(autoGroup);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.getServerLocator().setPreAcknowledge(preAcknowledge);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.getServerLocator().setConnectionLoadBalancingPolicyClassName(loadBalancingPolicyClassName);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.getServerLocator().setAckBatchSize(ackBatchSize);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.getServerLocator().setDiscoveryInitialWaitTimeout(initialWaitTimeout);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.getServerLocator().setUseGlobalPools(useGlobalPools);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.getServerLocator().setScheduledThreadPoolMaxSize(scheduledThreadPoolMaxSize);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.getServerLocator().setThreadPoolMaxSize(threadPoolMaxSize);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.getServerLocator().setRetryInterval(retryInterval);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.getServerLocator().setRetryIntervalMultiplier(retryIntervalMultiplier);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.getServerLocator().setReconnectAttempts(reconnectAttempts);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }

      cf.getServerLocator().getStaticTransportConfigurations();
      cf.getServerLocator().getDiscoveryAddress();
      cf.getServerLocator().getDiscoveryPort();
      cf.getServerLocator().getDiscoveryRefreshTimeout();
      cf.getServerLocator().getClientFailureCheckPeriod();
      cf.getServerLocator().getConnectionTTL();
      cf.getServerLocator().getCallTimeout();
      cf.getServerLocator().getMinLargeMessageSize();
      cf.getServerLocator().getConsumerWindowSize();
      cf.getServerLocator().getConsumerMaxRate();
      cf.getServerLocator().getConfirmationWindowSize();
      cf.getServerLocator().getProducerMaxRate();
      cf.getServerLocator().isBlockOnAcknowledge();
      cf.getServerLocator().isBlockOnDurableSend();
      cf.getServerLocator().isBlockOnNonDurableSend();
      cf.getServerLocator().isAutoGroup();
      cf.getServerLocator().isPreAcknowledge();
      cf.getServerLocator().getConnectionLoadBalancingPolicyClassName();
      cf.getServerLocator().getAckBatchSize();
      cf.getServerLocator().getDiscoveryInitialWaitTimeout();
      cf.getServerLocator().isUseGlobalPools();
      cf.getServerLocator().getScheduledThreadPoolMaxSize();
      cf.getServerLocator().getThreadPoolMaxSize();
      cf.getServerLocator().getRetryInterval();
      cf.getServerLocator().getRetryIntervalMultiplier();
      cf.getServerLocator().getReconnectAttempts();

   }

   private void assertFactoryParams(final ServerLocator locator,
                                    final TransportConfiguration[] staticConnectors,
                                    final String discoveryAddress,
                                    final int discoveryPort,
                                    final long discoveryRefreshTimeout,
                                    final long clientFailureCheckPeriod,
                                    final long connectionTTL,
                                    final long callTimeout,
                                    final int minLargeMessageSize,
                                    final int consumerWindowSize,
                                    final int consumerMaxRate,
                                    final int confirmationWindowSize,
                                    final int producerMaxRate,
                                    final boolean blockOnAcknowledge,
                                    final boolean blockOnDurableSend,
                                    final boolean blockOnNonDurableSend,
                                    final boolean autoGroup,
                                    final boolean preAcknowledge,
                                    final String loadBalancingPolicyClassName,
                                    final int ackBatchSize,
                                    final long initialWaitTimeout,
                                    final boolean useGlobalPools,
                                    final int scheduledThreadPoolMaxSize,
                                    final int threadPoolMaxSize,
                                    final long retryInterval,
                                    final double retryIntervalMultiplier,
                                    final int reconnectAttempts)
   {
      if (staticConnectors == null)
      {
         Assert.assertNull(locator.getStaticTransportConfigurations());
      }
      else
      {
         assertEqualsTransportConfigurations(staticConnectors, locator.getStaticTransportConfigurations());
      }
      Assert.assertEquals(locator.getDiscoveryAddress(), discoveryAddress);
      Assert.assertEquals(locator.getDiscoveryPort(), discoveryPort);
      Assert.assertEquals(locator.getDiscoveryRefreshTimeout(), discoveryRefreshTimeout);
      Assert.assertEquals(locator.getClientFailureCheckPeriod(), clientFailureCheckPeriod);
      Assert.assertEquals(locator.getConnectionTTL(), connectionTTL);
      Assert.assertEquals(locator.getCallTimeout(), callTimeout);
      Assert.assertEquals(locator.getMinLargeMessageSize(), minLargeMessageSize);
      Assert.assertEquals(locator.getConsumerWindowSize(), consumerWindowSize);
      Assert.assertEquals(locator.getConsumerMaxRate(), consumerMaxRate);
      Assert.assertEquals(locator.getConfirmationWindowSize(), confirmationWindowSize);
      Assert.assertEquals(locator.getProducerMaxRate(), producerMaxRate);
      Assert.assertEquals(locator.isBlockOnAcknowledge(), blockOnAcknowledge);
      Assert.assertEquals(locator.isBlockOnDurableSend(), blockOnDurableSend);
      Assert.assertEquals(locator.isBlockOnNonDurableSend(), blockOnNonDurableSend);
      Assert.assertEquals(locator.isAutoGroup(), autoGroup);
      Assert.assertEquals(locator.isPreAcknowledge(), preAcknowledge);
      Assert.assertEquals(locator.getConnectionLoadBalancingPolicyClassName(),
                          loadBalancingPolicyClassName);
      Assert.assertEquals(locator.getAckBatchSize(), ackBatchSize);
      Assert.assertEquals(locator.getDiscoveryInitialWaitTimeout(), initialWaitTimeout);
      Assert.assertEquals(locator.isUseGlobalPools(), useGlobalPools);
      Assert.assertEquals(locator.getScheduledThreadPoolMaxSize(), scheduledThreadPoolMaxSize);
      Assert.assertEquals(locator.getThreadPoolMaxSize(), threadPoolMaxSize);
      Assert.assertEquals(locator.getRetryInterval(), retryInterval);
      Assert.assertEquals(locator.getRetryIntervalMultiplier(), retryIntervalMultiplier);
      Assert.assertEquals(locator.getReconnectAttempts(), reconnectAttempts);
   }

   private void startServer() throws Exception
   {
      Configuration liveConf = new ConfigurationImpl();
      liveConf.setSecurityEnabled(false);
      liveTC = new TransportConfiguration(InVMConnectorFactory.class.getName());
      liveConf.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      liveConf.getConnectorConfigurations().put(liveTC.getName(), liveTC);
      liveConf.setSharedStore(true);
      liveConf.setClustered(true);

      final long broadcastPeriod = 250;

      final String bcGroupName = "bc1";

      final int localBindPort = 5432;

      BroadcastGroupConfiguration bcConfig1 = new BroadcastGroupConfiguration(bcGroupName,
                                                                              null,
                                                                              localBindPort,
                                                                              groupAddress,
                                                                              groupPort,
                                                                              broadcastPeriod,
                                                                              Arrays.asList(liveTC.getName()));

      List<BroadcastGroupConfiguration> bcConfigs1 = new ArrayList<BroadcastGroupConfiguration>();
      bcConfigs1.add(bcConfig1);
      liveConf.setBroadcastGroupConfigurations(bcConfigs1);

      liveService = createServer(false, liveConf);
      liveService.start();
   }
}
