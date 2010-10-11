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

package org.hornetq.tests.integration.jms;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.Session;

import junit.framework.Assert;

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.server.impl.JMSFactoryType;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.core.config.BroadcastGroupConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.impl.invm.TransportConstants;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.UnitTestCase;

/**
 * 
 * A HornetQConnectionFactoryTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class HornetQConnectionFactoryTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(HornetQConnectionFactoryTest.class);

   private final String groupAddress = getUDPDiscoveryAddress();

   private final int groupPort = getUDPDiscoveryPort();

   private HornetQServer liveService;

   private HornetQServer backupService;

   private TransportConfiguration liveTC;

   private TransportConfiguration backupTC;

   public void testDefaultConstructor() throws Exception
   {
      HornetQConnectionFactory cf = (HornetQConnectionFactory) HornetQJMSClient.createConnectionFactory();
      assertFactoryParams(cf,
                          null,
                          null,
                          0,
                          HornetQClient.DEFAULT_DISCOVERY_REFRESH_TIMEOUT,
                          null,
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
                          HornetQClient.DEFAULT_ACK_BATCH_SIZE,
                          HornetQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT,
                          HornetQClient.DEFAULT_USE_GLOBAL_POOLS,
                          HornetQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,
                          HornetQClient.DEFAULT_THREAD_POOL_MAX_SIZE,
                          HornetQClient.DEFAULT_RETRY_INTERVAL,
                          HornetQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                          HornetQClient.DEFAULT_RECONNECT_ATTEMPTS,
                          HornetQClient.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN);
      Connection conn = null;

      try
      {
         conn = cf.createConnection();

         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Assert.fail("Should throw exception");
      }
      catch (Exception e)
      {
         e.printStackTrace();
         // Ok
      }
      if (conn != null)
      {
         conn.close();
      }

      HornetQConnectionFactoryTest.log.info("Got here");

      testSettersThrowException(cf);
   }

   public void testDefaultConstructorAndSetConnectorPairs() throws Exception
   {
      HornetQConnectionFactory cf = (HornetQConnectionFactory) HornetQJMSClient.createConnectionFactory();
      final List<Pair<TransportConfiguration, TransportConfiguration>> staticConnectors = new ArrayList<Pair<TransportConfiguration, TransportConfiguration>>();
      Pair<TransportConfiguration, TransportConfiguration> pair0 = new Pair<TransportConfiguration, TransportConfiguration>(liveTC,
                                                                                                                            backupTC);
      staticConnectors.add(pair0);
      cf.setStaticConnectors(staticConnectors);

      assertFactoryParams(cf,
                          staticConnectors,
                          null,
                          0,
                          HornetQClient.DEFAULT_DISCOVERY_REFRESH_TIMEOUT,
                          null,
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
                          HornetQClient.DEFAULT_ACK_BATCH_SIZE,
                          HornetQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT,
                          HornetQClient.DEFAULT_USE_GLOBAL_POOLS,
                          HornetQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,
                          HornetQClient.DEFAULT_THREAD_POOL_MAX_SIZE,
                          HornetQClient.DEFAULT_RETRY_INTERVAL,
                          HornetQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                          HornetQClient.DEFAULT_RECONNECT_ATTEMPTS,
                          HornetQClient.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN);

      Connection conn = cf.createConnection();

      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      testSettersThrowException(cf);

      conn.close();
   }

   public void testDiscoveryConstructor() throws Exception
   {
      HornetQConnectionFactory cf = (HornetQConnectionFactory) HornetQJMSClient.createConnectionFactory(groupAddress, groupPort, JMSFactoryType.CF);
      assertFactoryParams(cf,
                          null,
                          groupAddress,
                          groupPort,
                          HornetQClient.DEFAULT_DISCOVERY_REFRESH_TIMEOUT,
                          null,
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
                          HornetQClient.DEFAULT_ACK_BATCH_SIZE,
                          HornetQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT,
                          HornetQClient.DEFAULT_USE_GLOBAL_POOLS,
                          HornetQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,
                          HornetQClient.DEFAULT_THREAD_POOL_MAX_SIZE,
                          HornetQClient.DEFAULT_RETRY_INTERVAL,
                          HornetQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                          HornetQClient.DEFAULT_RECONNECT_ATTEMPTS,
                          HornetQClient.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN);
      Connection conn = cf.createConnection();

      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      testSettersThrowException(cf);

      conn.close();
   }

   public void testStaticConnectorListConstructor() throws Exception
   {
      final List<Pair<TransportConfiguration, TransportConfiguration>> staticConnectors = new ArrayList<Pair<TransportConfiguration, TransportConfiguration>>();
      Pair<TransportConfiguration, TransportConfiguration> pair0 = new Pair<TransportConfiguration, TransportConfiguration>(liveTC,
                                                                                                                            backupTC);
      staticConnectors.add(pair0);

      HornetQConnectionFactory cf = (HornetQConnectionFactory) HornetQJMSClient.createConnectionFactory(staticConnectors, JMSFactoryType.CF);
      assertFactoryParams(cf,
                          staticConnectors,
                          null,
                          0,
                          HornetQClient.DEFAULT_DISCOVERY_REFRESH_TIMEOUT,
                          null,
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
                          HornetQClient.DEFAULT_ACK_BATCH_SIZE,
                          HornetQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT,
                          HornetQClient.DEFAULT_USE_GLOBAL_POOLS,
                          HornetQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,
                          HornetQClient.DEFAULT_THREAD_POOL_MAX_SIZE,
                          HornetQClient.DEFAULT_RETRY_INTERVAL,
                          HornetQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                          HornetQClient.DEFAULT_RECONNECT_ATTEMPTS,
                          HornetQClient.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN);
      Connection conn = cf.createConnection();

      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      testSettersThrowException(cf);

      conn.close();

   }

   public void testStaticConnectorLiveAndBackupConstructor() throws Exception
   {
      final List<Pair<TransportConfiguration, TransportConfiguration>> staticConnectors = new ArrayList<Pair<TransportConfiguration, TransportConfiguration>>();
      Pair<TransportConfiguration, TransportConfiguration> pair0 = new Pair<TransportConfiguration, TransportConfiguration>(liveTC,
                                                                                                                            backupTC);
      staticConnectors.add(pair0);

      HornetQConnectionFactory cf = (HornetQConnectionFactory) HornetQJMSClient.createConnectionFactory(liveTC, backupTC, JMSFactoryType.CF);
      assertFactoryParams(cf,
                          staticConnectors,
                          null,
                          0,
                          HornetQClient.DEFAULT_DISCOVERY_REFRESH_TIMEOUT,
                          null,
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
                          HornetQClient.DEFAULT_ACK_BATCH_SIZE,
                          HornetQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT,
                          HornetQClient.DEFAULT_USE_GLOBAL_POOLS,
                          HornetQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,
                          HornetQClient.DEFAULT_THREAD_POOL_MAX_SIZE,
                          HornetQClient.DEFAULT_RETRY_INTERVAL,
                          HornetQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                          HornetQClient.DEFAULT_RECONNECT_ATTEMPTS,
                          HornetQClient.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN);
      Connection conn = cf.createConnection();

      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      testSettersThrowException(cf);

      conn.close();

   }

   public void testStaticConnectorLiveConstructor() throws Exception
   {
      final List<Pair<TransportConfiguration, TransportConfiguration>> staticConnectors = new ArrayList<Pair<TransportConfiguration, TransportConfiguration>>();
      Pair<TransportConfiguration, TransportConfiguration> pair0 = new Pair<TransportConfiguration, TransportConfiguration>(liveTC,
                                                                                                                            null);
      staticConnectors.add(pair0);

      HornetQConnectionFactory cf = (HornetQConnectionFactory) HornetQJMSClient.createConnectionFactory(liveTC);
      assertFactoryParams(cf,
                          staticConnectors,
                          null,
                          0,
                          HornetQClient.DEFAULT_DISCOVERY_REFRESH_TIMEOUT,
                          null,
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
                          HornetQClient.DEFAULT_ACK_BATCH_SIZE,
                          HornetQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT,
                          HornetQClient.DEFAULT_USE_GLOBAL_POOLS,
                          HornetQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,
                          HornetQClient.DEFAULT_THREAD_POOL_MAX_SIZE,
                          HornetQClient.DEFAULT_RETRY_INTERVAL,
                          HornetQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                          HornetQClient.DEFAULT_RECONNECT_ATTEMPTS,
                          HornetQClient.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN);
      Connection conn = cf.createConnection();

      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      testSettersThrowException(cf);

      conn.close();
   }

   public void testGettersAndSetters()
   {
      ClientSessionFactory csf = HornetQClient.createClientSessionFactory();

      List<Pair<TransportConfiguration, TransportConfiguration>> staticConnectors = new ArrayList<Pair<TransportConfiguration, TransportConfiguration>>();
      Pair<TransportConfiguration, TransportConfiguration> pair0 = new Pair<TransportConfiguration, TransportConfiguration>(liveTC,
                                                                                                                            backupTC);
      staticConnectors.add(pair0);
      HornetQConnectionFactory cf = (HornetQConnectionFactory) HornetQJMSClient.createConnectionFactory(csf);

      String discoveryAddress = RandomUtil.randomString();
      int discoveryPort = RandomUtil.randomPositiveInt();
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
      long initialWaitTimeout = RandomUtil.randomPositiveLong();
      boolean useGlobalPools = RandomUtil.randomBoolean();
      int scheduledThreadPoolMaxSize = RandomUtil.randomPositiveInt();
      int threadPoolMaxSize = RandomUtil.randomPositiveInt();
      long retryInterval = RandomUtil.randomPositiveLong();
      double retryIntervalMultiplier = RandomUtil.randomDouble();
      int reconnectAttempts = RandomUtil.randomPositiveInt();
      boolean failoverOnServerShutdown = RandomUtil.randomBoolean();

      cf.setStaticConnectors(staticConnectors);
      cf.setDiscoveryAddress(discoveryAddress);
      cf.setDiscoveryPort(discoveryPort);
      cf.setDiscoveryRefreshTimeout(discoveryRefreshTimeout);
      cf.setClientFailureCheckPeriod(clientFailureCheckPeriod);
      cf.setConnectionTTL(connectionTTL);
      cf.setCallTimeout(callTimeout);
      cf.setMinLargeMessageSize(minLargeMessageSize);
      cf.setConsumerWindowSize(consumerWindowSize);
      cf.setConsumerMaxRate(consumerMaxRate);
      cf.setConfirmationWindowSize(confirmationWindowSize);
      cf.setProducerMaxRate(producerMaxRate);
      cf.setBlockOnAcknowledge(blockOnAcknowledge);
      cf.setBlockOnDurableSend(blockOnDurableSend);
      cf.setBlockOnNonDurableSend(blockOnNonDurableSend);
      cf.setAutoGroup(autoGroup);
      cf.setPreAcknowledge(preAcknowledge);
      cf.setConnectionLoadBalancingPolicyClassName(loadBalancingPolicyClassName);
      cf.setDiscoveryInitialWaitTimeout(initialWaitTimeout);
      cf.setUseGlobalPools(useGlobalPools);
      cf.setScheduledThreadPoolMaxSize(scheduledThreadPoolMaxSize);
      cf.setThreadPoolMaxSize(threadPoolMaxSize);
      cf.setRetryInterval(retryInterval);
      cf.setRetryIntervalMultiplier(retryIntervalMultiplier);
      cf.setReconnectAttempts(reconnectAttempts);
      cf.setFailoverOnServerShutdown(failoverOnServerShutdown);

      Assert.assertEquals(staticConnectors, cf.getStaticConnectors());
      Assert.assertEquals(discoveryAddress, cf.getDiscoveryAddress());
      Assert.assertEquals(discoveryPort, cf.getDiscoveryPort());
      Assert.assertEquals(discoveryRefreshTimeout, cf.getDiscoveryRefreshTimeout());
      Assert.assertEquals(clientFailureCheckPeriod, cf.getClientFailureCheckPeriod());
      Assert.assertEquals(connectionTTL, cf.getConnectionTTL());
      Assert.assertEquals(callTimeout, cf.getCallTimeout());
      Assert.assertEquals(minLargeMessageSize, cf.getMinLargeMessageSize());
      Assert.assertEquals(consumerWindowSize, cf.getConsumerWindowSize());
      Assert.assertEquals(consumerMaxRate, cf.getConsumerMaxRate());
      Assert.assertEquals(confirmationWindowSize, cf.getConfirmationWindowSize());
      Assert.assertEquals(producerMaxRate, cf.getProducerMaxRate());
      Assert.assertEquals(blockOnAcknowledge, cf.isBlockOnAcknowledge());
      Assert.assertEquals(blockOnDurableSend, cf.isBlockOnDurableSend());
      Assert.assertEquals(blockOnNonDurableSend, cf.isBlockOnNonDurableSend());
      Assert.assertEquals(autoGroup, cf.isAutoGroup());
      Assert.assertEquals(preAcknowledge, cf.isPreAcknowledge());
      Assert.assertEquals(loadBalancingPolicyClassName, cf.getConnectionLoadBalancingPolicyClassName());
      Assert.assertEquals(initialWaitTimeout, cf.getDiscoveryInitialWaitTimeout());
      Assert.assertEquals(useGlobalPools, cf.isUseGlobalPools());
      Assert.assertEquals(scheduledThreadPoolMaxSize, cf.getScheduledThreadPoolMaxSize());
      Assert.assertEquals(threadPoolMaxSize, cf.getThreadPoolMaxSize());
      Assert.assertEquals(retryInterval, cf.getRetryInterval());
      Assert.assertEquals(retryIntervalMultiplier, cf.getRetryIntervalMultiplier());
      Assert.assertEquals(reconnectAttempts, cf.getReconnectAttempts());
      Assert.assertEquals(failoverOnServerShutdown, cf.isFailoverOnServerShutdown());

      cf.close();
   }

   private void testSettersThrowException(final HornetQConnectionFactory cf)
   {
      List<Pair<TransportConfiguration, TransportConfiguration>> staticConnectors = new ArrayList<Pair<TransportConfiguration, TransportConfiguration>>();
      Pair<TransportConfiguration, TransportConfiguration> pair0 = new Pair<TransportConfiguration, TransportConfiguration>(liveTC,
                                                                                                                            backupTC);
      staticConnectors.add(pair0);

      String discoveryAddress = RandomUtil.randomString();
      int discoveryPort = RandomUtil.randomPositiveInt();
      long discoveryRefreshTimeout = RandomUtil.randomPositiveLong();
      String clientID = RandomUtil.randomString();
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
      int dupsOKBatchSize = RandomUtil.randomPositiveInt();
      int transactionBatchSize = RandomUtil.randomPositiveInt();
      long initialWaitTimeout = RandomUtil.randomPositiveLong();
      boolean useGlobalPools = RandomUtil.randomBoolean();
      int scheduledThreadPoolMaxSize = RandomUtil.randomPositiveInt();
      int threadPoolMaxSize = RandomUtil.randomPositiveInt();
      long retryInterval = RandomUtil.randomPositiveLong();
      double retryIntervalMultiplier = RandomUtil.randomDouble();
      int reconnectAttempts = RandomUtil.randomPositiveInt();
      boolean failoverOnServerShutdown = RandomUtil.randomBoolean();

      try
      {
         cf.setStaticConnectors(staticConnectors);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setDiscoveryAddress(discoveryAddress);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setDiscoveryPort(discoveryPort);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setDiscoveryRefreshTimeout(discoveryRefreshTimeout);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setClientID(clientID);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setClientFailureCheckPeriod(clientFailureCheckPeriod);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setConnectionTTL(connectionTTL);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setCallTimeout(callTimeout);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setMinLargeMessageSize(minLargeMessageSize);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setConsumerWindowSize(consumerWindowSize);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setConsumerMaxRate(consumerMaxRate);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setConfirmationWindowSize(confirmationWindowSize);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setProducerMaxRate(producerMaxRate);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setBlockOnAcknowledge(blockOnAcknowledge);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setBlockOnDurableSend(blockOnDurableSend);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setBlockOnNonDurableSend(blockOnNonDurableSend);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setAutoGroup(autoGroup);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setPreAcknowledge(preAcknowledge);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setConnectionLoadBalancingPolicyClassName(loadBalancingPolicyClassName);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setDupsOKBatchSize(dupsOKBatchSize);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setTransactionBatchSize(transactionBatchSize);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setDiscoveryInitialWaitTimeout(initialWaitTimeout);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setUseGlobalPools(useGlobalPools);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setScheduledThreadPoolMaxSize(scheduledThreadPoolMaxSize);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setThreadPoolMaxSize(threadPoolMaxSize);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setRetryInterval(retryInterval);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setRetryIntervalMultiplier(retryIntervalMultiplier);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setReconnectAttempts(reconnectAttempts);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setFailoverOnServerShutdown(failoverOnServerShutdown);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }

      cf.getStaticConnectors();
      cf.getDiscoveryAddress();
      cf.getDiscoveryPort();
      cf.getDiscoveryRefreshTimeout();
      cf.getClientID();
      cf.getClientFailureCheckPeriod();
      cf.getConnectionTTL();
      cf.getCallTimeout();
      cf.getMinLargeMessageSize();
      cf.getConsumerWindowSize();
      cf.getConsumerMaxRate();
      cf.getConfirmationWindowSize();
      cf.getProducerMaxRate();
      cf.isBlockOnAcknowledge();
      cf.isBlockOnDurableSend();
      cf.isBlockOnNonDurableSend();
      cf.isAutoGroup();
      cf.isPreAcknowledge();
      cf.getConnectionLoadBalancingPolicyClassName();
      cf.getDupsOKBatchSize();
      cf.getTransactionBatchSize();
      cf.getDiscoveryInitialWaitTimeout();
      cf.isUseGlobalPools();
      cf.getScheduledThreadPoolMaxSize();
      cf.getThreadPoolMaxSize();
      cf.getRetryInterval();
      cf.getRetryIntervalMultiplier();
      cf.getReconnectAttempts();
      cf.isFailoverOnServerShutdown();

   }

   private void assertFactoryParams(final HornetQConnectionFactory cf,
                                    final List<Pair<TransportConfiguration, TransportConfiguration>> staticConnectors,
                                    final String discoveryAddress,
                                    final int discoveryPort,
                                    final long discoveryRefreshTimeout,
                                    final String clientID,
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
                                    final int dupsOKBatchSize,
                                    final int transactionBatchSize,
                                    final long initialWaitTimeout,
                                    final boolean useGlobalPools,
                                    final int scheduledThreadPoolMaxSize,
                                    final int threadPoolMaxSize,
                                    final long retryInterval,
                                    final double retryIntervalMultiplier,
                                    final int reconnectAttempts,
                                    final boolean failoverOnServerShutdown)
   {
      List<Pair<TransportConfiguration, TransportConfiguration>> cfStaticConnectors = cf.getStaticConnectors();
      if (staticConnectors == null)
      {
         Assert.assertNull(cfStaticConnectors);
      }
      else
      {
         Assert.assertEquals(staticConnectors.size(), cfStaticConnectors.size());

         for (int i = 0; i < staticConnectors.size(); i++)
         {
            Assert.assertEquals(staticConnectors.get(i), cfStaticConnectors.get(i));
         }
      }
      Assert.assertEquals(cf.getDiscoveryAddress(), discoveryAddress);
      Assert.assertEquals(cf.getDiscoveryPort(), discoveryPort);
      Assert.assertEquals(cf.getDiscoveryRefreshTimeout(), discoveryRefreshTimeout);
      Assert.assertEquals(cf.getClientID(), clientID);
      Assert.assertEquals(cf.getClientFailureCheckPeriod(), clientFailureCheckPeriod);
      Assert.assertEquals(cf.getConnectionTTL(), connectionTTL);
      Assert.assertEquals(cf.getCallTimeout(), callTimeout);
      Assert.assertEquals(cf.getMinLargeMessageSize(), minLargeMessageSize);
      Assert.assertEquals(cf.getConsumerWindowSize(), consumerWindowSize);
      Assert.assertEquals(cf.getConsumerMaxRate(), consumerMaxRate);
      Assert.assertEquals(cf.getConfirmationWindowSize(), confirmationWindowSize);
      Assert.assertEquals(cf.getProducerMaxRate(), producerMaxRate);
      Assert.assertEquals(cf.isBlockOnAcknowledge(), blockOnAcknowledge);
      Assert.assertEquals(cf.isBlockOnDurableSend(), blockOnDurableSend);
      Assert.assertEquals(cf.isBlockOnNonDurableSend(), blockOnNonDurableSend);
      Assert.assertEquals(cf.isAutoGroup(), autoGroup);
      Assert.assertEquals(cf.isPreAcknowledge(), preAcknowledge);
      Assert.assertEquals(cf.getConnectionLoadBalancingPolicyClassName(), loadBalancingPolicyClassName);
      Assert.assertEquals(cf.getDupsOKBatchSize(), dupsOKBatchSize);
      Assert.assertEquals(cf.getTransactionBatchSize(), transactionBatchSize);
      Assert.assertEquals(cf.getDiscoveryInitialWaitTimeout(), initialWaitTimeout);
      Assert.assertEquals(cf.isUseGlobalPools(), useGlobalPools);
      Assert.assertEquals(cf.getScheduledThreadPoolMaxSize(), scheduledThreadPoolMaxSize);
      Assert.assertEquals(cf.getThreadPoolMaxSize(), threadPoolMaxSize);
      Assert.assertEquals(cf.getRetryInterval(), retryInterval);
      Assert.assertEquals(cf.getRetryIntervalMultiplier(), retryIntervalMultiplier);
      Assert.assertEquals(cf.getReconnectAttempts(), reconnectAttempts);
      Assert.assertEquals(cf.isFailoverOnServerShutdown(), failoverOnServerShutdown);
   }

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      startLiveAndBackup();
   }

   @Override
   protected void tearDown() throws Exception
   {
      stopLiveAndBackup();

      liveService = null;

      backupService = null;

      liveTC = null;

      backupTC = null;

      super.tearDown();
   }

   private void stopLiveAndBackup() throws Exception
   {
      if (liveService.isStarted())
      {
         liveService.stop();
      }
      if (backupService.isStarted())
      {
         backupService.stop();
      }
   }

   private void startLiveAndBackup() throws Exception
   {
      Map<String, Object> backupParams = new HashMap<String, Object>();
      Configuration backupConf = new ConfigurationImpl();
      backupConf.setSecurityEnabled(false);
      backupConf.setClustered(true);
      backupParams.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      backupConf.getAcceptorConfigurations()
                .add(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory", backupParams));
      backupConf.setBackup(true);
      backupConf.setSharedStore(true);
      backupService = HornetQServers.newHornetQServer(backupConf, false);
      backupService.start();

      Configuration liveConf = new ConfigurationImpl();
      liveConf.setSecurityEnabled(false);
      liveTC = new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory");
      liveConf.getAcceptorConfigurations()
              .add(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory"));
      Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();
      backupTC = new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory", backupParams);
      connectors.put(backupTC.getName(), backupTC);
      connectors.put(liveTC.getName(), liveTC);
      liveConf.setConnectorConfigurations(connectors);
      liveConf.setSharedStore(true);
      liveConf.setBackupConnectorName(backupTC.getName());
      liveConf.setClustered(true);

      List<Pair<String, String>> connectorNames = new ArrayList<Pair<String, String>>();
      connectorNames.add(new Pair<String, String>(liveTC.getName(), backupTC.getName()));

      final long broadcastPeriod = 250;

      final String bcGroupName = "bc1";

      final int localBindPort = 5432;

      BroadcastGroupConfiguration bcConfig1 = new BroadcastGroupConfiguration(bcGroupName,
                                                                              null,
                                                                              localBindPort,
                                                                              groupAddress,
                                                                              groupPort,
                                                                              broadcastPeriod,
                                                                              connectorNames);

      List<BroadcastGroupConfiguration> bcConfigs1 = new ArrayList<BroadcastGroupConfiguration>();
      bcConfigs1.add(bcConfig1);
      liveConf.setBroadcastGroupConfigurations(bcConfigs1);

      liveService = HornetQServers.newHornetQServer(liveConf, false);
      liveService.start();
   }

}
