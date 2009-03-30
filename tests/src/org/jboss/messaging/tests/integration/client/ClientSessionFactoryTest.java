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
package org.jboss.messaging.tests.integration.client;

import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryInternal;
import org.jboss.messaging.core.client.impl.ClientSessionImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.cluster.BroadcastGroupConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.remoting.impl.invm.TransportConstants;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.integration.transports.netty.NettyConnectorFactory;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.utils.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ClientSessionFactoryTest extends ServiceTestBase
{
   private final String groupAddress = "230.1.2.3";

   private final int groupPort = 8765;

   private MessagingServer liveService;

   private MessagingServer backupService;

   private TransportConfiguration liveTC;

   private TransportConfiguration backupTC;

   public void testConstructor1() throws Exception
   {
      try
      {
         startLiveAndBackup();
         ClientSessionFactory cf = new ClientSessionFactoryImpl(groupAddress, groupPort);
         assertFactoryParams(cf,
                             ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE,
                             ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT,
                             ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE,
                             ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE,
                             ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS,
                             ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                             ClientSessionFactoryImpl.DEFAULT_PING_PERIOD,
                             ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE,
                             ClientSessionFactoryImpl.DEFAULT_SEND_WINDOW_SIZE,
                             ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP,
                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_PERSISTENT_SEND,
                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND,
                             ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE);
         ClientSessionImpl session = (ClientSessionImpl) cf.createSession(false, true, true);
         assertNotNull(session);
         session.close();
      }
      finally
      {
         stopLiveAndBackup();
      }
   }


   public void testConstructor2() throws Exception
   {
      try
      {
         startLiveAndBackup();
         ClientSessionFactory cf = new ClientSessionFactoryImpl(groupAddress, groupPort, 9999, 5555);
         assertFactoryParams(cf,
                             ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE,
                             ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT,
                             ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE,
                             ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE,
                             ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS,
                             ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                             ClientSessionFactoryImpl.DEFAULT_PING_PERIOD,
                             ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE,
                             ClientSessionFactoryImpl.DEFAULT_SEND_WINDOW_SIZE,
                             ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP,
                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_PERSISTENT_SEND,
                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND,
                             ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE);
         ClientSessionImpl session = (ClientSessionImpl) cf.createSession(false, true, true);
         assertNotNull(session);
         session.close();
      }
      finally
      {
         stopLiveAndBackup();
      }
   }

   public void testConstructor3() throws Exception
   {
      try
      {
         startLiveAndBackup();
         int batchSize = 33;
         long interval = 44l;
         double intervalMultiplier = 6.0;
         int reconnectAttempts = 8;
         boolean failoverOnServerShutdown = true;
         int maxConnections = 2;
         int minLargeMessageSize = 101;
         int producerMaxRate = 99;
         int windowSize = 88;
         int consumerMaxRate = 77;
         int windowSize1 = 66;
         long callTimeout = 55l;
         long ttl = 44l;
         long period = 33l;
         boolean onAcknowledge = true;
         boolean blockOnNonPersistentSend = true;
         boolean blockOnPersistentSend = true;
         boolean autoGroup = true;
         boolean preAcknowledge = true;
         ClientSessionFactory cf = new ClientSessionFactoryImpl(groupAddress, groupPort, 999, 555, org.jboss.messaging.core.client.impl.RandomConnectionLoadBalancingPolicy.class.getName(),
                                                                period, ttl, callTimeout, windowSize1,
                                                                consumerMaxRate, windowSize, producerMaxRate, minLargeMessageSize,
                                                                onAcknowledge, blockOnNonPersistentSend, blockOnPersistentSend, autoGroup, maxConnections, preAcknowledge, batchSize, interval,
                                                                intervalMultiplier, reconnectAttempts, failoverOnServerShutdown);
         assertFactoryParams(cf,
                             batchSize,
                             callTimeout,
                             consumerMaxRate,
                             windowSize1,
                             maxConnections,
                             minLargeMessageSize,
                             period,
                             producerMaxRate,
                             windowSize,
                             autoGroup,
                             onAcknowledge,
                             blockOnPersistentSend,
                             blockOnNonPersistentSend,
                             preAcknowledge);
         ClientSessionImpl session = (ClientSessionImpl) cf.createSession(false, true, true);
         assertNotNull(session);
         session.close();
      }
      finally
      {
         stopLiveAndBackup();
      }
   }

   public void testConstructor4() throws Exception
   {
      int batchSize = 33;
      long interval = 44l;
      double intervalMultiplier = 6.0;
      int reconnectAttempts = 8;
      boolean failoverOnServerShutdown = true;
      int maxConnections = 2;
      int minLargeMessageSize = 101;
      int producerMaxRate = 99;
      int windowSize = 88;
      int consumerMaxRate = 77;
      int windowSize1 = 66;
      long callTimeout = 55l;
      long ttl = 44l;
      long period = 33l;
      boolean onAcknowledge = true;
      boolean blockOnNonPersistentSend = true;
      boolean blockOnPersistentSend = true;
      boolean autoGroup = true;
      boolean preAcknowledge = true;
      ArrayList<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs = new ArrayList<Pair<TransportConfiguration, TransportConfiguration>>();
      try
      {
         startLiveAndBackup();
         connectorConfigs.add(new Pair<TransportConfiguration, TransportConfiguration>(liveTC, backupTC));
         ClientSessionFactory cf = new ClientSessionFactoryImpl(connectorConfigs, org.jboss.messaging.core.client.impl.RandomConnectionLoadBalancingPolicy.class.getName(),
                                                                period, ttl, callTimeout, windowSize1,
                                                                consumerMaxRate, windowSize, producerMaxRate, minLargeMessageSize,
                                                                onAcknowledge, blockOnNonPersistentSend, blockOnPersistentSend, autoGroup, maxConnections, preAcknowledge, batchSize, interval,
                                                                intervalMultiplier, reconnectAttempts, failoverOnServerShutdown);
         assertFactoryParams(cf,
                             batchSize,
                             callTimeout,
                             consumerMaxRate,
                             windowSize1,
                             maxConnections,
                             minLargeMessageSize,
                             period,
                             producerMaxRate,
                             windowSize,
                             autoGroup,
                             onAcknowledge,
                             blockOnPersistentSend,
                             blockOnNonPersistentSend,
                             preAcknowledge);
         ClientSessionImpl session = (ClientSessionImpl) cf.createSession(false, true, true);
         assertNotNull(session);
         session.close();
      }
      finally
      {
         stopLiveAndBackup();
      }
   }

   public void testConstructor5() throws Exception
   {
      try
      {
         startLiveAndBackup();
         long interval = 66l;
         double intervalMultiplier = 7.0;
         int reconnectAttempts = 44;
         boolean failoverOnServerShutdown = true;
         ClientSessionFactory cf = new ClientSessionFactoryImpl(liveTC, backupTC, failoverOnServerShutdown, interval, intervalMultiplier, reconnectAttempts);
         assertFactoryParams(cf,
                             ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE,
                             ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT,
                             ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE,
                             ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE,
                             ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS,
                             ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                             ClientSessionFactoryImpl.DEFAULT_PING_PERIOD,
                             ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE,
                             ClientSessionFactoryImpl.DEFAULT_SEND_WINDOW_SIZE,
                             ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP,
                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_PERSISTENT_SEND,
                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND,
                             ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE);
         ClientSessionImpl session = (ClientSessionImpl) cf.createSession(false, true, true);
         assertNotNull(session);
         session.close();
      }
      finally
      {
         stopLiveAndBackup();
      }
   }

   public void testConstructor6() throws Exception
   {
      try
      {
         startLiveAndBackup();
         long interval = 66l;
         double intervalMultiplier = 7.0;
         int reconnectAttempts = 44;
         ClientSessionFactory cf = new ClientSessionFactoryImpl(liveTC, interval, intervalMultiplier, reconnectAttempts);
         assertFactoryParams(cf,
                             ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE,
                             ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT,
                             ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE,
                             ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE,
                             ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS,
                             ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                             ClientSessionFactoryImpl.DEFAULT_PING_PERIOD,
                             ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE,
                             ClientSessionFactoryImpl.DEFAULT_SEND_WINDOW_SIZE,
                             ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP,
                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_PERSISTENT_SEND,
                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND,
                             ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE);
         ClientSessionImpl session = (ClientSessionImpl) cf.createSession(false, true, true);
         assertNotNull(session);
         session.close();
      }
      finally
      {
         stopLiveAndBackup();
      }
   }

   public void testConstructor7() throws Exception
   {
      try
      {
         startLiveAndBackup();
         ClientSessionFactory cf = new ClientSessionFactoryImpl(liveTC, backupTC);
         assertFactoryParams(cf,
                             ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE,
                             ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT,
                             ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE,
                             ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE,
                             ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS,
                             ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                             ClientSessionFactoryImpl.DEFAULT_PING_PERIOD,
                             ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE,
                             ClientSessionFactoryImpl.DEFAULT_SEND_WINDOW_SIZE,
                             ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP,
                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_PERSISTENT_SEND,
                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND,
                             ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE);
         ClientSessionImpl session = (ClientSessionImpl) cf.createSession(false, true, true);
         assertNotNull(session);
         session.close();
      }
      finally
      {
         stopLiveAndBackup();
      }
   }

   public void testConstructor8() throws Exception
   {
      try
      {
         int batchSize = 33;
         long interval = 44l;
         double intervalMultiplier = 6.0;
         int reconnectAttempts = 8;
         int maxConnections = 2;
         int minLargeMessageSize = 101;
         int producerMaxRate = 99;
         int windowSize = 88;
         int consumerMaxRate = 77;
         int windowSize1 = 66;
         long callTimeout = 55l;
         long ttl = 44l;
         long period = 33l;
         boolean onAcknowledge = true;
         boolean blockOnNonPersistentSend = true;
         boolean blockOnPersistentSend = true;
         boolean autoGroup = true;
         boolean preAcknowledge = true;
         boolean failoverOnServerShutdown = true;
         
         startLiveAndBackup();
         ClientSessionFactory cf = new ClientSessionFactoryImpl(liveTC, backupTC,
                                                                failoverOnServerShutdown,
                                                                org.jboss.messaging.core.client.impl.RandomConnectionLoadBalancingPolicy.class.getName(),
                                                                period, ttl, callTimeout, windowSize1,
                                                                consumerMaxRate, windowSize, producerMaxRate, minLargeMessageSize,
                                                                onAcknowledge, blockOnNonPersistentSend, blockOnPersistentSend, autoGroup, maxConnections, preAcknowledge, batchSize, interval,
                                                                intervalMultiplier, reconnectAttempts);

         assertFactoryParams(cf,
                             batchSize,
                             callTimeout,
                             consumerMaxRate,
                             windowSize1,
                             maxConnections,
                             minLargeMessageSize,
                             period,
                             producerMaxRate,
                             windowSize,
                             autoGroup,
                             onAcknowledge,
                             blockOnPersistentSend,
                             blockOnNonPersistentSend,
                             preAcknowledge);
         ClientSessionImpl session = (ClientSessionImpl) cf.createSession(false, true, true);
         assertNotNull(session);
         session.close();
      }
      finally
      {
         stopLiveAndBackup();
      }
   }

   public void testConstructor9() throws Exception
   {
      try
      {
         startLiveAndBackup();
         ClientSessionFactory cf = new ClientSessionFactoryImpl(liveTC);
         assertFactoryParams(cf,
                             ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE,
                             ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT,
                             ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE,
                             ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE,
                             ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS,
                             ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                             ClientSessionFactoryImpl.DEFAULT_PING_PERIOD,
                             ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE,
                             ClientSessionFactoryImpl.DEFAULT_SEND_WINDOW_SIZE,
                             ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP,
                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_PERSISTENT_SEND,
                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND,
                             ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE);
         ClientSessionImpl session = (ClientSessionImpl) cf.createSession(false, true, true);
         assertNotNull(session);
         session.close();
      }
      finally
      {
         stopLiveAndBackup();
      }
   }

   public void testNumSessionsNumConnections() throws Exception
   {
      try
      {
         startLiveAndBackup();
         ClientSessionFactoryInternal cf = new ClientSessionFactoryImpl(liveTC);

         assertFactoryParams(cf,
                             ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE,
                             ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT,
                             ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE,
                             ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE,
                             ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS,
                             ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                             ClientSessionFactoryImpl.DEFAULT_PING_PERIOD,
                             ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE,
                             ClientSessionFactoryImpl.DEFAULT_SEND_WINDOW_SIZE,
                             ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP,
                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_PERSISTENT_SEND,
                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND,
                             ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE);
         ClientSessionImpl sessions[] = new ClientSessionImpl[ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS * 2];
         for (int i = 0; i < sessions.length; i++)
         {
            sessions[i] = (ClientSessionImpl) cf.createSession(false, true, true);

         }
         assertEquals(ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS * 2, cf.numSessions());
         assertEquals(ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS, cf.numConnections());
         for (ClientSessionImpl session : sessions)
         {
            session.close();
         }
      }
      finally
      {
         stopLiveAndBackup();
      }
   }

   public void testSetters() throws Exception
   {
      int batchSize = 33;
      int minLargeMessageSize = 101;
      int producerMaxRate = 99;
      int windowSize = 88;
      int consumerMaxRate = 77;
      int windowSize1 = 66;
      boolean onAcknowledge = true;
      boolean blockOnNonPersistentSend = true;
      boolean blockOnPersistentSend = true;
      boolean autoGroup = true;
      boolean preAcknowledge = true;
      try
      {
         startLiveAndBackup();
         ClientSessionFactory cf = new ClientSessionFactoryImpl(liveTC);
         assertFactoryParams(cf,
                             ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE,
                             ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT,
                             ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE,
                             ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE,
                             ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS,
                             ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                             ClientSessionFactoryImpl.DEFAULT_PING_PERIOD,
                             ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE,
                             ClientSessionFactoryImpl.DEFAULT_SEND_WINDOW_SIZE,
                             ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP,
                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_PERSISTENT_SEND,
                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND,
                             ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE);
         cf.setAckBatchSize(batchSize);
         cf.setAutoGroup(autoGroup);
         cf.setBlockOnAcknowledge(onAcknowledge);
         cf.setBlockOnNonPersistentSend(blockOnNonPersistentSend);
         cf.setBlockOnPersistentSend(blockOnPersistentSend);
         cf.setConsumerMaxRate(consumerMaxRate);
         cf.setConsumerWindowSize(windowSize1);
         cf.setMinLargeMessageSize(minLargeMessageSize);
         cf.setPreAcknowledge(preAcknowledge);
         cf.setProducerMaxRate(producerMaxRate);
         cf.setSendWindowSize(windowSize);
         assertFactoryParams(cf,
                             batchSize,
                             ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT,
                             consumerMaxRate,
                             windowSize1,
                             ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS,
                             minLargeMessageSize,
                             ClientSessionFactoryImpl.DEFAULT_PING_PERIOD,
                             producerMaxRate,
                             windowSize,
                             autoGroup,
                             onAcknowledge,
                             blockOnPersistentSend,
                             blockOnNonPersistentSend,
                             preAcknowledge);
         ClientSessionImpl session = (ClientSessionImpl) cf.createSession(false, true, true);
         assertNotNull(session);
         session.close();
      }
      finally
      {
         stopLiveAndBackup();
      }
   }

   public void testCreateSessionFailureWithSimpleConstructorWhenNoServer() throws Exception
   {

      /****************************/
      /* No JBM Server is started */
      /****************************/

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(NettyConnectorFactory.class.getName()));

      try
      {
         sf.createSession(false, true, true);
         fail("Can not create a session when there is no running JBM Server");
      }
      catch (Exception e)
      {
      }

   }

   /**
    * The test is commented because it generates an infinite loop.
    * The configuration to have it occured is:
    * - no backup & default values for max retries before/after failover
    *
    * - The infinite loop is in ConnectionManagerImpl.getConnectionForCreateSession()
    *   - getConnection(1) always return null (no server to connect to)
    *   - failover() always return true
    *        - the value returned by failover() comes from the reconnect() method
    *        - when there is no session already connected, the reconnect() method does *nothing*
    *          and returns true (while nothing has been reconnected)
    */
   public void testCreateSessionFailureWithDefaultValuesWhenNoServer() throws Exception
   {

      /****************************/
      /* No JBM Server is started */
      /****************************/

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(NettyConnectorFactory.class.getName()),
                                                             null,
                                                             ClientSessionFactoryImpl.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN,
                                                             ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL,
                                                             ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                                                             ClientSessionFactoryImpl.DEFAULT_RECONNECT_ATTEMPTS);

      try
      {
         sf.createSession(false, true, true);
         fail("Can not create a session when there is no running JBM Server");
      }
      catch (Exception e)
      {
      }
   }

   private void assertFactoryParams(ClientSessionFactory cf,
                                    int ackBatchSize,
                                    long callTimeout,
                                    int consumerMaxRate,
                                    int consumerWindowSize,
                                    int maxConnections,
                                    int minLargeMessageSize,
                                    long pingPeriod,
                                    int producerMaxRate,
                                    int sendWindowSize,
                                    boolean autoGroup,
                                    boolean blockOnAcknowledge,
                                    boolean blockOnNonPersistentSend,
                                    boolean blockOnPersistentSend,
                                    boolean preAcknowledge)
   {
      assertEquals(cf.getAckBatchSize(), ackBatchSize);
      assertEquals(cf.getCallTimeout(), callTimeout);
      assertEquals(cf.getConsumerMaxRate(), consumerMaxRate);
      assertEquals(cf.getConsumerWindowSize(), consumerWindowSize);
      assertEquals(cf.getMaxConnections(), maxConnections);
      assertEquals(cf.getMinLargeMessageSize(), minLargeMessageSize);
      assertEquals(cf.getPingPeriod(), pingPeriod);
      assertEquals(cf.getProducerMaxRate(), producerMaxRate);
      assertEquals(cf.getSendWindowSize(), sendWindowSize);
      assertEquals(cf.isAutoGroup(), autoGroup);
      assertEquals(cf.isBlockOnAcknowledge(), blockOnAcknowledge);
      assertEquals(cf.isBlockOnNonPersistentSend(), blockOnNonPersistentSend);
      assertEquals(cf.isBlockOnPersistentSend(), blockOnPersistentSend);
      assertEquals(cf.isPreAcknowledge(), preAcknowledge);
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
            .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory",
                                            backupParams));
      backupConf.setBackup(true);
      backupService = Messaging.newNullStorageMessagingServer(backupConf);
      backupService.start();

      Configuration liveConf = new ConfigurationImpl();
      liveConf.setSecurityEnabled(false);
      liveTC = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory");
      liveConf.getAcceptorConfigurations()
            .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory"));
      Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();
      backupTC = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                            backupParams);
      connectors.put(backupTC.getName(), backupTC);
      connectors.put(liveTC.getName(), liveTC);
      liveConf.setConnectorConfigurations(connectors);
      liveConf.setBackupConnectorName(backupTC.getName());
      liveConf.setClustered(true);

      List<Pair<String, String>> connectorNames = new ArrayList<Pair<String, String>>();
      connectorNames.add(new Pair<String, String>(liveTC.getName(), backupTC.getName()));

      final long broadcastPeriod = 250;

      final String bcGroupName = "bc1";

      final int localBindPort = 5432;

      BroadcastGroupConfiguration bcConfig1 = new BroadcastGroupConfiguration(bcGroupName,
                                                                              localBindPort,
                                                                              groupAddress,
                                                                              groupPort,
                                                                              broadcastPeriod,
                                                                              connectorNames);

      List<BroadcastGroupConfiguration> bcConfigs1 = new ArrayList<BroadcastGroupConfiguration>();
      bcConfigs1.add(bcConfig1);
      liveConf.setBroadcastGroupConfigurations(bcConfigs1);

      liveService = Messaging.newNullStorageMessagingServer(liveConf);
      liveService.start();
   }
}
