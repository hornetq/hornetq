/*
 * Copyright 2009 Red Hat, Inc.
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package org.hornetq.tests.integration.ra;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import javax.jms.Connection;
import javax.resource.ResourceException;
import javax.resource.spi.endpoint.MessageEndpoint;

import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ServerLocatorImpl;
import org.hornetq.core.config.UDPBroadcastGroupConfiguration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.client.HornetQDestination;
import org.hornetq.ra.HornetQResourceAdapter;
import org.hornetq.ra.inflow.HornetQActivation;
import org.hornetq.ra.inflow.HornetQActivationSpec;
import org.hornetq.tests.unit.ra.MessageEndpointFactory;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.DefaultSensitiveStringCodec;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         Created Jul 7, 2010
 */
public class ResourceAdapterTest extends HornetQRATestBase
{
   public void testStartStopActivationManyTimes() throws Exception
   {
      HornetQServer server = createServer(false);

      server.start();
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory factory = createSessionFactory(locator);
      ClientSession session = factory.createSession(false, false, false);
      HornetQDestination queue = (HornetQDestination)HornetQJMSClient.createQueue("test");
      session.createQueue(queue.getSimpleAddress(), queue.getSimpleAddress(), true);
      session.close();

      HornetQResourceAdapter ra = new HornetQResourceAdapter();

      ra.setConnectorClassName("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory");
      ra.setUserName("userGlobal");
      ra.setPassword("passwordGlobal");
      ra.setTransactionManagerLocatorClass("");
      ra.setTransactionManagerLocatorMethod("");
      ra.start(new org.hornetq.tests.unit.ra.BootstrapContext());

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
      spec.setMaxSession(15);

      HornetQActivation activation = new HornetQActivation(ra, new MessageEndpointFactory(), spec);

      ServerLocatorImpl serverLocator = (ServerLocatorImpl)ra.getDefaultHornetQConnectionFactory().getServerLocator();

      Field f = Class.forName(ServerLocatorImpl.class.getName()).getDeclaredField("factories");

      f.setAccessible(true);

      Set<ClientSessionFactoryInternal> factories = (Set<ClientSessionFactoryInternal>)f.get(serverLocator);

      for (int i = 0; i < 10; i++)
      {
         System.out.println(i);
         assertEquals(factories.size(), 0);
         activation.start();
         assertEquals(factories.size(), 15);
         activation.stop();
         assertEquals(factories.size(), 0);
      }

      System.out.println("before RA stop => " + factories.size());
      ra.stop();
      System.out.println("after RA stop => " + factories.size());
      assertEquals(factories.size(), 0);
      locator.close();
   }

   public void testStartStop() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = new HornetQResourceAdapter();
      qResourceAdapter.setConnectorClassName(UnitTestCase.INVM_CONNECTOR_FACTORY);
      HornetQRATestBase.MyBootstrapContext ctx = new HornetQRATestBase.MyBootstrapContext();

      qResourceAdapter.setTransactionManagerLocatorClass("");
      qResourceAdapter.start(ctx);
      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);
      qResourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      CountDownLatch latch = new CountDownLatch(1);
      DummyMessageEndpoint endpoint = new DummyMessageEndpoint(latch);
      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint, false);
      qResourceAdapter.endpointActivation(endpointFactory, spec);
      qResourceAdapter.stop();
      assertTrue(endpoint.released);
   }

   public void testSetters() throws Exception
   {
      Boolean b = Boolean.TRUE;
      Long l = (long)1000;
      Integer i = 1000;
      Double d = (double)1000;
      String className = "testConnector";
      String backupConn = "testBackupConnector";
      String testConfig = "key=val";
      String testid = "testid";
      String testBalancer = "testBalancer";
      String testParams = "key=val";
      String testaddress = "testaddress";
      String loadbalancer = "loadbalancer";
      String testpass = "testpass";
      String testuser = "testuser";
      HornetQResourceAdapter qResourceAdapter = new HornetQResourceAdapter();
      testParams(b, l, i, d, className, backupConn, testConfig, testid, testBalancer, testParams, testaddress, testpass, testuser, qResourceAdapter);
   }

   public void testSetters2() throws Exception
   {
      Boolean b = Boolean.FALSE;
      Long l = (long) 2000;
      Integer i = 2000;
      Double d = (double) 2000;
      String className = "testConnector2";
      String backupConn = "testBackupConnector2";
      String testConfig = "key2=val2";
      String testid = "testid2";
      String testBalancer = "testBalancer2";
      String testParams = "key=val2";
      String testaddress = "testaddress2";
      String loadbalancer = "loadbalancer2";
      String testpass = "testpass2";
      String testuser = "testuser2";
      HornetQResourceAdapter qResourceAdapter = new HornetQResourceAdapter();
      testParams(b, l, i, d, className, backupConn, testConfig, testid, testBalancer, testParams, testaddress, testpass, testuser, qResourceAdapter);
   }


   private void testParams(Boolean b, Long l, Integer i, Double d, String className, String backupConn, String testConfig, String testid, String testBalancer, String testParams, String testaddress, String testpass, String testuser, HornetQResourceAdapter qResourceAdapter)
   {
      qResourceAdapter.setUseLocalTx(b);
      qResourceAdapter.setConnectorClassName(className);
      qResourceAdapter.setAutoGroup(b);
      qResourceAdapter.setBlockOnAcknowledge(b);
      qResourceAdapter.setBlockOnDurableSend(b);
      qResourceAdapter.setBlockOnNonDurableSend(b);
      qResourceAdapter.setCallTimeout(l);
      qResourceAdapter.setClientFailureCheckPeriod(l);
      qResourceAdapter.setClientID(testid);
      qResourceAdapter.setConfirmationWindowSize(i);
      qResourceAdapter.setConnectionLoadBalancingPolicyClassName(testBalancer);
      qResourceAdapter.setConnectionParameters(testParams);
      qResourceAdapter.setConnectionTTL(l);
      qResourceAdapter.setConsumerMaxRate(i);
      qResourceAdapter.setConsumerWindowSize(i);
      qResourceAdapter.setDiscoveryAddress(testaddress);
      qResourceAdapter.setDiscoveryInitialWaitTimeout(l);
      qResourceAdapter.setDiscoveryPort(i);
      qResourceAdapter.setDiscoveryRefreshTimeout(l);
      qResourceAdapter.setDupsOKBatchSize(i);
      qResourceAdapter.setMinLargeMessageSize(i);
      qResourceAdapter.setPassword(testpass);
      qResourceAdapter.setPreAcknowledge(b);
      qResourceAdapter.setProducerMaxRate(i);
      qResourceAdapter.setReconnectAttempts(i);
      qResourceAdapter.setRetryInterval(l);
      qResourceAdapter.setRetryIntervalMultiplier(d);
      qResourceAdapter.setScheduledThreadPoolMaxSize(i);
      qResourceAdapter.setThreadPoolMaxSize(i);
      qResourceAdapter.setTransactionBatchSize(i);
      qResourceAdapter.setUseGlobalPools(b);
      qResourceAdapter.setUseLocalTx(b);
      qResourceAdapter.setUserName(testuser);

      assertEquals(qResourceAdapter.getUseLocalTx(), b);
      assertEquals(qResourceAdapter.getConnectorClassName(), className);
      assertEquals(qResourceAdapter.getAutoGroup(), b);
      //assertEquals(qResourceAdapter.getBackupTransportConfiguration(),"testConfig");
      assertEquals(qResourceAdapter.getBlockOnAcknowledge(), b);
      assertEquals(qResourceAdapter.getBlockOnDurableSend(), b);
      assertEquals(qResourceAdapter.getBlockOnNonDurableSend(), b);
      assertEquals(qResourceAdapter.getCallTimeout(), l);
      assertEquals(qResourceAdapter.getClientFailureCheckPeriod(), l);
      assertEquals(qResourceAdapter.getClientID(), testid);
      assertEquals(qResourceAdapter.getConfirmationWindowSize(), i);
      assertEquals(qResourceAdapter.getConnectionLoadBalancingPolicyClassName(), testBalancer);
      assertEquals(qResourceAdapter.getConnectionParameters(), testParams);
      assertEquals(qResourceAdapter.getConnectionTTL(), l);
      assertEquals(qResourceAdapter.getConsumerMaxRate(), i);
      assertEquals(qResourceAdapter.getConsumerWindowSize(), i);
      assertEquals(qResourceAdapter.getDiscoveryAddress(), testaddress);
      assertEquals(qResourceAdapter.getDiscoveryInitialWaitTimeout(), l);
      assertEquals(qResourceAdapter.getDiscoveryPort(), i);
      assertEquals(qResourceAdapter.getDiscoveryRefreshTimeout(), l);
      assertEquals(qResourceAdapter.getDupsOKBatchSize(), i);
      assertEquals(qResourceAdapter.getMinLargeMessageSize(), i);
      assertEquals(qResourceAdapter.getPassword(), testpass);
      assertEquals(qResourceAdapter.getPreAcknowledge(), b);
      assertEquals(qResourceAdapter.getProducerMaxRate(), i);
      assertEquals(qResourceAdapter.getReconnectAttempts(), i);
      assertEquals(qResourceAdapter.getRetryInterval(), l);
      assertEquals(qResourceAdapter.getRetryIntervalMultiplier(), d);
      assertEquals(qResourceAdapter.getScheduledThreadPoolMaxSize(), i);
      assertEquals(qResourceAdapter.getThreadPoolMaxSize(), i);
      assertEquals(qResourceAdapter.getTransactionBatchSize(), i);
      assertEquals(qResourceAdapter.getUseGlobalPools(), b);
      assertEquals(qResourceAdapter.getUseLocalTx(), b);
      assertEquals(qResourceAdapter.getUserName(), testuser);
   }

   //https://issues.jboss.org/browse/JBPAPP-5790
   public void testResourceAdapterSetup() throws Exception
   {
      HornetQResourceAdapter adapter = new HornetQResourceAdapter();
      adapter.setDiscoveryAddress("231.1.1.1");
      HornetQConnectionFactory factory = adapter.getDefaultHornetQConnectionFactory();
      long initWait = factory.getDiscoveryGroupConfiguration().getDiscoveryInitialWaitTimeout();
      long refresh = factory.getDiscoveryGroupConfiguration().getRefreshTimeout();
      int port = ((UDPBroadcastGroupConfiguration)factory.getDiscoveryGroupConfiguration().getBroadcastEndpointFactoryConfiguration()).getGroupPort();

      //defaults
      assertEquals(10000l, refresh);
      assertEquals(10000l, initWait);
      assertEquals(9876, port);

      adapter = new HornetQResourceAdapter();
      adapter.setDiscoveryAddress("231.1.1.1");
      adapter.setDiscoveryPort(9876);
      adapter.setDiscoveryRefreshTimeout(1234l);
      factory = adapter.getDefaultHornetQConnectionFactory();
      initWait = factory.getDiscoveryGroupConfiguration().getDiscoveryInitialWaitTimeout();
      refresh = factory.getDiscoveryGroupConfiguration().getRefreshTimeout();

      //override refresh timeout
      assertEquals(1234l, refresh);
      assertEquals(10000l, initWait);

      adapter = new HornetQResourceAdapter();
      adapter.setDiscoveryAddress("231.1.1.1");
      adapter.setDiscoveryPort(9876);
      adapter.setDiscoveryInitialWaitTimeout(9999l);
      factory = adapter.getDefaultHornetQConnectionFactory();
      initWait = factory.getDiscoveryGroupConfiguration().getDiscoveryInitialWaitTimeout();
      refresh = factory.getDiscoveryGroupConfiguration().getRefreshTimeout();

      //override initial wait
      assertEquals(10000l, refresh);
      assertEquals(9999l, initWait);

      adapter = new HornetQResourceAdapter();
      adapter.setDiscoveryAddress("231.1.1.1");
      adapter.setDiscoveryPort(9876);
      adapter.setDiscoveryInitialWaitTimeout(9999l);
      factory = adapter.getDefaultHornetQConnectionFactory();
      initWait = factory.getDiscoveryGroupConfiguration().getDiscoveryInitialWaitTimeout();
      refresh = factory.getDiscoveryGroupConfiguration().getRefreshTimeout();

      //override initial wait
      assertEquals(10000l, refresh);
      assertEquals(9999l, initWait);

   }

    //https://issues.jboss.org/browse/JBPAPP-5836
   public void testResourceAdapterSetupOverrideCFParams() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = new HornetQResourceAdapter();
      qResourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      qResourceAdapter.setConnectionParameters("server-id=0");
      HornetQRATestBase.MyBootstrapContext ctx = new HornetQRATestBase.MyBootstrapContext();

      qResourceAdapter.setTransactionManagerLocatorClass("");
      qResourceAdapter.start(ctx);
      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);
      //now override the connector class
      spec.setConnectorClassName(NETTY_CONNECTOR_FACTORY);
      spec.setConnectionParameters("port=5445");
      CountDownLatch latch = new CountDownLatch(1);
      DummyMessageEndpoint endpoint = new DummyMessageEndpoint(latch);
      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint, false);
      qResourceAdapter.endpointActivation(endpointFactory, spec);
      qResourceAdapter.stop();
      assertTrue(endpoint.released);
   }

   public void testResourceAdapterSetupOverrideNoCFParams() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = new HornetQResourceAdapter();
      qResourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      qResourceAdapter.setConnectionParameters("server-id=0");
      HornetQRATestBase.MyBootstrapContext ctx = new HornetQRATestBase.MyBootstrapContext();

      qResourceAdapter.setTransactionManagerLocatorClass("");
      qResourceAdapter.start(ctx);
      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);

      CountDownLatch latch = new CountDownLatch(1);
      DummyMessageEndpoint endpoint = new DummyMessageEndpoint(latch);
      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint, false);
      qResourceAdapter.endpointActivation(endpointFactory, spec);
      checkUpdates(qResourceAdapter, spec);
      assertTrue(endpoint.released);
   }

   public void testResourceAdapterSetupNoOverrideDiscovery() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = new HornetQResourceAdapter();
      qResourceAdapter.setDiscoveryAddress("231.6.6.6");
      qResourceAdapter.setDiscoveryPort(1234);
      qResourceAdapter.setDiscoveryRefreshTimeout(1l);
      qResourceAdapter.setDiscoveryInitialWaitTimeout(1l);
      HornetQRATestBase.MyBootstrapContext ctx = new HornetQRATestBase.MyBootstrapContext();

      qResourceAdapter.setTransactionManagerLocatorClass("");
      qResourceAdapter.start(ctx);
      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);
      HornetQConnectionFactory fac = qResourceAdapter.createHornetQConnectionFactory(spec);
      DiscoveryGroupConfiguration dc = fac.getServerLocator().getDiscoveryGroupConfiguration();
      UDPBroadcastGroupConfiguration udpDg = (UDPBroadcastGroupConfiguration) dc.getBroadcastEndpointFactoryConfiguration();
      assertEquals(udpDg.getGroupAddress(), "231.6.6.6");
      assertEquals(udpDg.getGroupPort(), 1234);
      assertEquals(dc.getRefreshTimeout(), 1l);
      assertEquals(dc.getDiscoveryInitialWaitTimeout(), 1l);
      qResourceAdapter.stop();
   }

   public void testResourceAdapterSetupOverrideDiscovery() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = new HornetQResourceAdapter();
      qResourceAdapter.setDiscoveryAddress("231.7.7.7");

      //qResourceAdapter.getTransactionManagerLocatorClass
      HornetQRATestBase.MyBootstrapContext ctx = new HornetQRATestBase.MyBootstrapContext();

      qResourceAdapter.setTransactionManagerLocatorClass("");

      qResourceAdapter.start(ctx);
      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);
      spec.setSetupAttempts(0);
      spec.setDiscoveryAddress("231.6.6.6");
      spec.setDiscoveryPort(1234);
      spec.setDiscoveryInitialWaitTimeout(1l);
      spec.setDiscoveryRefreshTimeout(1l);
      HornetQConnectionFactory fac = qResourceAdapter.createHornetQConnectionFactory(spec);
      DiscoveryGroupConfiguration dc = fac.getServerLocator().getDiscoveryGroupConfiguration();
      UDPBroadcastGroupConfiguration udpDg = (UDPBroadcastGroupConfiguration) dc.getBroadcastEndpointFactoryConfiguration();
      assertEquals(udpDg.getGroupAddress(), "231.6.6.6");
      assertEquals(udpDg.getGroupPort(), 1234);
      assertEquals(dc.getRefreshTimeout(), 1l);
      assertEquals(dc.getDiscoveryInitialWaitTimeout(), 1l);
      qResourceAdapter.stop();
   }

   public void testResourceAdapterSetupNoHAOverride() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = new HornetQResourceAdapter();
      qResourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      qResourceAdapter.setConnectionParameters("server-id=0");
      qResourceAdapter.setHA(true);
      HornetQRATestBase.MyBootstrapContext ctx = new HornetQRATestBase.MyBootstrapContext();

      qResourceAdapter.setTransactionManagerLocatorClass("");
      qResourceAdapter.start(ctx);
      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);

      HornetQConnectionFactory fac = qResourceAdapter.createHornetQConnectionFactory(spec);

      assertTrue(fac.isHA());

      checkUpdates(qResourceAdapter, spec);
   }

   public void testResourceAdapterSetupNoHADefault() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = new HornetQResourceAdapter();
      qResourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      qResourceAdapter.setConnectionParameters("server-id=0");
      HornetQRATestBase.MyBootstrapContext ctx = new HornetQRATestBase.MyBootstrapContext();

      qResourceAdapter.setTransactionManagerLocatorClass("");
      qResourceAdapter.start(ctx);
      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);

      HornetQConnectionFactory fac = qResourceAdapter.createHornetQConnectionFactory(spec);

      assertFalse(fac.isHA());

      checkUpdates(qResourceAdapter, spec);
   }


   public void testResourceAdapterSetupHAOverride() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = new HornetQResourceAdapter();
      qResourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      qResourceAdapter.setConnectionParameters("server-id=0");
      HornetQRATestBase.MyBootstrapContext ctx = new HornetQRATestBase.MyBootstrapContext();

      qResourceAdapter.setTransactionManagerLocatorClass("");
      qResourceAdapter.start(ctx);
      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);
      spec.setHA(true);
      HornetQConnectionFactory fac = qResourceAdapter.createHornetQConnectionFactory(spec);

      assertTrue(fac.isHA());

      qResourceAdapter.stop();
      assertTrue(spec.isHasBeenUpdated());
   }

   public void testResourceAdapterSetupNoReconnectAttemptsOverride() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = new HornetQResourceAdapter();
      qResourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      qResourceAdapter.setConnectionParameters("server-id=0");
      qResourceAdapter.setReconnectAttempts(100);
      HornetQRATestBase.MyBootstrapContext ctx = new HornetQRATestBase.MyBootstrapContext();

      qResourceAdapter.setTransactionManagerLocatorClass("");
      qResourceAdapter.start(ctx);
      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);

      HornetQConnectionFactory fac = qResourceAdapter.createHornetQConnectionFactory(spec);

      assertEquals(100, fac.getReconnectAttempts());

      checkUpdates(qResourceAdapter, spec);
   }

   public void testResourceAdapterSetupReconnectAttemptDefault() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = new HornetQResourceAdapter();
      qResourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      qResourceAdapter.setConnectionParameters("server-id=0");
      HornetQRATestBase.MyBootstrapContext ctx = new HornetQRATestBase.MyBootstrapContext();

      qResourceAdapter.setTransactionManagerLocatorClass("");
      qResourceAdapter.start(ctx);
      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);

      HornetQConnectionFactory fac = qResourceAdapter.createHornetQConnectionFactory(spec);

      assertEquals(-1, fac.getReconnectAttempts());

      checkUpdates(qResourceAdapter, spec);
   }

   /**
    * @param qResourceAdapter
    * @param spec
    */
   private void checkUpdates(HornetQResourceAdapter qResourceAdapter, HornetQActivationSpec spec)
   {
      qResourceAdapter.stop();
      assertFalse("spec should not be updated", spec.isHasBeenUpdated());
   }

   public void testResourceAdapterSetupReconnectAttemptsOverride() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = new HornetQResourceAdapter();
      qResourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      qResourceAdapter.setConnectionParameters("server-id=0");
      HornetQRATestBase.MyBootstrapContext ctx = new HornetQRATestBase.MyBootstrapContext();

      qResourceAdapter.setTransactionManagerLocatorClass("");
      qResourceAdapter.start(ctx);
      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);
      spec.setReconnectAttempts(100);
      HornetQConnectionFactory fac = qResourceAdapter.createHornetQConnectionFactory(spec);

      assertEquals(100, fac.getReconnectAttempts());

      qResourceAdapter.stop();
      assertTrue(spec.isHasBeenUpdated());
   }

   public void testMaskPassword() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = new HornetQResourceAdapter();
      qResourceAdapter.setConnectorClassName(UnitTestCase.INVM_CONNECTOR_FACTORY);
      HornetQRATestBase.MyBootstrapContext ctx = new HornetQRATestBase.MyBootstrapContext();

      DefaultSensitiveStringCodec codec = new DefaultSensitiveStringCodec();
      String mask = (String)codec.encode("helloworld");

      qResourceAdapter.setUseMaskedPassword(true);
      qResourceAdapter.setPassword(mask);

      qResourceAdapter.setTransactionManagerLocatorClass("");
      qResourceAdapter.start(ctx);

      assertEquals("helloworld", qResourceAdapter.getPassword());

      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);

      mask = (String)codec.encode("mdbpassword");
      spec.setPassword(mask);
      qResourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      CountDownLatch latch = new CountDownLatch(1);
      DummyMessageEndpoint endpoint = new DummyMessageEndpoint(latch);
      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint, false);
      qResourceAdapter.endpointActivation(endpointFactory, spec);

      assertEquals("mdbpassword", spec.getPassword());

      qResourceAdapter.stop();
      assertTrue(endpoint.released);
   }

   public void testMaskPassword2() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = new HornetQResourceAdapter();
      qResourceAdapter.setConnectorClassName(UnitTestCase.INVM_CONNECTOR_FACTORY);
      HornetQRATestBase.MyBootstrapContext ctx = new HornetQRATestBase.MyBootstrapContext();

      qResourceAdapter.setUseMaskedPassword(true);
      qResourceAdapter.setPasswordCodec(DefaultSensitiveStringCodec.class.getName() + ";key=anotherkey");

      DefaultSensitiveStringCodec codec = new DefaultSensitiveStringCodec();
      Map<String, String> prop = new HashMap<String, String>();

      prop.put("key", "anotherkey");
      codec.init(prop);

      String mask = (String)codec.encode("helloworld");

      qResourceAdapter.setPassword(mask);

      qResourceAdapter.setTransactionManagerLocatorClass("");
      qResourceAdapter.start(ctx);

      assertEquals("helloworld", qResourceAdapter.getPassword());

      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);

      mask = (String)codec.encode("mdbpassword");
      spec.setPassword(mask);
      qResourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      CountDownLatch latch = new CountDownLatch(1);
      DummyMessageEndpoint endpoint = new DummyMessageEndpoint(latch);
      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint, false);
      qResourceAdapter.endpointActivation(endpointFactory, spec);

      assertEquals("mdbpassword", spec.getPassword());

      qResourceAdapter.stop();
      assertTrue(endpoint.released);
   }

   @Override
   public boolean isSecure()
   {
      return false;
   }

   class DummyEndpoint implements MessageEndpoint
   {
      public void beforeDelivery(Method method) throws NoSuchMethodException, ResourceException
      {
         //To change body of implemented methods use File | Settings | File Templates.
      }

      public void afterDelivery() throws ResourceException
      {
         //To change body of implemented methods use File | Settings | File Templates.
      }

      public void release()
      {
         //To change body of implemented methods use File | Settings | File Templates.
      }
   }
}
