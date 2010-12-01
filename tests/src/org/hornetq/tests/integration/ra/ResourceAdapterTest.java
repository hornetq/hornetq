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

import org.hornetq.ra.HornetQResourceAdapter;
import org.hornetq.ra.inflow.HornetQActivationSpec;
import org.hornetq.tests.util.ServiceTestBase;

import javax.resource.ResourceException;
import javax.resource.spi.BootstrapContext;
import javax.resource.spi.ResourceAdapterInternalException;
import javax.resource.spi.UnavailableException;
import javax.resource.spi.XATerminator;
import javax.resource.spi.endpoint.MessageEndpoint;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.resource.spi.work.WorkManager;
import javax.transaction.xa.XAResource;
import java.lang.reflect.Method;
import java.util.Timer;
import java.util.concurrent.CountDownLatch;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         Created Jul 7, 2010
 */
public class ResourceAdapterTest extends HornetQRATestBase
{
   public void testStartStop() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = new HornetQResourceAdapter();
      HornetQRATestBase.MyBootstrapContext ctx = new HornetQRATestBase.MyBootstrapContext();
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
      Long l = (long) 1000;
      Integer i = 1000;
      Double d = (double) 1000;
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
      qResourceAdapter.setFailoverOnServerShutdown(b);
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
      assertEquals(qResourceAdapter.getFailoverOnServerShutdown(), b);
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
