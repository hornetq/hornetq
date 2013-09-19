package org.hornetq.tests.integration.ra;

import org.hornetq.core.server.Queue;
import org.hornetq.ra.HornetQResourceAdapter;
import org.hornetq.ra.inflow.HornetQActivationSpec;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

public class HornetQClusteredTest extends HornetQRAClusteredTestBase
{

   /*
   * the second server has no queue so this tests for partial initialisation
   * */
   @Test
   public void testShutdownOnPartialConnect() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = newResourceAdapter();
      MyBootstrapContext ctx = new MyBootstrapContext();
      qResourceAdapter.setHA(true);
      qResourceAdapter.start(ctx);
      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setSetupAttempts(0);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);
      spec.setHA(true);
      qResourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY + "," + INVM_CONNECTOR_FACTORY);
      qResourceAdapter.setConnectionParameters("server-id=0, server-id=1");
      CountDownLatch latch = new CountDownLatch(1);
      DummyMessageEndpoint endpoint = new DummyMessageEndpoint(latch);
      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint, false);
      qResourceAdapter.endpointActivation(endpointFactory, spec);
      //make sure thet activation didnt start, i.e. no MDB consumers
      assertEquals(((Queue)server.getPostOffice().getBinding(MDBQUEUEPREFIXEDSIMPLE).getBindable()).getConsumerCount(), 0);
      qResourceAdapter.endpointDeactivation(endpointFactory, spec);

      qResourceAdapter.stop();
   }
}
