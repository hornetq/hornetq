package org.hornetq.byteman.tests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hornetq.api.config.HornetQDefaultConfiguration;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.BridgeConfiguration;
import org.hornetq.core.config.CoreQueueConfiguration;
import org.hornetq.core.remoting.impl.invm.TransportConstants;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.cluster.impl.BridgeImpl;
import org.hornetq.tests.util.ServiceTestBase;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
public class BridgeServerLocatorConfigurationTest extends ServiceTestBase
{

   private static final long BRIDGE_TTL = 1234L;
   private static final String BRIDGE_NAME = "bridge1";

   protected boolean isNetty()
   {
      return false;
   }

   private String getConnector()
   {
      if (isNetty())
      {
         return NETTY_CONNECTOR_FACTORY;
      }
      return INVM_CONNECTOR_FACTORY;
   }

   @Test
   @BMRule(name = "check connection ttl",
            targetClass = "org.hornetq.byteman.tests.BridgeServerLocatorConfigurationTest",
            targetMethod = "getBridgeTTL(HornetQServer, String)", targetLocation = "EXIT",
            action = "$! = $0.getConfiguredBridge($1).serverLocator.getConnectionTTL();")
   /**
    * Checks the connection ttl by using byteman to override the methods on this class to return the value of private variables in the Bridge.
    * @throws Exception
    *
    * The byteman rule on this test overwrites the {@link #getBridgeTTL} method to retrieve the bridge called {@link @BRIDGE_NAME}.
    * It the overrides the return value to be the value of the connection ttl. Note that the unused String parameter is required to
    * ensure that byteman populates the $1 variable, otherwise it will not bind correctly.
    */
   public void testConnectionTTLOnBridge() throws Exception
   {
      Map<String, Object> server0Params = new HashMap<String, Object>();
      HornetQServer serverWithBridge = createClusteredServerWithParams(isNetty(), 0, true, server0Params);

      Map<String, Object> server1Params = new HashMap<String, Object>();
      if (isNetty())
      {
         server1Params.put("port", org.hornetq.core.remoting.impl.netty.TransportConstants.DEFAULT_PORT + 1);
      }
      else
      {
         server1Params.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      }
      HornetQServer server1 = createClusteredServerWithParams(isNetty(), 1, true, server1Params);
      ServerLocator locator = null;
      try
      {
         final String testAddress = "testAddress";
         final String queueName0 = "queue0";
         final String forwardAddress = "forwardAddress";
         final String queueName1 = "queue1";

         Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();
         TransportConfiguration server1tc = new TransportConfiguration(getConnector(), server1Params);
         connectors.put(server1tc.getName(), server1tc);

         serverWithBridge.getConfiguration().setConnectorConfigurations(connectors);

         ArrayList<String> staticConnectors = new ArrayList<String>();
         staticConnectors.add(server1tc.getName());

         BridgeConfiguration bridgeConfiguration =
                  new BridgeConfiguration(BRIDGE_NAME, queueName0, forwardAddress, null, null,
                                          HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                          HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD, BRIDGE_TTL, 1000,
                                          HornetQClient.DEFAULT_MAX_RETRY_INTERVAL, 1d, 0, 0, true, 1024,
                                          staticConnectors, false, HornetQDefaultConfiguration.getDefaultClusterUser(),
                                          HornetQDefaultConfiguration.getDefaultClusterPassword());

         List<BridgeConfiguration> bridgeConfigs = new ArrayList<BridgeConfiguration>();
         bridgeConfigs.add(bridgeConfiguration);
         serverWithBridge.getConfiguration().setBridgeConfigurations(bridgeConfigs);

         CoreQueueConfiguration queueConfig0 = new CoreQueueConfiguration(testAddress, queueName0, null, true);
         List<CoreQueueConfiguration> queueConfigs0 = new ArrayList<CoreQueueConfiguration>();
         queueConfigs0.add(queueConfig0);
         serverWithBridge.getConfiguration().setQueueConfigurations(queueConfigs0);

         CoreQueueConfiguration queueConfig1 = new CoreQueueConfiguration(forwardAddress, queueName1, null, true);
         List<CoreQueueConfiguration> queueConfigs1 = new ArrayList<CoreQueueConfiguration>();
         queueConfigs1.add(queueConfig1);
         server1.getConfiguration().setQueueConfigurations(queueConfigs1);

         server1.start();
         waitForServer(server1);

         serverWithBridge.start();
         waitForServer(serverWithBridge);

         long bridgeTTL = getBridgeTTL(serverWithBridge, BRIDGE_NAME);

         assertEquals(BRIDGE_TTL, bridgeTTL);
      }
      finally
      {
         if (locator != null)
         {
            locator.close();
         }

         serverWithBridge.stop();

         server1.stop();
      }
   }

   /**
    * Method for byteman to wrap around and do its magic with to return the ttl from private members
    * rather than -1
    * @param bridgeServer
    * @param bridgeName
    * @return
    */
   private long getBridgeTTL(HornetQServer bridgeServer, String bridgeName)
   {
      return -1L;
   }

   /**
    * Byteman seems to need this method so that it gets back the concrete type not the interface
    * @param bridgeServer
    * @return
    */
   private BridgeImpl getConfiguredBridge(HornetQServer bridgeServer)
   {
      return getConfiguredBridge(bridgeServer, BRIDGE_NAME);
   }

   private BridgeImpl getConfiguredBridge(HornetQServer bridgeServer, String bridgeName)
   {
      return (BridgeImpl)bridgeServer.getClusterManager().getBridges().get(bridgeName);
   }
}
