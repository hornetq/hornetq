package org.hornetq.byteman.tests;

import javax.net.ssl.SSLEngine;
import java.util.HashMap;
import java.util.Map;

import org.hornetq.api.core.HornetQNotConnectedException;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
public class PoodleTest extends ServiceTestBase
{
   public static final String SERVER_SIDE_KEYSTORE = "server-side.keystore";
   public static final String CLIENT_SIDE_TRUSTSTORE = "client-side.truststore";
   public static final String PASSWORD = "secureexample";

   private HornetQServer server;

   private TransportConfiguration tc;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      ConfigurationImpl config = createBasicConfig();
      config.setSecurityEnabled(false);
      Map<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      params.put(TransportConstants.KEYSTORE_PATH_PROP_NAME, SERVER_SIDE_KEYSTORE);
      params.put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, PASSWORD);
      config.getAcceptorConfigurations().add(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params));
      server = createServer(false, config);
      server.start();
      waitForServer(server);
      tc = new TransportConfiguration(NETTY_CONNECTOR_FACTORY);
   }

   /*
   * simple test to make sure connect still works with some network latency  built into netty
   * */
   @Test
   @BMRules
      (
         rules =
            {
               @BMRule
                  (
                     name = "rule to force sslv3",
                     targetClass = "org.hornetq.core.remoting.impl.netty.NettyConnector$1",
                     targetMethod = "getPipeline",
                     targetLocation = "AFTER INVOKE createSSLEngine",
                     action = "org.hornetq.byteman.tests.PoodleTest.setEnabledProtocols($!);"
                  )
            }
      )
   public void testPOODLE() throws Exception
   {
      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, CLIENT_SIDE_TRUSTSTORE);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, PASSWORD);

      ServerLocator locator = addServerLocator(HornetQClient.createServerLocatorWithoutHA(tc));
      try
      {
         createSessionFactory(locator);
         Assert.fail();
      }
      catch (HornetQNotConnectedException e)
      {
         Assert.assertTrue(true);
      }
   }

   public static void setEnabledProtocols(SSLEngine engine)
   {
      engine.setEnabledProtocols(new String[]{"SSLv3"});
   }
}
