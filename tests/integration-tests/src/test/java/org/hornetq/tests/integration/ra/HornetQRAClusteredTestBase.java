package org.hornetq.tests.integration.ra;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.tests.unit.util.InVMContext;
import org.hornetq.tests.util.UnitTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.Set;

public class HornetQRAClusteredTestBase extends HornetQRATestBase
{
   private HornetQServer secondaryServer;
   private JMSServerManagerImpl secondaryJmsServer;
   private TransportConfiguration secondaryConnector;
   private TransportConfiguration primaryConnector;

   @Before
   @Override
   public void setUp() throws Exception
   {
      primaryConnector = new TransportConfiguration(INVM_CONNECTOR_FACTORY);
      HashMap<String, Object> params = new HashMap();
      params.put("server-id", "1");
      secondaryConnector = new TransportConfiguration(INVM_CONNECTOR_FACTORY, params);
      super.setUp();
      Configuration conf = createSecondaryDefaultConfig(true, true);

      secondaryServer = HornetQServers.newHornetQServer(conf, mbeanServer, usePersistence());
      addServer(secondaryServer);
      secondaryJmsServer = new JMSServerManagerImpl(secondaryServer);
      //context = new InVMContext();
      //secondaryJmsServer.setContext(context);
      secondaryJmsServer.start();
      waitForTopology(secondaryServer, 2);
   }

   @Override
   public void tearDown() throws Exception
   {
      if(secondaryJmsServer != null)
         secondaryJmsServer.stop();
      super.tearDown();
   }

   protected Configuration createDefaultConfig(boolean netty) throws Exception
   {
      Configuration conf = createSecondaryDefaultConfig(netty, false);
      return conf;
   }

   protected Configuration createSecondaryDefaultConfig(boolean netty, boolean secondary) throws Exception
   {
      ConfigurationImpl configuration = createBasicConfig(-1);

      configuration.setFileDeploymentEnabled(false);
      configuration.setJMXManagementEnabled(false);

      configuration.getAcceptorConfigurations().clear();

      HashMap invmMap = new HashMap();
      if(secondary)
      {
         invmMap.put("server-id", "1");
      }
      TransportConfiguration invmTransportConfig = new TransportConfiguration(INVM_ACCEPTOR_FACTORY, invmMap);
      configuration.getAcceptorConfigurations().add(invmTransportConfig);

      HashMap nettyMap = new HashMap();
      if(secondary)
      {
         nettyMap.put("port", "5545");
      }
      TransportConfiguration nettyTransportConfig = new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, nettyMap);
      configuration.getAcceptorConfigurations().add(nettyTransportConfig);

      if(secondary)
      {
         configuration.getConnectorConfigurations().put("invm2", secondaryConnector);
         configuration.getConnectorConfigurations().put("invm", primaryConnector);
         UnitTestCase.basicClusterConnectionConfig(configuration, "invm2", "invm");
         configuration.setJournalDirectory("/tmp/hornetq-unit-test/");
         configuration.setBindingsDirectory("/tmp/hornetq-unit-test/");
         configuration.setLargeMessagesDirectory("/tmp/hornetq-unit-test/");
         configuration.setPagingDirectory("/tmp/hornetq-unit-test/");
      }
      else
      {
         configuration.getConnectorConfigurations().put("invm", secondaryConnector);
         configuration.getConnectorConfigurations().put("invm2", primaryConnector);
         UnitTestCase.basicClusterConnectionConfig(configuration, "invm", "invm2");
      }

      configuration.setSecurityEnabled(false);
      configuration.setJMXManagementEnabled(true);
      return configuration;
   }
}
