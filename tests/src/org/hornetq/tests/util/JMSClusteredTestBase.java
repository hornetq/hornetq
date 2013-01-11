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

package org.hornetq.tests.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.ConnectionFactory;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.naming.NamingException;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.server.config.impl.JMSConfigurationImpl;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.tests.integration.cluster.distribution.ClusterTestBase;
import org.hornetq.tests.unit.util.InVMContext;

/**
 * A JMSBaseTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class JMSClusteredTestBase extends ServiceTestBase
{

   private static final Logger log = Logger.getLogger(ClusterTestBase.class);

   protected HornetQServer server1;

   protected JMSServerManagerImpl jmsServer1;

   protected HornetQServer server2;

   protected JMSServerManagerImpl jmsServer2;

   protected ConnectionFactory cf1;

   protected ConnectionFactory cf2;

   protected InVMContext context1;

   protected InVMContext context2;

   private static final int MAX_HOPS = 1;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // TestCase overrides -------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   /**
    * @throws Exception
    * @throws NamingException
    */
   protected Queue createQueue(final String name) throws Exception, NamingException
   {
      jmsServer2.createQueue(false, name, null, true, "/queue/" + name);
      jmsServer1.createQueue(false, name, null, true, "/queue/" + name);

      assertTrue(waitForBindings(server1, "jms.queue." + name, false, 1, 0, 10000));
      assertTrue(waitForBindings(server2, "jms.queue." + name, false, 1, 0, 10000));

      return (Queue)context1.lookup("/queue/" + name);
   }

   protected Topic createTopic(final String name) throws Exception, NamingException
   {
      jmsServer2.createTopic(false, name, "/topic/" + name);
      jmsServer1.createTopic(false, name, "/topic/" + name);

      return (Topic)context1.lookup("/topic/" + name);
   }

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      setupServer2();
      setupServer1();

      jmsServer1.start();
      jmsServer1.activated();
      waitForServer(jmsServer1.getHornetQServer());

      jmsServer2.start();
      jmsServer2.activated();
      waitForServer(jmsServer2.getHornetQServer());

      waitForTopology(jmsServer1.getHornetQServer(), 2);

      waitForTopology(jmsServer2.getHornetQServer(), 2);

      cf1 = (ConnectionFactory) HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                                                generateInVMParams(0)));
      cf2 = (ConnectionFactory)HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                                                generateInVMParams(1)));
   }

   /**
    * @param toOtherServerPair
    * @throws Exception
    */
   private void setupServer2() throws Exception
   {
      Configuration conf2 = createConfigServer2();

      JMSConfigurationImpl jmsconfig = new JMSConfigurationImpl();

      server2 = HornetQServers.newHornetQServer(conf2, false);
      jmsServer2 = new JMSServerManagerImpl(server2, jmsconfig);
      context2 = new InVMContext();
      jmsServer2.setContext(context2);
   }

   /**
    * @return
    */
   protected Configuration createConfigServer2()
   {
      List<String> toOtherServerPair = new ArrayList<String>();
      toOtherServerPair.add("toServer1");

      Configuration conf2 = createDefaultConfig(1, generateInVMParams(1), InVMAcceptorFactory.class.getCanonicalName());
      conf2.setSecurityEnabled(false);
      conf2.setJMXManagementEnabled(true);
      conf2.setPersistenceEnabled(false);

      conf2.getConnectorConfigurations().put("toServer1",
                                             new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                                        generateInVMParams(0)));
      conf2.getConnectorConfigurations().put("server2",
                                             new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                                        generateInVMParams(1)));

      conf2.setClustered(true);

      conf2.getClusterConfigurations().add(new ClusterConnectionConfiguration("to-server1",
                                                                              "jms",
                                                                                 "server2",
                                                                              1000,
                                                                              true,
                                                                              false,
                                                                              MAX_HOPS,
                                                                              1024,
                                                                              toOtherServerPair, false));
      return conf2;
   }

   /**
    * @param toOtherServerPair
    * @throws Exception
    */
   private void setupServer1() throws Exception
   {
      Configuration conf1 = createConfigServer1();

      JMSConfigurationImpl jmsconfig = new JMSConfigurationImpl();

      server1 = HornetQServers.newHornetQServer(conf1, false);
      jmsServer1 = new JMSServerManagerImpl(server1, jmsconfig);
      context1 = new InVMContext();
      jmsServer1.setContext(context1);
   }

   /**
    * @return
    */
   protected Configuration createConfigServer1()
   {
      List<String> toOtherServerPair = new ArrayList<String>();
      toOtherServerPair.add("toServer2");

      Configuration conf1 = createDefaultConfig(0, generateInVMParams(0), InVMAcceptorFactory.class.getCanonicalName());

      conf1.setSecurityEnabled(false);
      conf1.setJMXManagementEnabled(true);
      conf1.setPersistenceEnabled(false);

      conf1.getConnectorConfigurations().put("toServer2",
                                             new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                                        generateInVMParams(1)));
      conf1.getConnectorConfigurations().put("server1",
                                             new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                                        generateInVMParams(0)));

      conf1.setClustered(true);

      conf1.getClusterConfigurations().add(new ClusterConnectionConfiguration("to-server2",
                                                                              "jms",
                                                                              "server1",
                                                                              1000,
                                                                              true,
                                                                              false,
                                                                              MAX_HOPS,
                                                                              1024,
                                                                              toOtherServerPair, false));
      return conf1;
   }

   @Override
   protected void tearDown() throws Exception
   {

      try
      {
         jmsServer2.stop();

         server2.stop();

         context2.close();
      }
      catch (Throwable e)
      {
         log.warn("Can't stop server2", e);
      }
      
      ((HornetQConnectionFactory)cf1).close();

      ((HornetQConnectionFactory)cf2).close();

      server2 = null;

      jmsServer2 = null;

      context2 = null;

      cf1 = null;

      try
      {
         jmsServer1.stop();

         server1.stop();

         context1.close();
      }
      catch (Throwable e)
      {
         log.warn("Can't stop server1", e);
      }

      server1 = null;

      jmsServer1 = null;

      context1 = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   protected Map<String, Object> generateInVMParams(final int node)
   {
      Map<String, Object> params = new HashMap<String, Object>();

      params.put(org.hornetq.core.remoting.impl.invm.TransportConstants.SERVER_ID_PROP_NAME, node);

      return params;
   }


}
