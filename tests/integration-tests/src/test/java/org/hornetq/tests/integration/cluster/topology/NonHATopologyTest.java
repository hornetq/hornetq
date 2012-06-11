/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.tests.integration.cluster.topology;

import java.util.ArrayList;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.client.impl.Topology;
import org.hornetq.core.client.impl.TopologyMember;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * I have added this test to help validate if the connectors from Recovery will be
 * properly updated
 * 
 * Created to verify HORNETQ-913 / AS7-4548
 *
 * @author clebertsuconic
 *
 *
 */
public class NonHATopologyTest extends ServiceTestBase
{

   public void testNetty() throws Exception
   {
      internalTest(true);
   }

   public void testInVM() throws Exception
   {
      internalTest(false);
   }

   public void internalTest(boolean isNetty) throws Exception
   {

      HornetQServer server = null;
      ServerLocatorInternal locator = null;

      try
      {

         server = createServer(false, isNetty);

         if (!isNetty)
         {
            server.getConfiguration()
                  .getAcceptorConfigurations()
                  .add(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY));
            server.getConfiguration()
                  .getConnectorConfigurations()
                  .put("netty", new TransportConfiguration(NETTY_CONNECTOR_FACTORY));

            server.getConfiguration().setClustered(true);

            ArrayList<String> list = new ArrayList<String>();
            list.add("netty");
            server.getConfiguration()
                  .getClusterConfigurations()
                  .add(new ClusterConnectionConfiguration("tst", "jms", "netty", 1000, true, false, 1, 1000, list, true));

         }

         server.start();

         locator = (ServerLocatorInternal)createNonHALocator(isNetty);

         ClientSessionFactory factory = createSessionFactory(locator);

         Topology topology = locator.getTopology();

         assertEquals(1, topology.getMembers().size());

         factory.close();

         if (!isNetty)
         {
            TopologyMember member = topology.getMembers().iterator().next();
            if (isNetty)
            {
               assertEquals(NettyConnectorFactory.class.getName(), member.getA().getFactoryClassName());
            }
            else
            {
               assertEquals(InVMConnectorFactory.class.getName(), member.getA().getFactoryClassName());
            }
         }

      }
      finally
      {
         try
         {
            locator.close();
         }
         catch (Exception ignored)
         {
         }

         try
         {
            server.stop();
         }
         catch (Exception ignored)
         {
         }

         server = null;

         locator = null;
      }

   }
}
