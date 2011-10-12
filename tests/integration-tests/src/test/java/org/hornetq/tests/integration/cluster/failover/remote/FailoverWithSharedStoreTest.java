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

package org.hornetq.tests.integration.cluster.failover.remote;

import java.util.ArrayList;
import java.util.List;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.server.JournalType;
import org.hornetq.tests.integration.cluster.distribution.ClusterTestBase;
import org.hornetq.tests.integration.cluster.util.RemoteProcessHornetQServer;
import org.hornetq.tests.integration.cluster.util.RemoteServerConfiguration;
import org.hornetq.tests.integration.cluster.util.TestableServer;

/**
 * A ServerTest
 *
 * @author jmesnil
 *
 *
 */
public class FailoverWithSharedStoreTest extends ClusterTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public class SharedLiveServerConfiguration extends RemoteServerConfiguration
   {

      @Override
      public Configuration getConfiguration()
      {
         Configuration config = createBasicConfig();
         config.setSecurityEnabled(false);
         config.setJournalType(JournalType.NIO);
         config.setSharedStore(true);
         config.setClustered(true);
         config.getAcceptorConfigurations().add(createTransportConfiguration(true, true, generateParams(0, true)));
         config.getConnectorConfigurations().put("self",
                                                 createTransportConfiguration(true, false, generateParams(0, true)));
         config.getClusterConfigurations().add(new ClusterConnectionConfiguration("cluster",
                                                                                  "foo",
                                                                                  "self",
                                                                                  -1,
                                                                                  false,
                                                                                  false,
                                                                                  1,
                                                                                  1,
                                                                                  new ArrayList<String>(), false));
         return config;
      }

   }

   public class SharedBackupServerConfiguration extends RemoteServerConfiguration
   {

      @Override
      public Configuration getConfiguration()
      {
         Configuration config = createBasicConfig();
         config.setSecurityEnabled(false);
         config.setJournalType(JournalType.NIO);
         config.setSharedStore(true);
         config.setBackup(true);
         config.setClustered(true);
         config.getAcceptorConfigurations().add(createTransportConfiguration(true, true, generateParams(1, true)));
         config.setLiveConnectorName("live");
         config.getConnectorConfigurations().put("live",
                                                 createTransportConfiguration(true, false, generateParams(0, true)));
         config.getConnectorConfigurations().put("self",
                                                 createTransportConfiguration(true, false, generateParams(1, true)));
         List<String> connectors = new ArrayList<String>();
         connectors.add("live");
         config.getClusterConfigurations().add(new ClusterConnectionConfiguration("cluster",
                                                                                  "foo",
                                                                                  "self",
                                                                                  -1,
                                                                                  false,
                                                                                  false,
                                                                                  1,
                                                                                  1,
                                                                                  connectors, false));
         return config;
      }

   }

   protected TestableServer createLiveServer() {
      return new RemoteProcessHornetQServer(SharedLiveServerConfiguration.class.getName());
   }
   
   protected TestableServer createBackupServer() {
      return new RemoteProcessHornetQServer(SharedBackupServerConfiguration.class.getName());
   }
   
   public void testCrashLiveServer() throws Exception
   {
      TestableServer liveServer = null;
      TestableServer backupServer = null;
      try
      {
         liveServer = createLiveServer();
         backupServer = createBackupServer();
         
         liveServer.start();
         backupServer.start();
         
         ServerLocator locator = HornetQClient.createServerLocatorWithHA(createTransportConfiguration(true,
                                                                                                      false,
                                                                                                      generateParams(0,
                                                                                                                     true)));
         locator.setFailoverOnInitialConnection(true);

         ClientSessionFactory sf = locator.createSessionFactory();
         ClientSession prodSession = sf.createSession();
         prodSession.createQueue("foo", "bar", true);
         ClientProducer producer = prodSession.createProducer("foo");
         ClientMessage message = prodSession.createMessage(true);
         message.putStringProperty("key", "value");
         producer.send(message);
         prodSession.commit();
         prodSession.close();

         liveServer.crash();
         liveServer = null;
         Thread.sleep(5000);

         sf = locator.createSessionFactory();
         ClientSession consSession = sf.createSession();
         consSession.start();
         ClientConsumer consumer = consSession.createConsumer("bar");
         ClientMessage receivedMessage = consumer.receive(5000);
         assertNotNull(receivedMessage);
         assertEquals(message.getStringProperty("key"), receivedMessage.getStringProperty("key"));
         receivedMessage.acknowledge();

         consumer.close();
         consSession.deleteQueue("bar");
         locator.close();

      }
      catch(Exception e)
      {
         e.printStackTrace();
      }
      finally
      {
         if (liveServer != null)
         {
            liveServer.stop();
         }
         if (backupServer != null)
         {
            backupServer.stop();
         }
      }

   }

   public void testNoConnection() throws Exception
   {
      ServerLocator locator = HornetQClient.createServerLocatorWithHA(new TransportConfiguration(NettyConnectorFactory.class.getName()));
      try
      {
         locator.createSessionFactory();
         fail();
      }
      catch (HornetQException e)
      {
         assertEquals(HornetQException.NOT_CONNECTED, e.getCode());
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
