/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.integration.cluster.distribution;

import static org.jboss.messaging.core.remoting.impl.invm.TransportConstants.SERVER_ID_PROP_NAME;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.cluster.BridgeConfiguration;
import org.jboss.messaging.core.config.cluster.ClusterConnectionConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.Bindings;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.postoffice.QueueBinding;
import org.jboss.messaging.core.postoffice.impl.LocalQueueBinding;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.cluster.RemoteQueueBinding;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.util.Pair;
import org.jboss.messaging.util.SimpleString;

/**
 * A ClusterTestBase
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 30 Jan 2009 11:29:43
 *
 *
 */
public class ClusterTestBase extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(ClusterTestBase.class);

   private static final long WAIT_TIMEOUT = 10000;
  
   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      clearData();
   }

   @Override
   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   // Private -------------------------------------------------------------------------------------------------------

   private static final int MAX_CONSUMERS = 100;

   private ClientConsumer[] consumers = new ClientConsumer[MAX_CONSUMERS];

   private static final SimpleString COUNT_PROP = new SimpleString("count_prop");

   protected static final SimpleString FILTER_PROP = new SimpleString("animal");

   private static final int MAX_SERVERS = 10;

   private MessagingService[] services = new MessagingService[MAX_SERVERS];

   private ClientSessionFactory[] sfs = new ClientSessionFactory[MAX_SERVERS];
   
   protected void waitForBindings(int node,
                                final String address,
                                final int count,
                                final int consumerCount,
                                final boolean local) throws Exception
   {
      //log.info("waiting for bindings on node " + node + " address " + address + " count " + count + " consumerCount " + consumerCount + " local " + local);
      MessagingService service = this.services[node];

      if (service == null)
      {
         throw new IllegalArgumentException("No service at " + node);
      }

      PostOffice po = service.getServer().getPostOffice();

      long start = System.currentTimeMillis();

      do
      {
         Bindings bindings = po.getBindingsForAddress(new SimpleString(address));
         
         int bindingCount = 0;

         int totConsumers = 0;

         for (Binding binding : bindings.getBindings())
         {
            if ((binding instanceof LocalQueueBinding && local) || (binding instanceof RemoteQueueBinding && !local))
            {
               QueueBinding qBinding = (QueueBinding)binding;

               bindingCount++;

               totConsumers += qBinding.consumerCount();
            }
         }
         
         log.info("binding count " + bindingCount + " consumer Count " + totConsumers);

         if (bindingCount == count && totConsumers == consumerCount)
         {
            log.info("Waited " + (System.currentTimeMillis() - start));
            return;
         }

         Thread.sleep(10);
      }
      while (System.currentTimeMillis() - start < WAIT_TIMEOUT);

      throw new IllegalStateException("Timed out waiting for bindings");
   }

   protected void createQueue(int node, String address, String queueName, String filterVal, boolean durable) throws Exception
   {
      ClientSessionFactory sf = this.sfs[node];

      if (sf == null)
      {
         throw new IllegalArgumentException("No sf at " + node);
      }

      ClientSession session = sf.createSession(false, true, true);

      String filterString = null;

      if (filterVal != null)
      {
         filterString = FILTER_PROP.toString() + "='" + filterVal + "'";
      }

      session.createQueue(address, queueName, filterString, durable, false);

      session.close();
   }

   protected void deleteQueue(int node, String queueName) throws Exception
   {
      ClientSessionFactory sf = this.sfs[node];

      if (sf == null)
      {
         throw new IllegalArgumentException("No sf at " + node);
      }

      ClientSession session = sf.createSession(false, true, true);

      session.deleteQueue(queueName);

      session.close();
   }

   protected void addConsumer(int consumerID, int node, String queueName, String filterVal) throws Exception
   {      
      if (consumers[consumerID] != null)
      {
         throw new IllegalArgumentException("Already a consumer at " + node);
      }

      ClientSessionFactory sf = this.sfs[node];

      if (sf == null)
      {
         throw new IllegalArgumentException("No sf at " + node);
      }

      ClientSession session = sf.createSession(false, true, true);

      String filterString = null;

      if (filterVal != null)
      {
         filterString = FILTER_PROP.toString() + "='" + filterVal + "'";
      }

      ClientConsumer consumer = session.createConsumer(queueName, filterString);

      session.start();

      consumers[consumerID] = consumer;
   }

   protected void removeConsumer(int consumerID) throws Exception
   {
      ClientConsumer consumer = consumers[consumerID];

      if (consumer == null)
      {
         throw new IllegalArgumentException("No consumer at " + consumerID);
      }

      consumer.close();
   }

   protected void send(int node, String address, int numMessages, boolean durable, String filterVal) throws Exception
   {
      ClientSessionFactory sf = this.sfs[node];

      if (sf == null)
      {
         throw new IllegalArgumentException("No sf at " + node);
      }

      ClientSession session = sf.createSession(false, true, true);

      ClientProducer producer = session.createProducer(address);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(durable);

         if (filterVal != null)
         {
            message.putStringProperty(FILTER_PROP, new SimpleString(filterVal));
         }

         message.putIntProperty(COUNT_PROP, i);

         producer.send(message);
      }

      session.close();
   }

   protected void verifyReceiveAll(int numMessages, int... consumerIDs) throws Exception
   {
      for (int i = 0; i < consumerIDs.length; i++)
      {
         ClientConsumer consumer = consumers[consumerIDs[i]];

         if (consumer == null)
         {
            throw new IllegalArgumentException("No consumer at " + consumerIDs[i]);
         }

         for (int j = 0; j < numMessages; j++)
         {
            ClientMessage message = consumer.receive(500);

            assertNotNull("consumer " + consumerIDs[i] + " did not receive message " + j, message);

            assertEquals(j, message.getProperty(COUNT_PROP));
         }
      }
   }

   protected void verifyReceiveRoundRobin(int numMessages, int... consumerIDs) throws Exception
   {
      int count = 0;

      for (int i = 0; i < numMessages; i++)
      {
         ClientConsumer consumer = consumers[consumerIDs[count]];

         if (consumer == null)
         {
            throw new IllegalArgumentException("No consumer at " + consumerIDs[i]);
         }

         ClientMessage message = consumer.receive(500);

         assertNotNull("consumer " + consumerIDs[count] + " did not receive message " + i, message);

         assertEquals(i, message.getProperty(COUNT_PROP));

         count++;

         if (count == consumerIDs.length)
         {
            count = 0;
         }
      }
   }

   protected void verifyNotReceive(int... consumerIDs) throws Exception
   {
      for (int i = 0; i < consumerIDs.length; i++)
      {
         ClientConsumer consumer = consumers[consumerIDs[i]];

         if (consumer == null)
         {
            throw new IllegalArgumentException("No consumer at " + consumerIDs[i]);
         }

         assertNull("consumer " + i + " received message", consumer.receive(200));
      }
   }

   protected void setupSessionFactory(int node, boolean netty)
   {
      if (sfs[node] != null)
      {
         throw new IllegalArgumentException("Already a service at " + node);
      }

      Map<String, Object> params = generateParams(node, netty);

      TransportConfiguration serverTotc;

      if (netty)
      {
         serverTotc = new TransportConfiguration(NETTY_CONNECTOR_FACTORY, params);
      }
      else
      {
         serverTotc = new TransportConfiguration(INVM_CONNECTOR_FACTORY, params);
      }

      ClientSessionFactory sf = new ClientSessionFactoryImpl(serverTotc);
      
      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      sfs[node] = sf;
   }

   protected void setupServer(int node, boolean fileStorage, boolean netty)
   {
      if (services[node] != null)
      {
         throw new IllegalArgumentException("Already a service at node " + node);
      }

      Configuration configuration = new ConfigurationImpl();

      configuration.setSecurityEnabled(false);
      configuration.setBindingsDirectory(getBindingsDir(node));
      configuration.setJournalMinFiles(2);
      configuration.setJournalDirectory(getJournalDir(node));
      configuration.setJournalFileSize(100 * 1024);
      configuration.setPagingDirectory(getPageDir(node));
      configuration.setLargeMessagesDirectory(getLargeMessagesDir(node));
      configuration.setClustered(true);

      configuration.getAcceptorConfigurations().clear();

      Map<String, Object> params = generateParams(node, netty);

      TransportConfiguration invmtc = new TransportConfiguration(INVM_ACCEPTOR_FACTORY, params);
      configuration.getAcceptorConfigurations().add(invmtc);

      if (netty)
      {
         TransportConfiguration nettytc = new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params);
         configuration.getAcceptorConfigurations().add(nettytc);
      }

      MessagingService service;

      if (fileStorage)
      {
         service = Messaging.newMessagingService(configuration);
      }
      else
      {
         service = Messaging.newNullStorageMessagingService(configuration);
      }
      services[node] = service;
   }

   private Map<String, Object> generateParams(int node, boolean netty)
   {
      Map<String, Object> params = new HashMap<String, Object>();
      params.put(SERVER_ID_PROP_NAME, node);

      if (netty)
      {
         params.put(org.jboss.messaging.integration.transports.netty.TransportConstants.PORT_PROP_NAME,
                    org.jboss.messaging.integration.transports.netty.TransportConstants.DEFAULT_PORT + node);
      }

      return params;
   }

   protected void clearServer(int node)
   {
      if (services[node] != null)
      {
         throw new IllegalArgumentException("No service at node " + node);
      }

      services[node] = null;
   }

   protected void setupClusterConnection(String name,
                                       int nodeFrom,
                                       int nodeTo,
                                       String address,
                                       boolean forwardWhenNoConsumers,
                                       boolean netty)
   {
      MessagingService serviceFrom = services[nodeFrom];

      Map<String, TransportConfiguration> connectors = serviceFrom.getServer()
                                                                  .getConfiguration()
                                                                  .getConnectorConfigurations();

      Map<String, Object> params = generateParams(nodeTo, netty);

      TransportConfiguration serverTotc;

      if (netty)
      {
         serverTotc = new TransportConfiguration(NETTY_CONNECTOR_FACTORY, params);
      }
      else
      {
         serverTotc = new TransportConfiguration(INVM_CONNECTOR_FACTORY, params);
      }

      connectors.put(serverTotc.getName(), serverTotc);

      serviceFrom.getServer().getConfiguration().setConnectorConfigurations(connectors);

      Pair<String, String> connectorPair = new Pair<String, String>(serverTotc.getName(), null);

      List<Pair<String, String>> pairs = new ArrayList<Pair<String, String>>();
      pairs.add(connectorPair);

      BridgeConfiguration bridgeConfiguration = new BridgeConfiguration(null,
                                                                        null,
                                                                        null,
                                                                        null,
                                                                        1,
                                                                        -1,
                                                                        null,
                                                                        10,
                                                                        1d,
                                                                        -1,
                                                                        -1,
                                                                        false,
                                                                        connectorPair);

      ClusterConnectionConfiguration clusterConf = new ClusterConnectionConfiguration(name,
                                                                                      address,
                                                                                      bridgeConfiguration,
                                                                                      true,
                                                                                      forwardWhenNoConsumers,
                                                                                      pairs);
      List<ClusterConnectionConfiguration> clusterConfs = serviceFrom.getServer()
                                                                     .getConfiguration()
                                                                     .getClusterConfigurations();

      clusterConfs.add(clusterConf);

      serviceFrom.getServer().getConfiguration().setClusterConfigurations(clusterConfs);
   }

   protected void startServers(int... nodes) throws Exception
   {
      for (int i = 0; i < nodes.length; i++)
      {
         services[nodes[i]].start();
      }
   }

   protected void stopServers(int... nodes) throws Exception
   {
      for (int i = 0; i < nodes.length; i++)
      {
         services[nodes[i]].stop();
      }
   }

}
