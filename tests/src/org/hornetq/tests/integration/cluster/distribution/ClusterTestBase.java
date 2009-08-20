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

package org.hornetq.tests.integration.cluster.distribution;

import static org.hornetq.core.remoting.impl.invm.TransportConstants.SERVER_ID_PROP_NAME;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.client.impl.ConnectionManagerImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.cluster.BroadcastGroupConfiguration;
import org.hornetq.core.config.cluster.ClusterConnectionConfiguration;
import org.hornetq.core.config.cluster.DiscoveryGroupConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.Bindings;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.postoffice.QueueBinding;
import org.hornetq.core.postoffice.impl.LocalQueueBinding;
import org.hornetq.core.server.JournalType;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.cluster.ClusterConnection;
import org.hornetq.core.server.cluster.RemoteQueueBinding;
import org.hornetq.integration.transports.netty.TransportConstants;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.Pair;
import org.hornetq.utils.SimpleString;

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

   private static final int[] PORTS = {TransportConstants.DEFAULT_PORT,
                                       TransportConstants.DEFAULT_PORT + 1,
                                       TransportConstants.DEFAULT_PORT + 2,
                                       TransportConstants.DEFAULT_PORT + 3,
                                       TransportConstants.DEFAULT_PORT + 4,
                                       TransportConstants.DEFAULT_PORT + 5,
                                       TransportConstants.DEFAULT_PORT + 6,
                                       TransportConstants.DEFAULT_PORT + 7,
                                       TransportConstants.DEFAULT_PORT + 8,
                                       TransportConstants.DEFAULT_PORT + 9,
   };

   private static final long WAIT_TIMEOUT = 10000;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      checkFreePort(PORTS);
      
      clearData();
   }
   
   @Override
   protected void tearDown() throws Exception
   {
      checkFreePort(PORTS);
      
      servers = null;

      sfs = null;
      
      consumers = null;
      
      consumers = new ConsumerHolder[MAX_CONSUMERS];

      super.tearDown();
   }

   // Private -------------------------------------------------------------------------------------------------------

   private static final int MAX_CONSUMERS = 100;

   private static class ConsumerHolder
   {
      final ClientConsumer consumer;

      final ClientSession session;

      ConsumerHolder(final ClientConsumer consumer, final ClientSession session)
      {
         this.consumer = consumer;

         this.session = session;
      }
   }

   private ConsumerHolder[] consumers = new ConsumerHolder[MAX_CONSUMERS];

   private static final SimpleString COUNT_PROP = new SimpleString("count_prop");

   protected static final SimpleString FILTER_PROP = new SimpleString("animal");

   private static final int MAX_SERVERS = 10;

   private HornetQServer[] servers = new HornetQServer[MAX_SERVERS];

   private ClientSessionFactory[] sfs = new ClientSessionFactory[MAX_SERVERS];

   protected void failNode(TransportConfiguration conf)
   {
      ConnectionManagerImpl.failAllConnectionsForConnector(conf);
   }

   protected void waitForMessages(int node, final String address, final int count) throws Exception
   {
      HornetQServer server = this.servers[node];

      if (server == null)
      {
         throw new IllegalArgumentException("No server at " + node);
      }

      PostOffice po = server.getPostOffice();

      long start = System.currentTimeMillis();

      int messageCount = 0;

      do
      {
         messageCount = getMessageCount(po, address);

         // log.info(node + " messageCount " + messageCount);

         if (messageCount == count)
         {
            // log.info("Waited " + (System.currentTimeMillis() - start));
            return;
         }

         Thread.sleep(100);
      }
      while (System.currentTimeMillis() - start < WAIT_TIMEOUT);

      //System.out.println(threadDump(" - fired by ClusterTestBase::waitForBindings"));

      throw new IllegalStateException("Timed out waiting for messages (messageCount = " + messageCount +
                                      ", expecting = " +
                                      count);
   }

   protected void waitForBindings(int node,
                                  final String address,
                                  final int count,
                                  final int consumerCount,
                                  final boolean local) throws Exception
   {
//      log.info("waiting for bindings on node " + node +
//               " address " +
//               address +
//               " count " +
//               count +
//               " consumerCount " +
//               consumerCount +
//               " local " +
//               local);
      HornetQServer server = this.servers[node];

      if (server == null)
      {
         throw new IllegalArgumentException("No server at " + node);
      }

      PostOffice po = server.getPostOffice();

      long start = System.currentTimeMillis();

      int bindingCount = 0;

      int totConsumers = 0;

      do
      {
         bindingCount = 0;

         totConsumers = 0;

         Bindings bindings = po.getBindingsForAddress(new SimpleString(address));

         for (Binding binding : bindings.getBindings())
         {
            if ((binding instanceof LocalQueueBinding && local) || (binding instanceof RemoteQueueBinding && !local))
            {
               QueueBinding qBinding = (QueueBinding)binding;

               bindingCount++;

               totConsumers += qBinding.consumerCount();
            }
         }

         //log.info(node + " binding count " + bindingCount + " consumer Count " + totConsumers);

         if (bindingCount == count && totConsumers == consumerCount)
         {
            // log.info("Waited " + (System.currentTimeMillis() - start));
            return;
         }

         Thread.sleep(100);
      }
      while (System.currentTimeMillis() - start < WAIT_TIMEOUT);

      // System.out.println(threadDump(" - fired by ClusterTestBase::waitForBindings"));

      String msg = "Timed out waiting for bindings (bindingCount = " + bindingCount +
                   ", totConsumers = " +
                   totConsumers;

      log.error(msg);

      throw new IllegalStateException(msg);
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

      session.createQueue(address, queueName, filterString, durable);

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
      try
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

         consumers[consumerID] = new ConsumerHolder(consumer, session);
      }
      catch (Exception e)
      {
         // Proxy the faliure and print a dump into System.out, so it is captured by Hudson reports
         e.printStackTrace();
         System.out.println(threadDump(" - fired by ClusterTestBase::addConsumer"));

         throw e;
      }
   }

   protected void removeConsumer(int consumerID) throws Exception
   {
      ConsumerHolder holder = consumers[consumerID];

      if (holder == null)
      {
         throw new IllegalArgumentException("No consumer at " + consumerID);
      }

      holder.consumer.close();
      holder.session.close();

      consumers[consumerID] = null;
   }

   protected void closeAllConsumers() throws Exception
   {
      for (int i = 0; i < consumers.length; i++)
      {
         ConsumerHolder holder = consumers[i];

         if (holder != null)
         {
            holder.consumer.close();
            holder.session.close();

            consumers[i] = null;
         }
      }
   }

   protected void closeAllSessionFactories() throws Exception
   {
      for (int i = 0; i < sfs.length; i++)
      {
         ClientSessionFactory sf = sfs[i];

         if (sf != null)
         {
            sf.close();

            sfs[i] = null;
         }
      }
   }

   protected void closeSessionFactory(int node)
   {
      ClientSessionFactory sf = this.sfs[node];

      if (sf == null)
      {
         throw new IllegalArgumentException("No sf at " + node);
      }

      sf.close();

      sfs[node] = null;
   }

   protected void sendInRange(int node, String address, int msgStart, int msgEnd, boolean durable, String filterVal) throws Exception
   {
      ClientSessionFactory sf = this.sfs[node];

      if (sf == null)
      {
         throw new IllegalArgumentException("No sf at " + node);
      }

      ClientSession session = sf.createSession(false, true, true);

      ClientProducer producer = session.createProducer(address);

      for (int i = msgStart; i < msgEnd; i++)
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

   protected void send(int node, String address, int numMessages, boolean durable, String filterVal) throws Exception
   {
      sendInRange(node, address, 0, numMessages, durable, filterVal);
   }

   protected void verifyReceiveAllInRange(boolean ack, int msgStart, int msgEnd, int... consumerIDs) throws Exception
   {
      verifyReceiveAllInRangeNotBefore(ack, -1, msgStart, msgEnd, consumerIDs);
   }

   protected void verifyReceiveAllInRange(int msgStart, int msgEnd, int... consumerIDs) throws Exception
   {
      verifyReceiveAllInRangeNotBefore(false, -1, msgStart, msgEnd, consumerIDs);
   }

   protected void verifyReceiveAllInRangeNotBefore(boolean ack,
                                                   long firstReceiveTime,
                                                   int msgStart,
                                                   int msgEnd,
                                                   int... consumerIDs) throws Exception
   {
      boolean outOfOrder = false;
      for (int i = 0; i < consumerIDs.length; i++)
      {
         ConsumerHolder holder = consumers[consumerIDs[i]];

         if (holder == null)
         {
            throw new IllegalArgumentException("No consumer at " + consumerIDs[i]);
         }

         for (int j = msgStart; j < msgEnd; j++)
         {
            ClientMessage message = holder.consumer.receive(2000);

            if (message == null)
            {
               log.info("*** dumping consumers:");

               dumpConsumers();

               assertNotNull("consumer " + consumerIDs[i] + " did not receive message " + j, message);
            }
           
            if (ack)
            {
               message.acknowledge();
            }

            if (firstReceiveTime != -1)
            {
               assertTrue("Message received too soon", System.currentTimeMillis() >= firstReceiveTime);
            }

            if (j != (Integer)(message.getProperty(COUNT_PROP)))
            {
               outOfOrder = true;
               System.out.println("Message j=" + j + " was received out of order = " + message.getProperty(COUNT_PROP));
            }
         }
      }

      assertFalse("Messages were consumed out of order, look at System.out for more information", outOfOrder);
   }

   private void dumpConsumers() throws Exception
   {
      for (int i = 0; i < consumers.length; i++)
      {
         if (consumers[i] != null)
         {
            log.info("Dumping consumer " + i);

            checkReceive(i);
         }
      }
   }

   protected void verifyReceiveAll(boolean ack, int numMessages, int... consumerIDs) throws Exception
   {
      verifyReceiveAllInRange(ack, 0, numMessages, consumerIDs);
   }

   protected void verifyReceiveAll(int numMessages, int... consumerIDs) throws Exception
   {
      verifyReceiveAllInRange(false, 0, numMessages, consumerIDs);
   }

   protected void verifyReceiveAllNotBefore(long firstReceiveTime, int numMessages, int... consumerIDs) throws Exception
   {
      verifyReceiveAllInRangeNotBefore(false, firstReceiveTime, 0, numMessages, consumerIDs);
   }

   protected void checkReceive(int... consumerIDs) throws Exception
   {
      for (int i = 0; i < consumerIDs.length; i++)
      {
         ConsumerHolder holder = consumers[consumerIDs[i]];

         if (holder == null)
         {
            throw new IllegalArgumentException("No consumer at " + consumerIDs[i]);
         }

         ClientMessage message;
         do
         {
            message = holder.consumer.receive(500);

            if (message != null)
            {
               log.info("check receive Consumer " + consumerIDs[i] +
                        " received message " +
                        message.getProperty(COUNT_PROP));
            }
            else
            {
               log.info("check receive Consumer " + consumerIDs[i] + " null message");
            }
         }
         while (message != null);

      }
   }

   protected void verifyReceiveRoundRobin(int numMessages, int... consumerIDs) throws Exception
   {
      int count = 0;

      for (int i = 0; i < numMessages; i++)
      {
         ConsumerHolder holder = consumers[consumerIDs[count]];

         if (holder == null)
         {
            throw new IllegalArgumentException("No consumer at " + consumerIDs[i]);
         }

         ClientMessage message = holder.consumer.receive(500);

         assertNotNull("consumer " + consumerIDs[count] + " did not receive message " + i, message);

         assertEquals("consumer " + consumerIDs[count] + " message " + i, i, message.getProperty(COUNT_PROP));

         count++;

         if (count == consumerIDs.length)
         {
            count = 0;
         }
      }
   }

   /*
    * With some tests we cannot guarantee the order in which the bridges in the cluster startup so the round robin order is not predefined.
    * In which case we test the messages are round robin'd in any specific order that contains all the consumers
    */
   protected void verifyReceiveRoundRobinInSomeOrder(int numMessages, int... consumerIDs) throws Exception
   {
      verifyReceiveRoundRobinInSomeOrder(true, numMessages, consumerIDs);
   }

   protected void verifyReceiveRoundRobinInSomeOrder(boolean ack, int numMessages, int... consumerIDs) throws Exception
   {
      Map<Integer, Integer> countMap = new HashMap<Integer, Integer>();

      Set<Integer> counts = new HashSet<Integer>();

      for (int i = 0; i < consumerIDs.length; i++)
      {
         ConsumerHolder holder = consumers[consumerIDs[i]];

         if (holder == null)
         {
            throw new IllegalArgumentException("No consumer at " + consumerIDs[i]);
         }

         ClientMessage message;
         do
         {
            message = holder.consumer.receive(200);

            if (message != null)
            {
               int count = (Integer)message.getProperty(COUNT_PROP);

               Integer prevCount = countMap.get(i);

               if (prevCount != null)
               {
                  assertTrue(count == prevCount + consumerIDs.length);
               }

               assertFalse(counts.contains(count));

               counts.add(count);

               countMap.put(i, count);

               if (ack)
               {
                  message.acknowledge();
               }

               //log.info("consumer " + consumerIDs[i] +" returns " + count);
            }
            else
            {
              // log.info("consumer " + consumerIDs[i] +" returns null");
            }
         }
         while (message != null);
      }

      for (int i = 0; i < numMessages; i++)
      {
         assertTrue(counts.contains(i));
      }
   }

   protected void verifyReceiveRoundRobinInSomeOrderWithCounts(boolean ack, int[] messageCounts, int... consumerIDs) throws Exception
   {
      List<LinkedList<Integer>> receivedCounts = new ArrayList<LinkedList<Integer>>();

      Set<Integer> counts = new HashSet<Integer>();

      for (int i = 0; i < consumerIDs.length; i++)
      {
         ConsumerHolder holder = consumers[consumerIDs[i]];

         if (holder == null)
         {
            throw new IllegalArgumentException("No consumer at " + consumerIDs[i]);
         }

         LinkedList<Integer> list = new LinkedList<Integer>();

         receivedCounts.add(list);

         ClientMessage message;
         do
         {
            message = holder.consumer.receive(1000);

            if (message != null)
            {
               int count = (Integer)message.getProperty(COUNT_PROP);

               // log.info("consumer " + consumerIDs[i] + " received message " + count);

               assertFalse(counts.contains(count));

               counts.add(count);

               list.add(count);

               if (ack)
               {
                  message.acknowledge();
               }
            }
         }
         while (message != null);
      }

      for (int i = 0; i < messageCounts.length; i++)
      {
         assertTrue(counts.contains(messageCounts[i]));
      }

      LinkedList[] lists = new LinkedList[consumerIDs.length];

      for (int i = 0; i < messageCounts.length; i++)
      {
         for (LinkedList<Integer> list : receivedCounts)
         {
            int elem = list.get(0);

            if (elem == messageCounts[i])
            {
               lists[i] = list;

               break;
            }
         }
      }
      int index = 0;

      for (int i = 0; i < messageCounts.length; i++)
      {
         LinkedList list = lists[index];

         assertNotNull(list);

         int elem = (Integer)list.poll();

         assertEquals(messageCounts[i], elem);

         // log.info("got elem " + messageCounts[i] + " at pos " + index);

         index++;

         if (index == lists.length)
         {
            index = 0;
         }
      }

   }

   protected void verifyReceiveRoundRobinInSomeOrderNoAck(int numMessages, int... consumerIDs) throws Exception
   {
      verifyReceiveRoundRobinInSomeOrder(false, numMessages, consumerIDs);
   }

   protected int[] getReceivedOrder(int consumerID) throws Exception
   {
      ConsumerHolder consumer = this.consumers[consumerID];

      if (consumer == null)
      {
         throw new IllegalArgumentException("No consumer at " + consumerID);
      }

      List<Integer> ints = new ArrayList<Integer>();

      ClientMessage message = null;

      do
      {
         message = consumer.consumer.receive(500);

         if (message != null)
         {
            int count = (Integer)message.getProperty(COUNT_PROP);

            ints.add(count);
         }
      }
      while (message != null);

      int[] res = new int[ints.size()];

      int j = 0;

      for (Integer i : ints)
      {
         res[j++] = i;
      }

      return res;
   }

   protected void verifyNotReceive(int... consumerIDs) throws Exception
   {
      for (int i = 0; i < consumerIDs.length; i++)
      {
         ConsumerHolder holder = consumers[consumerIDs[i]];

         if (holder == null)
         {
            throw new IllegalArgumentException("No consumer at " + consumerIDs[i]);
         }

         assertNull("consumer " + i + " received message", holder.consumer.receive(200));
      }
   }

   protected void setupSessionFactory(int node, boolean netty)
   {
      if (sfs[node] != null)
      {
         throw new IllegalArgumentException("Already a server at " + node);
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

   protected void setupSessionFactory(int node, int backupNode, boolean netty, boolean blocking)
   {
      if (sfs[node] != null)
      {
         throw new IllegalArgumentException("Already a server at " + node);
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

      Map<String, Object> backupParams = generateParams(backupNode, netty);

      TransportConfiguration serverBackuptc;

      if (netty)
      {
         serverBackuptc = new TransportConfiguration(NETTY_CONNECTOR_FACTORY, backupParams);
      }
      else
      {
         serverBackuptc = new TransportConfiguration(INVM_CONNECTOR_FACTORY, backupParams);
      }

      ClientSessionFactory sf = new ClientSessionFactoryImpl(serverTotc, serverBackuptc);
      
      sf.setFailoverOnServerShutdown(false);
      sf.setRetryInterval(100);
      sf.setRetryIntervalMultiplier(1d);
      sf.setReconnectAttempts(-1);
      sf.setBlockOnNonPersistentSend(blocking);
      sf.setBlockOnPersistentSend(blocking);

      sfs[node] = sf;
   }

   protected void setupSessionFactory(int node, int backupNode, boolean netty)
   {
      this.setupSessionFactory(node, backupNode, netty, true);
   }

   protected HornetQServer getServer(int node)
   {
      if (servers[node] == null)
      {
         throw new IllegalArgumentException("No server at node " + node);
      }

      return servers[node];
   }

   protected void setupServer(int node, boolean fileStorage, boolean netty)
   {
      setupServer(node, fileStorage, netty, false, -1);
   }

   protected void setupServer(int node, boolean fileStorage, boolean netty, boolean backup)
   {
      setupServer(node, fileStorage, netty, backup, -1);
   }

   protected void setupServer(int node, boolean fileStorage, boolean netty, int backupNode)
   {
      setupServer(node, fileStorage, netty, false, backupNode);
   }

   protected void setupServer(int node, boolean fileStorage, boolean netty, boolean backup, int backupNode)
   {
      if (servers[node] != null)
      {
         throw new IllegalArgumentException("Already a server at node " + node);
      }

      Configuration configuration = new ConfigurationImpl();

      configuration.setSecurityEnabled(false);
      configuration.setBindingsDirectory(getBindingsDir(node, backup));
      configuration.setJournalMinFiles(2);
      configuration.setJournalMaxAIO(1000);
      configuration.setJournalDirectory(getJournalDir(node, backup));
      configuration.setJournalFileSize(100 * 1024);
      configuration.setJournalType(JournalType.ASYNCIO);
      configuration.setJournalMaxAIO(1000);
      configuration.setPagingDirectory(getPageDir(node, backup));
      configuration.setLargeMessagesDirectory(getLargeMessagesDir(node, backup));
      configuration.setClustered(true);
      configuration.setJournalCompactMinFiles(0);
      configuration.setBackup(backup);

      if (backupNode != -1)
      {
         Map<String, Object> backupParams = generateParams(backupNode, netty);

         if (netty)
         {
            TransportConfiguration nettyBackuptc = new TransportConfiguration(NETTY_CONNECTOR_FACTORY, backupParams);

            configuration.getConnectorConfigurations().put(nettyBackuptc.getName(), nettyBackuptc);

            configuration.setBackupConnectorName(nettyBackuptc.getName());
         }
         else
         {
            TransportConfiguration invmBackuptc = new TransportConfiguration(INVM_CONNECTOR_FACTORY, backupParams);

            configuration.getConnectorConfigurations().put(invmBackuptc.getName(), invmBackuptc);

            configuration.setBackupConnectorName(invmBackuptc.getName());
         }
      }

      configuration.getAcceptorConfigurations().clear();

      Map<String, Object> params = generateParams(node, netty);

      TransportConfiguration invmtc = new TransportConfiguration(INVM_ACCEPTOR_FACTORY, params);
      configuration.getAcceptorConfigurations().add(invmtc);

      if (netty)
      {
         TransportConfiguration nettytc = new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params);
         configuration.getAcceptorConfigurations().add(nettytc);
      }

      HornetQServer server;

      if (fileStorage)
      {
         server = HornetQ.newHornetQServer(configuration);
      }
      else
      {
         server = HornetQ.newHornetQServer(configuration, false);
      }
      servers[node] = server;
   }

   protected void setupServerWithDiscovery(int node,
                                           String groupAddress,
                                           int port,
                                           boolean fileStorage,
                                           boolean netty,
                                           boolean backup)
   {
      setupServerWithDiscovery(node, groupAddress, port, fileStorage, netty, backup, -1);
   }

   protected void setupServerWithDiscovery(int node,
                                           String groupAddress,
                                           int port,
                                           boolean fileStorage,
                                           boolean netty,
                                           int backupNode)
   {
      setupServerWithDiscovery(node, groupAddress, port, fileStorage, netty, false, backupNode);
   }

   protected void setupServerWithDiscovery(int node,
                                           String groupAddress,
                                           int port,
                                           boolean fileStorage,
                                           boolean netty,
                                           boolean backup,
                                           int backupNode)
   {
      if (servers[node] != null)
      {
         throw new IllegalArgumentException("Already a server at node " + node);
      }

      Configuration configuration = new ConfigurationImpl();

      configuration.setSecurityEnabled(false);
      configuration.setBindingsDirectory(getBindingsDir(node, false));
      configuration.setJournalMinFiles(2);
      configuration.setJournalDirectory(getJournalDir(node, false));
      configuration.setJournalFileSize(100 * 1024);
      configuration.setJournalType(JournalType.ASYNCIO);
      configuration.setJournalMaxAIO(1000);
      configuration.setPagingDirectory(getPageDir(node, false));
      configuration.setLargeMessagesDirectory(getLargeMessagesDir(node, false));
      configuration.setClustered(true);
      configuration.setBackup(backup);

      TransportConfiguration nettyBackuptc = null;
      TransportConfiguration invmBackuptc = null;

      if (backupNode != -1)
      {
         Map<String, Object> backupParams = generateParams(backupNode, netty);

         if (netty)
         {
            nettyBackuptc = new TransportConfiguration(NETTY_CONNECTOR_FACTORY, backupParams);

            configuration.getConnectorConfigurations().put(nettyBackuptc.getName(), nettyBackuptc);

            configuration.setBackupConnectorName(nettyBackuptc.getName());
         }
         else
         {
            invmBackuptc = new TransportConfiguration(INVM_CONNECTOR_FACTORY, backupParams);

            configuration.getConnectorConfigurations().put(invmBackuptc.getName(), invmBackuptc);

            configuration.setBackupConnectorName(invmBackuptc.getName());
         }
      }

      configuration.getAcceptorConfigurations().clear();

      Map<String, Object> params = generateParams(node, netty);

      TransportConfiguration invmtc = new TransportConfiguration(INVM_ACCEPTOR_FACTORY, params);
      configuration.getAcceptorConfigurations().add(invmtc);

      if (netty)
      {
         TransportConfiguration nettytc = new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params);
         configuration.getAcceptorConfigurations().add(nettytc);
      }

      TransportConfiguration invmtc_c = new TransportConfiguration(INVM_CONNECTOR_FACTORY, params);
      configuration.getConnectorConfigurations().put(invmtc_c.getName(), invmtc_c);

      List<Pair<String, String>> connectorPairs = new ArrayList<Pair<String, String>>();

      if (netty)
      {
         TransportConfiguration nettytc_c = new TransportConfiguration(NETTY_CONNECTOR_FACTORY, params);
         configuration.getConnectorConfigurations().put(nettytc_c.getName(), nettytc_c);

         connectorPairs.add(new Pair<String, String>(nettytc_c.getName(),
                                                     nettyBackuptc == null ? null : nettyBackuptc.getName()));
      }
      else
      {
         connectorPairs.add(new Pair<String, String>(invmtc_c.getName(), invmBackuptc == null ? null
                                                                                             : invmBackuptc.getName()));
      }

      BroadcastGroupConfiguration bcConfig = new BroadcastGroupConfiguration("bg1",
                                                                             null,
                                                                             -1,
                                                                             groupAddress,
                                                                             port,
                                                                             250,
                                                                             connectorPairs);

      configuration.getBroadcastGroupConfigurations().add(bcConfig);

      DiscoveryGroupConfiguration dcConfig = new DiscoveryGroupConfiguration("dg1", groupAddress, port, 5000);

      configuration.getDiscoveryGroupConfigurations().put(dcConfig.getName(), dcConfig);

      HornetQServer server;

      if (fileStorage)
      {
         server = HornetQ.newHornetQServer(configuration);
      }
      else
      {
         server = HornetQ.newHornetQServer(configuration, false);
      }
      servers[node] = server;
   }

   protected Map<String, Object> generateParams(int node, boolean netty)
   {
      Map<String, Object> params = new HashMap<String, Object>();
      params.put(SERVER_ID_PROP_NAME, node);

      if (netty)
      {
         params.put(org.hornetq.integration.transports.netty.TransportConstants.PORT_PROP_NAME,
                    org.hornetq.integration.transports.netty.TransportConstants.DEFAULT_PORT + node);
      }

      return params;
   }

   protected void clearServer(int... nodes)
   {
      for (int i = 0; i < nodes.length; i++)
      {
         if (servers[nodes[i]] == null)
         {
            throw new IllegalArgumentException("No server at node " + nodes[i]);
         }

         servers[nodes[i]] = null;
      }
   }

   protected void clearAllServers()
   {
      for (int i = 0; i < servers.length; i++)
      {
         servers[i] = null;
      }
   }

   protected void setupClusterConnection(String name,
                                         int nodeFrom,
                                         int nodeTo,
                                         String address,
                                         boolean forwardWhenNoConsumers,
                                         int maxHops,
                                         boolean netty)
   {
      HornetQServer serverFrom = servers[nodeFrom];

      if (serverFrom == null)
      {
         throw new IllegalStateException("No server at node " + nodeFrom);
      }

      // Map<String, TransportConfiguration> connectors = serviceFrom
      // .getConfiguration()
      // .getConnectorConfigurations();

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

      serverFrom.getConfiguration().getConnectorConfigurations().put(serverTotc.getName(), serverTotc);

      Pair<String, String> connectorPair = new Pair<String, String>(serverTotc.getName(), null);

      List<Pair<String, String>> pairs = new ArrayList<Pair<String, String>>();
      pairs.add(connectorPair);

      ClusterConnectionConfiguration clusterConf = new ClusterConnectionConfiguration(name,
                                                                                      address,
                                                                                      100,
                                                                                      true,
                                                                                      forwardWhenNoConsumers,
                                                                                      maxHops,
                                                                                      pairs);
      serverFrom.getConfiguration().getClusterConfigurations().add(clusterConf);
   }

   protected void setupClusterConnection(String name,
                                         String address,
                                         boolean forwardWhenNoConsumers,
                                         int maxHops,
                                         boolean netty,
                                         int nodeFrom,
                                         int... nodesTo)
   {
      HornetQServer serverFrom = servers[nodeFrom];

      if (serverFrom == null)
      {
         throw new IllegalStateException("No server at node " + nodeFrom);
      }

      Map<String, TransportConfiguration> connectors = serverFrom.getConfiguration().getConnectorConfigurations();

      List<Pair<String, String>> pairs = new ArrayList<Pair<String, String>>();

      for (int i = 0; i < nodesTo.length; i++)
      {
         Map<String, Object> params = generateParams(nodesTo[i], netty);

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

         Pair<String, String> connectorPair = new Pair<String, String>(serverTotc.getName(), null);

         pairs.add(connectorPair);
      }

      ClusterConnectionConfiguration clusterConf = new ClusterConnectionConfiguration(name,
                                                                                      address,
                                                                                      250,
                                                                                      true,
                                                                                      forwardWhenNoConsumers,
                                                                                      maxHops,
                                                                                      pairs);

      serverFrom.getConfiguration().getClusterConfigurations().add(clusterConf);
   }

   protected void setupClusterConnectionWithBackups(String name,
                                                    String address,
                                                    boolean forwardWhenNoConsumers,
                                                    int maxHops,
                                                    boolean netty,
                                                    int nodeFrom,
                                                    int[] nodesTo,
                                                    int[] backupsTo)
   {
      HornetQServer serverFrom = servers[nodeFrom];

      if (serverFrom == null)
      {
         throw new IllegalStateException("No server at node " + nodeFrom);
      }

      Map<String, TransportConfiguration> connectors = serverFrom.getConfiguration().getConnectorConfigurations();

      List<Pair<String, String>> pairs = new ArrayList<Pair<String, String>>();

      for (int i = 0; i < nodesTo.length; i++)
      {
         Map<String, Object> params = generateParams(nodesTo[i], netty);

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

         Map<String, Object> backupParams = generateParams(backupsTo[i], netty);

         TransportConfiguration serverBackupTotc;

         if (netty)
         {
            serverBackupTotc = new TransportConfiguration(NETTY_CONNECTOR_FACTORY, backupParams);
         }
         else
         {
            serverBackupTotc = new TransportConfiguration(INVM_CONNECTOR_FACTORY, backupParams);
         }

         connectors.put(serverBackupTotc.getName(), serverBackupTotc);

         Pair<String, String> connectorPair = new Pair<String, String>(serverTotc.getName(), serverBackupTotc.getName());

         // Pair<String, String> connectorPair = new Pair<String, String>(serverTotc.getName(), null);

         pairs.add(connectorPair);
      }

      ClusterConnectionConfiguration clusterConf = new ClusterConnectionConfiguration(name,
                                                                                      address,
                                                                                      250,
                                                                                      true,
                                                                                      forwardWhenNoConsumers,
                                                                                      maxHops,
                                                                                      pairs);

      serverFrom.getConfiguration().getClusterConfigurations().add(clusterConf);
   }

   protected void setupDiscoveryClusterConnection(String name,
                                                  int node,
                                                  String discoveryGroupName,
                                                  String address,
                                                  boolean forwardWhenNoConsumers,
                                                  int maxHops,
                                                  boolean netty)
   {
      HornetQServer server = servers[node];

      if (server == null)
      {
         throw new IllegalStateException("No server at node " + node);
      }

      ClusterConnectionConfiguration clusterConf = new ClusterConnectionConfiguration(name,
                                                                                      address,
                                                                                      100,
                                                                                      true,
                                                                                      forwardWhenNoConsumers,
                                                                                      maxHops,
                                                                                      discoveryGroupName);
      List<ClusterConnectionConfiguration> clusterConfs = server.getConfiguration().getClusterConfigurations();

      clusterConfs.add(clusterConf);
   }

   protected void startServers(int... nodes) throws Exception
   {
      for (int i = 0; i < nodes.length; i++)
      {
         log.info("starting server " + nodes[i]);

         servers[nodes[i]].start();

         log.info("started server " + nodes[i]);
      }
   }

   protected void stopClusterConnections(int... nodes) throws Exception
   {
      for (int i = 0; i < nodes.length; i++)
      {
         if (servers[nodes[i]].isStarted())
         {
            for (ClusterConnection cc : servers[nodes[i]].getClusterManager().getClusterConnections())
            {
               cc.stop();
            }
         }
      }
   }

   protected void stopServers(int... nodes) throws Exception
   {
      for (int i = 0; i < nodes.length; i++)
      {
         if (servers[nodes[i]].isStarted())
         {
            try
            {
               log.info("stopping server " + nodes[i]);
               servers[nodes[i]].stop();
               log.info("server stopped");
            }
            catch (Exception e)
            {
               log.warn(e.getMessage(), e);
            }
         }
      }
   }

}
