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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.BroadcastGroupConfiguration;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.Bindings;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.postoffice.QueueBinding;
import org.hornetq.core.postoffice.impl.LocalQueueBinding;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.core.server.NodeManager;
import org.hornetq.core.server.cluster.ClusterConnection;
import org.hornetq.core.server.cluster.ClusterManager;
import org.hornetq.core.server.cluster.RemoteQueueBinding;
import org.hornetq.core.server.group.GroupingHandler;
import org.hornetq.core.server.group.impl.GroupingHandlerConfiguration;
import org.hornetq.tests.integration.cluster.util.InVMNodeManager;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.UnitTestCase;

/**
 * A ClusterTestBase
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * Created 30 Jan 2009 11:29:43
 *
 *
 */
public abstract class ClusterTestBase extends ServiceTestBase
{
   private final Logger log = Logger.getLogger(this.getClass());
   public ClusterTestBase()
   {
      super();
   }

   public ClusterTestBase(String name)
   {
      super(name);
   }


   private static final int[] PORTS = { TransportConstants.DEFAULT_PORT,
                                       TransportConstants.DEFAULT_PORT + 1,
                                       TransportConstants.DEFAULT_PORT + 2,
                                       TransportConstants.DEFAULT_PORT + 3,
                                       TransportConstants.DEFAULT_PORT + 4,
                                       TransportConstants.DEFAULT_PORT + 5,
                                       TransportConstants.DEFAULT_PORT + 6,
                                       TransportConstants.DEFAULT_PORT + 7,
                                       TransportConstants.DEFAULT_PORT + 8,
                                       TransportConstants.DEFAULT_PORT + 9, };

   protected int getLargeMessageSize()
   {
      return 500;
   }

   protected boolean isLargeMessage()
   {
      return false;
   }


   private static final long TIMEOUT_START_SERVER = 400;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      forceGC();

      UnitTestCase.checkFreePort(ClusterTestBase.PORTS);

      clearData();

      consumers = new ConsumerHolder[ClusterTestBase.MAX_CONSUMERS];

      servers = new HornetQServer[ClusterTestBase.MAX_SERVERS];

      timeStarts = new long[ClusterTestBase.MAX_SERVERS];

      sfs = new ClientSessionFactory[ClusterTestBase.MAX_SERVERS];

      nodeManagers = new NodeManager[ClusterTestBase.MAX_SERVERS];

      for (int i = 0, nodeManagersLength = nodeManagers.length; i < nodeManagersLength; i++)
      {
         nodeManagers[i] = new InVMNodeManager();
      }

      locators = new ServerLocator[ClusterTestBase.MAX_SERVERS];

   }

   @Override
   protected void tearDown() throws Exception
   {
      log.info("#test tearDown");
      for (ServerLocator locator : locators)
      {
         try
         {
            locator.close();
         }
         catch (Exception e)
         {
            //
         }
      }

      locators = null;

      locators = new ServerLocator[ClusterTestBase.MAX_SERVERS];
      UnitTestCase.checkFreePort(ClusterTestBase.PORTS);

      servers = null;

      sfs = null;

      consumers = null;

      consumers = new ConsumerHolder[ClusterTestBase.MAX_CONSUMERS];

      nodeManagers = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------------------------------------------------------

   private static final int MAX_CONSUMERS = 100;

   private static class ConsumerHolder
   {
      final ClientConsumer consumer;

      final ClientSession session;

      final int id;

      ConsumerHolder(final int id, final ClientConsumer consumer, final ClientSession session)
      {
         this.id = id;

         this.consumer = consumer;

         this.session = session;
      }
   }

   private static final SimpleString COUNT_PROP = new SimpleString("count_prop");

   protected static final SimpleString FILTER_PROP = new SimpleString("animal");

   private static final int MAX_SERVERS = 10;

   protected ConsumerHolder[] consumers;

   protected HornetQServer[] servers;

   protected long[] timeStarts;

   protected NodeManager[] nodeManagers;

   protected ServerLocator[] locators;

   protected ClientSessionFactory[] sfs;

   protected ClientConsumer getConsumer(final int node)
   {
      return consumers[node].consumer;
   }

   protected void waitForMessages(final int node, final String address, final int count) throws Exception
   {
      HornetQServer server = servers[node];

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

         if (messageCount == count)
         {
            return;
         }

         Thread.sleep(10);
      }
      while (System.currentTimeMillis() - start < ClusterTestBase.WAIT_TIMEOUT);

      throw new IllegalStateException("Timed out waiting for messages (messageCount = " + messageCount +
                                      ", expecting = " +
                                      count);
   }

   protected void waitForServerRestart(final int node) throws Exception
   {
      long start = System.currentTimeMillis();
      do
      {
         if (servers[node].isInitialised())
         {
            return;
         }
         Thread.sleep(10);
      }
      while (System.currentTimeMillis() - start < ClusterTestBase.WAIT_TIMEOUT);
      String msg = "Timed out waiting for server starting = " + node;

      log.error(msg);

      throw new IllegalStateException(msg);
   }

   protected void waitForBindings(final int node,
                                  final String address,
                                  final int expectedBindingCount,
                                  final int expectedConsumerCount,
                                  final boolean local) throws Exception
   {
      log.debug("waiting for bindings on node " + node +
                " address " +
                address +
                " expectedBindingCount " +
                expectedBindingCount +
                " consumerCount " +
                expectedConsumerCount +
                " local " +
                local);

      HornetQServer server = servers[node];

      if (server == null)
      {
         throw new IllegalArgumentException("No server at " + node);
      }

      long timeout = ClusterTestBase.WAIT_TIMEOUT;
      

      if  (waitForBindings(server, address, local, expectedBindingCount, expectedConsumerCount, timeout))
      {
         return;
      }

      
      PostOffice po = server.getPostOffice();
      Bindings bindings = po.getBindingsForAddress(new SimpleString(address));

      System.out.println("=======================================================================");
      System.out.println("Binding information for address = " + address + " on node " + node);

      for (Binding binding : bindings.getBindings())
      {
         if (binding instanceof LocalQueueBinding && local || binding instanceof RemoteQueueBinding && !local)
         {
            QueueBinding qBinding = (QueueBinding)binding;

            System.out.println("Binding = " + qBinding + ", queue=" + qBinding.getQueue());
         }
      }

      StringWriter writer = new StringWriter();
      PrintWriter out = new PrintWriter(writer);

      try
      {
         for (HornetQServer hornetQServer : servers)
         {
            if (hornetQServer != null)
            {
               out.println(clusterDescription(hornetQServer));
               out.println(debugBindings(hornetQServer, hornetQServer.getConfiguration()
                                                                     .getManagementNotificationAddress()
                                                                     .toString()));
            }
         }

         for (HornetQServer hornetQServer : servers)
         {
            out.println("Management bindings on " + hornetQServer);
            if (hornetQServer != null)
            {
               out.println(debugBindings(hornetQServer, hornetQServer.getConfiguration()
                                                                     .getManagementNotificationAddress()
                                                                     .toString()));
            }
         }
      }
      catch (Throwable dontCare)
      {
      }

      logAndSystemOut(writer.toString());

      throw new IllegalStateException("Didn't get the expected number of bindings, look at the logging for more information");
   }

   protected String debugBindings(final HornetQServer server, final String address) throws Exception
   {

      StringWriter str = new StringWriter();
      PrintWriter out = new PrintWriter(str);

      if (server == null)
      {
         return "server is shutdown";
      }
      PostOffice po = server.getPostOffice();

      if (po == null)
      {
         return "server is shutdown";
      }
      Bindings bindings = po.getBindingsForAddress(new SimpleString(address));

      out.println("=======================================================================");
      out.println("Binding information for address = " + address + " on " + server);

      for (Binding binding : bindings.getBindings())
      {
         QueueBinding qBinding = (QueueBinding)binding;

         out.println("Binding = " + qBinding + ", queue=" + qBinding.getQueue());
      }
      out.println("=======================================================================");

      return str.toString();

   }

   protected void createQueue(final int node,
                              final String address,
                              final String queueName,
                              final String filterVal,
                              final boolean durable) throws Exception
   {
      ClientSessionFactory sf = sfs[node];

      if (sf == null)
      {
         throw new IllegalArgumentException("No sf at " + node);
      }

      ClientSession session = sf.createSession(false, true, true);

      String filterString = null;

      if (filterVal != null)
      {
         filterString = ClusterTestBase.FILTER_PROP.toString() + "='" + filterVal + "'";
      }

      log.info("Creating " + queueName + " , address " + address + " on " + servers[node]);

      session.createQueue(address, queueName, filterString, durable);

      session.close();
   }

   protected void deleteQueue(final int node, final String queueName) throws Exception
   {
      ClientSessionFactory sf = sfs[node];

      if (sf == null)
      {
         throw new IllegalArgumentException("No sf at " + node);
      }

      ClientSession session = sf.createSession(false, true, true);

      session.deleteQueue(queueName);

      session.close();
   }

   protected void addConsumer(final int consumerID, final int node, final String queueName, final String filterVal) throws Exception
   {
      try
      {
         if (consumers[consumerID] != null)
         {
            throw new IllegalArgumentException("Already a consumer at " + node);
         }

         ClientSessionFactory sf = sfs[node];

         if (sf == null)
         {
            throw new IllegalArgumentException("No sf at " + node);
         }

         ClientSession session = sf.createSession(false, true, true);

         String filterString = null;

         if (filterVal != null)
         {
            filterString = ClusterTestBase.FILTER_PROP.toString() + "='" + filterVal + "'";
         }

         ClientConsumer consumer = session.createConsumer(queueName, filterString);

         session.start();

         consumers[consumerID] = new ConsumerHolder(consumerID, consumer, session);
      }
      catch (Exception e)
      {
         // Proxy the faliure and print a dump into System.out, so it is captured by Hudson reports
         e.printStackTrace();
         System.out.println(UnitTestCase.threadDump(" - fired by ClusterTestBase::addConsumer"));

         throw e;
      }
   }

   protected void removeConsumer(final int consumerID) throws Exception
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
            // holder.session.close();

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

   protected void closeAllServerLocatorsFactories() throws Exception
   {
      for (int i = 0; i < locators.length; i++)
      {
         ServerLocator sf = locators[i];

         if (sf != null)
         {
            sf.close();

            locators[i] = null;
         }
      }
   }

   protected void closeSessionFactory(final int node)
   {
      ClientSessionFactory sf = sfs[node];

      if (sf == null)
      {
         throw new IllegalArgumentException("No sf at " + node);
      }

      sf.close();

      sfs[node] = null;
   }

   protected void sendInRange(final int node,
                              final String address,
                              final int msgStart,
                              final int msgEnd,
                              final boolean durable,
                              final String filterVal) throws Exception
   {
      sendInRange(node, address, msgStart, msgEnd, durable, filterVal, null);
   }

   protected void sendInRange(final int node,
                              final String address,
                              final int msgStart,
                              final int msgEnd,
                              final boolean durable,
                              final String filterVal,
                              final AtomicInteger duplicateDetectionSeq) throws Exception
   {
      ClientSessionFactory sf = sfs[node];

      if (sf == null)
      {
         throw new IllegalArgumentException("No sf at " + node);
      }

      ClientSession session = sf.createSession(false, false, false);

      try
      {
         ClientProducer producer = session.createProducer(address);

         for (int i = msgStart; i < msgEnd; i++)
         {
            ClientMessage message = session.createMessage(durable);

            if (filterVal != null)
            {
               message.putStringProperty(ClusterTestBase.FILTER_PROP, new SimpleString(filterVal));
            }

            if (duplicateDetectionSeq != null)
            {
               String str = Integer.toString(duplicateDetectionSeq.incrementAndGet());
               message.putStringProperty(Message.HDR_DUPLICATE_DETECTION_ID, new SimpleString(str));
            }

            message.putIntProperty(ClusterTestBase.COUNT_PROP, i);

            if (isLargeMessage())
            {
               message.setBodyInputStream(createFakeLargeStream(getLargeMessageSize()));
            }

            producer.send(message);

            if (i % 100 == 0)
            {
               session.commit();
            }
         }

         session.commit();
      }
      finally
      {
         session.close();
      }
   }

   protected void sendWithProperty(final int node,
                                   final String address,
                                   final int numMessages,
                                   final boolean durable,
                                   final SimpleString key,
                                   final SimpleString val) throws Exception
   {
      sendInRange(node, address, 0, numMessages, durable, key, val);
   }

   protected void sendInRange(final int node,
                              final String address,
                              final int msgStart,
                              final int msgEnd,
                              final boolean durable,
                              final SimpleString key,
                              final SimpleString val) throws Exception
   {
      ClientSessionFactory sf = sfs[node];

      if (sf == null)
      {
         throw new IllegalArgumentException("No sf at " + node);
      }

      ClientSession session = sf.createSession(false, true, true);

      try
      {
         ClientProducer producer = session.createProducer(address);

         for (int i = msgStart; i < msgEnd; i++)
         {
            ClientMessage message = session.createMessage(durable);

            if (isLargeMessage())
            {
               message.setBodyInputStream(createFakeLargeStream(getLargeMessageSize()));
            }

            message.putStringProperty(key, val);
            message.putIntProperty(ClusterTestBase.COUNT_PROP, i);

            producer.send(message);
         }
      }
      finally
      {
         session.close();
      }
   }

   protected void setUpGroupHandler(final GroupingHandlerConfiguration.TYPE type, final int node)
   {
      setUpGroupHandler(type, node, 5000);
   }

   protected void setUpGroupHandler(final GroupingHandlerConfiguration.TYPE type, final int node, final int timeout)
   {
      servers[node].getConfiguration()
                   .setGroupingHandlerConfiguration(new GroupingHandlerConfiguration(new SimpleString("grouparbitrator"),
                                                                                     type,
                                                                                     new SimpleString("queues"),
                                                                                     timeout));
   }

   protected void setUpGroupHandler(final GroupingHandler groupingHandler, final int node)
   {
      servers[node].setGroupingHandler(groupingHandler);
   }

   protected void send(final int node,
                       final String address,
                       final int numMessages,
                       final boolean durable,
                       final String filterVal) throws Exception
   {
      send(node, address, numMessages, durable, filterVal, null);
   }

   protected void send(final int node,
                       final String address,
                       final int numMessages,
                       final boolean durable,
                       final String filterVal,
                       final AtomicInteger duplicateDetectionCounter) throws Exception
   {
      sendInRange(node, address, 0, numMessages, durable, filterVal, duplicateDetectionCounter);
   }

   protected void verifyReceiveAllInRange(final boolean ack,
                                          final int msgStart,
                                          final int msgEnd,
                                          final int... consumerIDs) throws Exception
   {
      verifyReceiveAllInRangeNotBefore(ack, -1, msgStart, msgEnd, consumerIDs);
   }

   protected void verifyReceiveAllInRange(final int msgStart, final int msgEnd, final int... consumerIDs) throws Exception
   {
      verifyReceiveAllInRangeNotBefore(false, -1, msgStart, msgEnd, consumerIDs);
   }

   protected void verifyReceiveAllWithGroupIDRoundRobin(final int msgStart, final int msgEnd, final int... consumerIDs) throws Exception
   {
      verifyReceiveAllWithGroupIDRoundRobin(true, -1, msgStart, msgEnd, consumerIDs);
   }

   protected int verifyReceiveAllOnSingleConsumer(final int msgStart, final int msgEnd, final int... consumerIDs) throws Exception
   {
      return verifyReceiveAllOnSingleConsumer(true, msgStart, msgEnd, consumerIDs);
   }

   protected void verifyReceiveAllWithGroupIDRoundRobin(final boolean ack,
                                                        final long firstReceiveTime,
                                                        final int msgStart,
                                                        final int msgEnd,
                                                        final int... consumerIDs) throws Exception
   {
      HashMap<SimpleString, Integer> groupIdsReceived = new HashMap<SimpleString, Integer>();
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

               Assert.assertNotNull("consumer " + consumerIDs[i] + " did not receive message " + j, message);
            }

            if (ack)
            {
               message.acknowledge();
            }

            if (firstReceiveTime != -1)
            {
               Assert.assertTrue("Message received too soon", System.currentTimeMillis() >= firstReceiveTime);
            }

            SimpleString id = (SimpleString)message.getObjectProperty(Message.HDR_GROUP_ID);
            System.out.println("received " + id + " on consumer " + consumerIDs[i]);
            if (groupIdsReceived.get(id) == null)
            {
               groupIdsReceived.put(id, i);
            }
            else if (groupIdsReceived.get(id) != i)
            {
               Assert.fail("consumer " + groupIdsReceived.get(id) +
                           " already bound to groupid " +
                           id +
                           " received on consumer " +
                           i);
            }

         }

      }

   }

   protected int verifyReceiveAllOnSingleConsumer(final boolean ack,
                                                  final int msgStart,
                                                  final int msgEnd,
                                                  final int... consumerIDs) throws Exception
   {
      int groupIdsReceived = -1;
      for (int i = 0; i < consumerIDs.length; i++)
      {
         ConsumerHolder holder = consumers[consumerIDs[i]];

         if (holder == null)
         {
            throw new IllegalArgumentException("No consumer at " + consumerIDs[i]);
         }
         ClientMessage message = holder.consumer.receive(2000);
         if (message != null)
         {
            groupIdsReceived = i;
            for (int j = msgStart + 1; j < msgEnd; j++)
            {
               message = holder.consumer.receive(2000);

               if (message == null)
               {
                  Assert.fail("consumer " + i + " did not receive all messages");
               }

               if (ack)
               {
                  message.acknowledge();
               }
            }
         }

      }
      return groupIdsReceived;

   }

   protected void verifyReceiveAllInRangeNotBefore(final boolean ack,
                                                   final long firstReceiveTime,
                                                   final int msgStart,
                                                   final int msgEnd,
                                                   final int... consumerIDs) throws Exception
   {
      boolean outOfOrder = false;
      String firstOutOfOrderMessage = null;
      for (int consumerID : consumerIDs)
      {
         ConsumerHolder holder = consumers[consumerID];

         if (holder == null)
         {
            throw new IllegalArgumentException("No consumer at " + consumerID);
         }

         for (int j = msgStart; j < msgEnd; j++)
         {

            ClientMessage message = holder.consumer.receive(WAIT_TIMEOUT);

            if (message == null)
            {
               log.info("*** dumping consumers:");

               dumpConsumers();

               Assert.fail("consumer " + consumerID + " did not receive message " + j);
            }

            log.info("msg on ClusterTestBase = " + message);


            if (isLargeMessage())
            {
               checkMessageBody(message);
            }

            if (ack)
            {
               message.acknowledge();
            }

            if (firstReceiveTime != -1)
            {
               Assert.assertTrue("Message received too soon", System.currentTimeMillis() >= firstReceiveTime);
            }

            if (j != (Integer)message.getObjectProperty(ClusterTestBase.COUNT_PROP))
            {
               if (firstOutOfOrderMessage == null)
               {
                  firstOutOfOrderMessage = "expected " + j +
                                           " received " +
                                           message.getObjectProperty(ClusterTestBase.COUNT_PROP);
               }
               outOfOrder = true;
               System.out.println("Message j=" + j +
                                  " was received out of order = " +
                                  message.getObjectProperty(ClusterTestBase.COUNT_PROP));
               log.info("Message j=" + j +
                        " was received out of order = " +
                        message.getObjectProperty(ClusterTestBase.COUNT_PROP));
            }
         }
      }

      Assert.assertFalse("Messages were consumed out of order::" + firstOutOfOrderMessage, outOfOrder);
   }

   private void dumpConsumers() throws Exception
   {
      for (int i = 0; i < consumers.length; i++)
      {
         if (consumers[i] != null && !consumers[i].consumer.isClosed())
         {
            log.info("Dumping consumer " + i);

            checkReceive(i);
         }
      }
   }

   protected String clusterDescription(HornetQServer server)
   {
      String br = "-------------------------\n";
      String out = br;
      out += "HornetQ server " + server + "\n";
      ClusterManager clusterManager = server.getClusterManager();
      if (clusterManager == null)
      {
         out += "N/A";
      }
      else
      {
         for (ClusterConnection cc : clusterManager.getClusterConnections())
         {
            out += cc.describe() + "\n";
            out += cc.getTopology().describe();
         }
      }
      out += "\n\nfull topology:";
      return out + br;
   }

   protected void verifyReceiveAll(final boolean ack, final int numMessages, final int... consumerIDs) throws Exception
   {
      verifyReceiveAllInRange(ack, 0, numMessages, consumerIDs);
   }

   protected void verifyReceiveAll(final int numMessages, final int... consumerIDs) throws Exception
   {
      verifyReceiveAllInRange(false, 0, numMessages, consumerIDs);
   }

   protected void verifyReceiveAllNotBefore(final long firstReceiveTime,
                                            final int numMessages,
                                            final int... consumerIDs) throws Exception
   {
      verifyReceiveAllInRangeNotBefore(false, firstReceiveTime, 0, numMessages, consumerIDs);
   }

   protected void checkReceive(final int... consumerIDs) throws Exception
   {
      for (int consumerID : consumerIDs)
      {
         ConsumerHolder holder = consumers[consumerID];

         if (holder == null)
         {
            throw new IllegalArgumentException("No consumer at " + consumerID);
         }

         ClientMessage message;
         do
         {
            message = holder.consumer.receive(500);

            if (message != null)
            {
               log.info("check receive Consumer " + consumerID +
                                        " received message " +
                                        message.getObjectProperty(ClusterTestBase.COUNT_PROP));
            }
            else
            {
               log.info("check receive Consumer " + consumerID + " null message");
            }
         }
         while (message != null);

      }
   }

   protected void verifyReceiveRoundRobin(final int numMessages, final int... consumerIDs) throws Exception
   {
      int count = 0;

      for (int i = 0; i < numMessages; i++)
      {
         // We may use a negative number in some tests to ignore the consumer, case we know the server is down
         if (consumerIDs[count] >= 0)
         {
            ConsumerHolder holder = consumers[consumerIDs[count]];

            if (holder == null)
            {
               throw new IllegalArgumentException("No consumer at " + consumerIDs[i]);
            }

            ClientMessage message = holder.consumer.receive(WAIT_TIMEOUT);

            message.acknowledge();

            consumers[consumerIDs[count]].session.commit();

            System.out.println("Msg: " + message);

            Assert.assertNotNull("consumer " + consumerIDs[count] + " did not receive message " + i, message);

            Assert.assertEquals("consumer " + consumerIDs[count] + " message " + i,
                                i,
                                message.getObjectProperty(ClusterTestBase.COUNT_PROP));

         }

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
   protected void verifyReceiveRoundRobinInSomeOrder(final int numMessages, final int... consumerIDs) throws Exception
   {
      if (numMessages < consumerIDs.length)
      {
         throw new IllegalStateException("You must send more messages than consumers specified or the algorithm " + "won't work");
      }

      verifyReceiveRoundRobinInSomeOrder(true, numMessages, consumerIDs);
   }

   class OrderedConsumerHolder implements Comparable<OrderedConsumerHolder>
   {
      ConsumerHolder consumer;

      int order;

      public int compareTo(final OrderedConsumerHolder o)
      {
         int thisOrder = order;
         int otherOrder = o.order;
         return thisOrder < otherOrder ? -1 : thisOrder == otherOrder ? 0 : 1;
      }
   }

   protected void verifyReceiveRoundRobinInSomeOrder(final boolean ack, final int numMessages, final int... consumerIDs) throws Exception
   {
      if (numMessages < consumerIDs.length)
      {
         throw new IllegalStateException("not enough messages");
      }

      // First get one from each consumer to determine the order, then we sort them in this order

      List<OrderedConsumerHolder> sorted = new ArrayList<OrderedConsumerHolder>();

      for (int consumerID : consumerIDs)
      {
         ConsumerHolder holder = consumers[consumerID];

         ClientMessage msg = holder.consumer.receive(10000);

         Assert.assertNotNull(msg);

         int count = msg.getIntProperty(ClusterTestBase.COUNT_PROP);

         OrderedConsumerHolder orderedHolder = new OrderedConsumerHolder();

         orderedHolder.consumer = holder;
         orderedHolder.order = count;

         sorted.add(orderedHolder);

         if (ack)
         {
            msg.acknowledge();
         }
      }

      // Now sort them

      Collections.sort(sorted);

      // First verify the first lot received are ok

      int count = 0;

      for (OrderedConsumerHolder holder : sorted)
      {
         if (holder.order != count)
         {
            throw new IllegalStateException("Out of order");
         }

         count++;
      }

      // Now check the rest are in order too

      outer: while (count < numMessages)
      {
         for (OrderedConsumerHolder holder : sorted)
         {
            ClientMessage msg = holder.consumer.consumer.receive(10000);

            Assert.assertNotNull(msg);

            int p = msg.getIntProperty(ClusterTestBase.COUNT_PROP);

            if (p != count)
            {
               throw new IllegalStateException("Out of order 2");
            }

            if (ack)
            {
               msg.acknowledge();
            }

            count++;

            if (count == numMessages)
            {
               break outer;
            }

         }
      }
   }

   protected void verifyReceiveRoundRobinInSomeOrderWithCounts(final boolean ack,
                                                               final int[] messageCounts,
                                                               final int... consumerIDs) throws Exception
   {
      List<LinkedList<Integer>> receivedCounts = new ArrayList<LinkedList<Integer>>();

      Set<Integer> counts = new HashSet<Integer>();

      for (int consumerID : consumerIDs)
      {
         ConsumerHolder holder = consumers[consumerID];

         if (holder == null)
         {
            throw new IllegalArgumentException("No consumer at " + consumerID);
         }

         LinkedList<Integer> list = new LinkedList<Integer>();

         receivedCounts.add(list);

         ClientMessage message;
         do
         {
            message = holder.consumer.receive(1000);

            if (message != null)
            {
               int count = (Integer)message.getObjectProperty(ClusterTestBase.COUNT_PROP);

               checkMessageBody(message);


               // log.info("consumer " + consumerIDs[i] + " received message " + count);

               Assert.assertFalse(counts.contains(count));

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

      for (int messageCount : messageCounts)
      {
         Assert.assertTrue(counts.contains(messageCount));
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

      for (int messageCount : messageCounts)
      {
         LinkedList list = lists[index];

         Assert.assertNotNull(list);

         int elem = (Integer)list.poll();

         Assert.assertEquals(messageCount, elem);

         index++;

         if (index == lists.length)
         {
            index = 0;
         }
      }

   }

   /**
    * @param message
    */
   private void checkMessageBody(ClientMessage message)
   {
      if (isLargeMessage())
      {
         for (int posMsg = 0 ; posMsg < getLargeMessageSize(); posMsg++)
         {
            assertEquals(getSamplebyte(posMsg), message.getBodyBuffer().readByte());
         }
      }
   }

   protected void verifyReceiveRoundRobinInSomeOrderNoAck(final int numMessages, final int... consumerIDs) throws Exception
   {
      if (numMessages < consumerIDs.length)
      {
         throw new IllegalStateException("You must send more messages than consumers specified or the algorithm " + "won't work");
      }

      verifyReceiveRoundRobinInSomeOrder(false, numMessages, consumerIDs);
   }

   protected int[] getReceivedOrder(final int consumerID) throws Exception
   {
      return getReceivedOrder(consumerID, false);
   }

   protected int[] getReceivedOrder(final int consumerID, final boolean ack) throws Exception
   {
      ConsumerHolder consumer = consumers[consumerID];

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

            if (isLargeMessage())
            {
               checkMessageBody(message);
            }

            if (ack)
            {
               message.acknowledge();
            }

            int count = (Integer)message.getObjectProperty(ClusterTestBase.COUNT_PROP);

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

      if (ack)
      {
         // just to flush acks
         consumers[consumerID].session.commit();
      }

      return res;
   }

   protected void verifyNotReceive(final int... consumerIDs) throws Exception
   {
      for (int i = 0; i < consumerIDs.length; i++)
      {
         ConsumerHolder holder = consumers[consumerIDs[i]];

         if (holder == null)
         {
            throw new IllegalArgumentException("No consumer at " + consumerIDs[i]);
         }

         Assert.assertNull("consumer " + i + " received message", holder.consumer.receiveImmediate());
      }
   }

   protected void setupSessionFactory(final int node, final boolean netty) throws Exception
   {
      setupSessionFactory(node, netty, false);
   }

   protected void setupSessionFactory(final int node, final boolean netty, boolean ha) throws Exception
   {
      if (sfs[node] != null)
      {
         throw new IllegalArgumentException("Already a factory at " + node);
      }

      Map<String, Object> params = generateParams(node, netty);

      TransportConfiguration serverTotc;

      if (netty)
      {
         serverTotc = new TransportConfiguration(ServiceTestBase.NETTY_CONNECTOR_FACTORY, params);
      }
      else
      {
         serverTotc = new TransportConfiguration(ServiceTestBase.INVM_CONNECTOR_FACTORY, params);
      }

      if (ha)
      {
         locators[node] = HornetQClient.createServerLocatorWithHA(serverTotc);
      }
      else
      {
         locators[node] = HornetQClient.createServerLocatorWithoutHA(serverTotc);
      }

      locators[node].setBlockOnNonDurableSend(true);
      locators[node].setBlockOnDurableSend(true);
      ClientSessionFactory sf = locators[node].createSessionFactory();

      ClientSession session = sf.createSession();
      session.close();
      sfs[node] = sf;
   }

   protected void setupSessionFactory(final int node, final boolean netty, int reconnectAttempts) throws Exception
   {
      if (sfs[node] != null)
      {
         throw new IllegalArgumentException("Already a server at " + node);
      }

      Map<String, Object> params = generateParams(node, netty);

      TransportConfiguration serverTotc;

      if (netty)
      {
         serverTotc = new TransportConfiguration(ServiceTestBase.NETTY_CONNECTOR_FACTORY, params);
      }
      else
      {
         serverTotc = new TransportConfiguration(ServiceTestBase.INVM_CONNECTOR_FACTORY, params);
      }

      locators[node] = HornetQClient.createServerLocatorWithoutHA(serverTotc);

      locators[node].setBlockOnNonDurableSend(true);
      locators[node].setBlockOnDurableSend(true);
      locators[node].setReconnectAttempts(reconnectAttempts);
      ClientSessionFactory sf = locators[node].createSessionFactory();

      sfs[node] = sf;
   }

   protected void setupSessionFactory(final int node, final int backupNode, final boolean netty, final boolean blocking) throws Exception
   {
      if (sfs[node] != null)
      {
         throw new IllegalArgumentException("Already a server at " + node);
      }

      Map<String, Object> params = generateParams(node, netty);

      TransportConfiguration serverTotc;

      if (netty)
      {
         serverTotc = new TransportConfiguration(ServiceTestBase.NETTY_CONNECTOR_FACTORY, params);
      }
      else
      {
         serverTotc = new TransportConfiguration(ServiceTestBase.INVM_CONNECTOR_FACTORY, params);
      }

      locators[node] = HornetQClient.createServerLocatorWithHA(serverTotc);
      locators[node].setRetryInterval(100);
      locators[node].setRetryIntervalMultiplier(1d);
      locators[node].setReconnectAttempts(-1);
      locators[node].setBlockOnNonDurableSend(blocking);
      locators[node].setBlockOnDurableSend(blocking);

      ClientSessionFactory sf = locators[node].createSessionFactory();
      sfs[node] = sf;
   }

   protected void setupSessionFactory(final int node, final int backupNode, final boolean netty) throws Exception
   {
      this.setupSessionFactory(node, backupNode, netty, true);
   }

   protected HornetQServer getServer(final int node)
   {
      if (servers[node] == null)
      {
         throw new IllegalArgumentException("No server at node " + node);
      }

      return servers[node];
   }

   protected void setupServer(final int node, final boolean fileStorage, final boolean netty)
   {
      setupLiveServer(node, fileStorage, false, netty);
   }

   protected void setupLiveServer(final int node,
                                  final boolean fileStorage,
                                  final boolean sharedStorage,
                                  final boolean netty)
   {
      if (servers[node] != null)
      {
         throw new IllegalArgumentException("Already a server at node " + node);
      }

      Configuration configuration = createBasicConfig();

      configuration.setSecurityEnabled(false);
      configuration.setJournalMinFiles(2);
      configuration.setJournalMaxIO_AIO(1000);
      configuration.setJournalFileSize(100 * 1024);
      configuration.setJournalType(getDefaultJournalType());
      configuration.setSharedStore(sharedStorage);
      configuration.setThreadPoolMaxSize(10);
      if (sharedStorage)
      {
         // Shared storage will share the node between the backup and live node
         configuration.setBindingsDirectory(getBindingsDir(node, false));
         configuration.setJournalDirectory(getJournalDir(node, false));
         configuration.setPagingDirectory(getPageDir(node, false));
         configuration.setLargeMessagesDirectory(getLargeMessagesDir(node, false));
      }
      else
      {
         configuration.setBindingsDirectory(getBindingsDir(node, true));
         configuration.setJournalDirectory(getJournalDir(node, true));
         configuration.setPagingDirectory(getPageDir(node, true));
         configuration.setLargeMessagesDirectory(getLargeMessagesDir(node, true));
      }
      configuration.setClustered(true);
      configuration.setJournalCompactMinFiles(0);

      configuration.getAcceptorConfigurations().clear();
      configuration.getAcceptorConfigurations().add(createTransportConfiguration(netty, true, generateParams(node,
                                                                                                             netty)));

      HornetQServer server;

      if (fileStorage)
      {
         if (sharedStorage)
         {
            server = createInVMFailoverServer(true, configuration, nodeManagers[node], node);
         }
         else
         {
            server = HornetQServers.newHornetQServer(configuration);
            server.setIdentity("Server " + node);
         }
      }
      else
      {
         if (sharedStorage)
         {
            server = createInVMFailoverServer(false, configuration, nodeManagers[node], node);
         }
         else
         {
            server = HornetQServers.newHornetQServer(configuration, false);
            server.setIdentity("Server " + node);
         }
      }

      server.setIdentity(this.getClass().getSimpleName() + "/Live(" + node + ")");
      servers[node] = server;
   }

   protected void setupBackupServer(final int node,
                                    final int liveNode,
                                    final boolean fileStorage,
                                    final boolean sharedStorage,
                                    final boolean netty)
   {
      if (servers[node] != null)
      {
         throw new IllegalArgumentException("Already a server at node " + node);
      }

      Configuration configuration = createBasicConfig();

      configuration.setSecurityEnabled(false);
      configuration.setJournalMinFiles(2);
      configuration.setJournalMaxIO_AIO(1000);
      configuration.setJournalFileSize(100 * 1024);
      configuration.setJournalType(getDefaultJournalType());
      configuration.setSharedStore(sharedStorage);
      if (sharedStorage)
      {
         // Shared storage will share the node between the backup and live node
         configuration.setBindingsDirectory(getBindingsDir(liveNode, false));
         configuration.setJournalDirectory(getJournalDir(liveNode, false));
         configuration.setPagingDirectory(getPageDir(liveNode, false));
         configuration.setLargeMessagesDirectory(getLargeMessagesDir(liveNode, false));
      }
      else
      {
         configuration.setBindingsDirectory(getBindingsDir(node, true));
         configuration.setJournalDirectory(getJournalDir(node, true));
         configuration.setPagingDirectory(getPageDir(node, true));
         configuration.setLargeMessagesDirectory(getLargeMessagesDir(node, true));
      }
      configuration.setClustered(true);
      configuration.setJournalCompactMinFiles(0);
      configuration.setBackup(true);

      configuration.getAcceptorConfigurations().clear();
      TransportConfiguration acceptorConfig = createTransportConfiguration(netty, true, generateParams(node, netty));
      configuration.getAcceptorConfigurations().add(acceptorConfig);
      // add backup connector
      TransportConfiguration liveConfig = createTransportConfiguration(netty, false, generateParams(liveNode, netty));
      configuration.getConnectorConfigurations().put(liveConfig.getName(), liveConfig);
      TransportConfiguration backupConfig = createTransportConfiguration(netty, false, generateParams(node, netty));
      configuration.getConnectorConfigurations().put(backupConfig.getName(), backupConfig);

      HornetQServer server;

      if (fileStorage)
      {
         if (sharedStorage)
         {
            server = createInVMFailoverServer(true, configuration, nodeManagers[liveNode], liveNode);
         }
         else
         {
            server = HornetQServers.newHornetQServer(configuration);
            server.setIdentity("Server " + liveNode);
         }
      }
      else
      {
         if (sharedStorage)
         {
            server = createInVMFailoverServer(true, configuration, nodeManagers[liveNode], liveNode);
         }
         else
         {
            server = HornetQServers.newHornetQServer(configuration, false);
            server.setIdentity("Server " + liveNode);
         }
      }
      server.setIdentity(this.getClass().getSimpleName() + "/Backup(" + node + " of live " + liveNode + ")");
      servers[node] = server;
   }

   protected void setupLiveServerWithDiscovery(final int node,
                                               final String groupAddress,
                                               final int port,
                                               final boolean fileStorage,
                                               final boolean netty,
                                               final boolean sharedStorage)
   {
      if (servers[node] != null)
      {
         throw new IllegalArgumentException("Already a server at node " + node);
      }

      Configuration configuration = createBasicConfig();

      configuration.setSecurityEnabled(false);
      configuration.setBindingsDirectory(getBindingsDir(node, false));
      configuration.setJournalMinFiles(2);
      configuration.setJournalDirectory(getJournalDir(node, false));
      configuration.setJournalFileSize(100 * 1024);
      configuration.setJournalType(getDefaultJournalType());
      configuration.setJournalMaxIO_AIO(1000);
      configuration.setPagingDirectory(getPageDir(node, false));
      configuration.setLargeMessagesDirectory(getLargeMessagesDir(node, false));
      configuration.setClustered(true);
      configuration.setBackup(false);

      configuration.getAcceptorConfigurations().clear();

      Map<String, Object> params = generateParams(node, netty);

      configuration.getAcceptorConfigurations().add(createTransportConfiguration(netty, true, params));

      TransportConfiguration connector = createTransportConfiguration(netty, false, params);
      configuration.getConnectorConfigurations().put(connector.getName(), connector);

      List<String> connectorPairs = new ArrayList<String>();
      connectorPairs.add(connector.getName());

      BroadcastGroupConfiguration bcConfig = new BroadcastGroupConfiguration("bg1",
                                                                             null,
                                                                             -1,
                                                                             groupAddress,
                                                                             port,
                                                                             200,
                                                                             connectorPairs);

      configuration.getBroadcastGroupConfigurations().add(bcConfig);

      DiscoveryGroupConfiguration dcConfig = new DiscoveryGroupConfiguration("dg1",
                                                                             null,
                                                                             groupAddress,
                                                                             port,
                                                                             1000,
                                                                             1000);

      configuration.getDiscoveryGroupConfigurations().put(dcConfig.getName(), dcConfig);

      HornetQServer server;
      if (fileStorage)
      {
         if (sharedStorage)
         {
            server = createInVMFailoverServer(true, configuration, nodeManagers[node], node);
         }
         else
         {
            server = HornetQServers.newHornetQServer(configuration);
            server.setIdentity("Server " + node);
         }
      }
      else
      {
         if (sharedStorage)
         {
            server = createInVMFailoverServer(false, configuration, nodeManagers[node], node);
         }
         else
         {
            server = HornetQServers.newHornetQServer(configuration, false);
            server.setIdentity("Server " + node);
         }
      }
      servers[node] = server;
   }

   protected void setupBackupServerWithDiscovery(final int node,
                                                 final int liveNode,
                                                 final String groupAddress,
                                                 final int port,
                                                 final boolean fileStorage,
                                                 final boolean netty,
                                                 final boolean sharedStorage)
   {
      if (servers[node] != null)
      {
         throw new IllegalArgumentException("Already a server at node " + node);
      }

      Configuration configuration = createBasicConfig();

      configuration.setSecurityEnabled(false);
      configuration.setSharedStore(sharedStorage);
      if (sharedStorage)
      {
         // Shared storage will share the node between the backup and live node
         configuration.setBindingsDirectory(getBindingsDir(liveNode, false));
         configuration.setJournalDirectory(getJournalDir(liveNode, false));
         configuration.setPagingDirectory(getPageDir(liveNode, false));
         configuration.setLargeMessagesDirectory(getLargeMessagesDir(liveNode, false));
      }
      else
      {
         configuration.setBindingsDirectory(getBindingsDir(node, true));
         configuration.setJournalDirectory(getJournalDir(node, true));
         configuration.setPagingDirectory(getPageDir(node, true));
         configuration.setLargeMessagesDirectory(getLargeMessagesDir(node, true));
      }
      configuration.setClustered(true);
      configuration.setBackup(true);

      configuration.getAcceptorConfigurations().clear();

      Map<String, Object> params = generateParams(node, netty);

      configuration.getAcceptorConfigurations().add(createTransportConfiguration(netty, true, params));

      TransportConfiguration connector = createTransportConfiguration(netty, false, params);
      configuration.getConnectorConfigurations().put(connector.getName(), connector);

      List<String> connectorPairs = new ArrayList<String>();
      connectorPairs.add(connector.getName());

      BroadcastGroupConfiguration bcConfig = new BroadcastGroupConfiguration("bg1",
                                                                             null,
                                                                             -1,
                                                                             groupAddress,
                                                                             port,
                                                                             1000,
                                                                             connectorPairs);

      configuration.getBroadcastGroupConfigurations().add(bcConfig);

      DiscoveryGroupConfiguration dcConfig = new DiscoveryGroupConfiguration("dg1",
                                                                             null,
                                                                             groupAddress,
                                                                             port,
                                                                             5000,
                                                                             5000);

      configuration.getDiscoveryGroupConfigurations().put(dcConfig.getName(), dcConfig);

      HornetQServer server;
      if (fileStorage)
      {
         if (sharedStorage)
         {
            server = createInVMFailoverServer(true, configuration, nodeManagers[liveNode], liveNode);
         }
         else
         {
            server = HornetQServers.newHornetQServer(configuration);
            server.setIdentity("Server " + liveNode);
         }
      }
      else
      {
         if (sharedStorage)
         {
            server = createInVMFailoverServer(false, configuration, nodeManagers[liveNode], liveNode);
         }
         else
         {
            server = HornetQServers.newHornetQServer(configuration, false);
         }
      }
      servers[node] = server;
   }

   protected void clearServer(final int... nodes)
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

   protected void setupClusterConnection(final String name,
                                         final int nodeFrom,
                                         final int nodeTo,
                                         final String address,
                                         final boolean forwardWhenNoConsumers,
                                         final int maxHops,
                                         final boolean netty,
                                         final boolean allowDirectConnectionsOnly)
   {
      HornetQServer serverFrom = servers[nodeFrom];

      if (serverFrom == null)
      {
         throw new IllegalStateException("No server at node " + nodeFrom);
      }

      TransportConfiguration connectorFrom = createTransportConfiguration(netty, false, generateParams(nodeFrom, netty));
      serverFrom.getConfiguration().getConnectorConfigurations().put(name, connectorFrom);

      List<String> pairs = null;

      if (nodeTo != -1)
      {
         TransportConfiguration serverTotc = createTransportConfiguration(netty, false, generateParams(nodeTo, netty));
         serverFrom.getConfiguration().getConnectorConfigurations().put(serverTotc.getName(), serverTotc);
         pairs = new ArrayList<String>();
         pairs.add(serverTotc.getName());
      }

      ClusterConnectionConfiguration clusterConf = new ClusterConnectionConfiguration(name,
                                                                                      address,
                                                                                      name,
                                                                                      100,
                                                                                      true,
                                                                                      forwardWhenNoConsumers,
                                                                                      maxHops,
                                                                                      1024,
                                                                                      pairs,
                                                                                      allowDirectConnectionsOnly);
      serverFrom.getConfiguration().getClusterConfigurations().add(clusterConf);
   }

   protected void setupClusterConnection(final String name,
                                         final String address,
                                         final boolean forwardWhenNoConsumers,
                                         final int maxHops,
                                         final boolean netty,
                                         final int nodeFrom,
                                         final int... nodesTo)
   {
      HornetQServer serverFrom = servers[nodeFrom];

      if (serverFrom == null)
      {
         throw new IllegalStateException("No server at node " + nodeFrom);
      }

      TransportConfiguration connectorFrom = createTransportConfiguration(netty, false, generateParams(nodeFrom, netty));
      serverFrom.getConfiguration().getConnectorConfigurations().put(connectorFrom.getName(), connectorFrom);

      List<String> pairs = new ArrayList<String>();
      for (int element : nodesTo)
      {
         TransportConfiguration serverTotc = createTransportConfiguration(netty, false, generateParams(element, netty));
         serverFrom.getConfiguration().getConnectorConfigurations().put(serverTotc.getName(), serverTotc);
         pairs.add(serverTotc.getName());
      }

      ClusterConnectionConfiguration clusterConf = createClusterConfig(name,
                                                                       address,
                                                                       forwardWhenNoConsumers,
                                                                       maxHops,
                                                                       connectorFrom,
                                                                       pairs);

      serverFrom.getConfiguration().getClusterConfigurations().add(clusterConf);
   }

   protected void setupClusterConnection(final String name,
                                         final String address,
                                         final boolean forwardWhenNoConsumers,
                                         final int maxHops,
                                         final int reconnectAttempts,
                                         final long retryInterval,
                                         final boolean netty,
                                         final int nodeFrom,
                                         final int... nodesTo)
   {
      HornetQServer serverFrom = servers[nodeFrom];

      if (serverFrom == null)
      {
         throw new IllegalStateException("No server at node " + nodeFrom);
      }

      TransportConfiguration connectorFrom = createTransportConfiguration(netty, false, generateParams(nodeFrom, netty));
      serverFrom.getConfiguration().getConnectorConfigurations().put(connectorFrom.getName(), connectorFrom);

      List<String> pairs = new ArrayList<String>();
      for (int element : nodesTo)
      {
         TransportConfiguration serverTotc = createTransportConfiguration(netty, false, generateParams(element, netty));
         serverFrom.getConfiguration().getConnectorConfigurations().put(serverTotc.getName(), serverTotc);
         pairs.add(serverTotc.getName());
      }
      ClusterConnectionConfiguration clusterConf = new ClusterConnectionConfiguration(name,
                                                                                      address,
                                                                                      connectorFrom.getName(),
                                                                                      HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                                                      ConfigurationImpl.DEFAULT_CLUSTER_FAILURE_CHECK_PERIOD,
                                                                                      ConfigurationImpl.DEFAULT_CLUSTER_CONNECTION_TTL,
                                                                                      retryInterval,
                                                                                      ConfigurationImpl.DEFAULT_CLUSTER_RETRY_INTERVAL_MULTIPLIER,
                                                                                      ConfigurationImpl.DEFAULT_CLUSTER_MAX_RETRY_INTERVAL,
                                                                                      reconnectAttempts,
                                                                                      1000,
                                                                                      true,
                                                                                      forwardWhenNoConsumers,
                                                                                      maxHops,
                                                                                      1024,
                                                                                      pairs,
                                                                                      false);

      serverFrom.getConfiguration().getClusterConfigurations().add(clusterConf);
   }

   /**
    * @param name
    * @param address
    * @param forwardWhenNoConsumers
    * @param maxHops
    * @param connectorFrom
    * @param pairs
    * @return
    */
   protected ClusterConnectionConfiguration createClusterConfig(final String name,
                                                                final String address,
                                                                final boolean forwardWhenNoConsumers,
                                                                final int maxHops,
                                                                TransportConfiguration connectorFrom,
                                                                List<String> pairs)
   {
      ClusterConnectionConfiguration clusterConf = new ClusterConnectionConfiguration(name,
                                                                                      address,
                                                                                      connectorFrom.getName(),
                                                                                      250,
                                                                                      true,
                                                                                      forwardWhenNoConsumers,
                                                                                      maxHops,
                                                                                      1024,
                                                                                      pairs,
                                                                                      false);
      return clusterConf;
   }

   protected void setupClusterConnectionWithBackups(final String name,
                                                    final String address,
                                                    final boolean forwardWhenNoConsumers,
                                                    final int maxHops,
                                                    final boolean netty,
                                                    final int nodeFrom,
                                                    final int[] nodesTo)
   {
      HornetQServer serverFrom = servers[nodeFrom];

      if (serverFrom == null)
      {
         throw new IllegalStateException("No server at node " + nodeFrom);
      }

      TransportConfiguration connectorFrom = createTransportConfiguration(netty, false, generateParams(nodeFrom, netty));
      serverFrom.getConfiguration().getConnectorConfigurations().put(name, connectorFrom);

      List<String> pairs = new ArrayList<String>();
      for (int element : nodesTo)
      {
         TransportConfiguration serverTotc = createTransportConfiguration(netty, false, generateParams(element, netty));
         serverFrom.getConfiguration().getConnectorConfigurations().put(serverTotc.getName(), serverTotc);
         pairs.add(serverTotc.getName());
      }

      ClusterConnectionConfiguration clusterConf = new ClusterConnectionConfiguration(name,
                                                                                      address,
                                                                                      name,
                                                                                      250,
                                                                                      true,
                                                                                      forwardWhenNoConsumers,
                                                                                      maxHops,
                                                                                      1024,
                                                                                      pairs,
                                                                                      false);

      serverFrom.getConfiguration().getClusterConfigurations().add(clusterConf);
   }

   protected void setupDiscoveryClusterConnection(final String name,
                                                  final int node,
                                                  final String discoveryGroupName,
                                                  final String address,
                                                  final boolean forwardWhenNoConsumers,
                                                  final int maxHops,
                                                  final boolean netty)
   {
      HornetQServer server = servers[node];

      if (server == null)
      {
         throw new IllegalStateException("No server at node " + node);
      }

      TransportConfiguration connectorConfig = createTransportConfiguration(netty, false, generateParams(node, netty));
      server.getConfiguration().getConnectorConfigurations().put(name, connectorConfig);

      ClusterConnectionConfiguration clusterConf = new ClusterConnectionConfiguration(name,
                                                                                      address,
                                                                                      name,
                                                                                      100,
                                                                                      true,
                                                                                      forwardWhenNoConsumers,
                                                                                      maxHops,
                                                                                      1024,
                                                                                      discoveryGroupName);
      List<ClusterConnectionConfiguration> clusterConfs = server.getConfiguration().getClusterConfigurations();

      clusterConfs.add(clusterConf);
   }

   protected void startServers(final int... nodes) throws Exception
   {
      for (int node : nodes)
      {
         log.info("#test start node " + node);
         if (System.currentTimeMillis() - timeStarts[node] < TIMEOUT_START_SERVER)
         {
            Thread.sleep(TIMEOUT_START_SERVER);
         }
         timeStarts[node] = System.currentTimeMillis();

         servers[node].setIdentity("server " + node);
         log.info("starting server " + servers[node]);
         servers[node].start();

         log.info("started server " + servers[node]);

         log.info("started server " + node);

         waitForServer(servers[node]);

      }

   }

   protected void stopClusterConnections(final int... nodes) throws Exception
   {
      for (int node : nodes)
      {
         if (servers[node].isStarted())
         {
            for (ClusterConnection cc : servers[node].getClusterManager().getClusterConnections())
            {
               cc.stop();
               cc.flushExecutor();
            }
         }
      }
   }

   protected void stopServers(final int... nodes) throws Exception
   {
      log.info("Stopping nodes " + Arrays.toString(nodes));
      for (int node : nodes)
      {
         log.info("#test stop server " + node);
         if (servers[node] != null && servers[node].isStarted())
         {
            try
            {
               if (System.currentTimeMillis() - timeStarts[node] < TIMEOUT_START_SERVER)
               {
                  // We can't stop and start a node too fast (faster than what the Topology could realize about this
                 Thread.sleep(TIMEOUT_START_SERVER);
               }


               timeStarts[node] = System.currentTimeMillis();

               log.info("stopping server " + node);
               servers[node].stop();
               log.info("server " + node + " stopped");
            }
            catch (Exception e)
            {
               log.warn(e.getMessage(), e);
            }
         }
      }
   }

   protected boolean isFileStorage()
   {
      return true;
   }
}
