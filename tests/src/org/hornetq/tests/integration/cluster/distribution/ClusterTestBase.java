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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.hornetq.api.core.Message;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.*;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.Bindings;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.postoffice.QueueBinding;
import org.hornetq.core.postoffice.impl.LocalQueueBinding;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.core.server.JournalType;
import org.hornetq.core.server.cluster.BroadcastGroupConfiguration;
import org.hornetq.core.server.cluster.ClusterConnection;
import org.hornetq.core.server.cluster.ClusterConnectionConfiguration;
import org.hornetq.core.server.cluster.DiscoveryGroupConfiguration;
import org.hornetq.core.server.cluster.RemoteQueueBinding;
import org.hornetq.core.server.group.GroupingHandler;
import org.hornetq.core.server.group.impl.GroupingHandlerConfiguration;
import org.hornetq.integration.transports.netty.TransportConstants;
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
   private static final Logger log = Logger.getLogger(ClusterTestBase.class);

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

   private static final long WAIT_TIMEOUT = 10000;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      UnitTestCase.checkFreePort(ClusterTestBase.PORTS);

      clearData();

      consumers = new ConsumerHolder[ClusterTestBase.MAX_CONSUMERS];

      servers = new HornetQServer[ClusterTestBase.MAX_SERVERS];

      sfs = new ClientSessionFactory[ClusterTestBase.MAX_SERVERS];

   }

   @Override
   protected void tearDown() throws Exception
   {
      UnitTestCase.checkFreePort(ClusterTestBase.PORTS);

      servers = null;

      sfs = null;

      consumers = null;

      consumers = new ConsumerHolder[ClusterTestBase.MAX_CONSUMERS];

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

   private ConsumerHolder[] consumers;

   protected HornetQServer[] servers;

   protected ClientSessionFactory[] sfs;

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

      ClusterTestBase.log.error(msg);

      throw new IllegalStateException(msg);
   }

   protected void waitForBindings(final int node,
                                  final String address,
                                  final int count,
                                  final int consumerCount,
                                  final boolean local) throws Exception
   {
      // System.out.println("waiting for bindings on node " + node +
      // " address " +
      // address +
      // " count " +
      // count +
      // " consumerCount " +
      // consumerCount +
      // " local " +
      // local);
      HornetQServer server = servers[node];

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
            if (binding instanceof LocalQueueBinding && local || binding instanceof RemoteQueueBinding && !local)
            {
               QueueBinding qBinding = (QueueBinding)binding;

               bindingCount++;

               totConsumers += qBinding.consumerCount();
            }
         }

         if (bindingCount == count && totConsumers == consumerCount)
         {
            return;
         }

         Thread.sleep(10);
      }
      while (System.currentTimeMillis() - start < ClusterTestBase.WAIT_TIMEOUT);

      // System.out.println(threadDump(" - fired by ClusterTestBase::waitForBindings"));

      String msg = "Timed out waiting for bindings (bindingCount = " + bindingCount +
                   ", totConsumers = " +
                   totConsumers +
                   ")";

      ClusterTestBase.log.error(msg);

      // Sending thread dump into junit report.. trying to get some information about the server case the binding didn't
      // arrive
      System.out.println(UnitTestCase.threadDump(msg));

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
      System.out.println("=======================================================================");

      throw new IllegalStateException(msg);
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

            if (filterVal != null)
            {
               message.putStringProperty(ClusterTestBase.FILTER_PROP, new SimpleString(filterVal));
            }

            message.putIntProperty(ClusterTestBase.COUNT_PROP, i);

            producer.send(message);
         }
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
      sendInRange(node, address, 0, numMessages, durable, filterVal);
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
               ClusterTestBase.log.info("*** dumping consumers:");

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
      for (int consumerID : consumerIDs)
      {
         ConsumerHolder holder = consumers[consumerID];

         if (holder == null)
         {
            throw new IllegalArgumentException("No consumer at " + consumerID);
         }

         for (int j = msgStart; j < msgEnd; j++)
         {
            ClientMessage message = holder.consumer.receive(2000);

            if (message == null)
            {
               ClusterTestBase.log.info("*** dumping consumers:");

               dumpConsumers();

               Assert.assertNotNull("consumer " + consumerID + " did not receive message " + j, message);
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
               outOfOrder = true;
               System.out.println("Message j=" + j +
                                  " was received out of order = " +
                                  message.getObjectProperty(ClusterTestBase.COUNT_PROP));
            }
         }
      }

      Assert.assertFalse("Messages were consumed out of order, look at System.out for more information", outOfOrder);
   }

   private void dumpConsumers() throws Exception
   {
      for (int i = 0; i < consumers.length; i++)
      {
         if (consumers[i] != null)
         {
            ClusterTestBase.log.info("Dumping consumer " + i);

            checkReceive(i);
         }
      }
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
               ClusterTestBase.log.info("check receive Consumer " + consumerID +
                                        " received message " +
                                        message.getObjectProperty(ClusterTestBase.COUNT_PROP));
            }
            else
            {
               ClusterTestBase.log.info("check receive Consumer " + consumerID + " null message");
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
         ConsumerHolder holder = consumers[consumerIDs[count]];

         if (holder == null)
         {
            throw new IllegalArgumentException("No consumer at " + consumerIDs[i]);
         }

         ClientMessage message = holder.consumer.receive(500);

         Assert.assertNotNull("consumer " + consumerIDs[count] + " did not receive message " + i, message);

         Assert.assertEquals("consumer " + consumerIDs[count] + " message " + i,
                             i,
                             message.getObjectProperty(ClusterTestBase.COUNT_PROP));

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

   protected void setupSessionFactory(final int node, final boolean netty)
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

      ClientSessionFactory sf = HornetQClient.createClientSessionFactory(serverTotc);

      sf.setBlockOnNonDurableSend(true);
      sf.setBlockOnDurableSend(true);

      sfs[node] = sf;
   }

   protected void setupSessionFactory(final int node, final int backupNode, final boolean netty, final boolean blocking)
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

      TransportConfiguration serverBackuptc = null;

      if (backupNode != -1)
      {
         Map<String, Object> backupParams = generateParams(backupNode, netty);

         if (netty)
         {
            serverBackuptc = new TransportConfiguration(ServiceTestBase.NETTY_CONNECTOR_FACTORY, backupParams);
         }
         else
         {
            serverBackuptc = new TransportConfiguration(ServiceTestBase.INVM_CONNECTOR_FACTORY, backupParams);
         }
      }

      ClientSessionFactory sf = HornetQClient.createClientSessionFactory(serverTotc, serverBackuptc);

      sf.setFailoverOnServerShutdown(false);
      sf.setRetryInterval(100);
      sf.setRetryIntervalMultiplier(1d);
      sf.setReconnectAttempts(-1);
      sf.setBlockOnNonDurableSend(blocking);
      sf.setBlockOnDurableSend(blocking);

      sfs[node] = sf;
   }

   protected void setupSessionFactory(final int node, final int backupNode, final boolean netty)
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
      setupServer(node, fileStorage, netty, false, -1);
   }

   protected void setupServer(final int node, final boolean fileStorage, final boolean netty, final boolean backup)
   {
      setupServer(node, fileStorage, netty, backup, -1);
   }

   protected void setupServer(final int node, final boolean fileStorage, final boolean netty, final int backupNode)
   {
      setupServer(node, fileStorage, netty, false, backupNode);
   }

   protected void setupServer(final int node,
                              final boolean fileStorage,
                              final boolean netty,
                              final boolean backup,
                              final int backupNode)
   {
      setupServer(node, fileStorage, true, netty, backup, backupNode);
   }

   protected void setupServer(final int node,
                              final boolean fileStorage,
                              final boolean sharedStorage,
                              final boolean netty,
                              final boolean backup,
                              final int backupNode)
   {
      if (servers[node] != null)
      {
         throw new IllegalArgumentException("Already a server at node " + node);
      }

      Configuration configuration = new ConfigurationImpl();

      configuration.setSecurityEnabled(false);
      configuration.setJournalMinFiles(2);
      configuration.setJournalMaxIO_AIO(1000);
      configuration.setJournalFileSize(100 * 1024);
      configuration.setJournalType(JournalType.ASYNCIO);
      configuration.setSharedStore(sharedStorage);
      if (sharedStorage)
      {
         // Shared storage will share the node between the backup and live node
         int nodeDirectoryToUse = backupNode == -1 ? node : backupNode;
         configuration.setBindingsDirectory(getBindingsDir(nodeDirectoryToUse, false));
         configuration.setJournalDirectory(getJournalDir(nodeDirectoryToUse, false));
         configuration.setPagingDirectory(getPageDir(nodeDirectoryToUse, false));
         configuration.setLargeMessagesDirectory(getLargeMessagesDir(nodeDirectoryToUse, false));
      }
      else
      {
         configuration.setBindingsDirectory(getBindingsDir(node, backup));
         configuration.setJournalDirectory(getJournalDir(node, backup));
         configuration.setPagingDirectory(getPageDir(node, backup));
         configuration.setLargeMessagesDirectory(getLargeMessagesDir(node, backup));
      }
      configuration.setClustered(true);
      configuration.setJournalCompactMinFiles(0);
      configuration.setBackup(backup);

      if (backupNode != -1)
      {
         Map<String, Object> backupParams = generateParams(backupNode, netty);

         if (netty)
         {
            TransportConfiguration nettyBackuptc = new TransportConfiguration(ServiceTestBase.NETTY_CONNECTOR_FACTORY,
                                                                              backupParams);

            configuration.getConnectorConfigurations().put(nettyBackuptc.getName(), nettyBackuptc);

            configuration.setBackupConnectorName(nettyBackuptc.getName());
         }
         else
         {
            TransportConfiguration invmBackuptc = new TransportConfiguration(ServiceTestBase.INVM_CONNECTOR_FACTORY,
                                                                             backupParams);

            configuration.getConnectorConfigurations().put(invmBackuptc.getName(), invmBackuptc);

            configuration.setBackupConnectorName(invmBackuptc.getName());
         }
      }

      configuration.getAcceptorConfigurations().clear();

      Map<String, Object> params = generateParams(node, netty);

      if (netty)
      {
         TransportConfiguration nettytc = new TransportConfiguration(ServiceTestBase.NETTY_ACCEPTOR_FACTORY, params);
         configuration.getAcceptorConfigurations().add(nettytc);
      }
      else
      {
         TransportConfiguration invmtc = new TransportConfiguration(ServiceTestBase.INVM_ACCEPTOR_FACTORY, params);
         configuration.getAcceptorConfigurations().add(invmtc);
      }

      HornetQServer server;

      if (fileStorage)
      {
         server = HornetQServers.newHornetQServer(configuration);
      }
      else
      {
         server = HornetQServers.newHornetQServer(configuration, false);
      }
      servers[node] = server;
   }

   protected void setupServerWithDiscovery(final int node,
                                           final String groupAddress,
                                           final int port,
                                           final boolean fileStorage,
                                           final boolean netty,
                                           final boolean backup)
   {
      setupServerWithDiscovery(node, groupAddress, port, fileStorage, netty, backup, -1);
   }

   protected void setupServerWithDiscovery(final int node,
                                           final String groupAddress,
                                           final int port,
                                           final boolean fileStorage,
                                           final boolean netty,
                                           final int backupNode)
   {
      setupServerWithDiscovery(node, groupAddress, port, fileStorage, netty, false, backupNode);
   }

   protected void setupServerWithDiscovery(final int node,
                                           final String groupAddress,
                                           final int port,
                                           final boolean fileStorage,
                                           final boolean netty,
                                           final boolean backup,
                                           final int backupNode)
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
      configuration.setJournalMaxIO_AIO(1000);
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
            nettyBackuptc = new TransportConfiguration(ServiceTestBase.NETTY_CONNECTOR_FACTORY, backupParams);

            configuration.getConnectorConfigurations().put(nettyBackuptc.getName(), nettyBackuptc);

            configuration.setBackupConnectorName(nettyBackuptc.getName());
         }
         else
         {
            invmBackuptc = new TransportConfiguration(ServiceTestBase.INVM_CONNECTOR_FACTORY, backupParams);

            configuration.getConnectorConfigurations().put(invmBackuptc.getName(), invmBackuptc);

            configuration.setBackupConnectorName(invmBackuptc.getName());
         }
      }

      configuration.getAcceptorConfigurations().clear();

      Map<String, Object> params = generateParams(node, netty);

      if (netty)
      {
         TransportConfiguration nettytc = new TransportConfiguration(ServiceTestBase.NETTY_ACCEPTOR_FACTORY, params);
         configuration.getAcceptorConfigurations().add(nettytc);
      }
      else
      {
         TransportConfiguration invmtc = new TransportConfiguration(ServiceTestBase.INVM_ACCEPTOR_FACTORY, params);
         configuration.getAcceptorConfigurations().add(invmtc);
      }

      List<Pair<String, String>> connectorPairs = new ArrayList<Pair<String, String>>();

      if (netty)
      {
         TransportConfiguration nettytc_c = new TransportConfiguration(ServiceTestBase.NETTY_CONNECTOR_FACTORY, params);
         configuration.getConnectorConfigurations().put(nettytc_c.getName(), nettytc_c);

         connectorPairs.add(new Pair<String, String>(nettytc_c.getName(),
                                                     nettyBackuptc == null ? null : nettyBackuptc.getName()));
      }
      else
      {
         TransportConfiguration invmtc_c = new TransportConfiguration(ServiceTestBase.INVM_CONNECTOR_FACTORY, params);
         configuration.getConnectorConfigurations().put(invmtc_c.getName(), invmtc_c);

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
         server = HornetQServers.newHornetQServer(configuration);
      }
      else
      {
         server = HornetQServers.newHornetQServer(configuration, false);
      }
      servers[node] = server;
   }

   protected Map<String, Object> generateParams(final int node, final boolean netty)
   {
      Map<String, Object> params = new HashMap<String, Object>();

      if (netty)
      {
         params.put(org.hornetq.integration.transports.netty.TransportConstants.PORT_PROP_NAME,
                    org.hornetq.integration.transports.netty.TransportConstants.DEFAULT_PORT + node);
      }
      else
      {
         params.put(org.hornetq.core.remoting.impl.invm.TransportConstants.SERVER_ID_PROP_NAME, node);
      }

      return params;
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
                                         final boolean netty)
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
         serverTotc = new TransportConfiguration(ServiceTestBase.NETTY_CONNECTOR_FACTORY, params);
      }
      else
      {
         serverTotc = new TransportConfiguration(ServiceTestBase.INVM_CONNECTOR_FACTORY, params);
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
                                                                                      1024,
                                                                                      pairs);
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

      Map<String, TransportConfiguration> connectors = serverFrom.getConfiguration().getConnectorConfigurations();

      List<Pair<String, String>> pairs = new ArrayList<Pair<String, String>>();

      for (int element : nodesTo)
      {
         Map<String, Object> params = generateParams(element, netty);

         TransportConfiguration serverTotc;

         if (netty)
         {
            serverTotc = new TransportConfiguration(ServiceTestBase.NETTY_CONNECTOR_FACTORY, params);
         }
         else
         {
            serverTotc = new TransportConfiguration(ServiceTestBase.INVM_CONNECTOR_FACTORY, params);
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
                                                                                      1024,
                                                                                      pairs);

      serverFrom.getConfiguration().getClusterConfigurations().add(clusterConf);
   }

   protected void setupClusterConnectionWithBackups(final String name,
                                                    final String address,
                                                    final boolean forwardWhenNoConsumers,
                                                    final int maxHops,
                                                    final boolean netty,
                                                    final int nodeFrom,
                                                    final int[] nodesTo,
                                                    final int[] backupsTo)
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
            serverTotc = new TransportConfiguration(ServiceTestBase.NETTY_CONNECTOR_FACTORY, params);
         }
         else
         {
            serverTotc = new TransportConfiguration(ServiceTestBase.INVM_CONNECTOR_FACTORY, params);
         }

         connectors.put(serverTotc.getName(), serverTotc);

         Map<String, Object> backupParams = generateParams(backupsTo[i], netty);

         TransportConfiguration serverBackupTotc;

         if (netty)
         {
            serverBackupTotc = new TransportConfiguration(ServiceTestBase.NETTY_CONNECTOR_FACTORY, backupParams);
         }
         else
         {
            serverBackupTotc = new TransportConfiguration(ServiceTestBase.INVM_CONNECTOR_FACTORY, backupParams);
         }

         connectors.put(serverBackupTotc.getName(), serverBackupTotc);

         Pair<String, String> connectorPair = new Pair<String, String>(serverTotc.getName(), serverBackupTotc.getName());

         pairs.add(connectorPair);
      }

      ClusterConnectionConfiguration clusterConf = new ClusterConnectionConfiguration(name,
                                                                                      address,
                                                                                      250,
                                                                                      true,
                                                                                      forwardWhenNoConsumers,
                                                                                      maxHops,
                                                                                      1024,
                                                                                      pairs);

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

      ClusterConnectionConfiguration clusterConf = new ClusterConnectionConfiguration(name,
                                                                                      address,
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
         ClusterTestBase.log.info("starting server " + node);

         servers[node].start();

         ClusterTestBase.log.info("started server " + node);
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
            }
         }
      }
   }

   protected void stopServers(final int... nodes) throws Exception
   {
      for (int node : nodes)
      {
         if (servers[node].isStarted())
         {
            try
            {
               ClusterTestBase.log.info("stopping server " + node);
               servers[node].stop();
               ClusterTestBase.log.info("server stopped");
            }
            catch (Exception e)
            {
               ClusterTestBase.log.warn(e.getMessage(), e);
            }
         }
      }
   }

   protected boolean isFileStorage()
   {
      return true;
   }
}
