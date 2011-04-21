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

import java.nio.ByteBuffer;
import java.util.ArrayList;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.server.cluster.ClusterConnection;
import org.hornetq.core.server.cluster.MessageFlowRecord;
import org.hornetq.core.server.cluster.impl.ClusterConnectionImpl;
import org.hornetq.core.server.impl.QueueImpl;
import org.hornetq.core.settings.impl.AddressSettings;

/**
 * A MessageRedistributionTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 10 Feb 2009 18:41:57
 *
 *
 */
public class MessageRedistributionTest extends ClusterTestBase
{
   private static final Logger log = Logger.getLogger(MessageRedistributionTest.class);

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      start();
   }

   private void start() throws Exception
   {
      setupServers();

      setRedistributionDelay(0);
   }

   private void stop() throws Exception
   {
      stopServers();
   }

   @Override
   protected void tearDown() throws Exception
   {
      stop();

      super.tearDown();
   }

   protected boolean isNetty()
   {
      return false;
   }

   public void testRedistributionWhenConsumerIsClosed() throws Exception
   {
      setupCluster(false);

      MessageRedistributionTest.log.info("Doing test");

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      send(0, "queues.testaddress", 20, false, null);

      getReceivedOrder(0);
      int[] ids1 = getReceivedOrder(1);
      getReceivedOrder(2);

      removeConsumer(1);

      verifyReceiveRoundRobinInSomeOrderWithCounts(false, ids1, 0, 2);

      MessageRedistributionTest.log.info("Test done");
   }

   // https://issues.jboss.org/browse/HORNETQ-654
   public void testRedistributionWhenConsumerIsClosedAndRestart() throws Exception
   {
      setupCluster(false);

      MessageRedistributionTest.log.info("Doing test");

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, true);
      createQueue(1, "queues.testaddress", "queue0", null, true);
      createQueue(2, "queues.testaddress", "queue0", null, true);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      send(0, "queues.testaddress", 20, true, null);

      getReceivedOrder(0, true);
      int[] ids1 = getReceivedOrder(1, false);
      getReceivedOrder(2, true);

      for (ClusterConnection conn : servers[1].getClusterManager().getClusterConnections())
      {
         ClusterConnectionImpl impl = (ClusterConnectionImpl)conn;
         for (MessageFlowRecord record : impl.getRecords().values())
         {
            if (record.getBridge() != null)
            {
               System.out.println("stop record bridge");
               record.getBridge().stop();
            }
         }
      }

      removeConsumer(1);
      
      // Need to wait some time as we need to handle all redistributions before we stop the servers
      Thread.sleep(5000);

      for (int i = 0; i <= 2; i++)
      {
         servers[i].stop();
         servers[i] = null;
      }
      
      setupServers();
      
      setupCluster(false);
      
      startServers(0, 1, 2);
      
      for (int i = 0 ; i <= 2; i++)
      {
         consumers[i] = null;
         sfs[i] = null;
      }

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      addConsumer(0, 0, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 1, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);

      verifyReceiveRoundRobinInSomeOrderWithCounts(false, ids1, 0, 2);

      MessageRedistributionTest.log.info("Test done");
   }

   public void testRedistributionWhenConsumerIsClosedNotConsumersOnAllNodes() throws Exception
   {
      setupCluster(false);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 1, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);

      send(0, "queues.testaddress", 20, false, null);

      int[] ids1 = getReceivedOrder(1);
      getReceivedOrder(2);

      removeConsumer(1);

      verifyReceiveRoundRobinInSomeOrderWithCounts(false, ids1, 2);
   }

   public void testNoRedistributionWhenConsumerIsClosedForwardWhenNoConsumersTrue() throws Exception
   {
      // x
      setupCluster(true);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      send(0, "queues.testaddress", 20, false, null);

      removeConsumer(1);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 0, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 1, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);

      addConsumer(1, 1, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      verifyReceiveRoundRobinInSomeOrder(20, 0, 1, 2);
   }

   public void testNoRedistributionWhenConsumerIsClosedNoConsumersOnOtherNodes() throws Exception
   {
      setupCluster(false);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(1, 1, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 2, 1, false);
      waitForBindings(1, "queues.testaddress", 2, 0, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);

      send(0, "queues.testaddress", 20, false, null);

      removeConsumer(1);

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 0, true);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 2, 0, false);
      waitForBindings(1, "queues.testaddress", 2, 0, false);
      waitForBindings(2, "queues.testaddress", 2, 0, false);

      addConsumer(1, 1, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 2, 1, false);
      waitForBindings(1, "queues.testaddress", 2, 0, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);

      verifyReceiveAll(20, 1);
   }

   public void testRedistributeWithScheduling() throws Exception
   {
      setupCluster(false);

      AddressSettings setting = new AddressSettings();
      setting.setRedeliveryDelay(10000);
      servers[0].getAddressSettingsRepository().addMatch("queues.testaddress", setting);
      servers[0].getAddressSettingsRepository().addMatch("queue0", setting);
      servers[1].getAddressSettingsRepository().addMatch("queue0", setting);
      servers[1].getAddressSettingsRepository().addMatch("queues.testaddress", setting);
      
      startServers(0);
      
      setupSessionFactory(0, isNetty());
      
      createQueue(0, "queues.testaddress", "queue0", null, false);
      
      ClientSession session0 = sfs[0].createSession(false, false, false);
      
      ClientProducer prod0 = session0.createProducer("queues.testaddress");
      
      for (int i = 0 ; i < 100; i++)
      {
         ClientMessage msg = session0.createMessage(true);
         msg.putIntProperty("key", i);
         
         byte[] bytes = new byte[24];
         
         ByteBuffer bb = ByteBuffer.wrap(bytes);
         
         bb.putLong((long)i);
         
         msg.putBytesProperty(MessageImpl.HDR_BRIDGE_DUPLICATE_ID, bytes);

         prod0.send(msg);
         
         session0.commit();
      }
      
      session0.close();
      
      session0 = sfs[0].createSession(true, false, false);

      ClientConsumer consumer0 = session0.createConsumer("queue0");
      
      session0.start();
      
      ArrayList<Xid> xids = new ArrayList<Xid>();
      
      for (int i = 0 ; i < 100; i++)
      {
         Xid xid = newXID();
         
         session0.start(xid, XAResource.TMNOFLAGS);
         
         ClientMessage msg = consumer0.receive(5000);
         
         msg.acknowledge();
         
         session0.end(xid, XAResource.TMSUCCESS);
         
         session0.prepare(xid);
         
         xids.add(xid);
      }
      
      session0.close();
      
      sfs[0].close();
      sfs[0] = null;
      
      
      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);
      
      ClientSession session1 = sfs[1].createSession(false, false);
      session1.start();
      ClientConsumer consumer1 = session1.createConsumer("queue0");
      
      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 2, 1, false);
      waitForBindings(1, "queues.testaddress", 2, 0, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);
      
      session0 = sfs[0].createSession(true, false, false);
      
      for (Xid xid: xids)
      {
         session0.rollback(xid);
      }
      
      
      for (int i = 0 ; i < 100; i++)
      {
         ClientMessage msg = consumer1.receive(15000);
         assertNotNull(msg);
         msg.acknowledge();
      }
      
      session1.commit();
      
   }

   public void testRedistributionWhenConsumerIsClosedQueuesWithFilters() throws Exception
   {
      setupCluster(false);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      String filter1 = "giraffe";
      String filter2 = "platypus";

      createQueue(0, "queues.testaddress", "queue0", filter1, false);
      createQueue(1, "queues.testaddress", "queue0", filter2, false);
      createQueue(2, "queues.testaddress", "queue0", filter1, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      send(0, "queues.testaddress", 20, false, filter1);

      int[] ids0 = getReceivedOrder(0);
      getReceivedOrder(1);
      getReceivedOrder(2);

      removeConsumer(0);

      verifyReceiveRoundRobinInSomeOrderWithCounts(false, ids0, 2);
   }

   public void testRedistributionWhenConsumerIsClosedConsumersWithFilters() throws Exception
   {
      setupCluster(false);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      String filter1 = "giraffe";
      String filter2 = "platypus";

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", filter1);
      addConsumer(1, 1, "queue0", filter2);
      addConsumer(2, 2, "queue0", filter1);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      send(0, "queues.testaddress", 20, false, filter1);

      int[] ids0 = getReceivedOrder(0);
      getReceivedOrder(1);
      getReceivedOrder(2);

      removeConsumer(0);

      verifyReceiveRoundRobinInSomeOrderWithCounts(false, ids0, 2);
   }

   public void testRedistributionWhenRemoteConsumerIsAdded() throws Exception
   {
      setupCluster(false);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 0, true);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 2, 0, false);
      waitForBindings(1, "queues.testaddress", 2, 1, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);

      send(0, "queues.testaddress", 20, false, null);

      removeConsumer(0);

      addConsumer(1, 1, "queue0", null);

      verifyReceiveAll(20, 1);
      verifyNotReceive(1);
   }

   public void testBackAndForth() throws Exception
   {
      for (int i = 0; i < 10; i++)
      {
         setupCluster(false);

         startServers(0, 1, 2);

         setupSessionFactory(0, isNetty());
         setupSessionFactory(1, isNetty());
         setupSessionFactory(2, isNetty());

         final String ADDRESS = "queues.testaddress";
         final String QUEUE = "queue0";

         createQueue(0, ADDRESS, QUEUE, null, false);
         createQueue(1, ADDRESS, QUEUE, null, false);
         createQueue(2, ADDRESS, QUEUE, null, false);

         addConsumer(0, 0, QUEUE, null);

         waitForBindings(0, ADDRESS, 1, 1, true);
         waitForBindings(1, ADDRESS, 1, 0, true);
         waitForBindings(2, ADDRESS, 1, 0, true);

         waitForBindings(0, ADDRESS, 2, 0, false);
         waitForBindings(1, ADDRESS, 2, 1, false);
         waitForBindings(2, ADDRESS, 2, 1, false);

         send(0, ADDRESS, 20, false, null);

         waitForMessages(0, ADDRESS, 20);

         removeConsumer(0);

         waitForBindings(0, ADDRESS, 1, 0, true);
         waitForBindings(1, ADDRESS, 1, 0, true);
         waitForBindings(2, ADDRESS, 1, 0, true);

         waitForBindings(0, ADDRESS, 2, 0, false);
         waitForBindings(1, ADDRESS, 2, 0, false);
         waitForBindings(2, ADDRESS, 2, 0, false);

         addConsumer(1, 1, QUEUE, null);

         waitForBindings(0, ADDRESS, 1, 0, true);
         waitForBindings(1, ADDRESS, 1, 1, true);
         waitForBindings(2, ADDRESS, 1, 0, true);

         waitForMessages(1, ADDRESS, 20);
         waitForMessages(0, ADDRESS, 0);

         waitForBindings(0, ADDRESS, 2, 1, false);
         waitForBindings(1, ADDRESS, 2, 0, false);
         waitForBindings(2, ADDRESS, 2, 1, false);

         removeConsumer(1);

         waitForBindings(0, ADDRESS, 1, 0, true);
         waitForBindings(1, ADDRESS, 1, 0, true);
         waitForBindings(2, ADDRESS, 1, 0, true);

         waitForBindings(0, ADDRESS, 2, 0, false);
         waitForBindings(1, ADDRESS, 2, 0, false);
         waitForBindings(2, ADDRESS, 2, 0, false);

         addConsumer(0, 0, QUEUE, null);

         waitForBindings(0, ADDRESS, 1, 1, true);
         waitForBindings(1, ADDRESS, 1, 0, true);
         waitForBindings(2, ADDRESS, 1, 0, true);

         waitForBindings(0, ADDRESS, 2, 0, false);
         waitForBindings(1, ADDRESS, 2, 1, false);
         waitForBindings(2, ADDRESS, 2, 1, false);

         waitForMessages(0, ADDRESS, 20);

         verifyReceiveAll(20, 0);
         verifyNotReceive(0);

         addConsumer(1, 1, QUEUE, null);
         verifyNotReceive(1);
         removeConsumer(1);

         stop();
         start();
      }

   }

   public void testBackAndForth2() throws Exception
   {
      for (int i = 0; i < 10; i++)
      {
         setupCluster(false);

         startServers(0, 1);

         setupSessionFactory(0, isNetty());
         setupSessionFactory(1, isNetty());

         final String ADDRESS = "queues.testaddress";
         final String QUEUE = "queue0";

         createQueue(0, ADDRESS, QUEUE, null, false);
         createQueue(1, ADDRESS, QUEUE, null, false);

         addConsumer(0, 0, QUEUE, null);

         waitForBindings(0, ADDRESS, 1, 1, true);
         waitForBindings(1, ADDRESS, 1, 0, true);

         waitForBindings(0, ADDRESS, 1, 0, false);
         waitForBindings(1, ADDRESS, 1, 1, false);

         send(1, ADDRESS, 20, false, null);

         waitForMessages(0, ADDRESS, 20);

         removeConsumer(0);

         waitForBindings(0, ADDRESS, 1, 0, true);
         waitForBindings(1, ADDRESS, 1, 0, true);

         waitForBindings(0, ADDRESS, 1, 0, false);
         waitForBindings(1, ADDRESS, 1, 0, false);

         addConsumer(1, 1, QUEUE, null);

         waitForMessages(1, ADDRESS, 20);
         waitForMessages(0, ADDRESS, 0);

         waitForBindings(0, ADDRESS, 1, 1, false);
         waitForBindings(1, ADDRESS, 1, 0, false);

         removeConsumer(1);

         addConsumer(0, 0, QUEUE, null);

         waitForMessages(1, ADDRESS, 0);
         waitForMessages(0, ADDRESS, 20);

         removeConsumer(0);
         addConsumer(1, 1, QUEUE, null);

         waitForMessages(1, ADDRESS, 20);
         waitForMessages(0, ADDRESS, 0);

         verifyReceiveAll(20, 1);

         stop();
         start();
      }

   }

   public void testRedistributionToQueuesWhereNotAllMessagesMatch() throws Exception
   {
      setupCluster(false);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      String filter1 = "giraffe";
      String filter2 = "platypus";

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 0, true);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 2, 0, false);
      waitForBindings(1, "queues.testaddress", 2, 1, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);

      sendInRange(0, "queues.testaddress", 0, 10, false, filter1);
      sendInRange(0, "queues.testaddress", 10, 20, false, filter2);

      removeConsumer(0);
      addConsumer(1, 1, "queue0", filter1);
      addConsumer(2, 2, "queue0", filter2);

      verifyReceiveAllInRange(0, 10, 1);
      verifyReceiveAllInRange(10, 20, 2);
   }

   public void testDelayedRedistribution() throws Exception
   {
      final long delay = 1000;
      setRedistributionDelay(delay);

      setupCluster(false);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 0, true);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 2, 0, false);
      waitForBindings(1, "queues.testaddress", 2, 1, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);

      send(0, "queues.testaddress", 20, false, null);

      long start = System.currentTimeMillis();

      removeConsumer(0);
      addConsumer(1, 1, "queue0", null);

      long minReceiveTime = start + delay;

      verifyReceiveAllNotBefore(minReceiveTime, 20, 1);
   }

   public void testDelayedRedistributionCancelled() throws Exception
   {
      final long delay = 1000;
      setRedistributionDelay(delay);

      setupCluster(false);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 0, true);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 2, 0, false);
      waitForBindings(1, "queues.testaddress", 2, 1, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);

      send(0, "queues.testaddress", 20, false, null);

      removeConsumer(0);
      addConsumer(1, 1, "queue0", null);

      Thread.sleep(delay / 2);

      // Add it back on the local queue - this should stop any redistributionm
      addConsumer(0, 0, "queue0", null);

      Thread.sleep(delay);

      verifyReceiveAll(20, 0);
   }

   public void testRedistributionNumberOfMessagesGreaterThanBatchSize() throws Exception
   {
      setupCluster(false);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 0, true);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 2, 0, false);
      waitForBindings(1, "queues.testaddress", 2, 1, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);

      send(0, "queues.testaddress", QueueImpl.REDISTRIBUTOR_BATCH_SIZE * 2, false, null);

      removeConsumer(0);
      addConsumer(1, 1, "queue0", null);

      verifyReceiveAll(QueueImpl.REDISTRIBUTOR_BATCH_SIZE * 2, 1);
   }

   /*
    * Start one node with no consumers and send some messages
    * Start another node add a consumer and verify all messages are redistribute
    * https://jira.jboss.org/jira/browse/HORNETQ-359
    */
   public void testRedistributionWhenNewNodeIsAddedWithConsumer() throws Exception
   {
      setupCluster(false);

      startServers(0);

      setupSessionFactory(0, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);

      waitForBindings(0, "queues.testaddress", 1, 0, true);

      send(0, "queues.testaddress", 20, false, null);

      // Now bring up node 1

      startServers(1);

      setupSessionFactory(1, isNetty());

      createQueue(1, "queues.testaddress", "queue0", null, false);

      waitForBindings(1, "queues.testaddress", 1, 0, true);
      waitForBindings(0, "queues.testaddress", 1, 0, false);

      addConsumer(0, 1, "queue0", null);

      verifyReceiveAll(20, 0);
      verifyNotReceive(0);
   }

   protected void setupCluster(final boolean forwardWhenNoConsumers) throws Exception
   {
      setupClusterConnection("cluster0", "queues", forwardWhenNoConsumers, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", forwardWhenNoConsumers, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", forwardWhenNoConsumers, 1, isNetty(), 2, 0, 1);
   }

   protected void setRedistributionDelay(final long delay)
   {
      AddressSettings as = new AddressSettings();
      as.setRedistributionDelay(delay);

      getServer(0).getAddressSettingsRepository().addMatch("queues.*", as);
      getServer(1).getAddressSettingsRepository().addMatch("queues.*", as);
      getServer(2).getAddressSettingsRepository().addMatch("queues.*", as);
   }

   protected void setupServers() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());
   }

   protected void stopServers() throws Exception
   {
      closeAllConsumers();

      closeAllSessionFactories();

      closeAllServerLocatorsFactories();

      stopServers(0, 1, 2);

      clearServer(0, 1, 2);
   }

}
