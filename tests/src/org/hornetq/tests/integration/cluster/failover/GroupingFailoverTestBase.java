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
package org.hornetq.tests.integration.cluster.failover;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.core.server.cluster.MessageFlowRecord;
import org.hornetq.core.server.cluster.impl.ClusterConnectionImpl;
import org.hornetq.core.server.group.impl.GroupingHandlerConfiguration;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.tests.integration.cluster.distribution.ClusterTestBase;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         Created Oct 26, 2009
 */
public abstract class GroupingFailoverTestBase extends ClusterTestBase
{

   public void testGroupingLocalHandlerFails() throws Exception
   {
      setupReplicatedServer(2, isFileStorage(), isNetty(), 0);

      setupMasterServer(0, isFileStorage(), isNetty());

      setupServer(1, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1);

      setupClusterConnectionWithBackups("cluster1", "queues", false, 1, isNetty(), 1, new int[] { 0 }, new int[] { 2 });

      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 2);

      startServers(2, 0, 1);

      try
      {
         setupSessionFactory(0, isNetty());
         setupSessionFactory(1, isNetty());

         createQueue(0, "queues.testaddress", "queue0", null, true);
         createQueue(1, "queues.testaddress", "queue0", null, true);

         waitForBindings(0, "queues.testaddress", 1, 0, true);
         waitForBindings(1, "queues.testaddress", 1, 0, true);

         addConsumer(0, 0, "queue0", null);
         addConsumer(1, 1, "queue0", null);

         waitForBindings(0, "queues.testaddress", 1, 1, false);
         waitForBindings(1, "queues.testaddress", 1, 1, false);

         sendWithProperty(0, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id1"));

         verifyReceiveAll(10, 0);

         closeSessionFactory(0);

         final CountDownLatch latch = new CountDownLatch(1);

         class MyListener implements FailureListener
         {
            public void connectionFailed(final HornetQException me)
            {
               latch.countDown();
            }
         }

         Map<String, MessageFlowRecord> records = ((ClusterConnectionImpl)getServer(1).getClusterManager()
                                                                                      .getClusterConnection(new SimpleString("cluster1"))).getRecords();
         RemotingConnection rc = records.get("0").getBridge().getForwardingConnection();
         rc.addFailureListener(new MyListener());
         fail(rc, latch);

         waitForServerRestart(2);

         setupSessionFactory(2, isNetty());

         addConsumer(2, 2, "queue0", null);

         waitForBindings(2, "queues.testaddress", 1, 1, true);

         waitForBindings(2, "queues.testaddress", 1, 1, false);

         waitForBindings(1, "queues.testaddress", 1, 1, true);

         waitForBindings(1, "queues.testaddress", 1, 1, false);

         sendWithProperty(2, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id1"));

         verifyReceiveAll(10, 2);

         System.out.println("*****************************************************************************");
      }
      finally
      {
         closeAllConsumers();

         closeAllSessionFactories();

         stopServers(0, 1, 2);
      }
   }

   public void testGroupingLocalHandlerFailsMultipleGroups() throws Exception
   {
      setupReplicatedServer(2, isFileStorage(), isNetty(), 0);

      setupMasterServer(0, isFileStorage(), isNetty());

      setupServer(1, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1);

      setupClusterConnectionWithBackups("cluster1", "queues", false, 1, isNetty(), 1, new int[] { 0 }, new int[] { 2 });

      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 2);

      startServers(2, 0, 1);

      try
      {
         setupSessionFactory(0, isNetty());
         setupSessionFactory(1, isNetty());

         createQueue(0, "queues.testaddress", "queue0", null, true);

         waitForBindings(0, "queues.testaddress", 1, 0, true);

         createQueue(1, "queues.testaddress", "queue0", null, true);

         waitForBindings(1, "queues.testaddress", 1, 0, true);

         addConsumer(0, 0, "queue0", null);
         addConsumer(1, 1, "queue0", null);

         waitForBindings(0, "queues.testaddress", 1, 1, false);
         waitForBindings(1, "queues.testaddress", 1, 1, false);

         sendWithProperty(0, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id1"));
         sendWithProperty(0, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id2"));
         sendWithProperty(0, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id3"));
         sendWithProperty(0, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id4"));
         sendWithProperty(0, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id5"));
         sendWithProperty(0, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id6"));

         verifyReceiveAllWithGroupIDRoundRobin(0, 30, 0, 1);

         closeSessionFactory(0);

         final CountDownLatch latch = new CountDownLatch(1);

         class MyListener implements FailureListener
         {
            public void connectionFailed(final HornetQException me)
            {
               latch.countDown();
            }
         }

         Map<String, MessageFlowRecord> records = ((ClusterConnectionImpl)getServer(1).getClusterManager()
                                                                                      .getClusterConnection(new SimpleString("cluster1"))).getRecords();
         RemotingConnection rc = records.get("0").getBridge().getForwardingConnection();
         rc.addFailureListener(new MyListener());
         fail(rc, latch);

         waitForServerRestart(2);

         setupSessionFactory(2, isNetty());

         addConsumer(2, 2, "queue0", null);

         waitForBindings(2, "queues.testaddress", 1, 1, true);

         waitForBindings(2, "queues.testaddress", 1, 1, false);

         waitForBindings(1, "queues.testaddress", 1, 1, true);

         waitForBindings(1, "queues.testaddress", 1, 1, false);

         sendWithProperty(2, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id1"));
         sendWithProperty(2, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id2"));
         sendWithProperty(2, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id3"));
         sendWithProperty(2, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id4"));
         sendWithProperty(2, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id5"));
         sendWithProperty(2, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id6"));

         verifyReceiveAllWithGroupIDRoundRobin(0, 30, 1, 2);

         System.out.println("*****************************************************************************");
      }
      finally
      {
         closeAllConsumers();

         closeAllSessionFactories();

         stopServers(0, 1, 2);
      }
   }

   abstract void setupMasterServer(int i, boolean fileStorage, boolean netty);

   public boolean isNetty()
   {
      return true;
   }

   abstract void setupReplicatedServer(int node, boolean fileStorage, boolean netty, int backupNode);

   private void fail(final RemotingConnection conn, final CountDownLatch latch) throws InterruptedException
   {
      // Simulate failure on connection
      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      // Wait to be informed of failure

      boolean ok = latch.await(1000, TimeUnit.MILLISECONDS);

      Assert.assertTrue(ok);
   }

}
