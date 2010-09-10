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

import org.hornetq.core.logging.Logger;

/**
 * A SymmetricClusterWithDiscoveryTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 3 Feb 2009 09:10:43
 *
 *
 */
public class SymmetricClusterWithDiscoveryTest extends SymmetricClusterTest
{
   private static final Logger log = Logger.getLogger(SymmetricClusterWithDiscoveryTest.class);

   protected static final String groupAddress = getUDPDiscoveryAddress();

   protected static final int groupPort = getUDPDiscoveryPort();

   protected boolean isNetty()
   {
      return false;
   }

   @Override
   protected void setupCluster() throws Exception
   {
      setupCluster(false);
   }

   @Override
   protected void setupCluster(final boolean forwardWhenNoConsumers) throws Exception
   {
      setupDiscoveryClusterConnection("cluster0", 0, "dg1", "queues", forwardWhenNoConsumers, 1, isNetty());

      setupDiscoveryClusterConnection("cluster1", 1, "dg1", "queues", forwardWhenNoConsumers, 1, isNetty());

      setupDiscoveryClusterConnection("cluster2", 2, "dg1", "queues", forwardWhenNoConsumers, 1, isNetty());

      setupDiscoveryClusterConnection("cluster3", 3, "dg1", "queues", forwardWhenNoConsumers, 1, isNetty());

      setupDiscoveryClusterConnection("cluster4", 4, "dg1", "queues", forwardWhenNoConsumers, 1, isNetty());
   }

   @Override
   protected void setupServers() throws Exception
   {
      setupServerWithDiscovery(0,
                               SymmetricClusterWithDiscoveryTest.groupAddress,
                               SymmetricClusterWithDiscoveryTest.groupPort,
                               isFileStorage(),
                               isNetty(),
                               false);
      setupServerWithDiscovery(1,
                               SymmetricClusterWithDiscoveryTest.groupAddress,
                               SymmetricClusterWithDiscoveryTest.groupPort,
                               isFileStorage(),
                               isNetty(),
                               false);
      setupServerWithDiscovery(2,
                               SymmetricClusterWithDiscoveryTest.groupAddress,
                               SymmetricClusterWithDiscoveryTest.groupPort,
                               isFileStorage(),
                               isNetty(),
                               false);
      setupServerWithDiscovery(3,
                               SymmetricClusterWithDiscoveryTest.groupAddress,
                               SymmetricClusterWithDiscoveryTest.groupPort,
                               isFileStorage(),
                               isNetty(),
                               false);
      setupServerWithDiscovery(4,
                               SymmetricClusterWithDiscoveryTest.groupAddress,
                               SymmetricClusterWithDiscoveryTest.groupPort,
                               isFileStorage(),
                               isNetty(),
                               false);
   }

   /*
    * This is like testStopStartServers but we make sure we pause longer than discovery group timeout
    * before restarting (5 seconds)
    */
   public void testStartStopServersWithPauseBeforeRestarting() throws Exception
   {
      setupCluster();

      startServers();

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());
      setupSessionFactory(3, isNetty());
      setupSessionFactory(4, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue1", null, false);
      createQueue(2, "queues.testaddress", "queue2", null, false);
      createQueue(3, "queues.testaddress", "queue3", null, false);
      createQueue(4, "queues.testaddress", "queue4", null, false);

      createQueue(0, "queues.testaddress", "queue5", null, false);
      createQueue(1, "queues.testaddress", "queue6", null, false);
      createQueue(2, "queues.testaddress", "queue7", null, false);
      createQueue(3, "queues.testaddress", "queue8", null, false);
      createQueue(4, "queues.testaddress", "queue9", null, false);

      createQueue(0, "queues.testaddress", "queue10", null, false);
      createQueue(1, "queues.testaddress", "queue11", null, false);
      createQueue(2, "queues.testaddress", "queue12", null, false);
      createQueue(3, "queues.testaddress", "queue13", null, false);
      createQueue(4, "queues.testaddress", "queue14", null, false);

      createQueue(0, "queues.testaddress", "queue15", null, false);
      createQueue(1, "queues.testaddress", "queue15", null, false);
      createQueue(2, "queues.testaddress", "queue15", null, false);
      createQueue(3, "queues.testaddress", "queue15", null, false);
      createQueue(4, "queues.testaddress", "queue15", null, false);

      createQueue(2, "queues.testaddress", "queue16", null, false);
      createQueue(3, "queues.testaddress", "queue16", null, false);
      createQueue(4, "queues.testaddress", "queue16", null, false);

      createQueue(0, "queues.testaddress", "queue17", null, false);
      createQueue(1, "queues.testaddress", "queue17", null, false);
      createQueue(4, "queues.testaddress", "queue17", null, false);

      createQueue(3, "queues.testaddress", "queue18", null, false);
      createQueue(4, "queues.testaddress", "queue18", null, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue1", null);
      addConsumer(2, 2, "queue2", null);
      addConsumer(3, 3, "queue3", null);
      addConsumer(4, 4, "queue4", null);

      addConsumer(5, 0, "queue5", null);
      addConsumer(6, 1, "queue6", null);
      addConsumer(7, 2, "queue7", null);
      addConsumer(8, 3, "queue8", null);
      addConsumer(9, 4, "queue9", null);

      addConsumer(10, 0, "queue10", null);
      addConsumer(11, 1, "queue11", null);
      addConsumer(12, 2, "queue12", null);
      addConsumer(13, 3, "queue13", null);
      addConsumer(14, 4, "queue14", null);

      addConsumer(15, 0, "queue15", null);
      addConsumer(16, 1, "queue15", null);
      addConsumer(17, 2, "queue15", null);
      addConsumer(18, 3, "queue15", null);
      addConsumer(19, 4, "queue15", null);

      addConsumer(20, 2, "queue16", null);
      addConsumer(21, 3, "queue16", null);
      addConsumer(22, 4, "queue16", null);

      addConsumer(23, 0, "queue17", null);
      addConsumer(24, 1, "queue17", null);
      addConsumer(25, 4, "queue17", null);

      addConsumer(26, 3, "queue18", null);
      addConsumer(27, 4, "queue18", null);

      waitForBindings(0, "queues.testaddress", 5, 5, true);
      waitForBindings(1, "queues.testaddress", 5, 5, true);
      waitForBindings(2, "queues.testaddress", 5, 5, true);
      waitForBindings(3, "queues.testaddress", 6, 6, true);
      waitForBindings(4, "queues.testaddress", 7, 7, true);

      waitForBindings(0, "queues.testaddress", 23, 23, false);
      waitForBindings(1, "queues.testaddress", 23, 23, false);
      waitForBindings(2, "queues.testaddress", 23, 23, false);
      waitForBindings(3, "queues.testaddress", 22, 22, false);
      waitForBindings(4, "queues.testaddress", 21, 21, false);

      send(0, "queues.testaddress", 10, false, null);

      verifyReceiveAll(10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);

      verifyReceiveRoundRobinInSomeOrder(10, 15, 16, 17, 18, 19);

      verifyReceiveRoundRobinInSomeOrder(10, 20, 21, 22);

      verifyReceiveRoundRobinInSomeOrder(10, 23, 24, 25);

      verifyReceiveRoundRobinInSomeOrder(10, 26, 27);

      removeConsumer(0);
      removeConsumer(5);
      removeConsumer(10);
      removeConsumer(15);
      removeConsumer(23);
      removeConsumer(3);
      removeConsumer(8);
      removeConsumer(13);
      removeConsumer(18);
      removeConsumer(21);
      removeConsumer(26);

      closeSessionFactory(0);
      closeSessionFactory(3);

      stopServers(0, 3);

      Thread.sleep(10000);

      startServers(3, 0);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(3, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(3, "queues.testaddress", "queue3", null, false);

      createQueue(0, "queues.testaddress", "queue5", null, false);
      createQueue(3, "queues.testaddress", "queue8", null, false);

      createQueue(0, "queues.testaddress", "queue10", null, false);
      createQueue(3, "queues.testaddress", "queue13", null, false);

      createQueue(0, "queues.testaddress", "queue15", null, false);
      createQueue(3, "queues.testaddress", "queue15", null, false);

      createQueue(3, "queues.testaddress", "queue16", null, false);

      createQueue(0, "queues.testaddress", "queue17", null, false);

      createQueue(3, "queues.testaddress", "queue18", null, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(3, 3, "queue3", null);

      addConsumer(5, 0, "queue5", null);
      addConsumer(8, 3, "queue8", null);

      addConsumer(10, 0, "queue10", null);
      addConsumer(13, 3, "queue13", null);

      addConsumer(15, 0, "queue15", null);
      addConsumer(18, 3, "queue15", null);

      addConsumer(21, 3, "queue16", null);

      addConsumer(23, 0, "queue17", null);

      addConsumer(26, 3, "queue18", null);

      waitForBindings(0, "queues.testaddress", 5, 5, true);
      waitForBindings(1, "queues.testaddress", 5, 5, true);
      waitForBindings(2, "queues.testaddress", 5, 5, true);
      waitForBindings(3, "queues.testaddress", 6, 6, true);
      waitForBindings(4, "queues.testaddress", 7, 7, true);

      waitForBindings(0, "queues.testaddress", 23, 23, false);
      waitForBindings(1, "queues.testaddress", 23, 23, false);
      waitForBindings(2, "queues.testaddress", 23, 23, false);
      waitForBindings(3, "queues.testaddress", 22, 22, false);
      waitForBindings(4, "queues.testaddress", 21, 21, false);

      send(0, "queues.testaddress", 10, false, null);

      verifyReceiveAll(10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);

      verifyReceiveRoundRobinInSomeOrder(10, 15, 16, 17, 18, 19);

      verifyReceiveRoundRobinInSomeOrder(10, 20, 21, 22);

      verifyReceiveRoundRobinInSomeOrder(10, 23, 24, 25);

      verifyReceiveRoundRobinInSomeOrder(10, 26, 27);
   }

}
