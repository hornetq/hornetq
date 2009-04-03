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

package org.jboss.messaging.tests.integration.cluster.failover;

import org.jboss.messaging.core.logging.Logger;

/**
 * A DiscoveryClusterWithBackupFailoverTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class DiscoveryClusterWithBackupFailoverTest extends ClusterWithBackupFailoverTest
{
   private static final Logger log = Logger.getLogger(DiscoveryClusterWithBackupFailoverTest.class);

   protected static final String groupAddress = "230.1.2.3";

   protected static final int groupPort = 6745;

   protected boolean isNetty()
   {
      return false;
   }

   protected boolean isFileStorage()
   {
      return false;
   }

   @Override
   public void testFailAllNodes() throws Exception
   {
      for (int i = 0; i < 5; i++)
      {
         log.info("*** iteration " + i);

         tearDown();

         super.clearAllServers();

         setUp();

         this.setupCluster();

         startServers(3, 4, 5, 0, 1, 2);

         setupSessionFactory(0, 3, isNetty(), false);
         setupSessionFactory(1, 4, isNetty(), false);
         setupSessionFactory(2, 5, isNetty(), false);

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

         send(0, "queues.testaddress", 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(10, 0, 1, 2);

         send(1, "queues.testaddress", 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(10, 0, 1, 2);

         send(2, "queues.testaddress", 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(10, 0, 1, 2);

         failNode(0);

         send(0, "queues.testaddress", 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(10, 0, 1, 2);

         send(1, "queues.testaddress", 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(10, 0, 1, 2);

         send(2, "queues.testaddress", 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(10, 0, 1, 2);

         failNode(1);

         send(0, "queues.testaddress", 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(10, 0, 1, 2);

         send(1, "queues.testaddress", 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(10, 0, 1, 2);

         send(2, "queues.testaddress", 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(10, 0, 1, 2);

         failNode(2);

         send(0, "queues.testaddress", 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(10, 0, 1, 2);

         send(1, "queues.testaddress", 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(10, 0, 1, 2);

         send(2, "queues.testaddress", 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(10, 0, 1, 2);

         stopServers();
      }

   }

   @Override
   protected void setupCluster(final boolean forwardWhenNoConsumers) throws Exception
   {
      // The lives

      setupDiscoveryClusterConnection("cluster0", 0, "dg1", "queues", forwardWhenNoConsumers, 1, isNetty());

      setupDiscoveryClusterConnection("cluster1", 1, "dg1", "queues", forwardWhenNoConsumers, 1, isNetty());

      setupDiscoveryClusterConnection("cluster2", 2, "dg1", "queues", forwardWhenNoConsumers, 1, isNetty());

      // The backups

      setupDiscoveryClusterConnection("cluster0", 3, "dg1", "queues", forwardWhenNoConsumers, 1, isNetty());

      setupDiscoveryClusterConnection("cluster1", 4, "dg1", "queues", forwardWhenNoConsumers, 1, isNetty());

      setupDiscoveryClusterConnection("cluster2", 5, "dg1", "queues", forwardWhenNoConsumers, 1, isNetty());
   }

   @Override
   protected void setupServers() throws Exception
   {
      // The lives
      setupServerWithDiscovery(0, groupAddress, groupPort, isFileStorage(), isNetty(), 3);
      setupServerWithDiscovery(1, groupAddress, groupPort, isFileStorage(), isNetty(), 4);
      setupServerWithDiscovery(2, groupAddress, groupPort, isFileStorage(), isNetty(), 5);

      // The backups
      setupServerWithDiscovery(3, groupAddress, groupPort, isFileStorage(), isNetty(), true);
      setupServerWithDiscovery(4, groupAddress, groupPort, isFileStorage(), isNetty(), true);
      setupServerWithDiscovery(5, groupAddress, groupPort, isFileStorage(), isNetty(), true);
   }

}
