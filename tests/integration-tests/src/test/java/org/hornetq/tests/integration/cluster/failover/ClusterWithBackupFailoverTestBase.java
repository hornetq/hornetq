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

package org.hornetq.tests.integration.cluster.failover;

import java.util.Map;
import java.util.Set;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.cluster.BroadcastGroup;
import org.hornetq.core.server.cluster.impl.ClusterManagerImpl;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.tests.integration.cluster.distribution.ClusterTestBase;
import org.hornetq.tests.util.UnitTestCase;

/**
 *
 * A ClusterWithBackupFailoverTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * Created 9 Mar 2009 16:31:21
 */
public abstract class ClusterWithBackupFailoverTestBase extends ClusterTestBase
{
   protected static final String QUEUE_NAME = "queue0";
   protected static final String QUEUES_TESTADDRESS = "queues.testaddress";
   private static final Logger log = Logger.getLogger(ClusterWithBackupFailoverTestBase.class);

   protected abstract void setupCluster(final boolean forwardWhenNoConsumers) throws Exception;

   protected abstract void setupServers() throws Exception;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      //FailoverManagerImpl.enableDebug();

      setupServers();
   }

   protected boolean isNetty()
   {
      return false;
   }

   public void testFailLiveNodes() throws Exception
   {
         setupCluster();

         startServers(3, 4, 5, 0, 1, 2);

         setupSessionFactory(0, 3, isNetty(), false);
         setupSessionFactory(1, 4, isNetty(), false);
         setupSessionFactory(2, 5, isNetty(), false);

         createQueue(0, QUEUES_TESTADDRESS, QUEUE_NAME, null, true);
         createQueue(1, QUEUES_TESTADDRESS, QUEUE_NAME, null, true);
         createQueue(2, QUEUES_TESTADDRESS, QUEUE_NAME, null, true);

         addConsumer(0, 0, QUEUE_NAME, null);
         addConsumer(1, 1, QUEUE_NAME, null);
         addConsumer(2, 2, QUEUE_NAME, null);

         waitForBindings();

         send(0, QUEUES_TESTADDRESS, 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

         send(1, QUEUES_TESTADDRESS, 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

         send(2, QUEUES_TESTADDRESS, 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

         failNode(0);

         // live nodes
         waitForBindings(1, QUEUES_TESTADDRESS, 1, 1, true);
         waitForBindings(2, QUEUES_TESTADDRESS, 1, 1, true);
         // activated backup nodes
         waitForBindings(3, QUEUES_TESTADDRESS, 1, 1, true);

         // live nodes
         waitForBindings(1, QUEUES_TESTADDRESS, 2, 2, false);
         waitForBindings(2, QUEUES_TESTADDRESS, 2, 2, false);
         // activated backup nodes
         waitForBindings(3, QUEUES_TESTADDRESS, 2, 2, false);

         ClusterWithBackupFailoverTestBase.log.info("** now sending");

         send(0, QUEUES_TESTADDRESS, 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

         send(1, QUEUES_TESTADDRESS, 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

         send(2, QUEUES_TESTADDRESS, 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

         failNode(1);

         // live nodes
         waitForBindings(2, QUEUES_TESTADDRESS, 1, 1, true);
         // activated backup nodes
         waitForBindings(3, QUEUES_TESTADDRESS, 1, 1, true);
         waitForBindings(4, QUEUES_TESTADDRESS, 1, 1, true);

         // live nodes
         waitForBindings(2, QUEUES_TESTADDRESS, 2, 2, false);
         // activated backup nodes
         waitForBindings(3, QUEUES_TESTADDRESS, 2, 2, false);
         waitForBindings(4, QUEUES_TESTADDRESS, 2, 2, false);

         send(0, QUEUES_TESTADDRESS, 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

         send(1, QUEUES_TESTADDRESS, 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

         send(2, QUEUES_TESTADDRESS, 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

         failNode(2);

         // activated backup nodes
         waitForBindings(3, QUEUES_TESTADDRESS, 1, 1, true);
         waitForBindings(4, QUEUES_TESTADDRESS, 1, 1, true);
         waitForBindings(5, QUEUES_TESTADDRESS, 1, 1, true);

         // activated backup nodes
         waitForBindings(3, QUEUES_TESTADDRESS, 2, 2, false);
         waitForBindings(4, QUEUES_TESTADDRESS, 2, 2, false);
         waitForBindings(5, QUEUES_TESTADDRESS, 2, 2, false);

         send(0, QUEUES_TESTADDRESS, 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

         send(1, QUEUES_TESTADDRESS, 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

         send(2, QUEUES_TESTADDRESS, 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

         removeConsumer(0);
         removeConsumer(1);
         removeConsumer(2);
   }

   private void waitForBindings() throws Exception
   {
      waitForBindings(0, QUEUES_TESTADDRESS, 1, 1, true);
      waitForBindings(1, QUEUES_TESTADDRESS, 1, 1, true);
      waitForBindings(2, QUEUES_TESTADDRESS, 1, 1, true);

      waitForBindings(0, QUEUES_TESTADDRESS, 2, 2, false);
      waitForBindings(1, QUEUES_TESTADDRESS, 2, 2, false);
      waitForBindings(2, QUEUES_TESTADDRESS, 2, 2, false);
   }

   public void testFailBackupNodes() throws Exception
   {
      setupCluster();

      startServers(3, 4, 5, 0, 1, 2);

      setupSessionFactory(0, 3, isNetty(), false);
      setupSessionFactory(1, 4, isNetty(), false);
      setupSessionFactory(2, 5, isNetty(), false);

      createQueue(0, QUEUES_TESTADDRESS, QUEUE_NAME, null, true);
      createQueue(1, QUEUES_TESTADDRESS, QUEUE_NAME, null, true);
      createQueue(2, QUEUES_TESTADDRESS, QUEUE_NAME, null, true);

      addConsumer(0, 0, QUEUE_NAME, null);
      addConsumer(1, 1, QUEUE_NAME, null);
      addConsumer(2, 2, QUEUE_NAME, null);

      waitForBindings();

      send(0, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(1, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(2, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      failNode(3);

      waitForBindings();

      send(0, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(1, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(2, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      failNode(4);

      waitForBindings();

      send(0, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(1, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(2, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      failNode(5);

      waitForBindings();

      send(0, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(1, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(2, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      removeConsumer(0);
      removeConsumer(1);
      removeConsumer(2);
   }

   protected void setupCluster() throws Exception
   {
      setupCluster(false);
   }

   protected void failNode(final int node) throws Exception
   {
      ClusterWithBackupFailoverTestBase.log.info("*** failing node " + node);

      Map<String, Object> params = generateParams(node, isNetty());

      TransportConfiguration serverTC;

      if (isNetty())
      {
         serverTC = new TransportConfiguration(UnitTestCase.NETTY_CONNECTOR_FACTORY, params);
      }
      else
      {
         serverTC = new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY, params);
      }

      HornetQServer server = getServer(node);

      // Prevent remoting service taking any more connections
      server.getRemotingService().freeze();

      if (server.getClusterManager() != null)
      {
         // Stop it broadcasting
         for (BroadcastGroup group : server.getClusterManager().getBroadcastGroups())
         {
            group.stop();
         }
      }
      Set<RemotingConnection> connections = server.getRemotingService().getConnections();
      for (RemotingConnection remotingConnection : connections)
      {
         remotingConnection.destroy();
         server.getRemotingService().removeConnection(remotingConnection.getID());
      }

      ClusterManagerImpl clusterManager = (ClusterManagerImpl) server.getClusterManager();
      clusterManager.clear();

      server.stop(true);
   }

   public void testFailAllNodes() throws Exception
   {
      setupCluster();

      startServers(0, 1, 2, 3, 4, 5);

      setupSessionFactory(0, 3, isNetty(), false);
      setupSessionFactory(1, 4, isNetty(), false);
      setupSessionFactory(2, 5, isNetty(), false);

      createQueue(0, QUEUES_TESTADDRESS, QUEUE_NAME, null, true);
      createQueue(1, QUEUES_TESTADDRESS, QUEUE_NAME, null, true);
      createQueue(2, QUEUES_TESTADDRESS, QUEUE_NAME, null, true);

      addConsumer(0, 0, QUEUE_NAME, null);
      addConsumer(1, 1, QUEUE_NAME, null);
      addConsumer(2, 2, QUEUE_NAME, null);

      waitForBindings();

      send(0, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(1, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(2, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      failNode(0);

      // live nodes
      waitForBindings(1, QUEUES_TESTADDRESS, 1, 1, true);
      waitForBindings(2, QUEUES_TESTADDRESS, 1, 1, true);
      // activated backup nodes
      waitForBindings(3, QUEUES_TESTADDRESS, 1, 1, true);

      // live nodes
      waitForBindings(1, QUEUES_TESTADDRESS, 2, 2, false);
      waitForBindings(2, QUEUES_TESTADDRESS, 2, 2, false);
      // activated backup nodes
      waitForBindings(3, QUEUES_TESTADDRESS, 2, 2, false);

      ClusterWithBackupFailoverTestBase.log.info("** now sending");

      send(0, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(1, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(2, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      removeConsumer(0);
      failNode(3);

      // live nodes
      waitForBindings(1, QUEUES_TESTADDRESS, 1, 1, true);
      waitForBindings(2, QUEUES_TESTADDRESS, 1, 1, true);

      // live nodes
      waitForBindings(1, QUEUES_TESTADDRESS, 1, 1, false);
      waitForBindings(2, QUEUES_TESTADDRESS, 1, 1, false);

      send(1, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 1, 2);

      send(2, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 1, 2);

      failNode(1);

      // live nodes
      waitForBindings(2, QUEUES_TESTADDRESS, 1, 1, true);
      // activated backup nodes
      waitForBindings(4, QUEUES_TESTADDRESS, 1, 1, true);

      // live nodes
      waitForBindings(2, QUEUES_TESTADDRESS, 1, 1, false);
      // activated backup nodes
      waitForBindings(4, QUEUES_TESTADDRESS, 1, 1, false);

      send(1, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 1, 2);

      send(2, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 1, 2);

      removeConsumer(1);
      failNode(4);

      // live nodes
      waitForBindings(2, QUEUES_TESTADDRESS, 1, 1, true);
      // live nodes
      waitForBindings(2, QUEUES_TESTADDRESS, 1, 0, false);

      send(2, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 2);

      failNode(2);

      // live nodes
      waitForBindings(5, QUEUES_TESTADDRESS, 1, 1, true);
      // live nodes
      waitForBindings(5, QUEUES_TESTADDRESS, 0, 0, false);

      send(2, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 2);

      removeConsumer(2);
      failNode(5);
   }
}
