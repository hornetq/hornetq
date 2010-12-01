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
import org.hornetq.tests.util.ServiceTestBase;

/**
 * 
 * A ClusterWithBackupFailoverTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 9 Mar 2009 16:31:21
 *
 *
 */
public abstract class ClusterWithBackupFailoverTestBase extends ClusterTestBase
{
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

   @Override
   protected void tearDown() throws Exception
   {
      stopServers();

     // FailoverManagerImpl.disableDebug();

      super.tearDown();
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

         send(0, "queues.testaddress", 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

         send(1, "queues.testaddress", 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

         send(2, "queues.testaddress", 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

         failNode(0);

         // live nodes
         waitForBindings(1, "queues.testaddress", 1, 1, true);
         waitForBindings(2, "queues.testaddress", 1, 1, true);
         // activated backup nodes
         waitForBindings(3, "queues.testaddress", 1, 1, true);

         // live nodes
         waitForBindings(1, "queues.testaddress", 2, 2, false);
         waitForBindings(2, "queues.testaddress", 2, 2, false);
         // activated backup nodes
         waitForBindings(3, "queues.testaddress", 2, 2, false);

         ClusterWithBackupFailoverTestBase.log.info("** now sending");

         send(0, "queues.testaddress", 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

         send(1, "queues.testaddress", 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

         send(2, "queues.testaddress", 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

         failNode(1);

         // live nodes
         waitForBindings(2, "queues.testaddress", 1, 1, true);
         // activated backup nodes
         waitForBindings(3, "queues.testaddress", 1, 1, true);
         waitForBindings(4, "queues.testaddress", 1, 1, true);

         // live nodes
         waitForBindings(2, "queues.testaddress", 2, 2, false);
         // activated backup nodes
         waitForBindings(3, "queues.testaddress", 2, 2, false);
         waitForBindings(4, "queues.testaddress", 2, 2, false);

         send(0, "queues.testaddress", 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

         send(1, "queues.testaddress", 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

         send(2, "queues.testaddress", 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

         failNode(2);

         // activated backup nodes
         waitForBindings(3, "queues.testaddress", 1, 1, true);
         waitForBindings(4, "queues.testaddress", 1, 1, true);
         waitForBindings(5, "queues.testaddress", 1, 1, true);

         // activated backup nodes
         waitForBindings(3, "queues.testaddress", 2, 2, false);
         waitForBindings(4, "queues.testaddress", 2, 2, false);
         waitForBindings(5, "queues.testaddress", 2, 2, false);

         send(0, "queues.testaddress", 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

         send(1, "queues.testaddress", 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

         send(2, "queues.testaddress", 10, false, null);
         verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

         removeConsumer(0);
         removeConsumer(1);
         removeConsumer(2);

         stopServers();

         ClusterWithBackupFailoverTestBase.log.info("*** test done");
   }
   
   public void testFailBackupNodes() throws Exception
   {
      setupCluster();

      startServers(3, 4, 5, 0, 1, 2);

      setupSessionFactory(0, 3, isNetty(), false);
      setupSessionFactory(1, 4, isNetty(), false);
      setupSessionFactory(2, 5, isNetty(), false);

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

      send(0, "queues.testaddress", 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(1, "queues.testaddress", 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(2, "queues.testaddress", 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      failNode(3);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      send(0, "queues.testaddress", 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(1, "queues.testaddress", 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(2, "queues.testaddress", 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      failNode(4);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      send(0, "queues.testaddress", 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(1, "queues.testaddress", 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(2, "queues.testaddress", 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      failNode(5);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      send(0, "queues.testaddress", 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(1, "queues.testaddress", 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(2, "queues.testaddress", 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      

      removeConsumer(0);
      removeConsumer(1);
      removeConsumer(2);

      stopServers();

      ClusterWithBackupFailoverTestBase.log.info("*** test done");
   }

   protected void setupCluster() throws Exception
   {
      setupCluster(false);
   }

   protected void stopServers() throws Exception
   {

      closeAllServerLocatorsFactories();
      closeAllConsumers();

      closeAllSessionFactories();

      stopServers(0, 1, 2, 3, 4, 5);
   }

   protected void failNode(final int node) throws Exception
   {
      ClusterWithBackupFailoverTestBase.log.info("*** failing node " + node);

      Map<String, Object> params = generateParams(node, isNetty());

      TransportConfiguration serverTC;

      if (isNetty())
      {
         serverTC = new TransportConfiguration(ServiceTestBase.NETTY_CONNECTOR_FACTORY, params);
      }
      else
      {
         serverTC = new TransportConfiguration(ServiceTestBase.INVM_CONNECTOR_FACTORY, params);
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
      //FailoverManagerImpl.failAllConnectionsForConnector(serverTC);

      server.kill();
   }

   public void testFailAllNodes() throws Exception
   {
      setupCluster();

      startServers(3, 4, 5, 0, 1, 2);

      setupSessionFactory(0, 3, isNetty(), false);
      setupSessionFactory(1, 4, isNetty(), false);
      setupSessionFactory(2, 5, isNetty(), false);

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

      send(0, "queues.testaddress", 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(1, "queues.testaddress", 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(2, "queues.testaddress", 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      failNode(0);

      // live nodes
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);
      // activated backup nodes
      waitForBindings(3, "queues.testaddress", 1, 1, true);

      // live nodes
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);
      // activated backup nodes
      waitForBindings(3, "queues.testaddress", 2, 2, false);

      ClusterWithBackupFailoverTestBase.log.info("** now sending");

      send(0, "queues.testaddress", 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(1, "queues.testaddress", 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(2, "queues.testaddress", 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      removeConsumer(0);
      failNode(3);

      // live nodes
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      // live nodes
      waitForBindings(1, "queues.testaddress", 1, 1, false);
      waitForBindings(2, "queues.testaddress", 1, 1, false);

      send(1, "queues.testaddress", 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 1, 2);

      send(2, "queues.testaddress", 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 1, 2);

      failNode(1);

      // live nodes
      waitForBindings(2, "queues.testaddress", 1, 1, true);
      // activated backup nodes
      waitForBindings(4, "queues.testaddress", 1, 1, true);

      // live nodes
      waitForBindings(2, "queues.testaddress", 1, 1, false);
      // activated backup nodes
      waitForBindings(4, "queues.testaddress", 1, 1, false);

      send(1, "queues.testaddress", 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 1, 2);

      send(2, "queues.testaddress", 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 1, 2);

      removeConsumer(1);
      failNode(4);

      // live nodes
      waitForBindings(2, "queues.testaddress", 1, 1, true);
      // live nodes
      waitForBindings(2, "queues.testaddress", 1, 0, false);
     
      send(2, "queues.testaddress", 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 2);

      failNode(2);

      // live nodes
      waitForBindings(5, "queues.testaddress", 1, 1, true);
      // live nodes
      waitForBindings(5, "queues.testaddress", 0, 0, false);

      send(2, "queues.testaddress", 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 2);

      removeConsumer(2);
      failNode(5);

      stopServers();

      ClusterWithBackupFailoverTestBase.log.info("*** test done");
   }
}
