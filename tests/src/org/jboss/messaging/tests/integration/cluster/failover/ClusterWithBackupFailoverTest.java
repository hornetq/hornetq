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

import java.util.Map;

import org.jboss.messaging.core.client.impl.ConnectionManagerImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.tests.integration.cluster.distribution.ClusterTestBase;

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
public class ClusterWithBackupFailoverTest extends ClusterTestBase
{
   private static final Logger log = Logger.getLogger(ClusterWithBackupFailoverTest.class);

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      ConnectionManagerImpl.enableDebug();

      setupServers();
   }

   @Override
   protected void tearDown() throws Exception
   {
      stopServers();

      super.tearDown();
   }

   protected boolean isNetty()
   {
      return false;
   }

   protected boolean isFileStorage()
   {
      return false;
   }

   public void testFailAllNodes() throws Exception
   {
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

//      send(1, "queues.testaddress", 10, false, null);
//      verifyReceiveRoundRobinInSomeOrder(10, 0, 1, 2);
//            
//      send(2, "queues.testaddress", 10, false, null);
//      verifyReceiveRoundRobinInSomeOrder(10, 0, 1, 2);

      failNode(2);

      send(0, "queues.testaddress", 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(10, 0, 1, 2);

//      send(1, "queues.testaddress", 10, false, null);
//      verifyReceiveRoundRobinInSomeOrder(10, 0, 1, 2);
//            
//      send(2, "queues.testaddress", 10, false, null);
//      verifyReceiveRoundRobinInSomeOrder(10, 0, 1, 2);

      stopServers();
   }

   protected void setupCluster() throws Exception
   {
      setupCluster(false);
   }

   protected void setupCluster(final boolean forwardWhenNoConsumers) throws Exception
   {
      setupClusterConnectionWithBackups("cluster0",
                                        "queues",
                                        forwardWhenNoConsumers,
                                        1,
                                        isNetty(),
                                        0,
                                        new int[] { 1, 2 },
                                        new int[] { 4, 5 });

      setupClusterConnectionWithBackups("cluster1",
                                        "queues",
                                        forwardWhenNoConsumers,
                                        1,
                                        isNetty(),
                                        1,
                                        new int[] { 0, 2 },
                                        new int[] { 3, 5 });

      setupClusterConnectionWithBackups("cluster2",
                                        "queues",
                                        forwardWhenNoConsumers,
                                        1,
                                        isNetty(),
                                        2,
                                        new int[] { 0, 1 },
                                        new int[] { 3, 4 });

      setupClusterConnectionWithBackups("cluster0",
                                        "queues",
                                        forwardWhenNoConsumers,
                                        1,
                                        isNetty(),
                                        3,
                                        new int[] { 1, 2 },
                                        new int[] { 4, 5 });

      setupClusterConnectionWithBackups("cluster1",
                                        "queues",
                                        forwardWhenNoConsumers,
                                        1,
                                        isNetty(),
                                        4,
                                        new int[] { 0, 2 },
                                        new int[] { 3, 5 });

      setupClusterConnectionWithBackups("cluster2",
                                        "queues",
                                        forwardWhenNoConsumers,
                                        1,
                                        isNetty(),
                                        5,
                                        new int[] { 0, 1 },
                                        new int[] { 3, 4 });
   }

   protected void setupServers() throws Exception
   {
      // The backups
      setupServer(3, isFileStorage(), isNetty(), true);
      setupServer(4, isFileStorage(), isNetty(), true);
      setupServer(5, isFileStorage(), isNetty(), true);

      // The lives
      setupServer(0, isFileStorage(), isNetty(), 3);
      setupServer(1, isFileStorage(), isNetty(), 4);
      setupServer(2, isFileStorage(), isNetty(), 5);
   }

   protected void stopServers() throws Exception
   {
      closeAllConsumers();

      closeAllSessionFactories();

      stopServers(0, 1, 2, 3, 4, 5);
   }

   protected void failNode(int node) throws Exception
   {
      log.info("*** failing node " + node);

      Map<String, Object> params = generateParams(node, isNetty());

      TransportConfiguration serverTC;

      if (isNetty())
      {
         serverTC = new TransportConfiguration(NETTY_CONNECTOR_FACTORY, params);
      }
      else
      {
         serverTC = new TransportConfiguration(INVM_CONNECTOR_FACTORY, params);
      }
      
      MessagingServer server = getServer(node);
      
      //Prevent remoting service taking any more connections
      server.getRemotingService().freeze();
      
      server.getClusterManager().stop();

      //Fail all client connections that go to this node
      super.failNode(serverTC);
   }

}
