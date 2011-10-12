/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.tests.integration.cluster.topology;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.client.impl.ServerLocatorImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.cluster.ClusterConnection;
import org.hornetq.core.server.cluster.ClusterManager;
import org.hornetq.tests.integration.cluster.distribution.ClusterTestBase;
import org.hornetq.tests.util.RandomUtil;

/**
 * A TopologyClusterTestBase
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public abstract class TopologyClusterTestBase extends ClusterTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   private static final Logger log = Logger.getLogger(TopologyClusterTestBase.class);

   private static final long WAIT_TIMEOUT = 5000;

   abstract protected ServerLocator createHAServerLocator();

   abstract protected void setupServers() throws Exception;

   abstract protected void setupCluster() throws Exception;

   abstract protected boolean isNetty() throws Exception;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      setupServers();

      setupCluster();
   }

   @Override
   protected void tearDown() throws Exception
   {
      stopServers(0, 1, 2, 3, 4);

      super.tearDown();
   }

   /**
    * Check that the actual list of received nodeIDs correspond to the expected order of nodes
    */
   protected void checkOrder(int[] expected, String[] nodeIDs, List<String> actual)
   {
      assertEquals(expected.length, actual.size());
      for (int i = 0; i < expected.length; i++)
      {
         assertEquals("did not receive expected nodeID at " + i, nodeIDs[expected[i]], actual.get(i));
      }
   }

   protected void checkContains(int[] expected, String[] nodeIDs, List<String> actual)
   {
      long start = System.currentTimeMillis();
      do
      {
         if (expected.length != actual.size())
         {
            continue;
         }
         boolean ok = true;
         for (int i = 0; i < expected.length; i++)
         {
            ok = (ok && actual.contains(nodeIDs[expected[i]]));
         }
         if (ok) 
         {
            return;
         }
      } while(System.currentTimeMillis() - start < 5000);
      fail("did not contain all expected node ID: " + actual);
   }

   protected String[] getNodeIDs(int... nodes)
   {
      String[] nodeIDs = new String[nodes.length];
      for (int i = 0; i < nodes.length; i++)
      {
         nodeIDs[i] = servers[i].getNodeID().toString();
      }
      return nodeIDs;
   }

   protected ClientSession checkSessionOrReconnect(ClientSession session, ServerLocator locator) throws Exception
   {
      try
      {
         String rand = RandomUtil.randomString();
         session.createQueue(rand, rand);
         session.deleteQueue(rand);
         return session;
      }
      catch (HornetQException e)
      {
         if (e.getCode() == HornetQException.OBJECT_CLOSED || e.getCode() == HornetQException.UNBLOCKED)
         {
         ClientSessionFactory sf = locator.createSessionFactory();
         return sf.createSession();
         }
         else
         {
            throw e;
         }
      }
   }

   protected void waitForClusterConnections(final int node, final int count) throws Exception
   {
      HornetQServer server = servers[node];

      if (server == null)
      {
         throw new IllegalArgumentException("No server at " + node);
      }

      ClusterManager clusterManager = server.getClusterManager();

      long start = System.currentTimeMillis();

      do
      {
         int nodesCount = 0;

         for (ClusterConnection clusterConn : clusterManager.getClusterConnections())
         {
            nodesCount += clusterConn.getNodes().size();
         }

         if (nodesCount == count)
         {
            return;
         }

         Thread.sleep(10);
      }
      while (System.currentTimeMillis() - start < WAIT_TIMEOUT);
 
      log.error(clusterDescription(servers[node]));
      throw new IllegalStateException("Timed out waiting for cluster connections ");
   }
   
   public void testReceiveNotificationsWhenOtherNodesAreStartedAndStopped() throws Throwable
   {
      startServers(0);

      ServerLocator locator = createHAServerLocator();
      
      ((ServerLocatorImpl)locator).getTopology().setOwner("testReceive");

      final List<String> nodes = new ArrayList<String>();
      final CountDownLatch upLatch = new CountDownLatch(5);
      final CountDownLatch downLatch = new CountDownLatch(4);

      locator.addClusterTopologyListener(new ClusterTopologyListener()
      {
         public void nodeUP(final long uniqueEventID, 
                            String nodeID,
                            Pair<TransportConfiguration, TransportConfiguration> connectorPair,
                            boolean last)
         {
            if(!nodes.contains(nodeID))
            {
               System.out.println("Node UP " + nodeID + " added");
               log.info("Node UP " + nodeID + " added");
               nodes.add(nodeID);
               upLatch.countDown();
            }
            else
            {
               System.out.println("Node UP " + nodeID + " was already here");
               log.info("Node UP " + nodeID + " was already here");
            }
         }

         public void nodeDown(final long uniqueEventID, String nodeID)
         {
            if (nodes.contains(nodeID))
            {
               log.info("Node down " + nodeID + " accepted");
               System.out.println("Node down " + nodeID + " accepted");
               nodes.remove(nodeID);
               downLatch.countDown();
            }
            else
            {
               log.info("Node down " + nodeID + " already removed");
               System.out.println("Node down " + nodeID + " already removed");
            }
         }
      });

      ClientSessionFactory sf = locator.createSessionFactory();

      startServers(1, 4, 3, 2);
      String[] nodeIDs = getNodeIDs(0, 1, 2, 3, 4);

      assertTrue("Was not notified that all servers are UP", upLatch.await(10, SECONDS));
      checkContains(new int[] { 0, 1, 4, 3, 2 }, nodeIDs, nodes);

      waitForClusterConnections(0, 4);
      waitForClusterConnections(1, 4);
      waitForClusterConnections(2, 4);
      waitForClusterConnections(3, 4);
      waitForClusterConnections(4, 4);

      stopServers(2, 3, 1, 4);

      assertTrue("Was not notified that all servers are DOWN", downLatch.await(10, SECONDS));
      checkContains(new int[] { 0 }, nodeIDs, nodes);

      sf.close();
      
      locator.close();
      
      stopServers(0);
   }

   public void testReceiveNotifications() throws Throwable
   {
      startServers(0, 1, 2, 3, 4);
      String[] nodeIDs = getNodeIDs(0, 1, 2, 3, 4);

      ServerLocator locator = createHAServerLocator();

      final List<String> nodes = new ArrayList<String>();
      final CountDownLatch upLatch = new CountDownLatch(5);
      final CountDownLatch downLatch = new CountDownLatch(4);

      locator.addClusterTopologyListener(new ClusterTopologyListener()
      {
         public void nodeUP(final long uniqueEventID, 
                            String nodeID,
                            Pair<TransportConfiguration, TransportConfiguration> connectorPair,
                            boolean last)
         {
            if (!nodes.contains(nodeID))
            {
               nodes.add(nodeID);
               upLatch.countDown();
            }
         }

         public void nodeDown(final long uniqueEventID, String nodeID)
         {
            if (nodes.contains(nodeID))
            {
               nodes.remove(nodeID);
               downLatch.countDown();
            }
         }
      });

      ClientSessionFactory sf = locator.createSessionFactory();

      assertTrue("Was not notified that all servers are UP", upLatch.await(10, SECONDS));
      checkContains(new int[] { 0, 1, 2, 3, 4 }, nodeIDs, nodes);

      ClientSession session = sf.createSession();
      
      waitForClusterConnections(0, 4);
      waitForClusterConnections(1, 4);
      waitForClusterConnections(2, 4);
      waitForClusterConnections(3, 4);
      waitForClusterConnections(4, 4);

      stopServers(0);
      session = checkSessionOrReconnect(session, locator);
      checkContains(new int[] { 1, 2, 3, 4 }, nodeIDs, nodes);

      stopServers(2);
      session = checkSessionOrReconnect(session, locator);
      checkContains(new int[] { 1, 3, 4 }, nodeIDs, nodes);

      stopServers(4);
      session = checkSessionOrReconnect(session, locator);
      checkContains(new int[] { 1, 3 }, nodeIDs, nodes);

      stopServers(3);
      session = checkSessionOrReconnect(session, locator);
      checkContains(new int[] { 1 }, nodeIDs, nodes);

      stopServers(1);
      
      assertTrue("Was not notified that all servers are DOWN", downLatch.await(10, SECONDS));
      checkContains(new int[] {}, nodeIDs, nodes);

      sf.close();
      
      locator.close();
   }

   public void testStopNodes() throws Throwable
   {
      startServers(0, 1, 2, 3, 4);
      String[] nodeIDs = getNodeIDs(0, 1, 2, 3, 4);

      ServerLocator locator = createHAServerLocator();

      final List<String> nodes = new ArrayList<String>();
      final CountDownLatch upLatch = new CountDownLatch(5);

      locator.addClusterTopologyListener(new ClusterTopologyListener()
      {
         public void nodeUP(final long uniqueEventID, String nodeID,
                            Pair<TransportConfiguration, TransportConfiguration> connectorPair,
                            boolean last)
         {
            if (!nodes.contains(nodeID))
            {
               nodes.add(nodeID);
               upLatch.countDown();
            }
         }

         public void nodeDown(final long uniqueEventID, String nodeID)
         {
            if (nodes.contains(nodeID))
            {
               nodes.remove(nodeID);
            }
         }
      });

      ClientSessionFactory sf = locator.createSessionFactory();

      assertTrue("Was not notified that all servers are UP", upLatch.await(10, SECONDS));
      checkContains(new int[] { 0, 1, 2, 3, 4 }, nodeIDs, nodes);

      waitForClusterConnections(0, 4);
      waitForClusterConnections(1, 4);
      waitForClusterConnections(2, 4);
      waitForClusterConnections(3, 4);
      waitForClusterConnections(4, 4);

      ClientSession session = sf.createSession();
      
      stopServers(0);
      assertFalse(servers[0].isStarted());
      session = checkSessionOrReconnect(session, locator);
      checkContains(new int[] { 1, 2, 3, 4 }, nodeIDs, nodes);

      stopServers(2);
      assertFalse(servers[2].isStarted());
      session = checkSessionOrReconnect(session, locator);
      checkContains(new int[] { 1, 3, 4 }, nodeIDs, nodes);

      stopServers(4);
      assertFalse(servers[4].isStarted());
      session = checkSessionOrReconnect(session, locator);
      checkContains(new int[] { 1, 3 }, nodeIDs, nodes);

      stopServers(3);
      assertFalse(servers[3].isStarted());

      session = checkSessionOrReconnect(session, locator);
      checkContains(new int[] { 1 }, nodeIDs, nodes);

      stopServers(1);
      assertFalse(servers[1].isStarted());
      try
      {
         session = checkSessionOrReconnect(session, locator);
         fail();
      }
      catch (Exception e)
      {

      }
      
      locator.close();
   }
   
   public void testMultipleClientSessionFactories() throws Throwable
   {
      startServers(0, 1, 2, 3, 4);
      String[] nodeIDs = getNodeIDs(0, 1, 2, 3, 4);

      ServerLocator locator = createHAServerLocator();

      final List<String> nodes = new ArrayList<String>();
      final CountDownLatch upLatch = new CountDownLatch(5);
      final CountDownLatch downLatch = new CountDownLatch(4);

      locator.addClusterTopologyListener(new ClusterTopologyListener()
      {
         public void nodeUP(final long uniqueEventID, String nodeID, 
                            Pair<TransportConfiguration, TransportConfiguration> connectorPair,
                            boolean last)
         {
            if (!nodes.contains(nodeID))
            {
               nodes.add(nodeID);
               upLatch.countDown();
            }
         }

         public void nodeDown(final long uniqueEventID, String nodeID)
         {
            if (nodes.contains(nodeID))
            {
               nodes.remove(nodeID);
               downLatch.countDown();
            }
         }
      });

      ClientSessionFactory[] sfs = new ClientSessionFactory[] {
                                                               locator.createSessionFactory(),
                                                               locator.createSessionFactory(),
                                                               locator.createSessionFactory(),
                                                               locator.createSessionFactory(),
                                                               locator.createSessionFactory() };
      assertTrue("Was not notified that all servers are UP", upLatch.await(10, SECONDS));
      checkContains(new int[] { 0, 1, 2, 3, 4 }, nodeIDs, nodes);

      waitForClusterConnections(0, 4);
      waitForClusterConnections(1, 4);
      waitForClusterConnections(2, 4);
      waitForClusterConnections(3, 4);
      waitForClusterConnections(4, 4);
      //we cant close all of the servers, we need to leave one up to notify us
      stopServers(4, 2, 3, 1);

      boolean ok = downLatch.await(10, SECONDS);
      if(!ok)
      {
         System.out.println("TopologyClusterTestBase.testMultipleClientSessionFactories");
      }
      assertTrue("Was not notified that all servers are Down", ok);
      checkContains(new int[] { 0 }, nodeIDs, nodes);
      
      for (int i = 0; i < sfs.length; i++)
      {
         ClientSessionFactory sf = sfs[i];
         sf.close();
      }
      
      locator.close();

      stopServers(0);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
