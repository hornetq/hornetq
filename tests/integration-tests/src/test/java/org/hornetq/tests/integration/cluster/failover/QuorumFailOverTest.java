package org.hornetq.tests.integration.cluster.failover;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClusterTopologyListener;

public class QuorumFailOverTest extends StaticClusterWithBackupFailoverTest
{

   public void testQuorumVoting() throws Exception
   {

      setupCluster();

      startServers(0, 1, 2, 3, 4, 5);
      for (int i : new int[] { 0, 1, 2 })
      {
         setupSessionFactory(i, i + 3, isNetty(), false);
      }

      createQueue(0, QUEUES_TESTADDRESS, QUEUE_NAME, null, true);

      final TopologyListener liveTopologyListener = new TopologyListener("LIVE-1");

      locators[0].addClusterTopologyListener(liveTopologyListener);

      final TopologyListener backupTopologyListener = new TopologyListener("LIVE-2");
      locators[1].addClusterTopologyListener(backupTopologyListener);

      assertTrue("we assume 3 is a backup", servers[3].getConfiguration().isBackup());
      assertFalse("no shared storage", servers[3].getConfiguration().isSharedStore());

      // assertEquals(liveTopologyListener.toString(), 6, liveTopologyListener.nodes.size());
      // assertEquals(backupTopologyListener.toString(), 6, backupTopologyListener.nodes.size());

      failNode(0);
      waitForBindings(3, QUEUES_TESTADDRESS, 1, 1, true);

      assertTrue(servers[3].waitForInitialization(10, TimeUnit.SECONDS));
      assertFalse("3 should have failed over ", servers[3].getConfiguration().isBackup());
      servers[1].stop();
      assertFalse("4 should have failed over ", servers[4].getConfiguration().isBackup());
   }

   @Override
   protected boolean isSharedStorage()
   {
      return false;
   }

   private static class TopologyListener implements ClusterTopologyListener
   {
      final String prefix;
      final ConcurrentMap<String, Pair<TransportConfiguration, TransportConfiguration>> nodes =
               new ConcurrentHashMap<String, Pair<TransportConfiguration, TransportConfiguration>>();
      public TopologyListener(String string)
      {
         prefix = string;
      }

      @Override
      public void nodeUP(long eventUID, String nodeID,
                         Pair<TransportConfiguration, TransportConfiguration> connectorPair, boolean last)
      {
         nodes.put(nodeID, connectorPair);
         System.out.println(prefix + " UP: " + nodeID + " connectPair=" + connectorPair);
      }

      @Override
      public void nodeDown(long eventUID, String nodeID)
      {
         nodes.remove(nodeID);
         System.out.println(prefix + " DOWN: " + nodeID);
      }

      @Override
      public String toString()
      {
         return "TopologyListener(" + prefix + ", #=" + nodes.size() + ")";
      }
   }
}
