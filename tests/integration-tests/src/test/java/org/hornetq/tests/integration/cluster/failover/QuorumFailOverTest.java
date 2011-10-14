package org.hornetq.tests.integration.cluster.failover;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClusterTopologyListener;

public class QuorumFailOverTest extends StaticClusterWithBackupFailoverTest
{

   public void testQuorumVoting() throws Exception
   {
      setupCluster();
      startServers(0, 1, 2, 3, 4, 5);

      final TopologyListener liveTopologyListener = new TopologyListener("LIVE-1");
      fail("must rewrite to use new interfaces");
      // servers[0].getClusterManager().addClusterTopologyListener(liveTopologyListener, true);

      final TopologyListener backupTopologyListener = new TopologyListener("BACKUP-3");
      // servers[3].getClusterManager().addClusterTopologyListener(backupTopologyListener, true);

      assertTrue("we assume 3 is a backup", servers[3].getConfiguration().isBackup());
      assertFalse("no shared storage", servers[3].getConfiguration().isSharedStore());

      setupSessionFactory(0, 3, isNetty(), false);
      setupSessionFactory(1, 4, isNetty(), false);
      setupSessionFactory(2, 5, isNetty(), false);

      assertEquals(liveTopologyListener.toString(), 6, liveTopologyListener.nodes.size());
      assertEquals(backupTopologyListener.toString(), 6, backupTopologyListener.nodes.size());

      failNode(0);
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
      public
 void nodeUP(long eventUID, String nodeID,
               Pair<TransportConfiguration, TransportConfiguration> connectorPair, boolean last)
      {
         nodes.put(nodeID, connectorPair);
         System.out.println(prefix + " UP: " + nodeID);
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
