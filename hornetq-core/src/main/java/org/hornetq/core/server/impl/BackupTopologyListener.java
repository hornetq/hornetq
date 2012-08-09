package org.hornetq.core.server.impl;

import java.util.concurrent.CountDownLatch;

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClusterTopologyListener;

final class BackupTopologyListener implements ClusterTopologyListener
{

   private final CountDownLatch latch = new CountDownLatch(1);
   private final String ownId;

   public BackupTopologyListener(String ownId)
   {
      this.ownId = ownId;
   }

   @Override
   public void nodeUP(long eventUID, String nodeName, String nodeID, Pair<TransportConfiguration, TransportConfiguration> connectorPair,
                      boolean last)
   {
      if (ownId.equals(nodeID) && connectorPair.getB() != null)
         latch.countDown();
   }

   @Override
   public void nodeDown(long eventUID, String nodeID)
   {
      // no-op
   }

   boolean waitForBackup()
   {
      try
      {
         latch.await();
      }
      catch (InterruptedException e)
      {
         return false;
      }
      return true;
   }
}
