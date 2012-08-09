package org.hornetq.core.server.impl;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClusterTopologyListener;

final class BackupTopologyListener implements ClusterTopologyListener
{

   private final CountDownLatch latch = new CountDownLatch(1);
   private final String ownId;
   private static final int WAIT_TIMEOUT = 60;

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
         return latch.await(WAIT_TIMEOUT, TimeUnit.SECONDS);
      }
      catch (InterruptedException e)
      {
         return false;
      }
   }
}
