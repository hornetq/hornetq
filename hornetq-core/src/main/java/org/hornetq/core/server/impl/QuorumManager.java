package org.hornetq.core.server.impl;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.client.impl.ServerLocatorImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.FailureListener;

/**
 * Manages a quorum of servers used to determine whether a given server is running or not.
 * <p>
 * The use case scenario is an eventual connection loss between the live and the backup, where the
 * quorum will help a remote backup deciding whether to replace its 'live' server or to keep trying
 * to reconnect.
 */
public final class QuorumManager implements FailureListener
{
   private static final Logger log = Logger.getLogger(QuorumManager.class);
   private String targetServerID = "";
   private final Map<String, Pair<TransportConfiguration, TransportConfiguration>> nodes =
            new ConcurrentHashMap<String, Pair<TransportConfiguration, TransportConfiguration>>();

   private final ExecutorService executor;
   private final String serverIdentity;
   private final CountDownLatch latch;
   private volatile BACKUP_ACTIVATION signal;

   /** safety parameter to make _sure_ we get out of await() */
   private static final int LATCH_TIMEOUT = 60;
   private static final long DISCOVERY_TIMEOUT = 5;

   public QuorumManager(ServerLocator serverLocator, ExecutorService executor, String identity)
   {
      this.serverIdentity = identity;
      this.executor = executor;
      this.latch = new CountDownLatch(1);
      // locator.addClusterTopologyListener(this);
   }

   public void setLiveID(String liveID)
   {
      targetServerID = liveID;
   }

   public boolean isNodeDown()
   {
      if (nodes.size() == 0)
      {
         return true;
      }

      final int size = nodes.size();
      Set<ServerLocator> locatorsList = new HashSet<ServerLocator>(size);
      AtomicInteger pingCount = new AtomicInteger(0);
      final CountDownLatch latch = new CountDownLatch(size);
      try
      {
         for (Entry<String, Pair<TransportConfiguration, TransportConfiguration>> pair : nodes.entrySet())
         {
            if (targetServerID.equals(pair.getKey()))
               continue;
            TransportConfiguration serverTC = pair.getValue().getA();
            ServerLocatorImpl locator = (ServerLocatorImpl)HornetQClient.createServerLocatorWithoutHA(serverTC);
            locatorsList.add(locator);
            executor.submit(new ServerConnect(latch, pingCount, locator));
         }
         // Some servers may have disappeared between the latch creation
         for (int i = 0; i < size - locatorsList.size(); i++)
         {
            latch.countDown();
         }
         try
         {
            latch.await(LATCH_TIMEOUT, TimeUnit.SECONDS);
         }
         catch (InterruptedException interruption)
         {
            // No-op. As the best the quorum can do now is to return the latest number it has
         }
         return pingCount.get() * 2 >= locatorsList.size();
      }
      finally
      {
         for (ServerLocator locator: locatorsList){
            try
            {
               locator.close();
            }
            catch (Exception e)
            {
               // no-op
            }
         }
      }
   }

   @Override
   public String toString()
   {
      return QuorumManager.class.getSimpleName() + "(server=" + serverIdentity + ")";
   }

   /**
    * Attempts to connect to a given server.
    */
   private static class ServerConnect implements Runnable
   {
      private final ServerLocatorImpl locator;
      private final CountDownLatch latch;
      private final AtomicInteger count;

      public ServerConnect(CountDownLatch latch, AtomicInteger count, ServerLocatorImpl serverLocator)
      {
         locator = serverLocator;
         this.latch = latch;
         this.count = count;
      }

      @Override
      public void run()
      {
         locator.setReconnectAttempts(0);
         locator.getDiscoveryGroupConfiguration().setDiscoveryInitialWaitTimeout(DISCOVERY_TIMEOUT);

         final ClientSessionFactory liveServerSessionFactory;
         try
         {
            liveServerSessionFactory = locator.connect();
            if (liveServerSessionFactory != null)
            {
               count.incrementAndGet();
            }
         }
         catch (Exception e)
         {
            // no-op
         }
         finally
         {
            latch.countDown();
            locator.close();
         }
      }
   }

   @Override
   public void connectionFailed(HornetQException exception, boolean failedOver)
   {
      // connection failed,
      if (exception.getCode() == HornetQException.DISCONNECTED)
      {
         log.info("Connection failed: " + exception);
         signal = BACKUP_ACTIVATION.FAIL_OVER;
         latch.countDown();
         return;
      }
      throw new RuntimeException(exception);

      // by the time it got here, the connection might have been re-established
      // check for it...
      // if connectionIsOk:
      // replicationEndPoint must see how up-to-date it is
      // If not:
      // 1. take a vote
      // if vote result is weird... retry connecting
   }

   enum BACKUP_ACTIVATION
   {
      FAIL_OVER, STOP;
   }

   /**
    * Called by the replicating backup (i.e. "SharedNothing" backup) to wait for the signal to
    * fail-over or to stop.
    * @return signal, indicating whether to stop or to fail-over
    */
   public final BACKUP_ACTIVATION waitForStatusChange()
   {
      try
      {
         latch.await();
      }
      catch (InterruptedException e)
      {
         return BACKUP_ACTIVATION.STOP;
      }
      return signal;
   }

   /**
    * Cause the Activation thread to exit and the server to be stopped.
    */
   public synchronized void causeExit()
   {
      signal = BACKUP_ACTIVATION.STOP;
      latch.countDown();
   }

   /**
    * Releases the semaphore, causing the Activation thread to fail-over.
    */
   public synchronized void failOver()
   {
      signal = BACKUP_ACTIVATION.FAIL_OVER;
      latch.countDown();
   }
}
