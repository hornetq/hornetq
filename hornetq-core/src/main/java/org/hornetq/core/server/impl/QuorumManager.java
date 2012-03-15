package org.hornetq.core.server.impl;

import java.util.Collection;
import java.util.LinkedList;
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
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ServerLocatorImpl;
import org.hornetq.core.client.impl.Topology;
import org.hornetq.core.client.impl.TopologyMember;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.core.server.HornetQServer;

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
   private final ExecutorService executor;
   private final String serverIdentity;
   private final CountDownLatch latch;
   private volatile BACKUP_ACTIVATION signal;
   private ClientSessionFactoryInternal sessionFactory;
   private final Topology topology;
   private final HornetQServer backupServer;

   /** safety parameter to make _sure_ we get out of await() */
   private static final int LATCH_TIMEOUT = 30;
   private static final long DISCOVERY_TIMEOUT = 5;
   private static final int RECONNECT_ATTEMPTS = 5;

   public QuorumManager(HornetQServer backup, ServerLocator serverLocator, ExecutorService executor, String identity)
   {
      this.serverIdentity = identity;
      this.executor = executor;
      this.latch = new CountDownLatch(1);
      this.backupServer = backup;
      topology = serverLocator.getTopology();
   }

   public void setLiveID(String liveID)
   {
      targetServerID = liveID;
   }

   public boolean isLiveDown()
   {
      Collection<TopologyMember> nodes = topology.getMembers();

      if (nodes.size() < 2) // the life server is also in the list
      {
         return true;
      }

      final int size = nodes.size();
      Collection<ServerLocator> locatorsList = new LinkedList<ServerLocator>();
      AtomicInteger pingCount = new AtomicInteger(0);
      final CountDownLatch latch = new CountDownLatch(size);
      int total = 0;
      try
      {
         for (TopologyMember tm : nodes)
         {
            Pair<TransportConfiguration, TransportConfiguration> pair = tm.getConnector();
//            if (targetServerID.equals(pair.getKey()))
//               continue;
            TransportConfiguration serverTC = pair.getA();
            if (serverTC == null)
            {
               latch.countDown();
               continue;
            }
            total++;
            ServerLocatorImpl locator = (ServerLocatorImpl)HornetQClient.createServerLocatorWithoutHA(serverTC);
            locatorsList.add(locator);
            executor.submit(new ServerConnect(latch, pingCount, locator));
         }

         try
         {
            latch.await(LATCH_TIMEOUT, TimeUnit.SECONDS);
         }
         catch (InterruptedException interruption)
         {
            // No-op. The best the quorum can do now is to return the latest number it has
         }
         // -1: because the live server is not being filtered out.
         return pingCount.get() * 2 >= locatorsList.size() - 1;
      }
      finally
      {
         for (ServerLocator locator : locatorsList)
         {
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
      // Check if connection was reestablished by the sessionFactory:
      if (sessionFactory.numConnections() > 0)
      {
         resetReplication();
         return;
      }
      if (!isLiveDown())
      {
         try
         {
            // no point in repeating all the reconnection logic
            sessionFactory.connect(RECONNECT_ATTEMPTS, false);
            resetReplication();
            return;
         }
         catch (HornetQException e)
         {
            if (e.getCode() != HornetQException.NOT_CONNECTED)
               log.warn("Unexpected exception while trying to reconnect", e);
         }
      }

      // live is assumed to be down, backup fails-over
      signal = BACKUP_ACTIVATION.FAIL_OVER;
      latch.countDown();
   }

   /**
    *
    */
   private void resetReplication()
   {
      new Thread(new ServerRestart(backupServer)).run();
   }

   private static final class ServerRestart implements Runnable
   {
      final HornetQServer backup;

      public ServerRestart(HornetQServer backup)
      {
         this.backup = backup;
      }

      @Override
      public void run()
      {
         try
         {
            backup.stop();
            backup.start();
         }
         catch (Exception e)
         {
            log.error("Error while restarting the backup server: " + backup, e);
         }
      }

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
    * Releases the latch, causing the backup activation thread to fail-over.
    */
   public synchronized void failOver()
   {
      signal = BACKUP_ACTIVATION.FAIL_OVER;
      latch.countDown();
   }

   /**
    * @param sessionFactory the session factory used to connect to the live server
    */
   public void setSessionFactory(ClientSessionFactoryInternal sessionFactory)
   {
      this.sessionFactory = sessionFactory;
   }
}
