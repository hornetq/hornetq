package org.hornetq.core.server.impl;

import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQExceptionType;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.core.client.SessionFailureListener;
import org.hornetq.api.core.client.TopologyMember;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ServerLocatorImpl;
import org.hornetq.core.client.impl.Topology;
import org.hornetq.core.client.impl.TopologyMemberImpl;
import org.hornetq.core.protocol.core.CoreRemotingConnection;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationLiveIsStoppingMessage.LiveStopping;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.NetworkHealthCheck;
import org.hornetq.core.server.NodeManager;

/**
 * Manages a quorum of servers used to determine whether a given server is running or not.
 * <p/>
 * The use case scenario is an eventual connection loss between the live and the backup, where the
 * quorum will help a remote backup deciding whether to replace its 'live' server or to keep trying
 * to reconnect.
 * <p/>
 * If the live server does an orderly shutdown, it will explicitly notify the backup through the
 * replication channel. In this scenario, no voting takes place and the backup fails-over. See
 * {@link #failOver(boolean)}.
 * <p/>
 * In case of
 * <ol>
 * <li>broken connection, which would trigger a
 * {@link FailureListener#connectionFailed(HornetQException, boolean)}. This is affected by the TTL
 * configuration.</li>
 * <li>getting a "nodeDown" topology notification. This would happen if a given server failed to
 * PING our live</li>
 * </ol>
 * The backup will try to connect to all the topology members, if it manages to reach at least half
 * of the servers, it concludes the network is operating normally and fail-over. If the backup finds
 * less than half of its peers, then it assumes a network failure and attempts to reconnect with the
 * live.
 */
public final class QuorumManager implements SessionFailureListener, ClusterTopologyListener
{
   private String targetServerID = "";
   private final ExecutorService executor;
   private final String serverIdentity;
   private CountDownLatch latch;
   private volatile BACKUP_ACTIVATION signal;
   private ClientSessionFactoryInternal sessionFactory;
   private final Topology topology;
   private CoreRemotingConnection connection;

   /**
    * safety parameter to make _sure_ we get out of await()
    */
   private static final int LATCH_TIMEOUT = 30;
   private static final int RECONNECT_ATTEMPTS = 5;

   private final Object decisionGuard = new Object();
   private final NodeManager nodeManager;
   private final ServerLocator serverLocator;
   private final ScheduledExecutorService scheduledPool;
   private final NetworkHealthCheck networkHealthCheck;

   /**
    * This is a safety net in case the live sends the first {@link ReplicationLiveIsStoppingMessage}
    * with code {@link LiveStopping#STOP_CALLED} and crashes before sending the second with
    * {@link LiveStopping#FAIL_OVER}.
    * <p/>
    * If the second message does come within this dead line, we fail over anyway.
    */
   public static final int WAIT_TIME_AFTER_FIRST_LIVE_STOPPING_MSG = 60;

   public QuorumManager(ServerLocator serverLocator, ExecutorService executor, ScheduledExecutorService scheduledPool,
                        String identity, NodeManager nodeManager, NetworkHealthCheck networkHealthCheck)
   {
      this.serverIdentity = identity;
      this.executor = executor;
      this.scheduledPool = scheduledPool;
      this.latch = new CountDownLatch(1);
      this.nodeManager = nodeManager;
      this.serverLocator = serverLocator;
      topology = serverLocator.getTopology();
      this.networkHealthCheck = networkHealthCheck;
   }

   @Override
   public void nodeUP(TopologyMember topologyMember, boolean last)
   {
      //noop
   }

   @Override
   public void nodeDown(long eventUID, String nodeID)
   {
      if (targetServerID.equals(nodeID))
      {
         decideOnAction();
      }
   }

   public void setLiveID(String liveID)
   {
      targetServerID = liveID;
      nodeManager.setNodeID(liveID);
      //now we are replicating we can start waiting for disconnect notifications so we can fail over
      sessionFactory.addFailureListener(this);
   }

   private boolean isLiveDown()
   {
      Collection<TopologyMemberImpl> nodes = topology.getMembers();
      Collection<ServerLocator> locatorsList = new LinkedList<ServerLocator>();
      AtomicInteger pingCount = new AtomicInteger(0);
      int total = 0;
      for (TopologyMemberImpl tm : nodes)
         if (useIt(tm))
            total++;

      if (total < 1)
         return true;

      final CountDownLatch voteLatch = new CountDownLatch(total);
      try
      {
         for (TopologyMemberImpl tm : nodes)
         {
            Pair<TransportConfiguration, TransportConfiguration> pair = tm.getConnector();

            TransportConfiguration serverTC = pair.getA();
            if (useIt(tm))
            {
               ServerLocatorImpl locator = (ServerLocatorImpl)HornetQClient.createServerLocatorWithoutHA(serverTC);
               locatorsList.add(locator);
               executor.submit(new QuorumVoteServerConnect(voteLatch, total, pingCount, locator, serverTC));
            }
         }

         try
         {
            voteLatch.await(LATCH_TIMEOUT, TimeUnit.SECONDS);
         }
         catch (InterruptedException interruption)
         {
            // No-op. The best the quorum can do now is to return the latest number it has
         }
         // -1: because the live server is not being filtered out.
         boolean vote = nodeIsDown(total, pingCount.get());
         HornetQServerLogger.LOGGER.trace("quorum vote is liveIsDown=" + vote + ", count=" + pingCount);
         return vote;
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

   /**
    * @param tm
    * @return
    */
   private boolean useIt(TopologyMemberImpl tm)
   {
      return tm.getLive() != null && !targetServerID.equals(tm.getLive().getName());
   }

   @Override
   public String toString()
   {
      return QuorumManager.class.getSimpleName() + "(server=" + serverIdentity + ")";
   }

   /**
    * Decides whether the server is to be considered down or not.
    *
    * @param totalServers
    * @param reachedServers
    * @return
    */
   private static boolean nodeIsDown(int totalServers, int reachedServers)
   { // at least half of the servers were reached
      return reachedServers * 2 >= totalServers - 1;
   }

   public void notifyRegistrationFailed()
   {
      signal = BACKUP_ACTIVATION.FAILURE_REPLICATING;
      latch.countDown();
   }

   public void notifyAlreadyReplicating()
   {
      signal = BACKUP_ACTIVATION.ALREADY_REPLICATING;
      latch.countDown();
   }

   /**
    * Attempts to connect to a given server.
    */
   private static class QuorumVoteServerConnect implements Runnable
   {
      private final ServerLocatorImpl locator;
      private final CountDownLatch latch;
      private final AtomicInteger count;
      private final TransportConfiguration tc;
      private final int total;

      public QuorumVoteServerConnect(CountDownLatch latch, int total, AtomicInteger count,
                                     ServerLocatorImpl serverLocator,
                                     TransportConfiguration serverTC)
      {
         this.total = total;
         this.locator = serverLocator;
         this.latch = latch;
         this.count = count;
         this.tc = serverTC;
      }


      @Override
      public void run()
      {
         locator.setReconnectAttempts(0);

         final ClientSessionFactory sessionFactory;
         ClientSession session;
         try
         {
            sessionFactory = locator.createSessionFactory(tc);
            if (sessionFactory != null)
            {
               session = sessionFactory.createSession();
               if (session != null)
               {
                  if (nodeIsDown(total, count.incrementAndGet()))
                  {
                     while (latch.getCount() > 0)
                     {
                        latch.countDown();
                     }
                  }
                  session.close();
                  sessionFactory.close();
               }
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
   public void beforeReconnect(HornetQException exception)
   {
      //noop
   }

   @Override
   public void connectionFailed(HornetQException exception, boolean failedOver)
   {
      decideOnAction();
   }

   private void decideOnAction()
   {
      //we may get called via multiple paths so need to guard
      synchronized (decisionGuard)
      {
         if (signal == BACKUP_ACTIVATION.FAIL_OVER)
         {
            if (networkHealthCheck != null && !networkHealthCheck.check())
            {
               signal = BACKUP_ACTIVATION.FAILURE_REPLICATING;
            }
            return;
         }
         if (!isLiveDown())
         {
            try
            {
               // no point in repeating all the reconnection logic
               sessionFactory.connect(RECONNECT_ATTEMPTS, false);
               return;
            }
            catch (HornetQException e)
            {
               if (e.getType() != HornetQExceptionType.NOT_CONNECTED)
                  HornetQServerLogger.LOGGER.errorReConnecting(e);
            }
         }

         if (networkHealthCheck != null && networkHealthCheck.check())
         {
            // live is assumed to be down, backup fails-over
            signal = BACKUP_ACTIVATION.FAIL_OVER;
         }
         else
         {
            HornetQServerLogger.LOGGER.serverIsolatedOnNetwork();
            signal = BACKUP_ACTIVATION.FAILURE_REPLICATING;
         }
      }
      latch.countDown();
   }

   enum BACKUP_ACTIVATION
   {
      FAIL_OVER, FAILURE_REPLICATING, ALREADY_REPLICATING, STOP;
   }

   /**
    * Called by the replicating backup (i.e. "SharedNothing" backup) to wait for the signal to
    * fail-over or to stop.
    *
    * @return signal, indicating whether to stop or to fail-over
    */
   public BACKUP_ACTIVATION waitForStatusChange()
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
    *
    * @param explicitSignal the state we want to set the quorum manager to return
    */
   public synchronized void causeExit(BACKUP_ACTIVATION explicitSignal)
   {
      removeListener();
      this.signal = explicitSignal;
      latch.countDown();
   }

   private void removeListener()
   {
      serverLocator.removeClusterTopologyListener(this);
      if (connection != null)
         connection.removeFailureListener(this);
   }

   /**
    * Releases the latch, causing the backup activation thread to fail-over.
    * <p/>
    * The use case is for when the 'live' has an orderly shutdown, in which case it informs the
    * backup that it should fail-over.
    */
   public synchronized void failOver(LiveStopping finalMessage)
   {
      removeListener();
      signal = BACKUP_ACTIVATION.FAIL_OVER;
      if (finalMessage == LiveStopping.FAIL_OVER)
      {
         latch.countDown();
      }
      if (finalMessage == LiveStopping.STOP_CALLED)
      {
         final CountDownLatch localLatch = latch;
         scheduledPool.schedule(new Runnable()
         {
            @Override
            public void run()
            {
               localLatch.countDown();
            }

         }, WAIT_TIME_AFTER_FIRST_LIVE_STOPPING_MSG, TimeUnit.SECONDS);
      }
   }

   /**
    * @param sessionFactory the session factory used to connect to the live server
    */
   public void setSessionFactory(ClientSessionFactoryInternal sessionFactory)
   {
      this.sessionFactory = sessionFactory;
   }

   /**
    * @param liveConnection
    */
   public void addAsFailureListenerOf(CoreRemotingConnection liveConnection)
   {
      this.connection = liveConnection;
      connection.addFailureListener(this);
      //connection.addCloseListener(this);
   }

   public synchronized void reset()
   {
      latch = new CountDownLatch(1);
   }
}
