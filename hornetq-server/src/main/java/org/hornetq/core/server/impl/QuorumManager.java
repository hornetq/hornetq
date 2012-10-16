package org.hornetq.core.server.impl;

import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQExceptionType;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.core.client.TopologyMember;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ServerLocatorImpl;
import org.hornetq.core.client.impl.Topology;
import org.hornetq.core.client.impl.TopologyMemberImpl;
import org.hornetq.core.protocol.core.CoreRemotingConnection;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.utils.Pair;

/**
 * Manages a quorum of servers used to determine whether a given server is running or not.
 * <p>
 * The use case scenario is an eventual connection loss between the live and the backup, where the
 * quorum will help a remote backup deciding whether to replace its 'live' server or to keep trying
 * to reconnect.
 */
public final class QuorumManager implements FailureListener, ClusterTopologyListener
{
   private String targetServerID = "";
   private final ExecutorService executor;
   private final String serverIdentity;
   private CountDownLatch latch;
   private volatile BACKUP_ACTIVATION signal;
   private ClientSessionFactoryInternal sessionFactory;
   private final Topology topology;
   private CoreRemotingConnection connection;

   /** safety parameter to make _sure_ we get out of await() */
   private static final int LATCH_TIMEOUT = 30;
   private static final int RECONNECT_ATTEMPTS = 5;

   private final Object decisionGuard = new Object();

   public QuorumManager(ServerLocator serverLocator, ExecutorService executor, String identity)
   {
      this.serverIdentity = identity;
      this.executor = executor;
      this.latch = new CountDownLatch(1);
      topology = serverLocator.getTopology();
   }

   @Override
   public void nodeUP(TopologyMember topologyMember, boolean last)
   {
      //noop
   }

   @Override
   public void nodeDown(long eventUID, String nodeID)
   {
      if(targetServerID.equals(nodeID))
      {
         decideOnAction();
      }
   }

   public void setLiveID(String liveID)
   {
      targetServerID = liveID;
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

      final CountDownLatch latch = new CountDownLatch(total);
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
               executor.submit(new ServerConnect(latch, total, pingCount, locator, serverTC));
            }
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
    * Returns {@link true} when a sufficient number of votes has been cast to take a decision.
    * @param total the total number of votes
    * @param pingCount the total number of servers that were reached
    * @param votesLeft the total number of servers for which we don't have a decision yet.
    * @return
    */
   private static final boolean isSufficient(int total, int pingCount, long votesLeft)
   {
      boolean notEnoughVotesLeft = total - 2 * (pingCount + votesLeft) > 0;
      return nodeIsDown(total, pingCount) || notEnoughVotesLeft;
   }

   /**
    * @param total
    * @param pingCount
    * @return
    */
   private static boolean nodeIsDown(int total, int pingCount)
   {
      return pingCount * 2 >= total - 1;
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
   private static class ServerConnect implements Runnable
   {
      private final ServerLocatorImpl locator;
      private final CountDownLatch latch;
      private final AtomicInteger count;
      private final TransportConfiguration tc;
      private final int total;

      public ServerConnect(CountDownLatch latch, int total, AtomicInteger count,
                           ServerLocatorImpl serverLocator,
                           TransportConfiguration serverTC)
      {
         this.total = total;
         this.locator = serverLocator;
         this.latch=latch;
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
                  if (isSufficient(total, count.incrementAndGet(), latch.getCount() - 1))
                  {
                     while (latch.getCount() > 0)
                        latch.countDown();
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
   public void connectionFailed(HornetQException exception, boolean failedOver)
   {
      decideOnAction();
   }

   private void decideOnAction()
   {
      //we may get called via multiple paths so need to guard
      synchronized (decisionGuard)
      {
         if(signal == BACKUP_ACTIVATION.FAIL_OVER)
         {
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
         // live is assumed to be down, backup fails-over
         signal = BACKUP_ACTIVATION.FAIL_OVER;
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
    * @param signal the state we want to set the quorum manager to
    */
   public synchronized void causeExit(BACKUP_ACTIVATION signal)
   {
      removeListener();
      this.signal = signal;
      latch.countDown();
   }

   private void removeListener()
   {
      if (connection == null)
         return;
      connection.removeFailureListener(this);
   }

   /**
    * Releases the latch, causing the backup activation thread to fail-over.
    */
   public synchronized void failOver()
   {
      removeListener();
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
