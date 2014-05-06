/*
 * Copyright 2005-2014 Red Hat, Inc.
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
package org.hornetq.core.server.cluster.qourum;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.core.client.TopologyMember;
import org.hornetq.core.client.impl.ServerLocatorImpl;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.client.impl.TopologyMemberImpl;
import org.hornetq.core.server.HornetQComponent;
import org.hornetq.core.server.HornetQServer;

/**
 * A QourumManager can be used to register a {@link org.hornetq.core.server.cluster.qourum.Quorum} to receive notifications
 * about changes to the cluster. A {@link org.hornetq.core.server.cluster.qourum.Quorum} can then issue a vote to the
 * remaining nodes in a cluster for a specific outcome
 */
public final class QuorumManager implements ClusterTopologyListener, HornetQComponent
{
   private final ExecutorService executor;

   /**
    * all the current registered {@link org.hornetq.core.server.cluster.qourum.Quorum}'s
    */
   private Map<String, Quorum> quorums = new HashMap<>();

   /**
    * any currently running runnables.
    */
   private final Map<QuorumVote, List<VoteRunnable>> voteRunnables = new HashMap<>();

   private HornetQServer server;

  /**
   * this holds the current state of the cluster
   */
   private ServerLocator serverLocator;

   private boolean started = false;

   /**
    * this is the max size that the cluster has been.
    */
   private int maxClusterSize = 0;

   public QuorumManager(HornetQServer server, ExecutorService threadPool)
   {
      this.server = server;
      this.executor = threadPool;
   }

   /**
    * we start by simply creating the server locator and connecting in a separate thread
    * @throws Exception
    */
   @Override
   public void start() throws Exception
   {
      if (started)
         return;
      started = true;
   }

   /**
    * stops the server locator
    * @throws Exception
    */
   @Override
   public void stop() throws Exception
   {
      if (!started)
         return;
      synchronized (voteRunnables)
      {
         started = false;
         for (List<VoteRunnable> runnables : voteRunnables.values())
         {
            for (VoteRunnable runnable : runnables)
            {
               runnable.close();
            }
         }
      }
      quorums.clear();
      for (Quorum quorum : quorums.values())
      {
         quorum.close();
      }
   }

   /**
    * are we started
    * @return
    */
   @Override
   public boolean isStarted()
   {
      return started;
   }

   /**
    * registers a {@link org.hornetq.core.server.cluster.qourum.Quorum} so that it can be notified of changes in the cluster.
    * @param quorum
    */
   public void registerQuorum(Quorum quorum)
   {
      quorums.put(quorum.getName(), quorum);
      quorum.setQuorumManager(this);
   }

   /**
    * unregisters a {@link org.hornetq.core.server.cluster.qourum.Quorum}.
    * @param quorum
    */
   public void unRegisterQuorum(Quorum quorum)
   {
      quorums.remove(quorum.getName());
   }

   /**
    * called by the {@link org.hornetq.core.client.impl.ServerLocatorInternal} when the topology changes. we update the
    * {@code maxClusterSize} if needed and inform the {@link org.hornetq.core.server.cluster.qourum.Quorum}'s.
    *
    * @param topologyMember the topolgy changed
    * @param last if the whole cluster topology is being transmitted (after adding the listener to
    *           the cluster connection) this parameter will be {@code true} for the last topology
    */
   @Override
   public void nodeUP(TopologyMember topologyMember, boolean last)
   {
      final int newClusterSize = serverLocator.getTopology().getMembers().size();
      maxClusterSize = newClusterSize > maxClusterSize ? newClusterSize : maxClusterSize;
      for (Quorum quorum : quorums.values())
      {
         quorum.nodeUp(serverLocator.getTopology());
      }
   }

   /**
    * notify the {@link org.hornetq.core.server.cluster.qourum.Quorum} of a topology change.
    * @param eventUID
    * @param nodeID the id of the node leaving the cluster
    */
   @Override
   public void nodeDown(long eventUID, String nodeID)
   {
      for (Quorum quorum : quorums.values())
      {
         quorum.nodeDown(serverLocator.getTopology(), eventUID, nodeID);
      }
   }

   /**
    * returns the maximum size this cluster has been.
    * @return max size
    */
   public int getMaxClusterSize()
   {
      return maxClusterSize;
   }

   /**
    * ask the quorum to vote within a specific quorum.
    *
    * @param quorumVote the vote to acquire
    */
   public void vote(final QuorumVote quorumVote)
   {
      List<VoteRunnable> runnables = new ArrayList<>();
      synchronized (voteRunnables)
      {
         if (!started)
            return;
         for (TopologyMemberImpl tm : serverLocator.getTopology().getMembers())
         {
            Pair<TransportConfiguration, TransportConfiguration> pair = tm.getConnector();

            final TransportConfiguration serverTC = pair.getA();

            final ServerLocatorImpl locator = (ServerLocatorImpl) HornetQClient.createServerLocatorWithoutHA(serverTC);
            VoteRunnable voteRunnable = new VoteRunnable(locator, serverTC, quorumVote);
            runnables.add(voteRunnable);
            executor.submit(voteRunnable);
         }
         voteRunnables.put(quorumVote, runnables);
      }
   }

   /**
    * must be called by the quorum when it is happy on an outcome. only one vote can take place at anyone time for a
    * specific quorum
    *
    * @param quorumVote the vote
    */
   public void voteComplete(QuorumVoteServerConnect quorumVote)
   {
      List<VoteRunnable> runnables = voteRunnables.remove(quorumVote);
      if (runnables != null)
      {
         for (VoteRunnable runnable : runnables)
         {
            runnable.close();
         }
      }
   }

   @Override
   public String toString()
   {
      return QuorumManager.class.getSimpleName() + "(server=" + server.getIdentity() + ")";
   }

   public void setServerLocator(ServerLocator serverLocator)
   {
      this.serverLocator = serverLocator;
   }

   public ServerLocator getServerLocator()
   {
      return serverLocator;
   }

   /**
    * this will connect to a node and then cast a vote. whether or not this vote is asked of the target node is dependant
    * on {@link org.hornetq.core.server.cluster.qourum.Vote#isRequestServerVote()}
    */
   private final class VoteRunnable implements Runnable
   {
      private final ServerLocatorInternal locator;
      private final TransportConfiguration serverTC;
      private final QuorumVote quorumVote;

      public VoteRunnable(ServerLocatorInternal locator, TransportConfiguration serverTC, QuorumVote quorumVote)
      {
         this.locator = locator;
         this.serverTC = serverTC;
         this.quorumVote = quorumVote;
      }

      @Override
      public void run()
      {
         final ClientSessionFactory sessionFactory;
         ClientSession session;
         try
         {
            Vote vote;
            sessionFactory = locator.createSessionFactory(serverTC);
            if (sessionFactory != null)
            {
               session = sessionFactory.createSession();
               if (session != null)
               {
                  vote = quorumVote.connected();
                  session.close();
                  sessionFactory.close();
                  if (vote.isRequestServerVote())
                  {
                     //todo ask the cluster
                  }
                  else
                  {
                     quorumVote.vote(vote);
                  }
               }
               else
               {
                  vote = quorumVote.notConnected();
                  quorumVote.vote(vote);
               }
            }
         }
         catch (Exception e)
         {
            // no-op
         }
         finally
         {
            locator.close();
         }
      }

      public void close()
      {
         if (locator != null)
         {
            locator.close();
         }
      }
   }
}
