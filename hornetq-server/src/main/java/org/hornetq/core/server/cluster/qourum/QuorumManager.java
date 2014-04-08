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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.TopologyMember;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ServerLocatorImpl;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.client.impl.TopologyMemberImpl;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.config.ConfigurationUtils;
import org.hornetq.core.protocol.ServerPacketDecoder;
import org.hornetq.core.protocol.core.CoreRemotingConnection;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.core.server.HornetQComponent;
import org.hornetq.core.server.HornetQMessageBundle;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServerLogger;

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
    * all the {@link org.hornetq.core.server.cluster.qourum.Quorum}'s that have registered for connection failures
    */
   private Map<Quorum, QuorumFailureListener> quorumFailureListeners = new HashMap<>();

   /**
    * any currently running runnables.
    */
   private final Map<QuorumVote, List<VoteRunnable>> voteRunnables = new HashMap<>();

   private HornetQServer server;

  /**
   * this holds the current state of the cluster
   */
   private ServerLocatorInternal serverLocator;

   private boolean started = false;

   private CountDownLatch connectedLatch;

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
      synchronized (this)
      {
         connectedLatch = new CountDownLatch(1);
         //we use the cluster connection configuration to connect to the cluster to find the live node we want to
         //connect to.
         ClusterConnectionConfiguration config =
            ConfigurationUtils.getReplicationClusterConfiguration(server.getConfiguration());
         if (serverLocator != null)
         {
            serverLocator.close();
         }
         serverLocator = getLocator(config);
      }

      synchronized (voteRunnables)
      {
         started = true;
         //if the cluster isn't available we want to hang around until it is
         serverLocator.setReconnectAttempts(-1);
         serverLocator.setInitialConnectAttempts(-1);
         //this is used for replication so need to use the server packet decoder
         serverLocator.setPacketDecoder(ServerPacketDecoder.INSTANCE);
         //receive topology updates
         serverLocator.addClusterTopologyListener(this);

         server.getExecutorFactory().getExecutor().execute(new ConnectRunnable());
      }
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
         serverLocator.close();
         serverLocator = null;
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
      quorumFailureListeners.clear();
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
    * the current size of the cluster
    *
    * @return current size
    */
   public int getCurrentClusterSize()
   {
      return serverLocator.getTopology().getMembers().size();
   }

   /**
    * @param liveConnection the connection to listen to
    * @param quorum the quorum to notify on failure
    */
   public void addAsFailureListenerOf(CoreRemotingConnection liveConnection, Quorum quorum)
   {
      QuorumFailureListener listener = new QuorumFailureListener(quorum);
      quorumFailureListeners.put(quorum, listener);
      liveConnection.addFailureListener(listener);
   }

   /**
    * removes the failure listener
    * @param connection
    * @param quorum
    */
   public void removeFailureListener(CoreRemotingConnection connection, Quorum quorum)
   {
      QuorumFailureListener listener = quorumFailureListeners.remove(quorum);
      if (connection != null && listener != null)
      {
         connection.removeFailureListener(listener);
      }
   }

   /**
    * add a cluster listener
    *
    * @param listener
    */
   public void addClusterTopologyListener(ClusterTopologyListener listener)
   {
      serverLocator.addClusterTopologyListener(listener);
   }

   /**
    * wait until we have connected to the cluster.
    *
    * @throws InterruptedException
    */
   public void awaitConnectionToCluster() throws InterruptedException
   {
      connectedLatch.await();
   }

   /**
    * add an interceptor
    *
    * @param interceptor
    */
   public void addIncomingInterceptor(Interceptor interceptor)
   {
      serverLocator.addIncomingInterceptor(interceptor);
   }

   /**
    * connect to a specific node in the cluster.
    *
    * @param transportConfiguration the configuration of the node to connect to.
    *
    * @return the ClientSessionFactoryInternal
    * @throws Exception
    */
   public ClientSessionFactoryInternal connectToNode(TransportConfiguration transportConfiguration) throws Exception
   {
      return (ClientSessionFactoryInternal) serverLocator.createSessionFactory(transportConfiguration, 0, false);
   }

   /**
    * retry interval for connecting to the cluster
    *
    * @return the retry interval
    */
   public long getRetryInterval()
   {
      return serverLocator.getRetryInterval();
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

   private ServerLocatorInternal getLocator(ClusterConnectionConfiguration config) throws HornetQException
   {
      ServerLocatorInternal locator;
      if (config.getDiscoveryGroupName() != null)
      {
         DiscoveryGroupConfiguration dg = server.getConfiguration().getDiscoveryGroupConfigurations().get(config.getDiscoveryGroupName());

         if (dg == null)
         {
            throw HornetQMessageBundle.BUNDLE.noDiscoveryGroupFound(dg);
         }
         locator = (ServerLocatorInternal) HornetQClient.createServerLocatorWithHA(dg);
      }
      else
      {
         TransportConfiguration[] tcConfigs = config.getStaticConnectors() != null ? connectorNameListToArray(config.getStaticConnectors())
            : null;

         locator = (ServerLocatorInternal) HornetQClient.createServerLocatorWithHA(tcConfigs);
      }
      return locator;
   }


   private TransportConfiguration[] connectorNameListToArray(final List<String> connectorNames)
   {
      TransportConfiguration[] tcConfigs = (TransportConfiguration[]) Array.newInstance(TransportConfiguration.class,
                                                                                        connectorNames.size());
      int count = 0;
      for (String connectorName : connectorNames)
      {
         TransportConfiguration connector = server.getConfiguration().getConnectorConfigurations().get(connectorName);

         if (connector == null)
         {
            HornetQServerLogger.LOGGER.bridgeNoConnector(connectorName);

            return null;
         }

         tcConfigs[count++] = connector;
      }

      return tcConfigs;
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

   private final class ConnectRunnable implements Runnable
   {
      @Override
      public void run()
      {
         try
         {
            serverLocator.connect();
            connectedLatch.countDown();
         }
         catch (HornetQException e)
         {
            if (!started)
               return;
            server.getScheduledPool().schedule(this, serverLocator.getRetryInterval(), TimeUnit.MILLISECONDS);
         }
      }
   }

   private final class QuorumFailureListener implements FailureListener
   {
      private final Quorum quorum;

      public QuorumFailureListener(Quorum quorum)
      {
         this.quorum = quorum;
      }

      @Override
      public void connectionFailed(HornetQException exception, boolean failedOver)
      {
         quorum.connectionFailed(serverLocator.getTopology());
      }
   }
}
