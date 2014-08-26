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

package org.hornetq.core.server.cluster;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.HornetQAlreadyReplicatingException;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ServerLocatorImpl;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.client.impl.Topology;
import org.hornetq.core.protocol.core.Channel;
import org.hornetq.core.protocol.core.ChannelHandler;
import org.hornetq.core.protocol.core.CoreRemotingConnection;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.PacketImpl;
import org.hornetq.core.protocol.core.impl.wireformat.BackupRegistrationMessage;
import org.hornetq.core.protocol.core.impl.wireformat.BackupReplicationStartFailedMessage;
import org.hornetq.core.protocol.core.impl.wireformat.BackupRequestMessage;
import org.hornetq.core.protocol.core.impl.wireformat.BackupResponseMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ClusterConnectMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ClusterConnectReplyMessage;
import org.hornetq.core.protocol.core.impl.wireformat.NodeAnnounceMessage;
import org.hornetq.core.protocol.core.impl.wireformat.QuorumVoteMessage;
import org.hornetq.core.protocol.core.impl.wireformat.QuorumVoteReplyMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ScaleDownAnnounceMessage;
import org.hornetq.core.server.HornetQComponent;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.cluster.ha.HAPolicy;
import org.hornetq.core.server.cluster.qourum.QuorumManager;
import org.hornetq.core.server.cluster.qourum.QuorumVoteHandler;
import org.hornetq.core.server.cluster.qourum.Vote;
import org.hornetq.spi.core.remoting.Acceptor;

/**
 * used for creating and managing cluster control connections for each cluster connection and the replication connection
 */
public class ClusterController implements HornetQComponent
{
   private static final boolean isTrace = HornetQServerLogger.LOGGER.isTraceEnabled();

   private final QuorumManager quorumManager;

   private final HornetQServer server;

   private Map<SimpleString, ServerLocatorInternal> locators = new HashMap<>();

   private SimpleString defaultClusterConnectionName;

   private ServerLocator defaultLocator;

   private ServerLocator replicationLocator;

   private final Executor executor;

   private CountDownLatch replicationClusterConnectedLatch;

   private boolean started;

   public ClusterController(HornetQServer server, ScheduledExecutorService scheduledExecutor)
   {
      this.server = server;
      executor = server.getExecutorFactory().getExecutor();
      quorumManager = new QuorumManager(scheduledExecutor, this);
   }

   @Override
   public void start() throws Exception
   {
      if (started)
         return;
      //set the default locator that will be used to connecting to the default cluster.
      defaultLocator = locators.get(defaultClusterConnectionName);
      //create a locator for replication, either the default or the specified if not set
      if (server.getConfiguration().getHAPolicy().getReplicationClustername() != null && !server.getConfiguration().getHAPolicy().getReplicationClustername().equals(defaultClusterConnectionName.toString()))
      {
         replicationLocator = locators.get(server.getConfiguration().getHAPolicy().getReplicationClustername());
         if (replicationLocator == null)
         {
            HornetQServerLogger.LOGGER.noClusterConnectionForReplicationCluster();
            replicationLocator = defaultLocator;
         }
      }
      else
      {
         replicationLocator = defaultLocator;
      }
      //latch so we know once we are connected
      replicationClusterConnectedLatch = new CountDownLatch(1);
      //and add the quorum manager as a topology listener
      defaultLocator.addClusterTopologyListener(quorumManager);
      //start the quorum manager
      quorumManager.start();
      started = true;
      //connect all the locators in a separate thread
      for (ServerLocatorInternal serverLocatorInternal : locators.values())
      {
         if (serverLocatorInternal.isConnectable())
         {
            executor.execute(new ConnectRunnable(serverLocatorInternal));
         }
      }
   }

   @Override
   public void stop() throws Exception
   {
      //close all the locators
      for (ServerLocatorInternal serverLocatorInternal : locators.values())
      {
         serverLocatorInternal.close();
      }
      //stop the quorum manager
      quorumManager.stop();
      started = false;
   }

   @Override
   public boolean isStarted()
   {
      return started;
   }

   public QuorumManager getQuorumManager()
   {
      return quorumManager;
   }

   //set the default cluster connections name
   public void setDefaultClusterConnectionName(SimpleString defaultClusterConnection)
   {
      this.defaultClusterConnectionName = defaultClusterConnection;
   }

   /**
    * add a locator for a cluster connection.
    *
    * @param name the cluster connection name
    * @param dg the discovery group to use
    */
   public void addClusterConnection(SimpleString name, DiscoveryGroupConfiguration dg)
   {
      ServerLocatorImpl serverLocator = (ServerLocatorImpl) HornetQClient.createServerLocatorWithHA(dg);
      //if the cluster isn't available we want to hang around until it is
      serverLocator.setReconnectAttempts(-1);
      serverLocator.setInitialConnectAttempts(-1);
      //this is used for replication so need to use the server packet decoder
      serverLocator.setProtocolManagerFactory(new HornetQServerSideProtocolManagerFactory());
      locators.put(name, serverLocator);
   }

   /**
    * add a locator for a cluster connection.
    *
    * @param name the cluster connection name
    * @param tcConfigs the transport configurations to use
    */
   public void addClusterConnection(SimpleString name, TransportConfiguration[] tcConfigs)
   {
      ServerLocatorImpl serverLocator = (ServerLocatorImpl) HornetQClient.createServerLocatorWithHA(tcConfigs);
      //if the cluster isn't available we want to hang around until it is
      serverLocator.setReconnectAttempts(-1);
      serverLocator.setInitialConnectAttempts(-1);
      //this is used for replication so need to use the server packet decoder
      serverLocator.setProtocolManagerFactory(new HornetQServerSideProtocolManagerFactory());
      locators.put(name, serverLocator);
   }

   /**
    * add a cluster listener
    *
    * @param listener
    */
   public void addClusterTopologyListenerForReplication(ClusterTopologyListener listener)
   {
      replicationLocator.addClusterTopologyListener(listener);
   }

   /**
    * add an interceptor
    *
    * @param interceptor
    */
   public void addIncomingInterceptorForReplication(Interceptor interceptor)
   {
      replicationLocator.addIncomingInterceptor(interceptor);
   }


   /**
    * connect to a specific node in the cluster used for replication
    *
    * @param transportConfiguration the configuration of the node to connect to.
    *
    * @return the Cluster Control
    * @throws Exception
    */
   public ClusterControl connectToNode(TransportConfiguration transportConfiguration) throws Exception
   {
      ClientSessionFactoryInternal sessionFactory = (ClientSessionFactoryInternal) defaultLocator.createSessionFactory(transportConfiguration, 0, false);

      return connectToNodeInCluster(sessionFactory);
   }

   /**
    * connect to a specific node in the cluster used for replication
    *
    * @param transportConfiguration the configuration of the node to connect to.
    *
    * @return the Cluster Control
    * @throws Exception
    */
   public ClusterControl connectToNodeInReplicatedCluster(TransportConfiguration transportConfiguration) throws Exception
   {
      ClientSessionFactoryInternal sessionFactory = (ClientSessionFactoryInternal) replicationLocator.createSessionFactory(transportConfiguration, 0, false);

      return connectToNodeInCluster(sessionFactory);
   }

   /**
    * connect to an already defined node in the cluster
    *
    * @param sf the session factory
    *
    * @return  the Cluster Control
    */
   public ClusterControl connectToNodeInCluster(ClientSessionFactoryInternal sf)
   {
      sf.getServerLocator().setProtocolManagerFactory(new HornetQServerSideProtocolManagerFactory());
      return new ClusterControl(sf, server);
   }

   /**
    * retry interval for connecting to the cluster
    *
    * @return the retry interval
    */
   public long getRetryIntervalForReplicatedCluster()
   {
      return replicationLocator.getRetryInterval();
   }


   /**
    * wait until we have connected to the cluster.
    *
    * @throws InterruptedException
    */
   public void awaitConnectionToReplicationCluster() throws InterruptedException
   {
      replicationClusterConnectedLatch.await();
   }

   /**
    * used to set a channel handler on the connection that can be used by the cluster control
    *
    * @param channel the channel to set the handler
    * @param acceptorUsed the acceptor used for connection
    * @param remotingConnection the connection itself
    */
   public void addClusterChannelHandler(Channel channel, Acceptor acceptorUsed, CoreRemotingConnection remotingConnection)
   {
      channel.setHandler(new ClusterControllerChannelHandler(channel, acceptorUsed, remotingConnection));
   }

   public int getDefaultClusterSize()
   {
      return defaultLocator.getTopology().getMembers().size();
   }

   public Topology getDefaultClusterTopology()
   {
      return defaultLocator.getTopology();
   }

   public SimpleString getNodeID()
   {
      return server.getNodeID();
   }

   public String getIdentity()
   {
      return server.getIdentity();
   }

   /**
    * a handler for handling packets sent between the cluster.
    */
   private final class ClusterControllerChannelHandler implements ChannelHandler
   {
      private final Channel clusterChannel;
      private final Acceptor acceptorUsed;
      private final CoreRemotingConnection remotingConnection;
      boolean authorized = false;

      public ClusterControllerChannelHandler(Channel clusterChannel, Acceptor acceptorUsed, CoreRemotingConnection remotingConnection)
      {
         this.clusterChannel = clusterChannel;
         this.acceptorUsed = acceptorUsed;
         this.remotingConnection = remotingConnection;
      }

      @Override
      public void handlePacket(Packet packet)
      {
         if (!authorized)
         {
            if (packet.getType() == PacketImpl.CLUSTER_CONNECT )
            {
               ClusterConnection clusterConnection = acceptorUsed.getClusterConnection();

               ClusterConnectMessage msg = (ClusterConnectMessage) packet;

               if (server.getConfiguration().isSecurityEnabled() && !clusterConnection.verify(msg.getClusterUser(), msg.getClusterPassword()))
               {
                  clusterChannel.send(new ClusterConnectReplyMessage(false));
               }
               else
               {
                  authorized = true;
                  clusterChannel.send(new ClusterConnectReplyMessage(true));
               }
            }
         }
         else
         {
            if (packet.getType() == PacketImpl.NODE_ANNOUNCE)
            {
               NodeAnnounceMessage msg = (NodeAnnounceMessage)packet;

               Pair<TransportConfiguration, TransportConfiguration> pair;
               if (msg.isBackup())
               {
                  pair = new Pair<>(null, msg.getConnector());
               }
               else
               {
                  pair = new Pair<>(msg.getConnector(), msg.getBackupConnector());
               }
               if (isTrace)
               {
                  HornetQServerLogger.LOGGER.trace("Server " + server + " receiving nodeUp from NodeID=" + msg.getNodeID() + ", pair=" + pair);
               }

               if (acceptorUsed != null)
               {
                  ClusterConnection clusterConn = acceptorUsed.getClusterConnection();
                  if (clusterConn != null)
                  {
                     String scaleDownGroupName = msg.getScaleDownGroupName();
                     clusterConn.nodeAnnounced(msg.getCurrentEventID(), msg.getNodeID(), msg.getBackupGroupName(), scaleDownGroupName, pair, msg.isBackup());
                  }
                  else
                  {
                     HornetQServerLogger.LOGGER.debug("Cluster connection is null on acceptor = " + acceptorUsed);
                  }
               }
               else
               {
                  HornetQServerLogger.LOGGER.debug("there is no acceptor used configured at the CoreProtocolManager " + this);
               }
            }
            else if (packet.getType() == PacketImpl.BACKUP_REGISTRATION)
            {
               BackupRegistrationMessage msg = (BackupRegistrationMessage)packet;
               ClusterConnection clusterConnection = acceptorUsed.getClusterConnection();
               try
               {
                  server.startReplication(remotingConnection, clusterConnection, getPair(msg.getConnector(), true),
                        msg.isFailBackRequest());
               }
               catch (HornetQAlreadyReplicatingException are)
               {
                  clusterChannel.send(new BackupReplicationStartFailedMessage(BackupReplicationStartFailedMessage.BackupRegistrationProblem.ALREADY_REPLICATING));
               }
               catch (HornetQException e)
               {
                  clusterChannel.send(new BackupReplicationStartFailedMessage(BackupReplicationStartFailedMessage.BackupRegistrationProblem.EXCEPTION));
               }
            }
            else if (packet.getType() == PacketImpl.QUORUM_VOTE)
            {
               QuorumVoteMessage quorumVoteMessage = (QuorumVoteMessage) packet;
               QuorumVoteHandler voteHandler = quorumManager.getVoteHandler(quorumVoteMessage.getHandler());
               quorumVoteMessage.decode(voteHandler);
               Vote vote = quorumManager.vote(quorumVoteMessage.getHandler(), quorumVoteMessage.getVote());
               clusterChannel.send(new QuorumVoteReplyMessage(quorumVoteMessage.getHandler(), vote));
            }
            else if (packet.getType() == PacketImpl.BACKUP_REQUEST)
            {
               BackupRequestMessage backupRequestMessage = (BackupRequestMessage) packet;
               boolean started = false;
               try
               {
                  if (backupRequestMessage.getBackupType() == HAPolicy.POLICY_TYPE.COLOCATED_REPLICATED)
                  {
                     started = server.getClusterManager().getHAManager().activateReplicatedBackup(backupRequestMessage.getBackupSize(), backupRequestMessage.getNodeID());
                  }
                  else
                  {
                     started = server.getClusterManager().getHAManager().activateSharedStoreBackup(backupRequestMessage.getBackupSize(),
                           backupRequestMessage.getJournalDirectory(),
                           backupRequestMessage.getBindingsDirectory(),
                           backupRequestMessage.getLargeMessagesDirectory(),
                           backupRequestMessage.getPagingDirectory());
                  }
               }
               catch (Exception e)
               {
                  //todo log a warning and send false
               }
               clusterChannel.send(new BackupResponseMessage(started));
            }
            else if (packet.getType() == PacketImpl.SCALEDOWN_ANNOUNCEMENT)
            {
               ScaleDownAnnounceMessage message = (ScaleDownAnnounceMessage) packet;
               //we don't really need to check as it should always be true
               if (server.getNodeID().equals(message.getTargetNodeId()))
               {
                  server.addScaledDownNode(message.getScaledDownNodeId());
               }
            }
         }
      }
      private Pair<TransportConfiguration, TransportConfiguration> getPair(TransportConfiguration conn,
                                                                           boolean isBackup)
      {
         if (isBackup)
         {
            return new Pair<>(null, conn);
         }
         return new Pair<>(conn, null);
      }
   }

   /**
    * used for making the initial connection in the cluster
    */
   private final class ConnectRunnable implements Runnable
   {
      private ServerLocatorInternal serverLocator;

      public ConnectRunnable(ServerLocatorInternal serverLocator)
      {
         this.serverLocator = serverLocator;
      }

      @Override
      public void run()
      {
         try
         {
            serverLocator.connect();
            if (serverLocator == replicationLocator)
            {
               replicationClusterConnectedLatch.countDown();
            }
         }
         catch (HornetQException e)
         {
            if (!started)
               return;
            server.getScheduledPool().schedule(this, serverLocator.getRetryInterval(), TimeUnit.MILLISECONDS);
         }
      }
   }
}
