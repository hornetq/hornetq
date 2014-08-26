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

package org.hornetq.core.protocol.proton.client;

import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.locks.Lock;

import io.netty.channel.ChannelPipeline;
import org.apache.qpid.proton.engine.EndpointState;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.remoting.ClientProtocolManager;
import org.hornetq.spi.core.remoting.Connection;
import org.hornetq.spi.core.remoting.TopologyResponseHandler;
import org.hornetq.spi.core.remoting.SessionContext;

/**
 * @author Clebert Suconic
 */

public class AMQPClientProtocolManager implements ClientProtocolManager
{
   public static final EnumSet<EndpointState> UNINITIALIZED = EnumSet.of(EndpointState.UNINITIALIZED);

   public static final EnumSet<EndpointState> INITIALIZED = EnumSet.complementOf(UNINITIALIZED);

   public static final EnumSet<EndpointState> ACTIVE = EnumSet.of(EndpointState.ACTIVE);

   public static final EnumSet<EndpointState> CLOSED = EnumSet.of(EndpointState.CLOSED);

   public static final EnumSet<EndpointState> ANY = EnumSet.of(EndpointState.ACTIVE, EndpointState.CLOSED, EndpointState.UNINITIALIZED);

   ClientSessionFactory sessionFactory;

   ClientProtonRemotingConnection connection;

   Connection transportConnection;

   TopologyResponseHandler topologyResponseHandler;

   boolean alive = true;

   public void setSessionFactory(ClientSessionFactory sessionFactory)
   {
      this.sessionFactory = sessionFactory;
   }


   public ClientSessionFactory getSessionFactory()
   {
      return sessionFactory;
   }

   @Override
   public RemotingConnection connect(Connection transportConnection, long callTimeout, long callFailoverTimeout, List<Interceptor> incomingInterceptors, List<Interceptor> outgoingInterceptors, TopologyResponseHandler topologyResponseHandler)
   {
      this.transportConnection = transportConnection;
      connection = new ClientProtonRemotingConnection(transportConnection);
      connection.open();
      this.topologyResponseHandler = topologyResponseHandler;
      return connection;
   }

   @Override
   public RemotingConnection getCurrentConnection()
   {
      return null;
   }


   @Override
   public Lock lockSessionCreation()
   {
      return null;
   }

   @Override
   public boolean waitOnLatch(long milliseconds) throws InterruptedException
   {
      return false;
   }

   @Override
   public void stop()
   {
      alive = false;
   }

   @Override
   public boolean isAlive()
   {
      return alive;
   }

   @Override
   public void sendSubscribeTopology(boolean isServer)
   {
      // TODO: is there a way to get a better topology about the server with Proton?
      Pair<TransportConfiguration, TransportConfiguration> config =  new Pair<>(transportConnection.getConnectorConfig(), null);
      topologyResponseHandler.notifyNodeUp(System.currentTimeMillis(), "PROTON", "PROTON", "PROTON", config, true);
   }

   @Override
   public void ping(long connectionTTL)
   {
   }


   /**
    * Used to establish specific parsing context at Netty.
    * so far useful for HornetQ core only
    * @param pipeline
    */
   public void addChannelHandlers(ChannelPipeline pipeline)
   {
     // pipeline.addLast("amqp-decoder", new HornetQAMQPFrameDecoder());
   }

   /**
    * TODO: in HornetQ the user/password is done at the create-session. We need to verify how to deal with this.
    *       Maybe a good approach would be to defer the first session created until here.
    * @return
    * @throws HornetQException
    */
   @Override
   public SessionContext createSessionContext(String name, String username, String password, boolean xa, boolean autoCommitSends, boolean autoCommitAcks, boolean preAcknowledge, int minLargeMessageSize, int confirmationWindowSize) throws HornetQException
   {
      return connection.createProtonSession();
   }

   @Override
   public boolean cleanupBeforeFailover(org.hornetq.api.core.HornetQException ex)
   {
      return false;
   }

   public String getName()
   {
      return "AMQP1_0";
   }

   @Override
   public boolean checkForFailover(String liveNodeID) throws HornetQException
   {
      return true;
   }
}
