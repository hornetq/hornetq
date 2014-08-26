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

package org.hornetq.core.protocol.proton;

import java.util.concurrent.Executor;

import io.netty.channel.ChannelPipeline;
import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.core.protocol.proton.sasl.HornetQPlainSASL;
import org.hornetq.core.remoting.impl.netty.NettyServerConnection;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.management.Notification;
import org.hornetq.core.server.management.NotificationListener;
import org.hornetq.spi.core.protocol.ConnectionEntry;
import org.hornetq.spi.core.protocol.ProtocolManager;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.remoting.Acceptor;
import org.hornetq.spi.core.remoting.Connection;
import org.proton.plug.AMQPServerConnectionContext;
import org.proton.plug.ServerSASL;
import org.proton.plug.context.server.ProtonServerConnectionContextFactory;

/**
 * A proton protocol manager, basically reads the Proton Input and maps proton resources to HornetQ resources
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ProtonProtocolManager implements ProtocolManager, NotificationListener
{
   private final HornetQServer server;

   public ProtonProtocolManager(HornetQServer server)
   {
      this.server = server;
   }

   public HornetQServer getServer()
   {
      return server;
   }

   @Override
   public void onNotification(Notification notification)
   {

   }

   @Override
   public ConnectionEntry createConnectionEntry(Acceptor acceptorUsed, Connection remotingConnection)
   {
      HornetQProtonConnectionCallback connectionCallback = new HornetQProtonConnectionCallback(this, remotingConnection);

      AMQPServerConnectionContext amqpConnection = ProtonServerConnectionContextFactory.getFactory().createConnection(connectionCallback);

      amqpConnection.createServerSASL(new ServerSASL[]{new HornetQPlainSASL(server.getSecurityStore(), server.getSecurityManager())});

      Executor executor = server.getExecutorFactory().getExecutor();

      HornetQProtonRemotingConnection delegate = new HornetQProtonRemotingConnection(this, amqpConnection, remotingConnection, executor);

      connectionCallback.setProtonConnectionDelegate(delegate);

      ConnectionEntry entry = new ConnectionEntry(delegate, executor,
                                                  System.currentTimeMillis(), HornetQClient.DEFAULT_CONNECTION_TTL);

      return entry;
   }

   @Override
   public void removeHandler(String name)
   {

   }

   @Override
   public void handleBuffer(RemotingConnection connection, HornetQBuffer buffer)
   {
      HornetQProtonRemotingConnection protonConnection = (HornetQProtonRemotingConnection)connection;

      protonConnection.bufferReceived(protonConnection.getID(), buffer);
   }

   @Override
   public void addChannelHandlers(ChannelPipeline pipeline)
   {

   }

   @Override
   public boolean isProtocol(byte[] array)
   {
      return array.length >= 4 && array[0] == (byte) 'A' && array[1] == (byte) 'M' && array[2] == (byte) 'Q' && array[3] == (byte) 'P';
   }

   @Override
   public void handshake(NettyServerConnection connection, HornetQBuffer buffer)
   {
   }


}
