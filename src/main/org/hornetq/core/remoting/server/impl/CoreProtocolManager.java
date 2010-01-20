/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.core.remoting.server.impl;

import java.util.List;

import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.remoting.Channel;
import org.hornetq.core.remoting.ChannelHandler;
import org.hornetq.core.remoting.CoreRemotingConnection;
import org.hornetq.core.remoting.Packet;
import org.hornetq.core.remoting.impl.RemotingConnectionImpl;
import org.hornetq.core.remoting.impl.wireformat.PacketImpl;
import org.hornetq.core.remoting.impl.wireformat.Ping;
import org.hornetq.core.remoting.server.ConnectionEntry;
import org.hornetq.core.remoting.server.ProtocolManager;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.impl.HornetQPacketHandler;
import org.hornetq.spi.core.remoting.Connection;

/**
 * A CoreProtocolManager
 *
 * @author Tim Fox
 *
 *
 */
public class CoreProtocolManager implements ProtocolManager
{
   private final HornetQServer server;
   
   private final List<Interceptor> interceptors;

   public CoreProtocolManager(final HornetQServer server,
                              final List<Interceptor> interceptors)
   {
      this.server = server;

      this.interceptors = interceptors;
   }

   public ConnectionEntry createConnectionEntry(final Connection connection)
   {
      final Configuration config = server.getConfiguration();
      
      CoreRemotingConnection rc = new RemotingConnectionImpl(connection,
                                                             interceptors,
                                                             config.isAsyncConnectionExecutionEnabled() ? server.getExecutorFactory().getExecutor()
                                                                                                       : null);

      Channel channel1 = rc.getChannel(1, -1);

      ChannelHandler handler = new HornetQPacketHandler(server, channel1, rc);

      channel1.setHandler(handler);

      long ttl = HornetQClient.DEFAULT_CONNECTION_TTL;

      if (config.getConnectionTTLOverride() != -1)
      {
         ttl = config.getConnectionTTLOverride();
      }

      final ConnectionEntry entry = new ConnectionEntry(rc, System.currentTimeMillis(), ttl);

      final Channel channel0 = rc.getChannel(0, -1);

      channel0.setHandler(new ChannelHandler()
      {
         public void handlePacket(final Packet packet)
         {
            if (packet.getType() == PacketImpl.PING)
            {
               Ping ping = (Ping)packet;

               if (config.getConnectionTTLOverride() == -1)
               {
                  // Allow clients to specify connection ttl
                  entry.ttl = ping.getConnectionTTL();
               }

               // Just send a ping back
               channel0.send(packet);
            }
         }
      });

      return entry;
   }
}
