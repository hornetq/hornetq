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

import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.hornetq.core.buffers.impl.ChannelBufferWrapper;
import org.hornetq.spi.core.remoting.Connection;
import org.hornetq.utils.ReusableLatch;
import org.proton.plug.AMQPConnectionCallback;
import org.proton.plug.AMQPConnectionContext;
import org.proton.plug.AMQPSessionCallback;
import org.proton.plug.util.DebugInfo;

/**
 * @author Clebert Suconic
 */

public class HornetQProtonConnectionCallback implements AMQPConnectionCallback
{
   private final ProtonProtocolManager manager;

   private final Connection connection;

   protected HornetQProtonRemotingConnection protonConnectionDelegate;

   protected AMQPConnectionContext amqpConnection;

   private final ReusableLatch latch = new ReusableLatch(0);

   public HornetQProtonConnectionCallback(ProtonProtocolManager manager, Connection connection)
   {
      this.manager = manager;
      this.connection = connection;
   }


   @Override
   public void close()
   {

   }

   @Override
   public void setConnection(AMQPConnectionContext connection)
   {
      this.amqpConnection = connection;
   }

   @Override
   public AMQPConnectionContext getConnection()
   {
      return amqpConnection;
   }

   public HornetQProtonRemotingConnection getProtonConnectionDelegate()
   {
      return protonConnectionDelegate;
   }

   public void setProtonConnectionDelegate(HornetQProtonRemotingConnection protonConnectionDelegate)
   {
      this.protonConnectionDelegate = protonConnectionDelegate;
   }

   public void onTransport(ByteBuf byteBuf, AMQPConnectionContext amqpConnection)
   {
      final int size = byteBuf.writerIndex();

      latch.countUp();
      connection.write(new ChannelBufferWrapper(byteBuf, true), true, true, new ChannelFutureListener()
      {
         @Override
         public void operationComplete(ChannelFuture future) throws Exception
         {
            latch.countDown();
         }
      });

      // move this somewhere else Urgently.. don't commit master with this
      if (DebugInfo.performSyncOnFlush)
      {
         try
         {
            latch.await(5, TimeUnit.SECONDS);
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }
      }
      amqpConnection.outputDone(size);
   }


   @Override
   public AMQPSessionCallback createSessionCallback(AMQPConnectionContext connection)
   {
      return new ProtonSessionIntegrationCallback(this, manager, connection);
   }

}
