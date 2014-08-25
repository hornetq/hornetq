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

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.core.client.HornetQClientLogger;
import org.hornetq.spi.core.protocol.AbstractRemotingConnection;
import org.hornetq.spi.core.remoting.Connection;
import org.proton.plug.AMQPConnectionContext;

/**
 *
 * This is a Server's Connection representation used by HornetQ.
 * @author Clebert Suconic
 */

public class HornetQProtonRemotingConnection extends AbstractRemotingConnection
{
   private final AMQPConnectionContext amqpConnection;

   private boolean destroyed = false;

   private final ProtonProtocolManager manager;


   public HornetQProtonRemotingConnection(ProtonProtocolManager manager, AMQPConnectionContext amqpConnection, Connection transportConnection, Executor executor)
   {
      super(transportConnection, executor);
      this.manager = manager;
      this.amqpConnection = amqpConnection;
   }

   public ProtonProtocolManager getManager()
   {
      return manager;
   }

   /*
    * This can be called concurrently by more than one thread so needs to be locked
    */
   public void fail(final HornetQException me, String scaleDownTargetNodeID)
   {
      if (destroyed)
      {
         return;
      }

      destroyed = true;

      HornetQClientLogger.LOGGER.connectionFailureDetected(me.getMessage(), me.getType());

      // Then call the listeners
      callFailureListeners(me, scaleDownTargetNodeID);

      callClosingListeners();

      internalClose();
   }


   @Override
   public void destroy()
   {
      synchronized (this)
      {
         if (destroyed)
         {
            return;
         }

         destroyed = true;
      }


      callClosingListeners();

      internalClose();

   }

   @Override
   public boolean isClient()
   {
      return false;
   }

   @Override
   public boolean isDestroyed()
   {
      return destroyed;
   }

   @Override
   public void disconnect(boolean criticalError)
   {
      getTransportConnection().close();
   }

   /**
    * Disconnect the connection, closing all channels
    */
   @Override
   public void disconnect(String scaleDownNodeID, boolean criticalError)
   {
      getTransportConnection().close();
   }

   @Override
   public boolean checkDataReceived()
   {
      return amqpConnection.checkDataReceived();
   }

   @Override
   public void flush()
   {
      amqpConnection.flush();
   }

   @Override
   public void bufferReceived(Object connectionID, HornetQBuffer buffer)
   {
      amqpConnection.inputBuffer(buffer.byteBuf());
      super.bufferReceived(connectionID, buffer);
   }

   private void internalClose()
   {
      // We close the underlying transport connection
      getTransportConnection().close();
   }
}
