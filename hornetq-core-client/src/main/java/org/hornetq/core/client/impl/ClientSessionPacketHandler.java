/*
 * Copyright 2009 Red Hat, Inc.
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

package org.hornetq.core.client.impl;

import static org.hornetq.core.protocol.core.impl.PacketImpl.EXCEPTION;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_RECEIVE_CONTINUATION;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_RECEIVE_LARGE_MSG;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_RECEIVE_MSG;

import org.hornetq.core.protocol.core.Channel;
import org.hornetq.core.protocol.core.ChannelHandler;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.PacketImpl;
import org.hornetq.core.protocol.core.impl.wireformat.HornetQExceptionMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionProducerCreditsFailMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionProducerCreditsMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionReceiveContinuationMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionReceiveLargeMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionReceiveMessage;
import org.hornetq.core.client.HornetQClientLogger;

/**
 *
 * A ClientSessionPacketHandler
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
final class ClientSessionPacketHandler implements ChannelHandler
{
   private final ClientSessionInternal clientSession;

   private final Channel channel;

   ClientSessionPacketHandler(final ClientSessionInternal clientSesssion, final Channel channel)
   {
      clientSession = clientSesssion;

      this.channel = channel;
   }

   public void handlePacket(final Packet packet)
   {
      byte type = packet.getType();

      try
      {
         switch (type)
         {
            case SESS_RECEIVE_CONTINUATION:
            {
               SessionReceiveContinuationMessage continuation = (SessionReceiveContinuationMessage)packet;

               clientSession.handleReceiveContinuation(continuation.getConsumerID(), continuation);

               break;
            }
            case SESS_RECEIVE_MSG:
            {
               SessionReceiveMessage message = (SessionReceiveMessage)packet;

               clientSession.handleReceiveMessage(message.getConsumerID(), message);

               break;
            }
            case SESS_RECEIVE_LARGE_MSG:
            {
               SessionReceiveLargeMessage message = (SessionReceiveLargeMessage)packet;

               clientSession.handleReceiveLargeMessage(message.getConsumerID(), message);

               break;
            }
            case PacketImpl.SESS_PRODUCER_CREDITS:
            {
               SessionProducerCreditsMessage message = (SessionProducerCreditsMessage)packet;

               clientSession.handleReceiveProducerCredits(message.getAddress(), message.getCredits());

               break;
            }
            case PacketImpl.SESS_PRODUCER_FAIL_CREDITS:
            {
               SessionProducerCreditsFailMessage message = (SessionProducerCreditsFailMessage)packet;

               clientSession.handleReceiveProducerFailCredits(message.getAddress(), message.getCredits());

               break;
            }
            case EXCEPTION:
            {
               // TODO - we can provide a means for async exceptions to get back to to client
               // For now we just log it
               HornetQExceptionMessage mem = (HornetQExceptionMessage)packet;

               HornetQClientLogger.LOGGER.receivedExceptionAsynchronously(mem.getException());

               break;
            }
            default:
            {
               throw new IllegalStateException("Invalid packet: " + type);
            }
         }
      }
      catch (Exception e)
      {
         HornetQClientLogger.LOGGER.failedToHandlePacket(e);
      }

      channel.confirm(packet);
   }
}