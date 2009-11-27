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

import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.EXCEPTION;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_RECEIVE_CONTINUATION;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_RECEIVE_LARGE_MSG;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_RECEIVE_MSG;

import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.Channel;
import org.hornetq.core.remoting.ChannelHandler;
import org.hornetq.core.remoting.Packet;
import org.hornetq.core.remoting.impl.wireformat.HornetQExceptionMessage;
import org.hornetq.core.remoting.impl.wireformat.PacketImpl;
import org.hornetq.core.remoting.impl.wireformat.SessionProducerCreditsMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionReceiveContinuationMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionReceiveLargeMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionReceiveMessage;

/**
 *
 * A ClientSessionPacketHandler
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ClientSessionPacketHandler implements ChannelHandler
{
   private static final Logger log = Logger.getLogger(ClientSessionPacketHandler.class);

   private final ClientSessionInternal clientSession;
   
   private final Channel channel;

   public ClientSessionPacketHandler(final ClientSessionInternal clientSesssion, final Channel channel)
   {
      this.clientSession = clientSesssion;
      
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
               SessionReceiveMessage message = (SessionReceiveMessage) packet;
               
               clientSession.handleReceiveMessage(message.getConsumerID(), message);               
               
               break;
            }
            case SESS_RECEIVE_LARGE_MSG:
            {
               SessionReceiveLargeMessage message = (SessionReceiveLargeMessage) packet;
               
               clientSession.handleReceiveLargeMessage(message.getConsumerID(), message);
                              
               break;
            }
            case PacketImpl.SESS_PRODUCER_CREDITS:
            {
               SessionProducerCreditsMessage message = (SessionProducerCreditsMessage)packet;
               
               clientSession.handleReceiveProducerCredits(message.getAddress(), message.getCredits(),
                                                          message.getOffset());
               
               break;
            }
            case EXCEPTION:
            {
               //TODO - we can provide a means for async exceptions to get back to to client
               //For now we just log it
               HornetQExceptionMessage mem = (HornetQExceptionMessage)packet;
               
               log.error("Received exception asynchronously from server", mem.getException());
               
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
         log.error("Failed to handle packet", e);
      }
      
      channel.confirm(packet);
   }
}