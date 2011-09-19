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
package org.hornetq.core.protocol.stomp;

import java.io.UnsupportedEncodingException;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.protocol.stomp.v10.StompFrameHandlerV10;
import org.hornetq.core.protocol.stomp.v11.StompFrameHandlerV11;
import org.hornetq.core.server.ServerMessage;

/**
 *
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 */
public abstract class VersionedStompFrameHandler
{
   private static final Logger log = Logger.getLogger(VersionedStompFrameHandler.class);

   protected StompConnection connection;
   
   public static VersionedStompFrameHandler getHandler(StompConnection connection, StompVersions version)
   {
      if (version == StompVersions.V1_0)
      {
         return new StompFrameHandlerV10(connection);
      }
      if (version == StompVersions.V1_1)
      {
         return new StompFrameHandlerV11(connection);
      }
      return null;
   }

   public StompFrame handleFrame(StompFrame request)
   {
      StompFrame response = null;
      
      if (Stomp.Commands.SEND.equals(request.getCommand()))
      {
         response = onSend(request);
      }
      else if (Stomp.Commands.ACK.equals(request.getCommand()))
      {
         response = onAck(request);
      }
      else if (Stomp.Commands.NACK.equals(request.getCommand()))
      {
         response = onNack(request);
      }
      else if (Stomp.Commands.BEGIN.equals(request.getCommand()))
      {
         response = onBegin(request);
      }
      else if (Stomp.Commands.COMMIT.equals(request.getCommand()))
      {
         response = onCommit(request);
      }
      else if (Stomp.Commands.ABORT.equals(request.getCommand()))
      {
         response = onAbort(request);
      }
      else if (Stomp.Commands.SUBSCRIBE.equals(request.getCommand()))
      {
         response = onSubscribe(request);
      }
      else if (Stomp.Commands.UNSUBSCRIBE.equals(request.getCommand()))
      {
         response = onUnsubscribe(request);
      }
      else if (Stomp.Commands.CONNECT.equals(request.getCommand()))
      {
         response = onConnect(request);
      }
      else if (Stomp.Commands.STOMP.equals(request.getCommand()))
      {
         response = onStomp(request);
      }
      else if (Stomp.Commands.DISCONNECT.equals(request.getCommand()))
      {
         response = onDisconnect(request);
      }
      else
      {
         response = onUnknown(request.getCommand());
      }

      if (response == null)
      {
         response = postprocess(request);
      }
      else
      {
         if (request.hasHeader(Stomp.Headers.RECEIPT_REQUESTED))
         {
            response.addHeader(Stomp.Headers.Response.RECEIPT_ID, request.getHeader(Stomp.Headers.RECEIPT_REQUESTED));
         }
      }
      
      return response;
   }

   public abstract StompFrame onConnect(StompFrame frame);
   public abstract StompFrame onDisconnect(StompFrame frame);
   public abstract StompFrame onSend(StompFrame frame);
   public abstract StompFrame onAck(StompFrame request);
   public abstract StompFrame onBegin(StompFrame frame);
   public abstract StompFrame onCommit(StompFrame request);
   public abstract StompFrame onAbort(StompFrame request);
   public abstract StompFrame onSubscribe(StompFrame request);
   public abstract StompFrame onUnsubscribe(StompFrame request);
   public abstract StompFrame onStomp(StompFrame request);
   public abstract StompFrame onNack(StompFrame request);
   
   public StompFrame onUnknown(String command)
   {
      StompFrame response = new HornetQStompException("Unsupported command " + command).getFrame();
      return response;
   }
   
   public StompFrame handleReceipt(String receiptID)
   {
      StompFrame receipt = new StompFrame(Stomp.Responses.RECEIPT);
      receipt.addHeader(Stomp.Headers.Response.RECEIPT_ID, receiptID);
      
      return receipt;
   }
   
   public abstract StompFrame postprocess(StompFrame request);

   public abstract StompFrame createMessageFrame(ServerMessage serverMessage,
         StompSubscription subscription, int deliveryCount) throws Exception;

   public abstract StompFrame createStompFrame(String command);

   public abstract StompFrame decode(StompDecoder decoder, final HornetQBuffer buffer) throws HornetQStompException;

}
