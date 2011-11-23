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

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.protocol.stomp.Stomp.Headers;
import org.hornetq.core.protocol.stomp.v10.StompFrameHandlerV10;
import org.hornetq.core.protocol.stomp.v11.StompFrameHandlerV11;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.utils.DataConstants;

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
   public abstract StompFrame onAck(StompFrame request);
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

   public abstract StompFrame createStompFrame(String command);

   public abstract StompFrame decode(StompDecoder decoder, final HornetQBuffer buffer) throws HornetQStompException;
   
   public StompFrame onCommit(StompFrame request)
   {
      StompFrame response = null;
      
      String txID = request.getHeader(Stomp.Headers.TRANSACTION);
      if (txID == null)
      {
         response = new HornetQStompException("transaction header is mandatory to COMMIT a transaction").getFrame();
         return response;
      }

      try
      {
         connection.commitTransaction(txID);
      }
      catch (HornetQStompException e)
      {
         response = e.getFrame();
      }
      return response;
   }

   public StompFrame onSend(StompFrame frame)
   {      
      StompFrame response = null;
      try
      {
         connection.validate();
         String destination = frame.getHeader(Stomp.Headers.Send.DESTINATION);
         String txID = frame.getHeader(Stomp.Headers.TRANSACTION);

         long timestamp = System.currentTimeMillis();

         ServerMessageImpl message = connection.createServerMessage();
         message.setTimestamp(timestamp);
         message.setAddress(SimpleString.toSimpleString(destination));
         StompUtils.copyStandardHeadersFromFrameToMessage(frame, message);
         if (frame.hasHeader(Stomp.Headers.CONTENT_LENGTH))
         {
            message.setType(Message.BYTES_TYPE);
            message.getBodyBuffer().writeBytes(frame.getBodyAsBytes());
         }
         else
         {
            message.setType(Message.TEXT_TYPE);
            String text = frame.getBody();
            message.getBodyBuffer().writeNullableSimpleString(SimpleString.toSimpleString(text));
         }

         connection.sendServerMessage(message, txID);
      }
      catch (HornetQStompException e)
      {
         response = e.getFrame();
      }
      catch (Exception e)
      {
         response = new HornetQStompException("Error handling send", e).getFrame();
      }

      return response;
   }

   public StompFrame onBegin(StompFrame frame)
   {
      StompFrame response = null;
      String txID = frame.getHeader(Stomp.Headers.TRANSACTION);
      if (txID == null)
      {
         response = new HornetQStompException("Need a transaction id to begin").getFrame();
      }
      else
      {
         try
         {
            connection.beginTransaction(txID);
         }
         catch (HornetQStompException e)
         {
            response = e.getFrame();
         }
      }
      return response;
   }

   public StompFrame onAbort(StompFrame request)
   {
      StompFrame response = null;
      String txID = request.getHeader(Stomp.Headers.TRANSACTION);

      if (txID == null)
      {
         response = new HornetQStompException("transaction header is mandatory to ABORT a transaction").getFrame();
         return response;
      }
      
      try
      {
         connection.abortTransaction(txID);
      }
      catch (HornetQStompException e)
      {
         response = e.getFrame();
      }
      
      return response;
   }

   public StompFrame onSubscribe(StompFrame request)
   {
      StompFrame response = null;
      String destination = request.getHeader(Stomp.Headers.Subscribe.DESTINATION);
      
      String selector = request.getHeader(Stomp.Headers.Subscribe.SELECTOR);
      String ack = request.getHeader(Stomp.Headers.Subscribe.ACK_MODE);
      String id = request.getHeader(Stomp.Headers.Subscribe.ID);
      String durableSubscriptionName = request.getHeader(Stomp.Headers.Subscribe.DURABLE_SUBSCRIBER_NAME);
      boolean noLocal = false;
      
      if (request.hasHeader(Stomp.Headers.Subscribe.NO_LOCAL))
      {
         noLocal = Boolean.parseBoolean(request.getHeader(Stomp.Headers.Subscribe.NO_LOCAL));
      }
      
      try
      {
         connection.subscribe(destination, selector, ack, id, durableSubscriptionName, noLocal);
      }
      catch (HornetQStompException e)
      {
         response = e.getFrame();
      }
      
      return response;
   }

   public StompFrame postprocess(StompFrame request)
   {
      StompFrame response = null;
      if (request.hasHeader(Stomp.Headers.RECEIPT_REQUESTED))
      {
         response = handleReceipt(request.getHeader(Stomp.Headers.RECEIPT_REQUESTED));
         if (request.getCommand().equals(Stomp.Commands.DISCONNECT))
         {
            response.setNeedsDisconnect(true);
         }
      }
      else
      {
         //request null, disconnect if so.
         if (request.getCommand().equals(Stomp.Commands.DISCONNECT))
         {
            this.connection.disconnect();
         }         
      }
      return response;
   }

   public StompFrame createMessageFrame(ServerMessage serverMessage,
         StompSubscription subscription, int deliveryCount) throws Exception
   {
      StompFrame frame = createStompFrame(Stomp.Responses.MESSAGE);

      if (subscription.getID() != null)
      {
         frame.addHeader(Stomp.Headers.Message.SUBSCRIPTION,
               subscription.getID());
      }

      synchronized (serverMessage)
      {

         HornetQBuffer buffer = serverMessage.getBodyBuffer();

         int bodyPos = serverMessage.getEndOfBodyPosition() == -1 ? buffer
               .writerIndex() : serverMessage.getEndOfBodyPosition();
         int size = bodyPos - buffer.readerIndex();
         buffer.readerIndex(MessageImpl.BUFFER_HEADER_SPACE
               + DataConstants.SIZE_INT);
         byte[] data = new byte[size];

         if (serverMessage.containsProperty(Stomp.Headers.CONTENT_LENGTH)
               || serverMessage.getType() == Message.BYTES_TYPE)
         {
            frame.addHeader(Headers.CONTENT_LENGTH, String.valueOf(data.length));
            buffer.readBytes(data);
         }
         else
         {
            SimpleString text = buffer.readNullableSimpleString();
            if (text != null)
            {
               data = text.toString().getBytes("UTF-8");
            }
            else
            {
               data = new byte[0];
            }
         }
         frame.setByteBody(data);

         serverMessage.getBodyBuffer().resetReaderIndex();

         StompUtils.copyStandardHeadersFromMessageToFrame(serverMessage, frame,
               deliveryCount);
      }

      return frame;
   }

}
