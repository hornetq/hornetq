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
package org.hornetq.core.protocol.stomp.v11;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.protocol.stomp.HornetQStompException;
import org.hornetq.core.protocol.stomp.Stomp;
import org.hornetq.core.protocol.stomp.StompConnection;
import org.hornetq.core.protocol.stomp.StompFrame;
import org.hornetq.core.protocol.stomp.StompSubscription;
import org.hornetq.core.protocol.stomp.StompUtils;
import org.hornetq.core.protocol.stomp.VersionedStompFrameHandler;
import org.hornetq.core.protocol.stomp.Stomp.Headers;
import org.hornetq.core.protocol.stomp.v10.StompFrameHandlerV10;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.spi.core.security.HornetQSecurityManager;
import org.hornetq.utils.DataConstants;

public class StompFrameHandlerV11 extends VersionedStompFrameHandler
{
   private static final Logger log = Logger.getLogger(StompFrameHandlerV11.class);

   public StompFrameHandlerV11(StompConnection connection)
   {
      this.connection = connection;
   }

   @Override
   public StompFrame onConnect(StompFrame frame)
   {
      StompFrame response = null;
      Map<String, String> headers = frame.getHeadersMap();
      String login = headers.get(Stomp.Headers.Connect.LOGIN);
      String passcode = headers.get(Stomp.Headers.Connect.PASSCODE);
      String clientID = headers.get(Stomp.Headers.Connect.CLIENT_ID);
      String requestID = headers.get(Stomp.Headers.Connect.REQUEST_ID);

      if (connection.validateUser(login, passcode))
      {
         connection.setClientID(clientID);
         connection.setValid(true);
         
         response = new StompFrame(Stomp.Responses.CONNECTED);
         
         //version
         response.addHeader(Stomp.Headers.Connected.VERSION, connection.getVersion());
         
         //session
         response.addHeader(Stomp.Headers.Connected.SESSION, connection.getID().toString());
         
         //server
         response.addHeader(Stomp.Headers.Connected.SERVER, connection.getHornetQServerName());
         
         if (requestID != null)
         {
            response.addHeader(Stomp.Headers.Connected.RESPONSE_ID, requestID);
         }
      }
      else
      {
         //not valid
         response = new StompFrame(Stomp.Responses.ERROR);
         response.addHeader(Stomp.Headers.Error.VERSION, "1.0,1.1");
         
         response.setBody("Supported protocol versions are 1.0 and 1.1");
         
         connection.sendFrame(response);
         connection.destroy();
         
         return null;
      }
      return response;
   }

   @Override
   public StompFrame onDisconnect(StompFrame frame)
   {
      connection.destroy();
      return null;
   }

   @Override
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

   @Override
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

   @Override
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

   @Override
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

   @Override
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

   @Override
   public StompFrame onUnsubscribe(StompFrame request)
   {
      StompFrame response = null;
      //unsubscribe in 1.1 only needs id header
      String id = request.getHeader(Stomp.Headers.Unsubscribe.ID);

      String subscriptionID = null;
      if (id != null)
      {
         subscriptionID = id;
      }
      else
      {
          response = new HornetQStompException("Must specify the subscription's id").getFrame();
          return response;
      }
      
      try
      {
         connection.unsubscribe(subscriptionID);
      }
      catch (HornetQStompException e)
      {
         response = e.getFrame();
      }
      return response;
   }

   @Override
   public StompFrame onAck(StompFrame request)
   {
      StompFrame response = null;
      
      String messageID = request.getHeader(Stomp.Headers.Ack.MESSAGE_ID);
      String txID = request.getHeader(Stomp.Headers.TRANSACTION);
      String subscriptionID = request.getHeader(Stomp.Headers.Ack.SUBSCRIPTION);

      if (txID != null)
      {
         log.warn("Transactional acknowledgement is not supported");
      }
      
      if (subscriptionID == null)
      {
         response = new HornetQStompException("subscription header is required").getFrame();
         return response;
      }
      
      try
      {
         connection.acknowledge(messageID, subscriptionID);
      }
      catch (HornetQStompException e)
      {
         response = e.getFrame();
      }

      return response;
   }

   @Override
   public StompFrame onStomp(StompFrame request)
   {
      return onConnect(request);
   }

   @Override
   public StompFrame onNack(StompFrame request)
   {
      //this eventually means discard the message (it never be redelivered again).
      //we can consider supporting redeliver to a different sub.
      return onAck(request);
   }

   @Override
   public StompFrame createMessageFrame(ServerMessage serverMessage,
         StompSubscription subscription, int deliveryCount)
         throws Exception
   {
      StompFrame frame = new StompFrame(Stomp.Responses.MESSAGE);
      
      if (subscription.getID() != null)
      {
         frame.addHeader(Stomp.Headers.Message.SUBSCRIPTION, subscription.getID());
      }
      
      HornetQBuffer buffer = serverMessage.getBodyBuffer();

      int bodyPos = serverMessage.getEndOfBodyPosition() == -1 ? buffer.writerIndex()
                                                              : serverMessage.getEndOfBodyPosition();
      int size = bodyPos - buffer.readerIndex();
      buffer.readerIndex(MessageImpl.BUFFER_HEADER_SPACE + DataConstants.SIZE_INT);
      byte[] data = new byte[size];
      if (serverMessage.containsProperty(Stomp.Headers.CONTENT_LENGTH) || serverMessage.getType() == Message.BYTES_TYPE)
      {
         frame.addHeader(Stomp.Headers.CONTENT_LENGTH, String.valueOf(data.length));
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
      frame.addHeader(Stomp.Headers.CONTENT_TYPE, "text/plain");
      
      serverMessage.getBodyBuffer().resetReaderIndex();

      StompUtils.copyStandardHeadersFromMessageToFrame(serverMessage, frame, deliveryCount);
      
      return frame;

   }

}
