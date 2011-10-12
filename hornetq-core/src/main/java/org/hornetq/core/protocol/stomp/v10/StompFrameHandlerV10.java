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
package org.hornetq.core.protocol.stomp.v10;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.protocol.stomp.FrameEventListener;
import org.hornetq.core.protocol.stomp.HornetQStompException;
import org.hornetq.core.protocol.stomp.Stomp;
import org.hornetq.core.protocol.stomp.StompConnection;
import org.hornetq.core.protocol.stomp.StompDecoder;
import org.hornetq.core.protocol.stomp.StompFrame;
import org.hornetq.core.protocol.stomp.StompSubscription;
import org.hornetq.core.protocol.stomp.StompUtils;
import org.hornetq.core.protocol.stomp.VersionedStompFrameHandler;
import org.hornetq.core.protocol.stomp.Stomp.Headers;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.utils.DataConstants;

/**
*
* @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
*/
public class StompFrameHandlerV10 extends VersionedStompFrameHandler implements FrameEventListener
{
   private static final Logger log = Logger.getLogger(StompFrameHandlerV10.class);
   
   public StompFrameHandlerV10(StompConnection connection)
   {
      this.connection = connection;
      connection.addStompEventListener(this);
   }

   @Override
   public StompFrame onConnect(StompFrame frame)
   {
      StompFrame response = null;
      Map<String, String> headers = frame.getHeadersMap();
      String login = (String)headers.get(Stomp.Headers.Connect.LOGIN);
      String passcode = (String)headers.get(Stomp.Headers.Connect.PASSCODE);
      String clientID = (String)headers.get(Stomp.Headers.Connect.CLIENT_ID);
      String requestID = (String)headers.get(Stomp.Headers.Connect.REQUEST_ID);

      if (connection.validateUser(login, passcode))
      {
         connection.setClientID(clientID);
         connection.setValid(true);
         
         response = new StompFrameV10(Stomp.Responses.CONNECTED);
         
         if (frame.hasHeader(Stomp.Headers.ACCEPT_VERSION))
         {
            response.addHeader(Stomp.Headers.Connected.VERSION, "1.0");
         }
         
         response.addHeader(Stomp.Headers.Connected.SESSION, connection.getID().toString());
         
         if (requestID != null)
         {
            response.addHeader(Stomp.Headers.Connected.RESPONSE_ID, requestID);
         }
      }
      else
      {
         //not valid
         response = new StompFrameV10(Stomp.Responses.ERROR);
         response.addHeader(Stomp.Headers.Error.MESSAGE, "Failed to connect");
         try
         {
            response.setBody("The login account is not valid.");
         }
         catch (UnsupportedEncodingException e)
         {
            log.error("Encoding problem", e);
         }
      }
      return response;
   }

   @Override
   public StompFrame onDisconnect(StompFrame frame)
   {
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
      String destination = request.getHeader(Stomp.Headers.Unsubscribe.DESTINATION);
      String id = request.getHeader(Stomp.Headers.Unsubscribe.ID);

      String subscriptionID = null;
      if (id != null)
      {
         subscriptionID = id;
      }
      else
      {
         if (destination == null)
         {
            response = new HornetQStompException("Must specify the subscription's id or " +
            		"the destination you are unsubscribing from").getFrame();
            return response;
         }
         subscriptionID = "subscription/" + destination;
      }
      
      try
      {
         connection.unsubscribe(subscriptionID);
      }
      catch (HornetQStompException e)
      {
         return e.getFrame();
      }
      return response;
   }

   @Override
   public StompFrame onAck(StompFrame request)
   {
      StompFrame response = null;
      
      String messageID = request.getHeader(Stomp.Headers.Ack.MESSAGE_ID);
      String txID = request.getHeader(Stomp.Headers.TRANSACTION);

      if (txID != null)
      {
         log.warn("Transactional acknowledgement is not supported");
      }
      
      try
      {
         connection.acknowledge(messageID, null);
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
      return onUnknown(request.getCommand());
   }

   @Override
   public StompFrame onNack(StompFrame request)
   {
      return onUnknown(request.getCommand());
   }

   @Override
   public StompFrame createMessageFrame(ServerMessage serverMessage,
         StompSubscription subscription, int deliveryCount) throws Exception
   {
      StompFrame frame = new StompFrame(Stomp.Responses.MESSAGE);
      
      if (subscription.getID() != null)
      {
         frame.addHeader(Stomp.Headers.Message.SUBSCRIPTION, subscription.getID());
      }

      synchronized(serverMessage)
      {

      HornetQBuffer buffer = serverMessage.getBodyBuffer();

      int bodyPos = serverMessage.getEndOfBodyPosition() == -1 ? buffer.writerIndex()
                                                              : serverMessage.getEndOfBodyPosition();
      int size = bodyPos - buffer.readerIndex();
      buffer.readerIndex(MessageImpl.BUFFER_HEADER_SPACE + DataConstants.SIZE_INT);
      byte[] data = new byte[size];
      
      if (serverMessage.containsProperty(Stomp.Headers.CONTENT_LENGTH) || serverMessage.getType() == Message.BYTES_TYPE)
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

      StompUtils.copyStandardHeadersFromMessageToFrame(serverMessage, frame, deliveryCount);
      }
      
      return frame;

   }

   @Override
   public StompFrame createStompFrame(String command)
   {
      return new StompFrameV10(command);
   }

   public StompFrame decode(StompDecoder decoder, final HornetQBuffer buffer) throws HornetQStompException
   {
      return decoder.defaultDecode(buffer);
   }

   @Override
   public void replySent(StompFrame reply)
   {
      if (reply.needsDisconnect())
      {
         connection.destroy();
      }
   }

   @Override
   public void requestAccepted(StompFrame request)
   {
      // TODO Auto-generated method stub
      
   }
   
   @Override
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

}
