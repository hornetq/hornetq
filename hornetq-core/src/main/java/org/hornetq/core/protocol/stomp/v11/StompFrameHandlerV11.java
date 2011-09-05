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
import org.hornetq.core.protocol.stomp.StompFrame;
import org.hornetq.core.protocol.stomp.StompSubscription;
import org.hornetq.core.protocol.stomp.StompUtils;
import org.hornetq.core.protocol.stomp.VersionedStompFrameHandler;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.utils.DataConstants;

/**
 * 
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 */
public class StompFrameHandlerV11 extends VersionedStompFrameHandler implements FrameEventListener
{
   private static final Logger log = Logger.getLogger(StompFrameHandlerV11.class);
   
   private HeartBeater heartBeater;

   public StompFrameHandlerV11(StompConnection connection)
   {
      this.connection = connection;
      connection.addStompEventListener(this);
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

      try
      {
         if (connection.validateUser(login, passcode))
         {
            connection.setClientID(clientID);
            connection.setValid(true);

            response = new StompFrame(Stomp.Responses.CONNECTED);

            // version
            response.addHeader(Stomp.Headers.Connected.VERSION,
                  connection.getVersion());

            // session
            response.addHeader(Stomp.Headers.Connected.SESSION, connection
                  .getID().toString());

            // server
            response.addHeader(Stomp.Headers.Connected.SERVER,
                  connection.getHornetQServerName());

            if (requestID != null)
            {
               response.addHeader(Stomp.Headers.Connected.RESPONSE_ID,
                     requestID);
            }

            // heart-beat. We need to start after connected frame has been sent.
            // otherwise the client may receive heart-beat before it receives
            // connected frame.
            String heartBeat = headers.get(Stomp.Headers.Connect.HEART_BEAT);

            if (heartBeat != null)
            {
               handleHeartBeat(heartBeat);
               response.addHeader(Stomp.Headers.Connected.HEART_BEAT, "20,100");
            }
         }
         else
         {
            // not valid
            response = new StompFrame(Stomp.Responses.ERROR, true);
            response.addHeader(Stomp.Headers.Error.VERSION, "1.0,1.1");

            response.setBody("Supported protocol versions are 1.0 and 1.1");
         }
      }
      catch (HornetQStompException e)
      {
         response = e.getFrame();
      }
      return response;
   }

   //ping parameters, hard-code for now
   //the server can support min 20 milliseconds and receive ping at 100 milliseconds (20,100)
   private void handleHeartBeat(String heartBeatHeader) throws HornetQStompException
   {
      String[] params = heartBeatHeader.split(",");
      if (params.length != 2)
      {
         throw new HornetQStompException("Incorrect heartbeat header " + heartBeatHeader);
      }
      
      //client ping
      long minPingInterval = Long.valueOf(params[0]);
      //client receive ping
      long minAcceptInterval = Long.valueOf(params[1]);
      
      if ((minPingInterval != 0) || (minAcceptInterval != 0))
      {
         heartBeater = new HeartBeater(minPingInterval, minAcceptInterval);
      }
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

   @Override
   public void replySent(StompFrame reply)
   {
      if (reply.getCommand().equals(Stomp.Responses.CONNECTED))
      {
         //kick off the pinger
         startHeartBeat();
      }
      if (reply.needsDisconnect())
      {
         connection.destroy();
      }
   }
   
   private void startHeartBeat()
   {
      if (heartBeater != null)
      {
         heartBeater.start();
      }
   }
   
   //server heart beat (20,100) (hard coded)
   //algorithm: 
   //(a) server ping: if server hasn't sent any frame within serverPing 
   //interval, send a ping. 
   //(b) accept ping: if server hasn't received any frame within
   // 2*serverAcceptPing, disconnect!
   private class HeartBeater extends Thread
   {
      long serverPing = 0;
      long serverAcceptPing = 0;
      long waitingTime = 0;
      volatile boolean shutdown = false;
      volatile long pings = 0;
      volatile long accepts = 0;

      public HeartBeater(long clientPing, long clientAcceptPing)
      {
         if (clientPing != 0)
         {
            serverAcceptPing = clientPing > 100 ? clientPing : 100;
         }
         
         if (clientAcceptPing != 0)
         {
            serverPing = clientAcceptPing > 20 ? clientAcceptPing : 20;
            if (serverAcceptPing != 0)
            {
               waitingTime = serverPing > serverAcceptPing ? serverAcceptPing : serverPing;
            }
            else
            {
               waitingTime = serverPing;
            }
         }
      }
      
      public void run()
      {
         long lastPing = 0;
         long lastAccepted = System.currentTimeMillis();
         
         synchronized (this)
         {
            while (!shutdown)
            {
               long dur1 = 0;
               long dur2 = 0;
               
               if (serverPing != 0)
               {
                  if (pings == 0)
                  {
                     dur1 = System.currentTimeMillis() - lastPing;
                     if (dur1 >= serverPing)
                     {
                        lastPing = System.currentTimeMillis();
                        connection.ping();
                        dur1 = 0;
                     }
                  }
                  else
                  {
                     dur1 = 5;
                     pings = 0;
                  }
               }

               if (serverAcceptPing != 0)
               {
                  if (accepts == 0)
                  {
                     dur2 = System.currentTimeMillis() - lastAccepted;
                     if (dur2 > (2 * serverAcceptPing))
                     {
                        connection.setValid(false);
                        shutdown = true;
                        break;
                     }
                  }
                  else
                  {
                     lastAccepted = System.currentTimeMillis();
                     accepts = 0;
                  }
               }
               
               long waitTime1 = serverPing - dur1;
               long waitTime2 = serverAcceptPing*2 - dur2;

               long waitTime = waitTime1 < waitTime2 ? waitTime1 : waitTime2;
               
               try
               {
                  this.wait(waitTime);
               }
               catch (InterruptedException e)
               {
               }
            }
         }
      }
   }

}
