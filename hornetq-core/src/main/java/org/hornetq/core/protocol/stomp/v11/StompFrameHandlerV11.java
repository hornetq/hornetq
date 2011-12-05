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
import java.util.concurrent.atomic.AtomicLong;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.protocol.stomp.FrameEventListener;
import org.hornetq.core.protocol.stomp.HornetQStompException;
import org.hornetq.core.protocol.stomp.SimpleBytes;
import org.hornetq.core.protocol.stomp.Stomp;
import org.hornetq.core.protocol.stomp.StompConnection;
import org.hornetq.core.protocol.stomp.StompDecoder;
import org.hornetq.core.protocol.stomp.StompFrame;
import org.hornetq.core.protocol.stomp.VersionedStompFrameHandler;

/**
 * 
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 */
public class StompFrameHandlerV11 extends VersionedStompFrameHandler implements FrameEventListener
{
   private static final Logger log = Logger.getLogger(StompFrameHandlerV11.class);
   
   private static final char ESC_CHAR = '\\';
   
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

            response = new StompFrameV11(Stomp.Responses.CONNECTED);

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
               if (heartBeater == null)
               {
                  response.addHeader(Stomp.Headers.Connected.HEART_BEAT, "0,0");
               }
               else
               {
                  response.addHeader(Stomp.Headers.Connected.HEART_BEAT, heartBeater.getServerHeartBeatValue());
               }
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
      catch (UnsupportedEncodingException e)
      {
         response = new HornetQStompException("Encoding error.", e).getFrame();
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
      if (this.heartBeater != null)
      {
         heartBeater.shutdown();
         try
         {
            heartBeater.join();
         }
         catch (InterruptedException e)
         {
            log.warn("Interrupted while waiting for heart beater to die", e);
         }
      }
      return null;
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
      if (!connection.isValid())
      {
         return onConnect(request);
      }
      return null;
   }

   @Override
   public StompFrame onNack(StompFrame request)
   {
      //this eventually means discard the message (it never be redelivered again).
      //we can consider supporting redeliver to a different sub.
      return onAck(request);
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
         connection.disconnect();
      }
      else
      {
         //update ping
         if (heartBeater != null)
         {
            heartBeater.pinged();
         }
      }
   }
   
   private void startHeartBeat()
   {
      if (heartBeater != null)
      {
         heartBeater.start();
      }
   }
   
   public StompFrame createPingFrame() throws UnsupportedEncodingException
   {
      StompFrame frame = new StompFrame(Stomp.Commands.STOMP);
      frame.setPing(true);
      return frame;
   }
   
   //server heart beat 
   //algorithm: 
   //(a) server ping: if server hasn't sent any frame within serverPing 
   //interval, send a ping. 
   //(b) accept ping: if server hasn't received any frame within
   // 2*serverAcceptPing, disconnect!
   private class HeartBeater extends Thread
   {
      final int MIN_SERVER_PING = 500;
      final int MIN_CLIENT_PING = 500;
      
      long serverPing = 0;
      long serverAcceptPing = 0;
      volatile boolean shutdown = false;
      AtomicLong lastPingTime = new AtomicLong(0);
      AtomicLong lastAccepted = new AtomicLong(0);
      StompFrame pingFrame;

      public HeartBeater(long clientPing, long clientAcceptPing)
      {
         if (clientPing != 0)
         {
            serverAcceptPing = clientPing > MIN_CLIENT_PING ? clientPing : MIN_CLIENT_PING;
         }
         
         if (clientAcceptPing != 0)
         {
            serverPing = clientAcceptPing > MIN_SERVER_PING ? clientAcceptPing : MIN_SERVER_PING;
         }
      }
      
      public synchronized void shutdown()
      {
         shutdown = true;
         this.notify();
      }

      public String getServerHeartBeatValue()
      {
         return String.valueOf(serverPing) + "," + String.valueOf(serverAcceptPing);
      }

      public void pinged()
      {
         lastPingTime.set(System.currentTimeMillis());
      }

      @Override
      public void run()
      {
         lastAccepted.set(System.currentTimeMillis());
         try
         {
            pingFrame = createPingFrame();
         }
         catch (UnsupportedEncodingException e1)
         {
            log.error("Cannot create ping frame due to encoding problem.", e1);
         }
         
         synchronized (this)
         {
            while (!shutdown)
            {
               long dur1 = 0;
               long dur2 = 0;
               
               if (serverPing != 0)
               {
                  dur1 = System.currentTimeMillis() - lastPingTime.get();
                  if (dur1 >= serverPing)
                  {
                     lastPingTime.set(System.currentTimeMillis());
                     connection.ping(pingFrame);
                     dur1 = 0;
                  }
               }

               if (serverAcceptPing != 0)
               {
                  dur2 = System.currentTimeMillis() - lastAccepted.get();
                  
                  if (dur2 > (2 * serverAcceptPing))
                  {
                     connection.disconnect();
                     shutdown = true;
                     break;
                  }
               }
               
               long waitTime1 = 0;
               long waitTime2 = 0;
               
               if (serverPing > 0)
               {
                  waitTime1 = serverPing - dur1;
               }
               
               if (serverAcceptPing > 0)
               {
                  waitTime2 = serverAcceptPing * 2 - dur2;
               }
               
               long waitTime = 10l;
               
               if ((waitTime1 > 0) && (waitTime1 > 0))
               {
                  waitTime = waitTime1 < waitTime2 ? waitTime1 : waitTime2;
               }
               else if (waitTime1 > 0)
               {
                  waitTime = waitTime1;
               }
               else if (waitTime2 > 0)
               {
                  waitTime = waitTime2;
               }
               
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

      public void pingAccepted()
      {
         this.lastAccepted.set(System.currentTimeMillis());
      }
   }

   @Override
   public void requestAccepted(StompFrame request)
   {
      if (heartBeater != null)
      {
         heartBeater.pingAccepted();
      }
   }

   @Override
   public StompFrame createStompFrame(String command)
   {
      return new StompFrameV11(command);
   }
   
   //all frame except CONNECT are decoded here.
   @Override
   public StompFrame decode(StompDecoder decoder, final HornetQBuffer buffer) throws HornetQStompException
   {
      int readable = buffer.readableBytes();

      if (decoder.data + readable >= decoder.workingBuffer.length)
      {
         decoder.resizeWorking(decoder.data + readable);
      }

      buffer.readBytes(decoder.workingBuffer, decoder.data, readable);

      decoder.data += readable;
      
      if (decoder.command == null)
      {
         int offset = 0;
         
         //check for ping
         while (decoder.workingBuffer[offset] == StompDecoder.NEW_LINE)
         {
            if (heartBeater != null)
            {
               //client ping
               heartBeater.pingAccepted();
            }
            // Yuck, some badly behaved STOMP clients add a \n *after* the terminating NUL char at the end of the
            // STOMP
            // frame this can manifest as an extra \n at the beginning when the next STOMP frame is read - we need to
            // deal
            // with this
            offset++;
            if (offset >= decoder.data)
            {
               decoder.data = 0;
               return null;
            }
         }
            
         if (decoder.data < 4)
         {
            // Need at least four bytes to identify the command
            // - up to 3 bytes for the command name + potentially another byte for a leading \n
            return null;
         }

         byte b = decoder.workingBuffer[offset];

         switch (b)
         {
            case StompDecoder.A:
            {
               if (decoder.workingBuffer[offset + 1] == StompDecoder.B)
               {
                  if (!decoder.tryIncrement(offset + StompDecoder.COMMAND_ABORT_LENGTH + 1))
                  {
                     return null;
                  }

                  // ABORT
                  decoder.command = StompDecoder.COMMAND_ABORT;
               }
               else
               {
                  if (!decoder.tryIncrement(offset + StompDecoder.COMMAND_ACK_LENGTH + 1))
                  {
                     return null;
                  }

                  // ACK
                  decoder.command = StompDecoder.COMMAND_ACK;
               }
               break;
            }
            case StompDecoder.B:
            {
               if (!decoder.tryIncrement(offset + StompDecoder.COMMAND_BEGIN_LENGTH + 1))
               {
                  return null;
               }

               // BEGIN
               decoder.command = StompDecoder.COMMAND_BEGIN;

               break;
            }
            case StompDecoder.C:
            {
               if (decoder.workingBuffer[offset + 2] == StompDecoder.M)
               {
                  if (!decoder.tryIncrement(offset + StompDecoder.COMMAND_COMMIT_LENGTH + 1))
                  {
                     return null;
                  }

                  // COMMIT
                  decoder.command = StompDecoder.COMMAND_COMMIT;
               }
               /**** added by meddy, 27 april 2011, handle header parser for reply to websocket protocol ****/
               else if (decoder.workingBuffer[offset+7] == StompDecoder.E) 
               {
                  if (!decoder.tryIncrement(offset + StompDecoder.COMMAND_CONNECTED_LENGTH + 1))
                  {
                     return null;
                  }

                  // CONNECTED
                  decoder.command = StompDecoder.COMMAND_CONNECTED;                  
               }
               /**** end ****/
               else
               {
                  if (!decoder.tryIncrement(offset + StompDecoder.COMMAND_CONNECT_LENGTH + 1))
                  {
                     return null;
                  }

                  // CONNECT
                  decoder.command = StompDecoder.COMMAND_CONNECT;
               }
               break;
            }
            case StompDecoder.D:
            {
               if (!decoder.tryIncrement(offset + StompDecoder.COMMAND_DISCONNECT_LENGTH + 1))
               {
                  return null;
               }

               // DISCONNECT
               decoder.command = StompDecoder.COMMAND_DISCONNECT;

               break;
            }
            case StompDecoder.R:
            {
               if (!decoder.tryIncrement(offset + StompDecoder.COMMAND_RECEIPT_LENGTH + 1))
               {
                  return null;
               }

               // RECEIPT
               decoder.command = StompDecoder.COMMAND_RECEIPT;

               break;
            }
            /**** added by meddy, 27 april 2011, handle header parser for reply to websocket protocol ****/
            case StompDecoder.E:
            {
               if (!decoder.tryIncrement(offset + StompDecoder.COMMAND_ERROR_LENGTH + 1))
               {
                  return null;
               }

               // ERROR
               decoder.command = StompDecoder.COMMAND_ERROR;

               break;
            }
            case StompDecoder.M:
            {
               if (!decoder.tryIncrement(offset + StompDecoder.COMMAND_MESSAGE_LENGTH + 1))
               {
                  return null;
               }

               // MESSAGE
               decoder.command = StompDecoder.COMMAND_MESSAGE;

               break;
            }
            /**** end ****/
            case StompDecoder.S:
            {
               if (decoder.workingBuffer[offset + 1] == StompDecoder.E)
               {
                  if (!decoder.tryIncrement(offset + StompDecoder.COMMAND_SEND_LENGTH + 1))
                  {
                     return null;
                  }

                  // SEND
                  decoder.command = StompDecoder.COMMAND_SEND;
               }
               else if (decoder.workingBuffer[offset + 1] == StompDecoder.U)
               {
                  if (!decoder.tryIncrement(offset + StompDecoder.COMMAND_SUBSCRIBE_LENGTH + 1))
                  {
                     return null;
                  }

                  // SUBSCRIBE
                  decoder.command = StompDecoder.COMMAND_SUBSCRIBE;
               }
               else
               {
                  if (!decoder.tryIncrement(offset + StompDecoder.COMMAND_STOMP_LENGTH + 1))
                  {
                     return null;
                  }

                  // SUBSCRIBE
                  decoder.command = StompDecoder.COMMAND_STOMP;
               }
               break;
            }
            case StompDecoder.U:
            {
               if (!decoder.tryIncrement(offset + StompDecoder.COMMAND_UNSUBSCRIBE_LENGTH + 1))
               {
                  return null;
               }

               // UNSUBSCRIBE
               decoder.command = StompDecoder.COMMAND_UNSUBSCRIBE;

               break;
            }
            case StompDecoder.N:
            {
               if (!decoder.tryIncrement(offset + StompDecoder.COMMAND_NACK_LENGTH + 1))
               {
                  return null;
               }
               //NACK
               decoder.command = StompDecoder.COMMAND_NACK;
               break;
            }
            default:
            {
               decoder.throwInvalid();
            }
         }

         // Sanity check

         if (decoder.workingBuffer[decoder.pos - 1] != StompDecoder.NEW_LINE)
         {
            decoder.throwInvalid();
         }
      }

      if (decoder.readingHeaders)
      {
         if (decoder.headerBytesCopyStart == -1)
         {
            decoder.headerBytesCopyStart = decoder.pos;
         }

         // Now the headers

         boolean isEscaping = false;
         SimpleBytes holder = new SimpleBytes(1024);      
         
         outer: while (true)
         {
            byte b = decoder.workingBuffer[decoder.pos++];

            switch (b)
            {
               //escaping
               case ESC_CHAR:
               {
                  if (isEscaping)
                  {
                     //this is a backslash
                     holder.append(b);
                     isEscaping = false;
                  }
                  else
                  {
                     //begin escaping
                     isEscaping = true;
                  }
                  break;
               }
               case StompDecoder.HEADER_SEPARATOR:
               {
                  if (isEscaping)
                  {
                     //a colon
                     holder.append(b);
                     isEscaping = false;
                  }
                  else
                  {
                     if (decoder.inHeaderName)
                     {
                        try
                        {
                           decoder.headerName = holder.getString();
                        }
                        catch (UnsupportedEncodingException e)
                        {
                           throw new HornetQStompException("Encoding exception", e);
                        }
                        
                        holder.reset();

                        decoder.inHeaderName = false;

                        decoder.headerBytesCopyStart = decoder.pos;

                        decoder.headerValueWhitespace = true;
                     }
                  }

                  decoder.whiteSpaceOnly = false;

                  break;
               }
               case StompDecoder.LN:
               {
                  if (isEscaping)
                  {
                     holder.append(StompDecoder.NEW_LINE);
                     isEscaping = false;
                  }
                  else
                  {
                     holder.append(b);
                  }
                  break;
               }
               case StompDecoder.NEW_LINE:
               {
                  if (decoder.whiteSpaceOnly)
                  {
                     // Headers are terminated by a blank line
                     decoder.readingHeaders = false;

                     break outer;
                  }

                  String headerValue;
                  try
                  {
                     headerValue = holder.getString();
                  }
                  catch (UnsupportedEncodingException e)
                  {
                     throw new HornetQStompException("Encoding exception.", e);
                  }
                  holder.reset();
                  
                  decoder.headers.put(decoder.headerName, headerValue);

                  if (decoder.headerName.equals(StompDecoder.CONTENT_LENGTH_HEADER_NAME))
                  {
                     decoder.contentLength = Integer.parseInt(headerValue);
                  }
                  
                  if (decoder.headerName.equals(StompDecoder.CONTENT_TYPE_HEADER_NAME))
                  {
                     decoder.contentType = headerValue;
                  }

                  decoder.whiteSpaceOnly = true;

                  decoder.headerBytesCopyStart = decoder.pos;

                  decoder.inHeaderName = true;

                  decoder.headerValueWhitespace = false;

                  break;
               }
               default:
               {
                  decoder.whiteSpaceOnly = false;

                  decoder.headerValueWhitespace = false;
                  
                  holder.append(b);
               }
            }
            if (decoder.pos == decoder.data)
            {
               // Run out of data

               return null;
            }
         }
      }

      // Now the body
      
      byte[] content = null;

      if (decoder.contentLength != -1)
      {
         if (decoder.pos + decoder.contentLength + 1 > decoder.data)
         {
            // Need more bytes
         }
         else
         {
            content = new byte[decoder.contentLength];

            System.arraycopy(decoder.workingBuffer, decoder.pos, content, 0, decoder.contentLength);

            decoder.pos += decoder.contentLength;
            
            //drain all the rest
            if (decoder.bodyStart == -1)
            {
               decoder.bodyStart = decoder.pos;
            }

            while (decoder.pos < decoder.data)
            {
               if (decoder.workingBuffer[decoder.pos++] == 0)
               {
                  break;
               }
            }
         }
      }
      else
      {
         // Need to scan for terminating NUL

         if (decoder.bodyStart == -1)
         {
            decoder.bodyStart = decoder.pos;
         }

         while (decoder.pos < decoder.data)
         {
            if (decoder.workingBuffer[decoder.pos++] == 0)
            {
               content = new byte[decoder.pos - decoder.bodyStart - 1];

               System.arraycopy(decoder.workingBuffer, decoder.bodyStart, content, 0, content.length);

               break;
            }
         }
      }
      
      if (content != null)
      {
         if (decoder.data > decoder.pos)
         {
            if (decoder.workingBuffer[decoder.pos] == StompDecoder.NEW_LINE) decoder.pos++;

            if (decoder.data > decoder.pos)
              // More data still in the buffer from the next packet
              System.arraycopy(decoder.workingBuffer, decoder.pos, decoder.workingBuffer, 0, decoder.data - decoder.pos);
         }

         decoder.data = decoder.data - decoder.pos;

         // reset

         StompFrame ret = new StompFrameV11(decoder.command, decoder.headers, content);

         decoder.init();

         return ret;
      }
      else
      {
         return null;
      }
   }
   
}
