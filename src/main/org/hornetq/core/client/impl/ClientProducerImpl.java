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

import static org.hornetq.utils.SimpleString.toSimpleString;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.hornetq.core.buffers.ChannelBuffers;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.Message;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.remoting.Channel;
import org.hornetq.core.remoting.impl.wireformat.SessionSendContinuationMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionSendLargeMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionSendMessage;
import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.utils.SimpleString;
import org.hornetq.utils.TokenBucketLimiter;
import org.hornetq.utils.UUIDGenerator;

/**
 * The client-side Producer connectionFactory class.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * @version <tt>$Revision$</tt> $Id$
 */
public class ClientProducerImpl implements ClientProducerInternal
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ClientProducerImpl.class);

   // Attributes -----------------------------------------------------------------------------------

   private final boolean trace = log.isTraceEnabled();

   private final SimpleString address;

   private final ClientSessionInternal session;

   private final Channel channel;

   private volatile boolean closed;

   // For rate throttling

   private final TokenBucketLimiter rateLimiter;

   private final boolean blockOnNonPersistentSend;

   private final boolean blockOnPersistentSend;

   private final SimpleString groupID;

   private final int minLargeMessageSize;

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public ClientProducerImpl(final ClientSessionInternal session,
                             final SimpleString address,
                             final TokenBucketLimiter rateLimiter,
                             final boolean blockOnNonPersistentSend,
                             final boolean blockOnPersistentSend,
                             final boolean autoGroup,
                             final int minLargeMessageSize,
                             final Channel channel)
   {
      this.channel = channel;

      this.session = session;

      this.address = address;

      this.rateLimiter = rateLimiter;

      this.blockOnNonPersistentSend = blockOnNonPersistentSend;

      this.blockOnPersistentSend = blockOnPersistentSend;

      if (autoGroup)
      {
         this.groupID = UUIDGenerator.getInstance().generateSimpleStringUUID();
      }
      else
      {
         this.groupID = null;
      }

      this.minLargeMessageSize = minLargeMessageSize;
   }

   // ClientProducer implementation ----------------------------------------------------------------

   public SimpleString getAddress()
   {
      return address;
   }

   public void send(final Message msg) throws HornetQException
   {
      checkClosed();

      doSend(null, msg);
   }

   public void send(final SimpleString address, final Message msg) throws HornetQException
   {
      checkClosed();

      doSend(address, msg);
   }

   public void send(String address, Message message) throws HornetQException
   {
      send(toSimpleString(address), message);
   }

   public synchronized void close() throws HornetQException
   {
      if (closed)
      {
         return;
      }

      doCleanup();
   }

   public void cleanUp()
   {
      if (closed)
      {
         return;
      }

      doCleanup();
   }

   public boolean isClosed()
   {
      return closed;
   }

   public boolean isBlockOnPersistentSend()
   {
      return blockOnPersistentSend;
   }

   public boolean isBlockOnNonPersistentSend()
   {
      return blockOnNonPersistentSend;
   }

   public int getMaxRate()
   {
      return rateLimiter == null ? -1 : rateLimiter.getRate();
   }

   // Public ---------------------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Package Private ------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   private void doCleanup()
   {
      session.removeProducer(this);

      closed = true;
   }

   private void doSend(final SimpleString address, final Message msg) throws HornetQException
   {
      if (address != null)
      {
         msg.setDestination(address);
      }
      else
      {
         msg.setDestination(this.address);
      }

      if (rateLimiter != null)
      {
         // Rate flow control

         rateLimiter.limit();
      }

      if (groupID != null)
      {
         msg.putStringProperty(MessageImpl.HDR_GROUP_ID, groupID);
      }

      boolean sendBlocking = msg.isDurable() ? blockOnPersistentSend : blockOnNonPersistentSend;

      SessionSendMessage message = new SessionSendMessage(msg, sendBlocking);
      
      session.workDone();

      if (msg.getBodyInputStream() != null || msg.getEncodeSize() >= minLargeMessageSize || msg.isLargeMessage())
      {
         sendMessageInChunks(sendBlocking, msg);
      }
      else if (sendBlocking)
      {
         //log.info("sending blocking");
         channel.sendBlocking(message);
      }
      else
      {
         channel.send(message);
      }
   }

   /**
    * @param msg
    * @throws HornetQException
    */
   private void sendMessageInChunks(final boolean sendBlocking, final Message msg) throws HornetQException
   {
      int headerSize = msg.getPropertiesEncodeSize();

      if (headerSize >= minLargeMessageSize)
      {
         throw new HornetQException(HornetQException.ILLEGAL_STATE, "Header size (" + headerSize +
                                                                    ") is too big, use the messageBody for large data, or increase minLargeMessageSize");
      }

      // msg.getBody() could be Null on LargeServerMessage
      if (msg.getBodyInputStream() == null && msg.getBody() != null)
      {
         msg.getBody().readerIndex(0);
      }

      HornetQBuffer headerBuffer = ChannelBuffers.buffer(headerSize);
      msg.encodeProperties(headerBuffer);

      SessionSendLargeMessage initialChunk = new SessionSendLargeMessage(headerBuffer.array());

      channel.send(initialChunk);

      InputStream input = msg.getBodyInputStream();
      
      if (input != null)
      {
         boolean lastChunk = false;

         while (!lastChunk)
         {
            byte[] buff = new byte[minLargeMessageSize];
            
            int pos = 0;
                                   
            do
            {               
               int numberOfBytesRead;
               
               int wanted = minLargeMessageSize - pos;
               
               try
               {
                  numberOfBytesRead = input.read(buff, pos, wanted);
               }
               catch (IOException e)
               {
                  throw new HornetQException(HornetQException.LARGE_MESSAGE_ERROR_BODY,
                                             "Error reading the LargeMessageBody",
                                             e);
               }
               
               if (numberOfBytesRead == -1)
               {                  
                  lastChunk = true;
                  
                  break;
               }
                                             
               pos += numberOfBytesRead;
            }
            while (pos < minLargeMessageSize);
                        
            if (lastChunk)
            {
               byte[] buff2 = new byte[pos];
               
               System.arraycopy(buff, 0, buff2, 0, pos);
               
               buff = buff2;
            }
            
            final SessionSendContinuationMessage chunk = new SessionSendContinuationMessage(buff,                                                                                           
                                                                                            !lastChunk,
                                                                                            lastChunk && sendBlocking);

            if (sendBlocking && lastChunk)
            {
               // When sending it blocking, only the last chunk will be blocking.
               channel.sendBlocking(chunk);
            }
            else
            {
               channel.send(chunk);
            }
         }

         try
         {
            input.close();
         }
         catch (IOException e)
         {
            throw new HornetQException(HornetQException.LARGE_MESSAGE_ERROR_BODY,
                                       "Error closing stream from LargeMessageBody",
                                       e);
         }
      }
      else
      {
         final long bodySize = msg.getLargeBodySize();

         for (int pos = 0; pos < bodySize;)
         {
            final boolean lastChunk;

            final int chunkLength = Math.min((int)(bodySize - pos), minLargeMessageSize);

            final HornetQBuffer bodyBuffer = ChannelBuffers.buffer(chunkLength);

            msg.encodeBody(bodyBuffer, pos, chunkLength);

            pos += chunkLength;

            lastChunk = pos >= bodySize;

            final SessionSendContinuationMessage chunk = new SessionSendContinuationMessage(bodyBuffer.array(),                                                                                            
                                                                                            !lastChunk,
                                                                                            lastChunk && sendBlocking);

            if (sendBlocking && lastChunk)
            {
               // When sending it blocking, only the last chunk will be blocking.
               channel.sendBlocking(chunk);
            }
            else
            {
               channel.send(chunk);
            }
         }
      }
   }

   private void checkClosed() throws HornetQException
   {
      if (closed)
      {
         throw new HornetQException(HornetQException.OBJECT_CLOSED, "Producer is closed");
      }
   }

   // Inner Classes --------------------------------------------------------------------------------

}
