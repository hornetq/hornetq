/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.messaging.core.client.impl;

import static org.jboss.messaging.utils.SimpleString.toSimpleString;

import org.jboss.messaging.core.buffers.ChannelBuffers;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionSendContinuationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionSendMessage;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.utils.SimpleString;
import org.jboss.messaging.utils.TokenBucketLimiter;
import org.jboss.messaging.utils.UUIDGenerator;

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

   public void send(final Message msg) throws MessagingException
   {
      checkClosed();

      doSend(null, msg);
   }

   public void send(final SimpleString address, final Message msg) throws MessagingException
   {
      checkClosed();

      doSend(address, msg);
   }
   
   public void send(String address, Message message) throws MessagingException
   {
      send(toSimpleString(address), message);
   }

   public synchronized void close() throws MessagingException
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

   private void doSend(final SimpleString address, final Message msg) throws MessagingException
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

      if (msg.getEncodeSize() >= minLargeMessageSize)
      {
         sendMessageInChunks(sendBlocking, (ClientMessageInternal)msg);
      }
      else if (sendBlocking)
      {
         channel.sendBlocking(message);
      }
      else
      {
         channel.send(message);
      }
   }

   /**
    * @param msg
    * @throws MessagingException
    */
   private void sendMessageInChunks(final boolean sendBlocking, final ClientMessageInternal msg) throws MessagingException
   {
      int headerSize = msg.getPropertiesEncodeSize();

      if (headerSize >= minLargeMessageSize)
      {
         throw new MessagingException(MessagingException.ILLEGAL_STATE,
                                      "Header size (" + headerSize + ") is too big, use the messageBody for large data, or increase minLargeMessageSize");
      }

      MessagingBuffer headerBuffer = ChannelBuffers.buffer(headerSize); 
      msg.encodeProperties(headerBuffer);

      final int bodySize = msg.getBodySize();

      SessionSendMessage initialChunk = new SessionSendMessage(headerBuffer.array(), false);

      channel.send(initialChunk);

      for (int pos = 0; pos < bodySize;)
      {
         final boolean lastChunk;
                  
         final int chunkLength = Math.min(bodySize - pos, minLargeMessageSize); 
         
         final MessagingBuffer bodyBuffer = ChannelBuffers.buffer(chunkLength); 

         msg.encodeBody(bodyBuffer, pos, chunkLength);

         pos += chunkLength;
         
         lastChunk = pos >= bodySize;

         final SessionSendContinuationMessage chunk = new SessionSendContinuationMessage(bodyBuffer.array(), !lastChunk, lastChunk && sendBlocking);

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

      // Note: This could be either a regular message, with a huge body,
      //       or a ClientFileMessage.
      if (msg.isFileMessage())
      {
         try
         {
            ((ClientFileMessageInternal)msg).closeChannel();
         }
         catch (Exception e)
         {
            log.warn(e.getMessage(), e);
         }
      }
   }

   private void checkClosed() throws MessagingException
   {
      if (closed)
      {
         throw new MessagingException(MessagingException.OBJECT_CLOSED, "Producer is closed");
      }
   }

   // Inner Classes --------------------------------------------------------------------------------

}
