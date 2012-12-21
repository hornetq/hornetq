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

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicLong;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQInterruptedException;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.BodyEncoder;
import org.hornetq.core.message.impl.MessageInternal;
import org.hornetq.core.protocol.core.Channel;
import org.hornetq.core.protocol.core.impl.wireformat.SessionSendContinuationMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionSendLargeMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionSendMessage;
import org.hornetq.utils.DeflaterReader;
import org.hornetq.utils.HornetQBufferInputStream;
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

   private final boolean trace = ClientProducerImpl.log.isTraceEnabled();

   private final SimpleString address;

   private final ClientSessionInternal session;

   private final Channel channel;

   private volatile boolean closed;

   // For rate throttling

   private final TokenBucketLimiter rateLimiter;

   private final boolean blockOnNonDurableSend;

   private final boolean blockOnDurableSend;

   private final SimpleString groupID;

   private final int minLargeMessageSize;

   private final ClientProducerCredits credits;

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public ClientProducerImpl(final ClientSessionInternal session,
                             final SimpleString address,
                             final TokenBucketLimiter rateLimiter,
                             final boolean blockOnNonDurableSend,
                             final boolean blockOnDurableSend,
                             final boolean autoGroup,
                             final SimpleString groupID,
                             final int minLargeMessageSize,
                             final Channel channel)
   {
      this.channel = channel;

      this.session = session;

      this.address = address;

      this.rateLimiter = rateLimiter;

      this.blockOnNonDurableSend = blockOnNonDurableSend;

      this.blockOnDurableSend = blockOnDurableSend;

      if (autoGroup)
      {
         this.groupID = UUIDGenerator.getInstance().generateSimpleStringUUID();
      }
      else
      {
         this.groupID = groupID;
      }

      this.minLargeMessageSize = minLargeMessageSize;

      if (address != null)
      {
         credits = session.getCredits(address, false);
      }
      else
      {
         credits = null;
      }
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

   public void send(final String address, final Message message) throws HornetQException
   {
      send(SimpleString.toSimpleString(address), message);
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

   public boolean isBlockOnDurableSend()
   {
      return blockOnDurableSend;
   }

   public boolean isBlockOnNonDurableSend()
   {
      return blockOnNonDurableSend;
   }

   public int getMaxRate()
   {
      return rateLimiter == null ? -1 : rateLimiter.getRate();
   }

   // Public ---------------------------------------------------------------------------------------

   public ClientProducerCredits getProducerCredits()
   {
      return credits;
   }

   // Protected ------------------------------------------------------------------------------------

   // Package Private ------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   private void doCleanup()
   {
      if (address != null)
      {
         session.returnCredits(address);
      }

      session.removeProducer(this);

      closed = true;
   }

   private void doSend(final SimpleString address, final Message msg) throws HornetQException
   {
      MessageInternal msgI = (MessageInternal)msg;

      ClientProducerCredits theCredits;

      boolean isLarge;

      // a note about the second check on the writerIndexSize,
      // If it's a server's message, it means this is being done through the bridge or some special consumer on the
      // server's on which case we can't' convert the message into large at the servers
      if (msgI.getBodyInputStream() != null || msgI.isLargeMessage() ||
         msgI.getBodyBuffer().writerIndex() > minLargeMessageSize && ! msgI.isServerMessage())
      {
         isLarge = true;
      }
      else
      {
         isLarge = false;
      }

      if (address != null)
      {
         if (!isLarge)
         {
            session.setAddress(msg, address);
         }
         else
         {
            msg.setAddress(address);
         }

         // Anonymous
         theCredits = session.getCredits(address, true);
      }
      else
      {
         if (!isLarge)
         {
            session.setAddress(msg, this.address);
         }
         else
         {
            msg.setAddress(this.address);
         }

         theCredits = credits;
      }

      if (rateLimiter != null)
      {
         // Rate flow control

         rateLimiter.limit();
      }

      if (groupID != null)
      {
         msgI.putStringProperty(Message.HDR_GROUP_ID, groupID);
      }

      boolean sendBlocking = msgI.isDurable() ? blockOnDurableSend : blockOnNonDurableSend;

      session.workDone();

      if (isLarge)
      {
         largeMessageSend(sendBlocking, msgI, theCredits);
      }
      else
      {
         SessionSendMessage packet = new SessionSendMessage(msgI, sendBlocking);

         if (sendBlocking)
         {
            channel.sendBlocking(packet);
         }
         else
         {
            channel.sendBatched(packet);
         }
      }

      try
      {
         // This will block if credits are not available

         // Note, that for a large message, the encode size only includes the properties + headers
         // Not the continuations, but this is ok since we are only interested in limiting the amount of
         // data in *memory* and continuations go straight to the disk

         if (!isLarge)
         {
            theCredits.acquireCredits(msgI.getEncodeSize());
         }
      }
      catch (InterruptedException e)
      {
         throw new HornetQInterruptedException(e);
      }
   }

   private void checkClosed() throws HornetQException
   {
      if (closed)
      {
         throw new HornetQException(HornetQException.OBJECT_CLOSED, "Producer is closed");
      }
   }

   // Methods to send Large Messages----------------------------------------------------------------

   /**
    * @param msgI
    * @throws HornetQException
    */
   private void largeMessageSend(final boolean sendBlocking,
                                 final MessageInternal msgI,
                                 final ClientProducerCredits credits) throws HornetQException
   {

      if (session.isCompressLargeMessages())
      {
         msgI.putBooleanProperty(Message.HDR_LARGE_COMPRESSED, true);
      }

      int headerSize = msgI.getHeadersAndPropertiesEncodeSize();

      if (msgI.getHeadersAndPropertiesEncodeSize() >= minLargeMessageSize)
      {
         throw new HornetQException(HornetQException.ILLEGAL_STATE, "Header size (" + headerSize +
                                                                    ") is too big, use the messageBody for large data, or increase minLargeMessageSize");
      }

      // msg.getBody() could be Null on LargeServerMessage
      if (msgI.getBodyInputStream() == null && msgI.getWholeBuffer() != null)
      {
         msgI.getWholeBuffer().readerIndex(0);
      }

      SessionSendLargeMessage initialChunk = new SessionSendLargeMessage(msgI);

      channel.send(initialChunk);

      try
      {
         credits.acquireCredits(msgI.getHeadersAndPropertiesEncodeSize());
      }
      catch (InterruptedException e)
      {
         throw new HornetQInterruptedException(e);
      }

      InputStream input = msgI.getBodyInputStream();

      if (msgI.isServerMessage())
      {
         largeMessageSendServer(sendBlocking, msgI, credits);
      }
      else if (input != null)
      {
         largeMessageSendStreamed(sendBlocking, msgI, input, credits);
      }
      else
      {
         largeMessageSendBuffered(sendBlocking, msgI, credits);
      }
   }

   /**
    * Used to send serverMessages through the bridges.
    * No need to validate compression here since the message is only compressed at the client
    * @param sendBlocking
    * @param msgI
    * @throws HornetQException
    */
   private void largeMessageSendServer(final boolean sendBlocking,
                                       final MessageInternal msgI,
                                       final ClientProducerCredits credits) throws HornetQException
   {
      BodyEncoder context = msgI.getBodyEncoder();

      final long bodySize = context.getLargeBodySize();

      context.open();
      try
      {

         for (int pos = 0; pos < bodySize;)
         {
            final boolean lastChunk;

            final int chunkLength = Math.min((int)(bodySize - pos), minLargeMessageSize);

            final HornetQBuffer bodyBuffer = HornetQBuffers.fixedBuffer(chunkLength);

            context.encode(bodyBuffer, chunkLength);

            pos += chunkLength;

            lastChunk = pos >= bodySize;

            final SessionSendContinuationMessage chunk = new SessionSendContinuationMessage(msgI,
                                                                                            bodyBuffer.toByteBuffer()
                                                                                                      .array(),
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

            try
            {
               credits.acquireCredits(chunk.getPacketSize());
            }
            catch (InterruptedException e)
            {
               throw new HornetQInterruptedException(e);
            }
         }
      }
      finally
      {
         context.close();
      }
   }

   /**
    * @param sendBlocking
    * @param msgI
    * @throws HornetQException
    */
   private void largeMessageSendBuffered(final boolean sendBlocking,
                                         final MessageInternal msgI,
                                         final ClientProducerCredits credits) throws HornetQException
   {
      msgI.getBodyBuffer().readerIndex(0);
      largeMessageSendStreamed(sendBlocking, msgI, new HornetQBufferInputStream(msgI.getBodyBuffer()), credits);
   }

   /**
    * @param sendBlocking
    * @param input
    * @throws HornetQException
    */
   private void largeMessageSendStreamed(final boolean sendBlocking,
                                         final MessageInternal msgI,
                                         final InputStream inputStreamParameter,
                                         final ClientProducerCredits credits) throws HornetQException
   {
      boolean lastPacket = false;

      InputStream input = inputStreamParameter;

      // We won't know the real size of the message since we are compressing while reading the streaming.
      // This counter will be passed to the deflater to be updated for every byte read
      AtomicLong messageSize = new AtomicLong();

      if (session.isCompressLargeMessages())
      {
         input = new DeflaterReader(inputStreamParameter, messageSize);
      }

      long totalSize = 0;

      while (!lastPacket)
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
               lastPacket = true;

               break;
            }

            pos += numberOfBytesRead;
         }
         while (pos < minLargeMessageSize);

         totalSize += pos;

         final SessionSendContinuationMessage chunk;

         if (lastPacket)
         {

            if (!session.isCompressLargeMessages())
            {
               messageSize.set(totalSize);
            }

            byte[] buff2 = new byte[pos];

            System.arraycopy(buff, 0, buff2, 0, pos);

            buff = buff2;

            chunk = new SessionSendContinuationMessage(msgI, buff, false, sendBlocking, messageSize.get());
         }
         else
         {
            chunk = new SessionSendContinuationMessage(msgI, buff, true, false);
         }

         if (sendBlocking && lastPacket)
         {
            // When sending it blocking, only the last chunk will be blocking.
            channel.sendBlocking(chunk);
         }
         else
         {
            channel.send(chunk);
         }

         try
         {
            credits.acquireCredits(chunk.getPacketSize());
         }
         catch (InterruptedException e)
         {
            throw new HornetQInterruptedException(e);
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
   // Inner Classes --------------------------------------------------------------------------------
}
