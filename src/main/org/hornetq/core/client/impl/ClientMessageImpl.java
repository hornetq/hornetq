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
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.BodyEncoder;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.utils.DataConstants;

/**
 * 
 * A ClientMessageImpl
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 *
 */
public class ClientMessageImpl extends MessageImpl implements ClientMessageInternal
{
   private static final Logger log = Logger.getLogger(ClientMessageImpl.class);

   // added this constant here so that the client package have no dependency on JMS
   public static final SimpleString REPLYTO_HEADER_NAME = new SimpleString("JMSReplyTo");

   private int deliveryCount;

   private ClientConsumerInternal consumer;

   private boolean largeMessage;

   private int flowControlSize = -1;

   /** Used on LargeMessages only */
   private InputStream bodyInputStream;

   /*
    * Constructor for when reading from remoting
    */
   public ClientMessageImpl()
   {
   }

   /*
    * Construct messages before sending
    */
   public ClientMessageImpl(final byte type,
                            final boolean durable,
                            final long expiration,
                            final long timestamp,
                            final byte priority,
                            final int initialMessageBufferSize)
   {
      super(type, durable, expiration, timestamp, priority, initialMessageBufferSize);
   }

   public void onReceipt(final ClientConsumerInternal consumer)
   {
      this.consumer = consumer;
   }

   public void setDeliveryCount(final int deliveryCount)
   {
      this.deliveryCount = deliveryCount;
   }

   public int getDeliveryCount()
   {
      return deliveryCount;
   }

   public void acknowledge() throws HornetQException
   {
      if (consumer != null)
      {
         consumer.acknowledge(this);
      }
   }

   public int getFlowControlSize()
   {
      if (flowControlSize < 0)
      {
         throw new IllegalStateException("Flow Control hasn't been set");
      }
      return flowControlSize;
   }

   public void setFlowControlSize(final int flowControlSize)
   {
      this.flowControlSize = flowControlSize;
   }

   /**
    * @return the largeMessage
    */
   public boolean isLargeMessage()
   {
      return largeMessage;
   }

   /**
    * @param largeMessage the largeMessage to set
    */
   public void setLargeMessage(final boolean largeMessage)
   {
      this.largeMessage = largeMessage;
   }

   public int getBodySize()
   {
      return buffer.writerIndex() - buffer.readerIndex();
   }

   @Override
   public String toString()
   {
      return "ClientMessage[messageID=" + messageID +
             ", durable=" +
             durable +
             ", address=" +
             getAddress() +
             "]";
   }

   // FIXME - only used for large messages - move it!
   /* (non-Javadoc)
    * @see org.hornetq.api.core.client.ClientMessage#saveToOutputStream(java.io.OutputStream)
    */
   public void saveToOutputStream(final OutputStream out) throws HornetQException
   {
      if (largeMessage)
      {
         ((LargeMessageBufferInternal)getWholeBuffer()).saveBuffer(out);
      }
      else
      {
         try
         {
            byte readBuffer[] = new byte[getBodySize()];
            getBodyBuffer().readBytes(readBuffer);
            out.write(readBuffer);
         }
         catch (IOException e)
         {
            throw new HornetQException(HornetQException.LARGE_MESSAGE_ERROR_BODY, "Error saving the message body", e);
         }
      }

   }

   /* (non-Javadoc)
    * @see org.hornetq.api.core.client.ClientMessage#setOutputStream(java.io.OutputStream)
    */
   public void setOutputStream(final OutputStream out) throws HornetQException
   {
      if (largeMessage)
      {
         ((LargeMessageBufferInternal)getWholeBuffer()).setOutputStream(out);
      }
      else
      {
         saveToOutputStream(out);
      }

   }

   /* (non-Javadoc)
    * @see org.hornetq.api.core.client.ClientMessage#waitOutputStreamCompletion()
    */
   public boolean waitOutputStreamCompletion(final long timeMilliseconds) throws HornetQException
   {
      if (largeMessage)
      {
         return ((LargeMessageBufferInternal)getWholeBuffer()).waitCompletion(timeMilliseconds);
      }
      else
      {
         return true;
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.api.core.client.impl.ClientMessageInternal#discardLargeBody()
    */
   public void discardLargeBody()
   {
      if (largeMessage)
      {
         ((LargeMessageBufferInternal)getWholeBuffer()).discardUnusedPackets();
      }
   }

   /**
    * @return the bodyInputStream
    */
   public InputStream getBodyInputStream()
   {
      return bodyInputStream;
   }

   /**
    * @param bodyInputStream the bodyInputStream to set
    */
   public void setBodyInputStream(final InputStream bodyInputStream)
   {
      this.bodyInputStream = bodyInputStream;
   }

   public void setBuffer(final HornetQBuffer buffer)
   {
      this.buffer = buffer;

      if (bodyBuffer != null)
      {
         bodyBuffer.setBuffer(buffer);
      }
   }

   @Override
   public BodyEncoder getBodyEncoder() throws HornetQException
   {
      return new DecodingContext();
   }

   private final class DecodingContext implements BodyEncoder
   {
      public DecodingContext()
      {
      }

      public void open()
      {
         getBodyBuffer().readerIndex(0);
      }

      public void close()
      {
      }

      public long getLargeBodySize()
      {
         if (isLargeMessage())
         {
            return getBodyBuffer().writerIndex();
         }
         else
         {
            return getBodyBuffer().writerIndex() - BODY_OFFSET;
         }
      }

      public int encode(final ByteBuffer bufferRead) throws HornetQException
      {
         HornetQBuffer buffer = HornetQBuffers.wrappedBuffer(bufferRead);
         return encode(buffer, bufferRead.capacity());
      }

      public int encode(final HornetQBuffer bufferOut, final int size)
      {
         byte[] bytes = new byte[size];
         getWholeBuffer().readBytes(bytes);
         bufferOut.writeBytes(bytes, 0, size);
         return size;
      }
   }

}
