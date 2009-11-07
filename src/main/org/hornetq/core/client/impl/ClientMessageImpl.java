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
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.hornetq.core.buffers.ChannelBuffers;
import org.hornetq.core.client.LargeMessageBuffer;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.message.BodyEncoder;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.utils.SimpleString;

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
   // added this constant here so that the client package have no dependency on JMS
   public static final SimpleString REPLYTO_HEADER_NAME = new SimpleString("JMSReplyTo");

   private int deliveryCount;

   private ClientConsumerInternal consumer;

   private boolean largeMessage;

   private int flowControlSize = -1;

   /*
    * Constructor for when reading from network
    */
   public ClientMessageImpl(final int deliveryCount)
   {
      super();

      this.deliveryCount = deliveryCount;
   }

   /*
    * Construct messages before sending
    */
   public ClientMessageImpl(final byte type,
                            final boolean durable,
                            final long expiration,
                            final long timestamp,
                            final byte priority,
                            final HornetQBuffer body)
   {
      super(type, durable, expiration, timestamp, priority, body);
   }

   public ClientMessageImpl(final byte type, final boolean durable, final HornetQBuffer body)
   {
      super(type, durable, 0, System.currentTimeMillis(), (byte)4, body);
   }

   public ClientMessageImpl(final boolean durable, final HornetQBuffer body)
   {
      super((byte)0, durable, 0, System.currentTimeMillis(), (byte)4, body);
   }
   
   public ClientMessageImpl(final boolean durable, final byte[] bytes)
   {
      super((byte)0, durable, 0, System.currentTimeMillis(), (byte)4, ChannelBuffers.dynamicBuffer(bytes));
   }
   
   public ClientMessageImpl(final boolean durable)
   {
      super((byte)0, durable, 0, System.currentTimeMillis(), (byte)4, ChannelBuffers.dynamicBuffer(1024));
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
   
   public long getLargeBodySize()
   {
      if (largeMessage)
      {
         return ((LargeMessageBuffer)getBody()).getSize();
      }
      else
      {
         return this.getBodySize();
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

   /* (non-Javadoc)
    * @see org.hornetq.core.client.impl.ClientMessageInternal#discardLargeBody()
    */
   public void discardLargeBody()
   {
      if (largeMessage)
      {
         ((LargeMessageBuffer)getBody()).discardUnusedPackets();
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.client.ClientMessage#saveToOutputStream(java.io.OutputStream)
    */
   public void saveToOutputStream(final OutputStream out) throws HornetQException
   {
      if (largeMessage)
      {
         ((LargeMessageBufferImpl)this.getBody()).saveBuffer(out);
      }
      else
      {
         try
         {
            out.write(this.getBody().array());
         }
         catch (IOException e)
         {
            throw new HornetQException(HornetQException.LARGE_MESSAGE_ERROR_BODY, "Error saving the message body", e);
         }
      }
      
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.client.ClientMessage#setOutputStream(java.io.OutputStream)
    */
   public void setOutputStream(final OutputStream out) throws HornetQException
   {
      if (largeMessage)
      {
         ((LargeMessageBufferImpl)this.getBody()).setOutputStream(out);
      }
      else
      {
         saveToOutputStream(out);
      }
      
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.client.ClientMessage#waitOutputStreamCompletion()
    */
   public boolean waitOutputStreamCompletion(final long timeMilliseconds) throws HornetQException
   {
      if (largeMessage)
      {
         return ((LargeMessageBufferImpl)this.getBody()).waitCompletion(timeMilliseconds);
      }
      else
      {
         return true;
      }
   }
   
   @Override
   public String toString()
   {
      return "ClientMessage[messageID=" + messageID +
             ", durable=" +
             durable +
             ", destination=" +
             getDestination() +
             "]";
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.message.Message#getBodyEncoder()
    */
   
}
