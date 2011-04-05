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

package org.hornetq.core.protocol.core.impl.wireformat;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.client.impl.ClientLargeMessageImpl;
import org.hornetq.core.client.impl.ClientLargeMessageInternal;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.impl.MessageInternal;
import org.hornetq.core.protocol.core.impl.PacketImpl;

/**
 * A SessionReceiveLargeMessage
 *
 * @author Clebert Suconic
 *
 *
 */
public class SessionReceiveLargeMessage extends PacketImpl
{
   private static final Logger log = Logger.getLogger(SessionReceiveLargeMessage.class);

   private MessageInternal message;

   /** Since we receive the message before the entire message was received, */
   private long largeMessageSize;

   private long consumerID;

   private int deliveryCount;

   // To be used on decoding at the client while receiving a large message
   public SessionReceiveLargeMessage()
   {
      super(PacketImpl.SESS_RECEIVE_LARGE_MSG);
      this.message = new ClientLargeMessageImpl();
   }

   public SessionReceiveLargeMessage(final long consumerID,
                                     final MessageInternal message,
                                     final long largeMessageSize,
                                     final int deliveryCount)
   {
      super(PacketImpl.SESS_RECEIVE_LARGE_MSG);
      
      this.consumerID = consumerID;

      this.message = message;

      this.deliveryCount = deliveryCount;

      this.largeMessageSize = largeMessageSize;
   }

   public MessageInternal getLargeMessage()
   {
      return message;
   }

   public long getConsumerID()
   {
      return consumerID;
   }

   public int getDeliveryCount()
   {
      return deliveryCount;
   }

   /**
    * @return the largeMessageSize
    */
   public long getLargeMessageSize()
   {
      return largeMessageSize;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeLong(consumerID);
      buffer.writeInt(deliveryCount);
      buffer.writeLong(largeMessageSize);
      message.encodeHeadersAndProperties(buffer);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      consumerID = buffer.readLong();
      deliveryCount = buffer.readInt();
      largeMessageSize = buffer.readLong();
      message.decodeHeadersAndProperties(buffer);
      ((ClientLargeMessageInternal)message).setLargeMessageSize(largeMessageSize);
   }

}
