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

package org.hornetq.core.remoting.impl.wireformat;

import org.hornetq.core.buffers.HornetQBuffer;
import org.hornetq.utils.DataConstants;

/**
 * A SessionReceiveLargeMessage
 *
 * @author Clebert Suconic
 *
 *
 */
public class SessionReceiveLargeMessage extends PacketImpl
{
   private byte[] largeMessageHeader;

   /** Since we receive the message before the entire message was received, */
   private long largeMessageSize;

   private long consumerID;

   private int deliveryCount;

   public SessionReceiveLargeMessage()
   {
      super(SESS_RECEIVE_LARGE_MSG);
   }

   public SessionReceiveLargeMessage(final long consumerID,
                                     final byte[] largeMessageHeader,
                                     final long largeMessageSize,
                                     final int deliveryCount)
   {
      super(SESS_RECEIVE_LARGE_MSG);

      this.consumerID = consumerID;

      this.largeMessageHeader = largeMessageHeader;

      this.deliveryCount = deliveryCount;

      this.largeMessageSize = largeMessageSize;
   }

   public byte[] getLargeMessageHeader()
   {
      return largeMessageHeader;
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
   
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeLong(consumerID);
      buffer.writeInt(deliveryCount);
      buffer.writeLong(largeMessageSize);
      buffer.writeInt(largeMessageHeader.length);
      buffer.writeBytes(largeMessageHeader);
   }

   public void decodeRest(final HornetQBuffer buffer)
   {
      consumerID = buffer.readLong();
      deliveryCount = buffer.readInt();
      largeMessageSize = buffer.readLong();
      int size = buffer.readInt();
      largeMessageHeader = new byte[size];
      buffer.readBytes(largeMessageHeader);
   }

}
