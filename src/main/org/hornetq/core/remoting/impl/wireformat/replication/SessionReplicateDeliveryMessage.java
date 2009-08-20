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

package org.hornetq.core.remoting.impl.wireformat.replication;

import org.hornetq.core.remoting.impl.wireformat.PacketImpl;
import org.hornetq.core.remoting.spi.MessagingBuffer;
import org.hornetq.utils.DataConstants;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public class SessionReplicateDeliveryMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private long consumerID;

   private long messageID;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionReplicateDeliveryMessage(final long consumerID, final long messageID)
   {
      super(SESS_REPLICATE_DELIVERY);

      this.consumerID = consumerID;

      this.messageID = messageID;
   }

   public SessionReplicateDeliveryMessage()
   {
      super(SESS_REPLICATE_DELIVERY);
   }

   // Public --------------------------------------------------------

   public long getConsumerID()
   {
      return consumerID;
   }

   public long getMessageID()
   {
      return messageID;
   }

   public int getRequiredBufferSize()
   {
      return BASIC_PACKET_SIZE + DataConstants.SIZE_LONG + DataConstants.SIZE_LONG;
   }

   @Override
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.writeLong(consumerID);

      buffer.writeLong(messageID);
   }

   @Override
   public void decodeBody(final MessagingBuffer buffer)
   {
      consumerID = buffer.readLong();

      messageID = buffer.readLong();
   }

   @Override
   public boolean isRequiresConfirmations()
   {
      return false;
   }

   @Override
   public boolean equals(final Object other)
   {
      if (other instanceof SessionReplicateDeliveryMessage == false)
      {
         return false;
      }

      SessionReplicateDeliveryMessage r = (SessionReplicateDeliveryMessage)other;

      return super.equals(other) && consumerID == r.consumerID && messageID == r.messageID;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
