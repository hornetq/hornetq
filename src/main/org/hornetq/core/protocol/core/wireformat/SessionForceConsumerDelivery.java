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

package org.hornetq.core.protocol.core.wireformat;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.protocol.core.PacketImpl;

/**
 * 
 * A SessionConsumerForceDelivery
 * 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class SessionForceConsumerDelivery extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private long consumerID;

   private long sequence;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionForceConsumerDelivery(final long consumerID, final long sequence)
   {
      super(PacketImpl.SESS_FORCE_CONSUMER_DELIVERY);

      this.consumerID = consumerID;
      this.sequence = sequence;
   }

   public SessionForceConsumerDelivery()
   {
      super(PacketImpl.SESS_FORCE_CONSUMER_DELIVERY);
   }

   // Public --------------------------------------------------------

   public long getConsumerID()
   {
      return consumerID;
   }

   public long getSequence()
   {
      return sequence;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeLong(consumerID);
      buffer.writeLong(sequence);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      consumerID = buffer.readLong();
      sequence = buffer.readLong();
   }

   @Override
   public String toString()
   {
      StringBuffer buf = new StringBuffer(getParentString());
      buf.append(", consumerID=" + consumerID);
      buf.append(", sequence=" + sequence);
      buf.append("]");
      return buf.toString();
   }

   @Override
   public boolean equals(final Object other)
   {
      if (other instanceof SessionForceConsumerDelivery == false)
      {
         return false;
      }

      SessionForceConsumerDelivery r = (SessionForceConsumerDelivery)other;

      return super.equals(other) && consumerID == r.consumerID && sequence == r.sequence;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
