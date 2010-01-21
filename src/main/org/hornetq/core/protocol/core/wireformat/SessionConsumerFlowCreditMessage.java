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
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class SessionConsumerFlowCreditMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private long consumerID;

   private int credits;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionConsumerFlowCreditMessage(final long consumerID, final int credits)
   {
      super(PacketImpl.SESS_FLOWTOKEN);

      this.consumerID = consumerID;

      this.credits = credits;
   }

   public SessionConsumerFlowCreditMessage()
   {
      super(PacketImpl.SESS_FLOWTOKEN);
   }

   // Public --------------------------------------------------------

   public long getConsumerID()
   {
      return consumerID;
   }

   public int getCredits()
   {
      return credits;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeLong(consumerID);
      buffer.writeInt(credits);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      consumerID = buffer.readLong();
      credits = buffer.readInt();
   }

   @Override
   public String toString()
   {
      return getParentString() + ", consumerID=" + consumerID + ", credits=" + credits + "]";
   }

   @Override
   public boolean equals(final Object other)
   {
      if (other instanceof SessionConsumerFlowCreditMessage == false)
      {
         return false;
      }

      SessionConsumerFlowCreditMessage r = (SessionConsumerFlowCreditMessage)other;

      return super.equals(other) && credits == r.credits && consumerID == r.consumerID;
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
