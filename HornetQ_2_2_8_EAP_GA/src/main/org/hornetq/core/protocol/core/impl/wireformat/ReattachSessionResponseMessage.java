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
import org.hornetq.core.protocol.core.impl.PacketImpl;

/**
 * 
 * A ReattachSessionResponseMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ReattachSessionResponseMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private int lastConfirmedCommandID;

   private boolean reattached;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReattachSessionResponseMessage(final int lastConfirmedCommandID, final boolean reattached)
   {
      super(PacketImpl.REATTACH_SESSION_RESP);

      this.lastConfirmedCommandID = lastConfirmedCommandID;

      this.reattached = reattached;
   }

   public ReattachSessionResponseMessage()
   {
      super(PacketImpl.REATTACH_SESSION_RESP);
   }

   // Public --------------------------------------------------------

   public int getLastConfirmedCommandID()
   {
      return lastConfirmedCommandID;
   }

   public boolean isReattached()
   {
      return reattached;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeInt(lastConfirmedCommandID);
      buffer.writeBoolean(reattached);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      lastConfirmedCommandID = buffer.readInt();
      reattached = buffer.readBoolean();
   }

   @Override
   public boolean isResponse()
   {
      return true;
   }

   @Override
   public boolean equals(final Object other)
   {
      if (other instanceof ReattachSessionResponseMessage == false)
      {
         return false;
      }

      ReattachSessionResponseMessage r = (ReattachSessionResponseMessage)other;

      return super.equals(other) && lastConfirmedCommandID == r.lastConfirmedCommandID;
   }

   @Override
   public final boolean isRequiresConfirmations()
   {
      return false;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
