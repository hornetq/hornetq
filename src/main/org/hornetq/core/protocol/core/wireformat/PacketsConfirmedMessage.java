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
 * A PacketsConfirmedMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class PacketsConfirmedMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private int commandID;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public PacketsConfirmedMessage(final int commandID)
   {
      super(PacketImpl.PACKETS_CONFIRMED);

      this.commandID = commandID;
   }

   public PacketsConfirmedMessage()
   {
      super(PacketImpl.PACKETS_CONFIRMED);
   }

   // Public --------------------------------------------------------

   public int getCommandID()
   {
      return commandID;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeInt(commandID);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      commandID = buffer.readInt();
   }

   @Override
   public String toString()
   {
      return getParentString() + ", commandID=" + commandID + "]";
   }

   @Override
   public boolean equals(final Object other)
   {
      if (other instanceof PacketsConfirmedMessage == false)
      {
         return false;
      }

      PacketsConfirmedMessage r = (PacketsConfirmedMessage)other;

      return super.equals(other) && commandID == r.commandID;
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
