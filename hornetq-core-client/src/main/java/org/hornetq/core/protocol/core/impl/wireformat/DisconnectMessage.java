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
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.protocol.core.impl.PacketImpl;

public class DisconnectMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private SimpleString nodeID;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public DisconnectMessage(final SimpleString nodeID)
   {
      super(DISCONNECT);

      this.nodeID = nodeID;
   }

   public DisconnectMessage()
   {
      super(DISCONNECT);
   }

   // Public --------------------------------------------------------

   public SimpleString getNodeID()
   {
      return nodeID;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeNullableSimpleString(nodeID);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      nodeID = buffer.readNullableSimpleString();
   }

   @Override
   public String toString()
   {
      StringBuffer buf = new StringBuffer(getParentString());
      buf.append(", nodeID=" + nodeID);
      buf.append("]");
      return buf.toString();
   }

   @Override
   public final boolean isRequiresConfirmations()
   {
      return false;
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((nodeID == null) ? 0 : nodeID.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
      {
         return true;
      }
      if (!super.equals(obj))
      {
         return false;
      }
      if (!(obj instanceof DisconnectMessage))
      {
         return false;
      }
      DisconnectMessage other = (DisconnectMessage)obj;
      if (nodeID == null)
      {
         if (other.nodeID != null)
         {
            return false;
         }
      }
      else if (!nodeID.equals(other.nodeID))
      {
         return false;
      }
      return true;
   }
}
