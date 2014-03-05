/*
 * Copyright 2005-2014 Red Hat, Inc.
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
import org.hornetq.api.core.TransportConfiguration;

public class DisconnectMessage_V2 extends DisconnectMessage
{
   private TransportConfiguration transportConfiguration;

   public DisconnectMessage_V2(final SimpleString nodeID, final TransportConfiguration transportConfiguration)
   {
      super(DISCONNECT_V2);

      this.nodeID = nodeID;

      this.transportConfiguration = transportConfiguration;
   }

   public DisconnectMessage_V2()
   {
      super(DISCONNECT_V2);
   }

   // Public --------------------------------------------------------

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      super.encodeRest(buffer);
      transportConfiguration.encode(buffer);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      super.decodeRest(buffer);
      transportConfiguration = new TransportConfiguration();
      transportConfiguration.decode(buffer);
   }

   @Override
   public String toString()
   {
      StringBuffer buf = new StringBuffer(getParentString());
      buf.append(", nodeID=" + nodeID);
      buf.append(", transportConfiguration=" + transportConfiguration);
      buf.append("]");
      return buf.toString();
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((transportConfiguration == null) ? 0 : transportConfiguration.hashCode());
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
      if (!(obj instanceof DisconnectMessage_V2))
      {
         return false;
      }
      DisconnectMessage_V2 other = (DisconnectMessage_V2) obj;
      if (transportConfiguration == null)
      {
         if (other.transportConfiguration != null)
         {
            return false;
         }
      }
      else if (!transportConfiguration.equals(other.transportConfiguration))
      {
         return false;
      }
      return true;
   }
}
