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
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;

/**
 * Clebert Suconic
 *
 */
public class ClusterTopologyChangeMessage_V2 extends ClusterTopologyChangeMessage
{
   private long uniqueEventID;
   private String nodeName;

   public ClusterTopologyChangeMessage_V2(final long uniqueEventID, final String nodeID, final String nodeName,
                                          final Pair<TransportConfiguration, TransportConfiguration> pair, final boolean last)
   {
      super(CLUSTER_TOPOLOGY_V2);

      this.nodeID = nodeID;

      this.pair = pair;

      this.last = last;

      this.exit = false;

      this.uniqueEventID = uniqueEventID;

      this.nodeName = nodeName;
   }

   public ClusterTopologyChangeMessage_V2(final long uniqueEventID, final String nodeID)
   {
      super(CLUSTER_TOPOLOGY_V2);

      this.exit = true;

      this.nodeID = nodeID;

      this.uniqueEventID = uniqueEventID;
   }

   public ClusterTopologyChangeMessage_V2()
   {
      super(CLUSTER_TOPOLOGY_V2);
   }

   /**
    * @return the uniqueEventID
    */
   public long getUniqueEventID()
   {
      return uniqueEventID;
   }

   public String getNodeName()
   {
      return nodeName;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeBoolean(exit);
      buffer.writeString(nodeID);
      buffer.writeLong(uniqueEventID);
      if (!exit)
      {
         if (pair.getA() != null)
         {
            buffer.writeBoolean(true);
            pair.getA().encode(buffer);
         }
         else
         {
            buffer.writeBoolean(false);
         }
         if (pair.getB() != null)
         {
            buffer.writeBoolean(true);
            pair.getB().encode(buffer);
         }
         else
         {
            buffer.writeBoolean(false);
         }
         buffer.writeBoolean(last);
      }
      buffer.writeNullableString(nodeName);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      exit = buffer.readBoolean();
      nodeID = buffer.readString();
      uniqueEventID = buffer.readLong();
      if (!exit)
      {
         boolean hasLive = buffer.readBoolean();
         TransportConfiguration a;
         if(hasLive)
         {
            a = new TransportConfiguration();
            a.decode(buffer);
         }
         else
         {
            a = null;
         }
         boolean hasBackup = buffer.readBoolean();
         TransportConfiguration b;
         if (hasBackup)
         {
            b = new TransportConfiguration();
            b.decode(buffer);
         }
         else
         {
            b = null;
         }
         pair = new Pair<TransportConfiguration, TransportConfiguration>(a, b);
         last = buffer.readBoolean();
      }
      if(buffer.readableBytes() > 0) {
        nodeName = buffer.readNullableString();
      }
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + (exit ? 1231 : 1237);
      result = prime * result + (last ? 1231 : 1237);
      result = prime * result + ((nodeID == null) ? 0 : nodeID.hashCode());
      result = prime * result + ((pair == null) ? 0 : pair.hashCode());
      result = prime * result + (int)(uniqueEventID ^ (uniqueEventID >>> 32));
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
      if (!(obj instanceof ClusterTopologyChangeMessage_V2))
      {
         return false;
      }
      ClusterTopologyChangeMessage_V2 other = (ClusterTopologyChangeMessage_V2)obj;
      if (exit != other.exit)
      {
         return false;
      }
      if (last != other.last)
      {
         return false;
      }
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
      if (pair == null)
      {
         if (other.pair != null)
         {
            return false;
         }
      }
      else if (!pair.equals(other.pair))
      {
         return false;
      }
      if (uniqueEventID != other.uniqueEventID)
      {
         return false;
      }
      return true;
   }
}
