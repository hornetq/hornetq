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
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;

/**
 * @author Justin Bertram
 */
public class ClusterTopologyChangeMessage_V3 extends ClusterTopologyChangeMessage_V2
{
   private String exportGroupName;

   public ClusterTopologyChangeMessage_V3(final long uniqueEventID, final String nodeID, final String backupGroupName, final String exportGroupName,
                                          final Pair<TransportConfiguration, TransportConfiguration> pair, final boolean last)
   {
      super(CLUSTER_TOPOLOGY_V3);

      this.nodeID = nodeID;

      this.pair = pair;

      this.last = last;

      this.exit = false;

      this.uniqueEventID = uniqueEventID;

      this.backupGroupName = backupGroupName;

      this.exportGroupName = exportGroupName;
   }

   public ClusterTopologyChangeMessage_V3(final long uniqueEventID, final String nodeID)
   {
      super(CLUSTER_TOPOLOGY_V3);

      this.exit = true;

      this.nodeID = nodeID;

      this.uniqueEventID = uniqueEventID;
   }

   public ClusterTopologyChangeMessage_V3()
   {
      super(CLUSTER_TOPOLOGY_V3);
   }

   public String getExportGroupName()
   {
      return exportGroupName;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      super.encodeRest(buffer);
      buffer.writeNullableString(exportGroupName);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      super.decodeRest(buffer);
      exportGroupName = buffer.readNullableString();
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((exportGroupName == null) ? 0 : exportGroupName.hashCode());
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
      if (!(obj instanceof ClusterTopologyChangeMessage_V3))
      {
         return false;
      }
      ClusterTopologyChangeMessage_V3 other = (ClusterTopologyChangeMessage_V3) obj;
      if (exportGroupName == null)
      {
         if (other.exportGroupName != null)
         {
            return false;
         }
      }
      else if (!exportGroupName.equals(other.exportGroupName))
      {
         return false;
      }
      return true;
   }
}
