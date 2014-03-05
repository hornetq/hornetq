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
import org.hornetq.api.core.TransportConfiguration;

/**
 * @author Justin Bertram
 */
public class NodeAnnounceMessage_V2 extends NodeAnnounceMessage
{
   private String exportGroupName;

   // Constructors --------------------------------------------------

   public NodeAnnounceMessage_V2(final long currentEventID, final String nodeID, final String backupGroupName, final String exportGroupName, final boolean backup, final TransportConfiguration tc, final TransportConfiguration backupConnector)
   {
      super(NODE_ANNOUNCE_V2);

      this.currentEventID = currentEventID;

      this.nodeID = nodeID;

      this.backupGroupName = backupGroupName;

      this.backup = backup;

      this.connector = tc;

      this.backupConnector = backupConnector;

      this.exportGroupName = exportGroupName;
   }

   public NodeAnnounceMessage_V2()
   {
      super(NODE_ANNOUNCE_V2);
   }

   // Public --------------------------------------------------------

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
   public String toString()
   {
      return "NodeAnnounceMessage_V2 [exportGroupName=" + exportGroupName +
         ", toString()=" +
         super.toString() +
         "]";
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
      if (!(obj instanceof NodeAnnounceMessage_V2))
      {
         return false;
      }
      NodeAnnounceMessage_V2 other = (NodeAnnounceMessage_V2) obj;
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
