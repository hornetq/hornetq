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
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.protocol.core.impl.PacketImpl;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class NodeAnnounceMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(NodeAnnounceMessage.class);

   // Attributes ----------------------------------------------------

   private String nodeID;
   
   private boolean backup;
   
   private long currentEventID;
   
   private TransportConfiguration connector;

   private TransportConfiguration backupConnector;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public NodeAnnounceMessage(final long currentEventID, final String nodeID, final boolean backup, final TransportConfiguration tc, final TransportConfiguration backupConnector)
   {
      super(PacketImpl.NODE_ANNOUNCE);

      this.currentEventID = currentEventID;
      
      this.nodeID = nodeID;
      
      this.backup = backup;
      
      this.connector = tc;
      
      this.backupConnector = backupConnector;
   }
   
   public NodeAnnounceMessage()
   {
      super(PacketImpl.NODE_ANNOUNCE);
   }

   // Public --------------------------------------------------------


   public String getNodeID()
   {
      return nodeID;
   }
   
   public boolean isBackup()
   {
      return backup;
   }
   
   public TransportConfiguration getConnector()
   {
      return connector;
   }
   
   public TransportConfiguration getBackupConnector()
   {
      return backupConnector;
   }

   /**
    * @return the currentEventID
    */
   public long getCurrentEventID()
   {
      return currentEventID;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeString(nodeID);
      buffer.writeBoolean(backup);
      buffer.writeLong(currentEventID);
      if (connector != null)
      {
         buffer.writeBoolean(true);
         connector.encode(buffer);
      }
      else
      {
         buffer.writeBoolean(false);
      }
      if (backupConnector != null)
      {
         buffer.writeBoolean(true);
         backupConnector.encode(buffer);
      }
      else
      {
         buffer.writeBoolean(false);
      }
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      this.nodeID = buffer.readString();
      this.backup = buffer.readBoolean();
      this.currentEventID = buffer.readLong();
      if (buffer.readBoolean())
      {
         connector = new TransportConfiguration();
         connector.decode(buffer);
      }
      if (buffer.readBoolean())
      {
         backupConnector = new TransportConfiguration();
         backupConnector.decode(buffer);
      }
   }

   /* (non-Javadoc)
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString()
   {
      return "NodeAnnounceMessage [backup=" + backup +
             ", connector=" +
             connector +
             ", nodeID=" +
             nodeID +
             ", toString()=" +
             super.toString() +
             "]";
   }


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
