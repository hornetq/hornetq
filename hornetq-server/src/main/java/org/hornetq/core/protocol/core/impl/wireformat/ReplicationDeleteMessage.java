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
 * A ReplicationAddMessage
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ReplicationDeleteMessage extends PacketImpl
{

   private long id;

   /** 0 - Bindings, 1 - MessagesJournal */
   private byte journalID;

   public ReplicationDeleteMessage()
   {
      super(PacketImpl.REPLICATION_DELETE);
   }

   public ReplicationDeleteMessage(final byte journalID, final long id)
   {
      this();
      this.journalID = journalID;
      this.id = id;
   }

   // Public --------------------------------------------------------

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeByte(journalID);
      buffer.writeLong(id);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      journalID = buffer.readByte();
      id = buffer.readLong();
   }

   /**
    * @return the id
    */
   public long getId()
   {
      return id;
   }

   /**
    * @return the journalID
    */
   public byte getJournalID()
   {
      return journalID;
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + (int)(id ^ (id >>> 32));
      result = prime * result + journalID;
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof ReplicationDeleteMessage))
         return false;
      ReplicationDeleteMessage other = (ReplicationDeleteMessage)obj;
      if (id != other.id)
         return false;
      if (journalID != other.journalID)
         return false;
      return true;
   }
}
