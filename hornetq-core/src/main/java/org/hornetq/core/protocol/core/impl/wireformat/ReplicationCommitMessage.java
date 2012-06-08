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
public class ReplicationCommitMessage extends PacketImpl
{

   /** 0 - Bindings, 1 - MessagesJournal */
   private byte journalID;

   private boolean rollback;

   private long txId;

   private boolean sync;


   public ReplicationCommitMessage()
   {
      super(PacketImpl.REPLICATION_COMMIT_ROLLBACK);
   }

   public ReplicationCommitMessage(final byte journalID, final boolean rollback, final long txId, boolean sync)
   {
      this();
      this.journalID = journalID;
      this.rollback = rollback;
      this.txId = txId;
      this.sync = sync;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeByte(journalID);
      buffer.writeBoolean(rollback);
      buffer.writeLong(txId);
      buffer.writeBoolean(sync);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      journalID = buffer.readByte();
      rollback = buffer.readBoolean();
      txId = buffer.readLong();
      sync = buffer.readBoolean();
   }

   public boolean isRollback()
   {
      return rollback;
   }

   public long getTxId()
   {
      return txId;
   }

   public boolean getSync()
   {
      return sync;
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
      result = prime * result + journalID;
      result = prime * result + (rollback ? 1231 : 1237);
      result = prime * result + (sync ? 1231 : 1237);
      result = prime * result + (int)(txId ^ (txId >>> 32));
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof ReplicationCommitMessage))
         return false;
      ReplicationCommitMessage other = (ReplicationCommitMessage)obj;
      if (journalID != other.journalID)
         return false;
      if (rollback != other.rollback)
         return false;
      if (sync != other.sync)
         return false;
      if (txId != other.txId)
         return false;
      return true;
   }

   @Override
   public String toString()
   {
      String txOperation = rollback ? "rollback" : "commmit";
      return ReplicationCommitMessage.class.getSimpleName() + "[type=" + getType() + ", channel=" + getChannelID() +
               ", journalID=" + journalID + ", txAction='" + txOperation + "']";
   }
}
