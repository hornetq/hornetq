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

import java.util.Arrays;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.core.protocol.core.impl.PacketImpl;

/**
 * A ReplicationAddMessage
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ReplicationPrepareMessage extends PacketImpl
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private long txId;

   /** 0 - Bindings, 1 - MessagesJournal */
   private byte journalID;

   private EncodingSupport encodingData;

   private byte[] recordData;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReplicationPrepareMessage()
   {
      super(PacketImpl.REPLICATION_PREPARE);
   }

   public ReplicationPrepareMessage(final byte journalID, final long txId, final EncodingSupport encodingData)
   {
      this();
      this.journalID = journalID;
      this.txId = txId;
      this.encodingData = encodingData;
   }

   // Public --------------------------------------------------------

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeByte(journalID);
      buffer.writeLong(txId);
      buffer.writeInt(encodingData.getEncodeSize());
      encodingData.encode(buffer);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      journalID = buffer.readByte();
      txId = buffer.readLong();
      int size = buffer.readInt();
      recordData = new byte[size];
      buffer.readBytes(recordData);
   }

   public long getTxId()
   {
      return txId;
   }

   /**
    * @return the journalID
    */
   public byte getJournalID()
   {
      return journalID;
   }

   /**
    * @return the recordData
    */
   public byte[] getRecordData()
   {
      return recordData;
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((encodingData == null) ? 0 : encodingData.hashCode());
      result = prime * result + journalID;
      result = prime * result + Arrays.hashCode(recordData);
      result = prime * result + (int)(txId ^ (txId >>> 32));
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
      if (!(obj instanceof ReplicationPrepareMessage))
      {
         return false;
      }
      ReplicationPrepareMessage other = (ReplicationPrepareMessage)obj;
      if (encodingData == null)
      {
         if (other.encodingData != null)
         {
            return false;
         }
      }
      else if (!encodingData.equals(other.encodingData))
      {
         return false;
      }
      if (journalID != other.journalID)
      {
         return false;
      }
      if (!Arrays.equals(recordData, other.recordData))
      {
         return false;
      }
      if (txId != other.txId)
      {
         return false;
      }
      return true;
   }
}
