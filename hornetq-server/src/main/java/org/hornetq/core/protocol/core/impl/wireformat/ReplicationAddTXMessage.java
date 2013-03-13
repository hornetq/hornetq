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
import org.hornetq.core.replication.ReplicationManager.ADD_OPERATION_TYPE;

/**
 * A ReplicationAddMessage
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ReplicationAddTXMessage extends PacketImpl
{

   private long txId;

   private long id;

   /** 0 - BindingsImpl, 1 - MessagesJournal */
   private byte journalID;

   private byte recordType;

   private EncodingSupport encodingData;

   private byte[] recordData;

   private ADD_OPERATION_TYPE operation;

   public ReplicationAddTXMessage()
   {
      super(PacketImpl.REPLICATION_APPEND_TX);
   }

   public ReplicationAddTXMessage(final byte journalID, final ADD_OPERATION_TYPE operation,
                                  final long txId,
                                  final long id,
                                  final byte recordType,
                                  final EncodingSupport encodingData)
   {
      this();
      this.journalID = journalID;
      this.operation = operation;
      this.txId = txId;
      this.id = id;
      this.recordType = recordType;
      this.encodingData = encodingData;
   }

   // Public --------------------------------------------------------

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeByte(journalID);
      buffer.writeBoolean(operation.toBoolean());
      buffer.writeLong(txId);
      buffer.writeLong(id);
      buffer.writeByte(recordType);
      buffer.writeInt(encodingData.getEncodeSize());
      encodingData.encode(buffer);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      journalID = buffer.readByte();
      operation = ADD_OPERATION_TYPE.toOperation(buffer.readBoolean());
      txId = buffer.readLong();
      id = buffer.readLong();
      recordType = buffer.readByte();
      final int recordDataSize = buffer.readInt();
      recordData = new byte[recordDataSize];
      buffer.readBytes(recordData);
   }

   /**
    * @return the id
    */
   public long getId()
   {
      return id;
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

   public ADD_OPERATION_TYPE getOperation()
   {
      return operation;
   }

   /**
    * @return the recordType
    */
   public byte getRecordType()
   {
      return recordType;
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
      result = prime * result + (int)(id ^ (id >>> 32));
      result = prime * result + journalID;
      result = prime * result + ((operation == null) ? 0 : operation.hashCode());
      result = prime * result + Arrays.hashCode(recordData);
      result = prime * result + recordType;
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
      if (!(obj instanceof ReplicationAddTXMessage))
         return false;
      ReplicationAddTXMessage other = (ReplicationAddTXMessage)obj;
      if (encodingData == null)
      {
         if (other.encodingData != null)
            return false;
      }
      else if (!encodingData.equals(other.encodingData))
         return false;
      if (id != other.id)
         return false;
      if (journalID != other.journalID)
         return false;
      if (operation != other.operation)
         return false;
      if (!Arrays.equals(recordData, other.recordData))
         return false;
      if (recordType != other.recordType)
         return false;
      if (txId != other.txId)
         return false;
      return true;
   }
}
