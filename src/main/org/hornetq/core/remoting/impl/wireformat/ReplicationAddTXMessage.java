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

package org.hornetq.core.remoting.impl.wireformat;

import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.utils.DataConstants;

/**
 * A ReplicationAddMessage
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ReplicationAddTXMessage extends PacketImpl
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private long txId;

   private long id;

   /** 0 - Bindings, 1 - MessagesJournal */
   private byte journalID;

   private boolean isUpdate;

   private byte recordType;

   private EncodingSupport encodingData;

   private byte[] recordData;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReplicationAddTXMessage()
   {
      super(REPLICATION_APPEND_TX);
   }

   public ReplicationAddTXMessage(final byte journalID,
                                  final boolean isUpdate,
                                  final long txId,
                                  final long id,
                                  final byte recordType,
                                  final EncodingSupport encodingData)
   {
      this();
      this.journalID = journalID;
      this.isUpdate = isUpdate;
      this.txId = txId;
      this.id = id;
      this.recordType = recordType;
      this.encodingData = encodingData;
   }

   // Public --------------------------------------------------------

   @Override
   public int getRequiredBufferSize()
   {
      return BASIC_PACKET_SIZE + DataConstants.SIZE_BYTE +
             DataConstants.SIZE_BOOLEAN +
             DataConstants.SIZE_LONG +
             DataConstants.SIZE_LONG +
             DataConstants.SIZE_BYTE +
             DataConstants.SIZE_INT +
             (encodingData != null ? encodingData.getEncodeSize() : recordData.length);

   }

   @Override
   public void encodeBody(final HornetQBuffer buffer)
   {
      buffer.writeByte(journalID);
      buffer.writeBoolean(isUpdate);
      buffer.writeLong(txId);
      buffer.writeLong(id);
      buffer.writeByte(recordType);
      buffer.writeInt(encodingData.getEncodeSize());
      encodingData.encode(buffer);
   }

   @Override
   public void decodeBody(final HornetQBuffer buffer)
   {
      journalID = buffer.readByte();
      isUpdate = buffer.readBoolean();
      txId = buffer.readLong();
      id = buffer.readLong();
      recordType = buffer.readByte();
      int size = buffer.readInt();
      recordData = new byte[size];
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

   public boolean isUpdate()
   {
      return isUpdate;
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

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
