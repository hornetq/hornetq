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

import org.hornetq.core.buffers.HornetQBuffer;
import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.core.logging.Logger;

/**
 * A ReplicationAddMessage
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ReplicationAddMessage extends PacketImpl
{

   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(ReplicationAddMessage.class);


   // Attributes ----------------------------------------------------

   private long id;

   /** 0 - Bindings, 1 - MessagesJournal */
   private byte journalID;

   private boolean isUpdate;

   private byte recordType;

   private EncodingSupport encodingData;

   private byte[] recordData;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReplicationAddMessage()
   {
      super(REPLICATION_APPEND);
   }

   public ReplicationAddMessage(final byte journalID,
                                final boolean isUpdate,
                                final long id,
                                final byte recordType,
                                final EncodingSupport encodingData)
   {
      this();
      this.journalID = journalID;
      this.isUpdate = isUpdate;
      this.id = id;
      this.recordType = recordType;
      this.encodingData = encodingData;
   }

   // Public --------------------------------------------------------

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeByte(journalID);
      buffer.writeBoolean(isUpdate);
      buffer.writeLong(id);
      buffer.writeByte(recordType);     
      buffer.writeInt(encodingData.getEncodeSize());
      encodingData.encode(buffer);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      journalID = buffer.readByte();
      isUpdate = buffer.readBoolean();
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
