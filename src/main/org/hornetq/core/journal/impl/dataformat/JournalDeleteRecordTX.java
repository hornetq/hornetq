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

package org.hornetq.core.journal.impl.dataformat;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.core.journal.impl.JournalImpl;

/**
 * A JournalDeleteRecordTX
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class JournalDeleteRecordTX extends JournalInternalRecord
{

   private final long txID;

   private final long id;

   private final EncodingSupport record;

   /**
    * @param txID
    * @param id
    * @param record
    */
   public JournalDeleteRecordTX(final long txID, final long id, final EncodingSupport record)
   {
      this.id = id;

      this.txID = txID;

      this.record = record;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.EncodingSupport#encode(org.hornetq.api.core.buffers.HornetQBuffer)
    */
   public void encode(final HornetQBuffer buffer)
   {
      buffer.writeByte(JournalImpl.DELETE_RECORD_TX);

      buffer.writeInt(fileID);

      buffer.writeByte(compactCount);

      buffer.writeLong(txID);

      buffer.writeLong(id);

      buffer.writeInt(record != null ? record.getEncodeSize() : 0);

      if (record != null)
      {
         record.encode(buffer);
      }

      buffer.writeInt(getEncodeSize());
   }

   @Override
   public int getEncodeSize()
   {
      return JournalImpl.SIZE_DELETE_RECORD_TX + (record != null ? record.getEncodeSize() : 0) + 1;
   }
}
