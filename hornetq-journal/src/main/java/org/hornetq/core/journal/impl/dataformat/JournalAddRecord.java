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
 * A JournalAddRecord
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class JournalAddRecord extends JournalInternalRecord
{

   private final long id;

   private final EncodingSupport record;

   private final byte recordType;

   private final boolean add;

   /**
    * @param id
    * @param recordType
    * @param record
    */
   public JournalAddRecord(final boolean add, final long id, final byte recordType, final EncodingSupport record)
   {
      this.id = id;

      this.record = record;

      this.recordType = recordType;

      this.add = add;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.EncodingSupport#encode(org.hornetq.api.core.buffers.HornetQBuffer)
    */
   public void encode(final HornetQBuffer buffer)
   {
      if (add)
      {
         buffer.writeByte(JournalImpl.ADD_RECORD);
      }
      else
      {
         buffer.writeByte(JournalImpl.UPDATE_RECORD);
      }

      buffer.writeInt(fileID);

      buffer.writeByte(compactCount);

      buffer.writeLong(id);

      buffer.writeInt(record.getEncodeSize());

      buffer.writeByte(recordType);

      record.encode(buffer);

      buffer.writeInt(getEncodeSize());
   }

   @Override
   public int getEncodeSize()
   {
      return JournalImpl.SIZE_ADD_RECORD + record.getEncodeSize() + 1;
   }
}