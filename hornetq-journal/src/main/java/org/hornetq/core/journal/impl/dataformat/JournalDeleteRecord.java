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
import org.hornetq.core.journal.impl.JournalImpl;

/**
 * A JournalDeleteRecord
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class JournalDeleteRecord extends JournalInternalRecord
{

   private final long id;

   /**
    * @param id
    */
   public JournalDeleteRecord(final long id)
   {
      this.id = id;
   }

   public void encode(final HornetQBuffer buffer)
   {
      buffer.writeByte(JournalImpl.DELETE_RECORD);

      buffer.writeInt(fileID);

      buffer.writeByte(compactCount);

      buffer.writeLong(id);

      buffer.writeInt(getEncodeSize());
   }

   @Override
   public int getEncodeSize()
   {
      return JournalImpl.SIZE_DELETE_RECORD + 1;
   }
}
