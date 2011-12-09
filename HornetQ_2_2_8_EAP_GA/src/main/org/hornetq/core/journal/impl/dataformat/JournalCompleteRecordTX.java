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
 * <p>A transaction record (Commit or Prepare), will hold the number of elements the transaction has on each file.</p>
 * <p>For example, a transaction was spread along 3 journal files with 10 pendingTransactions on each file. 
 *    (What could happen if there are too many pendingTransactions, or if an user event delayed pendingTransactions to come in time to a single file).</p>
 * <p>The element-summary will then have</p>
 * <p>FileID1, 10</p>
 * <p>FileID2, 10</p>
 * <p>FileID3, 10</p>
 * 
 * <br>
 * <p> During the load, the transaction needs to have 30 pendingTransactions spread across the files as originally written.</p>
 * <p> If for any reason there are missing pendingTransactions, that means the transaction was not completed and we should ignore the whole transaction </p>
 * <p> We can't just use a global counter as reclaiming could delete files after the transaction was successfully committed. 
 *     That also means not having a whole file on journal-reload doesn't mean we have to invalidate the transaction </p>
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class JournalCompleteRecordTX extends JournalInternalRecord
{
   private final boolean isCommit;

   public final long txID;

   private final EncodingSupport transactionData;

   private int numberOfRecords;

   public JournalCompleteRecordTX(final boolean isCommit, final long txID, final EncodingSupport transactionData)
   {
      this.isCommit = isCommit;

      this.txID = txID;

      this.transactionData = transactionData;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.EncodingSupport#encode(org.hornetq.api.core.buffers.HornetQBuffer)
    */
   public void encode(final HornetQBuffer buffer)
   {
      if (isCommit)
      {
         buffer.writeByte(JournalImpl.COMMIT_RECORD);
      }
      else
      {
         buffer.writeByte(JournalImpl.PREPARE_RECORD);
      }

      buffer.writeInt(fileID);
      
      buffer.writeByte(compactCount);

      buffer.writeLong(txID);

      buffer.writeInt(numberOfRecords);

      if (transactionData != null)
      {
         buffer.writeInt(transactionData.getEncodeSize());
      }

      if (transactionData != null)
      {
         transactionData.encode(buffer);
      }

      buffer.writeInt(getEncodeSize());
   }

   @Override
   public void setNumberOfRecords(final int records)
   {
      numberOfRecords = records;
   }

   @Override
   public int getNumberOfRecords()
   {
      return numberOfRecords;
   }

   @Override
   public int getEncodeSize()
   {
      if (isCommit)
      {
         return JournalImpl.SIZE_COMPLETE_TRANSACTION_RECORD + 1;
      }
      else
      {
         return JournalImpl.SIZE_PREPARE_RECORD + (transactionData != null ? transactionData.getEncodeSize() : 0) + 1;
      }
   }
}
