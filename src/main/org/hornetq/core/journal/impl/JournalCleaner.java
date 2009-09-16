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

package org.hornetq.core.journal.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.utils.DataConstants;

/**
 * A JournalCleaner
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class JournalCleaner extends AbstractJournalUpdateTask
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final HashMap<Long, AtomicInteger> transactionCounter = new HashMap<Long, AtomicInteger>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------
   /**
    * @param fileFactory
    * @param journal
    * @param nextOrderingID
    */
   protected JournalCleaner(final SequentialFileFactory fileFactory,
                            final JournalImpl journal,
                            final Set<Long> recordsSnapshot,
                            final int nextOrderingID) throws Exception
   {
      super(fileFactory, journal, recordsSnapshot, nextOrderingID);
      openFile();
   }

   // Public --------------------------------------------------------

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.impl.JournalReaderCallback#markAsDataFile(org.hornetq.core.journal.impl.JournalFile)
    */
   public void markAsDataFile(final JournalFile file)
   {
      // nothing to be done here
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.impl.JournalReaderCallback#onReadAddRecord(org.hornetq.core.journal.RecordInfo)
    */
   public void onReadAddRecord(final RecordInfo info) throws Exception
   {
      if (lookupRecord(info.id))
      {
         int size = JournalImpl.SIZE_ADD_RECORD + info.data.length;

         JournalImpl.writeAddRecord(fileID,
                                    info.id,
                                    info.getUserRecordType(),
                                    new JournalImpl.ByteArrayEncoding(info.data),
                                    size,
                                    getWritingChannel());
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.impl.JournalReaderCallback#onReadAddRecordTX(long, org.hornetq.core.journal.RecordInfo)
    */
   public void onReadAddRecordTX(final long transactionID, final RecordInfo recordInfo) throws Exception
   {
      if (lookupRecord(recordInfo.id))
      {
         incrementTransactionCounter(transactionID);

         int size = JournalImpl.SIZE_ADD_RECORD_TX + recordInfo.data.length;

         JournalImpl.writeAddRecordTX(fileID,
                                      transactionID,
                                      recordInfo.id,
                                      recordInfo.getUserRecordType(),
                                      new JournalImpl.ByteArrayEncoding(recordInfo.data),
                                      size,
                                      getWritingChannel());
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.impl.JournalReaderCallback#onReadCommitRecord(long, int)
    */
   public void onReadCommitRecord(final long transactionID, final int numberOfRecords) throws Exception
   {
      int txcounter = getTransactionCounter(transactionID);

      JournalImpl.writeTransaction(fileID,
                                   JournalImpl.COMMIT_RECORD,
                                   transactionID,
                                   null,
                                   JournalImpl.SIZE_COMPLETE_TRANSACTION_RECORD,
                                   txcounter,
                                   getWritingChannel());

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.impl.JournalReaderCallback#onReadDeleteRecord(long)
    */
   public void onReadDeleteRecord(final long recordID) throws Exception
   {
      JournalImpl.writeDeleteRecord(fileID, recordID, JournalImpl.SIZE_DELETE_RECORD, getWritingChannel());
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.impl.JournalReaderCallback#onReadDeleteRecordTX(long, org.hornetq.core.journal.RecordInfo)
    */
   public void onReadDeleteRecordTX(final long transactionID, final RecordInfo recordInfo) throws Exception
   {
      int size = JournalImpl.SIZE_DELETE_RECORD_TX + recordInfo.data.length;

      incrementTransactionCounter(transactionID);

      JournalImpl.writeDeleteRecordTransactional(fileID,
                                                 transactionID,
                                                 recordInfo.id,
                                                 new JournalImpl.ByteArrayEncoding(recordInfo.data),
                                                 size,
                                                 getWritingChannel());
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.impl.JournalReaderCallback#onReadPrepareRecord(long, byte[], int)
    */
   public void onReadPrepareRecord(final long transactionID, final byte[] extraData, final int numberOfRecords) throws Exception
   {
      int txcounter = getTransactionCounter(transactionID);

      int size = JournalImpl.SIZE_COMPLETE_TRANSACTION_RECORD + extraData.length + DataConstants.SIZE_INT;

      JournalImpl.writeTransaction(fileID,
                                   JournalImpl.PREPARE_RECORD,
                                   transactionID,
                                   new JournalImpl.ByteArrayEncoding(extraData),
                                   size,
                                   txcounter,
                                   getWritingChannel());
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.impl.JournalReaderCallback#onReadRollbackRecord(long)
    */
   public void onReadRollbackRecord(final long transactionID) throws Exception
   {
      JournalImpl.writeRollback(fileID, transactionID, getWritingChannel());
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.impl.JournalReaderCallback#onReadUpdateRecord(org.hornetq.core.journal.RecordInfo)
    */
   public void onReadUpdateRecord(final RecordInfo recordInfo) throws Exception
   {
      if (lookupRecord(recordInfo.id))
      {
         int size = JournalImpl.SIZE_UPDATE_RECORD + recordInfo.data.length;
         JournalImpl.writeUpdateRecord(fileID,
                                       recordInfo.id,
                                       recordInfo.userRecordType,
                                       new JournalImpl.ByteArrayEncoding(recordInfo.data),
                                       size,
                                       getWritingChannel());
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.impl.JournalReaderCallback#onReadUpdateRecordTX(long, org.hornetq.core.journal.RecordInfo)
    */
   public void onReadUpdateRecordTX(final long transactionID, final RecordInfo recordInfo) throws Exception
   {
      if (lookupRecord(recordInfo.id))
      {
         incrementTransactionCounter(transactionID);
         int size = JournalImpl.SIZE_UPDATE_RECORD_TX + recordInfo.data.length;
         JournalImpl.writeUpdateRecordTX(fileID,
                                         transactionID,
                                         recordInfo.id,
                                         recordInfo.userRecordType,
                                         new JournalImpl.ByteArrayEncoding(recordInfo.data),
                                         size,
                                         getWritingChannel());
      }
   }

   /**
    * Read files that depend on this file.
    * Commits and rollbacks are also counted as negatives. We need to fix those also.
    * @param dependencies
    */
   public void fixDependencies(final JournalFile originalFile, final ArrayList<JournalFile> dependencies)  throws Exception
   {
      for (JournalFile dependency : dependencies)
      {
         fixDependency(originalFile, dependency);
      }
      
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected int incrementTransactionCounter(final long transactionID)
   {
      AtomicInteger counter = transactionCounter.get(transactionID);
      if (counter == null)
      {
         counter = new AtomicInteger(0);
         transactionCounter.put(transactionID, counter);
      }

      return counter.incrementAndGet();
   }

   protected int getTransactionCounter(final long transactionID)
   {
      AtomicInteger counter = transactionCounter.get(transactionID);
      if (counter == null)
      {
         return 0;
      }
      else
      {
         return counter.intValue();
      }
   }

   // Private -------------------------------------------------------
   private void fixDependency(final JournalFile originalFile, final JournalFile dependency) throws Exception
   {
      JournalReaderCallback txfix = new JournalReaderCallbackAbstract()
      {
         public void onReadCommitRecord(long transactionID, int numberOfRecords) throws Exception
         {
            if (transactionCounter.containsKey(transactionID))
            {
               dependency.incNegCount(originalFile);
            }
         }
         
         public void onReadRollbackRecord(long transactionID) throws Exception
         {
            if (transactionCounter.containsKey(transactionID))
            {
               dependency.incNegCount(originalFile);
            }
         }
      };
      
      JournalImpl.readJournalFile(fileFactory, dependency, txfix);
   }


   // Inner classes -------------------------------------------------

}
