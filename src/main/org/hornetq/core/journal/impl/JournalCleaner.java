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
import org.hornetq.core.journal.impl.dataformat.ByteArrayEncoding;
import org.hornetq.core.journal.impl.dataformat.JournalAddRecord;
import org.hornetq.core.journal.impl.dataformat.JournalAddRecordTX;
import org.hornetq.core.journal.impl.dataformat.JournalCompleteRecordTX;
import org.hornetq.core.journal.impl.dataformat.JournalDeleteRecord;
import org.hornetq.core.journal.impl.dataformat.JournalDeleteRecordTX;
import org.hornetq.core.journal.impl.dataformat.JournalRollbackRecordTX;

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
                            final long nextOrderingID) throws Exception
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
         writeEncoder(new JournalAddRecord(true, info.id, info.getUserRecordType(), new ByteArrayEncoding(info.data)));
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

         writeEncoder(new JournalAddRecordTX(true,
                                             transactionID,
                                             recordInfo.id,
                                             recordInfo.getUserRecordType(),
                                             new ByteArrayEncoding(recordInfo.data)));
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.impl.JournalReaderCallback#onReadCommitRecord(long, int)
    */
   public void onReadCommitRecord(final long transactionID, final int numberOfRecords) throws Exception
   {
      int txcounter = getTransactionCounter(transactionID);

      writeEncoder(new JournalCompleteRecordTX(true, transactionID, null), txcounter);
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.impl.JournalReaderCallback#onReadDeleteRecord(long)
    */
   public void onReadDeleteRecord(final long recordID) throws Exception
   {
      writeEncoder(new JournalDeleteRecord(recordID));
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.impl.JournalReaderCallback#onReadDeleteRecordTX(long, org.hornetq.core.journal.RecordInfo)
    */
   public void onReadDeleteRecordTX(final long transactionID, final RecordInfo recordInfo) throws Exception
   {
      incrementTransactionCounter(transactionID);

      writeEncoder(new JournalDeleteRecordTX(transactionID, recordInfo.id, new ByteArrayEncoding(recordInfo.data)));
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.impl.JournalReaderCallback#onReadPrepareRecord(long, byte[], int)
    */
   public void onReadPrepareRecord(final long transactionID, final byte[] extraData, final int numberOfRecords) throws Exception
   {
      int txcounter = getTransactionCounter(transactionID);

      writeEncoder(new JournalCompleteRecordTX(false, transactionID, new ByteArrayEncoding(extraData)), txcounter);
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.impl.JournalReaderCallback#onReadRollbackRecord(long)
    */
   public void onReadRollbackRecord(final long transactionID) throws Exception
   {
      writeEncoder(new JournalRollbackRecordTX(transactionID));
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.impl.JournalReaderCallback#onReadUpdateRecord(org.hornetq.core.journal.RecordInfo)
    */
   public void onReadUpdateRecord(final RecordInfo recordInfo) throws Exception
   {
      if (lookupRecord(recordInfo.id))
      {
         writeEncoder(new JournalAddRecord(false,
                                           recordInfo.id,
                                           recordInfo.userRecordType,
                                           new ByteArrayEncoding(recordInfo.data)));
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

         writeEncoder(new JournalAddRecordTX(false,
                                             transactionID,
                                             recordInfo.id,
                                             recordInfo.userRecordType,
                                             new ByteArrayEncoding(recordInfo.data)));
      }
   }

   /**
    * Read files that depend on this file.
    * Commits and rollbacks are also counted as negatives. We need to fix those also.
    * @param dependencies
    */
   public void fixDependencies(final JournalFile originalFile, final ArrayList<JournalFile> dependencies) throws Exception
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
         @Override
         public void onReadCommitRecord(final long transactionID, final int numberOfRecords) throws Exception
         {
            if (transactionCounter.containsKey(transactionID))
            {
               dependency.incNegCount(originalFile);
            }
         }

         @Override
         public void onReadRollbackRecord(final long transactionID) throws Exception
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
