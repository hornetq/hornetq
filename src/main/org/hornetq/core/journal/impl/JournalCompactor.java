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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.hornetq.core.buffers.HornetQBuffer;
import org.hornetq.core.buffers.HornetQBuffers;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.journal.impl.JournalImpl.JournalRecord;
import org.hornetq.core.logging.Logger;
import org.hornetq.utils.DataConstants;
import org.hornetq.utils.Pair;

/**
 * A JournalCompactor
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class JournalCompactor extends AbstractJournalUpdateTask
{

   private static final Logger log = Logger.getLogger(JournalCompactor.class);
   
   // Snapshot of transactions that were pending when the compactor started
   private final Map<Long, PendingTransaction> pendingTransactions = new ConcurrentHashMap<Long, PendingTransaction>();

   private final Map<Long, JournalRecord> newRecords = new HashMap<Long, JournalRecord>();

   private final Map<Long, JournalTransaction> newTransactions = new HashMap<Long, JournalTransaction>();

   /** Commands that happened during compacting
    *  We can't process any counts during compacting, as we won't know in what files the records are taking place, so
    *  we cache those updates. As soon as we are done we take the right account. */
   private final LinkedList<CompactCommand> pendingCommands = new LinkedList<CompactCommand>();

   public static SequentialFile readControlFile(final SequentialFileFactory fileFactory,
                                                final List<String> dataFiles,
                                                final List<String> newFiles,
                                                final List<Pair<String, String>> renameFile) throws Exception
   {
      SequentialFile controlFile = fileFactory.createSequentialFile(FILE_COMPACT_CONTROL, 1);

      if (controlFile.exists())
      {
         JournalFile file = new JournalFileImpl(controlFile, -1);

         final ArrayList<RecordInfo> records = new ArrayList<RecordInfo>();

         JournalImpl.readJournalFile(fileFactory, file, new JournalReaderCallbackAbstract()
         {
            @Override
            public void onReadAddRecord(final RecordInfo info) throws Exception
            {
               records.add(info);
            }
         });

         if (records.size() == 0)
         {
            return null;
         }
         else
         {
            HornetQBuffer input = HornetQBuffers.wrappedBuffer(records.get(0).data);

            int numberDataFiles = input.readInt();

            for (int i = 0; i < numberDataFiles; i++)
            {
               dataFiles.add(input.readUTF());
            }

            int numberNewFiles = input.readInt();

            for (int i = 0; i < numberNewFiles; i++)
            {
               newFiles.add(input.readUTF());
            }

            int numberRenames = input.readInt();
            for (int i = 0; i < numberRenames; i++)
            {
               String from = input.readUTF();
               String to = input.readUTF();
               renameFile.add(new Pair<String, String>(from, to));
            }

         }

         return controlFile;
      }
      else
      {
         return null;
      }
   }

   public List<JournalFile> getNewDataFiles()
   {
      return newDataFiles;
   }

   public Map<Long, JournalRecord> getNewRecords()
   {
      return newRecords;
   }

   public Map<Long, JournalTransaction> getNewTransactions()
   {
      return newTransactions;
   }

   public JournalCompactor(final SequentialFileFactory fileFactory,
                           final JournalImpl journal,
                           final Set<Long> recordsSnapshot,
                           final int firstFileID)
   {
      super(fileFactory, journal, recordsSnapshot, firstFileID);
   }

   /** This methods informs the Compactor about the existence of a pending (non committed) transaction */
   public void addPendingTransaction(final long transactionID, final long ids[])
   {
      pendingTransactions.put(transactionID, new PendingTransaction(ids));
   }

   /**
    * @param id
    * @param journalTransaction
    */
   public void addCommandCommit(final JournalTransaction liveTransaction, final JournalFile currentFile)
   {
      pendingCommands.add(new CommitCompactCommand(liveTransaction, currentFile));

      long ids[] = liveTransaction.getPositiveArray();

      PendingTransaction oldTransaction = pendingTransactions.get(liveTransaction.getId());
      long ids2[] = null;

      if (oldTransaction != null)
      {
         ids2 = oldTransaction.pendingIDs;
      }

      /** If a delete comes for these records, while the compactor still working, we need to be able to take them into account for later deletes
       *  instead of throwing exceptions about non existent records */
      if (ids != null)
      {
         for (long id : ids)
         {
            addToRecordsSnaptsho(id);
         }
      }

      if (ids2 != null)
      {
         for (long id : ids2)
         {
            addToRecordsSnaptsho(id);
         }
      }
   }

   public void addCommandRollback(final JournalTransaction liveTransaction, final JournalFile currentFile)
   {
      pendingCommands.add(new RollbackCompactCommand(liveTransaction, currentFile));
   }

   /**
    * @param id
    * @param usedFile
    */
   public void addCommandDelete(final long id, final JournalFile usedFile)
   {
      pendingCommands.add(new DeleteCompactCommand(id, usedFile));
   }

   /**
    * @param id
    * @param usedFile
    */
   public void addCommandUpdate(final long id, final JournalFile usedFile, final int size)
   {
      pendingCommands.add(new UpdateCompactCommand(id, usedFile, size));
   }

   private void checkSize(final int size) throws Exception
   {
      if (getWritingChannel() == null)
      {
         openFile();
      }
      else
      {
         if (getWritingChannel().writerIndex() + size > getWritingChannel().capacity())
         {
            openFile();
         }
      }
   }
   /**
    * Replay pending counts that happened during compacting
    */
   public void replayPendingCommands()
   {
      for (CompactCommand command : pendingCommands)
      {
         try
         {
            command.execute();
         }
         catch (Exception e)
         {
            log.warn("Error replaying pending commands after compacting", e);
         }
      }

      pendingCommands.clear();
   }

   // JournalReaderCallback implementation -------------------------------------------

   public void onReadAddRecord(final RecordInfo info) throws Exception
   {
      if (lookupRecord(info.id))
      {
         int size = JournalImpl.SIZE_ADD_RECORD + info.data.length;

         checkSize(size);

         JournalImpl.writeAddRecord(fileID,
                                    info.id,
                                    info.getUserRecordType(),
                                    new JournalImpl.ByteArrayEncoding(info.data),
                                    size,
                                    getWritingChannel());

         newRecords.put(info.id, new JournalRecord(currentFile, size));
      }
   }

   public void onReadAddRecordTX(final long transactionID, final RecordInfo info) throws Exception
   {
      if (pendingTransactions.get(transactionID) != null)
      {
         JournalTransaction newTransaction = getNewJournalTransaction(transactionID);

         int size = JournalImpl.SIZE_ADD_RECORD_TX + info.data.length;

         checkSize(size);

         newTransaction.addPositive(currentFile, info.id, size);

         JournalImpl.writeAddRecordTX(fileID,
                                      transactionID,
                                      info.id,
                                      info.getUserRecordType(),
                                      new JournalImpl.ByteArrayEncoding(info.data),
                                      size,
                                      getWritingChannel());
      }
      else
      {
         // Will try it as a regular record, the method addRecord will validate if this is a live record or not
         onReadAddRecord(info);
      }
   }

   public void onReadCommitRecord(final long transactionID, final int numberOfRecords) throws Exception
   {
      
      if (pendingTransactions.get(transactionID) != null)
      {
         // Sanity check, this should never happen
         throw new IllegalStateException("Inconsistency during compacting: CommitRecord ID = " + transactionID +
                                         " for an already committed transaction during compacting");
      }
   }

   public void onReadDeleteRecord(final long recordID) throws Exception
   {
      if (newRecords.get(recordID) != null)
      {
         // Sanity check, it should never happen
         throw new IllegalStateException("Inconsistency during compacting: Delete record being read on an existent record");
      }

   }

   public void onReadDeleteRecordTX(final long transactionID, final RecordInfo info) throws Exception
   {
      if (pendingTransactions.get(transactionID) != null)
      {
         JournalTransaction newTransaction = getNewJournalTransaction(transactionID);

         int size = JournalImpl.SIZE_DELETE_RECORD_TX + info.data.length;

         checkSize(size);

         JournalImpl.writeDeleteRecordTransactional(fileID,
                                                    transactionID,
                                                    info.id,
                                                    new JournalImpl.ByteArrayEncoding(info.data),
                                                    size,
                                                    getWritingChannel());

         newTransaction.addNegative(currentFile, info.id);
      }
      // else.. nothing to be done
   }

   public void markAsDataFile(final JournalFile file)
   {
      // nothing to be done here
   }

   public void onReadPrepareRecord(final long transactionID, final byte[] extraData, final int numberOfRecords) throws Exception
   {
      if (pendingTransactions.get(transactionID) != null)
      {

         JournalTransaction newTransaction = getNewJournalTransaction(transactionID);

         int size = JournalImpl.SIZE_COMPLETE_TRANSACTION_RECORD + extraData.length + DataConstants.SIZE_INT;

         checkSize(size);

         JournalImpl.writeTransaction(fileID,
                                      JournalImpl.PREPARE_RECORD,
                                      transactionID,
                                      new JournalImpl.ByteArrayEncoding(extraData),
                                      size,
                                      newTransaction.getCounter(currentFile),
                                      getWritingChannel());

         newTransaction.prepare(currentFile);

      }
   }

   public void onReadRollbackRecord(final long transactionID) throws Exception
   {
      if (pendingTransactions.get(transactionID) != null)
      {
         // Sanity check, this should never happen
         throw new IllegalStateException("Inconsistency during compacting: RollbackRecord ID = " + transactionID +
                                         " for an already rolled back transaction during compacting");
      }
   }

   public void onReadUpdateRecord(final RecordInfo info) throws Exception
   {
      if (lookupRecord(info.id))
      {
         int size = JournalImpl.SIZE_UPDATE_RECORD + info.data.length;

         checkSize(size);

         JournalRecord newRecord = newRecords.get(info.id);

         if (newRecord == null)
         {
            log.warn("Couldn't find addRecord information for record " + info.id + " during compacting");
         }
         else
         {
            newRecord.addUpdateFile(currentFile, size);
         }

         JournalImpl.writeUpdateRecord(fileID,
                                       info.id,
                                       info.userRecordType,
                                       new JournalImpl.ByteArrayEncoding(info.data),
                                       size,
                                       getWritingChannel());

      }
   }

   public void onReadUpdateRecordTX(final long transactionID, final RecordInfo info) throws Exception
   {
      if (pendingTransactions.get(transactionID) != null)
      {
         JournalTransaction newTransaction = getNewJournalTransaction(transactionID);

         int size = JournalImpl.SIZE_UPDATE_RECORD_TX + info.data.length;

         checkSize(size);

         JournalImpl.writeUpdateRecordTX(fileID,
                                         transactionID,
                                         info.id,
                                         info.userRecordType,
                                         new JournalImpl.ByteArrayEncoding(info.data),
                                         size,
                                         getWritingChannel());

         newTransaction.addPositive(currentFile, info.id, size);
      }
      else
      {
         onReadUpdateRecord(info);
      }
   }

   /**
    * @param transactionID
    * @return
    */
   private JournalTransaction getNewJournalTransaction(final long transactionID)
   {
      JournalTransaction newTransaction = newTransactions.get(transactionID);
      if (newTransaction == null)
      {
         newTransaction = new JournalTransaction(transactionID, journal);
         newTransactions.put(transactionID, newTransaction);
      }
      return newTransaction;
   }

   private static abstract class CompactCommand
   {
      abstract void execute() throws Exception;
   }

   private class DeleteCompactCommand extends CompactCommand
   {
      long id;

      JournalFile usedFile;

      public DeleteCompactCommand(final long id, final JournalFile usedFile)
      {
         this.id = id;
         this.usedFile = usedFile;
      }

      @Override
      void execute() throws Exception
      {
         JournalRecord deleteRecord = journal.getRecords().remove(id);
         deleteRecord.delete(usedFile);
      }
   }

   private static class PendingTransaction
   {
      long pendingIDs[];

      PendingTransaction(final long ids[])
      {
         pendingIDs = ids;
      }

   }

   private class UpdateCompactCommand extends CompactCommand
   {
      private long id;

      private JournalFile usedFile;

      private final int size;

      public UpdateCompactCommand(final long id, final JournalFile usedFile, final int size)
      {
         this.id = id;
         this.usedFile = usedFile;
         this.size = size;
      }

      @Override
      void execute() throws Exception
      {
         JournalRecord updateRecord = journal.getRecords().get(id);
         updateRecord.addUpdateFile(usedFile, size);
      }
   }

   private class CommitCompactCommand extends CompactCommand
   {
      private final JournalTransaction liveTransaction;

      /** File containing the commit record */
      private final JournalFile commitFile;

      public CommitCompactCommand(final JournalTransaction liveTransaction, final JournalFile commitFile)
      {
         this.liveTransaction = liveTransaction;
         this.commitFile = commitFile;
      }

      @Override
      void execute() throws Exception
      {
         JournalTransaction newTransaction = newTransactions.get(liveTransaction.getId());
         if (newTransaction != null)
         {
            liveTransaction.merge(newTransaction);
            liveTransaction.commit(commitFile);
         }
         newTransactions.remove(liveTransaction.getId());
      }
   }

   private class RollbackCompactCommand extends CompactCommand
   {
      private final JournalTransaction liveTransaction;

      /** File containing the commit record */
      private final JournalFile rollbackFile;

      public RollbackCompactCommand(final JournalTransaction liveTransaction, final JournalFile rollbackFile)
      {
         this.liveTransaction = liveTransaction;
         this.rollbackFile = rollbackFile;
      }

      @Override
      void execute() throws Exception
      {
         JournalTransaction newTransaction = newTransactions.get(liveTransaction.getId());
         if (newTransaction != null)
         {
            liveTransaction.merge(newTransaction);
            liveTransaction.rollback(rollbackFile);
         }
         newTransactions.remove(liveTransaction.getId());
      }
   }

}
