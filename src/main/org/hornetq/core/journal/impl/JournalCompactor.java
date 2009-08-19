/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.hornetq.core.journal.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.hornetq.core.buffers.ChannelBuffer;
import org.hornetq.core.buffers.ChannelBuffers;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.journal.impl.JournalImpl.JournalRecord;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.spi.MessagingBuffer;
import org.hornetq.utils.ConcurrentHashSet;
import org.hornetq.utils.DataConstants;

/**
 * A JournalCompactor
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class JournalCompactor implements JournalReaderCallback
{

   private static final String FILE_COMPACT_CONTROL = "journal-rename-control.ctr";

   private static final Logger log = Logger.getLogger(JournalCompactor.class);

   private final JournalImpl journal;

   private final SequentialFileFactory fileFactory;

   private JournalFile currentFile;

   private SequentialFile sequentialFile;

   private int fileID;

   private ChannelBuffer writingChannel;

   private int nextOrderingID;

   private final List<JournalFile> newDataFiles = new ArrayList<JournalFile>();

   private final Set<Long> recordsSnapshot = new ConcurrentHashSet<Long>();;

   // Snapshot of transactions that were pending when the compactor started
   private final Map<Long, PendingTransaction> pendingTransactions = new ConcurrentHashMap<Long, PendingTransaction>();

   private final Map<Long, JournalRecord> newRecords = new HashMap<Long, JournalRecord>();

   private final Map<Long, JournalTransaction> newTransactions = new HashMap<Long, JournalTransaction>();

   /** Commands that happened during compacting
    *  We can't process any counts during compacting, as we won't know in what files the records are taking place, so
    *  we cache those updates. As soon as we are done we take the right account. */
   private final LinkedList<CompactCommand> pendingCommands = new LinkedList<CompactCommand>();

   /**
    * @param tmpRenameFile
    * @param files
    * @param newFiles
    */
   public static SequentialFile writeControlFile(final SequentialFileFactory fileFactory,
                                                 final List<JournalFile> files,
                                                 final List<JournalFile> newFiles) throws Exception
   {

      SequentialFile controlFile = fileFactory.createSequentialFile(FILE_COMPACT_CONTROL, 1);

      try
      {
         controlFile.open(1);

         ChannelBuffer renameBuffer = ChannelBuffers.dynamicBuffer(1);

         renameBuffer.writeInt(-1);
         renameBuffer.writeInt(-1);

         MessagingBuffer filesToRename = ChannelBuffers.dynamicBuffer(1);

         // DataFiles first

         filesToRename.writeInt(files.size());

         for (JournalFile file : files)
         {
            filesToRename.writeUTF(file.getFile().getFileName());
         }

         filesToRename.writeInt(newFiles.size());

         for (JournalFile file : newFiles)
         {
            filesToRename.writeUTF(file.getFile().getFileName());
         }

         JournalImpl.writeAddRecord(-1,
                                    1,
                                    (byte)0,
                                    new JournalImpl.ByteArrayEncoding(filesToRename.array()),
                                    JournalImpl.SIZE_ADD_RECORD + filesToRename.array().length,
                                    renameBuffer);

         ByteBuffer writeBuffer = fileFactory.newBuffer(renameBuffer.writerIndex());

         writeBuffer.put(renameBuffer.array(), 0, renameBuffer.writerIndex());

         writeBuffer.rewind();

         controlFile.write(writeBuffer, true);

         return controlFile;
      }
      finally
      {
         controlFile.close();
      }
   }

   public static SequentialFile readControlFile(final SequentialFileFactory fileFactory,
                                                final List<String> dataFiles,
                                                final List<String> newFiles) throws Exception
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
            ChannelBuffer input = ChannelBuffers.wrappedBuffer(records.get(0).data);

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
      this.fileFactory = fileFactory;
      this.journal = journal;
      this.recordsSnapshot.addAll(recordsSnapshot);
      nextOrderingID = firstFileID;
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
            recordsSnapshot.add(id);
         }
      }

      if (ids2 != null)
      {
         for (long id : ids2)
         {
            recordsSnapshot.add(id);
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

   public boolean lookupRecord(final long id)
   {
      return recordsSnapshot.contains(id);
   }

   private void checkSize(final int size) throws Exception
   {
      if (writingChannel == null)
      {
         openFile();
      }
      else
      {
         if (writingChannel.writerIndex() + size > writingChannel.capacity())
         {
            openFile();
         }
      }
   }

   /** Write pending output into file */
   public void flush() throws Exception
   {
      if (writingChannel != null)
      {
         sequentialFile.position(0);
         sequentialFile.write(writingChannel.toByteBuffer(), true);
         sequentialFile.close();
         newDataFiles.add(currentFile);
      }

      writingChannel = null;
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
      if (recordsSnapshot.contains(info.id))
      {
         int size = JournalImpl.SIZE_ADD_RECORD + info.data.length;

         checkSize(size);

         JournalImpl.writeAddRecord(fileID,
                                    info.id,
                                    info.getUserRecordType(),
                                    new JournalImpl.ByteArrayEncoding(info.data),
                                    size,
                                    writingChannel);

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
                                      writingChannel);
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
                                                    writingChannel);

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
                                      newTransaction,
                                      new JournalImpl.ByteArrayEncoding(extraData),
                                      size,
                                      newTransaction.getCounter(currentFile),
                                      writingChannel);

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
      if (recordsSnapshot.contains(info.id))
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
                                       writingChannel);

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
                                         writingChannel);

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

   /**
    * @throws Exception
    */
   private void openFile() throws Exception
   {
      flush();

      ByteBuffer bufferWrite = fileFactory.newBuffer(journal.getFileSize());
      writingChannel = ChannelBuffers.wrappedBuffer(bufferWrite);

      currentFile = journal.getFile(false, false, false);
      sequentialFile = currentFile.getFile();
      sequentialFile.renameTo(sequentialFile.getFileName() + ".cmp");
            
      sequentialFile.open(1);
      fileID = nextOrderingID++;
      currentFile = new JournalFileImpl(sequentialFile, fileID);

      writingChannel.writeInt(fileID);
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
