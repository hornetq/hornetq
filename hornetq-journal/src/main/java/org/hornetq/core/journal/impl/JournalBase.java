package org.hornetq.core.journal.impl;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.core.journal.IOCompletion;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.journal.impl.dataformat.ByteArrayEncoding;
import org.hornetq.core.logging.Logger;

abstract class JournalBase
{

   protected final JournalFilesRepository filesRepository;
   protected final SequentialFileFactory fileFactory;
   protected volatile JournalFile currentFile;
   protected final int fileSize;

   private static final Logger log = Logger.getLogger(JournalBase.class);
   private static final boolean trace = log.isTraceEnabled();

   public JournalBase(SequentialFileFactory fileFactory, JournalFilesRepository journalFilesRepository, int fileSize)
   {
      if (fileSize < JournalImpl.MIN_FILE_SIZE)
      {
         throw new IllegalArgumentException("File size cannot be less than " + JournalImpl.MIN_FILE_SIZE + " bytes");
      }
      if (fileSize % fileFactory.getAlignment() != 0)
      {
         throw new IllegalArgumentException("Invalid journal-file-size " + fileSize + ", It should be multiple of " +
                  fileFactory.getAlignment());
      }
      this.fileFactory = fileFactory;
      this.filesRepository = journalFilesRepository;
      this.fileSize = fileSize;
   }

   abstract public void appendAddRecord(final long id, final byte recordType, final EncodingSupport record,
            final boolean sync, final IOCompletion callback) throws Exception;

   abstract public void appendAddRecordTransactional(final long txID, final long id, final byte recordType,
            final EncodingSupport record) throws Exception;

   abstract public void appendCommitRecord(final long txID, final boolean sync, final IOCompletion callback,
            boolean lineUpContext) throws Exception;

   abstract public void appendDeleteRecord(final long id, final boolean sync, final IOCompletion callback)
            throws Exception;

   abstract public void appendDeleteRecordTransactional(final long txID, final long id, final EncodingSupport record)
            throws Exception;

   abstract public void appendPrepareRecord(final long txID, final EncodingSupport transactionData, final boolean sync,
            final IOCompletion callback) throws Exception;

   abstract public void appendUpdateRecord(final long id, final byte recordType, final EncodingSupport record,
            final boolean sync, final IOCompletion callback) throws Exception;

   abstract public void appendUpdateRecordTransactional(final long txID, final long id, final byte recordType,
            final EncodingSupport record) throws Exception;

   abstract public void appendRollbackRecord(final long txID, final boolean sync, final IOCompletion callback)
            throws Exception;


   public void appendAddRecord(long id, byte recordType, byte[] record, boolean sync) throws Exception
   {
      appendAddRecord(id, recordType, new ByteArrayEncoding(record), sync);
   }

   public void appendAddRecord(long id, byte recordType, byte[] record, boolean sync, IOCompletion completionCallback)
            throws Exception
   {
      appendAddRecord(id, recordType, new ByteArrayEncoding(record), sync, completionCallback);
   }

   public void appendAddRecord(long id, byte recordType, EncodingSupport record, boolean sync) throws Exception
   {
      SyncIOCompletion callback = getSyncCallback(sync);

      appendAddRecord(id, recordType, record, sync, callback);

      if (callback != null)
      {
         callback.waitCompletion();
      }
   }

   public void appendCommitRecord(final long txID, final boolean sync) throws Exception
   {
      SyncIOCompletion syncCompletion = getSyncCallback(sync);

      appendCommitRecord(txID, sync, syncCompletion, true);

      if (syncCompletion != null)
      {
         syncCompletion.waitCompletion();
      }
   }

   public void appendCommitRecord(final long txID, final boolean sync, final IOCompletion callback) throws Exception
   {
      appendCommitRecord(txID, sync, callback, true);
   }
   public void appendUpdateRecord(final long id, final byte recordType, final byte[] record, final boolean sync)
            throws Exception
   {
      appendUpdateRecord(id, recordType, new ByteArrayEncoding(record), sync);
   }

   public void appendUpdateRecordTransactional(final long txID, final long id, final byte recordType,
            final byte[] record) throws Exception
   {
      appendUpdateRecordTransactional(txID, id, recordType, new ByteArrayEncoding(record));
   }

   public void appendUpdateRecord(final long id, final byte recordType, final byte[] record, final boolean sync,
            final IOCompletion callback) throws Exception
   {
      appendUpdateRecord(id, recordType, new ByteArrayEncoding(record), sync, callback);
   }

   public void appendAddRecordTransactional(final long txID, final long id, final byte recordType, final byte[] record)
            throws Exception
   {
      appendAddRecordTransactional(txID, id, recordType, new ByteArrayEncoding(record));
   }

   public void appendDeleteRecordTransactional(final long txID, final long id) throws Exception
   {
      appendDeleteRecordTransactional(txID, id, NullEncoding.instance);
   }

   public void appendPrepareRecord(final long txID, final byte[] transactionData, final boolean sync,
            final IOCompletion completion) throws Exception
   {
      appendPrepareRecord(txID, new ByteArrayEncoding(transactionData), sync, completion);
   }

   public void appendPrepareRecord(final long txID, final byte[] transactionData, final boolean sync) throws Exception
   {
      appendPrepareRecord(txID, new ByteArrayEncoding(transactionData), sync);
   }

   public void appendPrepareRecord(final long txID, final EncodingSupport transactionData, final boolean sync)
            throws Exception
   {
      SyncIOCompletion syncCompletion = getSyncCallback(sync);

      appendPrepareRecord(txID, transactionData, sync, syncCompletion);

      if (syncCompletion != null)
      {
         syncCompletion.waitCompletion();
      }
   }

   public void appendDeleteRecordTransactional(final long txID, final long id, final byte[] record) throws Exception
   {
      appendDeleteRecordTransactional(txID, id, new ByteArrayEncoding(record));
   }

   public void
            appendUpdateRecord(final long id, final byte recordType, final EncodingSupport record, final boolean sync)
                     throws Exception
   {
      SyncIOCompletion callback = getSyncCallback(sync);

      appendUpdateRecord(id, recordType, record, sync, callback);

      if (callback != null)
      {
         callback.waitCompletion();
      }
   }

   public void appendRollbackRecord(final long txID, final boolean sync) throws Exception
   {
      SyncIOCompletion syncCompletion = getSyncCallback(sync);

      appendRollbackRecord(txID, sync, syncCompletion);

      if (syncCompletion != null)
      {
         syncCompletion.waitCompletion();
      }

   }

   public void appendDeleteRecord(final long id, final boolean sync) throws Exception
   {
      SyncIOCompletion callback = getSyncCallback(sync);

      appendDeleteRecord(id, sync, callback);

      if (callback != null)
      {
         callback.waitCompletion();
      }
   }

   /**
    * @param size
    * @throws Exception
    */
   protected void switchFileIfNecessary(int size) throws Exception
   {
      // We take into account the fileID used on the Header
      if (size > fileSize - currentFile.getFile().calculateBlockStart(JournalImpl.SIZE_HEADER))
      {
         throw new IllegalArgumentException("Record is too large to store " + size);
      }

      if (!currentFile.getFile().fits(size))
      {
         moveNextFile(true);

         // The same check needs to be done at the new file also
         if (!currentFile.getFile().fits(size))
         {
            // Sanity check, this should never happen
            throw new IllegalStateException("Invalid logic on buffer allocation");
         }
      }
   }

   abstract void scheduleReclaim();

   // You need to guarantee lock.acquire() before calling this method
   protected void moveNextFile(final boolean scheduleReclaim) throws Exception
   {
      filesRepository.closeFile(currentFile);

      currentFile = filesRepository.openFile();

      if (scheduleReclaim)
      {
         scheduleReclaim();
      }

      if (trace)
      {
         log.info("moveNextFile: " + currentFile);
      }

      fileFactory.activateBuffer(currentFile.getFile());
   }

   protected SyncIOCompletion getSyncCallback(final boolean sync)
   {
      if (fileFactory.isSupportsCallbacks())
      {
         if (sync)
         {
            return new SimpleWaitIOCallback();
         }
         return DummyCallback.getInstance();
      }
      return null;
   }

   private static class NullEncoding implements EncodingSupport
   {

      private static NullEncoding instance = new NullEncoding();

      public void decode(final HornetQBuffer buffer)
      {
         // no-op
      }

      public void encode(final HornetQBuffer buffer)
      {
         // no-op
      }

      public int getEncodeSize()
      {
         return 0;
      }
   }

   /**
    * @param lastDataPos
    * @throws Exception
    */
   protected void setUpCurrentFile(int lastDataPos) throws Exception
   {
      // Create any more files we need

      filesRepository.ensureMinFiles();

      // The current file is the last one that has data

      currentFile = filesRepository.pollLastDataFile();

      if (currentFile != null)
      {
         currentFile.getFile().open();

         currentFile.getFile().position(currentFile.getFile().calculateBlockStart(lastDataPos));
      }
      else
      {
         currentFile = filesRepository.getFreeFile();

         filesRepository.openFile(currentFile, true);
      }

      fileFactory.activateBuffer(currentFile.getFile());

      filesRepository.pushOpenedFile();
   }

   public int getFileSize()
   {
      return fileSize;
   }
}
