package org.hornetq.core.journal.impl;

import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hornetq.api.core.HornetQException;
import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.core.journal.IOCompletion;
import org.hornetq.core.journal.Journal;
import org.hornetq.core.journal.JournalLoadInformation;
import org.hornetq.core.journal.LoaderCallback;
import org.hornetq.core.journal.PreparedTransactionInfo;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.TransactionFailureCallback;
import org.hornetq.core.journal.impl.dataformat.ByteArrayEncoding;
import org.hornetq.core.journal.impl.dataformat.JournalAddRecord;
import org.hornetq.core.journal.impl.dataformat.JournalInternalRecord;

/**
 * Journal used at a replicating backup server during the synchronization of data with the 'live'
 * server.
 * <p>
 * Its main purpose is to store the data like a Journal would but without verifying records.
 */
public class ReplicatingJournal implements Journal
{
   private final ReentrantLock lockAppend = new ReentrantLock();
   private final ReadWriteLock journalLock = new ReentrantReadWriteLock();

   private final JournalFile file;
   private final boolean hasCallbackSupport;

   /**
    * @param file
    */
   public ReplicatingJournal(JournalFile file, boolean hasCallbackSupport)
   {
      this.file = file;
      this.hasCallbackSupport = hasCallbackSupport;
   }

   @Override
   public void start() throws Exception
   {
      throw new HornetQException(HornetQException.UNSUPPORTED_PACKET);
   }

   @Override
   public void stop() throws Exception
   {
      throw new HornetQException(HornetQException.UNSUPPORTED_PACKET);
   }

   @Override
   public boolean isStarted()
   {
      throw new UnsupportedOperationException();
   }

   // ------------------------
   @Override
   public void appendAddRecord(long id, byte recordType, byte[] record, boolean sync) throws Exception
   {
      appendAddRecord(id, recordType, new ByteArrayEncoding(record), sync);
   }

   @Override
   public void appendAddRecord(long id, byte recordType, byte[] record, boolean sync, IOCompletion completionCallback)
            throws Exception
   {
      appendAddRecord(id, recordType, new ByteArrayEncoding(record), sync, completionCallback);
   }

   @Override
   public void appendAddRecord(long id, byte recordType, EncodingSupport record, boolean sync) throws Exception
   {
      SyncIOCompletion callback = getSyncCallback(sync);

      appendAddRecord(id, recordType, record, sync, callback);

      if (callback != null)
      {
         callback.waitCompletion();
      }
   }

   // ------------------------

   private void readLockJournal()
   {
      journalLock.readLock().lock();
   }

   private void readUnlockJournal()
   {
      journalLock.readLock().unlock();
   }

   @Override
   public void appendAddRecord(long id, byte recordType, EncodingSupport record, boolean sync,
            IOCompletion callback) throws Exception
   {
      JournalInternalRecord addRecord = new JournalAddRecord(true, id, recordType, record);

         if (callback != null)
         {
            callback.storeLineUp();
         }

         lockAppend.lock();
         try
         {
            JournalFile usedFile = appendRecord(addRecord, false, sync, null, callback);
         }
         finally
         {
            lockAppend.unlock();
         }

   }

   /**
    * @param addRecord
    * @param b
    * @param sync
    * @param object
    * @param callback
    * @return
    */
   private JournalFile appendRecord(JournalInternalRecord addRecord, boolean b, boolean sync, Object object,
            IOCompletion callback)
   {
      // TODO Auto-generated method stub
      return null;
   }

   @Override
   public void appendUpdateRecord(long id, byte recordType, byte[] record, boolean sync) throws Exception
   {
      throw new HornetQException(HornetQException.UNSUPPORTED_PACKET);
   }

   @Override
   public void
            appendUpdateRecord(long id, byte recordType, byte[] record, boolean sync, IOCompletion completionCallback)
                     throws Exception
   {
      throw new HornetQException(HornetQException.UNSUPPORTED_PACKET);
   }

   @Override
   public void appendUpdateRecord(long id, byte recordType, EncodingSupport record, boolean sync) throws Exception
   {
      throw new HornetQException(HornetQException.UNSUPPORTED_PACKET);
   }

   @Override
   public void appendUpdateRecord(long id, byte recordType, EncodingSupport record, boolean sync,
            IOCompletion completionCallback) throws Exception
   {
      throw new HornetQException(HornetQException.UNSUPPORTED_PACKET);
   }

   @Override
   public void appendDeleteRecord(long id, boolean sync) throws Exception
   {
      throw new HornetQException(HornetQException.UNSUPPORTED_PACKET);
   }

   @Override
   public void appendDeleteRecord(long id, boolean sync, IOCompletion completionCallback) throws Exception
   {
      throw new HornetQException(HornetQException.UNSUPPORTED_PACKET);
   }

   @Override
   public void appendAddRecordTransactional(long txID, long id, byte recordType, byte[] record) throws Exception
   {
      throw new HornetQException(HornetQException.UNSUPPORTED_PACKET);
   }

   @Override
   public void appendAddRecordTransactional(long txID, long id, byte recordType, EncodingSupport record)
            throws Exception
   {
      throw new HornetQException(HornetQException.UNSUPPORTED_PACKET);
   }

   @Override
   public void appendUpdateRecordTransactional(long txID, long id, byte recordType, byte[] record) throws Exception
   {
      throw new HornetQException(HornetQException.UNSUPPORTED_PACKET);
   }

   @Override
   public void appendUpdateRecordTransactional(long txID, long id, byte recordType, EncodingSupport record)
            throws Exception
   {
      throw new HornetQException(HornetQException.UNSUPPORTED_PACKET);
   }

   @Override
   public void appendDeleteRecordTransactional(long txID, long id, byte[] record) throws Exception
   {
      throw new HornetQException(HornetQException.UNSUPPORTED_PACKET);
   }

   @Override
   public void appendDeleteRecordTransactional(long txID, long id, EncodingSupport record) throws Exception
   {
      throw new HornetQException(HornetQException.UNSUPPORTED_PACKET);
   }

   @Override
   public void appendDeleteRecordTransactional(long txID, long id) throws Exception
   {
      throw new HornetQException(HornetQException.UNSUPPORTED_PACKET);
   }

   @Override
   public void appendCommitRecord(long txID, boolean sync) throws Exception
   {
      throw new HornetQException(HornetQException.UNSUPPORTED_PACKET);
   }

   @Override
   public void appendCommitRecord(long txID, boolean sync, IOCompletion callback) throws Exception
   {
      throw new HornetQException(HornetQException.UNSUPPORTED_PACKET);
   }

   @Override
   public void appendCommitRecord(long txID, boolean sync, IOCompletion callback, boolean lineUpContext)
            throws Exception
   {
      throw new HornetQException(HornetQException.UNSUPPORTED_PACKET);
   }

   @Override
   public void appendPrepareRecord(long txID, EncodingSupport transactionData, boolean sync) throws Exception
   {
      throw new HornetQException(HornetQException.UNSUPPORTED_PACKET);
   }

   @Override
   public void appendPrepareRecord(long txID, EncodingSupport transactionData, boolean sync, IOCompletion callback)
            throws Exception
   {
      throw new HornetQException(HornetQException.UNSUPPORTED_PACKET);
   }

   @Override
   public void appendPrepareRecord(long txID, byte[] transactionData, boolean sync) throws Exception
   {
      throw new HornetQException(HornetQException.UNSUPPORTED_PACKET);
   }

   @Override
   public void appendPrepareRecord(long txID, byte[] transactionData, boolean sync, IOCompletion callback)
            throws Exception
   {
      throw new HornetQException(HornetQException.UNSUPPORTED_PACKET);
   }

   @Override
   public void appendRollbackRecord(long txID, boolean sync) throws Exception
   {
      throw new HornetQException(HornetQException.UNSUPPORTED_PACKET);
   }

   @Override
   public void appendRollbackRecord(long txID, boolean sync, IOCompletion callback) throws Exception
   {
      throw new HornetQException(HornetQException.UNSUPPORTED_PACKET);
   }

   @Override
   public JournalLoadInformation load(LoaderCallback reloadManager) throws Exception
   {
      throw new HornetQException(HornetQException.UNSUPPORTED_PACKET);
   }

   @Override
   public JournalLoadInformation loadInternalOnly() throws Exception
   {
      throw new HornetQException(HornetQException.UNSUPPORTED_PACKET);
   }

   @Override
   public void lineUpContex(IOCompletion callback)
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public JournalLoadInformation load(List<RecordInfo> committedRecords,
            List<PreparedTransactionInfo> preparedTransactions, TransactionFailureCallback transactionFailure)
            throws Exception
   {
      throw new HornetQException(HornetQException.UNSUPPORTED_PACKET);
   }

   @Override
   public int getAlignment() throws Exception
   {
      throw new HornetQException(HornetQException.UNSUPPORTED_PACKET);
   }

   @Override
   public int getNumberOfRecords()
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public int getUserVersion()
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void perfBlast(int pages)
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void runDirectJournalBlast() throws Exception
   {
      throw new UnsupportedOperationException();
   }

   private SyncIOCompletion getSyncCallback(final boolean sync)
   {
      if (hasCallbackSupport)
      {
         if (sync)
         {
            return new SimpleWaitIOCallback();
         }
         return DummyCallback.getInstance();
      }
      return null;
   }
}
