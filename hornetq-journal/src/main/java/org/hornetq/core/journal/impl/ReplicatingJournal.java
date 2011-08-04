package org.hornetq.core.journal.impl;

import java.util.List;

import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.core.journal.IOCompletion;
import org.hornetq.core.journal.Journal;
import org.hornetq.core.journal.JournalLoadInformation;
import org.hornetq.core.journal.LoaderCallback;
import org.hornetq.core.journal.PreparedTransactionInfo;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.TransactionFailureCallback;

/**
 * Journal used at a replicating backup server during the synchronization of data with the 'live'
 * server.
 * <p>
 * Its main purpose is to store the data like a Journal would but without verifying records.
 */
public class ReplicatingJournal implements Journal
{

   private final JournalFile file;

   /**
    * @param file
    */
   public ReplicatingJournal(JournalFile file)
   {
      this.file = file;
   }

   @Override
   public void start() throws Exception
   {
      // TODO Auto-generated method stub

   }

   @Override
   public void stop() throws Exception
   {
      // TODO Auto-generated method stub

   }

   @Override
   public boolean isStarted()
   {
      // TODO Auto-generated method stub
      return false;
   }

   @Override
   public void appendAddRecord(long id, byte recordType, byte[] record, boolean sync) throws Exception
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void appendAddRecord(long id, byte recordType, byte[] record, boolean sync, IOCompletion completionCallback)
                                                                                                                   throws Exception
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void appendAddRecord(long id, byte recordType, EncodingSupport record, boolean sync) throws Exception
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void appendAddRecord(long id, byte recordType, EncodingSupport record, boolean sync,
                               IOCompletion completionCallback) throws Exception
   {
      throw new UnsupportedOperationException();

   }

   @Override
   public void appendUpdateRecord(long id, byte recordType, byte[] record, boolean sync) throws Exception
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void
            appendUpdateRecord(long id, byte recordType, byte[] record, boolean sync, IOCompletion completionCallback)
                     throws Exception
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void appendUpdateRecord(long id, byte recordType, EncodingSupport record, boolean sync) throws Exception
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void appendUpdateRecord(long id, byte recordType, EncodingSupport record, boolean sync,
                                  IOCompletion completionCallback) throws Exception
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void appendDeleteRecord(long id, boolean sync) throws Exception
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void appendDeleteRecord(long id, boolean sync, IOCompletion completionCallback) throws Exception
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void appendAddRecordTransactional(long txID, long id, byte recordType, byte[] record) throws Exception
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void appendAddRecordTransactional(long txID, long id, byte recordType, EncodingSupport record)
            throws Exception
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void appendUpdateRecordTransactional(long txID, long id, byte recordType, byte[] record) throws Exception
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void appendUpdateRecordTransactional(long txID, long id, byte recordType, EncodingSupport record)
            throws Exception
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void appendDeleteRecordTransactional(long txID, long id, byte[] record) throws Exception
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void appendDeleteRecordTransactional(long txID, long id, EncodingSupport record) throws Exception
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void appendDeleteRecordTransactional(long txID, long id) throws Exception
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void appendCommitRecord(long txID, boolean sync) throws Exception
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void appendCommitRecord(long txID, boolean sync, IOCompletion callback) throws Exception
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void
            appendCommitRecord(long txID, boolean sync, IOCompletion callback, boolean lineUpContext) throws Exception
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void appendPrepareRecord(long txID, EncodingSupport transactionData, boolean sync) throws Exception
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void appendPrepareRecord(long txID, EncodingSupport transactionData, boolean sync, IOCompletion callback)
            throws Exception
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void appendPrepareRecord(long txID, byte[] transactionData, boolean sync) throws Exception
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void
            appendPrepareRecord(long txID, byte[] transactionData, boolean sync, IOCompletion callback)
                                                                                                       throws Exception
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void appendRollbackRecord(long txID, boolean sync) throws Exception
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void appendRollbackRecord(long txID, boolean sync, IOCompletion callback) throws Exception
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public JournalLoadInformation load(LoaderCallback reloadManager) throws Exception
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public JournalLoadInformation loadInternalOnly() throws Exception
   {
      throw new UnsupportedOperationException();
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
      throw new UnsupportedOperationException();
   }

   @Override
   public int getAlignment() throws Exception
   {
      throw new UnsupportedOperationException();
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
}
