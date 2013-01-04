package org.hornetq.core.persistence.impl.journal;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.transaction.xa.Xid;

import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.journal.PreparedTransactionInfo;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.journal.TransactionFailureCallback;
import org.hornetq.core.journal.impl.JournalFile;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.core.journal.impl.JournalReaderCallback;
import org.hornetq.core.journal.impl.NIOSequentialFileFactory;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager.AckDescribe;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager.ReferenceDescribe;
import org.hornetq.utils.Base64;
import org.hornetq.utils.XidCodecSupport;

/**
 * Outputs a String description of the Journals contents.
 * <p>
 * Meant to be used in debugging.
 */
public final class DescribeJournal
{
   static final void describeBindingJournal(final String bindingsDir) throws Exception
   {

      SequentialFileFactory bindingsFF = new NIOSequentialFileFactory(bindingsDir, null);

      JournalImpl bindings = new JournalImpl(1024 * 1024, 2, -1, 0, bindingsFF, "hornetq-bindings", "bindings", 1);
      describeJournal(bindingsFF, bindings, bindingsDir);
   }

   static final void describeMessagesJournal(final String messagesDir) throws Exception
   {

      SequentialFileFactory messagesFF = new NIOSequentialFileFactory(messagesDir, null);

      // Will use only default values. The load function should adapt to anything different
      ConfigurationImpl defaultValues = new ConfigurationImpl();

      JournalImpl messagesJournal = new JournalImpl(defaultValues.getJournalFileSize(),
         defaultValues.getJournalMinFiles(),
         0,
         0,
         messagesFF,
         "hornetq-data",
         "hq",
         1);

      describeJournal(messagesFF, messagesJournal, messagesDir);
   }

   /**
    * @param fileFactory
    * @param journal
    * @throws Exception
    */
   private static void
            describeJournal(SequentialFileFactory fileFactory, JournalImpl journal, final String path) throws Exception
   {
      List<JournalFile> files = journal.orderFiles();

      final PrintStream out = System.out;

      out.println("Journal path: " + path);

      for (JournalFile file : files)
      {
         out.println("#" + file + " (size=" + file.getFile().size() + ")");

         JournalImpl.readJournalFile(fileFactory, file, new JournalReaderCallback()
         {

            public void onReadUpdateRecordTX(final long transactionID, final RecordInfo recordInfo) throws Exception
            {
               out.println("operation@UpdateTX;txID=" + transactionID + "," + describeRecord(recordInfo));
            }

            public void onReadUpdateRecord(final RecordInfo recordInfo) throws Exception
            {
               out.println("operation@Update;" + describeRecord(recordInfo));
            }

            public void onReadRollbackRecord(final long transactionID) throws Exception
            {
               out.println("operation@Rollback;txID=" + transactionID);
            }

            public
                     void
                     onReadPrepareRecord(final long transactionID, final byte[] extraData, final int numberOfRecords)
                                                                                                                     throws Exception
            {
               out.println("operation@Prepare,txID=" + transactionID + ",numberOfRecords=" + numberOfRecords +
                        ",extraData=" + encode(extraData) + ", xid=" + toXid(extraData));
            }

            public void onReadDeleteRecordTX(final long transactionID, final RecordInfo recordInfo) throws Exception
            {
               out.println("operation@DeleteRecordTX;txID=" + transactionID + "," + describeRecord(recordInfo));
            }

            public void onReadDeleteRecord(final long recordID) throws Exception
            {
               out.println("operation@DeleteRecord;recordID=" + recordID);
            }

            public void onReadCommitRecord(final long transactionID, final int numberOfRecords) throws Exception
            {
               out.println("operation@Commit;txID=" + transactionID + ",numberOfRecords=" + numberOfRecords);
            }

            public void onReadAddRecordTX(final long transactionID, final RecordInfo recordInfo) throws Exception
            {
               out.println("operation@AddRecordTX;txID=" + transactionID + "," + describeRecord(recordInfo));
            }

            public void onReadAddRecord(final RecordInfo recordInfo) throws Exception
            {
               out.println("operation@AddRecord;" + describeRecord(recordInfo));
            }

            public void markAsDataFile(final JournalFile file1)
            {
            }
         });
      }

      out.println();

      out.println("### Surviving Records Summary ###");

      List<RecordInfo> records = new LinkedList<RecordInfo>();
      List<PreparedTransactionInfo> preparedTransactions = new LinkedList<PreparedTransactionInfo>();

      journal.start();

      final StringBuffer bufferFailingTransactions = new StringBuffer();

      int messageCount = 0;
      Map<Long, Integer> messageRefCounts = new HashMap<Long, Integer>();
      int preparedMessageCount = 0;
      Map<Long, Integer> preparedMessageRefCount = new HashMap<Long, Integer>();
      journal.load(records, preparedTransactions, new TransactionFailureCallback()
      {

         public void failedTransaction(long transactionID, List<RecordInfo> records1, List<RecordInfo> recordsToDelete)
         {
            bufferFailingTransactions.append("Transaction " + transactionID + " failed with these records:\n");
            for (RecordInfo info : records1)
            {
               bufferFailingTransactions.append("- " + describeRecord(info) + "\n");
            }

            for (RecordInfo info : recordsToDelete)
            {
               bufferFailingTransactions.append("- " + describeRecord(info) + " <marked to delete>\n");
            }

         }
      }, false);

      for (RecordInfo info : records)
      {
         Object o = JournalStorageManager.newObjectEncoding(info);
         if (info.getUserRecordType() == JournalRecordIds.ADD_MESSAGE)
         {
            messageCount++;
         }
         else if (info.getUserRecordType() == JournalRecordIds.ADD_REF)
         {
            ReferenceDescribe ref = (ReferenceDescribe)o;
            Integer count = messageRefCounts.get(ref.refEncoding.queueID);
            if (count == null)
            {
               count = 1;
               messageRefCounts.put(ref.refEncoding.queueID, count);
            }
            else
            {
               messageRefCounts.put(ref.refEncoding.queueID, count + 1);
            }
         }
         else if (info.getUserRecordType() == JournalRecordIds.ACKNOWLEDGE_REF)
         {
            AckDescribe ref = (AckDescribe)o;
            Integer count = messageRefCounts.get(ref.refEncoding.queueID);
            if (count == null)
            {
               messageRefCounts.put(ref.refEncoding.queueID, 0);
            }
            else
            {
               messageRefCounts.put(ref.refEncoding.queueID, count - 1);
            }
         }
         out.println(describeRecord(info, o));
      }

      out.println();
      out.println("### Prepared TX ###");

      for (PreparedTransactionInfo tx : preparedTransactions)
      {
         System.out.println(tx.id);
         for (RecordInfo info : tx.records)
         {
            Object o = JournalStorageManager.newObjectEncoding(info);
            out.println("- " + describeRecord(info, o));
            if (info.getUserRecordType() == 31)
            {
               preparedMessageCount++;
            }
            else if (info.getUserRecordType() == 32)
            {
               ReferenceDescribe ref = (ReferenceDescribe)o;
               Integer count = preparedMessageRefCount.get(ref.refEncoding.queueID);
               if (count == null)
               {
                  count = 1;
                  preparedMessageRefCount.put(ref.refEncoding.queueID, count);
               }
               else
               {
                  preparedMessageRefCount.put(ref.refEncoding.queueID, count + 1);
               }
            }
         }

         for (RecordInfo info : tx.recordsToDelete)
         {
            out.println("- " + describeRecord(info) + " <marked to delete>");
         }
      }

      String missingTX = bufferFailingTransactions.toString();

      if (missingTX.length() > 0)
      {
         out.println();
         out.println("### Failed Transactions (Missing commit/prepare/rollback record) ###");
      }

      out.println(bufferFailingTransactions.toString());

      out.println("### Message Counts ###");
      out.println("message count=" + messageCount);
      out.println("message reference count");
      for (Map.Entry<Long, Integer> longIntegerEntry : messageRefCounts.entrySet())
      {
         System.out.println("queue id " + longIntegerEntry.getKey() + ",count=" + longIntegerEntry.getValue());
      }

      out.println("prepared message count=" + preparedMessageCount);

      for (Map.Entry<Long, Integer> longIntegerEntry : preparedMessageRefCount.entrySet())
      {
         System.out.println("queue id " + longIntegerEntry.getKey() + ",count=" + longIntegerEntry.getValue());
      }

      journal.stop();
   }

   private static String describeRecord(RecordInfo info)
   {
      return "recordID=" + info.id + ";userRecordType=" + info.userRecordType + ";isUpdate=" + info.isUpdate + ";" +
               JournalStorageManager.newObjectEncoding(info);
   }

   private static String describeRecord(RecordInfo info, Object o)
   {
      return "recordID=" + info.id + ";userRecordType=" + info.userRecordType + ";isUpdate=" + info.isUpdate + ";" + o;
   }

   private static String encode(final byte[] data)
   {
      return Base64.encodeBytes(data, 0, data.length, Base64.DONT_BREAK_LINES | Base64.URL_SAFE);
   }

   private static Xid toXid(final byte[] data)
   {
      try
      {
         return XidCodecSupport.decodeXid(HornetQBuffers.wrappedBuffer(data));
      }
      catch (Exception e)
      {
         return null;
      }
   }
}
