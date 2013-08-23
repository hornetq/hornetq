package org.hornetq.core.persistence.impl.journal;

import static org.hornetq.core.persistence.impl.journal.JournalRecordIds.ACKNOWLEDGE_CURSOR;
import static org.hornetq.core.persistence.impl.journal.JournalRecordIds.ACKNOWLEDGE_REF;
import static org.hornetq.core.persistence.impl.journal.JournalRecordIds.ADDRESS_SETTING_RECORD;
import static org.hornetq.core.persistence.impl.journal.JournalRecordIds.ADD_LARGE_MESSAGE;
import static org.hornetq.core.persistence.impl.journal.JournalRecordIds.ADD_LARGE_MESSAGE_PENDING;
import static org.hornetq.core.persistence.impl.journal.JournalRecordIds.ADD_MESSAGE;
import static org.hornetq.core.persistence.impl.journal.JournalRecordIds.ADD_REF;
import static org.hornetq.core.persistence.impl.journal.JournalRecordIds.DUPLICATE_ID;
import static org.hornetq.core.persistence.impl.journal.JournalRecordIds.HEURISTIC_COMPLETION;
import static org.hornetq.core.persistence.impl.journal.JournalRecordIds.ID_COUNTER_RECORD;
import static org.hornetq.core.persistence.impl.journal.JournalRecordIds.PAGE_CURSOR_COMPLETE;
import static org.hornetq.core.persistence.impl.journal.JournalRecordIds.PAGE_CURSOR_COUNTER_INC;
import static org.hornetq.core.persistence.impl.journal.JournalRecordIds.PAGE_CURSOR_COUNTER_VALUE;
import static org.hornetq.core.persistence.impl.journal.JournalRecordIds.PAGE_TRANSACTION;
import static org.hornetq.core.persistence.impl.journal.JournalRecordIds.QUEUE_BINDING_RECORD;
import static org.hornetq.core.persistence.impl.journal.JournalRecordIds.SECURITY_RECORD;
import static org.hornetq.core.persistence.impl.journal.JournalRecordIds.SET_SCHEDULED_DELIVERY_TIME;
import static org.hornetq.core.persistence.impl.journal.JournalRecordIds.UPDATE_DELIVERY_COUNT;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.transaction.xa.Xid;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.core.journal.PreparedTransactionInfo;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.journal.TransactionFailureCallback;
import org.hornetq.core.journal.impl.JournalFile;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.core.journal.impl.JournalReaderCallback;
import org.hornetq.core.journal.impl.NIOSequentialFileFactory;
import org.hornetq.core.paging.cursor.impl.PageSubscriptionCounterImpl;
import org.hornetq.core.paging.impl.PageTransactionInfoImpl;
import org.hornetq.core.persistence.impl.journal.BatchingIDGenerator.IDCounterEncoding;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager.AckDescribe;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager.CursorAckRecordEncoding;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager.DeliveryCountUpdateEncoding;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager.DuplicateIDEncoding;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager.HeuristicCompletionEncoding;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager.LargeMessageEncoding;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager.PageCountRecord;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager.PageCountRecordInc;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager.PageUpdateTXEncoding;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager.PendingLargeMessageEncoding;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager.RefEncoding;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager.ScheduledDeliveryEncoding;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.utils.Base64;
import org.hornetq.utils.XidCodecSupport;

/**
 * Outputs a String description of the Journals contents.
 * <p>
 * Meant to be used in debugging.
 */
public final class DescribeJournal
{
   public static final void describeBindingsJournal(final String bindingsDir) throws Exception
   {

      SequentialFileFactory bindingsFF = new NIOSequentialFileFactory(bindingsDir, null);

      JournalImpl bindings = new JournalImpl(1024 * 1024, 2, -1, 0, bindingsFF, "hornetq-bindings", "bindings", 1);
      describeJournal(bindingsFF, bindings, bindingsDir);
   }

   public static final void describeMessagesJournal(final String messagesDir) throws Exception
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

      final Map<Long, PageSubscriptionCounterImpl> counters = new HashMap<Long, PageSubscriptionCounterImpl>();

      out.println("Journal path: " + path);

      for (JournalFile file : files)
      {
         out.println("#" + file + " (size=" + file.getFile().size() + ")");

         JournalImpl.readJournalFile(fileFactory, file, new JournalReaderCallback()
         {

            public void onReadUpdateRecordTX(final long transactionID, final RecordInfo recordInfo) throws Exception
            {
               out.println("operation@UpdateTX;txID=" + transactionID + "," + describeRecord(recordInfo));
               checkRecordCounter(recordInfo);
            }

            public void onReadUpdateRecord(final RecordInfo recordInfo) throws Exception
            {
               out.println("operation@Update;" + describeRecord(recordInfo));
               checkRecordCounter(recordInfo);
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

            public void checkRecordCounter(RecordInfo info)
            {
               if (info.getUserRecordType() == JournalRecordIds.PAGE_CURSOR_COUNTER_VALUE)
               {
                  PageCountRecord encoding = (PageCountRecord)newObjectEncoding(info);
                  long queueIDForCounter = encoding.queueID;

                  PageSubscriptionCounterImpl subsCounter = lookupCounter(counters, queueIDForCounter);

                  if (subsCounter.getValue() != 0 && subsCounter.getValue() != encoding.value);
                  {
                     out.println("####### Counter replace wrongly on queue " + queueIDForCounter + " oldValue=" + subsCounter.getValue() + " newValue=" + encoding.value);
                  }

                  subsCounter.loadValue(info.id, encoding.value);
                  subsCounter.processReload();
                  out.print("#Counter queue " + queueIDForCounter + " value=" + subsCounter.getValue() + ", result=" + subsCounter.getValue());
                  if (subsCounter.getValue() < 0)
                  {
                     out.println(" #NegativeCounter!!!!");
                  }
                  else
                  {
                     out.println();
                  }
                  out.println();
               }
               else if (info.getUserRecordType() == JournalRecordIds.PAGE_CURSOR_COUNTER_INC)
               {
                  PageCountRecordInc encoding = (PageCountRecordInc)newObjectEncoding(info);
                  long queueIDForCounter = encoding.queueID;

                  PageSubscriptionCounterImpl subsCounter = lookupCounter(counters, queueIDForCounter);

                  subsCounter.loadInc(info.id, (int)encoding.value);
                  subsCounter.processReload();
                  out.print("#Counter queue " + queueIDForCounter +  " value=" + subsCounter.getValue() + " increased by " + encoding.value);
                  if (subsCounter.getValue() < 0)
                  {
                     out.println(" #NegativeCounter!!!!");
                  }
                  else
                  {
                     out.println();
                  }
                  out.println();
               }
            }
         });
      }

      out.println();

      if (counters.size() != 0)
      {
         out.println("#Counters during initial load:");
         printCounters(out, counters);
      }

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

      counters.clear();

      for (RecordInfo info : records)
      {
         PageSubscriptionCounterImpl subsCounter = null;
         long queueIDForCounter = 0;

         Object o = newObjectEncoding(info);
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
         else if (info.getUserRecordType() == JournalRecordIds.PAGE_CURSOR_COUNTER_VALUE)
         {
            PageCountRecord encoding = (PageCountRecord)o;
            queueIDForCounter = encoding.queueID;

            subsCounter = lookupCounter(counters, queueIDForCounter);

            subsCounter.loadValue(info.id, encoding.value);
            subsCounter.processReload();
         }
         else if (info.getUserRecordType() == JournalRecordIds.PAGE_CURSOR_COUNTER_INC)
         {
            PageCountRecordInc encoding = (PageCountRecordInc)o;
            queueIDForCounter = encoding.queueID;

            subsCounter = lookupCounter(counters, queueIDForCounter);

            subsCounter.loadInc(info.id, (int)encoding.value);
            subsCounter.processReload();
         }

         out.println(describeRecord(info, o));

         if (subsCounter != null)
         {
            out.println("##SubsCounter for queue=" + queueIDForCounter + ", value=" + subsCounter.getValue());
            out.println();
         }
      }

      if (counters.size() > 0)
      {
         out.println("### Page Counters");
         printCounters(out, counters);
      }


      out.println();
      out.println("### Prepared TX ###");

      for (PreparedTransactionInfo tx : preparedTransactions)
      {
         out.println(tx.id);
         for (RecordInfo info : tx.records)
         {
            Object o = newObjectEncoding(info);
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

   protected static void printCounters(final PrintStream out, final Map<Long, PageSubscriptionCounterImpl> counters)
   {
      for (Map.Entry<Long, PageSubscriptionCounterImpl> entry: counters.entrySet())
      {
         out.println("Queue " + entry.getKey() + " value=" + entry.getValue().getValue());
      }
   }

   protected static PageSubscriptionCounterImpl lookupCounter(Map<Long, PageSubscriptionCounterImpl> counters,
                                                              long queueIDForCounter)
   {
      PageSubscriptionCounterImpl subsCounter;
      subsCounter = counters.get(queueIDForCounter);
      if (subsCounter == null)
      {
         subsCounter = new PageSubscriptionCounterImpl(null, null, null, false, -1);
         counters.put(queueIDForCounter, subsCounter);
      }
      return subsCounter;
   }

   private static String describeRecord(RecordInfo info)
   {
      return "recordID=" + info.id + ";userRecordType=" + info.userRecordType + ";isUpdate=" + info.isUpdate + ";" +
               newObjectEncoding(info);
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

   public static Object newObjectEncoding(RecordInfo info)
   {
      return newObjectEncoding(info, null);
   }

   public static Object newObjectEncoding(RecordInfo info, JournalStorageManager storageManager)
   {
      HornetQBuffer buffer = HornetQBuffers.wrappedBuffer(info.data);
      long id = info.id;
      int rec = info.getUserRecordType();

      switch (rec)
      {
         case ADD_LARGE_MESSAGE_PENDING:
         {
            PendingLargeMessageEncoding lmEncoding = new PendingLargeMessageEncoding();
            lmEncoding.decode(buffer);

            return lmEncoding;
         }
         case ADD_LARGE_MESSAGE:
         {

            LargeServerMessage largeMessage = new LargeServerMessageImpl(storageManager);

            LargeMessageEncoding messageEncoding = new LargeMessageEncoding(largeMessage);

            messageEncoding.decode(buffer);

            return new MessageDescribe(largeMessage);
         }
         case ADD_MESSAGE:
         {
            ServerMessage message = new ServerMessageImpl(rec, 50);

            message.decode(buffer);

            return new MessageDescribe(message);
         }
         case ADD_REF:
         {
            final RefEncoding encoding = new RefEncoding();
            encoding.decode(buffer);
            return new ReferenceDescribe(encoding);
         }

         case ACKNOWLEDGE_REF:
         {
            final RefEncoding encoding = new RefEncoding();
            encoding.decode(buffer);
            return new AckDescribe(encoding);
         }

         case UPDATE_DELIVERY_COUNT:
         {
            DeliveryCountUpdateEncoding updateDeliveryCount = new DeliveryCountUpdateEncoding();
            updateDeliveryCount.decode(buffer);
            return updateDeliveryCount;
         }

         case PAGE_TRANSACTION:
         {
            if (info.isUpdate)
            {
               PageUpdateTXEncoding pageUpdate = new PageUpdateTXEncoding();

               pageUpdate.decode(buffer);

               return pageUpdate;
            }
            else
            {
               PageTransactionInfoImpl pageTransactionInfo = new PageTransactionInfoImpl();

               pageTransactionInfo.decode(buffer);

               pageTransactionInfo.setRecordID(info.id);

               return pageTransactionInfo;
            }
         }

         case SET_SCHEDULED_DELIVERY_TIME:
         {
            ScheduledDeliveryEncoding encoding = new ScheduledDeliveryEncoding();

            encoding.decode(buffer);

            return encoding;
         }
         case DUPLICATE_ID:
         {
            DuplicateIDEncoding encoding = new DuplicateIDEncoding();

            encoding.decode(buffer);

            return encoding;
         }
         case HEURISTIC_COMPLETION:
         {
            HeuristicCompletionEncoding encoding = new HeuristicCompletionEncoding();

            encoding.decode(buffer);

            return encoding;
         }
         case ACKNOWLEDGE_CURSOR:
         {
            CursorAckRecordEncoding encoding = new CursorAckRecordEncoding();

            encoding.decode(buffer);

            return encoding;
         }
         case PAGE_CURSOR_COUNTER_VALUE:
         {
            PageCountRecord encoding = new PageCountRecord();

            encoding.decode(buffer);

            return encoding;
         }

         case PAGE_CURSOR_COMPLETE:
         {
            CursorAckRecordEncoding encoding = new PageCompleteCursorAckRecordEncoding();

            encoding.decode(buffer);

            return encoding;
         }

         case PAGE_CURSOR_COUNTER_INC:
         {
            PageCountRecordInc encoding = new PageCountRecordInc();

            encoding.decode(buffer);

            return encoding;
         }

         case QUEUE_BINDING_RECORD:
            return JournalStorageManager.newBindingEncoding(id, buffer);

         case ID_COUNTER_RECORD:
            EncodingSupport idReturn = new IDCounterEncoding();
            idReturn.decode(buffer);

            return idReturn;

         case JournalRecordIds.GROUP_RECORD:
            return JournalStorageManager.newGroupEncoding(id, buffer);

         case ADDRESS_SETTING_RECORD:
            return JournalStorageManager.newAddressEncoding(id, buffer);

         case SECURITY_RECORD:
            return JournalStorageManager.newSecurityRecord(id, buffer);

         default:
            return null;
      }
   }

   private final static class PageCompleteCursorAckRecordEncoding extends CursorAckRecordEncoding
   {

      @Override
      public String toString()
      {
         return "PGComplete [queueID=" + queueID + ", position=" + position + "]";
      }
   }

   public static final class MessageDescribe
   {
      public MessageDescribe(Message msg)
      {
         this.msg = msg;
      }

      Message msg;

      @Override
      public String toString()
      {
         StringBuffer buffer = new StringBuffer();
         buffer.append(msg.isLargeMessage() ? "LargeMessage(" : "Message(");
         buffer.append("messageID=" + msg.getMessageID());
         if (msg.getUserID() != null)
         {
            buffer.append(";userMessageID=" + msg.getUserID().toString());
         }
         buffer.append(";properties=[");

         Set<SimpleString> properties = msg.getPropertyNames();

         for (SimpleString prop : properties)
         {
            Object value = msg.getObjectProperty(prop);
            if (value instanceof byte[])
            {
               buffer.append(prop + "=" + Arrays.toString((byte[])value) + ",");

            }
            else
            {
               buffer.append(prop + "=" + value + ",");
            }
         }

         buffer.append("#properties = " + properties.size());

         buffer.append("]");

         buffer.append(" - " + msg.toString());

         return buffer.toString();
      }

   }

   public static final class ReferenceDescribe
   {
      public RefEncoding refEncoding;

      public ReferenceDescribe(RefEncoding refEncoding)
      {
         this.refEncoding = refEncoding;
      }

      @Override
      public String toString()
      {
         return "AddRef;" + refEncoding;
      }

      @Override
      public int hashCode()
      {
         final int prime = 31;
         int result = 1;
         result = prime * result + ((refEncoding == null) ? 0 : refEncoding.hashCode());
         return result;
      }

      @Override
      public boolean equals(Object obj)
      {
         if (this == obj)
            return true;
         if (obj == null)
            return false;
         if (!(obj instanceof ReferenceDescribe))
            return false;
         ReferenceDescribe other = (ReferenceDescribe)obj;
         if (refEncoding == null)
         {
            if (other.refEncoding != null)
               return false;
         }
         else if (!refEncoding.equals(other.refEncoding))
            return false;
         return true;
      }
   }

}
