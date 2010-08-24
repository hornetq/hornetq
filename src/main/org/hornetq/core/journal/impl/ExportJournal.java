/*
 * Copyright 2010 Red Hat, Inc.
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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.List;

import org.hornetq.core.journal.*;

import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.utils.Base64;

/**
 * Use this class to export the journal data. You can use it as a main class or through its native method {@link ExportJournal#exportJournal(String, String, String, int, int, String)}
 * 
 * If you use the main method, use it as  <JournalDirectory> <JournalPrefix> <FileExtension> <MinFiles> <FileSize> <FileOutput>
 * 
 * Example: java -cp hornetq-core.jar org.hornetq.core.journal.impl.ExportJournal /journalDir hornetq-data hq 2 10485760 /tmp/export.dat
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ExportJournal
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public static void main(String arg[])
   {
      if (arg.length != 5)
      {
         System.err.println("Use: java -cp hornetq-core.jar org.hornetq.core.journal.impl.ExportJournal <JournalDirectory> <JournalPrefix> <FileExtension> <FileSize> <FileOutput>");
         return;
      }

      try
      {
         exportJournal(arg[0], arg[1], arg[2], 2, Integer.parseInt(arg[3]), arg[4]);
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }

   }

   public static void exportJournal(String directory,
                                    String journalPrefix,
                                    String journalSuffix,
                                    int minFiles,
                                    int fileSize,
                                    String fileOutput) throws Exception
   {
      
      FileOutputStream fileOut = new FileOutputStream(new File(fileOutput));

      BufferedOutputStream buffOut = new BufferedOutputStream(fileOut);

      PrintStream out = new PrintStream(buffOut);
      
      exportJournal(directory, journalPrefix, journalSuffix, minFiles, fileSize, out);
      
      out.close();
   }

   
   public static void exportJournal(String directory,
                                    String journalPrefix,
                                    String journalSuffix,
                                    int minFiles,
                                    int fileSize,
                                    PrintStream out) throws Exception
   {
      NIOSequentialFileFactory nio = new NIOSequentialFileFactory(directory);

      JournalImpl journal = new JournalImpl(fileSize, minFiles, 0, 0, nio, journalPrefix, journalSuffix, 1);

      List<JournalFile> files = journal.orderFiles();

      for (JournalFile file : files)
      {
         out.println("#File," + file);

         exportJournalFile(out, nio, file);
      }
   }

   /**
    * @param out
    * @param fileFactory
    * @param file
    * @throws Exception
    */
   public static void exportJournalFile(final PrintStream out, SequentialFileFactory fileFactory, JournalFile file) throws Exception
   {
      JournalImpl.readJournalFile(fileFactory, file, new JournalReaderCallback()
      {

         public void onReadUpdateRecordTX(long transactionID, RecordInfo recordInfo) throws Exception
         {
            out.println("operation@UpdateTX,txID@" + transactionID + "," + describeRecord(recordInfo));
         }

         public void onReadUpdateRecord(RecordInfo recordInfo) throws Exception
         {
            out.println("operation@Update," + describeRecord(recordInfo));
         }

         public void onReadRollbackRecord(long transactionID) throws Exception
         {
            out.println("operation@Rollback,txID@" + transactionID);
         }

         public void onReadPrepareRecord(long transactionID, byte[] extraData, int numberOfRecords) throws Exception
         {
            out.println("operation@Prepare,txID@" + transactionID +
                        ",numberOfRecords@" +
                        numberOfRecords +
                        ",extraData@" +
                        encode(extraData));
         }

         public void onReadDeleteRecordTX(long transactionID, RecordInfo recordInfo) throws Exception
         {
            out.println("operation@DeleteRecordTX,txID@" + transactionID + "," + describeRecord(recordInfo));
         }

         public void onReadDeleteRecord(long recordID) throws Exception
         {
            out.println("operation@DeleteRecord,id@" + recordID);
         }

         public void onReadCommitRecord(long transactionID, int numberOfRecords) throws Exception
         {
            out.println("operation@Commit,txID@" + transactionID + ",numberOfRecords@" + numberOfRecords);
         }

         public void onReadAddRecordTX(long transactionID, RecordInfo recordInfo) throws Exception
         {
            out.println("operation@AddRecordTX,txID@" + transactionID + "," + describeRecord(recordInfo));
         }

         public void onReadAddRecord(RecordInfo recordInfo) throws Exception
         {
            out.println("operation@AddRecord," + describeRecord(recordInfo));
         }

         public void markAsDataFile(JournalFile file)
         {
         }
      });
   }

   private static String describeRecord(RecordInfo recordInfo)
   {
      return "id@" + recordInfo.id +
             ",userRecordType@" +
             recordInfo.userRecordType +
             ",length@" +
             recordInfo.data.length +
             ",isUpdate@" +
             recordInfo.isUpdate +
             ",compactCount@" +
             recordInfo.compactCount +
             ",data@" +
             encode(recordInfo.data);
   }

   private static String encode(byte[] data)
   {
      return Base64.encodeBytes(data, 0, data.length, Base64.DONT_BREAK_LINES | Base64.URL_SAFE);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
