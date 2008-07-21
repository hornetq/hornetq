/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.stress.journal;

import java.io.File;
import java.nio.ByteBuffer;

import org.jboss.messaging.core.asyncio.impl.AsynchronousFileImpl;
import org.jboss.messaging.core.journal.LoadManager;
import org.jboss.messaging.core.journal.PreparedTransactionInfo;
import org.jboss.messaging.core.journal.RecordInfo;
import org.jboss.messaging.core.journal.impl.JournalImpl;
import org.jboss.messaging.tests.stress.StressTestBase;
import org.jboss.messaging.tests.stress.journal.remote.RemoteJournalAppender;

public class ValidateTransactionHealthTest extends StressTestBase
{
   
   // Constants -----------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------
   
   public void testAIO() throws Exception
   {
      internalTest("aio", "/tmp/aiojournal", 100000, 100, true, true);
   }
   
   public void testAIONonTransactional() throws Exception
   {
      internalTest("aio", "/tmp/aiojournal", 100000, 0, true, true);
   }
   
   public void testAIONonTransactionalNoExternalProcess() throws Exception
   {
      internalTest("aio", "/tmp/aiojournal", 100000, 0, true, false);
   }
   
   public void testNIO() throws Exception
   {
      internalTest("nio", "/tmp/niojournal", 100000, 100, true, true);
   }
   
   public void testNIONonTransactional() throws Exception
   {
      internalTest("nio", "/tmp/niojournal", 100000, 0, true, true);
   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   private void internalTest(String type, String journalDir,
         long numberOfRecords, int transactionSize, boolean append, boolean externalProcess) throws Exception
   {
      if (type.equals("aio") && !AsynchronousFileImpl.isLoaded())
      {
         // Using System.out as this output will go towards junit report
         System.out.println("AIO not found, test being ignored on this platform");
         return;
      }
      
      // This property could be set to false for debug purposes.
      if (append)
      {
         File file = new File(journalDir);
         deleteDirectory(file);
         file.mkdir();
         
         if (externalProcess)
         {
            RemoteProcess process = startProcess(true, RemoteJournalAppender.class
                  .getCanonicalName(), type, journalDir, Long
                  .toString(numberOfRecords), Integer.toString(transactionSize));
            process.getProcess().waitFor();
            assertEquals(RemoteJournalAppender.OK, process.getProcess().exitValue());
         }
         else
         {
            JournalImpl journal = RemoteJournalAppender.appendData(type, journalDir, numberOfRecords, transactionSize);
            journal.stop();
         }
      }
      
      //reload(type, journalDir, numberOfRecords);
   }
   
   private void reload(String type, String journalDir, long numberOfRecords)
         throws Exception
   {
      JournalImpl journal = RemoteJournalAppender.createJournal(type,
            journalDir);
      
      journal.start();
      Loader loadTest = new Loader(numberOfRecords);
      journal.load(loadTest);
      assertEquals(numberOfRecords, loadTest.numberOfAdds);
      assertEquals(0, loadTest.numberOfPreparedTransactions);
      assertEquals(0, loadTest.numberOfUpdates);
      assertEquals(0, loadTest.numberOfDeletes);
      
      if (loadTest.ex != null)
      {
         throw loadTest.ex;
      }
   }
   
   // Inner classes -------------------------------------------------
   
   class Loader implements LoadManager
   {
      int numberOfPreparedTransactions = 0;
      int numberOfAdds = 0;
      int numberOfDeletes = 0;
      int numberOfUpdates = 0;
      long expectedRecords = 0;
      
      Exception ex = null;
      
      long lastID = 0;
      
      public Loader(long expectedRecords)
      {
         this.expectedRecords = expectedRecords;
      }
      
      public void addPreparedTransaction(
            PreparedTransactionInfo preparedTransaction)
      {
         numberOfPreparedTransactions++;
         
      }
      
      public void addRecord(RecordInfo info)
      {
         if (info.id - lastID > 1)
         {
            System.out.println("id = " + info.id + " last id = " + lastID);
         }
         
         ByteBuffer buffer = ByteBuffer.wrap(info.data);
         long recordValue = buffer.getLong();
         
         if (recordValue != (expectedRecords - info.id))
         {
            ex = new Exception("Content not as expected (" + recordValue
                  + " != " + info.id + ")");
            
         }
         
         lastID = info.id;
         numberOfAdds++;
         
      }
      
      public void deleteRecord(long id)
      {
         numberOfDeletes++;
         
      }
      
      public void updateRecord(RecordInfo info)
      {
         numberOfUpdates++;
         
      }
      
   }
   
}
