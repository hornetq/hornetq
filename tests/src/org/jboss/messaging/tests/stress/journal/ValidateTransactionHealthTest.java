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
import org.jboss.messaging.tests.stress.journal.remote.RemoteJournalAppender;
import org.jboss.messaging.tests.util.SpawnedVMSupport;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * 
 * This test spawns a remote VM, as we want to "crash" the VM right after the journal is filled with data
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class ValidateTransactionHealthTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testAIO() throws Exception
   {
      internalTest("aio", getTestDir(), 10000, 100, true, true, 1);
   }

   public void testAIOHugeTransaction() throws Exception
   {
      internalTest("aio", getTestDir(), 10000, 10000, true, true, 1);
   }

   public void testAIOMultiThread() throws Exception
   {
      internalTest("aio", getTestDir(), 1000, 100, true, true, 10);
   }

   public void testAIONonTransactionalNoExternalProcess() throws Exception
   {
      internalTest("aio", getTestDir(), 1000, 0, true, false, 10);
   }

   public void testNIO() throws Exception
   {
      internalTest("nio", getTestDir(), 10000, 100, true, true, 1);
   }

   public void testNIOHugeTransaction() throws Exception
   {
      internalTest("nio", getTestDir(), 10000, 10000, true, true, 1);
   }

   public void testNIOMultiThread() throws Exception
   {
      internalTest("nio", getTestDir(), 1000, 100, true, true, 10);
   }

   public void testNIONonTransactional() throws Exception
   {
      internalTest("nio", getTestDir(), 10000, 0, true, true, 1);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      
      File file = new File(getTestDir());
      deleteDirectory(file);
      file.mkdir();
   }

   // Private -------------------------------------------------------

   private void internalTest(final String type,
                             final String journalDir,
                             final long numberOfRecords,
                             final int transactionSize,
                             final boolean append,
                             final boolean externalProcess,
                             final int numberOfThreads) throws Exception
   {
      try
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
            if (externalProcess)
            {
               Process process = SpawnedVMSupport.spawnVM(RemoteJournalAppender.class.getCanonicalName(),
                                                          type,
                                                          journalDir,
                                                          Long.toString(numberOfRecords),
                                                          Integer.toString(transactionSize),
                                                          Integer.toString(numberOfThreads));
               process.waitFor();
               assertEquals(RemoteJournalAppender.OK, process.exitValue());
            }
            else
            {
               JournalImpl journal = RemoteJournalAppender.appendData(type,
                                                                      journalDir,
                                                                      numberOfRecords,
                                                                      transactionSize,
                                                                      numberOfThreads);
               journal.stop();
            }
         }

         reload(type, journalDir, numberOfRecords, numberOfThreads);
      }
      finally
      {
         File file = new File(journalDir);
         deleteDirectory(file);
      }
   }

   private void reload(final String type, final String journalDir, final long numberOfRecords, final int numberOfThreads) throws Exception
   {
      JournalImpl journal = RemoteJournalAppender.createJournal(type, journalDir);

      journal.start();
      Loader loadTest = new Loader(numberOfRecords);
      journal.load(loadTest);
      assertEquals(numberOfRecords * numberOfThreads, loadTest.numberOfAdds);
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

      public Loader(final long expectedRecords)
      {
         this.expectedRecords = expectedRecords;
      }

      public void addPreparedTransaction(final PreparedTransactionInfo preparedTransaction)
      {
         numberOfPreparedTransactions++;

      }

      public void addRecord(final RecordInfo info)
      {
         if (info.id == lastID)
         {
            System.out.println("id = " + info.id + " last id = " + lastID);
         }

         ByteBuffer buffer = ByteBuffer.wrap(info.data);
         long recordValue = buffer.getLong();

         if (recordValue != info.id)
         {
            ex = new Exception("Content not as expected (" + recordValue + " != " + info.id + ")");

         }

         lastID = info.id;
         numberOfAdds++;

      }

      public void deleteRecord(final long id)
      {
         numberOfDeletes++;

      }

      public void updateRecord(final RecordInfo info)
      {
         numberOfUpdates++;

      }

   }

}
