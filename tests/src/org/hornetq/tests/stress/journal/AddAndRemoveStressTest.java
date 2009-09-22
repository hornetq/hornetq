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

package org.hornetq.tests.stress.journal;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.hornetq.core.journal.LoaderCallback;
import org.hornetq.core.journal.PreparedTransactionInfo;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.journal.impl.AIOSequentialFileFactory;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.tests.unit.core.journal.impl.fakes.SimpleEncoding;
import org.hornetq.tests.util.UnitTestCase;

/**
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class AddAndRemoveStressTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   private static final LoaderCallback dummyLoader = new LoaderCallback()
   {

      public void addPreparedTransaction(final PreparedTransactionInfo preparedTransaction)
      {
      }

      public void addRecord(final RecordInfo info)
      {
      }

      public void deleteRecord(final long id)
      {
      }

      public void updateRecord(final RecordInfo info)
      {
      }

      public void failedTransaction(long transactionID, List<RecordInfo> records, List<RecordInfo> recordsToDelete)
      {
      }
   };

   private static final long NUMBER_OF_MESSAGES = 210000l;

   private static final int NUMBER_OF_FILES_ON_JOURNAL = 6;

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testInsertAndLoad() throws Exception
   {

      SequentialFileFactory factory = new AIOSequentialFileFactory(getTestDir());
      JournalImpl impl = new JournalImpl(10 * 1024 * 1024,
                                         NUMBER_OF_FILES_ON_JOURNAL,
                                         0,
                                         0,
                                         factory,
                                         "hq",
                                         "hq",
                                         1000);

      impl.start();

      impl.load(dummyLoader);

      for (long i = 1; i <= NUMBER_OF_MESSAGES; i++)
      {
         if (i % 10000 == 0)
         {
            System.out.println("Append " + i);
         }
         impl.appendAddRecord(i, (byte)0, new SimpleEncoding(1024, (byte)'f'), false);
      }

      impl.stop();

      factory = new AIOSequentialFileFactory(getTestDir());
      impl = new JournalImpl(10 * 1024 * 1024, NUMBER_OF_FILES_ON_JOURNAL, 0, 0, factory, "hq", "hq", 1000);

      impl.start();

      impl.load(dummyLoader);

      for (long i = 1; i <= NUMBER_OF_MESSAGES; i++)
      {
         if (i % 10000 == 0)
         {
            System.out.println("Delete " + i);
         }

         impl.appendDeleteRecord(i, false);
      }

      impl.stop();

      factory = new AIOSequentialFileFactory(getTestDir());
      impl = new JournalImpl(10 * 1024 * 1024, NUMBER_OF_FILES_ON_JOURNAL, 0, 0, factory, "hq", "hq", 1000);

      impl.start();

      ArrayList<RecordInfo> info = new ArrayList<RecordInfo>();
      ArrayList<PreparedTransactionInfo> trans = new ArrayList<PreparedTransactionInfo>();

      impl.load(info, trans, null);

      impl.forceMoveNextFile();

      if (info.size() > 0)
      {
         System.out.println("Info ID: " + info.get(0).id);
      }
      
      impl.stop();

      assertEquals(0, info.size());
      assertEquals(0, trans.size());

      assertEquals(0, impl.getDataFilesCount());

   }

   public void testInsertUpdateAndLoad() throws Exception
   {

      SequentialFileFactory factory = new AIOSequentialFileFactory(getTestDir());
      JournalImpl impl = new JournalImpl(10 * 1024 * 1024,
                                         NUMBER_OF_FILES_ON_JOURNAL,
                                         0,
                                         0,
                                         factory,
                                         "hq",
                                         "hq",
                                         1000);

      impl.start();

      impl.load(dummyLoader);

      for (long i = 1; i <= NUMBER_OF_MESSAGES; i++)
      {
         if (i % 10000 == 0)
         {
            System.out.println("Append " + i);
         }
         impl.appendAddRecord(i, (byte)21, new SimpleEncoding(40, (byte)'f'), false);
         impl.appendUpdateRecord(i, (byte)22, new SimpleEncoding(40, (byte)'g'), false);
      }

      impl.stop();

      factory = new AIOSequentialFileFactory(getTestDir());
      impl = new JournalImpl(10 * 1024 * 1024, 10, 0, 0, factory, "hq", "hq", 1000);

      impl.start();

      impl.load(dummyLoader);

      for (long i = 1; i <= NUMBER_OF_MESSAGES; i++)
      {
         if (i % 10000 == 0)
         {
            System.out.println("Delete " + i);
         }

         impl.appendDeleteRecord(i, false);
      }

      impl.stop();

      factory = new AIOSequentialFileFactory(getTestDir());
      impl = new JournalImpl(10 * 1024 * 1024, NUMBER_OF_FILES_ON_JOURNAL, 0, 0, factory, "hq", "hq", 1000);

      impl.start();

      ArrayList<RecordInfo> info = new ArrayList<RecordInfo>();
      ArrayList<PreparedTransactionInfo> trans = new ArrayList<PreparedTransactionInfo>();

      impl.load(info, trans, null);

      if (info.size() > 0)
      {
         System.out.println("Info ID: " + info.get(0).id);
      }

      impl.forceMoveNextFile();
      impl.checkReclaimStatus();
      
      impl.stop();

      assertEquals(0, info.size());
      assertEquals(0, trans.size());
      assertEquals(0, impl.getDataFilesCount());

      System.out.println("Size = " + impl.getDataFilesCount());

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      File file = new File(getTestDir());
      deleteDirectory(file);
      file.mkdirs();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
