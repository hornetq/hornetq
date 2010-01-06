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

package org.hornetq.tests.unit.core.persistence.impl;

import java.io.File;
import java.util.ArrayList;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.core.journal.Journal;
import org.hornetq.core.journal.PreparedTransactionInfo;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.core.journal.impl.NIOSequentialFileFactory;
import org.hornetq.core.persistence.impl.journal.BatchingIDGenerator;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager;
import org.hornetq.tests.util.UnitTestCase;

/**
 * A BatchIDGeneratorUnitTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class BatchIDGeneratorUnitTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testSequence() throws Exception
   {
      NIOSequentialFileFactory factory = new NIOSequentialFileFactory(getTestDir());
      Journal journal = new JournalImpl(10 * 1024, 2, 0, 0, factory, "test-data", "tst", 1);

      journal.start();

      journal.load(new ArrayList<RecordInfo>(), new ArrayList<PreparedTransactionInfo>(), null);

      BatchingIDGenerator batch = new BatchingIDGenerator(0, 1000, journal);
      long id1 = batch.generateID();
      long id2 = batch.generateID();

      Assert.assertTrue(id2 > id1);

      journal.stop();
      batch = new BatchingIDGenerator(0, 1000, journal);
      loadIDs(journal, batch);

      long id3 = batch.generateID();

      Assert.assertEquals(1000, id3);

      long id4 = batch.generateID();

      Assert.assertTrue(id4 > id3 && id4 < 2000);

      batch.close();

      journal.stop();
      batch = new BatchingIDGenerator(0, 1000, journal);
      loadIDs(journal, batch);

      long id5 = batch.generateID();
      Assert.assertTrue(id5 > id4 && id5 < 2000);

      long lastId = id5;

      boolean close = true;
      for (int i = 0; i < 100000; i++)
      {
         if (i % 1000 == 0)
         {
            System.out.println("lastId = " + lastId);
            // interchanging closes and simulated crashes
            if (close)
            {
               batch.close();
            }

            close = !close;

            journal.stop();
            batch = new BatchingIDGenerator(0, 1000, journal);
            loadIDs(journal, batch);
         }

         long id = batch.generateID();

         Assert.assertTrue(id > lastId);

         lastId = id;
      }

      batch.close();
      journal.stop();
      batch = new BatchingIDGenerator(0, 1000, journal);
      loadIDs(journal, batch);

      lastId = batch.getCurrentID();

      journal.stop();
      batch = new BatchingIDGenerator(0, 1000, journal);
      loadIDs(journal, batch);

      Assert.assertEquals("No Ids were generated, so the currentID was supposed to stay the same",
                          lastId,
                          batch.getCurrentID());

   }

   protected void loadIDs(final Journal journal, final BatchingIDGenerator batch) throws Exception
   {
      ArrayList<RecordInfo> records = new ArrayList<RecordInfo>();
      ArrayList<PreparedTransactionInfo> tx = new ArrayList<PreparedTransactionInfo>();

      journal.start();
      journal.load(records, tx, null);

      Assert.assertEquals(0, tx.size());

      Assert.assertTrue(records.size() > 0);

      for (RecordInfo record : records)
      {
         if (record.userRecordType == JournalStorageManager.ID_COUNTER_RECORD)
         {
            HornetQBuffer buffer = HornetQBuffers.wrappedBuffer(record.data);
            batch.loadState(record.id, buffer);
         }
      }
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

   @Override
   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
