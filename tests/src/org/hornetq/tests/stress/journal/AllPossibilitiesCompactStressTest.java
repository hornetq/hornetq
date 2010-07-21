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

package org.hornetq.tests.stress.journal;

import java.io.File;
import java.io.FilenameFilter;

import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.core.journal.impl.NIOSequentialFileFactory;
import org.hornetq.tests.unit.core.journal.impl.JournalImplTestBase;
import org.hornetq.utils.SimpleIDGenerator;
import org.hornetq.utils.VariableLatch;

/**
 * A NIORandomCompactTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class AllPossibilitiesCompactStressTest extends JournalImplTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private VariableLatch startedCompactingLatch = null;

   private VariableLatch releaseCompactingLatch = null;

   private Thread tCompact = null;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      tCompact = null;

      startedCompactingLatch = new VariableLatch();

      releaseCompactingLatch = new VariableLatch();

      File file = new File(getTestDir());

      deleteDirectory(file);

      file.mkdir();
   }

   protected void tearDown() throws Exception
   {

      File testDir = new File(getTestDir());

      File files[] = testDir.listFiles(new FilenameFilter()
      {

         public boolean accept(File dir, String name)
         {
            return name.startsWith(filePrefix) && name.endsWith(fileExtension);
         }
      });

      for (File file : files)
      {
         assertEquals("File " + file + " doesn't have the expected number of bytes", fileSize, file.length());
      }

      super.tearDown();
   }

   int startCompactAt;

   int joinCompactAt;

   int secondCompactAt;

   int currentOperation;

   SimpleIDGenerator idGen = new SimpleIDGenerator(1000);

   
   public void createJournal() throws Exception
   {
      journal = new JournalImpl(fileSize, minFiles, 0, 0, fileFactory, filePrefix, fileExtension, maxAIO)
      {

         @Override
         public void onCompactDone()
         {
            startedCompactingLatch.down();
            try
            {
               releaseCompactingLatch.waitCompletion();
            }
            catch (InterruptedException e)
            {
               e.printStackTrace();
            }
         }
      };

      journal.setAutoReclaim(false);
   }


   public void testMixOperations() throws Exception
   {

      setup(2, 60 * 1024, false);
      
      startCompactAt = joinCompactAt = secondCompactAt = -1;
      
      currentOperation = 0;
      internalTest();
      int MAX_OPERATIONS = currentOperation;
      
      System.out.println("Using MAX_OPERATIONS = " + MAX_OPERATIONS);

      for (startCompactAt = 0; startCompactAt < MAX_OPERATIONS; startCompactAt++)
      {
         for (joinCompactAt = startCompactAt; joinCompactAt < MAX_OPERATIONS; joinCompactAt++)
         {
            for (secondCompactAt = joinCompactAt; secondCompactAt < MAX_OPERATIONS; secondCompactAt++)
            {
               System.out.println("start=" + startCompactAt + ", join=" + joinCompactAt + ", second=" + secondCompactAt);
               
               currentOperation = 0;
               try
               {
                  tearDown();
                  setUp();
                  internalTest();
               }
               catch (Throwable e)
               {
                  throw new Exception("Error at compact=" + startCompactAt +
                                      ", joinCompactAt=" +
                                      joinCompactAt +
                                      ", secondCompactAt=" +
                                      secondCompactAt, e);
               }
            }
         }
      }
   }

   protected void beforeJournalOperation() throws Exception
   {
      checkJournalOperation();
   }

   /**
    * @throws InterruptedException
    * @throws Exception
    */
   private void checkJournalOperation() throws InterruptedException, Exception
   {
      if (startCompactAt == currentOperation)
      {
         threadCompact();
      }
      if (joinCompactAt == currentOperation)
      {
         joinCompact();
      }
      if (secondCompactAt == currentOperation)
      {
         journal.compact();
      }

      currentOperation++;
   }

   public void internalTest() throws Exception
   {
      createJournal();
      
      startJournal();

      loadAndCheck();

      long consumerTX = idGen.generateID();

      long firstID = idGen.generateID();

      long appendTX = idGen.generateID();

      long addedRecord = idGen.generateID();

      long addRecord2 = idGen.generateID();

      long addRecord3 = idGen.generateID();

      long addRecord4 = idGen.generateID();

      long addRecordStay = idGen.generateID();
      
      long addRecord5 = idGen.generateID();

      add(addRecordStay);

      add(addRecord2);

      add(addRecord4);

      update(addRecord2);

      addTx(consumerTX, firstID);

      updateTx(consumerTX, addRecord4);

      addTx(consumerTX, addRecord5);
      
      addTx(appendTX, addedRecord);

      commit(appendTX);

      updateTx(consumerTX, addedRecord);

      commit(consumerTX);

      delete(addRecord4);

      delete(addedRecord);

      add(addRecord3);

      long updateTX = idGen.generateID();
      
      updateTx(updateTX, addRecord3);
      
      commit(updateTX);
      
      delete(addRecord5);
      
      checkJournalOperation();

      stopJournal();

      createJournal();

      startJournal();

      loadAndCheck();
      
      stopJournal();
   }

   /**
    * @param releaseCompactingLatch
    * @param tCompact
    * @throws InterruptedException
    */
   private void joinCompact() throws InterruptedException
   {
      releaseCompactingLatch.down();

      tCompact.join();

      tCompact = null;
   }

   /**
    * @param startedCompactingLatch
    * @return
    * @throws InterruptedException
    */
   private void threadCompact() throws InterruptedException
   {
      tCompact = new Thread()
      {
         @Override
         public void run()
         {
            try
            {
               journal.compact();
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }
      };

      tCompact.start();

      startedCompactingLatch.waitCompletion();
   }

   /* (non-Javadoc)
    * @see org.hornetq.tests.unit.core.journal.impl.JournalImplTestBase#getFileFactory()
    */
   @Override
   protected SequentialFileFactory getFileFactory() throws Exception
   {
      return new NIOSequentialFileFactory(getTestDir());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
