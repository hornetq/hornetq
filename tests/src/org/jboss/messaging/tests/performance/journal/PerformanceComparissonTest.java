/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.performance.journal;

import java.io.File;

import org.jboss.messaging.core.journal.EncodingSupport;
import org.jboss.messaging.core.journal.LoadManager;
import org.jboss.messaging.core.journal.PreparedTransactionInfo;
import org.jboss.messaging.core.journal.RecordInfo;
import org.jboss.messaging.core.journal.SequentialFileFactory;
import org.jboss.messaging.core.journal.impl.AIOSequentialFileFactory;
import org.jboss.messaging.core.journal.impl.JournalImpl;
import org.jboss.messaging.core.journal.impl.NIOSequentialFileFactory;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * A PerformanceComparissonTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class PerformanceComparissonTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final long NUM_RECORDS = 100000;

   private final long WARMUP_RECORDS = 1000;

   private int SIZE_RECORD = 1000;

   private final byte ADD_RECORD = 1;

   private final byte UPDATE1 = 2;

   private final byte UPDATE2 = 3;

   private final int ITERATIONS = 2;

   private final boolean PERFORM_UPDATE = true;

   private static final LoadManager dummyLoader = new LoadManager()
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
   };

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();
      File file = new File(getTestDir());

      deleteDirectory(file);

      file.mkdirs();
   }

   protected void tearDown() throws Exception
   {
      // super.tearDown();
   }

   public void disabled_testAddDeleteAIO() throws Exception
   {
      for (int i = 0; i < ITERATIONS; i++)
      {
         if (i > 0)
         {
            tearDown();
            setUp();
         }

         System.out.println("Test AIO # " + i);
         testAddDeleteJournal(new AIOSequentialFileFactory(getTestDir(), 100 * 1024, 2),
                              NUM_RECORDS,
                              SIZE_RECORD,
                              20,
                              10 * 1024 * 1024);

      }
   }

   public void disabled_testAddDeleteNIO() throws Exception
   {
      for (int i = 0; i < ITERATIONS; i++)
      {
         if (i > 0)
         {
            tearDown();
            setUp();
         }
         System.out.println("Test NIO # " + i);
         testAddDeleteJournal(new NIOSequentialFileFactory(getTestDir()),
                              NUM_RECORDS,
                              SIZE_RECORD,
                              20,
                              10 * 1024 * 1024);
      }
   }

   public void testAddDeleteJournal(SequentialFileFactory fileFactory,
                                    long records,
                                    int size,
                                    int numberOfFiles,
                                    int journalSize) throws Exception
   {

      JournalImpl journal = new JournalImpl(journalSize, // 10M.. we believe that's the usual cilinder
                                            // size.. not an exact science here
                                            numberOfFiles, // number of files pre-allocated
                                            true, // sync on commit
                                            false, // no sync on non transactional
                                            fileFactory, // AIO or NIO
                                            "jbm", // file name
                                            "jbm", // extension
                                            500); // it's like a semaphore for callback on the AIO layer
      // this during record writes

      journal.start();
      journal.load(dummyLoader);

      FakeMessage msg = new FakeMessage(size);
      FakeQueueEncoding update = new FakeQueueEncoding();

      long timeStart = System.currentTimeMillis();
      for (long i = 0; i < records; i++)
      {
         if (i == WARMUP_RECORDS)
         {
            timeStart = System.currentTimeMillis();
         }
         journal.appendAddRecord(i, ADD_RECORD, msg);
         if (PERFORM_UPDATE)
         {
            journal.appendUpdateRecord(i, UPDATE1, update);
         }
      }

      for (long i = 0; i < records; i++)
      {
         journal.appendUpdateRecord(i, UPDATE2, update);
         journal.appendDeleteRecord(i);
      }

      System.out.println("Produced records before stop " + (NUM_RECORDS - WARMUP_RECORDS) +
                         " in " +
                         (System.currentTimeMillis() - timeStart) +
                         " milliseconds");

      journal.stop();

      System.out.println("Produced records after stop " + (NUM_RECORDS - WARMUP_RECORDS) +
                         " in " +
                         (System.currentTimeMillis() - timeStart) +
                         " milliseconds");

      journal = new JournalImpl(journalSize, // 10M.. we believe that's the usual cilinder
                                // size.. not an exact science here
                                numberOfFiles, // number of files pre-allocated
                                true, // sync on commit
                                false, // no sync on non transactional
                                fileFactory, // AIO or NIO
                                "jbm", // file name
                                "jbm", // extension
                                500); // it's like a semaphore for callback on the AIO layer
      // this during record writes

      journal.start();
      journal.load(dummyLoader);

   }

   public void testAIO() throws Exception
   {
      for (int i = 0; i < ITERATIONS; i++)
      {
         if (i > 0)
         {
            tearDown();
            setUp();
         }

         System.out.println("Test AIO # " + i);
         testJournal(new AIOSequentialFileFactory(getTestDir(), 1024 * 1024, 2),
                     NUM_RECORDS,
                     SIZE_RECORD,
                     13,
                     10 * 1024 * 1024);

      }
   }

   public void testNIO() throws Exception
   {
      for (int i = 0; i < ITERATIONS; i++)
      {
         if (i > 0)
         {
            tearDown();
            setUp();
         }
         System.out.println("Test NIO # " + i);
         testJournal(new NIOSequentialFileFactory(getTestDir()), NUM_RECORDS, SIZE_RECORD, 13, 10 * 1024 * 1024);
      }
   }
   
   
   public void testTransactional() throws Exception
   {
      //SequentialFileFactory factory = new AIOSequentialFileFactory(getTestDir(), 1024 * 1024, 1);
      SequentialFileFactory factory = new NIOSequentialFileFactory(getTestDir());
      
      JournalImpl journal = new JournalImpl(1024 * 1024 * 10, // 10M.. we believe that's the usual cilinder
                                            // size.. not an exact science here
                                            10, // number of files pre-allocated
                                            true, // sync on commit
                                            false, // no sync on non transactional
                                            factory, // AIO or NIO
                                            "jbm", // file name
                                            "jbm", // extension
                                            500); // it's like a semaphore for callback on the AIO layer
      
      journal.start();
      journal.load(dummyLoader);
      
      long id = 1;
      
      long start = System.currentTimeMillis();
      for (int i = 0 ; i < 200; i++)
      {
         journal.appendAddRecordTransactional(i, id++, (byte)1, new byte[]{(byte)1});
         journal.appendCommitRecord(i);
         
      }
      long end = System.currentTimeMillis();
      
      
      System.out.println("Value = " + (end - start));
      
      journal.stop();
      
      
      
   }

   public void testDeleteme() throws Exception
   {

      JournalImpl journal = new JournalImpl(1024 * 1024 * 10, // 10M.. we believe that's the usual cilinder
                                            // size.. not an exact science here
                                            10, // number of files pre-allocated
                                            true, // sync on commit
                                            false, // no sync on non transactional
                                            new AIOSequentialFileFactory(getTestDir(), 1024 * 1024, 2), // AIO or NIO
                                            "jbm", // file name
                                            "jbm", // extension
                                            500); // it's like a semaphore for callback on the AIO layer
      // this during record writes

      journal.start();
      journal.load(dummyLoader);

      FakeMessage msg = new FakeMessage(1024);
      FakeQueueEncoding update = new FakeQueueEncoding();

      journal.appendAddRecord(1, (byte)1, msg);

      journal.forceMoveNextFile();

      journal.appendUpdateRecord(1, (byte)2, update);

      journal.appendAddRecord(2, (byte)1, msg);

      journal.appendDeleteRecord(1);

      journal.forceMoveNextFile();

      journal.appendDeleteRecord(2);

      journal.stop();

      journal = new JournalImpl(1024 * 1024 * 10, // 10M.. we believe that's the usual cilinder
                                // size.. not an exact science here
                                2, // number of files pre-allocated
                                true, // sync on commit
                                false, // no sync on non transactional
                                new AIOSequentialFileFactory(getTestDir(), 1024 * 1024, 2), // AIO or NIO
                                "jbm", // file name
                                "jbm", // extension
                                500); // it's like a semaphore for callback on the AIO layer
      // this during record writes

      journal.start();
      journal.load(dummyLoader);

   }

   public void testJournal(SequentialFileFactory fileFactory, long records, int size, int numberOfFiles, int journalSize) throws Exception
   {

      JournalImpl journal = new JournalImpl(journalSize, // 10M.. we believe that's the usual cilinder
                                            // size.. not an exact science here
                                            numberOfFiles, // number of files pre-allocated
                                            true, // sync on commit
                                            false, // no sync on non transactional
                                            fileFactory, // AIO or NIO
                                            "jbm", // file name
                                            "jbm", // extension
                                            500); // it's like a semaphore for callback on the AIO layer
      // this during record writes

      journal.start();
      journal.load(dummyLoader);

      FakeMessage msg = new FakeMessage(size);
      FakeQueueEncoding update = new FakeQueueEncoding();

      long timeStart = System.currentTimeMillis();
      for (long i = 0; i < records; i++)
      {
//         System.out.println("record # " + i);
         if (i == WARMUP_RECORDS)
         {
            timeStart = System.currentTimeMillis();
         }
         journal.appendAddRecord(i, ADD_RECORD, msg);
         if (PERFORM_UPDATE)
         {
            journal.appendUpdateRecord(i, UPDATE1, update);
         }
      }

      System.out.println("Produced records before stop " + (NUM_RECORDS - WARMUP_RECORDS) +
                         " in " +
                         (System.currentTimeMillis() - timeStart) +
                         " milliseconds");

      journal.stop();

      System.out.println("Produced records after stop " + (NUM_RECORDS - WARMUP_RECORDS) +
                         " in " +
                         (System.currentTimeMillis() - timeStart) +
                         " milliseconds");

   }

   class FakeMessage implements EncodingSupport
   {
      final int size;

      byte bytes[];

      FakeMessage(int size)
      {
         this.size = size;
         bytes = new byte[size];
         for (int i = 0; i < size; i++)
         {
            bytes[i] = (byte)'a';
         }
      }

      public void decode(MessagingBuffer buffer)
      {
      }

      public void encode(MessagingBuffer buffer)
      {
         buffer.writeBytes(this.bytes);
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.journal.EncodingSupport#getEncodeSize()
       */
      public int getEncodeSize()
      {
         return size;
      }

   }

   private static class FakeQueueEncoding implements EncodingSupport
   {

      public FakeQueueEncoding()
      {
      }

      public void decode(final MessagingBuffer buffer)
      {
      }

      public void encode(final MessagingBuffer buffer)
      {
         for (int i = 0 ; i < 8; i++)
         {
            buffer.writeByte((byte)'q');
         }
      }

      public int getEncodeSize()
      {
         return 8;
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
