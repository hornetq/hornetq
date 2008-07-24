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

package org.jboss.messaging.tests.timing.core.journal.impl;

import java.util.ArrayList;

import org.jboss.messaging.core.journal.PreparedTransactionInfo;
import org.jboss.messaging.core.journal.RecordInfo;
import org.jboss.messaging.core.journal.SequentialFileFactory;
import org.jboss.messaging.core.journal.impl.JournalImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.tests.unit.core.journal.impl.fakes.FakeSequentialFileFactory;
import org.jboss.messaging.tests.unit.core.journal.impl.fakes.SimpleEncoding;
import org.jboss.messaging.tests.util.UnitTestCase;

public class JournalAsyncTimeoutsTest extends UnitTestCase
{
   
   // Constants -----------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   private FakeSequentialFileFactory factory;
   
   JournalImpl journalImpl = null;
   
   private ArrayList<RecordInfo> records = null;
   
   private ArrayList<PreparedTransactionInfo> transactions = null;
   
   // Static --------------------------------------------------------
   
   private static final Logger log = Logger
         .getLogger(JournalAsyncTimeoutsTest.class);
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------
   
   public void testAsynchronousCommit() throws Exception
   {
//      final int JOURNAL_SIZE = 20000;
//      
//      setupJournal(JOURNAL_SIZE, 100, 5);
//      
//      assertEquals(2, factory.listFiles("tt").size());
//      
//      assertEquals(0, records.size());
//      assertEquals(0, transactions.size());
//      
//      for (int i = 0; i < 10 ; i++)
//      {
//         journalImpl.appendAddRecordTransactional(1l, (long)i, (byte)0, new SimpleEncoding(1, (byte)15));
//         journalImpl.forceMoveNextFile();
//      }
//      
//      
//      for (int i = 10; i < 20 ; i++)
//      {
//         journalImpl.appendAddRecordTransactional(1l, (long)i, (byte)0, new SimpleEncoding(1, (byte)15));
//         journalImpl.forceMoveNextFile();
//      }
//      
//      journalImpl.forceMoveNextFile();
//      
//      journalImpl.appendCommitRecord(1l);
//      
   }
   
   
   
   public void testTransactionTimeoutOnCommit() throws Exception
   {
      final int JOURNAL_SIZE = 20000;
      
      setupJournal(JOURNAL_SIZE, 1, 5, 1000);
      
      assertEquals(5, factory.listFiles("tt").size());
      
      assertEquals(0, records.size());
      assertEquals(0, transactions.size());

      factory.setHoldCallbacks(true);
      
      for (int i = 0; i < 20; i++)
      {
         journalImpl.appendAddRecordTransactional(1l, (long) i, (byte) 0,
               new SimpleEncoding(1, (byte) 15));
      }
      
      try
      {
         journalImpl.appendCommitRecord(1l);
         fail ("Supposed to timeout");
      }
      catch (Exception e)
      {
      }

      factory.flushAllCallbacks();
      
      factory.setHoldCallbacks(false);
      
      journalImpl.appendRollbackRecord(1l);
      
      setupJournal(JOURNAL_SIZE, 1, 5, 1000);
      
      assertEquals(0, records.size());
      assertEquals(0, journalImpl.getDataFilesCount());
   }
   
   public void testTransactionTimeoutOnRollback() throws Exception
   {
      final int JOURNAL_SIZE = 20000;
      
      setupJournal(JOURNAL_SIZE, 1, 5, 1000);
      
      assertEquals(5, factory.listFiles("tt").size());
      
      assertEquals(0, records.size());
      assertEquals(0, transactions.size());

      factory.setHoldCallbacks(true);
      
      for (int i = 0; i < 20; i++)
      {
         journalImpl.appendAddRecordTransactional(1l, (long) i, (byte) 0,
               new SimpleEncoding(1, (byte) 15));
      }
      
      try
      {
         journalImpl.appendRollbackRecord(1l);
         fail ("Supposed to timeout");
      }
      catch (Exception e)
      {
      }

      factory.flushAllCallbacks();
      
      factory.setHoldCallbacks(false);
      
      // it shouldn't fail
      journalImpl.appendRollbackRecord(1l);
      
      setupJournal(JOURNAL_SIZE, 1, 5, 1000);
      
      assertEquals(0, records.size());
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      
      records = new ArrayList<RecordInfo>();
      
      transactions = new ArrayList<PreparedTransactionInfo>();
      
      factory = null;
      
      journalImpl = null;
      
   }
   
   @Override
   protected void tearDown() throws Exception
   {
      super.tearDown();
      
      if (journalImpl != null)
      {
         try
         {
            journalImpl.stop();
         }
         catch (Throwable ignored)
         {
         }
      }
   }
   
   // Private -------------------------------------------------------
   private void setupJournal(final int journalSize, final int alignment,
         final int numberOfMinimalFiles, final int timeout) throws Exception
   {
      if (factory == null)
      {
         factory = new FakeSequentialFileFactory(alignment, true);
      }
      
      if (journalImpl != null)
      {
         journalImpl.stop();
      }
      
      journalImpl = new JournalImpl(journalSize, numberOfMinimalFiles, true,
            true, factory, "tt", "tt", 1000, timeout);
      
      journalImpl.start();
      
      records.clear();
      transactions.clear();
      
      journalImpl.load(records, transactions);
   }
   
   // Inner classes -------------------------------------------------
   
}
