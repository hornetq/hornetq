package org.jboss.messaging.tests.util;
import java.util.ArrayList;

import org.jboss.messaging.core.journal.PreparedTransactionInfo;
import org.jboss.messaging.core.journal.RecordInfo;
import org.jboss.messaging.core.journal.SequentialFileFactory;
import org.jboss.messaging.core.journal.impl.AIOSequentialFileFactory;
import org.jboss.messaging.core.journal.impl.JournalImpl;
import org.jboss.messaging.core.journal.impl.NIOSequentialFileFactory;
import org.jboss.messaging.utils.TimeAndCounterIDGenerator;

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

/**
 * A JournalExample: Just an example on how to use the Journal Directly
 *
 * TODO: find a better place to store this example
 * 
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class JournalExample
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   
   public static void main(String arg[])
   {
      TimeAndCounterIDGenerator idgenerator = new TimeAndCounterIDGenerator();
      try
      {
         SequentialFileFactory fileFactory = new AIOSequentialFileFactory("/tmp"); // any dir you want
         //SequentialFileFactory fileFactory = new NIOSequentialFileFactory("/tmp"); // any dir you want
         JournalImpl journalExample = new JournalImpl(
                                                      10 * 1024 * 1024, // 10M.. we believe that's the usual cilinder size.. not an exact science here
                                                      2, // number of files pre-allocated
                                                      true, // sync on commit
                                                      false, // no sync on non transactional
                                                      fileFactory, // AIO or NIO
                                                      "exjournal", // file name
                                                      "dat", // extension
                                                       10000, // it's like a semaphore for callback on the AIO layer
                                                      5 * 1024); // avg buffer size.. it will reuse any buffer smaller than this during record writes
         
         ArrayList<RecordInfo> committedRecords = new ArrayList<RecordInfo>();
         ArrayList<PreparedTransactionInfo> preparedTransactions = new ArrayList<PreparedTransactionInfo>();
         journalExample.start();
         System.out.println("Loading records and creating data files");
         journalExample.load(committedRecords, preparedTransactions);

         System.out.println("Loaded Record List:");
         
         for (RecordInfo record: committedRecords)
         {
            System.out.println("Record id = " + record.id + " userType = " + record.userRecordType + " with " + record.data.length + " bytes is stored on the journal");
         }

         System.out.println("Adding Records:");
         
         for (int i = 0 ; i < 10; i++)
         {
            journalExample.appendAddRecord(idgenerator.generateID(), (byte)1, new byte[] { 0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2} );
         }
         
         long tx = idgenerator.generateID(); // some id generation system
         
         for (int i = 0 ; i < 100; i++)
         {
            journalExample.appendAddRecordTransactional(tx, idgenerator.generateID(), (byte)2, new byte[] { 0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2, 5});
         }
         
         // After this is complete, you're sure the records are there
         journalExample.appendCommitRecord(tx);

         System.out.println("Done!");
         
         journalExample.stop();
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }
   
}
