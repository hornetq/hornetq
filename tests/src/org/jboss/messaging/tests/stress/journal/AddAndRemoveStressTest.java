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
import java.util.ArrayList;

import org.jboss.messaging.core.journal.LoadManager;
import org.jboss.messaging.core.journal.PreparedTransactionInfo;
import org.jboss.messaging.core.journal.RecordInfo;
import org.jboss.messaging.core.journal.SequentialFileFactory;
import org.jboss.messaging.core.journal.impl.AIOSequentialFileFactory;
import org.jboss.messaging.core.journal.impl.JournalImpl;
import org.jboss.messaging.tests.unit.core.journal.impl.fakes.SimpleEncoding;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class AddAndRemoveStressTest extends UnitTestCase
{
   
   protected String journalDir = System.getProperty("java.io.tmpdir", "/tmp") + "/journal-test";
   
   // Constants -----------------------------------------------------

   private static final LoadManager dummyLoader = new LoadManager(){

      public void addPreparedTransaction(
            PreparedTransactionInfo preparedTransaction)
      {
      }

      public void addRecord(RecordInfo info)
      {
      }

      public void deleteRecord(long id)
      {
      }

      public void updateRecord(RecordInfo info)
      {
      }};
   

   private static final long NUMBER_OF_MESSAGES = 210000l;
   
   // Attributes ----------------------------------------------------
   
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------
   
   public void testInsertAndLoad() throws Exception
   {
      
      File file = new File(journalDir);
      deleteDirectory(file);
      file.mkdirs();
      
      SequentialFileFactory factory = new AIOSequentialFileFactory(journalDir);
      JournalImpl impl = new JournalImpl(10*1024*1024, 60, true, false, factory, "jbm", "jbm", 1000, 0);

      impl.start();
      
      impl.load(dummyLoader);
      
      for (long i = 1; i <= NUMBER_OF_MESSAGES; i++)
      {
         if (i % 10000 == 0)
         {
            System.out.println("Append " + i);
         }
         impl.appendAddRecord(i, (byte)0, new SimpleEncoding(1024, (byte)'f'));
      }
      
      impl.stop();
      
      
      factory = new AIOSequentialFileFactory(journalDir);
      impl = new JournalImpl(10*1024*1024, 60, true, false, factory, "jbm", "jbm", 1000, 0);

      impl.start();
      
      impl.load(dummyLoader);

      for (long i = 1; i <= NUMBER_OF_MESSAGES; i++)
      {
         if (i % 10000 == 0)
         {
            System.out.println("Delete " + i);
         }
         
         impl.appendDeleteRecord(i);
      }
      
      impl.stop();
      
      factory = new AIOSequentialFileFactory(journalDir);
      impl = new JournalImpl(10*1024*1024, 60, true, false, factory, "jbm", "jbm", 1000, 0);

      impl.start();
      

      
      ArrayList<RecordInfo> info = new ArrayList<RecordInfo>();
      ArrayList<PreparedTransactionInfo> trans  = new ArrayList<PreparedTransactionInfo>();
      
      impl.load(info, trans);

      
      if (info.size() > 0)
      {
         System.out.println("Info ID: " + info.get(0).id);
      }

      assertEquals(0, info.size());
      assertEquals(0, trans.size());
      
      
   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
   
}
