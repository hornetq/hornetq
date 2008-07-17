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


package org.jboss.messaging.tests.unit.core.journal.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.easymock.IArgumentMatcher;
import org.jboss.messaging.core.journal.SequentialFile;
import org.jboss.messaging.core.journal.SequentialFileFactory;
import org.jboss.messaging.core.journal.impl.JournalImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.tests.unit.core.journal.impl.fakes.SimpleEncoding;
import org.jboss.messaging.tests.util.UnitTestCase;

public class EasyMockJournalTest extends UnitTestCase
{
   
   // Constants -----------------------------------------------------
   
   // Attributes ----------------------------------------------------

   SequentialFileFactory mockFactory = null;
   SequentialFile file1 = null;
   SequentialFile file2 = null;
   JournalImpl journalImpl = null;
   
   // Static --------------------------------------------------------

   private static final Logger log = Logger.getLogger(EasyMockJournalTest.class);
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   public void testAppendRecord() throws Exception
   {
      EasyMock.expect(file1.write(compareByteBuffer(autoEncode(JournalImpl.ADD_RECORD, 
                                               /*FileID*/1, 
                                               /*RecordLength*/1, 
                                               /* ID */14l, 
                                               /*RecordType*/(byte)33, 
                                               /* body */(byte)10, 
                                               JournalImpl.SIZE_ADD_RECORD + 1)), EasyMock.eq(true))).andReturn(JournalImpl.SIZE_ADD_RECORD + 1);

      
      EasyMock.replay(mockFactory, file1, file2);
      
      journalImpl.appendAddRecord(14l, (byte) 33, new byte[]{ (byte) 10 });
      
      EasyMock.verify(mockFactory, file1, file2);

      EasyMock.reset(mockFactory, file1, file2);
      
      stubValues();

      EasyMock.expect(file1.write(compareByteBuffer(autoEncode(JournalImpl.ADD_RECORD, 
            /*FileID*/1, 
            /*RecordLength*/1, 
            /* ID */14l, 
            /*RecordType*/(byte)33, 
            /* body */(byte)10, 
            JournalImpl.SIZE_ADD_RECORD + 1)), EasyMock.eq(true))).andReturn(JournalImpl.SIZE_ADD_RECORD + 1);
      
      EasyMock.replay(mockFactory, file1, file2);
      
      journalImpl.appendAddRecord(14l, (byte)33, new SimpleEncoding(1,(byte)10));

      EasyMock.verify(mockFactory, file1, file2);

   }


   public void testDeleteRecord() throws Exception
   {
      EasyMock.expect(file1.write(compareByteBuffer(autoEncode(JournalImpl.ADD_RECORD, 
                                               /*FileID*/1, 
                                               /*RecordLength*/1, 
                                               /* ID */14l, 
                                               /*RecordType*/(byte)33, 
                                               /* body */(byte)10, 
                                               JournalImpl.SIZE_ADD_RECORD + 1)), EasyMock.eq(true))).andReturn(JournalImpl.SIZE_ADD_RECORD + 1);

      
      EasyMock.expect(file1.write(compareByteBuffer(autoEncode(JournalImpl.DELETE_RECORD, 
            /*FileID*/1, 
            /* ID */14l, 
            JournalImpl.SIZE_DELETE_RECORD)), EasyMock.eq(true))).andReturn(JournalImpl.SIZE_DELETE_RECORD);

      EasyMock.replay(mockFactory, file1, file2);
      
      journalImpl.appendAddRecord(14l, (byte) 33, new byte[]{ (byte) 10 });
      
      journalImpl.appendDeleteRecord(14l);

      EasyMock.verify(mockFactory, file1, file2);
   }
   
   public void testDeleteTransRecord() throws Exception
   {
      EasyMock.expect(file1.write(compareByteBuffer(autoEncode(JournalImpl.ADD_RECORD, 
            /*FileID*/1, 
            /*RecordLength*/1, 
            /* ID */15l, 
            /*RecordType*/(byte)33, 
            /* body */(byte)10, 
            JournalImpl.SIZE_ADD_RECORD + 1)), EasyMock.eq(true))).andReturn(JournalImpl.SIZE_ADD_RECORD + 1);


      EasyMock.expect(file1.write(compareByteBuffer(autoEncode(JournalImpl.DELETE_RECORD_TX, 
            /*FileID*/1, 
            /* Transaction ID*/ 100l,
            /* ID */15l, 
            JournalImpl.SIZE_DELETE_RECORD_TX)), EasyMock.eq(false))).andReturn(JournalImpl.SIZE_DELETE_RECORD_TX);
      
      EasyMock.expect(file1.write(compareByteBuffer(autoEncode(JournalImpl.PREPARE_RECORD, 
            /*FileID*/1, 
            /* Transaction ID*/ 100l,
            /* Number of Elements */ 1,
            JournalImpl.SIZE_PREPARE_RECORD)), EasyMock.eq(true))).andReturn(JournalImpl.SIZE_PREPARE_RECORD);
      
      EasyMock.expect(file1.write(compareByteBuffer(autoEncode(JournalImpl.COMMIT_RECORD, 
            /*FileID*/1, 
            /* Transaction ID*/ 100l,
            /* Number of Elements */ 1,
            JournalImpl.SIZE_COMMIT_RECORD)), EasyMock.eq(true))).andReturn(JournalImpl.SIZE_COMMIT_RECORD);
      
      EasyMock.replay(mockFactory, file1, file2);
      
      journalImpl.appendAddRecord(15l, (byte) 33, new byte[]{ (byte) 10 });
      
      journalImpl.appendDeleteRecordTransactional(100l, 15l);
      
      journalImpl.appendPrepareRecord(100l);
      
      journalImpl.appendCommitRecord(100l);
      
      EasyMock.verify(mockFactory, file1, file2);
   }

   public void testAppendAndCommitRecord() throws Exception
   {
      EasyMock.expect(
            file1.write(compareByteBuffer(autoEncode(JournalImpl.ADD_RECORD_TX,
            /* FileID */1,
            /* RecordLength */1,
            /* TXID */3l,
            /* RecordType */(byte) 33,
            /* ID */14l,
            /* body */(byte) 10, JournalImpl.SIZE_ADD_RECORD_TX + 1)),
                  EasyMock.eq(false))).andReturn(
            JournalImpl.SIZE_ADD_RECORD_TX + 1);
      
      EasyMock.expect(
            file1.write(compareByteBuffer(autoEncode(JournalImpl.ADD_RECORD_TX,
            /* FileID */1,
            /* RecordLength */1,
            /* TXID */3l,
            /* RecordType */(byte) 33,
            /* ID */15l,
            /* body */(byte) 10, JournalImpl.SIZE_ADD_RECORD_TX + 1)),
                  EasyMock.eq(false))).andReturn(
            JournalImpl.SIZE_ADD_RECORD_TX + 1);

      EasyMock.expect(file1.write(compareByteBuffer(autoEncode(JournalImpl.PREPARE_RECORD, 
            /*FileID*/1, 
            /* TXID */ 3l,
            /* Number Of Elements */ 2,
            JournalImpl.SIZE_PREPARE_RECORD)), EasyMock.eq(true))).andReturn(JournalImpl.SIZE_PREPARE_RECORD);
      
      EasyMock.expect(file1.write(compareByteBuffer(autoEncode(JournalImpl.COMMIT_RECORD, 
            /*FileID*/1, 
            /* TXID */ 3l,
            /* Number Of Elements */ 2,
            JournalImpl.SIZE_COMMIT_RECORD)), EasyMock.eq(true))).andReturn(JournalImpl.SIZE_COMMIT_RECORD);

      EasyMock.replay(mockFactory, file1, file2);
      
      journalImpl.appendAddRecordTransactional(3, 14l, (byte)33, new SimpleEncoding(1,(byte)10));
      
      journalImpl.appendAddRecordTransactional(3, 15l, (byte) 33, new byte[]{ (byte) 10 });
      
      journalImpl.appendPrepareRecord(3l);
      
      journalImpl.appendCommitRecord(3l);
      
      EasyMock.verify(mockFactory, file1, file2);
   }

   public void testAppendAndRollbacktRecord() throws Exception
   {
      EasyMock.expect(
            file1.write(compareByteBuffer(autoEncode(JournalImpl.ADD_RECORD_TX,
            /* FileID */1,
            /* RecordLength */1,
            /* TXID */3l,
            /* RecordType */(byte) 33,
            /* ID */14l,
            /* body */(byte) 10, JournalImpl.SIZE_ADD_RECORD_TX + 1)),
                  EasyMock.eq(false))).andReturn(
            JournalImpl.SIZE_ADD_RECORD_TX + 1);
      
      EasyMock.expect(file1.write(compareByteBuffer(autoEncode(JournalImpl.ROLLBACK_RECORD, 
            /*FileID*/1, 
            /* TXID */ 3l,
            /* NumberOfElements */ 1,
            JournalImpl.SIZE_ROLLBACK_RECORD)), EasyMock.eq(true))).andReturn(JournalImpl.SIZE_ROLLBACK_RECORD);

      EasyMock.replay(mockFactory, file1, file2);
      
      journalImpl.appendAddRecordTransactional(3, 14l, (byte)33, new SimpleEncoding(1,(byte)10));
      
      journalImpl.appendRollbackRecord(3l);
      
      EasyMock.verify(mockFactory, file1, file2);
   }
   
   public void testupdateRecordNonTrans() throws Exception
   {
      EasyMock.expect(file1.write(compareByteBuffer(autoEncode(JournalImpl.ADD_RECORD, 
            /* FileID */1, 
            /* RecordLength */1, 
            /* ID */15l, 
            /* RecordType */(byte)33, 
            /* body */(byte)10, 
            JournalImpl.SIZE_ADD_RECORD + 1)), EasyMock.eq(true))).andReturn(JournalImpl.SIZE_ADD_RECORD + 1);

      EasyMock.expect(file1.write(compareByteBuffer(autoEncode(JournalImpl.UPDATE_RECORD, 
            /* FileID */1, 
            /* RecordLength */1, 
            /* ID */15l, 
            /* RecordType */(byte)34, 
            /* body */(byte)11, 
            JournalImpl.SIZE_UPDATE_RECORD + 1)), EasyMock.eq(true))).andReturn(JournalImpl.SIZE_UPDATE_RECORD + 1);
      
      EasyMock.expect(file1.write(compareByteBuffer(autoEncode(JournalImpl.UPDATE_RECORD, 
            /* FileID */1, 
            /* RecordLength */1, 
            /* ID */15l, 
            /* RecordType */(byte)35, 
            /* body */(byte)12, 
            JournalImpl.SIZE_UPDATE_RECORD + 1)), EasyMock.eq(true))).andReturn(JournalImpl.SIZE_UPDATE_RECORD + 1);
      
     EasyMock.replay(mockFactory, file1, file2);
      
      journalImpl.appendAddRecord(15l, (byte) 33, new byte[]{ (byte) 10 });
      
      journalImpl.appendUpdateRecord(15l, (byte)34, new SimpleEncoding(1, (byte)11));
      
      journalImpl.appendUpdateRecord(15l, (byte)35, new byte[]{ (byte) 12});
      
      EasyMock.verify(mockFactory, file1, file2);

   }

   
   public void testupdateRecordTrans() throws Exception
   {
      EasyMock.expect(file1.write(compareByteBuffer(autoEncode(JournalImpl.ADD_RECORD, 
            /* FileID */1, 
            /* RecordLength */1, 
            /* ID */15l, 
            /* RecordType */(byte)33, 
            /* body */(byte)10, 
            JournalImpl.SIZE_ADD_RECORD + 1)), EasyMock.eq(true))).andReturn(JournalImpl.SIZE_ADD_RECORD + 1);

      EasyMock.expect(file1.write(compareByteBuffer(autoEncode(JournalImpl.UPDATE_RECORD_TX, 
            /* FileID */1, 
            /* RecordLength */1,
            /* TransactionID */33l,
            /* RecordType */ (byte)34,
            /* ID */15l, 
            /* body */(byte)11, 
            JournalImpl.SIZE_UPDATE_RECORD_TX + 1)), EasyMock.eq(false))).andReturn(JournalImpl.SIZE_UPDATE_RECORD_TX + 1);
      
      EasyMock.expect(file1.write(compareByteBuffer(autoEncode(JournalImpl.UPDATE_RECORD_TX, 
            /* FileID */1, 
            /* RecordLength */1,
            /* TransactionID */33l,
            /* RecordType */ (byte)35,
            /* ID */15l, 
            /* body */(byte)12, 
            JournalImpl.SIZE_UPDATE_RECORD_TX + 1)), EasyMock.eq(false))).andReturn(JournalImpl.SIZE_UPDATE_RECORD_TX + 1);
      
      EasyMock.expect(file1.write(compareByteBuffer(autoEncode(JournalImpl.COMMIT_RECORD, 
            /*FileID*/1, 
            /* Transaction ID*/ 33l,
            /* NumberOfElements*/ 2,
            JournalImpl.SIZE_COMMIT_RECORD)), EasyMock.eq(true))).andReturn(JournalImpl.SIZE_COMMIT_RECORD);
      
      EasyMock.replay(mockFactory, file1, file2);
      
      journalImpl.appendAddRecord(15l, (byte) 33, new byte[]{ (byte) 10 });
      
      journalImpl.appendUpdateRecordTransactional(33l, 15l, (byte)34, new SimpleEncoding(1, (byte)11));
      
      journalImpl.appendUpdateRecordTransactional(33l, 15l, (byte)35, new byte[]{ (byte) 12});
      
      journalImpl.appendCommitRecord(33l);
      
      EasyMock.verify(mockFactory, file1, file2);

   }

   // Protected -----------------------------------------------------
   
   protected void tearDown() throws Exception
   {
      super.tearDown();
      
      if (journalImpl != null)
      {
         EasyMock.reset(mockFactory, file1, file2);
         stubValues();
         try {journalImpl.stop();} catch (Throwable ignored) {}
      }
      
   }
   
   protected void setUp() throws Exception
   {
      journalImpl = newJournal();
   }
   
   // Private -------------------------------------------------------
   
   private JournalImpl newJournal() throws Exception
   {
      mockFactory = EasyMock.createMock(SequentialFileFactory.class);
      file1 = EasyMock.createMock(SequentialFile.class);
      file2 = EasyMock.createMock(SequentialFile.class);
      
      stubValues();
      
      EasyMock.expect(mockFactory.createSequentialFile(EasyMock.isA(String.class), EasyMock.anyInt(), EasyMock.anyLong())).andReturn(file1);
      
      EasyMock.expect(mockFactory.createSequentialFile(EasyMock.isA(String.class), EasyMock.anyInt(), EasyMock.anyLong())).andReturn(file2);
      
      file1.open();
      
      EasyMock.expectLastCall().anyTimes();
      
      file2.open();
      
      EasyMock.expectLastCall().anyTimes();
      
      file1.close();
      
      EasyMock.expectLastCall().anyTimes();
      
      file2.close();
      
      EasyMock.expectLastCall().anyTimes();
      
      file1.fill(0, 100 * 1024, (byte) 'J');
      
      file2.fill(0, 100 * 1024, (byte) 'J');
      
      EasyMock.expect(file1.write(compareByteBuffer(autoEncode((int)1)), EasyMock.eq(true))).andReturn(4);
      EasyMock.expect(file2.write(compareByteBuffer(autoEncode((int)2)), EasyMock.eq(true))).andReturn(4);
      
      file1.position(4);
      
      file2.position(4);
      
      EasyMock.replay(mockFactory, file1, file2);
      
      JournalImpl journalImpl = new JournalImpl(100 * 1024, 2,
            true, true,
            mockFactory,
            "tt", "tt", 1000, 1000);
      
      journalImpl.start();
      
      journalImpl.load(new ArrayList(), new ArrayList());
      
      EasyMock.verify(mockFactory, file1, file2);
      
      EasyMock.reset(mockFactory, file1, file2);
      
      stubValues();
      
      return journalImpl;
   }


   private void stubValues() throws Exception
   {
      EasyMock.expect(mockFactory.getAlignment()).andStubReturn(1);
      EasyMock.expect(mockFactory.isSupportsCallbacks()).andStubReturn(false);

      EasyMock.expect(mockFactory.listFiles("tt")).andStubReturn(
            new ArrayList<String>());
      
      EasyMock.expect(mockFactory.newBuffer(EasyMock.anyInt())).andStubAnswer(
            new IAnswer<ByteBuffer>()
            {
               
               public ByteBuffer answer() throws Throwable
               {
                  Integer valueInt = (Integer) EasyMock.getCurrentArguments()[0];
                  
                  return ByteBuffer.allocateDirect(valueInt);
               }
            });
      
      EasyMock.expect(file1.calculateBlockStart(EasyMock.anyInt()))
            .andStubAnswer(new IAnswer<Integer>()
            {
               
               public Integer answer() throws Throwable
               {
                  return (Integer) EasyMock.getCurrentArguments()[0];
               }
            });
      
      EasyMock.expect(file2.calculateBlockStart(EasyMock.anyInt()))
            .andStubAnswer(new IAnswer<Integer>()
            {
               
               public Integer answer() throws Throwable
               {
                  return (Integer) EasyMock.getCurrentArguments()[0];
               }
            });
      
      
      EasyMock.expect(file1.getAlignment()).andStubReturn(1);
      EasyMock.expect(file2.getAlignment()).andStubReturn(1);
      
   }
   
   
   private ByteBuffer compareByteBuffer(final byte expectedArray[])
   {
      
      EasyMock.reportMatcher(new IArgumentMatcher()
      {

         public void appendTo(StringBuffer buffer)
         {
            buffer.append("ByteArray");
         }

         public boolean matches(Object argument)
         {
            ByteBuffer buffer = (ByteBuffer) argument;
            
            buffer.rewind();
            byte[] compareArray = new byte[buffer.limit()];
            buffer.get(compareArray);
            
            if (compareArray.length != expectedArray.length)
            {
               return false;
            }
            
            for (int i = 0; i < expectedArray.length; i++)
            {
               if (expectedArray[i] != compareArray[i])
               {
                  return false;
               }
            }
            
            return true;
         }
         
      });
      
      return null;
   }

   // Package protected ---------------------------------------------
   
   // Inner classes -------------------------------------------------
   
   
   
}
