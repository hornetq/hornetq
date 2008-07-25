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

import java.io.File;
import java.nio.ByteBuffer;

import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.jboss.messaging.core.asyncio.AsynchronousFile;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.journal.IOCallback;
import org.jboss.messaging.core.journal.SequentialFile;
import org.jboss.messaging.core.journal.impl.AIOSequentialFile;
import org.jboss.messaging.tests.util.UnitTestCase;


/**
 * Test AIOSEquentialFile using an EasyMock
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class AIOSequentialFileTest extends UnitTestCase
{
   
   // Constants -----------------------------------------------------
   
   // Attributes ----------------------------------------------------

   AsynchronousFile mockFile;
   
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------
   
   public void testOpen() throws Exception
   {
      openFile();
   }

   public void testAlignment() throws Exception
   {
      SequentialFile file = new MockAIOSequentialFileImpl("/tmp", "nothing", 1);
      
      try
      {
         file.getAlignment();
         fail("Exception expected");
      }
      catch (Exception ignored)
      {
      }
      
      file = openFile();
      
      EasyMock.expect(mockFile.getBlockSize()).andReturn(512);
      
      EasyMock.replay(mockFile);
      
      assertEquals(512, file.getAlignment());
      
      EasyMock.verify(mockFile);
   }
   
   public void testCalculateblockStart() throws Exception
   {
      SequentialFile file = new MockAIOSequentialFileImpl("/tmp", "nothing", 1);
      
      try
      {
         file.calculateBlockStart(10);
         fail("Exception expected");
      }
      catch (Exception ignored)
      {
      }
      
      file = openFile();
      
      EasyMock.expect(mockFile.getBlockSize()).andReturn(512);
      
      EasyMock.replay(mockFile);
      
      assertEquals(1024, file.calculateBlockStart(900));
      
      EasyMock.verify(mockFile);
   }
   
   public void testClose() throws Exception
   {
      SequentialFile file = new MockAIOSequentialFileImpl("/tmp", "nothing", 1);
      
      try
      {
         file.close();
         fail("Exception expected");
      }
      catch (Exception ignored)
      {
      }
      
      file = openFile();
      
      mockFile.close();
      
      EasyMock.replay(mockFile);
      
      file.close();
      
      EasyMock.verify(mockFile);
   }
   
   public void testDelete() throws Exception
   {
      File tmpFile = File.createTempFile("temporaryTestFile", ".tmp");
      
      assertTrue(tmpFile.exists());
      
      SequentialFile fileImpl = new MockAIOSequentialFileImpl(tmpFile.getParent(), tmpFile.getName(), 1);
      
      fileImpl.delete();

      // delete on a closed file
      assertFalse(tmpFile.exists());
      
      tmpFile = File.createTempFile("temporaryTestFile", ".tmp");
      
      assertTrue(tmpFile.exists());
      
      fileImpl = openFile(tmpFile.getParent(), tmpFile.getName());
      
      mockFile.close();
      
      EasyMock.replay(mockFile);
      
      fileImpl.delete();
      
      assertFalse(tmpFile.exists());
      
      EasyMock.verify(mockFile);
   }
   
   public void testFill() throws Exception
   {
      SequentialFile fileImpl = openFile();
      
      validateFill(fileImpl, 3*100*1024*1024, 3, 100*1024*1024);
      
      validateFill(fileImpl, 3*10*1024*1024, 3, 10*1024*1024);
      
      validateFill(fileImpl, 7*1024*1024, 7, 1024*1024);
      
      validateFill(fileImpl, 7*10*1024, 7, 10*1024);
      
      validateFill(fileImpl, 7*512, 7, 512);
      
      validateFill(fileImpl, 300, 1, 512);
   }
   
   public void testWriteWithCallback() throws Exception
   {
      SequentialFile fileImpl = openFile();      
      
      ByteBuffer buffer = ByteBuffer.allocate(512);
      
      IOCallback callback = new IOCallback()
      {

         public void done()
         {
         }

         public void onError(int errorCode, String errorMessage)
         {
         }
      };

      mockFile.write(EasyMock.eq(512l * 3l), EasyMock.eq(512l), EasyMock.same(buffer), EasyMock.same(callback));

      mockFile.close();      

      EasyMock.replay(mockFile);
      
      fileImpl.position(512 * 3);
      
      fileImpl.write(buffer, callback);
      
      // We need that to make sure the executor is cleared before the verify
      fileImpl.close();
      
      EasyMock.verify(mockFile);
   }

   public void testWriteWithSyncOnCallback() throws Exception
   {
      SequentialFile fileImpl = openFile();      
      
      ByteBuffer buffer = ByteBuffer.allocate(512);

      mockFile.write(EasyMock.eq(512l * 3l), EasyMock.eq(512l), EasyMock.same(buffer), EasyMock.isA(IOCallback.class));
      
      EasyMock.expectLastCall().andAnswer(new IAnswer<Object>(){

         public Object answer() throws Throwable
         {
            IOCallback callback = (IOCallback) EasyMock.getCurrentArguments()[3];
            
            callback.done();
            
            return null;
         }
         
      });

      EasyMock.replay(mockFile);
      
      fileImpl.position(512 * 3);
      
      fileImpl.write(buffer, true);
      
      EasyMock.verify(mockFile);
   }

   public void testWriteWithNoSyncOnCallback() throws Exception
   {
      SequentialFile fileImpl = openFile();      
      
      ByteBuffer buffer = ByteBuffer.allocate(512);

      mockFile.write(EasyMock.eq(512l * 3l), EasyMock.eq(512l), EasyMock.same(buffer), EasyMock.isA(IOCallback.class));

      EasyMock.expectLastCall().andAnswer(new IAnswer<Object>(){

         public Object answer() throws Throwable
         {
            IOCallback callback = (IOCallback) EasyMock.getCurrentArguments()[3];
            
            callback.done();
            
            return null;
         }
         
      });

      mockFile.close();
      
      EasyMock.replay(mockFile);
      
      fileImpl.position(512 * 3);
      
      fileImpl.write(buffer, false);
      
      fileImpl.close();
      
      EasyMock.verify(mockFile);
   }

   public void testWriteWithSyncAndCallbackError() throws Exception
   {
      SequentialFile fileImpl = openFile();      
      
      ByteBuffer buffer = ByteBuffer.allocate(512);

      mockFile.write(EasyMock.eq(512l * 3l), EasyMock.eq(512l), EasyMock.same(buffer), EasyMock.isA(IOCallback.class));
      
      EasyMock.expectLastCall().andAnswer(new IAnswer<Object>(){

         public Object answer() throws Throwable
         {
            IOCallback callback = (IOCallback) EasyMock.getCurrentArguments()[3];
            
            callback.onError(100, "Fake Message");
            
            return null;
         }
         
      });

      EasyMock.replay(mockFile);
      
      fileImpl.position(512 * 3);
      
      try
      {
         fileImpl.write(buffer, true);
         fail("Exception was expected");
      }
      catch (MessagingException e)
      {
         assertEquals(100, e.getCode());
         assertEquals("Fake Message", e.getMessage());
      }
      
      EasyMock.verify(mockFile);
   }

   public void testWriteWithSyncAndException() throws Exception
   {
      SequentialFile fileImpl = openFile();      
      
      ByteBuffer buffer = ByteBuffer.allocate(512);

      mockFile.write(EasyMock.eq(512l * 3l), EasyMock.eq(512l), EasyMock.same(buffer), EasyMock.isA(IOCallback.class));
      
      EasyMock.expectLastCall().andAnswer(new IAnswer<Object>(){

         public Object answer() throws Throwable
         {
            throw new IllegalArgumentException("Fake Message");
         }
         
      });

      EasyMock.replay(mockFile);
      
      fileImpl.position(512 * 3);
      
      try
      {
         fileImpl.write(buffer, true);
         fail("Exception was expected");
      }
      catch (MessagingException e)
      {
         assertEquals(-1, e.getCode());
         assertEquals("Fake Message", e.getMessage());
      }
      
      EasyMock.verify(mockFile);
   }

   public void testReadWithCallback() throws Exception
   {
      SequentialFile fileImpl = openFile();      
      
      ByteBuffer buffer = ByteBuffer.allocate(512);
      
      IOCallback callback = new IOCallback()
      {

         public void done()
         {
         }

         public void onError(int errorCode, String errorMessage)
         {
         }
      };
      
      mockFile.read(EasyMock.eq(512l * 3l), EasyMock.eq(512l), EasyMock.same(buffer), EasyMock.same(callback));

      EasyMock.replay(mockFile);
      
      fileImpl.position(512 * 3);
      
      fileImpl.read(buffer, callback);
      
      EasyMock.verify(mockFile);
   }

   public void testReadWithoutCallback() throws Exception
   {
      SequentialFile fileImpl = openFile();      
      
      ByteBuffer buffer = ByteBuffer.allocate(512);
      
      mockFile.read(EasyMock.eq(512l * 3l), EasyMock.eq(512l), EasyMock.same(buffer), EasyMock.isA(IOCallback.class));
      
      EasyMock.expectLastCall().andAnswer(new IAnswer<Object>(){

         public Object answer() throws Throwable
         {
            IOCallback callback = (IOCallback) EasyMock.getCurrentArguments()[3];
            
            callback.done();
            
            return null;
         }
         
      });
      

      EasyMock.replay(mockFile);
      
      fileImpl.position(512 * 3);
      
      fileImpl.read(buffer);
      
      EasyMock.verify(mockFile);
   }

   public void testReadWithoutCallbackOnError() throws Exception
   {
      SequentialFile fileImpl = openFile();      
      
      ByteBuffer buffer = ByteBuffer.allocate(512);
      
      mockFile.read(EasyMock.eq(512l * 3l), EasyMock.eq(512l), EasyMock.same(buffer), EasyMock.isA(IOCallback.class));
      
      EasyMock.expectLastCall().andAnswer(new IAnswer<Object>(){

         public Object answer() throws Throwable
         {
            IOCallback callback = (IOCallback) EasyMock.getCurrentArguments()[3];
            
            callback.onError(100, "Fake Message");
            
            return null;
         }
         
      });
      

      EasyMock.replay(mockFile);
      
      fileImpl.position(512 * 3);
      
      try
      {
         fileImpl.read(buffer);
         fail("Expected Exception");
      }
      catch (MessagingException e)
      {
         assertEquals(100, e.getCode());
         assertEquals("Fake Message", "Fake Message");
      }
      
      EasyMock.verify(mockFile);
   }

   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   protected void setUp()
   {
      this.mockFile = null;
   }
   
   // Private -------------------------------------------------------

   private void validateFill(SequentialFile fileImpl, int totalSize, int numberOfBlocksExpected, long blockSizeExpected) throws Exception
   {
      EasyMock.expect(mockFile.getBlockSize()).andReturn(512);

      mockFile.fill(512, numberOfBlocksExpected, blockSizeExpected, (byte)'b');

      EasyMock.replay(mockFile);
      
      fileImpl.fill(5, totalSize, (byte)'b');
      
      EasyMock.verify(mockFile);
      
      EasyMock.reset(mockFile);
   }

   private SequentialFile openFile() throws Exception
   {
      return openFile ("/tmp", "nothing");
   }

   private SequentialFile openFile(String directory, String fileName) throws Exception
   {
      mockFile = EasyMock.createStrictMock(AsynchronousFile.class);
      
      mockFile.open(directory + "/" + fileName, 1);
      
      EasyMock.replay(mockFile);
      
      SequentialFile file = new MockAIOSequentialFileImpl(directory, fileName, 1);
      
      file.open();
      
      EasyMock.verify(mockFile);
      
      EasyMock.reset(mockFile);

      return file;
   }
   

   // Inner classes -------------------------------------------------
   
   class MockAIOSequentialFileImpl extends AIOSequentialFile
   {

      public MockAIOSequentialFileImpl(String journalDir, String fileName, int maxIO)
            throws Exception
      {
         super(journalDir, fileName, maxIO);
      }
      
      
      protected AsynchronousFile newFile()
      {
         return mockFile;
      }
      
   }
   
}
