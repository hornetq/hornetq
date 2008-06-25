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
import java.util.List;
import java.util.UUID;

import org.jboss.messaging.core.asyncio.impl.AsynchronousFileImpl;
import org.jboss.messaging.core.journal.SequentialFile;
import org.jboss.messaging.core.journal.SequentialFileFactory;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * 
 * A SequentialFileFactoryTestBase
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public abstract class SequentialFileFactoryTestBase extends UnitTestCase
{
   protected final Logger log = Logger.getLogger(this.getClass());
   
   
   protected void setUp() throws Exception
   {
      super.setUp();
      
      factory = createFactory();
   }
   
   protected void tearDown() throws Exception
   {
      super.tearDown();
      assertEquals(0, AsynchronousFileImpl.getTotalMaxIO());
   }
   
   protected abstract SequentialFileFactory createFactory();
   
   protected SequentialFileFactory factory;
      
   public void testCreateAndListFiles() throws Exception
   {
      List<String> expectedFiles = new ArrayList<String>();
      
      final int numFiles = 10;
      
      for (int i = 0; i < numFiles; i++)
      {
         String fileName = UUID.randomUUID().toString() + ".jbm";
         
         expectedFiles.add(fileName);
         
         SequentialFile sf = factory.createSequentialFile(fileName, 1, 120);
         
         sf.open();
         
         assertEquals(fileName, sf.getFileName());
         
         sf.close();
      }           
      
      //Create a couple with a different extension - they shouldn't be picked up
      
      SequentialFile sf1 = factory.createSequentialFile("different.file", 1, 120);
      sf1.open();
      
      SequentialFile sf2 = factory.createSequentialFile("different.cheese", 1, 120);
      sf2.open();
                  
      List<String> fileNames = factory.listFiles("jbm");
      
      assertEquals(expectedFiles.size(), fileNames.size());
      
      for (String fileName: expectedFiles)
      {
         assertTrue(fileNames.contains(fileName));
      }
      
      fileNames = factory.listFiles("file");
      
      assertEquals(1, fileNames.size());
      
      assertTrue(fileNames.contains("different.file"));  
      
      fileNames = factory.listFiles("cheese");
      
      assertEquals(1, fileNames.size());
      
      assertTrue(fileNames.contains("different.cheese"));   
      
      sf1.close();
      sf2.close();
   }
   
   
   public void testFill() throws Exception
   {
      SequentialFile sf = factory.createSequentialFile("fill.jbm", 1, 120);
      
      sf.open();
      
      try
      {
      
         checkFill(sf, 0, 2048, (byte)'X');
         
         checkFill(sf, 512, 512, (byte)'Y');
         
         checkFill(sf, 0, 1, (byte)'Z');
         
         checkFill(sf, 512, 1, (byte)'A');
         
         checkFill(sf, 1024, 512*4, (byte)'B');
      }
      finally
      {
         sf.close();
      }
   }
   
   public void testDelete() throws Exception
   {
      SequentialFile sf = factory.createSequentialFile("delete-me.jbm", 1, 120);
      
      sf.open();
      
      SequentialFile sf2 = factory.createSequentialFile("delete-me2.jbm", 1, 120);
      
      sf2.open();
      
      List<String> fileNames = factory.listFiles("jbm");
      
      assertEquals(2, fileNames.size());
      
      assertTrue(fileNames.contains("delete-me.jbm"));
      
      assertTrue(fileNames.contains("delete-me2.jbm"));
      
      sf.delete();
      
      fileNames = factory.listFiles("jbm");
      
      assertEquals(1, fileNames.size());
      
      assertTrue(fileNames.contains("delete-me2.jbm"));
      
      sf2.close();
      
   }
   
   public void testWriteandRead() throws Exception
   {
      SequentialFile sf = factory.createSequentialFile("write.jbm", 1, 120);
      
      sf.open();
      
      String s1 = "aardvark";
      byte[] bytes1 = s1.getBytes("UTF-8");
      ByteBuffer bb1 = factory.wrapBuffer(bytes1);
      
      String s2 = "hippopotamus";
      byte[] bytes2 = s2.getBytes("UTF-8");
      ByteBuffer bb2 = factory.wrapBuffer(bytes2);
      
      String s3 = "echidna";
      byte[] bytes3 = s3.getBytes("UTF-8");
      ByteBuffer bb3 = factory.wrapBuffer(bytes3);
      
      int bytesWritten = sf.write(bb1, true);
      
      assertEquals(calculateRecordSize(bytes1.length, sf.getAlignment()), bytesWritten);
      
      bytesWritten = sf.write(bb2, true);
      
      assertEquals(calculateRecordSize(bytes2.length, sf.getAlignment()), bytesWritten);
      
      bytesWritten = sf.write(bb3, true);
      
      assertEquals(calculateRecordSize(bytes3.length, sf.getAlignment()), bytesWritten);
      
      sf.position(0);
      
      ByteBuffer rb1 = factory.newBuffer(bytes1.length);
      ByteBuffer rb2 = factory.newBuffer(bytes2.length);
      ByteBuffer rb3 = factory.newBuffer(bytes3.length);

      int bytesRead = sf.read(rb1);
      assertEquals(calculateRecordSize(bytes1.length, sf.getAlignment()), bytesRead);     

      for (int i=0; i<bytes1.length; i++)
      {
      	assertEquals(bytes1[i], rb1.get(i));
      }
      
      bytesRead = sf.read(rb2);
      assertEquals(calculateRecordSize(bytes2.length, sf.getAlignment()), bytesRead);     
      for (int i=0; i<bytes2.length; i++)
      {
      	assertEquals(bytes2[i], rb2.get(i));
      }
      
      
      bytesRead = sf.read(rb3);
      assertEquals(calculateRecordSize(bytes3.length, sf.getAlignment()), bytesRead);     
      for (int i=0; i<bytes3.length; i++)
      {
      	assertEquals(bytes3[i], rb3.get(i));
      }
      
      sf.close();
      
   }
   
   public void testPosition() throws Exception
   {
      SequentialFile sf = factory.createSequentialFile("position.jbm", 1, 120);
      
      sf.open();
      
      String s1 = "orange";
      byte[] bytes1 = s1.getBytes("UTF-8");
      ByteBuffer bb1 = factory.wrapBuffer(bytes1); 
      
      String s2 = "grapefruit";
      byte[] bytes2 = s1.getBytes("UTF-8");
      ByteBuffer bb2 = factory.wrapBuffer(bytes2);
      
      String s3 = "lemon";
      byte[] bytes3 = s3.getBytes("UTF-8");
      ByteBuffer bb3 = factory.wrapBuffer(bytes3);
      
      int bytesWritten = sf.write(bb1, true);
      
      assertEquals(bb1.limit(), bytesWritten);
      
      bytesWritten = sf.write(bb2, true);
      
      assertEquals(bb2.limit(), bytesWritten);
      
      bytesWritten = sf.write(bb3, true);
      
      assertEquals(bb3.limit(), bytesWritten);
      
      byte[] rbytes1 = new byte[bytes1.length];
      
      byte[] rbytes2 = new byte[bytes2.length];
      
      byte[] rbytes3 = new byte[bytes3.length];
      
      ByteBuffer rb1 = factory.newBuffer(rbytes1.length);
      ByteBuffer rb2 = factory.newBuffer(rbytes2.length);
      ByteBuffer rb3 = factory.newBuffer(rbytes3.length);
      
      sf.position(bb1.limit() + bb2.limit());
      
      int bytesRead = sf.read(rb3);
      assertEquals(rb3.limit(), bytesRead);
      rb3.rewind();
      rb3.get(rbytes3);
      assertEqualsByteArrays(bytes3, rbytes3);    
      
      sf.position(rb1.limit());
      
      bytesRead = sf.read(rb2);
      assertEquals(rb2.limit(), bytesRead);
      rb2.get(rbytes2);
      assertEqualsByteArrays(bytes2, rbytes2);
      
      sf.position(0);
      
      bytesRead = sf.read(rb1);
      assertEquals(rb1.limit(), bytesRead);
      rb1.get(rbytes1);
      
      assertEqualsByteArrays(bytes1, rbytes1);
      
      sf.close();
   }
    
   public void testOpenClose() throws Exception
   {
      SequentialFile sf = factory.createSequentialFile("openclose.jbm", 1, 120);
      
      sf.open();
      
      String s1 = "cheesecake";
      byte[] bytes1 = s1.getBytes("UTF-8");
      ByteBuffer bb1 = factory.wrapBuffer(bytes1);
      
      int bytesWritten = sf.write(bb1, true);
      
      assertEquals(bb1.limit(), bytesWritten);
      
      sf.close();
      
      try
      {
         sf.write(bb1, true);
         
         fail("Should throw exception");
      }
      catch (Exception e)
      {
         //OK
      }
      
      sf.open();
      
      sf.write(bb1, true);
      
      sf.close();
   }
   
   // Private ---------------------------------
   
   protected void checkFill(SequentialFile file, int pos, int size, byte fillChar) throws Exception
   {
      file.fill(pos, size, fillChar);
      
      file.close();
      
      file.open();
      
      file.position(pos);
      
      ByteBuffer bb = factory.newBuffer(size);
      
      int bytesRead = file.read(bb);
      
      assertEquals(calculateRecordSize(size, file.getAlignment()), bytesRead);
      
      for (int i = 0; i < size; i++)
      {
         //log.info(" i is " + i);
         assertEquals(fillChar, bb.get(i));
      }
            
   }
   
}
