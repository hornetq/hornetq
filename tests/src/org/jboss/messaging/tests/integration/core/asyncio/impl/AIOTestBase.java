/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005, JBoss Inc., and individual contributors as indicated
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

package org.jboss.messaging.tests.integration.core.asyncio.impl;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

import org.jboss.messaging.core.asyncio.AIOCallback;
import org.jboss.messaging.core.asyncio.impl.AsynchronousFileImpl;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * The base class for AIO Tests
 * @author Clebert Suconic
 *
 */
public abstract class AIOTestBase extends UnitTestCase
{
   protected String fileDir = System.getProperty("user.home") + "/journal-test";
   protected String FILE_NAME = fileDir + "/fileUsedOnNativeTests.log";
   
   protected void setUp() throws Exception
   {
      super.setUp();
      
      File file = new File(fileDir);
      
      deleteDirectory(file);
      
      file.mkdir();
      
      if (!AsynchronousFileImpl.isLoaded())
      {
         fail(String.format("libAIO is not loaded on %s %s %s", System
               .getProperty("os.name"), System.getProperty("os.arch"), System
               .getProperty("os.version")));
      }
   }
   
   protected void tearDown() throws Exception
   {
      super.tearDown();
      assertEquals(0, AsynchronousFileImpl.getTotalMaxIO());
   }
   
   protected void encodeBufer(ByteBuffer buffer)
   {
      buffer.clear();
      int size = buffer.limit();
      for (int i = 0; i < size - 1; i++)
      {
         buffer.put((byte) ('a' + (i % 20)));
      }
      
      buffer.put((byte) '\n');
      
   }
   
   protected void preAlloc(AsynchronousFileImpl controller, long size)
   {
      System.out.println("Pre allocating");
      System.out.flush();
      long startPreAllocate = System.currentTimeMillis();
      controller.fill(0l, 1, size, (byte) 0);
      long endPreAllocate = System.currentTimeMillis() - startPreAllocate;
      if (endPreAllocate != 0)
      {
         System.out.println("PreAllocated the file (size = " + size
               + " bytes) in " + endPreAllocate + " Milliseconds, What means "
               + (size / endPreAllocate) + " bytes per millisecond");
      }
   }

   protected static class CountDownCallback implements AIOCallback
   {

       CountDownLatch latch;
       
       public CountDownCallback(CountDownLatch latch)
       {
           this.latch = latch;
       }
       
       boolean doneCalled = false;
       boolean errorCalled = false;
       int timesDoneCalled = 0;

       public void done()
       {
           doneCalled = true;
           timesDoneCalled++;
           if (latch != null) 
           {
               latch.countDown();
           }
       }

       public void onError(int errorCode, String errorMessage)
       {
           errorCalled = true;
           if (latch != null)
           {
               // even thought an error happened, we need to inform the latch, or the test won't finish
               latch.countDown();
           }
           System.out.println("Received an Error - " + errorCode + " message=" + errorMessage);
           
       }

   }

   
}
