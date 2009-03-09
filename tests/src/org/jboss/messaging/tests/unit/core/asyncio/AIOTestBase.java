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

package org.jboss.messaging.tests.unit.core.asyncio;

import org.jboss.messaging.core.asyncio.AIOCallback;
import org.jboss.messaging.core.asyncio.impl.AsynchronousFileImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.tests.util.UnitTestCase;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The base class for AIO Tests
 * @author Clebert Suconic
 *
 */
public abstract class AIOTestBase extends UnitTestCase
{
   // The AIO Test must use a local filesystem. Sometimes $HOME is on a NFS on
   // most enterprise systems

   protected String FILE_NAME = getTestDir() + "/fileUsedOnNativeTests.log";

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      File file = new File(getTestDir());

      deleteDirectory(file);

      file.mkdir();

      if (!AsynchronousFileImpl.isLoaded())
      {
         fail(String.format("libAIO is not loaded on %s %s %s",
                            System.getProperty("os.name"),
                            System.getProperty("os.arch"),
                            System.getProperty("os.version")));
      }
   }

   @Override
   protected void tearDown() throws Exception
   {
      assertEquals(0, AsynchronousFileImpl.getTotalMaxIO());

      super.tearDown();
   }

   protected void encodeBufer(final ByteBuffer buffer)
   {
      buffer.clear();
      int size = buffer.limit();
      for (int i = 0; i < size - 1; i++)
      {
         buffer.put((byte)('a' + i % 20));
      }

      buffer.put((byte)'\n');

   }

   protected void preAlloc(final AsynchronousFileImpl controller, final long size) throws MessagingException
   {
      controller.fill(0l, 1, size, (byte)0);
   }

   protected static class CountDownCallback implements AIOCallback
   {
      private final CountDownLatch latch;

      public CountDownCallback(final CountDownLatch latch)
      {
         this.latch = latch;
      }

      volatile boolean doneCalled = false;

      volatile boolean errorCalled = false;

      final AtomicInteger timesDoneCalled = new AtomicInteger(0);

      public void done()
      {
         doneCalled = true;
         timesDoneCalled.incrementAndGet();
         if (latch != null)
         {
            latch.countDown();
         }
      }

      public void onError(final int errorCode, final String errorMessage)
      {
         errorCalled = true;
         if (latch != null)
         {
            // even thought an error happened, we need to inform the latch,
               // or the test won't finish
            latch.countDown();
         }
      }
   }

}
