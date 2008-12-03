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

package org.jboss.messaging.tests.integration.journal;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.messaging.core.asyncio.impl.AsynchronousFileImpl;
import org.jboss.messaging.core.journal.IOCallback;
import org.jboss.messaging.core.journal.SequentialFile;
import org.jboss.messaging.core.journal.SequentialFileFactory;
import org.jboss.messaging.core.journal.impl.AIOSequentialFileFactory;
import org.jboss.messaging.tests.unit.core.journal.impl.SequentialFileFactoryTestBase;

/**
 * 
 * A AIOSequentialFileFactoryTest
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class AIOSequentialFileFactoryTest extends SequentialFileFactoryTestBase
{

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      if (!AsynchronousFileImpl.isLoaded())
      {
         fail(String.format("libAIO is not loaded on %s %s %s",
                            System.getProperty("os.name"),
                            System.getProperty("os.arch"),
                            System.getProperty("os.version")));
      }

      File file = new File(getTestDir());

      deleteDirectory(file);

      file.mkdirs();
   }

   @Override
   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   @Override
   protected SequentialFileFactory createFactory()
   {
      return new AIOSequentialFileFactory(getTestDir());
   }

   public void testBuffer() throws Exception
   {
      SequentialFile file = factory.createSequentialFile("filtetmp.log", 10);
      file.open();
      ByteBuffer buff = factory.newBuffer(10);
      assertEquals(512, buff.limit());
      file.close();
   }

   public void testBlockCallback() throws Exception
   {
      class BlockCallback implements IOCallback
      {
         AtomicInteger countDone = new AtomicInteger(0);

         AtomicInteger countError = new AtomicInteger(0);

         CountDownLatch blockLatch;

         BlockCallback()
         {
            blockLatch = new CountDownLatch(1);
         }

         public void release()
         {
            blockLatch.countDown();
         }

         public void done()
         {
            try
            {
               blockLatch.await();
            }
            catch (InterruptedException e)
            {
               e.printStackTrace();
            }

            countDone.incrementAndGet();
         }

         public void onError(final int errorCode, final String errorMessage)
         {
            try
            {
               blockLatch.await();
            }
            catch (InterruptedException e)
            {
               e.printStackTrace();
            }

            countError.incrementAndGet();
         }
      }

      BlockCallback callback = new BlockCallback();

      final int NUMBER_OF_RECORDS = 10000;

      SequentialFile file = factory.createSequentialFile("callbackBlock.log", 1000);
      file.open();
      file.fill(0, 512 * NUMBER_OF_RECORDS, (byte)'a');

      for (int i = 0; i < NUMBER_OF_RECORDS; i++)
      {
         ByteBuffer buffer = factory.newBuffer(512);

         buffer.putInt(i + 10);

         for (int j = buffer.position(); j < buffer.limit(); j++)
         {
            buffer.put((byte)'b');
         }

         file.write(buffer, callback);
      }

      callback.release();
      file.close();
      assertEquals(NUMBER_OF_RECORDS, callback.countDone.get());
      assertEquals(0, callback.countError.get());

      file.open();

      ByteBuffer buffer = factory.newBuffer(512);

      for (int i = 0; i < NUMBER_OF_RECORDS; i++)
      {

         file.read(buffer);
         buffer.rewind();

         int recordRead = buffer.getInt();

         assertEquals(i + 10, recordRead);

         for (int j = buffer.position(); j < buffer.limit(); j++)
         {
            assertEquals((byte)'b', buffer.get());
         }

      }

      file.close();
   }

}
