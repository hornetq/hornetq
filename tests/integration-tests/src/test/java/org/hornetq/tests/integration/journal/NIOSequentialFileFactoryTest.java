/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.tests.integration.journal;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.core.journal.IOCriticalErrorListener;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.journal.impl.NIOSequentialFileFactory;
import org.hornetq.tests.unit.core.journal.impl.SequentialFileFactoryTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A NIOSequentialFileFactoryTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class NIOSequentialFileFactoryTest extends SequentialFileFactoryTestBase
{

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      File file = new File(getTestDir());

      deleteDirectory(file);

      file.mkdir();
   }

   @Override
   protected SequentialFileFactory createFactory()
   {
      return new NIOSequentialFileFactory(getTestDir(), true);
   }


   @Test
   public void testInterrupts() throws Throwable
   {

      final EncodingSupport fakeEncoding = new EncodingSupport()
      {
         @Override
         public int getEncodeSize()
         {
            return 10;
         }

         @Override
         public void encode(HornetQBuffer buffer)
         {
            buffer.writeBytes(new byte[10]);
         }

         @Override
         public void decode(HornetQBuffer buffer)
         {

         }
      };

      final AtomicInteger calls = new AtomicInteger(0);
      final NIOSequentialFileFactory factory = new NIOSequentialFileFactory(getTestDir(), true, new IOCriticalErrorListener()
      {
         @Override
         public void onIOException(Throwable code, String message, SequentialFile file)
         {
            new Exception("shutdown").printStackTrace();
            calls.incrementAndGet();
         }
      });

      Thread threadOpen = new Thread()
      {
         public void run()
         {
            try
            {
               Thread.currentThread().interrupt();
               SequentialFile file = factory.createSequentialFile("file.txt", 1);
               file.open();
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }
      };

      threadOpen.start();
      threadOpen.join();

      Thread threadClose = new Thread()
      {
         public void run()
         {
            try
            {
               SequentialFile file = factory.createSequentialFile("file.txt", 1);
               file.open();
               file.write(fakeEncoding, true);
               Thread.currentThread().interrupt();
               file.close();
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }
      };

      threadClose.start();
      threadClose.join();

      Thread threadWrite = new Thread()
      {
         public void run()
         {
            try
            {
               SequentialFile file = factory.createSequentialFile("file.txt", 1);
               file.open();
               Thread.currentThread().interrupt();
               file.write(fakeEncoding, true);
               file.close();

            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }
      };

      threadWrite.start();
      threadWrite.join();

      Thread threadFill = new Thread()
      {
         public void run()
         {
            try
            {
               SequentialFile file = factory.createSequentialFile("file.txt", 1);
               file.open();
               Thread.currentThread().interrupt();
               file.fill(0, 1024, (byte) 0);
               file.close();

            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }
      };

      threadFill.start();
      threadFill.join();

      Thread threadWriteDirect = new Thread()
      {
         public void run()
         {
            try
            {
               SequentialFile file = factory.createSequentialFile("file.txt", 1);
               file.open();
               ByteBuffer buffer = ByteBuffer.allocate(10);
               buffer.put(new byte[10]);
               Thread.currentThread().interrupt();
               file.writeDirect(buffer, true);
               file.close();

            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }
      };

      threadWriteDirect.start();
      threadWriteDirect.join();

      Thread threadRead = new Thread()
      {
         public void run()
         {
            try
            {
               SequentialFile file = factory.createSequentialFile("file.txt", 1);
               file.open();
               file.write(fakeEncoding, true);
               file.position(0);
               ByteBuffer readBytes = ByteBuffer.allocate(fakeEncoding.getEncodeSize());
               Thread.currentThread().interrupt();
               file.read(readBytes);
               file.close();

            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }
      };

      threadRead.start();
      threadRead.join();

      // An interrupt exception shouldn't issue a shutdown
      Assert.assertEquals(0, calls.get());
   }
}
