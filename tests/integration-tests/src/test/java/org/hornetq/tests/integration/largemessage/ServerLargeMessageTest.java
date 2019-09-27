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

package org.hornetq.tests.integration.largemessage;

import org.hornetq.api.core.Message;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.journal.impl.AbstractSequentialFile;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager;
import org.hornetq.core.persistence.impl.journal.LargeServerMessageImpl;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.tests.unit.core.journal.impl.fakes.FakeSequentialFileFactory;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.UnitTestCase;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

/**
 * A ServerLargeMessageTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class ServerLargeMessageTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // The ClientConsumer should be able to also send ServerLargeMessages as that's done by the CoreBridge
   @Test
   public void testSendServerMessage() throws Exception
   {
      HornetQServer server = createServer(true);

      server.start();

      ServerLocator locator = createFactory(false);

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, false);

      try
      {
         LargeServerMessageImpl fileMessage = new LargeServerMessageImpl((JournalStorageManager)server.getStorageManager());

         fileMessage.setMessageID(1005);

         for (int i = 0; i < 2 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE; i++)
         {
            fileMessage.addBytes(new byte[]{UnitTestCase.getSamplebyte(i)});
         }
         // The server would be doing this
         fileMessage.putLongProperty(Message.HDR_LARGE_BODY_SIZE, 2 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);


         fileMessage.releaseResources();

         session.createQueue("A", "A");

         ClientProducer prod = session.createProducer("A");

         prod.send(fileMessage);

         fileMessage.deleteFile();

         session.commit();

         session.start();

         ClientConsumer cons = session.createConsumer("A");

         ClientMessage msg = cons.receive(5000);

         Assert.assertNotNull(msg);

         Assert.assertEquals(msg.getBodySize(), 2 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

         for (int i = 0; i < 2 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE; i++)
         {
            Assert.assertEquals(UnitTestCase.getSamplebyte(i), msg.getBodyBuffer().readByte());
         }

         msg.acknowledge();

         session.commit();

      }
      finally
      {
         sf.close();
         locator.close();
         server.stop();
      }
   }

   @Test
   public void testLargeServerMessageCopyIsolation() throws Exception
   {
      HornetQServer server = createServer(true);

      server.start();

      try
      {
         LargeServerMessageImpl largeMessage = new LargeServerMessageImpl((JournalStorageManager)server.getStorageManager());

         largeMessage.setMessageID(23456);

         for (int i = 0; i < 2 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE; i++)
         {
            largeMessage.addBytes(new byte[]{UnitTestCase.getSamplebyte(i)});
         }

         //now replace the underlying file with a fake
         replaceFile(largeMessage);

         ServerMessage copied = largeMessage.copy(99999);

         assertEquals(99999, copied.getMessageID());
      }
      finally
      {
         server.stop();
      }

   }

   private void replaceFile(LargeServerMessageImpl largeMessage) throws Exception
   {
      SequentialFile originalFile = largeMessage.getFile();
      MockSequentialFile mockFile = new MockSequentialFile(originalFile);

      Field fileField = LargeServerMessageImpl.class.getDeclaredField("file");
      fileField.setAccessible(true);
      fileField.set(largeMessage, mockFile);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private class MockSequentialFile extends AbstractSequentialFile
   {
      private SequentialFile originalFile;

      public MockSequentialFile(SequentialFile originalFile)
      {
         super(originalFile.getJavaFile().getParent(), originalFile.getJavaFile(), new FakeSequentialFileFactory(), null);
         this.originalFile = originalFile;
      }

      @Override
      public void open() throws Exception
      {
         //open and close it right away to simulate failure condition
         originalFile.open();
         originalFile.close();
      }

      @Override
      public boolean isOpen()
      {
         return originalFile.isOpen();
      }

      @Override
      public int getAlignment() throws Exception
      {
         return originalFile.getAlignment();
      }

      @Override
      public int calculateBlockStart(int position) throws Exception
      {
         return originalFile.calculateBlockStart(position);
      }

      @Override
      public void fill(int position, int size, byte fillCharacter) throws Exception
      {
         originalFile.fill(position, size, fillCharacter);
      }

      @Override
      public void writeDirect(ByteBuffer bytes, boolean sync, IOAsyncTask callback)
      {
         originalFile.writeDirect(bytes, sync, callback);
      }

      @Override
      public void writeDirect(ByteBuffer bytes, boolean sync) throws Exception
      {
         originalFile.writeDirect(bytes, sync);
      }

      @Override
      public void writeInternal(ByteBuffer bytes) throws Exception
      {
         originalFile.writeInternal(bytes);
      }

      @Override
      public int read(ByteBuffer bytes, IOAsyncTask callback) throws Exception
      {
         return originalFile.read(bytes, callback);
      }

      @Override
      public int read(ByteBuffer bytes) throws Exception
      {
         return originalFile.read(bytes);
      }

      @Override
      public void waitForClose() throws Exception
      {
         originalFile.waitForClose();
      }

      @Override
      public void sync() throws IOException
      {
         originalFile.sync();
      }

      @Override
      public long size() throws Exception
      {
         return originalFile.size();
      }

      @Override
      public SequentialFile cloneFile()
      {
         return originalFile.cloneFile();
      }
   }
}
