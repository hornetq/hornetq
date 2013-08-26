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
package org.hornetq.tests.integration.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicLong;

import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.DeflaterReader;
import org.junit.Assert;
import org.junit.Test;

/**
 * The test extends the LargeMessageTest and tests
 * the functionality of option avoid-large-messages
 *
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 */
public class LargeMessageAvoidLargeMessagesTest extends LargeMessageTest
{

   public LargeMessageAvoidLargeMessagesTest()
   {
      isCompressedTest = true;
   }

   @Override
   protected boolean isNetty()
   {
      return false;
   }

   @Override
   protected ServerLocator createFactory(final boolean isNetty) throws Exception
   {
      ServerLocator locator1 = super.createFactory(isNetty);
      locator1.setMinLargeMessageSize(10240);
      locator1.setCompressLargeMessage(true);
      return locator1;
   }

   //send some messages that can be compressed into regular size.
   @Test
   public void testSendRegularAfterCompression() throws Exception
   {
      HornetQServer server = createServer(true, isNetty());
      server.start();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = addClientSession(sf.createSession(false, false, false));

      session.createTemporaryQueue(ADDRESS, ADDRESS);

      ClientProducer producer = session.createProducer(ADDRESS);

      int minLargeSize = locator.getMinLargeMessageSize();

      TestLargeMessageInputStream input = new TestLargeMessageInputStream(minLargeSize);
      adjustLargeCompression(true, input, 1024);

      int num = 20;
      for (int i = 0; i < num; i++)
      {
         ClientMessage clientFile = session.createMessage(true);
         clientFile.setBodyInputStream(input.clone());

         producer.send(clientFile);
      }

      session.commit();

      session.start();

      //no file should be in the dir as we send it as regular
      validateNoFilesOnLargeDir();

      ClientConsumer consumer = session.createConsumer(ADDRESS);
      for (int j = 0; j < num; j++)
      {
         ClientMessage msg1 = consumer.receive(1000);
         Assert.assertNotNull(msg1);

         for (int i = 0 ; i < input.getSize(); i++)
         {
            byte b = msg1.getBodyBuffer().readByte();
            assertEquals("incorrect char ", input.getChar(i), b);
         }
         msg1.acknowledge();
      }

      session.commit();
      consumer.close();

      session.close();
   }

   //send some messages that cannot be compressed into regular messages
   @Test
   public void testSendLargeAfterUnableToSendRegular() throws Exception
   {
      HornetQServer server = createServer(true, isNetty());
      server.start();

      //reduce the minLargeMessageSize to make the test faster
      locator.setMinLargeMessageSize(5*1024);
      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = addClientSession(sf.createSession(false, false, false));

      session.createTemporaryQueue(ADDRESS, ADDRESS);

      ClientProducer producer = session.createProducer(ADDRESS);

      int minLargeSize = locator.getMinLargeMessageSize();
      TestLargeMessageInputStream input = new TestLargeMessageInputStream(minLargeSize);
      input.setSize(80 * minLargeSize);
      adjustLargeCompression(false, input, 40 * minLargeSize);

      int num = 10;
      for (int i = 0; i < num; i++)
      {
         ClientMessage clientFile = session.createMessage(true);
         clientFile.setBodyInputStream(input.clone());

         producer.send(clientFile);
      }

      session.commit();

      session.start();

      //no file should be in the dir as we send it as regular
      validateNoFilesOnLargeDir(num);

      ClientConsumer consumer = session.createConsumer(ADDRESS);
      for (int j = 0; j < num; j++)
      {
         ClientMessage msg1 = consumer.receive(1000);
         Assert.assertNotNull(msg1);

         for (int i = 0 ; i < input.getSize(); i++)
         {
            byte b = msg1.getBodyBuffer().readByte();
            assertEquals("incorrect char", input.getChar(i), b);
         }
         msg1.acknowledge();
      }

      session.commit();
      consumer.close();

      session.close();
   }

   @Test
   public void testMixedCompressionSendReceive() throws Exception
   {
      HornetQServer server = createServer(true, isNetty());
      server.start();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = addClientSession(sf.createSession(false, false, false));

      session.createTemporaryQueue(ADDRESS, ADDRESS);

      ClientProducer producer = session.createProducer(ADDRESS);

      final int minLargeSize = locator.getMinLargeMessageSize();
      TestLargeMessageInputStream regularInput = new TestLargeMessageInputStream(minLargeSize);
      adjustLargeCompression(true, regularInput, 1024);

      TestLargeMessageInputStream largeInput = new TestLargeMessageInputStream(minLargeSize);
      largeInput.setSize(100 * minLargeSize);
      adjustLargeCompression(false, largeInput, 50 * minLargeSize);

      int num = 6;
      for (int i = 0; i < num; i++)
      {
         ClientMessage clientFile = session.createMessage(true);
         if (i%2 == 0)
         {
            clientFile.setBodyInputStream(regularInput.clone());
         }
         else
         {
            clientFile.setBodyInputStream(largeInput.clone());
         }

         producer.send(clientFile);
      }

      session.commit();

      session.start();

      //half the messages are sent as large
      validateNoFilesOnLargeDir(num/2);

      ClientConsumer consumer = session.createConsumer(ADDRESS);
      for (int j = 0; j < num; j++)
      {
         ClientMessage msg1 = consumer.receive(1000);
         Assert.assertNotNull(msg1);

         if (j%2 == 0)
         {
            for (int i = 0 ; i < regularInput.getSize(); i++)
            {
               byte b = msg1.getBodyBuffer().readByte();
               assertEquals("incorrect char ", regularInput.getChar(i), b);
            }
         }
         else
         {
            for (int i = 0; i < largeInput.getSize(); i++)
            {
               byte b = msg1.getBodyBuffer().readByte();
               assertEquals("incorrect char ", largeInput.getChar(i), b);
            }
         }
         msg1.acknowledge();
      }

      session.commit();
      consumer.close();

      session.close();
   }

   private void adjustLargeCompression(boolean regular, TestLargeMessageInputStream stream, int step) throws IOException
   {
      int absoluteStep = Math.abs(step);
      while (true)
      {
         DeflaterReader compressor = new DeflaterReader(stream, new AtomicLong());
         try
         {
            byte[] buffer = new byte[1048 * 50];

            int totalCompressed = 0;
            int n = compressor.read(buffer);
            while (n != -1)
            {
               totalCompressed += n;
               n = compressor.read(buffer);
            }

            // check compressed size
            if (regular && (totalCompressed < stream.getMinLarge()))
            {
               // ok it can be sent as regular
               stream.resetAdjust(0);
               break;
            }
            else if ((!regular) && (totalCompressed > stream.getMinLarge()))
            {
               // now it cannot be sent as regular
               stream.resetAdjust(0);
               break;
            }
            else
            {
               stream.resetAdjust(regular ? -absoluteStep : absoluteStep);
            }
         }
         finally
         {
            compressor.close();
         }
      }
   }

   private static class TestLargeMessageInputStream extends InputStream
   {
      private final int minLarge;
      private int size;
      private int pos;

      public TestLargeMessageInputStream(int minLarge)
      {
         pos = 0;
         this.minLarge = minLarge;
         this.size = minLarge + 1024;
      }

      public int getChar(int index)
      {
         return 'A' + index % 26;
      }

      public void setSize(int size)
      {
         this.size = size;
      }

      public TestLargeMessageInputStream(TestLargeMessageInputStream other)
      {
         this.minLarge = other.minLarge;
         this.size = other.size;
         this.pos = other.pos;
      }

      public int getSize()
      {
         return size;
      }

      public int getMinLarge()
      {
         return this.minLarge;
      }

      @Override
      public int read() throws IOException
      {
         if (pos == size) return -1;
         pos++;

         return getChar(pos - 1);
      }

      public void resetAdjust(int step)
      {
         size += step;
         if (size <= minLarge)
         {
            throw new IllegalStateException("Couldn't adjust anymore, size smaller than minLarge " + minLarge);
         }
         pos = 0;
      }

      @Override
      public TestLargeMessageInputStream clone()
      {
         return new TestLargeMessageInputStream(this);
      }
   }

   //this test won't leave any large messages in the large-messages dir
   //because after compression, the messages are regulars at server.
   @Override
   @Test
   public void testDLALargeMessage() throws Exception
   {
      final int messageSize = (int) (3.5 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

      ClientSession session = null;

      HornetQServer server = createServer(true, isNetty());

      server.start();

      ClientSessionFactory sf = addSessionFactory(createSessionFactory(locator));

      session = addClientSession(sf.createSession(false, false, false));

      session.createQueue(ADDRESS, ADDRESS,
            true);
      session.createQueue(ADDRESS, ADDRESS.concat("-2"), true);

      SimpleString ADDRESS_DLA = ADDRESS.concat("-dla");

      AddressSettings addressSettings = new AddressSettings();

      addressSettings.setDeadLetterAddress(ADDRESS_DLA);
      addressSettings.setMaxDeliveryAttempts(1);

      server.getAddressSettingsRepository().addMatch("*", addressSettings);

      session.createQueue(ADDRESS_DLA, ADDRESS_DLA, true);

      ClientProducer producer = session
.createProducer(ADDRESS);

      Message clientFile = createLargeClientMessage(session, messageSize, true);

      producer.send(clientFile);

      session.commit();

      session.start();

      ClientConsumer consumer = session.createConsumer(ADDRESS_DLA);

      ClientConsumer consumerRollback = session
.createConsumer(ADDRESS);
      ClientMessage msg1 = consumerRollback.receive(1000);
      Assert.assertNotNull(msg1);
      msg1.acknowledge();
      session.rollback();
      consumerRollback.close();

      msg1 = consumer.receive(10000);

      Assert.assertNotNull(msg1);

      for (int i = 0; i < messageSize; i++)
      {
         Assert.assertEquals(UnitTestCase.getSamplebyte(i), msg1
               .getBodyBuffer().readByte());
      }

      session.close();
      server.stop();

      server = createServer(true, isNetty());

      server.start();

      sf = createSessionFactory(locator);

      session = sf.createSession(false, false, false);

      session.start();

      consumer = session.createConsumer(ADDRESS_DLA);

      msg1 = consumer.receive(10000);

      Assert.assertNotNull(msg1);

      for (int i = 0; i < messageSize; i++)
      {
         Assert.assertEquals(UnitTestCase.getSamplebyte(i), msg1
               .getBodyBuffer().readByte());
      }

      msg1.acknowledge();

      session.commit();

      //large message becomes a regular at server.
      validateNoFilesOnLargeDir(0);

      consumer = session.createConsumer(ADDRESS.concat("-2"));

      msg1 = consumer.receive(10000);

      Assert.assertNotNull(msg1);

      for (int i = 0; i < messageSize; i++)
      {
         Assert.assertEquals(UnitTestCase.getSamplebyte(i), msg1
               .getBodyBuffer().readByte());
      }

      msg1.acknowledge();

      session.commit();

      session.close();

      validateNoFilesOnLargeDir();
   }

   @Override
   @Test
   public void testSendServerMessage() throws Exception
   {
      // doesn't make sense as compressed
   }

}
