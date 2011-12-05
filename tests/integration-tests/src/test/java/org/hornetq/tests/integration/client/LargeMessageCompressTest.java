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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import junit.framework.Assert;

import org.hornetq.api.core.Message;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.UnitTestCase;

/**
 * A LargeMessageCompressTest
 *
 * Just extend the LargeMessageTest
 *
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 *
 *
 */
public class LargeMessageCompressTest extends LargeMessageTest
{
   // Constructors --------------------------------------------------

   @Override
   protected boolean isNetty()
   {
      return false;
   }

   @Override
   protected ServerLocator createFactory(final boolean isNetty) throws Exception
   {
      ServerLocator locator = super.createFactory(isNetty);
      locator.setCompressLargeMessage(true);
      return locator;
   }

   public void testLargeMessageCompression() throws Exception
   {
      final int messageSize = (int)(3.5 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

      ClientSession session = null;

      try
      {
         HornetQServer server = createServer(true, isNetty());

         server.start();

         ClientSessionFactory sf = locator.createSessionFactory();

         session = sf.createSession(false, false, false);

         session.createTemporaryQueue(LargeMessageTest.ADDRESS, LargeMessageTest.ADDRESS);

         ClientProducer producer = session.createProducer(LargeMessageTest.ADDRESS);

         Message clientFile = createLargeClientMessage(session, messageSize, true);

         producer.send(clientFile);

         session.commit();

         session.start();

         ClientConsumer consumer = session.createConsumer(LargeMessageTest.ADDRESS);
         ClientMessage msg1 = consumer.receive(1000);
         Assert.assertNotNull(msg1);

         for (int i = 0 ; i < messageSize; i++)
         {
            byte b = msg1.getBodyBuffer().readByte();
            assertEquals("position = "  + i, getSamplebyte(i), b);
         }

         msg1.acknowledge();
         session.commit();

         consumer.close();

         session.close();

         validateNoFilesOnLargeDir();
      }
      finally
      {
         try
         {
            session.close();
         }
         catch (Throwable ignored)
         {
         }
      }
   }

   public void testLargeMessageCompression2() throws Exception
   {
      final int messageSize = (int)(3.5 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

      ClientSession session = null;

      try
      {
         HornetQServer server = createServer(true, isNetty());

         server.start();

         ClientSessionFactory sf = locator.createSessionFactory();

         session = sf.createSession(false, false, false);

         session.createTemporaryQueue(LargeMessageTest.ADDRESS, LargeMessageTest.ADDRESS);

         ClientProducer producer = session.createProducer(LargeMessageTest.ADDRESS);

         Message clientFile = createLargeClientMessage(session, messageSize, true);

         producer.send(clientFile);

         session.commit();

         session.start();

         ClientConsumer consumer = session.createConsumer(LargeMessageTest.ADDRESS);
         ClientMessage msg1 = consumer.receive(1000);
         Assert.assertNotNull(msg1);

         String testDir = UnitTestCase.getTestDir();
         File testFile = new File(testDir, "async_large_message");
         FileOutputStream output = new FileOutputStream(testFile);

         msg1.setOutputStream(output);

         msg1.waitOutputStreamCompletion(0);

         msg1.acknowledge();

         session.commit();

         consumer.close();

         session.close();

         //verify
         FileInputStream input = new FileInputStream(testFile);
         for (int i = 0 ; i < messageSize; i++)
         {
            byte b = (byte)input.read();
            assertEquals("position = "  + i, getSamplebyte(i), b);
         }

         testFile.delete();
         validateNoFilesOnLargeDir();
      }
      finally
      {
         try
         {
            session.close();
         }
         catch (Throwable ignored)
         {
         }
      }
   }

   public void testLargeMessageCompression3() throws Exception
   {
      final int messageSize = (int)(3.5 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

      ClientSession session = null;

      try
      {
         HornetQServer server = createServer(true, isNetty());

         server.start();

         ClientSessionFactory sf = locator.createSessionFactory();

         session = sf.createSession(false, false, false);

         session.createTemporaryQueue(LargeMessageTest.ADDRESS, LargeMessageTest.ADDRESS);

         ClientProducer producer = session.createProducer(LargeMessageTest.ADDRESS);

         Message clientFile = createLargeClientMessage(session, messageSize, true);

         producer.send(clientFile);

         session.commit();

         session.start();

         ClientConsumer consumer = session.createConsumer(LargeMessageTest.ADDRESS);
         ClientMessage msg1 = consumer.receive(1000);
         Assert.assertNotNull(msg1);

         String testDir = UnitTestCase.getTestDir();
         File testFile = new File(testDir, "async_large_message");
         FileOutputStream output = new FileOutputStream(testFile);

         msg1.saveToOutputStream(output);

         msg1.acknowledge();

         session.commit();

         consumer.close();

         session.close();

         //verify
         FileInputStream input = new FileInputStream(testFile);
         for (int i = 0 ; i < messageSize; i++)
         {
            byte b = (byte)input.read();
            assertEquals("position = "  + i, getSamplebyte(i), b);
         }

         testFile.delete();
         validateNoFilesOnLargeDir();
      }
      finally
      {
         try
         {
            session.close();
         }
         catch (Throwable ignored)
         {
         }
      }
   }



   public void testLargeMessageCompressionRestartAndCheckSize() throws Exception
   {
      final int messageSize = 1024 * 1024;

      ClientSession session = null;

      try
      {
         HornetQServer server = createServer(true, isNetty());

         server.start();

         ClientSessionFactory sf = locator.createSessionFactory();

         session = sf.createSession(false, false, false);

         session.createQueue(LargeMessageTest.ADDRESS, LargeMessageTest.ADDRESS, true);

         ClientProducer producer = session.createProducer(LargeMessageTest.ADDRESS);

         byte [] msgs = new byte[1024 * 1024];
         for (int i = 0 ; i < msgs.length; i++)
         {
            msgs[i] = RandomUtil.randomByte();
         }

         Message clientFile = createLargeClientMessage(session, msgs, true);

         producer.send(clientFile);

         session.commit();

         session.close();

         sf.close();

         locator.close();

         server.stop();

         server = createServer(true, isNetty());

         server.start();

         locator = createFactory(isNetty());

         sf = locator.createSessionFactory();

         session = sf.createSession();

         session.start();

         ClientConsumer consumer = session.createConsumer(LargeMessageTest.ADDRESS);
         ClientMessage msg1 = consumer.receive(1000);
         Assert.assertNotNull(msg1);

         assertEquals(messageSize, msg1.getBodySize());

         String testDir = UnitTestCase.getTestDir();
         File testFile = new File(testDir, "async_large_message");
         FileOutputStream output = new FileOutputStream(testFile);

         msg1.saveToOutputStream(output);

         msg1.acknowledge();

         session.commit();

         consumer.close();

         session.close();

         //verify
         FileInputStream input = new FileInputStream(testFile);
         for (int i = 0 ; i < messageSize; i++)
         {
            byte b = (byte)input.read();
            assertEquals("position = "  + i, msgs[i], b);
         }

         testFile.delete();
         validateNoFilesOnLargeDir();
      }
      finally
      {
         try
         {
            session.close();
         }
         catch (Throwable ignored)
         {
         }
      }
   }


   @Override
   public void testSendServerMessage() throws Exception
   {
      // doesn't make sense as compressed
   }
}
