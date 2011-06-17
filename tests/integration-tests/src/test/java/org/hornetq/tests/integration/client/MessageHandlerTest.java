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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.*;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.CreateMessage;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class MessageHandlerTest extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(MessageHandlerTest.class);

   private HornetQServer server;

   private final SimpleString QUEUE = new SimpleString("ConsumerTestQueue");

   private ServerLocator locator;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      server = createServer(false);

      server.start();

      locator = createInVMNonHALocator();
   }

   @Override
   protected void tearDown() throws Exception
   {
      locator.close();

      server.stop();

      server = null;

      super.tearDown();
   }

   public void testSetMessageHandlerWithMessagesPending() throws Exception
   {
      ClientSessionFactory sf = locator.createSessionFactory();

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = CreateMessage.createTextMessage("m" + i, session);
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE, null, true);

      session.start();

      Thread.sleep(100);

      // Message should be in consumer

      class MyHandler implements MessageHandler
      {
         public void onMessage(final ClientMessage message)
         {
            try
            {
               Thread.sleep(10);

               message.acknowledge();
            }
            catch (Exception e)
            {
            }
         }
      }

      consumer.setMessageHandler(new MyHandler());

      // Let a few messages get processed
      Thread.sleep(100);

      // Now set null

      consumer.setMessageHandler(null);

      // Give a bit of time for some queued executors to run

      Thread.sleep(500);

      // Make sure no exceptions were thrown from onMessage
      Assert.assertNull(consumer.getLastException());

      session.close();
   }

   public void testSetResetMessageHandler() throws Exception
   {
      ClientSessionFactory sf = locator.createSessionFactory();

      final ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = CreateMessage.createTextMessage("m" + i, session);
         
         message.putIntProperty(new SimpleString("i"), i);

         producer.send(message);
      }

      final ClientConsumer consumer = session.createConsumer(QUEUE);

      CountDownLatch latch = new CountDownLatch(50);

      // Message should be in consumer

      class MyHandler implements MessageHandler
      {
         int messageReceived = 0;

         boolean failed;

         boolean started = true;

         private final CountDownLatch latch;

         public MyHandler(final CountDownLatch latch)
         {
            this.latch = latch;
         }

         public void onMessage(final ClientMessage message)
         {

            try
            {
               if (!started)
               {
                  failed = true;
               }
               
               messageReceived++;
               
               log.info("got message " + messageReceived);
               
               latch.countDown();

               if (latch.getCount() == 0)
               {
                  message.acknowledge();
                  
                  started = false;
                  
                  consumer.setMessageHandler(null);
               }

            }
            catch (Exception e)
            {
            }
         }
      }

      MyHandler handler = new MyHandler(latch);

      consumer.setMessageHandler(handler);
      
      session.start();


      latch.await();

      Thread.sleep(100);

      Assert.assertFalse(handler.failed);

      // Make sure no exceptions were thrown from onMessage
      Assert.assertNull(consumer.getLastException());
      latch = new CountDownLatch(50);
      handler = new MyHandler(latch);
      consumer.setMessageHandler(handler);
      session.start();
      Assert.assertTrue("message received " + handler.messageReceived, latch.await(5, TimeUnit.SECONDS));

      Thread.sleep(100);

      Assert.assertFalse(handler.failed);
      Assert.assertNull(consumer.getLastException());
      session.close();
   }

   public void testSetUnsetMessageHandler() throws Exception
   {
      ClientSessionFactory sf = locator.createSessionFactory();

      final ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = CreateMessage.createTextMessage("m" + i, session);
         message.putIntProperty(new SimpleString("i"), i);
         producer.send(message);
      }

      final ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      CountDownLatch latch = new CountDownLatch(50);

      // Message should be in consumer

      class MyHandler implements MessageHandler
      {
         int messageReceived = 0;

         boolean failed;

         boolean started = true;

         private final CountDownLatch latch;

         public MyHandler(final CountDownLatch latch)
         {
            this.latch = latch;
         }

         public void onMessage(final ClientMessage message)
         {

            try
            {
               if (!started)
               {
                  failed = true;
               }
               messageReceived++;
               latch.countDown();

               if (latch.getCount() == 0)
               {

                  message.acknowledge();
                  started = false;
                  consumer.setMessageHandler(null);
               }

            }
            catch (Exception e)
            {
            }
         }
      }

      MyHandler handler = new MyHandler(latch);

      consumer.setMessageHandler(handler);

      latch.await();

      Thread.sleep(100);

      Assert.assertFalse(handler.failed);

      // Make sure no exceptions were thrown from onMessage
      Assert.assertNull(consumer.getLastException());
      consumer.setMessageHandler(null);
      ClientMessage cm = consumer.receiveImmediate();
      Assert.assertNotNull(cm);

      session.close();
   }

   public void testSetUnsetResetMessageHandler() throws Exception
   {
      ClientSessionFactory sf = locator.createSessionFactory();

      final ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = CreateMessage.createTextMessage("m" + i, session);
         message.putIntProperty(new SimpleString("i"), i);
         producer.send(message);
      }

      final ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      CountDownLatch latch = new CountDownLatch(50);

      // Message should be in consumer

      class MyHandler implements MessageHandler
      {
         int messageReceived = 0;

         boolean failed;

         boolean started = true;

         private final CountDownLatch latch;

         public MyHandler(final CountDownLatch latch)
         {
            this.latch = latch;
         }

         public void onMessage(final ClientMessage message)
         {

            try
            {
               if (!started)
               {
                  failed = true;
               }
               messageReceived++;
               latch.countDown();

               if (latch.getCount() == 0)
               {

                  message.acknowledge();
                  started = false;
                  consumer.setMessageHandler(null);
               }

            }
            catch (Exception e)
            {
            }
         }
      }

      MyHandler handler = new MyHandler(latch);

      consumer.setMessageHandler(handler);

      latch.await();

      Thread.sleep(100);

      Assert.assertFalse(handler.failed);

      // Make sure no exceptions were thrown from onMessage
      Assert.assertNull(consumer.getLastException());
      consumer.setMessageHandler(null);
      ClientMessage cm = consumer.receiveImmediate();
      Assert.assertNotNull(cm);
      latch = new CountDownLatch(49);
      handler = new MyHandler(latch);
      consumer.setMessageHandler(handler);
      session.start();
      Assert.assertTrue("message received " + handler.messageReceived, latch.await(5, TimeUnit.SECONDS));

      Thread.sleep(100);

      Assert.assertFalse(handler.failed);
      Assert.assertNull(consumer.getLastException());
      session.close();
   }
}
