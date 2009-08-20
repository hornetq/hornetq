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

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.MessageHandler;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.MessagingServer;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.SimpleString;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class SessionStopStartTest extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(SessionStopStartTest.class);

   private MessagingServer server;

   private final SimpleString QUEUE = new SimpleString("ConsumerTestQueue");

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      server = createServer(false);

      server.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      server.stop();

      server = null;

      super.tearDown();
   }

   public void testStopStartConsumerSyncReceiveImmediate() throws Exception
   {
      ClientSessionFactory sf = createInVMFactory();

      final ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, session);
         message.putIntProperty(new SimpleString("i"), i);
         producer.send(message);
      }

      final ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();


      for (int i = 0; i < numMessages / 2; i++)
      {
         ClientMessage cm = consumer.receive(5000);
         assertNotNull(cm);
         cm.acknowledge();
      }
      session.stop();
      ClientMessage cm = consumer.receiveImmediate();
      assertNull(cm);

      session.start();
      for (int i = 0; i < numMessages / 2; i++)
      {
         cm = consumer.receive(5000);
         assertNotNull(cm);
         cm.acknowledge();
      }

      session.close();
   }

   public void testStopStartConsumerSyncReceive() throws Exception
   {
      ClientSessionFactory sf = createInVMFactory();

      final ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, session);
         message.putIntProperty(new SimpleString("i"), i);
         producer.send(message);
      }

      final ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();


      for (int i = 0; i < numMessages / 2; i++)
      {
         ClientMessage cm = consumer.receive(5000);
         assertNotNull(cm);
         cm.acknowledge();
      }
      session.stop();
      long time = System.currentTimeMillis();
      ClientMessage cm = consumer.receive(1000);
      long taken = System.currentTimeMillis() - time;
      assertTrue(taken >= 1000);
      assertNull(cm);

      session.start();
      for (int i = 0; i < numMessages / 2; i++)
      {
         cm = consumer.receive(5000);
         assertNotNull(cm);
         cm.acknowledge();
      }

      session.close();
   }

   public void testStopStartConsumerAsyncSyncStoppedByHandler() throws Exception
   {
      ClientSessionFactory sf = createInVMFactory();

      final ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, session);
         message.putIntProperty(new SimpleString("i"), i);
         producer.send(message);
      }

      final ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      final CountDownLatch latch = new CountDownLatch(10);

      // Message should be in consumer

      class MyHandler implements MessageHandler
      {
         boolean failed;

         boolean started = true;

         public void onMessage(final ClientMessage message)
         {

            try
            {
               if (!started)
               {
                  failed = true;
               }

               latch.countDown();

               if (latch.getCount() == 0)
               {

                  message.acknowledge();
                  session.stop();
                  started = false;
               }
            }
            catch (Exception e)
            {
            }
         }
      }

      MyHandler handler = new MyHandler();

      consumer.setMessageHandler(handler);

      latch.await();

      Thread.sleep(100);

      assertFalse(handler.failed);

      // Make sure no exceptions were thrown from onMessage
      assertNull(consumer.getLastException());
      consumer.setMessageHandler(null);
      session.start();
      for (int i = 0; i < 90; i++)
      {
         ClientMessage msg = consumer.receive(1000);
         if (msg == null)
         {
            System.out.println("ClientConsumerTest.testStopConsumer");
         }
         assertNotNull("message " + i, msg);
         msg.acknowledge();
      }

      assertNull(consumer.receiveImmediate());

      session.close();
   }

   public void testStopStartConsumerAsyncSync() throws Exception
   {
      ClientSessionFactory sf = createInVMFactory();

      final ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, session);
         message.putIntProperty(new SimpleString("i"), i);
         producer.send(message);
      }

      final ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      final CountDownLatch latch = new CountDownLatch(10);

      // Message should be in consumer

      class MyHandler implements MessageHandler
      {
         boolean failed;

         boolean started = true;

         public void onMessage(final ClientMessage message)
         {

            try
            {
               if (!started)
               {
                  failed = true;
               }

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

      MyHandler handler = new MyHandler();

      consumer.setMessageHandler(handler);

      latch.await();

      try
      {
         session.stop();
      }
      catch (Exception e)
      {
         log.warn(e.getMessage(), e);
         throw e;
      }

      assertFalse(handler.failed);

      // Make sure no exceptions were thrown from onMessage
      assertNull(consumer.getLastException());
      consumer.setMessageHandler(null);
      session.start();
      for (int i = 0; i < 90; i++)
      {
         ClientMessage msg = consumer.receive(1000);
         if (msg == null)
         {
            System.out.println("ClientConsumerTest.testStopConsumer");
         }
         assertNotNull("message " + i, msg);
         msg.acknowledge();
      }

      assertNull(consumer.receiveImmediate());

      session.close();
   }

   public void testStopStartConsumerAsyncASyncStoppeeByHandler() throws Exception
   {
      ClientSessionFactory sf = createInVMFactory();

      final ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, session);
         message.putIntProperty(new SimpleString("i"), i);
         producer.send(message);
      }

      final ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      CountDownLatch latch = new CountDownLatch(10);

      // Message should be in consumer

      class MyHandler implements MessageHandler
      {
         int messageReceived = 0;

         boolean failed;

         boolean started = true;

         private final CountDownLatch latch;

         private boolean stop = true;

         public MyHandler(CountDownLatch latch)
         {
            this.latch = latch;
         }

         public MyHandler(CountDownLatch latch, boolean stop)
         {
            this(latch);
            this.stop = stop;
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

               if (stop && latch.getCount() == 0)
               {

                  message.acknowledge();
                  session.stop();
                  started = false;
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

      assertFalse(handler.failed);

      // Make sure no exceptions were thrown from onMessage
      assertNull(consumer.getLastException());
      latch = new CountDownLatch(90);
      handler = new MyHandler(latch, false);
      consumer.setMessageHandler(handler);
      session.start();
      assertTrue("message received " + handler.messageReceived, latch.await(5, TimeUnit.SECONDS));

      Thread.sleep(100);

      assertFalse(handler.failed);
      assertNull(consumer.getLastException());
      session.close();
   }

   public void testStopStartConsumerAsyncASync() throws Exception
   {
      ClientSessionFactory sf = createInVMFactory();

      final ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, session);
         message.putIntProperty(new SimpleString("i"), i);
         producer.send(message);
      }

      final ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      CountDownLatch latch = new CountDownLatch(10);

      // Message should be in consumer

      class MyHandler implements MessageHandler
      {
         int messageReceived = 0;

         boolean failed;

         boolean started = true;

         private final CountDownLatch latch;

         private boolean stop = true;

         public MyHandler(CountDownLatch latch)
         {
            this.latch = latch;
         }

         public MyHandler(CountDownLatch latch, boolean stop)
         {
            this(latch);
            this.stop = stop;
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

               if (stop && latch.getCount() == 0)
               {

                  message.acknowledge();
                  consumer.setMessageHandler(null);
                  started = false;
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

      assertFalse(handler.failed);

      // Make sure no exceptions were thrown from onMessage
      assertNull(consumer.getLastException());
      latch = new CountDownLatch(90);
      handler = new MyHandler(latch, false);
      consumer.setMessageHandler(handler);
      session.start();
      assertTrue("message received " + handler.messageReceived, latch.await(5, TimeUnit.SECONDS));

      Thread.sleep(100);

      assertFalse(handler.failed);
      assertNull(consumer.getLastException());
      session.close();
   }
   
   private int getMessageEncodeSize(final SimpleString address) throws Exception
   {
      ClientSessionFactory cf = createInVMFactory();
      ClientSession session = cf.createSession(false, true, true);
      ClientMessage message = session.createClientMessage(false);
      // we need to set the destination so we can calculate the encodesize correctly
      message.setDestination(address);
      int encodeSize = message.getEncodeSize();
      session.close();
      cf.close();
      return encodeSize;      
   }

   public void testStopStartMultipleConsumers() throws Exception
   {
      ClientSessionFactory sf = createInVMFactory();
      sf.setConsumerWindowSize(this.getMessageEncodeSize(QUEUE) * 33);

      final ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, session);
         message.putIntProperty(new SimpleString("i"), i);
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);
      ClientConsumer consumer2 = session.createConsumer(QUEUE);
      ClientConsumer consumer3 = session.createConsumer(QUEUE);

      session.start();

      ClientMessage cm = consumer.receive(5000);
      assertNotNull(cm);
      cm.acknowledge();
      cm = consumer2.receive(5000);
      assertNotNull(cm);
      cm.acknowledge();
      cm = consumer3.receive(5000);
      assertNotNull(cm);
      cm.acknowledge();

      session.stop();
      cm = consumer.receiveImmediate();
      assertNull(cm);
      cm = consumer2.receiveImmediate();
      assertNull(cm);
      cm = consumer3.receiveImmediate();
      assertNull(cm);

      session.start();
      cm = consumer.receive(5000);
      assertNotNull(cm);
      cm = consumer2.receive(5000);
      assertNotNull(cm);
      cm = consumer3.receive(5000);
      assertNotNull(cm);
      session.close();
   }


   public void testStopStartAlreadyStartedSession() throws Exception
   {
      ClientSessionFactory sf = createInVMFactory();

      final ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, session);
         message.putIntProperty(new SimpleString("i"), i);
         producer.send(message);
      }

      final ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();


      for (int i = 0; i < numMessages / 2; i++)
      {
         ClientMessage cm = consumer.receive(5000);
         assertNotNull(cm);
         cm.acknowledge();
      }

      session.start();
      for (int i = 0; i < numMessages / 2; i++)
      {
         ClientMessage cm = consumer.receive(5000);
         assertNotNull(cm);
         cm.acknowledge();
      }

      session.close();
   }

   public void testStopAlreadyStoppedSession() throws Exception
   {
      ClientSessionFactory sf = createInVMFactory();

      final ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, session);
         message.putIntProperty(new SimpleString("i"), i);
         producer.send(message);
      }

      final ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();


      for (int i = 0; i < numMessages / 2; i++)
      {
         ClientMessage cm = consumer.receive(5000);
         assertNotNull(cm);
         cm.acknowledge();
      }
      session.stop();
      ClientMessage cm = consumer.receiveImmediate();
      assertNull(cm);

      session.stop();
      cm = consumer.receiveImmediate();
      assertNull(cm);

      session.start();
      for (int i = 0; i < numMessages / 2; i++)
      {
         cm = consumer.receive(5000);
         assertNotNull(cm);
         cm.acknowledge();
      }

      session.close();
   }

}
