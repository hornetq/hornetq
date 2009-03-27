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
package org.jboss.messaging.tests.integration.client;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.MessageHandler;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.utils.SimpleString;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ClientSessionStopStartTest extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(ClientConsumerTest.class);

   private MessagingService messagingService;

   private final SimpleString QUEUE = new SimpleString("ConsumerTestQueue");

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      messagingService = createService(false);

      messagingService.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      messagingService.stop();

      messagingService = null;

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

   public void testStopStartMultipleConsumers() throws Exception
   {
      ClientSessionFactory sf = createInVMFactory();
      ClientSession tempSess = sf.createSession(false, true, true);
      ClientMessage tempMessage = tempSess.createClientMessage(false);
      tempMessage.setDestination(QUEUE);
      int size = tempMessage.getEncodeSize();
      sf.setConsumerWindowSize(size * 33);
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
