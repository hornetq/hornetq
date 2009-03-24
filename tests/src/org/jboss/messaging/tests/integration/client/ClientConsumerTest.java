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

import java.util.concurrent.CountDownLatch;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.MessageHandler;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.utils.SimpleString;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class ClientConsumerTest extends ServiceTestBase
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

   public void testSimpleConsumerBrowser() throws Exception
   {
      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"));

      sf.setBlockOnNonPersistentSend(true);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, session);
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE, null, true);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive(1000);

         assertEquals("m" + i, message2.getBody().readString());
      }

      consumer.close();

      consumer = session.createConsumer(QUEUE, null, true);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive(1000);

         assertEquals("m" + i, message2.getBody().readString());
      }

      consumer.close();

      session.close();

   }

   public void testMessageCounter() throws Exception
   {
      ClientSessionFactory sf = createInVMFactory();

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);
      
      ClientSession session = sf.createSession(null, null, false, false, false, false, 0);

      session.createQueue(QUEUE, QUEUE, null, false, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, session);
         producer.send(message);
      }
      
      session.commit();
      session.start();
      
      assertEquals(100, getMessageCount(messagingService.getServer().getPostOffice(), QUEUE.toString()));

      ClientConsumer consumer = session.createConsumer(QUEUE, null, false);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);

         assertNotNull(message);
         message.acknowledge();

         session.commit();

         assertEquals("m" + i, message.getBody().readString());
      }

      session.close();

      assertEquals(0, getMessageCount(messagingService.getServer().getPostOffice(), QUEUE.toString()));

   }

   public void testConsumerBrowserWithSelector() throws Exception
   {

      ClientSessionFactory sf = createInVMFactory();

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, session);
         message.putIntProperty(new SimpleString("x"), i);
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE, new SimpleString("x >= 50"), true);

      for (int i = 50; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive(1000);

         assertEquals("m" + i, message2.getBody().readString());
      }

      consumer.close();

      consumer = session.createConsumer(QUEUE, null, true);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive(1000);

         assertEquals("m" + i, message2.getBody().readString());
      }

      consumer.close();

      session.close();

   }

   public void testConsumerBrowserWithStringSelector() throws Exception
   {

      ClientSessionFactory sf = createInVMFactory();

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, session);
         if (i % 2 == 0)
         {
            message.putStringProperty(new SimpleString("color"), new SimpleString("RED"));
         }
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE, new SimpleString("color = 'RED'"), true);

      for (int i = 0; i < numMessages; i += 2)
      {
         ClientMessage message2 = consumer.receive(1000);

         assertEquals("m" + i, message2.getBody().readString());
      }

      session.close();

   }

   public void testConsumerMultipleBrowser() throws Exception
   {

      ClientSessionFactory sf = createInVMFactory();

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, session);
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE, null, true);
      ClientConsumer consumer2 = session.createConsumer(QUEUE, null, true);
      ClientConsumer consumer3 = session.createConsumer(QUEUE, null, true);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive(1000);
         assertEquals("m" + i, message2.getBody().readString());
         message2 = consumer2.receive(1000);
         assertEquals("m" + i, message2.getBody().readString());
         message2 = consumer3.receive(1000);
         assertEquals("m" + i, message2.getBody().readString());
      }

      session.close();

   }

   public void testConsumerMultipleBrowserWithSelector() throws Exception
   {

      ClientSessionFactory sf = createInVMFactory();

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, session);
         message.putIntProperty(new SimpleString("x"), i);
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE, new SimpleString("x < 50"), true);
      ClientConsumer consumer2 = session.createConsumer(QUEUE, new SimpleString("x >= 50"), true);
      ClientConsumer consumer3 = session.createConsumer(QUEUE, null, true);

      for (int i = 0; i < 50; i++)
      {
         ClientMessage message2 = consumer.receive(1000);
         assertEquals("m" + i, message2.getBody().readString());
      }
      for (int i = 50; i < numMessages; i++)
      {
         ClientMessage message2 = consumer2.receive(1000);
         assertEquals("m" + i, message2.getBody().readString());
      }
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer3.receive(1000);
         assertEquals("m" + i, message2.getBody().readString());
      }

      session.close();

   }

   public void testConsumerBrowserMessages() throws Exception
   {
      testConsumerBrowserMessagesArentAcked(false);
   }

   public void testConsumerBrowserMessagesPreACK() throws Exception
   {
      testConsumerBrowserMessagesArentAcked(false);
   }

   private void testConsumerBrowserMessagesArentAcked(boolean preACK) throws Exception
   {
      ClientSessionFactory sf = createInVMFactory();

      ClientSession session = sf.createSession(null, null, false, true, true, preACK, 0);

      session.createQueue(QUEUE, QUEUE, null, false, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, session);
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE, null, true);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive(1000);

         assertEquals("m" + i, message2.getBody().readString());
      }
      // assert that all the messages are there and none have been acked
      assertEquals(0,
                   ((Queue)messagingService.getServer().getPostOffice().getBinding(QUEUE).getBindable()).getDeliveringCount());
      assertEquals(100,
                   ((Queue)messagingService.getServer().getPostOffice().getBinding(QUEUE).getBindable()).getMessageCount());

      session.close();
   }

   public void testConsumerBrowserMessageAckDoesNothing() throws Exception
   {
      ClientSessionFactory sf = createInVMFactory();

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, session);
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE, null, true);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive(1000);

         message2.acknowledge();

         assertEquals("m" + i, message2.getBody().readString());
      }
      // assert that all the messages are there and none have been acked
      assertEquals(0,
                   ((Queue)messagingService.getServer().getPostOffice().getBinding(QUEUE).getBindable()).getDeliveringCount());
      assertEquals(100,
                   ((Queue)messagingService.getServer().getPostOffice().getBinding(QUEUE).getBindable()).getMessageCount());

      session.close();
   }

   public void testSetMessageHandlerWithMessagesPending() throws Exception
   {
      ClientSessionFactory sf = createInVMFactory();

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, session);
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
      assertNull(consumer.getLastException());

      session.close();
   }

   public void testStopConsumer() throws Exception
   {
      ClientSessionFactory sf = createInVMFactory();

      final ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, session);
         producer.send(message);
      }

      final ClientConsumer consumer = session.createConsumer(QUEUE, null, true);

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
                  session.stop(); // Shouldn't this alone prevent messages being delivered to this Handler?
                  started = false;
                  consumer.setMessageHandler(null); // If we comment out this line, the test will fail
               }

               message.acknowledge();
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

      for (int i = 0; i < 90; i++)
      {
         ClientMessage msg = consumer.receive(1000);
         assertNotNull(msg);
         msg.acknowledge();
      }

      assertNull(consumer.receiveImmediate());

      session.close();
   }

   public void testConsumerAckImmediateAutoCommitTrue() throws Exception
   {
      ClientSessionFactory sf = createInVMFactory();

      ClientSession session = sf.createSession(false, true, true, true);

      session.createQueue(QUEUE, QUEUE, null, false, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, session);
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);
      session.start();
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive(1000);

         assertEquals("m" + i, message2.getBody().readString());
      }
      // assert that all the messages are there and none have been acked
      assertEquals(0,
                   ((Queue)messagingService.getServer().getPostOffice().getBinding(QUEUE).getBindable()).getDeliveringCount());
      assertEquals(0,
                   ((Queue)messagingService.getServer().getPostOffice().getBinding(QUEUE).getBindable()).getMessageCount());

      session.close();
   }

   public void testConsumerAckImmediateAutoCommitFalse() throws Exception
   {

      ClientSessionFactory sf = createInVMFactory();

      ClientSession session = sf.createSession(false, true, false, true);

      session.createQueue(QUEUE, QUEUE, null, false, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, session);
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);
      session.start();
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive(1000);

         assertEquals("m" + i, message2.getBody().readString());
      }
      // assert that all the messages are there and none have been acked
      assertEquals(0,
                   ((Queue)messagingService.getServer().getPostOffice().getBinding(QUEUE).getBindable()).getDeliveringCount());
      assertEquals(0,
                   ((Queue)messagingService.getServer().getPostOffice().getBinding(QUEUE).getBindable()).getMessageCount());

      session.close();
   }

   public void testConsumerAckImmediateAckIgnored() throws Exception
   {

      ClientSessionFactory sf = createInVMFactory();

      ClientSession session = sf.createSession(false, true, true, true);

      session.createQueue(QUEUE, QUEUE, null, false, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, session);
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);
      session.start();
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive(1000);

         assertEquals("m" + i, message2.getBody().readString());
         if (i < 50)
         {
            message2.acknowledge();
         }
      }
      // assert that all the messages are there and none have been acked
      assertEquals(0,
                   ((Queue)messagingService.getServer().getPostOffice().getBinding(QUEUE).getBindable()).getDeliveringCount());
      assertEquals(0,
                   ((Queue)messagingService.getServer().getPostOffice().getBinding(QUEUE).getBindable()).getMessageCount());

      session.close();
   }

   public void testConsumerAckImmediateCloseSession() throws Exception
   {

      ClientSessionFactory sf = createInVMFactory();

      ClientSession session = sf.createSession(false, true, true, true);

      session.createQueue(QUEUE, QUEUE, null, false, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, session);
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);
      session.start();
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive(1000);

         assertEquals("m" + i, message2.getBody().readString());
         if (i < 50)
         {
            message2.acknowledge();
         }
      }
      // assert that all the messages are there and none have been acked
      assertEquals(0,
                   ((Queue)messagingService.getServer().getPostOffice().getBinding(QUEUE).getBindable()).getDeliveringCount());
      assertEquals(0,
                   ((Queue)messagingService.getServer().getPostOffice().getBinding(QUEUE).getBindable()).getMessageCount());

      session.close();

      assertEquals(0,
                   ((Queue)messagingService.getServer().getPostOffice().getBinding(QUEUE).getBindable()).getDeliveringCount());
      assertEquals(0,
                   ((Queue)messagingService.getServer().getPostOffice().getBinding(QUEUE).getBindable()).getMessageCount());
   }

}
