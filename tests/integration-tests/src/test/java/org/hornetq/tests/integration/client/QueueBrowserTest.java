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

import junit.framework.Assert;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.*;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class QueueBrowserTest extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(QueueBrowserTest.class);

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

   private ClientSessionFactory sf;

   public void testSimpleConsumerBrowser() throws Exception
   {
      locator.setBlockOnNonDurableSend(true);

      sf = locator.createSessionFactory();
      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

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

         Assert.assertEquals("m" + i, message2.getBodyBuffer().readString());
      }

      consumer.close();

      consumer = session.createConsumer(QUEUE, null, true);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive(1000);

         Assert.assertEquals("m" + i, message2.getBodyBuffer().readString());
      }

      consumer.close();

      session.close();

      sf.close();

   }

   public void testConsumerBrowserWithSelector() throws Exception
   {

      ClientSessionFactory sf = locator.createSessionFactory();

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

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

         Assert.assertEquals("m" + i, message2.getBodyBuffer().readString());
      }

      consumer.close();

      consumer = session.createConsumer(QUEUE, null, true);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive(1000);

         Assert.assertEquals("m" + i, message2.getBodyBuffer().readString());
      }

      consumer.close();

      session.close();

      sf.close();

   }

   public void testConsumerBrowserWithStringSelector() throws Exception
   {

      ClientSessionFactory sf = locator.createSessionFactory();

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

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

         Assert.assertEquals("m" + i, message2.getBodyBuffer().readString());
      }

      session.close();

      sf.close();

   }

   public void testConsumerMultipleBrowser() throws Exception
   {

      ClientSessionFactory sf = locator.createSessionFactory();

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

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
         Assert.assertEquals("m" + i, message2.getBodyBuffer().readString());
         message2 = consumer2.receive(1000);
         Assert.assertEquals("m" + i, message2.getBodyBuffer().readString());
         message2 = consumer3.receive(1000);
         Assert.assertEquals("m" + i, message2.getBodyBuffer().readString());
      }

      session.close();
      sf.close();

   }

   public void testConsumerMultipleBrowserWithSelector() throws Exception
   {

      ClientSessionFactory sf = locator.createSessionFactory();

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

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
         Assert.assertEquals("m" + i, message2.getBodyBuffer().readString());
      }
      for (int i = 50; i < numMessages; i++)
      {
         ClientMessage message2 = consumer2.receive(1000);
         Assert.assertEquals("m" + i, message2.getBodyBuffer().readString());
      }
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer3.receive(1000);
         Assert.assertEquals("m" + i, message2.getBodyBuffer().readString());
      }

      session.close();

      sf.close();

   }

   public void testConsumerBrowserMessages() throws Exception
   {
      testConsumerBrowserMessagesArentAcked(false);
   }

   public void testConsumerBrowserMessagesPreACK() throws Exception
   {
      testConsumerBrowserMessagesArentAcked(false);
   }

   private void testConsumerBrowserMessagesArentAcked(final boolean preACK) throws Exception
   {
      ClientSessionFactory sf = locator.createSessionFactory();

      ClientSession session = sf.createSession(null, null, false, true, true, preACK, 0);

      session.createQueue(QUEUE, QUEUE, null, false);

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

         Assert.assertEquals("m" + i, message2.getBodyBuffer().readString());
      }
      // assert that all the messages are there and none have been acked
      Assert.assertEquals(0, ((Queue)server.getPostOffice().getBinding(QUEUE).getBindable()).getDeliveringCount());
      Assert.assertEquals(100, ((Queue)server.getPostOffice().getBinding(QUEUE).getBindable()).getMessageCount());

      session.close();

      sf.close();
   }

   public void testConsumerBrowserMessageAckDoesNothing() throws Exception
   {
      ClientSessionFactory sf = locator.createSessionFactory();

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

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

         Assert.assertEquals("m" + i, message2.getBodyBuffer().readString());
      }
      // assert that all the messages are there and none have been acked
      Assert.assertEquals(0, ((Queue)server.getPostOffice().getBinding(QUEUE).getBindable()).getDeliveringCount());
      Assert.assertEquals(100, ((Queue)server.getPostOffice().getBinding(QUEUE).getBindable()).getMessageCount());

      session.close();

      sf.close();
   }
   
   public void testBrowseWithZeroConsumerWindowSize() throws Exception
   {
      locator.setConsumerWindowSize(0);

      ClientSessionFactory sf = locator.createSessionFactory();

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      byte[] bytes = new byte[240];
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(false);
         
         message.getBodyBuffer().writeBytes(bytes);
         
         message.putIntProperty("foo", i);
         
         producer.send(message);
      }

      //Create a normal non browsing consumer
      ClientConsumer consumer = session.createConsumer(QUEUE);
      
      session.start();
      
      ClientConsumer browser = session.createConsumer(QUEUE, true);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = browser.receive(1000);

         assertEquals(i, message2.getIntProperty("foo").intValue());
      }

      session.close();

      sf.close();
   }

}
