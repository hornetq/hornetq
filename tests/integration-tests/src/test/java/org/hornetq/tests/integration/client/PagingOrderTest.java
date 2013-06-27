/*
 * Copyright 2010 Red Hat, Inc.
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
import org.junit.After;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.Bindings;
import org.hornetq.core.postoffice.impl.LocalQueueBinding;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.impl.QueueImpl;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.jms.client.HornetQJMSConnectionFactory;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.tests.unit.util.InVMNamingContext;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * A PagingOrderTest. PagingTest has a lot of tests already. I decided to create a newer one more
 * specialized on Ordering and counters
 * @author clebertsuconic
 */
public class PagingOrderTest extends ServiceTestBase
{

   private static final int PAGE_MAX = 100 * 1024;

   private static final int PAGE_SIZE = 10 * 1024;

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   static final SimpleString ADDRESS = new SimpleString("SimpleAddress");

   private Connection conn;

   @Override
   @After
   public void tearDown() throws Exception
   {
      try
      {
         if (conn != null)
            conn.close();
      }
      finally
      {
         super.tearDown();
      }
   }

   @Test
   public void testOrder1() throws Throwable
   {
      boolean persistentMessages = true;

      Configuration config = createDefaultConfig();

      config.setJournalSyncNonTransactional(false);

      HornetQServer server = createServer(true, config, PAGE_SIZE, PAGE_MAX, new HashMap<String, AddressSettings>());

      server.start();

      final int messageSize = 1024;

      final int numberOfMessages = 500;
      ServerLocator locator = createInVMNonHALocator();

         locator.setClientFailureCheckPeriod(1000);
         locator.setConnectionTTL(2000);
         locator.setReconnectAttempts(0);

         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);
         locator.setBlockOnAcknowledge(true);
         locator.setConsumerWindowSize(1024 * 1024);

      ClientSessionFactory sf = createSessionFactory(locator);

         ClientSession session = sf.createSession(false, false, false);

      server.createQueue(ADDRESS, ADDRESS, null, true, false);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         byte[] body = new byte[messageSize];

         ByteBuffer bb = ByteBuffer.wrap(body);

         for (int j = 1; j <= messageSize; j++)
         {
            bb.put(getSamplebyte(j));
         }

         for (int i = 0; i < numberOfMessages; i++)
         {
            ClientMessage message = session.createMessage(persistentMessages);

            HornetQBuffer bodyLocal = message.getBodyBuffer();

            bodyLocal.writeBytes(body);

            message.putIntProperty(new SimpleString("id"), i);

            producer.send(message);
            if (i % 1000 == 0)
            {
               session.commit();
            }
         }

         session.commit();

         session.close();

         session = sf.createSession(true, true, 0);

         session.start();

         ClientConsumer consumer = session.createConsumer(ADDRESS);

         for (int i = 0; i < numberOfMessages / 2; i++)
         {
            ClientMessage message = consumer.receive(5000);
            assertNotNull(message);
            assertEquals(i, message.getIntProperty("id").intValue());

            if (i < 100)
            {
               // Do not consume the last one so we could restart
               message.acknowledge();
            }
         }

         session.close();

         session = null;

         sf.close();
      sf = createSessionFactory(locator);

         locator = createInVMNonHALocator();

         session = sf.createSession(true, true, 0);

         session.start();

         consumer = session.createConsumer(ADDRESS);

         for (int i = 100; i < numberOfMessages; i++)
         {
            ClientMessage message = consumer.receive(5000);
            assertNotNull(message);
            assertEquals(i, message.getIntProperty("id").intValue());
            message.acknowledge();
         }

         session.close();
   }

   @Test
   public void testPageCounter() throws Throwable
   {
      boolean persistentMessages = true;

      Configuration config = createDefaultConfig();

      config.setJournalSyncNonTransactional(false);

      HornetQServer server = createServer(true, config, PAGE_SIZE, PAGE_MAX, new HashMap<String, AddressSettings>());

      server.start();

      final int messageSize = 1024;

      final int numberOfMessages = 500;

         ServerLocator locator = createInVMNonHALocator();

         locator.setClientFailureCheckPeriod(1000);
         locator.setConnectionTTL(2000);
         locator.setReconnectAttempts(0);

         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);
         locator.setBlockOnAcknowledge(true);
         locator.setConsumerWindowSize(1024 * 1024);

         ClientSessionFactory sf = createSessionFactory(locator);

         ClientSession session = sf.createSession(false, false, false);

         Queue q1 = server.createQueue(ADDRESS, ADDRESS, null, true, false);

         Queue q2 = server.createQueue(ADDRESS, new SimpleString("inactive"), null, true, false);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         byte[] body = new byte[messageSize];

         ByteBuffer bb = ByteBuffer.wrap(body);

         for (int j = 1; j <= messageSize; j++)
         {
            bb.put(getSamplebyte(j));
         }

         final AtomicInteger errors = new AtomicInteger(0);

         Thread t1 = new Thread()
         {
            @Override
            public void run()
            {
               try
               {
                  ServerLocator sl = createInVMNonHALocator();
                  ClientSessionFactory sf = sl.createSessionFactory();
                  ClientSession sess = sf.createSession(true, true, 0);
                  sess.start();
                  ClientConsumer cons = sess.createConsumer(ADDRESS);
                  for (int i = 0; i < numberOfMessages; i++)
                  {
                     ClientMessage msg = cons.receive(5000);
                     assertNotNull(msg);
                     assertEquals(i, msg.getIntProperty("id").intValue());
                     msg.acknowledge();
                  }

                  assertNull(cons.receiveImmediate());
                  sess.close();
                  sl.close();
               }
               catch (Throwable e)
               {
                  e.printStackTrace();
                  errors.incrementAndGet();
               }

            }
         };

         t1.start();

         for (int i = 0; i < numberOfMessages; i++)
         {
            ClientMessage message = session.createMessage(persistentMessages);

            HornetQBuffer bodyLocal = message.getBodyBuffer();

            bodyLocal.writeBytes(body);

            message.putIntProperty(new SimpleString("id"), i);

            producer.send(message);
            if (i % 20 == 0)
            {
               session.commit();
            }
         }

         session.commit();

         t1.join();

         assertEquals(0, errors.get());

         assertEquals(numberOfMessages, q2.getMessageCount());
         assertEquals(numberOfMessages, q2.getMessagesAdded());
         assertEquals(0, q1.getMessageCount());
         assertEquals(numberOfMessages, q1.getMessagesAdded());

         session.close();
         sf.close();
         locator.close();

         server.stop();

         server.start();

         Bindings bindings = server.getPostOffice().getBindingsForAddress(ADDRESS);

         q1 = null;
         q2 = null;

         for (Binding bind : bindings.getBindings())
         {
            if (bind instanceof LocalQueueBinding)
            {
               LocalQueueBinding qb = (LocalQueueBinding)bind;
               if (qb.getQueue().getName().equals(ADDRESS))
               {
                  q1 = qb.getQueue();
               }

               if (qb.getQueue().getName().equals(new SimpleString("inactive")))
               {
                  q2 = qb.getQueue();
               }
            }
         }

         assertNotNull(q1);

         assertNotNull(q2);

         assertEquals("q2 msg count", numberOfMessages, q2.getMessageCount());
         assertEquals("q2 msgs added", numberOfMessages, q2.getMessagesAdded());
         assertEquals("q1 msg count", 0, q1.getMessageCount());
         // 0, since nothing was sent to the queue after the server was restarted
         assertEquals("q1 msgs added", 0, q1.getMessagesAdded());

      }

   @Test
   public void testPageCounter2() throws Throwable
   {
      boolean persistentMessages = true;

      Configuration config = createDefaultConfig();

      config.setJournalSyncNonTransactional(false);

      HornetQServer server = createServer(true, config, PAGE_SIZE, PAGE_MAX, new HashMap<String, AddressSettings>());

      server.start();

      final int messageSize = 1024;

      final int numberOfMessages = 500;

         ServerLocator locator = createInVMNonHALocator();

         locator.setClientFailureCheckPeriod(1000);
         locator.setConnectionTTL(2000);
         locator.setReconnectAttempts(0);

         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);
         locator.setBlockOnAcknowledge(true);
         locator.setConsumerWindowSize(1024 * 1024);

         ClientSessionFactory sf = createSessionFactory(locator);

         ClientSession session = sf.createSession(false, false, false);

         Queue q1 = server.createQueue(ADDRESS, ADDRESS, null, true, false);

         Queue q2 = server.createQueue(ADDRESS, new SimpleString("inactive"), null, true, false);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         byte[] body = new byte[messageSize];

         ByteBuffer bb = ByteBuffer.wrap(body);

         for (int j = 1; j <= messageSize; j++)
         {
            bb.put(getSamplebyte(j));
         }

         final AtomicInteger errors = new AtomicInteger(0);

         Thread t1 = new Thread()
         {
            @Override
            public void run()
            {
               try
               {
                  ServerLocator sl = createInVMNonHALocator();
                  ClientSessionFactory sf = sl.createSessionFactory();
                  ClientSession sess = sf.createSession(true, true, 0);
                  sess.start();
                  ClientConsumer cons = sess.createConsumer(ADDRESS);
                  for (int i = 0; i < 100; i++)
                  {
                     ClientMessage msg = cons.receive(5000);
                     assertNotNull(msg);
                     assertEquals(i, msg.getIntProperty("id").intValue());
                     msg.acknowledge();
                  }
                  sess.close();
                  sl.close();
               }
               catch (Throwable e)
               {
                  e.printStackTrace();
                  errors.incrementAndGet();
               }

            }
         };

         for (int i = 0; i < numberOfMessages; i++)
         {
            ClientMessage message = session.createMessage(persistentMessages);

            HornetQBuffer bodyLocal = message.getBodyBuffer();

            bodyLocal.writeBytes(body);

            message.putIntProperty(new SimpleString("id"), i);

            producer.send(message);
            if (i % 20 == 0)
            {
               session.commit();
            }
         }

         session.commit();

         t1.start();
         t1.join();

         assertEquals(0, errors.get());
         long timeout = System.currentTimeMillis() + 10000;
         while (numberOfMessages - 100 != q1.getMessageCount() && System.currentTimeMillis() < timeout)
         {
            Thread.sleep(500);

         }

         assertEquals(numberOfMessages, q2.getMessageCount());
         assertEquals(numberOfMessages, q2.getMessagesAdded());
         assertEquals(numberOfMessages - 100, q1.getMessageCount());
         assertEquals(numberOfMessages, q2.getMessagesAdded());
   }

   @Test
   public void testOrderOverRollback() throws Throwable
   {
      boolean persistentMessages = true;

      Configuration config = createDefaultConfig();

      config.setJournalSyncNonTransactional(false);

      HornetQServer server = createServer(true, config, PAGE_SIZE, PAGE_MAX, new HashMap<String, AddressSettings>());

      server.start();

      final int messageSize = 1024;

      final int numberOfMessages = 3000;

         ServerLocator locator = createInVMNonHALocator();

         locator.setClientFailureCheckPeriod(1000);
         locator.setConnectionTTL(2000);
         locator.setReconnectAttempts(0);

         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);
         locator.setBlockOnAcknowledge(true);
         locator.setConsumerWindowSize(1024 * 1024);

         ClientSessionFactory sf = createSessionFactory(locator);

         ClientSession session = sf.createSession(false, false, false);

         server.createQueue(ADDRESS, ADDRESS, null, true, false);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         byte[] body = new byte[messageSize];

         ByteBuffer bb = ByteBuffer.wrap(body);

         for (int j = 1; j <= messageSize; j++)
         {
            bb.put(getSamplebyte(j));
         }

         for (int i = 0; i < numberOfMessages; i++)
         {
            ClientMessage message = session.createMessage(persistentMessages);

            HornetQBuffer bodyLocal = message.getBodyBuffer();

            bodyLocal.writeBytes(body);

            message.putIntProperty(new SimpleString("id"), i);

            producer.send(message);
            if (i % 1000 == 0)
            {
               session.commit();
            }
         }

         session.commit();

         session.close();

         session = sf.createSession(false, false, 0);

         session.start();

         ClientConsumer consumer = session.createConsumer(ADDRESS);

         for (int i = 0; i < numberOfMessages / 2; i++)
         {
            ClientMessage message = consumer.receive(5000);
            assertNotNull(message);
            assertEquals(i, message.getIntProperty("id").intValue());
            message.acknowledge();
         }

         session.rollback();

         session.close();

         session = sf.createSession(false, false, 0);

         session.start();

         consumer = session.createConsumer(ADDRESS);

         for (int i = 0; i < numberOfMessages; i++)
         {
            ClientMessage message = consumer.receive(5000);
            assertNotNull(message);
            assertEquals(i, message.getIntProperty("id").intValue());
            message.acknowledge();
         }

         session.commit();
   }

   @Test
   public void testOrderOverRollback2() throws Throwable
   {
      boolean persistentMessages = true;

      Configuration config = createDefaultConfig();

      config.setJournalSyncNonTransactional(false);

      HornetQServer server = createServer(true, config, PAGE_SIZE, PAGE_MAX, new HashMap<String, AddressSettings>());

      server.start();

      final int messageSize = 1024;

      final int numberOfMessages = 200;

         ServerLocator locator = createInVMNonHALocator();

         locator.setClientFailureCheckPeriod(1000);
         locator.setConnectionTTL(2000);
         locator.setReconnectAttempts(0);

         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);
         locator.setBlockOnAcknowledge(true);
         locator.setConsumerWindowSize(0);

         ClientSessionFactory sf = createSessionFactory(locator);

         ClientSession session = sf.createSession(false, false, false);

         QueueImpl queue = (QueueImpl)server.createQueue(ADDRESS, ADDRESS, null, true, false);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         byte[] body = new byte[messageSize];

         ByteBuffer bb = ByteBuffer.wrap(body);

         for (int j = 1; j <= messageSize; j++)
         {
            bb.put(getSamplebyte(j));
         }

         for (int i = 0; i < numberOfMessages; i++)
         {
            ClientMessage message = session.createMessage(persistentMessages);

            HornetQBuffer bodyLocal = message.getBodyBuffer();

            bodyLocal.writeBytes(body);

            message.putIntProperty(new SimpleString("id"), i);

            producer.send(message);
            if (i % 1000 == 0)
            {
               session.commit();
            }
         }

         session.commit();

         session.close();

         session = sf.createSession(false, false, 0);

         session.start();

         ClientConsumer consumer = session.createConsumer(ADDRESS);

         // number of references without paging
         int numberOfRefs = queue.getNumberOfReferences();

         // consume all non-paged references
         for (int ref = 0; ref < numberOfRefs; ref++)
         {
            ClientMessage msg = consumer.receive(5000);
            assertNotNull(msg);
            msg.acknowledge();
         }

         session.commit();

         session.close();

         session = sf.createSession(false, false, 0);

         session.start();

         consumer = session.createConsumer(ADDRESS);

         ClientMessage msg = consumer.receive(5000);
         assertNotNull(msg);
         int msgIDRolledBack = msg.getIntProperty("id").intValue();
         msg.acknowledge();

         session.rollback();

         msg = consumer.receive(5000);

         assertNotNull(msg);

         assertEquals(msgIDRolledBack, msg.getIntProperty("id").intValue());

         session.rollback();

         session.close();

         sf.close();
         locator.close();

         server.stop();

         server.start();

         locator = createInVMNonHALocator();

         locator.setClientFailureCheckPeriod(1000);
         locator.setConnectionTTL(2000);
         locator.setReconnectAttempts(0);

         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);
         locator.setBlockOnAcknowledge(true);
         locator.setConsumerWindowSize(0);

         sf = createSessionFactory(locator);

         session = sf.createSession(false, false, 0);

         session.start();

         consumer = session.createConsumer(ADDRESS);

         for (int i = msgIDRolledBack; i < numberOfMessages; i++)
         {
            ClientMessage message = consumer.receive(5000);
            assertNotNull(message);
            assertEquals(i, message.getIntProperty("id").intValue());
            message.acknowledge();
         }

         session.commit();

         session.close();
   }

   @Test
   public void testPagingOverCreatedDestinationTopics() throws Exception
   {

      Configuration config = createDefaultConfig();

      config.setJournalSyncNonTransactional(false);

      HornetQServer server = createServer(true, config, PAGE_SIZE, -1, new HashMap<String, AddressSettings>());

      JMSServerManagerImpl jmsServer = new JMSServerManagerImpl(server);
      InVMNamingContext context = new InVMNamingContext();
      jmsServer.setContext(context);
      jmsServer.start();

      jmsServer.createTopic(true, "tt", "/topic/TT");

      server.getHornetQServerControl().addAddressSettings("jms.topic.TT",
                                                          "DLQ",
                                                          "DLQ",
                                                          -1,
                                                          false,
                                                          5,
                                                          1024 * 1024,
                                                          1024 * 10,
                                                          5,
                                                          5,
                                                          1,
                                                          1000,
                                                          0,
                                                          false,
                                                          "PAGE");

      HornetQJMSConnectionFactory cf = (HornetQJMSConnectionFactory)HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF,
                                                                                                                      new TransportConfiguration(INVM_CONNECTOR_FACTORY));

      Connection conn = cf.createConnection();
      conn.setClientID("tst");
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Topic topic = (Topic)context.lookup("/topic/TT");
      sess.createDurableSubscriber(topic, "t1");

      MessageProducer prod = sess.createProducer(topic);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      TextMessage txt = sess.createTextMessage("TST");
      prod.send(txt);

      PagingStore store = server.getPagingManager().getPageStore(new SimpleString("jms.topic.TT"));

      assertEquals(1024 * 1024, store.getMaxSize());
      assertEquals(10 * 1024, store.getPageSizeBytes());

      jmsServer.stop();

      server = createServer(true, config, PAGE_SIZE, -1, new HashMap<String, AddressSettings>());

      jmsServer = new JMSServerManagerImpl(server);
      context = new InVMNamingContext();
      jmsServer.setContext(context);
      jmsServer.start();

      AddressSettings settings = server.getAddressSettingsRepository().getMatch("jms.topic.TT");

      assertEquals(1024 * 1024, settings.getMaxSizeBytes());
      assertEquals(10 * 1024, settings.getPageSizeBytes());
      assertEquals(AddressFullMessagePolicy.PAGE, settings.getAddressFullMessagePolicy());

      store = server.getPagingManager().getPageStore(new SimpleString("TT"));

      conn.close();

      server.stop();

   }

   @Test
   public void testPagingOverCreatedDestinationQueues() throws Exception
   {

      Configuration config = createDefaultConfig();

      config.setJournalSyncNonTransactional(false);

      HornetQServer server = createServer(true, config, -1, -1, AddressFullMessagePolicy.BLOCK, new HashMap<String, AddressSettings>());

      JMSServerManagerImpl jmsServer = new JMSServerManagerImpl(server);
      InVMNamingContext context = new InVMNamingContext();
      jmsServer.setContext(context);
      jmsServer.start();

      server.getHornetQServerControl().addAddressSettings("jms.queue.Q1",
                                                          "DLQ",
                                                          "DLQ",
                                                          -1,
                                                          false,
                                                          5,
                                                          100 * 1024,
                                                          10 * 1024,
                                                          5,
                                                          5,
                                                          1,
                                                          1000,
                                                          0,
                                                          false,
                                                          "PAGE");

      jmsServer.createQueue(true, "Q1", null, true, "/queue/Q1");

      HornetQJMSConnectionFactory cf = (HornetQJMSConnectionFactory)HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF,
                                                                                                                      new TransportConfiguration(INVM_CONNECTOR_FACTORY));

      conn = cf.createConnection();
      conn.setClientID("tst");
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      javax.jms.Queue queue = (javax.jms.Queue)context.lookup("/queue/Q1");

      MessageProducer prod = sess.createProducer(queue);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      BytesMessage bmt = sess.createBytesMessage();

      bmt.writeBytes(new byte[1024]);

      for (int i = 0 ; i < 500; i++)
      {
         prod.send(bmt);
      }

      PagingStore store = server.getPagingManager().getPageStore(new SimpleString("jms.queue.Q1"));

      assertEquals(100 * 1024, store.getMaxSize());
      assertEquals(10 * 1024, store.getPageSizeBytes());
      assertEquals(AddressFullMessagePolicy.PAGE, store.getAddressFullMessagePolicy());

      jmsServer.stop();

      server = createServer(true, config, -1, -1, AddressFullMessagePolicy.BLOCK, new HashMap<String, AddressSettings>());

      jmsServer = new JMSServerManagerImpl(server);
      context = new InVMNamingContext();
      jmsServer.setContext(context);
      jmsServer.start();

      AddressSettings settings = server.getAddressSettingsRepository().getMatch("jms.queue.Q1");

      assertEquals(100 * 1024, settings.getMaxSizeBytes());
      assertEquals(10 * 1024, settings.getPageSizeBytes());
      assertEquals(AddressFullMessagePolicy.PAGE, settings.getAddressFullMessagePolicy());

      store = server.getPagingManager().getPageStore(new SimpleString("jms.queue.Q1"));
      assertEquals(100 * 1024, store.getMaxSize());
      assertEquals(10 * 1024, store.getPageSizeBytes());
      assertEquals(AddressFullMessagePolicy.PAGE, store.getAddressFullMessagePolicy());
   }
}
