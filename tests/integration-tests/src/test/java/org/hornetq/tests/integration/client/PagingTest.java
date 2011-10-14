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
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import junit.framework.Assert;
import junit.framework.AssertionFailedError;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.MessageHandler;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.DivertConfiguration;
import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.journal.PreparedTransactionInfo;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.core.journal.impl.NIOSequentialFileFactory;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.paging.Page;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.paging.cursor.impl.PagePositionImpl;
import org.hornetq.core.persistence.OperationContext;
import org.hornetq.core.persistence.impl.journal.OperationContextImpl;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.impl.HornetQServerImpl;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.UnitTestCase;

/**
 * A PagingTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Dec 5, 2008 8:25:58 PM
 *
 *
 */
public class PagingTest extends ServiceTestBase
{
   private ServerLocator locator;

   public PagingTest(final String name)
   {
      super(name);
   }

   public PagingTest()
   {
      super();
   }

   // Constants -----------------------------------------------------
   private static final Logger log = Logger.getLogger(PagingTest.class);

   private static final int RECEIVE_TIMEOUT = 5000;

   private static final int PAGE_MAX = 100 * 1024;

   private static final int PAGE_SIZE = 10 * 1024;

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   static final SimpleString ADDRESS = new SimpleString("SimpleAddress");

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      locator = createInVMNonHALocator();
   }

   @Override
   protected void tearDown() throws Exception
   {
      locator.close();

      super.tearDown();
   }

   public void testPreparePersistent() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      config.setJournalSyncNonTransactional(false);

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      server.start();

      final int messageSize = 1024;

      final int numberOfMessages = 5000;

      final int numberOfTX = 10;

      final int messagesPerTX = numberOfMessages / numberOfTX;

      try
      {
         ServerLocator locator = createInVMNonHALocator();

         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);
         locator.setBlockOnAcknowledge(true);

         ClientSessionFactory sf = locator.createSessionFactory();

         ClientSession session = sf.createSession(false, false, false);

         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         byte[] body = new byte[messageSize];

         ByteBuffer bb = ByteBuffer.wrap(body);

         for (int j = 1; j <= messageSize; j++)
         {
            bb.put(getSamplebyte(j));
         }

         for (int i = 0; i < numberOfMessages; i++)
         {
            message = session.createMessage(true);

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
         session = null;

         sf.close();
         locator.close();

         server.stop();

         server = createServer(true,
                               config,
                               PagingTest.PAGE_SIZE,
                               PagingTest.PAGE_MAX,
                               new HashMap<String, AddressSettings>());
         server.start();

         locator = createInVMNonHALocator();
         sf = locator.createSessionFactory();

         Queue queue = server.locateQueue(ADDRESS);

         assertEquals(numberOfMessages, queue.getMessageCount());

         LinkedList<Xid> xids = new LinkedList<Xid>();

         int msgReceived = 0;
         for (int i = 0; i < numberOfTX; i++)
         {
            ClientSession sessionConsumer = sf.createSession(true, false, false);
            Xid xid = newXID();
            xids.add(xid);
            sessionConsumer.start(xid, XAResource.TMNOFLAGS);
            sessionConsumer.start();
            ClientConsumer consumer = sessionConsumer.createConsumer(PagingTest.ADDRESS);
            for (int msgCount = 0; msgCount < messagesPerTX; msgCount++)
            {
               if (msgReceived == numberOfMessages)
               {
                  break;
               }
               msgReceived++;
               ClientMessage msg = consumer.receive(10000);
               assertNotNull(msg);
               msg.acknowledge();
            }
            sessionConsumer.end(xid, XAResource.TMSUCCESS);
            sessionConsumer.prepare(xid);
            sessionConsumer.close();
         }

         ClientSession sessionCheck = sf.createSession(true, true);

         ClientConsumer consumer = sessionCheck.createConsumer(PagingTest.ADDRESS);

         assertNull(consumer.receiveImmediate());

         sessionCheck.close();

         assertEquals(numberOfMessages, queue.getMessageCount());

         sf.close();
         locator.close();

         server.stop();

         server = createServer(true,
                               config,
                               PagingTest.PAGE_SIZE,
                               PagingTest.PAGE_MAX,
                               new HashMap<String, AddressSettings>());
         server.start();
         
         waitForServer(server);

         queue = server.locateQueue(ADDRESS);

         locator = createInVMNonHALocator();
         sf = locator.createSessionFactory();

         session = sf.createSession(true, false, false);

         consumer = session.createConsumer(PagingTest.ADDRESS);

         session.start();

         assertEquals(numberOfMessages, queue.getMessageCount());

         ClientMessage msg = consumer.receive(5000);
         if (msg != null)
         {
            System.out.println("Msg " + msg.getIntProperty("id"));

            while (true)
            {
               ClientMessage msg2 = consumer.receive(1000);
               if (msg2 == null)
               {
                  break;
               }
               System.out.println("Msg received again : " + msg2.getIntProperty("id"));

            }
         }
         assertNull(msg);

         for (int i = xids.size() - 1; i >= 0; i--)
         {
            Xid xid = xids.get(i);
            session.rollback(xid);
         }
         System.out.println("msgCount = " + queue.getMessageCount());

         xids.clear();

         session.close();

         session = sf.createSession(false, false, false);

         session.start();

         consumer = session.createConsumer(PagingTest.ADDRESS);

         for (int i = 0; i < numberOfMessages; i++)
         {
            msg = consumer.receive(1000);
            assertNotNull(msg);
            msg.acknowledge();

            assertEquals(i, msg.getIntProperty("id").intValue());

            if (i % 500 == 0)
            {
               session.commit();
            }
         }

         session.commit();

         session.close();

         sf.close();

         locator.close();

         assertEquals(0, queue.getMessageCount());

         long timeout = System.currentTimeMillis() + 5000;
         while (timeout > System.currentTimeMillis() && queue.getPageSubscription().getPagingStore().isPaging())
         {
            Thread.sleep(100);
         }
         assertFalse(queue.getPageSubscription().getPagingStore().isPaging());
      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   public void testReceiveImmediate() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      config.setJournalSyncNonTransactional(false);

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      server.start();

      final int messageSize = 1024;

      final int numberOfMessages = 1000;

      try
      {
         ServerLocator locator = createInVMNonHALocator();

         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);
         locator.setBlockOnAcknowledge(true);

         ClientSessionFactory sf = locator.createSessionFactory();

         ClientSession session = sf.createSession(false, false, false);

         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         byte[] body = new byte[messageSize];

         ByteBuffer bb = ByteBuffer.wrap(body);

         for (int j = 1; j <= messageSize; j++)
         {
            bb.put(getSamplebyte(j));
         }

         for (int i = 0; i < numberOfMessages; i++)
         {
            message = session.createMessage(true);

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

         session = null;

         sf.close();
         locator.close();

         server.stop();

         server = createServer(true,
                               config,
                               PagingTest.PAGE_SIZE,
                               PagingTest.PAGE_MAX,
                               new HashMap<String, AddressSettings>());
         server.start();

         locator = createInVMNonHALocator();
         sf = locator.createSessionFactory();

         Queue queue = server.locateQueue(ADDRESS);

         assertEquals(numberOfMessages, queue.getMessageCount());

         LinkedList<Xid> xids = new LinkedList<Xid>();

         int msgReceived = 0;
         ClientSession sessionConsumer = sf.createSession(false, false, false);
         sessionConsumer.start();
         ClientConsumer consumer = sessionConsumer.createConsumer(PagingTest.ADDRESS);
         for (int msgCount = 0; msgCount < numberOfMessages; msgCount++)
         {
            log.info("Received " + msgCount);
            msgReceived++;
            ClientMessage msg = consumer.receiveImmediate();
            if (msg == null)
            {
               log.info("It's null. leaving now");
               sessionConsumer.commit();
               fail("Didn't receive a message");
            }
            msg.acknowledge();

            if (msgCount % 5 == 0)
            {
               log.info("commit");
               sessionConsumer.commit();
            }
         }

         sessionConsumer.commit();

         sessionConsumer.close();

         sf.close();

         locator.close();

         assertEquals(0, queue.getMessageCount());

         long timeout = System.currentTimeMillis() + 5000;
         while (timeout > System.currentTimeMillis() && queue.getPageSubscription().getPagingStore().isPaging())
         {
            Thread.sleep(100);
         }
         assertFalse(queue.getPageSubscription().getPagingStore().isPaging());
      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   /**
    * This test will remove all the page directories during a restart, simulating a crash scenario. The server should still start after this
    */
   public void testDeletePhisicalPages() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();
      config.setPersistDeliveryCountBeforeDelivery(true);

      config.setJournalSyncNonTransactional(false);

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      server.start();

      final int messageSize = 1024;

      final int numberOfMessages = 1000;

      try
      {
         ServerLocator locator = createInVMNonHALocator();

         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);
         locator.setBlockOnAcknowledge(true);

         ClientSessionFactory sf = locator.createSessionFactory();

         ClientSession session = sf.createSession(false, false, false);

         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         byte[] body = new byte[messageSize];

         ByteBuffer bb = ByteBuffer.wrap(body);

         for (int j = 1; j <= messageSize; j++)
         {
            bb.put(getSamplebyte(j));
         }

         for (int i = 0; i < numberOfMessages; i++)
         {
            message = session.createMessage(true);

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

         session = null;

         sf.close();
         locator.close();

         server.stop();

         server = createServer(true,
                               config,
                               PagingTest.PAGE_SIZE,
                               PagingTest.PAGE_MAX,
                               new HashMap<String, AddressSettings>());
         server.start();

         locator = createInVMNonHALocator();
         sf = locator.createSessionFactory();

         Queue queue = server.locateQueue(ADDRESS);

         assertEquals(numberOfMessages, queue.getMessageCount());

         LinkedList<Xid> xids = new LinkedList<Xid>();

         int msgReceived = 0;
         ClientSession sessionConsumer = sf.createSession(false, false, false);
         sessionConsumer.start();
         ClientConsumer consumer = sessionConsumer.createConsumer(PagingTest.ADDRESS);
         for (int msgCount = 0; msgCount < numberOfMessages; msgCount++)
         {
            log.info("Received " + msgCount);
            msgReceived++;
            ClientMessage msg = consumer.receiveImmediate();
            if (msg == null)
            {
               log.info("It's null. leaving now");
               sessionConsumer.commit();
               fail("Didn't receive a message");
            }
            msg.acknowledge();

            if (msgCount % 5 == 0)
            {
               log.info("commit");
               sessionConsumer.commit();
            }
         }

         sessionConsumer.commit();

         sessionConsumer.close();

         sf.close();

         locator.close();

         assertEquals(0, queue.getMessageCount());

         long timeout = System.currentTimeMillis() + 5000;
         while (timeout > System.currentTimeMillis() && queue.getPageSubscription().getPagingStore().isPaging())
         {
            Thread.sleep(100);
         }
         assertFalse(queue.getPageSubscription().getPagingStore().isPaging());

         server.stop();

         // Deleting the paging data. Simulating a failure
         // a dumb user, or anything that will remove the data
         deleteDirectory(new File(getPageDir()));

         server = createServer(true,
                               config,
                               PagingTest.PAGE_SIZE,
                               PagingTest.PAGE_MAX,
                               new HashMap<String, AddressSettings>());
         server.start();
         
         
         locator = createInVMNonHALocator();
         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);
         locator.setBlockOnAcknowledge(true);

         sf = locator.createSessionFactory();

         queue = server.locateQueue(ADDRESS);

         sf = locator.createSessionFactory();
         session = sf.createSession(false, false, false);

         producer = session.createProducer(PagingTest.ADDRESS);
         
         for (int i = 0; i < numberOfMessages; i++)
         {
            message = session.createMessage(true);

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
         
         server.stop();

         server = createServer(true,
                               config,
                               PagingTest.PAGE_SIZE,
                               PagingTest.PAGE_MAX,
                               new HashMap<String, AddressSettings>());
         server.start();

         locator = createInVMNonHALocator();
         sf = locator.createSessionFactory();

         queue = server.locateQueue(ADDRESS);

        // assertEquals(numberOfMessages, queue.getMessageCount());

         xids = new LinkedList<Xid>();

         msgReceived = 0;
         sessionConsumer = sf.createSession(false, false, false);
         sessionConsumer.start();
         consumer = sessionConsumer.createConsumer(PagingTest.ADDRESS);
         for (int msgCount = 0; msgCount < numberOfMessages; msgCount++)
         {
            log.info("Received " + msgCount);
            msgReceived++;
            ClientMessage msg = consumer.receiveImmediate();
            if (msg == null)
            {
               log.info("It's null. leaving now");
               sessionConsumer.commit();
               fail("Didn't receive a message");
            }
            msg.acknowledge();

            if (msgCount % 5 == 0)
            {
               log.info("commit");
               sessionConsumer.commit();
            }
         }

         sessionConsumer.commit();

         sessionConsumer.close();


      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   public void testMissingTXEverythingAcked() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      config.setJournalSyncNonTransactional(false);

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      server.start();

      final int messageSize = 1024;

      final int numberOfMessages = 5000;

      final int numberOfTX = 10;

      final int messagesPerTX = numberOfMessages / numberOfTX;

      try
      {
         ServerLocator locator = createInVMNonHALocator();

         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);
         locator.setBlockOnAcknowledge(true);

         ClientSessionFactory sf = locator.createSessionFactory();

         ClientSession session = sf.createSession(false, false, false);

         session.createQueue(ADDRESS.toString(), "q1", true);

         session.createQueue(ADDRESS.toString(), "q2", true);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         byte[] body = new byte[messageSize];

         ByteBuffer bb = ByteBuffer.wrap(body);

         for (int j = 1; j <= messageSize; j++)
         {
            bb.put(getSamplebyte(j));
         }

         for (int i = 0; i < numberOfMessages; i++)
         {
            message = session.createMessage(true);

            HornetQBuffer bodyLocal = message.getBodyBuffer();

            bodyLocal.writeBytes(body);

            message.putIntProperty(new SimpleString("id"), i);

            producer.send(message);
            if (i % messagesPerTX == 0)
            {
               session.commit();
            }
         }
         session.commit();
         session.close();
      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

      ArrayList<RecordInfo> records = new ArrayList<RecordInfo>();

      List<PreparedTransactionInfo> list = new ArrayList<PreparedTransactionInfo>();

      JournalImpl jrn = new JournalImpl(config.getJournalFileSize(),
                                        2,
                                        0,
                                        0,
                                        new NIOSequentialFileFactory(getJournalDir()),
                                        "hornetq-data",
                                        "hq",
                                        1);
      jrn.start();
      jrn.load(records, list, null);

      // Delete everything from the journal
      for (RecordInfo info : records)
      {
         if (!info.isUpdate)
         {
            jrn.appendDeleteRecord(info.id, false);
         }
      }

      jrn.stop();

      server = createServer(true,
                            config,
                            PagingTest.PAGE_SIZE,
                            PagingTest.PAGE_MAX,
                            new HashMap<String, AddressSettings>());

      server.start();

      Page pg = server.getPagingManager().getPageStore(ADDRESS).getCurrentPage();

      pg.open();

      List<PagedMessage> msgs = pg.read(server.getStorageManager());

      pg.close();

      long queues[] = new long[] { server.locateQueue(new SimpleString("q1")).getID() };

      for (long q : queues)
      {
         for (int i = 0; i < msgs.size(); i++)
         {
            server.getStorageManager().storeCursorAcknowledge(q, new PagePositionImpl(pg.getPageId(), i));
         }
      }

      server.stop();

      ServerLocator locator = createInVMNonHALocator();

      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setBlockOnAcknowledge(true);

      server = createServer(true,
                            config,
                            PagingTest.PAGE_SIZE,
                            PagingTest.PAGE_MAX,
                            new HashMap<String, AddressSettings>());

      server.start();

      ClientSessionFactory csf = locator.createSessionFactory();

      ClientSession sess = csf.createSession();

      sess.start();

      ClientConsumer cons = sess.createConsumer("q1");

      assertNull(cons.receive(500));

      Thread.sleep(5000);

      ClientConsumer cons2 = sess.createConsumer("q2");
      assertNull(cons2.receive(500));

      long timeout = System.currentTimeMillis() + 5000;

      while (System.currentTimeMillis() < timeout && server.getPagingManager().getPageStore(ADDRESS).isPaging())
      {
         Thread.sleep(100);
      }

      assertFalse(server.getPagingManager().getPageStore(ADDRESS).isPaging());

      sess.close();

      locator.close();

      server.stop();
   }

   public void testMissingTXEverythingAcked2() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      config.setJournalSyncNonTransactional(false);

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      server.start();

      final int messageSize = 1024;

      final int numberOfMessages = 6;

      final int numberOfTX = 2;

      final int messagesPerTX = numberOfMessages / numberOfTX;

      try
      {
         ServerLocator locator = createInVMNonHALocator();

         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);
         locator.setBlockOnAcknowledge(true);

         ClientSessionFactory sf = locator.createSessionFactory();

         ClientSession session = sf.createSession(false, false, false);

         session.createQueue(ADDRESS.toString(), "q1", true);

         session.createQueue(ADDRESS.toString(), "q2", true);

         server.getPagingManager().getPageStore(ADDRESS).startPaging();

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         byte[] body = new byte[messageSize];

         ByteBuffer bb = ByteBuffer.wrap(body);

         for (int j = 1; j <= messageSize; j++)
         {
            bb.put(getSamplebyte(j));
         }

         for (int i = 0; i < numberOfMessages; i++)
         {
            message = session.createMessage(true);

            HornetQBuffer bodyLocal = message.getBodyBuffer();

            bodyLocal.writeBytes(body);

            message.putStringProperty("id", "str-" + i);

            producer.send(message);
            if ((i + 1) % messagesPerTX == 0)
            {
               session.commit();
            }
         }
         session.commit();

         session.start();

         for (int i = 1; i <= 2; i++)
         {
            ClientConsumer cons = session.createConsumer("q" + i);

            for (int j = 0; j < 3; j++)
            {
               ClientMessage msg = cons.receive(5000);

               assertNotNull(msg);

               assertEquals("str-" + j, msg.getStringProperty("id"));

               msg.acknowledge();
            }

            session.commit();

         }

         session.close();
      }
      finally
      {
         locator.close();
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

      server = createServer(true,
                            config,
                            PagingTest.PAGE_SIZE,
                            PagingTest.PAGE_MAX,
                            new HashMap<String, AddressSettings>());

      server.start();

      ServerLocator locator = createInVMNonHALocator();

      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setBlockOnAcknowledge(true);

      ClientSessionFactory csf = locator.createSessionFactory();

      ClientSession session = csf.createSession();

      session.start();

      for (int i = 1; i <= 2; i++)
      {
         ClientConsumer cons = session.createConsumer("q" + i);

         for (int j = 3; j < 6; j++)
         {
            ClientMessage msg = cons.receive(5000);

            assertNotNull(msg);

            assertEquals("str-" + j, msg.getStringProperty("id"));

            msg.acknowledge();
         }

         session.commit();
         assertNull(cons.receive(500));

      }

      session.close();

      long timeout = System.currentTimeMillis() + 5000;

      while (System.currentTimeMillis() < timeout && server.getPagingManager().getPageStore(ADDRESS).isPaging())
      {
         Thread.sleep(100);
      }

      locator.close();

      server.stop();
   }

   public void testTwoQueuesOneNoRouting() throws Exception
   {
      boolean persistentMessages = true;

      clearData();

      Configuration config = createDefaultConfig();

      config.setJournalSyncNonTransactional(false);

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      server.start();

      final int messageSize = 1024;

      final int numberOfMessages = 1000;

      try
      {
         ServerLocator locator = createInVMNonHALocator();

         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);
         locator.setBlockOnAcknowledge(true);

         ClientSessionFactory sf = locator.createSessionFactory();

         ClientSession session = sf.createSession(false, false, false);

         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);
         session.createQueue(PagingTest.ADDRESS,
                             PagingTest.ADDRESS.concat("-invalid"),
                             new SimpleString(HornetQServerImpl.GENERIC_IGNORED_FILTER),
                             true);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         byte[] body = new byte[messageSize];

         for (int i = 0; i < numberOfMessages; i++)
         {
            message = session.createMessage(persistentMessages);

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

         session.start();

         ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

         for (int i = 0; i < numberOfMessages; i++)
         {
            message = consumer.receive(5000);
            assertNotNull(message);
            message.acknowledge();

            assertEquals(i, message.getIntProperty("id").intValue());
            if (i % 1000 == 0)
            {
               session.commit();
            }
         }

         session.commit();

         session.commit();

         session.commit();

         PagingStore store = server.getPagingManager().getPageStore(ADDRESS);
         store.getCursorProvider().cleanup();

         long timeout = System.currentTimeMillis() + 5000;
         while (store.isPaging() && timeout > System.currentTimeMillis())
         {
            Thread.sleep(100);
         }

         // It's async, so need to wait a bit for it happening
         assertFalse(server.getPagingManager().getPageStore(ADDRESS).isPaging());

         sf.close();

         locator.close();
      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   public void testSendReceivePagingPersistent() throws Exception
   {
      internaltestSendReceivePaging(true);
   }

   public void testSendReceivePagingNonPersistent() throws Exception
   {
      internaltestSendReceivePaging(false);
   }

   public void testWithDiverts() throws Exception
   {
      internalMultiQueuesTest(true);
   }

   public void testWithMultiQueues() throws Exception
   {
      internalMultiQueuesTest(false);
   }

   public void internalMultiQueuesTest(final boolean divert) throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      config.setJournalSyncNonTransactional(false);

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      if (divert)
      {
         DivertConfiguration divert1 = new DivertConfiguration("dv1",
                                                               "nm1",
                                                               PagingTest.ADDRESS.toString(),
                                                               PagingTest.ADDRESS.toString() + "-1",
                                                               true,
                                                               null,
                                                               null);

         DivertConfiguration divert2 = new DivertConfiguration("dv2",
                                                               "nm2",
                                                               PagingTest.ADDRESS.toString(),
                                                               PagingTest.ADDRESS.toString() + "-2",
                                                               true,
                                                               null,
                                                               null);

         ArrayList<DivertConfiguration> divertList = new ArrayList<DivertConfiguration>();
         divertList.add(divert1);
         divertList.add(divert2);

         config.setDivertConfigurations(divertList);
      }

      server.start();

      final int messageSize = 1024;

      final int numberOfMessages = 3000;

      final byte[] body = new byte[messageSize];

      ByteBuffer bb = ByteBuffer.wrap(body);

      for (int j = 1; j <= messageSize; j++)
      {
         bb.put(getSamplebyte(j));
      }
      
      final AtomicBoolean running = new AtomicBoolean(true);
      
      class TCount extends Thread
      {
         Queue queue;
         
         TCount(Queue queue)
         {
            this.queue = queue;
         }
         @Override
         public void run()
         {
            try
            {
               while (running.get())
               {
                 // log.info("Message count = " + queue.getMessageCount() + " on queue " + queue.getName());
                  queue.getMessagesAdded();
                  queue.getMessageCount();
                  //log.info("Message added = " + queue.getMessagesAdded() + " on queue " + queue.getName());
                  Thread.sleep(10);
               }
            }
            catch (InterruptedException e)
            {
               log.info("Thread interrupted");
            }
         }
      };
      
      TCount tcount1 = null;
      TCount tcount2 = null;
      

      try
      {
         {
            ServerLocator locator = createInVMNonHALocator();

            locator.setBlockOnNonDurableSend(true);
            locator.setBlockOnDurableSend(true);
            locator.setBlockOnAcknowledge(true);

            ClientSessionFactory sf = locator.createSessionFactory();

            ClientSession session = sf.createSession(false, false, false);

            if (divert)
            {
               session.createQueue(PagingTest.ADDRESS + "-1", PagingTest.ADDRESS + "-1", null, true);

               session.createQueue(PagingTest.ADDRESS + "-2", PagingTest.ADDRESS + "-2", null, true);
            }
            else
            {
               session.createQueue(PagingTest.ADDRESS.toString(), PagingTest.ADDRESS + "-1", null, true);

               session.createQueue(PagingTest.ADDRESS.toString(), PagingTest.ADDRESS + "-2", null, true);
            }
            
            
            ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

            ClientMessage message = null;

            for (int i = 0; i < numberOfMessages; i++)
            {
               if (i % 500 == 0)
               {
                  session.commit();
               }
               message = session.createMessage(true);

               HornetQBuffer bodyLocal = message.getBodyBuffer();

               bodyLocal.writeBytes(body);

               message.putIntProperty(new SimpleString("id"), i);

               producer.send(message);
            }

            session.commit();

            session.close();

            server.stop();

            sf.close();
            locator.close();
         }

         server = createServer(true,
                               config,
                               PagingTest.PAGE_SIZE,
                               PagingTest.PAGE_MAX,
                               new HashMap<String, AddressSettings>());
         server.start();
         
         Queue queue1 = server.locateQueue(PagingTest.ADDRESS.concat("-1"));
         
         Queue queue2 = server.locateQueue(PagingTest.ADDRESS.concat("-2"));
         
         assertNotNull(queue1);
         
         assertNotNull(queue2);
         
         assertNotSame(queue1, queue2);

         tcount1 = new TCount(queue1);
         
         tcount2 = new TCount(queue2);
         
         tcount1.start();
         tcount2.start();

         ServerLocator locator = createInVMNonHALocator();
         final ClientSessionFactory sf2 = locator.createSessionFactory();

         final AtomicInteger errors = new AtomicInteger(0);

         Thread threads[] = new Thread[2];

         for (int start = 1; start <= 2; start++)
         {

            final String addressToSubscribe = PagingTest.ADDRESS + "-" + start;

            threads[start - 1] = new Thread()
            {
               @Override
               public void run()
               {
                  try
                  {
                     ClientSession session = sf2.createSession(null, null, false, true, true, false, 0);

                     ClientConsumer consumer = session.createConsumer(addressToSubscribe);

                     session.start();

                     for (int i = 0; i < numberOfMessages; i++)
                     {
                        ClientMessage message2 = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

                        Assert.assertNotNull(message2);

                        Assert.assertEquals(i, message2.getIntProperty("id").intValue());

                        message2.acknowledge();

                        Assert.assertNotNull(message2);

                        if (i % 100 == 0)
                        {
                           if (i % 5000 == 0)
                           {
                              log.info(addressToSubscribe + " consumed " + i + " messages");
                           }
                           session.commit();
                        }

                        try
                        {
                           assertBodiesEqual(body, message2.getBodyBuffer());
                        }
                        catch (AssertionFailedError e)
                        {
                           PagingTest.log.info("Expected buffer:" + UnitTestCase.dumbBytesHex(body, 40));
                           PagingTest.log.info("Arriving buffer:" + UnitTestCase.dumbBytesHex(message2.getBodyBuffer()
                                                                                                      .toByteBuffer()
                                                                                                      .array(), 40));
                           throw e;
                        }
                     }

                     session.commit();

                     consumer.close();

                     session.close();
                  }
                  catch (Throwable e)
                  {
                     e.printStackTrace();
                     errors.incrementAndGet();
                  }

               }
            };
         }

         for (int i = 0; i < 2; i++)
         {
            threads[i].start();
         }

         for (int i = 0; i < 2; i++)
         {
            threads[i].join();
         }

         sf2.close();
         locator.close();

         assertEquals(0, errors.get());

         for (int i = 0; i < 20 && server.getPostOffice().getPagingManager().getTransactions().size() != 0; i++)
         {
            if (server.getPostOffice().getPagingManager().getTransactions().size() != 0)
            {
               // The delete may be asynchronous, giving some time case it eventually happen asynchronously
               Thread.sleep(500);
            }
         }

         assertEquals(0, server.getPostOffice().getPagingManager().getTransactions().size());

      }
      finally
      {
         running.set(false);
         
         if (tcount1 != null)
         {
            tcount1.interrupt();
            tcount1.join();
         }
         
         if (tcount2 != null)
         {
            tcount2.interrupt();
            tcount2.join();
         }
         
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   public void testMultiQueuesNonPersistentAndPersistent() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      config.setJournalSyncNonTransactional(false);

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      server.start();

      final int messageSize = 1024;

      final int numberOfMessages = 3000;

      final byte[] body = new byte[messageSize];

      ByteBuffer bb = ByteBuffer.wrap(body);

      for (int j = 1; j <= messageSize; j++)
      {
         bb.put(getSamplebyte(j));
      }

      try
      {
         {
            ServerLocator locator = createInVMNonHALocator();

            locator.setBlockOnNonDurableSend(true);
            locator.setBlockOnDurableSend(true);
            locator.setBlockOnAcknowledge(true);

            ClientSessionFactory sf = locator.createSessionFactory();

            ClientSession session = sf.createSession(false, false, false);

            session.createQueue(PagingTest.ADDRESS.toString(), PagingTest.ADDRESS + "-1", null, true);

            session.createQueue(PagingTest.ADDRESS.toString(), PagingTest.ADDRESS + "-2", null, false);

            ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

            ClientMessage message = null;

            for (int i = 0; i < numberOfMessages; i++)
            {
               if (i % 500 == 0)
               {
                  session.commit();
               }
               message = session.createMessage(true);

               HornetQBuffer bodyLocal = message.getBodyBuffer();

               bodyLocal.writeBytes(body);

               message.putIntProperty(new SimpleString("id"), i);

               producer.send(message);
            }

            session.commit();

            session.close();

            server.stop();

            sf.close();
            locator.close();
         }

         server = createServer(true,
                               config,
                               PagingTest.PAGE_SIZE,
                               PagingTest.PAGE_MAX,
                               new HashMap<String, AddressSettings>());
         server.start();

         ServerLocator locator = createInVMNonHALocator();
         final ClientSessionFactory sf2 = locator.createSessionFactory();

         final AtomicInteger errors = new AtomicInteger(0);

         Thread t = new Thread()
         {
            @Override
            public void run()
            {
               try
               {
                  ClientSession session = sf2.createSession(null, null, false, true, true, false, 0);

                  ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS + "-1");

                  session.start();

                  for (int i = 0; i < numberOfMessages; i++)
                  {
                     ClientMessage message2 = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

                     Assert.assertNotNull(message2);

                     Assert.assertEquals(i, message2.getIntProperty("id").intValue());

                     message2.acknowledge();

                     Assert.assertNotNull(message2);

                     if (i % 1000 == 0)
                        session.commit();

                     try
                     {
                        assertBodiesEqual(body, message2.getBodyBuffer());
                     }
                     catch (AssertionFailedError e)
                     {
                        PagingTest.log.info("Expected buffer:" + UnitTestCase.dumbBytesHex(body, 40));
                        PagingTest.log.info("Arriving buffer:" + UnitTestCase.dumbBytesHex(message2.getBodyBuffer()
                                                                                                   .toByteBuffer()
                                                                                                   .array(), 40));
                        throw e;
                     }
                  }

                  session.commit();

                  consumer.close();

                  session.close();
               }
               catch (Throwable e)
               {
                  e.printStackTrace();
                  errors.incrementAndGet();
               }

            }
         };

         t.start();
         t.join();

         assertEquals(0, errors.get());

         for (int i = 0; i < 20 && server.getPostOffice().getPagingManager().getPageStore(ADDRESS).isPaging(); i++)
         {
            // The delete may be asynchronous, giving some time case it eventually happen asynchronously
            Thread.sleep(500);
         }

         assertFalse(server.getPostOffice().getPagingManager().getPageStore(ADDRESS).isPaging());

         for (int i = 0; i < 20 && server.getPostOffice().getPagingManager().getTransactions().size() != 0; i++)
         {
            // The delete may be asynchronous, giving some time case it eventually happen asynchronously
            Thread.sleep(500);
         }

         assertEquals(0, server.getPostOffice().getPagingManager().getTransactions().size());

      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   private void internaltestSendReceivePaging(final boolean persistentMessages) throws Exception
   {

      System.out.println("PageDir:" + getPageDir());
      clearData();

      Configuration config = createDefaultConfig();

      config.setJournalSyncNonTransactional(false);

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      server.start();

      final int numberOfIntegers = 256;

      final int numberOfMessages = 1000;

      try
      {
         ServerLocator locator = createInVMNonHALocator();

         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);
         locator.setBlockOnAcknowledge(true);

         ClientSessionFactory sf = locator.createSessionFactory();

         ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         byte[] body = new byte[numberOfIntegers * 4];

         ByteBuffer bb = ByteBuffer.wrap(body);

         for (int j = 1; j <= numberOfIntegers; j++)
         {
            bb.putInt(j);
         }

         for (int i = 0; i < numberOfMessages; i++)
         {
            message = session.createMessage(persistentMessages);

            HornetQBuffer bodyLocal = message.getBodyBuffer();

            bodyLocal.writeBytes(body);

            message.putIntProperty(new SimpleString("id"), i);

            producer.send(message);
         }

         session.close();
         sf.close();
         locator.close();

         if (persistentMessages)
         {
            server.stop();

            server = createServer(true,
                                  config,
                                  PagingTest.PAGE_SIZE,
                                  PagingTest.PAGE_MAX,
                                  new HashMap<String, AddressSettings>());
            server.start();
         }

         locator = createInVMNonHALocator();
         sf = locator.createSessionFactory();

         session = sf.createSession(null, null, false, true, true, false, 0);

         ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

         session.start();

         for (int i = 0; i < numberOfMessages; i++)
         {
            ClientMessage message2 = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

            Assert.assertNotNull(message2);

            Assert.assertEquals(i, message2.getIntProperty("id").intValue());

            message2.acknowledge();

            Assert.assertNotNull(message2);

            if (i % 1000 == 0)
               session.commit();

            try
            {
               assertBodiesEqual(body, message2.getBodyBuffer());
            }
            catch (AssertionFailedError e)
            {
               PagingTest.log.info("Expected buffer:" + UnitTestCase.dumbBytesHex(body, 40));
               PagingTest.log.info("Arriving buffer:" + UnitTestCase.dumbBytesHex(message2.getBodyBuffer()
                                                                                          .toByteBuffer()
                                                                                          .array(), 40));
               throw e;
            }
         }

         consumer.close();

         session.close();

         sf.close();

         locator.close();
      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   private void assertBodiesEqual(final byte[] body, final HornetQBuffer buffer)
   {
      byte[] other = new byte[body.length];

      buffer.readBytes(other);

      UnitTestCase.assertEqualsByteArrays(body, other);
   }

   /**
    * - Make a destination in page mode
    * - Add stuff to a transaction
    * - Consume the entire destination (not in page mode any more)
    * - Add stuff to a transaction again
    * - Check order
    * 
    */
   public void testDepageDuringTransaction() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      server.start();

      final int messageSize = 1024; // 1k

      try
      {
         ServerLocator locator = createInVMNonHALocator();
         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);
         locator.setBlockOnAcknowledge(true);

         ClientSessionFactory sf = locator.createSessionFactory();

         ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         byte[] body = new byte[messageSize];
         // HornetQBuffer bodyLocal = HornetQChannelBuffers.buffer(DataConstants.SIZE_INT * numberOfIntegers);

         ClientMessage message = null;

         int numberOfMessages = 0;
         while (true)
         {
            message = session.createMessage(true);
            message.getBodyBuffer().writeBytes(body);

            // Stop sending message as soon as we start paging
            if (server.getPostOffice().getPagingManager().getPageStore(PagingTest.ADDRESS).isPaging())
            {
               break;
            }
            numberOfMessages++;

            producer.send(message);
         }

         Assert.assertTrue(server.getPostOffice().getPagingManager().getPageStore(PagingTest.ADDRESS).isPaging());

         session.start();

         ClientSession sessionTransacted = sf.createSession(null, null, false, false, false, false, 0);

         ClientProducer producerTransacted = sessionTransacted.createProducer(PagingTest.ADDRESS);

         for (int i = 0; i < 10; i++)
         {
            message = session.createMessage(true);
            message.getBodyBuffer().writeBytes(body);
            message.putIntProperty(new SimpleString("id"), i);

            // Consume messages to force an eventual out of order delivery
            if (i == 5)
            {
               ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);
               for (int j = 0; j < numberOfMessages; j++)
               {
                  ClientMessage msg = consumer.receive(PagingTest.RECEIVE_TIMEOUT);
                  msg.acknowledge();
                  Assert.assertNotNull(msg);
               }

               Assert.assertNull(consumer.receiveImmediate());
               consumer.close();
            }

            Integer messageID = (Integer)message.getObjectProperty(new SimpleString("id"));
            Assert.assertNotNull(messageID);
            Assert.assertEquals(messageID.intValue(), i);

            producerTransacted.send(message);
         }

         ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

         Assert.assertNull(consumer.receiveImmediate());

         sessionTransacted.commit();

         sessionTransacted.close();

         for (int i = 0; i < 10; i++)
         {
            message = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

            Assert.assertNotNull(message);

            Integer messageID = (Integer)message.getObjectProperty(new SimpleString("id"));

            Assert.assertNotNull(messageID);
            Assert.assertEquals("message received out of order", messageID.intValue(), i);

            message.acknowledge();
         }

         Assert.assertNull(consumer.receiveImmediate());

         consumer.close();

         session.close();

         sf.close();

         locator.close();
      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   /**
    * - Make a destination in page mode
    * - Add stuff to a transaction
    * - Consume the entire destination (not in page mode any more)
    * - Add stuff to a transaction again
    * - Check order
    * 
    *  Test under discussion at : http://community.jboss.org/thread/154061?tstart=0
    * 
    */
   public void testDepageDuringTransaction2() throws Exception
   {
      boolean IS_DURABLE_MESSAGE = true;
      clearData();

      Configuration config = createDefaultConfig();

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      server.start();

      final int messageSize = 1024; // 1k

      try
      {
         ServerLocator locator = createInVMNonHALocator();
         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);
         locator.setBlockOnAcknowledge(true);

         ClientSessionFactory sf = locator.createSessionFactory();

         byte[] body = new byte[messageSize];

         ClientSession sessionTransacted = sf.createSession(null, null, false, false, false, false, 0);
         ClientProducer producerTransacted = sessionTransacted.createProducer(PagingTest.ADDRESS);

         ClientSession session = sf.createSession(null, null, false, true, true, false, 0);
         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

         ClientMessage firstMessage = sessionTransacted.createMessage(IS_DURABLE_MESSAGE);
         firstMessage.getBodyBuffer().writeBytes(body);
         firstMessage.putIntProperty(new SimpleString("id"), 0);

         producerTransacted.send(firstMessage);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         int numberOfMessages = 0;
         while (true)
         {
            message = session.createMessage(IS_DURABLE_MESSAGE);
            message.getBodyBuffer().writeBytes(body);
            message.putIntProperty("id", numberOfMessages);
            message.putBooleanProperty("new", false);

            // Stop sending message as soon as we start paging
            if (server.getPostOffice().getPagingManager().getPageStore(PagingTest.ADDRESS).isPaging())
            {
               break;
            }
            numberOfMessages++;

            producer.send(message);
         }

         Assert.assertTrue(server.getPostOffice().getPagingManager().getPageStore(PagingTest.ADDRESS).isPaging());

         session.start();

         for (int i = 1; i < 10; i++)
         {
            message = session.createMessage(true);
            message.getBodyBuffer().writeBytes(body);
            message.putIntProperty(new SimpleString("id"), i);

            // Consume messages to force an eventual out of order delivery
            if (i == 5)
            {
               ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);
               for (int j = 0; j < numberOfMessages; j++)
               {
                  ClientMessage msg = consumer.receive(PagingTest.RECEIVE_TIMEOUT);
                  msg.acknowledge();
                  assertEquals(j, msg.getIntProperty("id").intValue());
                  assertFalse(msg.getBooleanProperty("new"));
                  Assert.assertNotNull(msg);
               }

               ClientMessage msgReceived = consumer.receiveImmediate();

               if (msgReceived != null)
               {
                  System.out.println("new = " + msgReceived.getBooleanProperty("new") +
                                     " id = " +
                                     msgReceived.getIntProperty("id"));
               }

               Assert.assertNull(msgReceived);
               consumer.close();
            }

            Integer messageID = (Integer)message.getObjectProperty(new SimpleString("id"));
            Assert.assertNotNull(messageID);
            Assert.assertEquals(messageID.intValue(), i);

            producerTransacted.send(message);
         }

         ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

         Assert.assertNull(consumer.receiveImmediate());

         sessionTransacted.commit();

         sessionTransacted.close();

         for (int i = 0; i < 10; i++)
         {
            message = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

            Assert.assertNotNull(message);

            Integer messageID = (Integer)message.getObjectProperty(new SimpleString("id"));

            // System.out.println(messageID);
            Assert.assertNotNull(messageID);
            Assert.assertEquals("message received out of order", i, messageID.intValue());

            message.acknowledge();
         }

         Assert.assertNull(consumer.receiveImmediate());

         consumer.close();

         session.close();

         sf.close();

         locator.close();
      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   public void testDepageDuringTransaction3() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      server.start();

      final int messageSize = 1024; // 1k

      try
      {
         ServerLocator locator = createInVMNonHALocator();
         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);
         locator.setBlockOnAcknowledge(true);

         ClientSessionFactory sf = locator.createSessionFactory();

         byte[] body = new byte[messageSize];

         ClientSession sessionTransacted = sf.createSession(null, null, false, false, false, false, 0);
         ClientProducer producerTransacted = sessionTransacted.createProducer(PagingTest.ADDRESS);

         ClientSession sessionNonTX = sf.createSession(true, true, 0);
         sessionNonTX.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

         ClientProducer producerNonTransacted = sessionNonTX.createProducer(PagingTest.ADDRESS);

         sessionNonTX.start();

         for (int i = 0; i < 50; i++)
         {
            // System.out.println("Sending " + i);
            ClientMessage message = sessionNonTX.createMessage(true);
            message.getBodyBuffer().writeBytes(body);
            message.putIntProperty(new SimpleString("id"), i);
            message.putStringProperty(new SimpleString("tst"), new SimpleString("i=" + i));

            producerTransacted.send(message);

            if (i % 2 == 0)
            {
               // System.out.println("Sending 20 msgs to make it page");
               for (int j = 0; j < 20; j++)
               {
                  ClientMessage msgSend = sessionNonTX.createMessage(true);
                  msgSend.putStringProperty(new SimpleString("tst"), new SimpleString("i=" + i + ", j=" + j));
                  msgSend.getBodyBuffer().writeBytes(new byte[10 * 1024]);
                  producerNonTransacted.send(msgSend);
               }
               assertTrue(server.getPostOffice().getPagingManager().getPageStore(PagingTest.ADDRESS).isPaging());
            }
            else
            {
               // System.out.println("Consuming 20 msgs to make it page");
               ClientConsumer consumer = sessionNonTX.createConsumer(PagingTest.ADDRESS);
               for (int j = 0; j < 20; j++)
               {
                  ClientMessage msgReceived = consumer.receive(10000);
                  assertNotNull(msgReceived);
                  msgReceived.acknowledge();
               }
               consumer.close();
            }
         }

         ClientConsumer consumerNonTX = sessionNonTX.createConsumer(PagingTest.ADDRESS);
         while (true)
         {
            ClientMessage msgReceived = consumerNonTX.receive(1000);
            if (msgReceived == null)
            {
               break;
            }
            msgReceived.acknowledge();
         }
         consumerNonTX.close();

         ClientConsumer consumer = sessionNonTX.createConsumer(PagingTest.ADDRESS);

         Assert.assertNull(consumer.receiveImmediate());

         sessionTransacted.commit();

         sessionTransacted.close();

         for (int i = 0; i < 50; i++)
         {
            ClientMessage message = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

            Assert.assertNotNull(message);

            Integer messageID = (Integer)message.getObjectProperty(new SimpleString("id"));

            Assert.assertNotNull("MessageID", messageID);
            Assert.assertEquals("message received out of order " + messageID, i, messageID.intValue());

            message.acknowledge();
         }

         Assert.assertNull(consumer.receiveImmediate());

         consumer.close();

         sessionNonTX.close();

         sf.close();

         locator.close();
      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   public void testDepageDuringTransaction4() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      server.getConfiguration().setJournalSyncNonTransactional(false);
      server.getConfiguration().setJournalSyncTransactional(false);

      server.start();

      final AtomicInteger errors = new AtomicInteger(0);

      final int messageSize = 1024; // 1k
      final int numberOfMessages = 10000;

      ServerLocator locator = createInVMNonHALocator();

      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setBlockOnAcknowledge(false);

      try
      {

         final ClientSessionFactory sf = locator.createSessionFactory();

         final byte[] body = new byte[messageSize];

         Thread producerThread = new Thread()
         {
            @Override
            public void run()
            {
               ClientSession sessionProducer = null;
               try
               {
                  sessionProducer = sf.createSession(false, false);
                  ClientProducer producer = sessionProducer.createProducer(ADDRESS);

                  for (int i = 0; i < numberOfMessages; i++)
                  {
                     ClientMessage msg = sessionProducer.createMessage(true);
                     msg.getBodyBuffer().writeBytes(body);
                     msg.putIntProperty("count", i);
                     producer.send(msg);

                     if (i % 100 == 0 && i != 0)
                     {
                        sessionProducer.commit();
                        // Thread.sleep(500);
                     }
                  }

                  sessionProducer.commit();

                  System.out.println("Producer gone");

               }
               catch (Throwable e)
               {
                  e.printStackTrace(); // >> junit report
                  errors.incrementAndGet();
               }
               finally
               {
                  try
                  {
                     if (sessionProducer != null)
                     {
                        sessionProducer.close();
                     }
                  }
                  catch (Throwable e)
                  {
                     e.printStackTrace();
                     errors.incrementAndGet();
                  }
               }
            }
         };

         ClientSession session = sf.createSession(true, true, 0);
         session.start();
         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

         producerThread.start();

         ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

         for (int i = 0; i < numberOfMessages; i++)
         {
            ClientMessage msg = consumer.receive(5000);
            assertNotNull(msg);
            assertEquals(i, msg.getIntProperty("count").intValue());
            msg.acknowledge();
            if (i > 0 && i % 10 == 0)
            {
               session.commit();
            }
         }
         session.commit();

         session.close();

         producerThread.join();

         locator.close();

         sf.close();

         assertEquals(0, errors.get());
      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }
   }

   public void testOrderingNonTX() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      final HornetQServer server = createServer(true,
                                                config,
                                                PagingTest.PAGE_SIZE,
                                                PagingTest.PAGE_SIZE * 2,
                                                new HashMap<String, AddressSettings>());

      server.getConfiguration().setJournalSyncNonTransactional(false);
      server.getConfiguration().setJournalSyncTransactional(false);

      server.start();

      final AtomicInteger errors = new AtomicInteger(0);

      final int messageSize = 1024;
      final int numberOfMessages = 2000;

      try
      {
         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);
         locator.setBlockOnAcknowledge(true);
         final ClientSessionFactory sf = locator.createSessionFactory();

         final CountDownLatch ready = new CountDownLatch(1);

         final byte[] body = new byte[messageSize];

         Thread producerThread = new Thread()
         {
            @Override
            public void run()
            {
               ClientSession sessionProducer = null;
               try
               {
                  sessionProducer = sf.createSession(true, true);
                  ClientProducer producer = sessionProducer.createProducer(ADDRESS);

                  for (int i = 0; i < numberOfMessages; i++)
                  {
                     ClientMessage msg = sessionProducer.createMessage(true);
                     msg.getBodyBuffer().writeBytes(body);
                     msg.putIntProperty("count", i);
                     producer.send(msg);

                     if (i == 1000)
                     {
                        // The session is not TX, but we do this just to perform a round trip to the server
                        // and make sure there are no pending messages
                        sessionProducer.commit();

                        assertTrue(server.getPagingManager().getPageStore(ADDRESS).isPaging());
                        ready.countDown();
                     }
                  }

                  sessionProducer.commit();

                  log.info("Producer gone");

               }
               catch (Throwable e)
               {
                  e.printStackTrace(); // >> junit report
                  errors.incrementAndGet();
               }
               finally
               {
                  try
                  {
                     if (sessionProducer != null)
                     {
                        sessionProducer.close();
                     }
                  }
                  catch (Throwable e)
                  {
                     e.printStackTrace();
                     errors.incrementAndGet();
                  }
               }
            }
         };

         ClientSession session = sf.createSession(true, true, 0);
         session.start();
         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

         producerThread.start();

         assertTrue(ready.await(10, TimeUnit.SECONDS));

         ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

         for (int i = 0; i < numberOfMessages; i++)
         {
            ClientMessage msg = consumer.receive(5000);
            assertNotNull(String.valueOf(i), msg);
            log.info("Received " + i + " with property = " + msg.getIntProperty("count"));
            if (i != msg.getIntProperty("count").intValue())
            {
               log.info("###### different");
            }
            // assertEquals(i, msg.getIntProperty("count").intValue());
            msg.acknowledge();
         }

         session.close();

         producerThread.join();

         assertEquals(0, errors.get());
      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   public void testPageOnSchedulingNoRestart() throws Exception
   {
      internalTestPageOnScheduling(false);
   }

   public void testPageOnSchedulingRestart() throws Exception
   {
      internalTestPageOnScheduling(true);
   }

   public void internalTestPageOnScheduling(final boolean restart) throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      config.setJournalSyncNonTransactional(false);

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      server.start();

      final int numberOfMessages = 1000;

      final int numberOfBytes = 1024;

      try
      {

         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);
         locator.setBlockOnAcknowledge(true);

         ClientSessionFactory sf = locator.createSessionFactory();
         ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         byte[] body = new byte[numberOfBytes];

         for (int j = 0; j < numberOfBytes; j++)
         {
            body[j] = UnitTestCase.getSamplebyte(j);
         }

         long scheduledTime = System.currentTimeMillis() + 5000;

         for (int i = 0; i < numberOfMessages; i++)
         {
            message = session.createMessage(true);

            message.getBodyBuffer().writeBytes(body);
            message.putIntProperty(new SimpleString("id"), i);

            PagingStore store = server.getPostOffice()
                                                                     .getPagingManager()
                                                                     .getPageStore(PagingTest.ADDRESS);

            // Worse scenario possible... only schedule what's on pages
            if (store.getCurrentPage() != null)
            {
               message.putLongProperty(Message.HDR_SCHEDULED_DELIVERY_TIME, scheduledTime);
            }

            producer.send(message);
         }

         if (restart)
         {
            session.close();

            server.stop();

            server = createServer(true,
                                  config,
                                  PagingTest.PAGE_SIZE,
                                  PagingTest.PAGE_MAX,
                                  new HashMap<String, AddressSettings>());
            server.start();

            sf = locator.createSessionFactory();

            session = sf.createSession(null, null, false, true, true, false, 0);
         }

         ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

         session.start();

         for (int i = 0; i < numberOfMessages; i++)
         {

            ClientMessage message2 = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

            Assert.assertNotNull(message2);

            message2.acknowledge();

            Assert.assertNotNull(message2);

            Long scheduled = (Long)message2.getObjectProperty(Message.HDR_SCHEDULED_DELIVERY_TIME);
            if (scheduled != null)
            {
               Assert.assertTrue("Scheduling didn't work", System.currentTimeMillis() >= scheduledTime);
            }

            try
            {
               assertBodiesEqual(body, message2.getBodyBuffer());
            }
            catch (AssertionFailedError e)
            {
               PagingTest.log.info("Expected buffer:" + UnitTestCase.dumbBytesHex(body, 40));
               PagingTest.log.info("Arriving buffer:" + UnitTestCase.dumbBytesHex(message2.getBodyBuffer()
                                                                                          .toByteBuffer()
                                                                                          .array(), 40));
               throw e;
            }
         }

         consumer.close();

         session.close();
      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   public void testRollbackOnSend() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      server.start();

      final int numberOfIntegers = 256;

      final int numberOfMessages = 10;

      try
      {

         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);
         locator.setBlockOnAcknowledge(true);

         ClientSessionFactory sf = locator.createSessionFactory();
         ClientSession session = sf.createSession(null, null, false, false, true, false, 0);

         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         for (int i = 0; i < numberOfMessages; i++)
         {
            message = session.createMessage(true);

            HornetQBuffer bodyLocal = message.getBodyBuffer();

            for (int j = 1; j <= numberOfIntegers; j++)
            {
               bodyLocal.writeInt(j);
            }

            message.putIntProperty(new SimpleString("id"), i);

            producer.send(message);
         }

         session.rollback();

         ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

         session.start();

         Assert.assertNull(consumer.receiveImmediate());

         session.close();
      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   public void testCommitOnSend() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      server.start();

      final int numberOfIntegers = 10;

      final int numberOfMessages = 500;

      try
      {

         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);
         locator.setBlockOnAcknowledge(true);

         ClientSessionFactory sf = locator.createSessionFactory();
         ClientSession session = sf.createSession(null, null, false, false, false, false, 0);

         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         for (int i = 0; i < numberOfMessages; i++)
         {
            message = session.createMessage(true);

            HornetQBuffer bodyLocal = message.getBodyBuffer();

            for (int j = 1; j <= numberOfIntegers; j++)
            {
               bodyLocal.writeInt(j);
            }

            message.putIntProperty(new SimpleString("id"), i);

            producer.send(message);
         }

         session.commit();

         session.close();

         locator.close();

         locator = createInVMNonHALocator();

         server.stop();

         server = createServer(true,
                               config,
                               PagingTest.PAGE_SIZE,
                               PagingTest.PAGE_MAX,
                               new HashMap<String, AddressSettings>());

         server.start();

         sf = locator.createSessionFactory();

         session = sf.createSession(null, null, false, false, false, false, 0);

         ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

         session.start();
         for (int i = 0; i < numberOfMessages; i++)
         {
            if (i == 55)
            {
               System.out.println("i = 55");
            }
            ClientMessage msg = consumer.receive(5000);
            Assert.assertNotNull("Received " + i, msg);
            msg.acknowledge();
            session.commit();
         }

         session.close();
      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   public void testParialConsume() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      server.start();

      final int numberOfMessages = 1000;

      try
      {

         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);
         locator.setBlockOnAcknowledge(true);

         ClientSessionFactory sf = locator.createSessionFactory();
         ClientSession session = sf.createSession(null, null, false, false, false, false, 0);

         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         for (int i = 0; i < numberOfMessages; i++)
         {
            message = session.createMessage(true);

            HornetQBuffer bodyLocal = message.getBodyBuffer();

            bodyLocal.writeBytes(new byte[1024]);

            message.putIntProperty(new SimpleString("id"), i);

            producer.send(message);
         }

         session.commit();

         session.close();

         locator.close();

         server.stop();

         server = createServer(true,
                               config,
                               PagingTest.PAGE_SIZE,
                               PagingTest.PAGE_MAX,
                               new HashMap<String, AddressSettings>());

         server.start();

         locator = createInVMNonHALocator();

         sf = locator.createSessionFactory();

         session = sf.createSession(null, null, false, false, false, false, 0);

         ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

         session.start();
         // 347 = I just picked any odd number, not rounded, to make sure it's not at the beggining of any page
         for (int i = 0; i < 347; i++)
         {
            ClientMessage msg = consumer.receive(5000);
            assertEquals(i, msg.getIntProperty("id").intValue());
            Assert.assertNotNull("Received " + i, msg);
            msg.acknowledge();
            session.commit();
         }

         session.close();

         locator.close();

         server.stop();

         server = createServer(true,
                               config,
                               PagingTest.PAGE_SIZE,
                               PagingTest.PAGE_MAX,
                               new HashMap<String, AddressSettings>());

         server.start();

         locator = createInVMNonHALocator();

         sf = locator.createSessionFactory();

         session = sf.createSession(null, null, false, false, false, false, 0);

         consumer = session.createConsumer(PagingTest.ADDRESS);

         session.start();
         for (int i = 347; i < numberOfMessages; i++)
         {
            ClientMessage msg = consumer.receive(5000);
            assertEquals(i, msg.getIntProperty("id").intValue());
            Assert.assertNotNull("Received " + i, msg);
            msg.acknowledge();
            session.commit();
         }

         session.close();
      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   public void testPageMultipleDestinations() throws Exception
   {
      internalTestPageMultipleDestinations(false);
   }

   public void testPageMultipleDestinationsTransacted() throws Exception
   {
      internalTestPageMultipleDestinations(true);
   }

   public void testDropMessages() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      HashMap<String, AddressSettings> settings = new HashMap<String, AddressSettings>();

      AddressSettings set = new AddressSettings();
      set.setAddressFullMessagePolicy(AddressFullMessagePolicy.DROP);

      settings.put(PagingTest.ADDRESS.toString(), set);

      HornetQServer server = createServer(true, config, 1024, 10 * 1024, settings);

      server.start();

      final int numberOfMessages = 1000;

      try
      {

         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);
         locator.setBlockOnAcknowledge(true);

         ClientSessionFactory sf = locator.createSessionFactory();
         ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         for (int i = 0; i < numberOfMessages; i++)
         {
            byte[] body = new byte[1024];

            message = session.createMessage(true);
            message.getBodyBuffer().writeBytes(body);

            producer.send(message);
         }

         ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

         session.start();

         for (int i = 0; i < 6; i++)
         {
            ClientMessage message2 = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

            Assert.assertNotNull(message2);

            message2.acknowledge();
         }

         Assert.assertNull(consumer.receiveImmediate());

         Assert.assertEquals(0, server.getPostOffice()
                                      .getPagingManager()
                                      .getPageStore(PagingTest.ADDRESS)
                                      .getAddressSize());

         for (int i = 0; i < numberOfMessages; i++)
         {
            byte[] body = new byte[1024];

            message = session.createMessage(true);
            message.getBodyBuffer().writeBytes(body);

            producer.send(message);
         }

         for (int i = 0; i < 6; i++)
         {
            ClientMessage message2 = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

            Assert.assertNotNull(message2);

            message2.acknowledge();
         }

         Assert.assertNull(consumer.receiveImmediate());

         session.close();

         session = sf.createSession(false, true, true);

         producer = session.createProducer(PagingTest.ADDRESS);

         for (int i = 0; i < numberOfMessages; i++)
         {
            byte[] body = new byte[1024];

            message = session.createMessage(true);
            message.getBodyBuffer().writeBytes(body);

            producer.send(message);
         }

         session.commit();

         consumer = session.createConsumer(PagingTest.ADDRESS);

         session.start();

         for (int i = 0; i < 6; i++)
         {
            ClientMessage message2 = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

            Assert.assertNotNull(message2);

            message2.acknowledge();
         }

         session.commit();

         Assert.assertNull(consumer.receiveImmediate());

         session.close();

         Assert.assertEquals(0, server.getPostOffice()
                                      .getPagingManager()
                                      .getPageStore(PagingTest.ADDRESS)
                                      .getAddressSize());

      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   public void testDropMessagesExpiring() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      HashMap<String, AddressSettings> settings = new HashMap<String, AddressSettings>();

      AddressSettings set = new AddressSettings();
      set.setAddressFullMessagePolicy(AddressFullMessagePolicy.DROP);

      settings.put(PagingTest.ADDRESS.toString(), set);

      HornetQServer server = createServer(true, config, 1024, 1024 * 1024, settings);

      server.start();

      final int numberOfMessages = 30000;

      try
      {

         locator.setAckBatchSize(0);

         ClientSessionFactory sf = locator.createSessionFactory();
         ClientSession session = sf.createSession();

         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         class MyHandler implements MessageHandler
         {
            int count;

            public void onMessage(ClientMessage message)
            {
               try
               {
                  Thread.sleep(1);
               }
               catch (Exception e)
               {

               }

               count++;

               if (count % 1000 == 0)
               {
                  log.info("received " + count);
               }

               try
               {
                  message.acknowledge();
               }
               catch (Exception e)
               {
                  e.printStackTrace();
               }
            }
         }

         ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

         session.start();

         consumer.setMessageHandler(new MyHandler());

         for (int i = 0; i < numberOfMessages; i++)
         {
            byte[] body = new byte[1024];

            message = session.createMessage(false);
            message.getBodyBuffer().writeBytes(body);

            message.setExpiration(System.currentTimeMillis() + 100);

            producer.send(message);
         }

         session.close();
      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   private void internalTestPageMultipleDestinations(final boolean transacted) throws Exception
   {
      Configuration config = createDefaultConfig();

      final int NUMBER_OF_BINDINGS = 100;

      int NUMBER_OF_MESSAGES = 2;

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      server.start();

      try
      {

         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);
         locator.setBlockOnAcknowledge(true);

         ClientSessionFactory sf = locator.createSessionFactory();
         ClientSession session = sf.createSession(null, null, false, !transacted, true, false, 0);

         for (int i = 0; i < NUMBER_OF_BINDINGS; i++)
         {
            session.createQueue(PagingTest.ADDRESS, new SimpleString("someQueue" + i), null, true);
         }

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         byte[] body = new byte[1024];

         message = session.createMessage(true);
         message.getBodyBuffer().writeBytes(body);

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
         {
            producer.send(message);

            if (transacted)
            {
               session.commit();
            }
         }

         session.close();

         server.stop();

         server = createServer(true,
                               config,
                               PagingTest.PAGE_SIZE,
                               PagingTest.PAGE_MAX,
                               new HashMap<String, AddressSettings>());
         server.start();

         sf = locator.createSessionFactory();

         session = sf.createSession(null, null, false, true, true, false, 0);

         session.start();

         for (int msg = 0; msg < NUMBER_OF_MESSAGES; msg++)
         {

            for (int i = 0; i < NUMBER_OF_BINDINGS; i++)
            {
               ClientConsumer consumer = session.createConsumer(new SimpleString("someQueue" + i));

               ClientMessage message2 = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

               Assert.assertNotNull(message2);

               message2.acknowledge();

               Assert.assertNotNull(message2);

               consumer.close();

            }
         }

         session.close();

         for (int i = 0; i < NUMBER_OF_BINDINGS; i++)
         {
            Queue queue = (Queue)server.getPostOffice().getBinding(new SimpleString("someQueue" + i)).getBindable();

            Assert.assertEquals("Queue someQueue" + i + " was supposed to be empty", 0, queue.getMessageCount());
            Assert.assertEquals("Queue someQueue" + i + " was supposed to be empty", 0, queue.getDeliveringCount());
         }

      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   public void testSyncPage() throws Exception
   {
      Configuration config = createDefaultConfig();

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      server.start();

      try
      {
         server.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true, false);

         final CountDownLatch pageUp = new CountDownLatch(0);
         final CountDownLatch pageDone = new CountDownLatch(1);

         OperationContext ctx = new OperationContext()
         {

            public void onError(int errorCode, String errorMessage)
            {
            }

            public void done()
            {
            }

            public void storeLineUp()
            {
            }

            public boolean waitCompletion(long timeout) throws Exception
            {
               return false;
            }

            public void waitCompletion() throws Exception
            {

            }

            public void replicationLineUp()
            {

            }

            public void replicationDone()
            {

            }

            public void pageSyncLineUp()
            {
               pageUp.countDown();
            }

            public void pageSyncDone()
            {
               pageDone.countDown();
            }

            public void executeOnCompletion(IOAsyncTask runnable)
            {

            }
         };

         OperationContextImpl.setContext(ctx);

         PagingManager paging = server.getPagingManager();

         PagingStore store = paging.getPageStore(ADDRESS);

         store.sync();

         assertTrue(pageUp.await(10, TimeUnit.SECONDS));

         assertTrue(pageDone.await(10, TimeUnit.SECONDS));

         server.stop();

      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }

         OperationContextImpl.clearContext();
      }

   }

   public void testSyncPageTX() throws Exception
   {
      Configuration config = createDefaultConfig();

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      server.start();

      try
      {
         server.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true, false);

         final CountDownLatch pageUp = new CountDownLatch(0);
         final CountDownLatch pageDone = new CountDownLatch(1);

         OperationContext ctx = new OperationContext()
         {

            public void onError(int errorCode, String errorMessage)
            {
            }

            public void done()
            {
            }

            public void storeLineUp()
            {
            }

            public boolean waitCompletion(long timeout) throws Exception
            {
               return false;
            }

            public void waitCompletion() throws Exception
            {

            }

            public void replicationLineUp()
            {

            }

            public void replicationDone()
            {

            }

            public void pageSyncLineUp()
            {
               pageUp.countDown();
            }

            public void pageSyncDone()
            {
               pageDone.countDown();
            }

            public void executeOnCompletion(IOAsyncTask runnable)
            {

            }
         };

         OperationContextImpl.setContext(ctx);

         PagingManager paging = server.getPagingManager();

         PagingStore store = paging.getPageStore(ADDRESS);

         store.sync();

         assertTrue(pageUp.await(10, TimeUnit.SECONDS));

         assertTrue(pageDone.await(10, TimeUnit.SECONDS));

         server.stop();

      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   public void testPagingOneDestinationOnly() throws Exception
   {
      SimpleString PAGED_ADDRESS = new SimpleString("paged");
      SimpleString NON_PAGED_ADDRESS = new SimpleString("non-paged");

      Configuration configuration = createDefaultConfig();

      Map<String, AddressSettings> addresses = new HashMap<String, AddressSettings>();

      addresses.put("#", new AddressSettings());

      AddressSettings pagedDestination = new AddressSettings();
      pagedDestination.setPageSizeBytes(1024);
      pagedDestination.setMaxSizeBytes(10 * 1024);

      addresses.put(PAGED_ADDRESS.toString(), pagedDestination);

      HornetQServer server = createServer(true, configuration, -1, -1, addresses);

      try
      {
         server.start();

         ClientSessionFactory sf = locator.createSessionFactory();

         ClientSession session = sf.createSession(false, true, false);

         session.createQueue(PAGED_ADDRESS, PAGED_ADDRESS, true);

         session.createQueue(NON_PAGED_ADDRESS, NON_PAGED_ADDRESS, true);

         ClientProducer producerPaged = session.createProducer(PAGED_ADDRESS);
         ClientProducer producerNonPaged = session.createProducer(NON_PAGED_ADDRESS);

         int NUMBER_OF_MESSAGES = 100;

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
         {
            ClientMessage msg = session.createMessage(true);
            msg.getBodyBuffer().writeBytes(new byte[512]);

            producerPaged.send(msg);
            producerNonPaged.send(msg);
         }

         session.close();

         Assert.assertTrue(server.getPostOffice().getPagingManager().getPageStore(PAGED_ADDRESS).isPaging());
         Assert.assertFalse(server.getPostOffice().getPagingManager().getPageStore(NON_PAGED_ADDRESS).isPaging());

         session = sf.createSession(false, true, false);

         session.start();

         ClientConsumer consumerNonPaged = session.createConsumer(NON_PAGED_ADDRESS);
         ClientConsumer consumerPaged = session.createConsumer(PAGED_ADDRESS);

         ClientMessage ackList[] = new ClientMessage[NUMBER_OF_MESSAGES];

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
         {
            ClientMessage msg = consumerNonPaged.receive(5000);
            Assert.assertNotNull(msg);
            ackList[i] = msg;
         }

         Assert.assertNull(consumerNonPaged.receiveImmediate());

         for (ClientMessage ack : ackList)
         {
            ack.acknowledge();
         }

         consumerNonPaged.close();

         session.commit();

         ackList = null;

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
         {
            ClientMessage msg = consumerPaged.receive(5000);
            Assert.assertNotNull(msg);
            msg.acknowledge();
            session.commit();
         }

         Assert.assertNull(consumerPaged.receiveImmediate());

         session.close();

      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testPagingDifferentSizes() throws Exception
   {
      SimpleString PAGED_ADDRESS_A = new SimpleString("paged-a");
      SimpleString PAGED_ADDRESS_B = new SimpleString("paged-b");

      Configuration configuration = createDefaultConfig();

      Map<String, AddressSettings> addresses = new HashMap<String, AddressSettings>();

      addresses.put("#", new AddressSettings());

      AddressSettings pagedDestinationA = new AddressSettings();
      pagedDestinationA.setPageSizeBytes(1024);
      pagedDestinationA.setMaxSizeBytes(10 * 1024);

      int NUMBER_MESSAGES_BEFORE_PAGING = 11;

      addresses.put(PAGED_ADDRESS_A.toString(), pagedDestinationA);

      AddressSettings pagedDestinationB = new AddressSettings();
      pagedDestinationB.setPageSizeBytes(2024);
      pagedDestinationB.setMaxSizeBytes(25 * 1024);

      addresses.put(PAGED_ADDRESS_B.toString(), pagedDestinationB);

      HornetQServer server = createServer(true, configuration, -1, -1, addresses);

      try
      {
         server.start();

         ClientSessionFactory sf = locator.createSessionFactory();

         ClientSession session = sf.createSession(false, true, false);

         session.createQueue(PAGED_ADDRESS_A, PAGED_ADDRESS_A, true);

         session.createQueue(PAGED_ADDRESS_B, PAGED_ADDRESS_B, true);

         ClientProducer producerA = session.createProducer(PAGED_ADDRESS_A);
         ClientProducer producerB = session.createProducer(PAGED_ADDRESS_B);

         int NUMBER_OF_MESSAGES = 100;

         for (int i = 0; i < NUMBER_MESSAGES_BEFORE_PAGING; i++)
         {
            ClientMessage msg = session.createMessage(true);
            msg.getBodyBuffer().writeBytes(new byte[512]);

            producerA.send(msg);
            producerB.send(msg);
         }

         session.commit(); // commit was called to clean the buffer only (making sure everything is on the server side)

         Assert.assertTrue(server.getPostOffice().getPagingManager().getPageStore(PAGED_ADDRESS_A).isPaging());
         Assert.assertFalse(server.getPostOffice().getPagingManager().getPageStore(PAGED_ADDRESS_B).isPaging());

         for (int i = 0; i < NUMBER_MESSAGES_BEFORE_PAGING; i++)
         {
            ClientMessage msg = session.createMessage(true);
            msg.getBodyBuffer().writeBytes(new byte[512]);

            producerA.send(msg);
            producerB.send(msg);
         }

         session.commit(); // commit was called to clean the buffer only (making sure everything is on the server side)

         Assert.assertTrue(server.getPostOffice().getPagingManager().getPageStore(PAGED_ADDRESS_A).isPaging());
         Assert.assertTrue(server.getPostOffice().getPagingManager().getPageStore(PAGED_ADDRESS_B).isPaging());

         for (int i = NUMBER_MESSAGES_BEFORE_PAGING * 2; i < NUMBER_OF_MESSAGES; i++)
         {
            ClientMessage msg = session.createMessage(true);
            msg.getBodyBuffer().writeBytes(new byte[512]);

            producerA.send(msg);
            producerB.send(msg);
         }

         session.close();

         Assert.assertTrue(server.getPostOffice().getPagingManager().getPageStore(PAGED_ADDRESS_A).isPaging());
         Assert.assertTrue(server.getPostOffice().getPagingManager().getPageStore(PAGED_ADDRESS_B).isPaging());

         session = sf.createSession(null, null, false, true, true, false, 0);

         session.start();

         ClientConsumer consumerA = session.createConsumer(PAGED_ADDRESS_A);

         ClientConsumer consumerB = session.createConsumer(PAGED_ADDRESS_B);

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
         {
            ClientMessage msg = consumerA.receive(5000);
            Assert.assertNotNull("Couldn't receive a message on consumerA, iteration = " + i, msg);
            msg.acknowledge();
         }

         Assert.assertNull(consumerA.receiveImmediate());

         consumerA.close();

         Assert.assertTrue(server.getPostOffice().getPagingManager().getPageStore(PAGED_ADDRESS_B).isPaging());

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
         {
            ClientMessage msg = consumerB.receive(5000);
            Assert.assertNotNull(msg);
            msg.acknowledge();
            session.commit();
         }

         Assert.assertNull(consumerB.receiveImmediate());

         consumerB.close();

         session.close();

      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testPageAndDepageRapidly() throws Exception
   {
      boolean persistentMessages = true;

      clearData();

      Configuration config = createDefaultConfig();

      config.setJournalSyncNonTransactional(false);
      config.setJournalFileSize(10 * 1024 * 1024);

      HornetQServer server = createServer(true, config, 512 * 1024, 1024 * 1024, new HashMap<String, AddressSettings>());

      server.start();

      final int messageSize = 51527;

      final int numberOfMessages = 200;

      try
      {
         ServerLocator locator = createInVMNonHALocator();

         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);
         locator.setBlockOnAcknowledge(true);

         final ClientSessionFactory sf = locator.createSessionFactory();

         ClientSession session = sf.createSession(true, true);

         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         final AtomicInteger errors = new AtomicInteger(0);

         Thread consumeThread = new Thread()
         {
            @Override
            public void run()
            {
               ClientSession sessionConsumer = null;
               try
               {
                  sessionConsumer = sf.createSession(false, false);
                  sessionConsumer.start();

                  ClientConsumer cons = sessionConsumer.createConsumer(ADDRESS);

                  for (int i = 0; i < numberOfMessages; i++)
                  {
                     ClientMessage msg = cons.receive(PagingTest.RECEIVE_TIMEOUT);
                     System.out.println("Message " + i + " consumed");
                     assertNotNull(msg);
                     msg.acknowledge();

                     if (i % 20 == 0)
                     {
                        System.out.println("Commit consumer");
                        sessionConsumer.commit();
                     }
                  }
                  sessionConsumer.commit();
               }
               catch (Throwable e)
               {
                  e.printStackTrace();
                  errors.incrementAndGet();
               }
               finally
               {
                  try
                  {
                     sessionConsumer.close();
                  }
                  catch (HornetQException e)
                  {
                     e.printStackTrace();
                     errors.incrementAndGet();
                  }
               }

            }
         };

         consumeThread.start();

         ClientMessage message = null;

         byte[] body = new byte[messageSize];

         for (int i = 0; i < numberOfMessages; i++)
         {
            message = session.createMessage(persistentMessages);

            System.out.println("Message " + i + " sent");

            HornetQBuffer bodyLocal = message.getBodyBuffer();

            bodyLocal.writeBytes(body);

            message.putIntProperty(new SimpleString("id"), i);

            producer.send(message);

            Thread.sleep(50);
         }

         consumeThread.join();

         long timeout = System.currentTimeMillis() + 5000;

         while (System.currentTimeMillis() < timeout && (server.getPagingManager().getPageStore(ADDRESS).isPaging() || server.getPagingManager()
                                                                                                                             .getPageStore(ADDRESS)
                                                                                                                             .getNumberOfPages() != 1))
         {
            Thread.sleep(1);
         }

         // It's async, so need to wait a bit for it happening
         assertFalse(server.getPagingManager().getPageStore(ADDRESS).isPaging());

         assertEquals(1, server.getPagingManager().getPageStore(ADDRESS).getNumberOfPages());

         sf.close();

         locator.close();
      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   public void testTwoQueuesDifferentFilters() throws Exception
   {
      boolean persistentMessages = true;

      clearData();

      Configuration config = createDefaultConfig();

      config.setJournalSyncNonTransactional(false);

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      server.start();

      final int messageSize = 1024;

      final int numberOfMessages = 200;

      try
      {
         ServerLocator locator = createInVMNonHALocator();

         locator.setClientFailureCheckPeriod(120000);
         locator.setConnectionTTL(5000000);
         locator.setCallTimeout(120000);

         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);
         locator.setBlockOnAcknowledge(true);

         ClientSessionFactory sf = locator.createSessionFactory();

         ClientSession session = sf.createSession(false, false, false);

         // note: if you want to change this, numberOfMessages has to be a multiple of NQUEUES
         int NQUEUES = 2;

         for (int i = 0; i < NQUEUES; i++)
         {
            session.createQueue(PagingTest.ADDRESS,
                                PagingTest.ADDRESS.concat("=" + i),
                                new SimpleString("propTest=" + i),
                                true);
         }

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         byte[] body = new byte[messageSize];

         for (int i = 0; i < numberOfMessages; i++)
         {
            message = session.createMessage(persistentMessages);

            HornetQBuffer bodyLocal = message.getBodyBuffer();

            bodyLocal.writeBytes(body);

            message.putIntProperty("propTest", i % NQUEUES);
            message.putIntProperty("id", i);

            producer.send(message);
            if (i % 1000 == 0)
            {
               session.commit();
            }
         }

         session.commit();

         session.start();

         for (int nqueue = 0; nqueue < NQUEUES; nqueue++)
         {
            ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS.concat("=" + nqueue));

            for (int i = 0; i < (numberOfMessages / NQUEUES); i++)
            {
               message = consumer.receive(500000);
               assertNotNull(message);
               message.acknowledge();

               assertEquals(nqueue, message.getIntProperty("propTest").intValue());
            }

            assertNull(consumer.receiveImmediate());

            consumer.close();

            session.commit();
         }

         PagingStore store = server.getPagingManager().getPageStore(ADDRESS);
         store.getCursorProvider().cleanup();

         long timeout = System.currentTimeMillis() + 5000;
         while (store.isPaging() && timeout > System.currentTimeMillis())
         {
            Thread.sleep(100);
         }

         // It's async, so need to wait a bit for it happening
         assertFalse(server.getPagingManager().getPageStore(ADDRESS).isPaging());

         sf.close();

         locator.close();
      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }
   }

   public void testTwoQueues() throws Exception
   {
      boolean persistentMessages = true;

      clearData();

      Configuration config = createDefaultConfig();

      config.setJournalSyncNonTransactional(false);

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      server.start();

      final int messageSize = 1024;

      final int numberOfMessages = 1000;

      try
      {
         ServerLocator locator = createInVMNonHALocator();

         locator.setClientFailureCheckPeriod(120000);
         locator.setConnectionTTL(5000000);
         locator.setCallTimeout(120000);

         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);
         locator.setBlockOnAcknowledge(true);

         ClientSessionFactory sf = locator.createSessionFactory();

         ClientSession session = sf.createSession(false, false, false);

         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS.concat("=1"), null, true);
         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS.concat("=2"), null, true);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         byte[] body = new byte[messageSize];

         for (int i = 0; i < numberOfMessages; i++)
         {
            message = session.createMessage(persistentMessages);

            HornetQBuffer bodyLocal = message.getBodyBuffer();

            bodyLocal.writeBytes(body);

            message.putIntProperty("propTest", i % 2 == 0 ? 1 : 2);

            producer.send(message);
            if (i % 1000 == 0)
            {
               session.commit();
            }
         }

         session.commit();

         session.start();

         for (int msg = 1; msg <= 2; msg++)
         {
            ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS.concat("=" + msg));

            for (int i = 0; i < numberOfMessages; i++)
            {
               message = consumer.receive(500000);
               assertNotNull(message);
               message.acknowledge();

               // assertEquals(msg, message.getIntProperty("propTest").intValue());

               System.out.println("i = " + i + " msg = " + message.getIntProperty("propTest"));
            }

            session.commit();

            assertNull(consumer.receiveImmediate());

            consumer.close();
         }

         PagingStore store = server.getPagingManager().getPageStore(ADDRESS);
         store.getCursorProvider().cleanup();

         long timeout = System.currentTimeMillis() + 5000;
         while (store.isPaging() && timeout > System.currentTimeMillis())
         {
            Thread.sleep(100);
         }

         store.getCursorProvider().cleanup();

         Thread.sleep(1000);

         // It's async, so need to wait a bit for it happening
         assertFalse(server.getPagingManager().getPageStore(ADDRESS).isPaging());

         sf.close();

         locator.close();
      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }
   }

   public void testDLAOnLargeMessageAndPaging() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();
      config.setThreadPoolMaxSize(5);

      config.setJournalSyncNonTransactional(false);

      Map<String, AddressSettings> settings = new HashMap<String, AddressSettings>();
      AddressSettings dla = new AddressSettings();
      dla.setMaxDeliveryAttempts(5);
      dla.setDeadLetterAddress(new SimpleString("DLA"));
      settings.put(ADDRESS.toString(), dla);

      final HornetQServer server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX, settings);

      server.start();

      final int messageSize = 1024;

      ServerLocator locator = null;
      ClientSessionFactory sf = null;
      ClientSession session = null;
      try
      {
         locator = createInVMNonHALocator();

         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);
         locator.setBlockOnAcknowledge(true);

         sf = locator.createSessionFactory();

         session = sf.createSession(false, false, false);

         session.createQueue(ADDRESS, ADDRESS, true);

         session.createQueue("DLA", "DLA");

         PagingStore pgStoreAddress = server.getPagingManager().getPageStore(ADDRESS);
         pgStoreAddress.startPaging();
         PagingStore pgStoreDLA = server.getPagingManager().getPageStore(new SimpleString("DLA"));

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         for (int i = 0; i < 100; i++)
         {
            log.debug("send message #" + i);
            ClientMessage message = session.createMessage(true);

            message.putStringProperty("id", "str" + i);

            message.setBodyInputStream(createFakeLargeStream(messageSize));

            producer.send(message);

            if ((i + 1) % 2 == 0)
            {
               session.commit();
            }
         }

         session.commit();

         session.start();

         ClientConsumer cons = session.createConsumer(ADDRESS);

         for (int msgNr = 0; msgNr < 2; msgNr++)
         {
            for (int i = 0; i < 5; i++)
            {
               ClientMessage msg = cons.receive(5000);

               assertNotNull(msg);

               msg.acknowledge();

               assertEquals("str" + msgNr, msg.getStringProperty("id"));

               for (int j = 0; j < messageSize; j++)
               {
                  assertEquals(getSamplebyte(j), msg.getBodyBuffer().readByte());
               }

               session.rollback();
            }

            pgStoreDLA.startPaging();
         }

         for (int i = 2; i < 100; i++)
         {
            log.debug("Received message " + i);
            ClientMessage message = cons.receive(5000);
            assertNotNull("Message " + i + " wasn't received", message);
            message.acknowledge();

            final AtomicInteger bytesOutput = new AtomicInteger(0);

            message.setOutputStream(new OutputStream()
            {
               @Override
               public void write(int b) throws IOException
               {
                  bytesOutput.incrementAndGet();
               }
            });

            try
            {
               if (!message.waitOutputStreamCompletion(10000))
               {
                  log.info(threadDump("dump"));
                  fail("Couldn't finish large message receiving");
               }
            }
            catch (Throwable e)
            {
               log.info("output bytes = " + bytesOutput);
               log.info(threadDump("dump"));
               fail("Couldn't finish large message receiving for id=" + message.getStringProperty("id") +
                    " with messageID=" +
                    message.getMessageID());
            }

         }

         assertNull(cons.receiveImmediate());

         cons.close();

         cons = session.createConsumer("DLA");

         for (int i = 0; i < 2; i++)
         {
            assertNotNull(cons.receive(5000));
         }

         sf.close();

         session.close();

         locator.close();

         server.stop();

         server.start();

         locator = createInVMNonHALocator();

         sf = locator.createSessionFactory();

         session = sf.createSession(false, false);

         session.start();

         cons = session.createConsumer(ADDRESS);

         for (int i = 2; i < 100; i++)
         {
            log.debug("Received message " + i);
            ClientMessage message = cons.receive(5000);
            assertNotNull(message);

            assertEquals("str" + i, message.getStringProperty("id"));

            message.acknowledge();

            message.setOutputStream(new OutputStream()
            {
               @Override
               public void write(int b) throws IOException
               {

               }
            });

            assertTrue(message.waitOutputStreamCompletion(5000));
         }

         assertNull(cons.receiveImmediate());

         cons.close();

         cons = session.createConsumer("DLA");

         for (int msgNr = 0; msgNr < 2; msgNr++)
         {
            ClientMessage msg = cons.receive(10000);

            assertNotNull(msg);

            assertEquals("str" + msgNr, msg.getStringProperty("id"));

            for (int i = 0; i < messageSize; i++)
            {
               assertEquals(getSamplebyte(i), msg.getBodyBuffer().readByte());
            }

            msg.acknowledge();
         }

         cons.close();

         cons = session.createConsumer(ADDRESS);

         session.commit();

         assertNull(cons.receiveImmediate());

         long timeout = System.currentTimeMillis() + 5000;

         pgStoreAddress = server.getPagingManager().getPageStore(ADDRESS);

         pgStoreAddress.getCursorProvider().getSubscription(server.locateQueue(ADDRESS).getID()).cleanupEntries();

         pgStoreAddress.getCursorProvider().cleanup();

         while (timeout > System.currentTimeMillis() && pgStoreAddress.isPaging())
         {
            Thread.sleep(50);
         }

         assertFalse(pgStoreAddress.isPaging());

         session.commit();
      }
      finally
      {
         session.close();
         sf.close();
         locator.close();
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }
   }

   public void testExpireLargeMessageOnPaging() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();
      config.setMessageExpiryScanPeriod(500);

      config.setJournalSyncNonTransactional(false);

      Map<String, AddressSettings> settings = new HashMap<String, AddressSettings>();
      AddressSettings dla = new AddressSettings();
      dla.setMaxDeliveryAttempts(5);
      dla.setDeadLetterAddress(new SimpleString("DLA"));
      dla.setExpiryAddress(new SimpleString("DLA"));
      settings.put(ADDRESS.toString(), dla);

      final HornetQServer server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX, settings);

      server.start();

      final int messageSize = 20;

      try
      {
         ServerLocator locator = createInVMNonHALocator();

         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);
         locator.setBlockOnAcknowledge(true);

         ClientSessionFactory sf = locator.createSessionFactory();

         ClientSession session = sf.createSession(false, false, false);

         session.createQueue(ADDRESS, ADDRESS, true);

         session.createQueue("DLA", "DLA");

         PagingStore pgStoreAddress = server.getPagingManager().getPageStore(ADDRESS);
         pgStoreAddress.startPaging();
         PagingStore pgStoreDLA = server.getPagingManager().getPageStore(new SimpleString("DLA"));

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         for (int i = 0; i < 500; i++)
         {
            if (i % 100 == 0)
               log.info("send message #" + i);
            message = session.createMessage(true);

            message.putStringProperty("id", "str" + i);

            message.setExpiration(System.currentTimeMillis() + 2000);

            if (i % 2 == 0)
            {
               message.setBodyInputStream(createFakeLargeStream(messageSize));
            }
            else
            {
               byte bytes[] = new byte[messageSize];
               for (int s = 0; s < bytes.length; s++)
               {
                  bytes[s] = getSamplebyte(s);
               }
               message.getBodyBuffer().writeBytes(bytes);
            }

            producer.send(message);

            if ((i + 1) % 2 == 0)
            {
               session.commit();
               if (i < 400)
               {
                  pgStoreAddress.forceAnotherPage();
               }
            }
         }

         session.commit();

         sf.close();

         locator.close();

         server.stop();

         Thread.sleep(3000);

         server.start();

         locator = createInVMNonHALocator();

         sf = locator.createSessionFactory();

         session = sf.createSession(false, false);

         session.start();

         ClientConsumer consAddr = session.createConsumer(ADDRESS);

         assertNull(consAddr.receive(1000));

         ClientConsumer cons = session.createConsumer("DLA");

         for (int i = 0; i < 500; i++)
         {
            log.info("Received message " + i);
            message = cons.receive(5000);
            assertNotNull(message);
            message.acknowledge();

            message.saveToOutputStream(new OutputStream()
            {
               @Override
               public void write(int b) throws IOException
               {

               }
            });
         }

         assertNull(cons.receiveImmediate());

         session.commit();

         cons.close();

         long timeout = System.currentTimeMillis() + 5000;

         pgStoreAddress = server.getPagingManager().getPageStore(ADDRESS);

         while (timeout > System.currentTimeMillis() && pgStoreAddress.isPaging())
         {
            Thread.sleep(50);
         }

         assertFalse(pgStoreAddress.isPaging());

         session.close();
      }
      finally
      {
         locator.close();
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected Configuration createDefaultConfig()
   {
      Configuration config = super.createDefaultConfig();
      config.setJournalSyncNonTransactional(false);
      return config;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
