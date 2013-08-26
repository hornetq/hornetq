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

import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.journal.LoaderCallback;
import org.hornetq.core.journal.PreparedTransactionInfo;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.core.journal.impl.NIOSequentialFileFactory;
import org.hornetq.core.persistence.impl.journal.JournalRecordIds;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.integration.IntegrationTestLogger;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * A RedeliveryConsumerTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 * Created Feb 17, 2009 6:06:11 PM
 *
 *
 */
public class RedeliveryConsumerTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   // Attributes ----------------------------------------------------

   HornetQServer server;

   final SimpleString ADDRESS = new SimpleString("address");

   ClientSessionFactory factory;

   private ServerLocator locator;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testRedeliveryMessageStrict() throws Exception
   {
      testDedeliveryMessageOnPersistent(true);
   }

   @Test
   public void testRedeliveryMessageSimpleCancel() throws Exception
   {
      testDedeliveryMessageOnPersistent(false);
   }

   @Test
   public void testDeliveryNonPersistent() throws Exception
   {
      testDelivery(false);
   }

   @Test
   public void testDeliveryPersistent() throws Exception
   {
      testDelivery(true);
   }

   public void testDelivery(final boolean persistent) throws Exception
   {
      setUp(true);
      ClientSession session = factory.createSession(false, false, false);
      ClientProducer prod = session.createProducer(ADDRESS);

      for (int i = 0; i < 10; i++)
      {
         prod.send(createTextMessage(session, Integer.toString(i), persistent));
      }

      session.commit();
      session.close();

      session = factory.createSession(null, null, false, true, true, true, 0);

      session.start();
      for (int loopAck = 0; loopAck < 5; loopAck++)
      {
         ClientConsumer browser = session.createConsumer(ADDRESS, null, true);
         for (int i = 0; i < 10; i++)
         {
            ClientMessage msg = browser.receive(1000);
            Assert.assertNotNull("element i=" + i + " loopAck = " + loopAck + " was expected", msg);
            msg.acknowledge();
            Assert.assertEquals(Integer.toString(i), getTextMessage(msg));

            // We don't change the deliveryCounter on Browser, so this should be always 0
            Assert.assertEquals(0, msg.getDeliveryCount());
         }

         session.commit();
         browser.close();
      }

      session.close();

      session = factory.createSession(false, false, false);
      session.start();

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      for (int loopAck = 0; loopAck < 5; loopAck++)
      {
         for (int i = 0; i < 10; i++)
         {
            ClientMessage msg = consumer.receive(1000);
            Assert.assertNotNull(msg);
            Assert.assertEquals(Integer.toString(i), getTextMessage(msg));

            // No ACK done, so deliveryCount should be always = 1
            Assert.assertEquals(1, msg.getDeliveryCount());
         }
         session.rollback();
      }

      if (persistent)
      {
         session.close();
         server.stop();
         server.start();
         factory = createSessionFactory(locator);
         session = factory.createSession(false, false, false);
         session.start();
         consumer = session.createConsumer(ADDRESS);
      }

      for (int loopAck = 1; loopAck <= 5; loopAck++)
      {
         for (int i = 0; i < 10; i++)
         {
            ClientMessage msg = consumer.receive(1000);
            Assert.assertNotNull(msg);
            msg.acknowledge();
            Assert.assertEquals(Integer.toString(i), getTextMessage(msg));
            Assert.assertEquals(loopAck, msg.getDeliveryCount());
         }
         if (loopAck < 5)
         {
            if (persistent)
            {
               session.close();
               server.stop();
               server.start();
               factory = createSessionFactory(locator);
               session = factory.createSession(false, false, false);
               session.start();
               consumer = session.createConsumer(ADDRESS);
            }
            else
            {
               session.rollback();
            }
         }
      }

      session.close();
   }

   protected void testDedeliveryMessageOnPersistent(final boolean strictUpdate) throws Exception
   {
      setUp(strictUpdate);
      ClientSession session = factory.createSession(false, false, false);

      RedeliveryConsumerTest.log.info("created");

      ClientProducer prod = session.createProducer(ADDRESS);
      prod.send(createTextMessage(session, "Hello"));
      session.commit();
      session.close();

      session = factory.createSession(false, false, false);
      session.start();
      ClientConsumer consumer = session.createConsumer(ADDRESS);

      ClientMessage msg = consumer.receive(1000);
      Assert.assertEquals(1, msg.getDeliveryCount());
      session.stop();

      // if strictUpdate == true, this will simulate a crash, where the server is stopped without closing/rolling back
      // the session, but the delivery count still persisted, so the final delivery count is 2 too.
      if (!strictUpdate)
      {
         // If non Strict, at least rollback/cancel should still update the delivery-counts
         session.rollback(true);

         session.close();
      }

      server.stop();

      // once the server is stopped, we close the session
      // to clean up its client resources
      session.close();

      server.start();

      factory = createSessionFactory(locator);

      session = factory.createSession(false, true, false);
      session.start();
      consumer = session.createConsumer(ADDRESS);
      msg = consumer.receive(1000);
      Assert.assertNotNull(msg);
      Assert.assertEquals(strictUpdate ? 2 : 2, msg.getDeliveryCount());
      session.close();
   }


   @Test
   public void testInfiniteDedeliveryMessageOnPersistent() throws Exception
   {
      internaltestInfiniteDedeliveryMessageOnPersistent(false);
   }

   private void internaltestInfiniteDedeliveryMessageOnPersistent(final boolean strict) throws Exception
   {
      setUp(strict);
      ClientSession session = factory.createSession(false, false, false);

      RedeliveryConsumerTest.log.info("created");

      ClientProducer prod = session.createProducer(ADDRESS);
      prod.send(createTextMessage(session, "Hello"));
      session.commit();
      session.close();


      int expectedCount = 1;
      for (int i = 0 ; i < 700; i++)
      {
         session = factory.createSession(false, false, false);
         session.start();
         ClientConsumer consumer = session.createConsumer(ADDRESS);
         ClientMessage msg = consumer.receive(5000);
         assertNotNull(msg);
         assertEquals(expectedCount, msg.getDeliveryCount());

         if (i % 100 == 0)
         {
            expectedCount++;
            msg.acknowledge();
            session.rollback();
         }
         session.close();
      }

      factory.close();
      server.stop();

      setUp(false);

      for (int i = 0 ; i < 700; i++)
      {
         session = factory.createSession(false, false, false);
         session.start();
         ClientConsumer consumer = session.createConsumer(ADDRESS);
         ClientMessage msg = consumer.receive(5000);
         assertNotNull(msg);
         assertEquals(expectedCount, msg.getDeliveryCount());
         session.close();
      }

      server.stop();


      JournalImpl journal = new JournalImpl(server.getConfiguration().getJournalFileSize(),
                                            2,
                                            0,
                                            0,
                                            new NIOSequentialFileFactory(server.getConfiguration().getJournalDirectory()),
                                            "hornetq-data",
                                            "hq",
                                            1);


      final AtomicInteger updates = new AtomicInteger();

      journal.start();
      journal.load(new LoaderCallback()
      {

         public void failedTransaction(long transactionID, List<RecordInfo> records, List<RecordInfo> recordsToDelete)
         {
         }

         public void updateRecord(RecordInfo info)
         {
            if (info.userRecordType == JournalRecordIds.UPDATE_DELIVERY_COUNT)
            {
               updates.incrementAndGet();
            }
         }

         public void deleteRecord(long id)
         {
         }

         public void addRecord(RecordInfo info)
         {
         }

         public void addPreparedTransaction(PreparedTransactionInfo preparedTransaction)
         {
         }
      });

      journal.stop();


      assertEquals(7, updates.get());

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   /**
    * @param persistDeliveryCountBeforeDelivery
    * @throws Exception
    * @throws HornetQException
    */
   private void setUp(final boolean persistDeliveryCountBeforeDelivery) throws Exception, HornetQException
   {
      Configuration config = createDefaultConfig();
      config.setPersistDeliveryCountBeforeDelivery(persistDeliveryCountBeforeDelivery);

      server = createServer(true, config);

      server.start();
      locator = createInVMNonHALocator();
      factory = createSessionFactory(locator);

      ClientSession session = factory.createSession(false, false, false);
      try
      {
         session.createQueue(ADDRESS, ADDRESS, true);
      }
      catch (HornetQException expected)
      {
         // in case of restart
      }

      session.close();
   }
}
