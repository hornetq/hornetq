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

import java.util.HashMap;
import java.util.Map;

import junit.framework.AssertionFailedError;

import org.hornetq.core.buffers.ChannelBuffers;
import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.paging.impl.TestSupportPageStore;
import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.DataConstants;
import org.hornetq.utils.SimpleString;

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

   public PagingTest(String name)
   {
      super(name);
   }

   public PagingTest()
   {
      super();
   }

   // Constants -----------------------------------------------------
   private static final Logger log = Logger.getLogger(PagingTest.class);

   private static final int RECEIVE_TIMEOUT = 30000;

   private static final int PAGE_MAX = 100 * 1024;

   private static final int PAGE_SIZE = 10 * 1024;

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   static final SimpleString ADDRESS = new SimpleString("SimpleAddress");

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testSendReceivePaging() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();
 
      HornetQServer server = createServer(true, config, PAGE_SIZE, PAGE_MAX, new HashMap<String, AddressSettings>());

      server.start();

      final int numberOfIntegers = 256;

      final int numberOfMessages = 10000;

      try
      {
         ClientSessionFactory sf = createInVMFactory();

         sf.setBlockOnNonPersistentSend(true);
         sf.setBlockOnPersistentSend(true);
         sf.setBlockOnAcknowledge(true);

         ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

         session.createQueue(ADDRESS, ADDRESS, null, true);

         ClientProducer producer = session.createProducer(ADDRESS);

         ClientMessage message = null;

         byte[] body = null;

         for (int i = 0; i < numberOfMessages; i++)
         {
            message = session.createClientMessage(true);

            HornetQBuffer bodyLocal = message.getBody();

            for (int j = 1; j <= numberOfIntegers; j++)
            {
               bodyLocal.writeInt(j);
            }

            if (body == null)
            {
               body = bodyLocal.array();
            }

            message.putIntProperty(new SimpleString("id"), i);

            producer.send(message);
         }

         session.close();

         assertTrue("TotalMemory expected to be > 0 when it was " + server.getPostOffice()
                                                                          .getPagingManager()
                                                                          .getTotalMemory(),
                    server.getPostOffice().getPagingManager().getTotalMemory() > 0);

         server.stop();

         server = createServer(true, config, PAGE_SIZE, PAGE_MAX, new HashMap<String, AddressSettings>());
         server.start();

         sf = createInVMFactory();

         assertTrue("TotalMemory expected to be > 0 when it was " + server.getPostOffice()
                                                                          .getPagingManager()
                                                                          .getTotalMemory(),
                    server.getPostOffice().getPagingManager().getTotalMemory() > 0);

         session = sf.createSession(null, null, false, true, true, false, 0);

         ClientConsumer consumer = session.createConsumer(ADDRESS);

         session.start();

         for (int i = 0; i < numberOfMessages; i++)
         {
            System.out.println("Message " + i + " of " + numberOfMessages);
            ClientMessage message2 = consumer.receive(RECEIVE_TIMEOUT);

            assertNotNull(message2);

            assertEquals(i, message2.getIntProperty("id").intValue());

            message2.acknowledge();

            assertNotNull(message2);
            
            session.commit();

            try
            {
               assertEqualsByteArrays(body.length, body, message2.getBody().array());
            }
            catch (AssertionFailedError e)
            {
               log.info("Expected buffer:" + dumbBytesHex(body, 40));
               log.info("Arriving buffer:" + dumbBytesHex(message2.getBody().array(), 40));
               throw e;
            }
         }

         consumer.close();

         session.close();

         assertEquals(0, server.getPostOffice().getPagingManager().getTotalMemory());

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
    */
   public void testDepageDuringTransaction() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      HornetQServer server = createServer(true, config, PAGE_SIZE, PAGE_MAX, new HashMap<String, AddressSettings>());

      server.start();

      final int numberOfIntegers = 256;

      try
      {
         ClientSessionFactory sf = createInVMFactory();

         sf.setBlockOnNonPersistentSend(true);
         sf.setBlockOnPersistentSend(true);
         sf.setBlockOnAcknowledge(true);

         ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

         session.createQueue(ADDRESS, ADDRESS, null, true);

         ClientProducer producer = session.createProducer(ADDRESS);

         HornetQBuffer bodyLocal = ChannelBuffers.buffer(DataConstants.SIZE_INT * numberOfIntegers);

         ClientMessage message = null;

         int numberOfMessages = 0;
         while (true)
         {
            message = session.createClientMessage(true);
            message.setBody(bodyLocal);

            // Stop sending message as soon as we start paging
            if (server.getPostOffice().getPagingManager().getPageStore(ADDRESS).isPaging())
            {
               break;
            }
            numberOfMessages++;

            producer.send(message);
         }

         assertTrue(server.getPostOffice().getPagingManager().getPageStore(ADDRESS).isPaging());

         session.start();

         ClientSession sessionTransacted = sf.createSession(null, null, false, false, false, false, 0);

         ClientProducer producerTransacted = sessionTransacted.createProducer(ADDRESS);

         for (int i = 0; i < 10; i++)
         {
            message = session.createClientMessage(true);
            message.setBody(bodyLocal);
            message.putIntProperty(new SimpleString("id"), i);

            // Consume messages to force an eventual out of order delivery
            if (i == 5)
            {
               ClientConsumer consumer = session.createConsumer(ADDRESS);
               for (int j = 0; j < numberOfMessages; j++)
               {
                  ClientMessage msg = consumer.receive(RECEIVE_TIMEOUT);
                  msg.acknowledge();
                  assertNotNull(msg);
               }

               assertNull(consumer.receiveImmediate());
               consumer.close();
            }

            Integer messageID = (Integer)message.getObjectProperty(new SimpleString("id"));
            assertNotNull(messageID);
            assertEquals(messageID.intValue(), i);

            producerTransacted.send(message);
         }

         ClientConsumer consumer = session.createConsumer(ADDRESS);

         assertNull(consumer.receiveImmediate());

         sessionTransacted.commit();

         sessionTransacted.close();

         for (int i = 0; i < 10; i++)
         {
            message = consumer.receive(RECEIVE_TIMEOUT);

            assertNotNull(message);

            Integer messageID = (Integer)message.getObjectProperty(new SimpleString("id"));

            assertNotNull(messageID);
            assertEquals("message received out of order", messageID.intValue(), i);

            message.acknowledge();
         }

         assertNull(consumer.receiveImmediate());

         consumer.close();

         session.close();

         assertEquals(0, server.getPostOffice().getPagingManager().getTotalMemory());

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

      HornetQServer server = createServer(true, config, PAGE_SIZE, PAGE_MAX, new HashMap<String, AddressSettings>());

      server.start();

      final int numberOfMessages = 10000;

      final int numberOfBytes = 1024;

      try
      {
         ClientSessionFactory sf = createInVMFactory();

         sf.setBlockOnNonPersistentSend(true);
         sf.setBlockOnPersistentSend(true);
         sf.setBlockOnAcknowledge(true);

         ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

         session.createQueue(ADDRESS, ADDRESS, null, true);

         ClientProducer producer = session.createProducer(ADDRESS);

         ClientMessage message = null;

         byte[] body = new byte[numberOfBytes];

         for (int j = 0; j < numberOfBytes; j++)
         {
            body[j] = getSamplebyte(j);
         }

         long scheduledTime = System.currentTimeMillis() + 5000;

         for (int i = 0; i < numberOfMessages; i++)
         {
            message = session.createClientMessage(true);

            message.setBody(ChannelBuffers.wrappedBuffer(body));
            message.putIntProperty(new SimpleString("id"), i);

            TestSupportPageStore store = (TestSupportPageStore)server.getPostOffice()
                                                                     .getPagingManager()
                                                                     .getPageStore(ADDRESS);

            // Worse scenario possible... only schedule what's on pages
            if (store.getCurrentPage() != null)
            {
               message.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, scheduledTime);
            }

            producer.send(message);
         }

         if (restart)
         {
            session.close();

            server.stop();

            server = createServer(true, config, PAGE_SIZE, PAGE_MAX, new HashMap<String, AddressSettings>());
            server.start();

            sf = createInVMFactory();

            session = sf.createSession(null, null, false, true, true, false, 0);
         }

         ClientConsumer consumer = session.createConsumer(ADDRESS);

         session.start();

         for (int i = 0; i < numberOfMessages; i++)
         {

            ClientMessage message2 = consumer.receive(RECEIVE_TIMEOUT);

            assertNotNull(message2);

            message2.acknowledge();

            assertNotNull(message2);

            Long scheduled = (Long)message2.getObjectProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME);
            if (scheduled != null)
            {
               assertTrue("Scheduling didn't work", System.currentTimeMillis() >= scheduledTime);
            }

            try
            {
               assertEqualsByteArrays(body.length, body, message2.getBody().array());
            }
            catch (AssertionFailedError e)
            {
               log.info("Expected buffer:" + dumbBytesHex(body, 40));
               log.info("Arriving buffer:" + dumbBytesHex(message2.getBody().array(), 40));
               throw e;
            }
         }

         consumer.close();

         session.close();

         assertEquals(0, server.getPostOffice().getPagingManager().getTotalMemory());

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

      HornetQServer server = createServer(true, config, PAGE_SIZE, PAGE_MAX, new HashMap<String, AddressSettings>());

      server.start();

      final int numberOfIntegers = 256;

      final int numberOfMessages = 10;

      try
      {
         ClientSessionFactory sf = createInVMFactory();

         sf.setBlockOnNonPersistentSend(true);
         sf.setBlockOnPersistentSend(true);
         sf.setBlockOnAcknowledge(true);

         ClientSession session = sf.createSession(null, null, false, false, true, false, 0);

         session.createQueue(ADDRESS, ADDRESS, null, true);

         ClientProducer producer = session.createProducer(ADDRESS);

         long initialSize = server.getPostOffice().getPagingManager().getTotalMemory();

         ClientMessage message = null;

         for (int i = 0; i < numberOfMessages; i++)
         {
            message = session.createClientMessage(true);

            HornetQBuffer bodyLocal = message.getBody();

            for (int j = 1; j <= numberOfIntegers; j++)
            {
               bodyLocal.writeInt(j);
            }

            message.putIntProperty(new SimpleString("id"), i);

            producer.send(message);
         }

         session.rollback();

         ClientConsumer consumer = session.createConsumer(ADDRESS);

         session.start();

         assertNull(consumer.receiveImmediate());

         session.close();

         assertEquals(initialSize, server.getPostOffice().getPagingManager().getTotalMemory());
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

      HornetQServer server = createServer(true, config, PAGE_SIZE, PAGE_MAX, new HashMap<String, AddressSettings>());

      server.start();

      final int numberOfIntegers = 10;

      final int numberOfMessages = 10;

      try
      {
         ClientSessionFactory sf = createInVMFactory();

         sf.setBlockOnNonPersistentSend(true);
         sf.setBlockOnPersistentSend(true);
         sf.setBlockOnAcknowledge(true);

         ClientSession session = sf.createSession(null, null, false, false, false, false, 0);

         session.createQueue(ADDRESS, ADDRESS, null, true);

         ClientProducer producer = session.createProducer(ADDRESS);

         long initialSize = server.getPostOffice().getPagingManager().getTotalMemory();

         ClientMessage message = null;

         for (int i = 0; i < numberOfMessages; i++)
         {
            message = session.createClientMessage(true);

            HornetQBuffer bodyLocal = message.getBody();

            for (int j = 1; j <= numberOfIntegers; j++)
            {
               bodyLocal.writeInt(j);
            }

            message.putIntProperty(new SimpleString("id"), i);

            producer.send(message);
         }

         session.commit();

         ClientConsumer consumer = session.createConsumer(ADDRESS);

         session.start();
         for (int i = 0; i < numberOfMessages; i++)
         {
            ClientMessage msg = consumer.receive(500);
            assertNotNull(msg);
            msg.acknowledge();
         }

         session.commit();

         session.close();

         assertEquals(initialSize, server.getPostOffice().getPagingManager().getTotalMemory());
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

      settings.put(ADDRESS.toString(), set);

      HornetQServer server = createServer(true, config, 1024, 10 * 1024, settings);

      server.start();

      final int numberOfMessages = 1000;

      try
      {
         ClientSessionFactory sf = createInVMFactory();

         sf.setBlockOnNonPersistentSend(true);
         sf.setBlockOnPersistentSend(true);
         sf.setBlockOnAcknowledge(true);

         ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

         session.createQueue(ADDRESS, ADDRESS, null, true);

         ClientProducer producer = session.createProducer(ADDRESS);

         ClientMessage message = null;

         for (int i = 0; i < numberOfMessages; i++)
         {
            HornetQBuffer bodyLocal = ChannelBuffers.wrappedBuffer(new byte[1024]);

            message = session.createClientMessage(true);
            message.setBody(bodyLocal);

            producer.send(message);
         }

         ClientConsumer consumer = session.createConsumer(ADDRESS);

         session.start();

         for (int i = 0; i < 9; i++)
         {
            ClientMessage message2 = consumer.receive(RECEIVE_TIMEOUT);

            assertNotNull(message2);

            message2.acknowledge();
         }

         assertNull(consumer.receiveImmediate());

         assertEquals(0, server.getPostOffice().getPagingManager().getTotalMemory());
         assertEquals(0, server.getPostOffice().getPagingManager().getPageStore(ADDRESS).getAddressSize());

         for (int i = 0; i < numberOfMessages; i++)
         {
            HornetQBuffer bodyLocal = ChannelBuffers.wrappedBuffer(new byte[1024]);

            message = session.createClientMessage(true);
            message.setBody(bodyLocal);

            producer.send(message);
         }

         for (int i = 0; i < 9; i++)
         {
            ClientMessage message2 = consumer.receive(RECEIVE_TIMEOUT);

            assertNotNull(message2);

            message2.acknowledge();
         }

         assertNull(consumer.receiveImmediate());

         session.close();

         session = sf.createSession(false, true, true);

         producer = session.createProducer(ADDRESS);

         for (int i = 0; i < numberOfMessages; i++)
         {
            HornetQBuffer bodyLocal = ChannelBuffers.wrappedBuffer(new byte[1024]);

            message = session.createClientMessage(true);
            message.setBody(bodyLocal);

            producer.send(message);
         }

         session.commit();

         consumer = session.createConsumer(ADDRESS);

         session.start();

         for (int i = 0; i < 9; i++)
         {
            ClientMessage message2 = consumer.receive(RECEIVE_TIMEOUT);

            assertNotNull(message2);

            message2.acknowledge();
         }

         session.commit();

         assertNull(consumer.receiveImmediate());

         session.close();

         assertEquals(0, server.getPostOffice().getPagingManager().getTotalMemory());
         assertEquals(0, server.getPostOffice().getPagingManager().getPageStore(ADDRESS).getAddressSize());

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

   private void internalTestPageMultipleDestinations(boolean transacted) throws Exception
   {
      Configuration config = createDefaultConfig();

      final int NUMBER_OF_BINDINGS = 100;

      int NUMBER_OF_MESSAGES = 2;

      HornetQServer server = createServer(true, config, PAGE_SIZE, PAGE_MAX, new HashMap<String, AddressSettings>());

      server.start();

      try
      {
         ClientSessionFactory sf = createInVMFactory();

         sf.setBlockOnNonPersistentSend(true);
         sf.setBlockOnPersistentSend(true);
         sf.setBlockOnAcknowledge(true);

         ClientSession session = sf.createSession(null, null, false, !transacted, true, false, 0);

         for (int i = 0; i < NUMBER_OF_BINDINGS; i++)
         {
            session.createQueue(ADDRESS, new SimpleString("someQueue" + i), null, true);
         }

         ClientProducer producer = session.createProducer(ADDRESS);

         ClientMessage message = null;

         HornetQBuffer bodyLocal = ChannelBuffers.wrappedBuffer(new byte[1024]);

         message = session.createClientMessage(true);
         message.setBody(bodyLocal);

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

         server = createServer(true, config, PAGE_SIZE, PAGE_MAX, new HashMap<String, AddressSettings>());
         server.start();

         sf = createInVMFactory();

         assertTrue(server.getPostOffice().getPagingManager().getTotalMemory() > 0);

         session = sf.createSession(null, null, false, true, true, false, 0);

         session.start();

         for (int msg = 0; msg < NUMBER_OF_MESSAGES; msg++)
         {

            for (int i = 0; i < NUMBER_OF_BINDINGS; i++)
            {
               ClientConsumer consumer = session.createConsumer(new SimpleString("someQueue" + i));

               ClientMessage message2 = consumer.receive(RECEIVE_TIMEOUT);

               assertNotNull(message2);

               message2.acknowledge();

               assertNotNull(message2);

               consumer.close();

            }
         }

         session.close();

         for (int i = 0; i < NUMBER_OF_BINDINGS; i++)
         {
            Queue queue = (Queue)server.getPostOffice().getBinding(new SimpleString("someQueue" + i)).getBindable();

            assertEquals("Queue someQueue" + i + " was supposed to be empty", 0, queue.getMessageCount());
            assertEquals("Queue someQueue" + i + " was supposed to be empty", 0, queue.getDeliveringCount());
         }

         assertEquals("There are pending messages on the server", 0, server.getPostOffice()
                                                                           .getPagingManager()
                                                                           .getTotalMemory());

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

         ClientSessionFactory sf = createInVMFactory();

         ClientSession session = sf.createSession(false, true, false);

         session.createQueue(PAGED_ADDRESS, PAGED_ADDRESS, true);

         session.createQueue(NON_PAGED_ADDRESS, NON_PAGED_ADDRESS, true);

         ClientProducer producerPaged = session.createProducer(PAGED_ADDRESS);
         ClientProducer producerNonPaged = session.createProducer(NON_PAGED_ADDRESS);

         int NUMBER_OF_MESSAGES = 100;

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
         {
            ClientMessage msg = session.createClientMessage(true);
            msg.getBody().writeBytes(new byte[512]);

            producerPaged.send(msg);
            producerNonPaged.send(msg);
         }

         session.close();

         assertTrue(server.getPostOffice().getPagingManager().getPageStore(PAGED_ADDRESS).isPaging());
         assertFalse(server.getPostOffice().getPagingManager().getPageStore(NON_PAGED_ADDRESS).isPaging());

         session = sf.createSession(false, true, false);

         session.start();

         ClientConsumer consumerNonPaged = session.createConsumer(NON_PAGED_ADDRESS);
         ClientConsumer consumerPaged = session.createConsumer(PAGED_ADDRESS);

         ClientMessage ackList[] = new ClientMessage[NUMBER_OF_MESSAGES];

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
         {
            ClientMessage msg = consumerNonPaged.receive(5000);
            assertNotNull(msg);
            ackList[i] = msg;
         }

         assertNull(consumerNonPaged.receiveImmediate());

         consumerNonPaged.close();

         for (ClientMessage ack : ackList)
         {
            ack.acknowledge();
         }

         session.commit();

         ackList = null;

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
         {
            ClientMessage msg = consumerPaged.receive(5000);
            assertNotNull(msg);
            msg.acknowledge();
            session.commit();
         }

         assertNull(consumerPaged.receiveImmediate());

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

      int NUMBER_MESSAGES_BEFORE_PAGING = 20;

      addresses.put(PAGED_ADDRESS_A.toString(), pagedDestinationA);

      AddressSettings pagedDestinationB = new AddressSettings();
      pagedDestinationB.setPageSizeBytes(2024);
      pagedDestinationB.setMaxSizeBytes(20 * 1024);

      addresses.put(PAGED_ADDRESS_B.toString(), pagedDestinationB);

      HornetQServer server = createServer(true, configuration, -1, -1, addresses);

      try
      {
         server.start();

         ClientSessionFactory sf = createInVMFactory();

         ClientSession session = sf.createSession(false, true, false);

         session.createQueue(PAGED_ADDRESS_A, PAGED_ADDRESS_A, true);

         session.createQueue(PAGED_ADDRESS_B, PAGED_ADDRESS_B, true);

         ClientProducer producerA = session.createProducer(PAGED_ADDRESS_A);
         ClientProducer producerB = session.createProducer(PAGED_ADDRESS_B);

         int NUMBER_OF_MESSAGES = 100;

         for (int i = 0; i < NUMBER_MESSAGES_BEFORE_PAGING; i++)
         {
            ClientMessage msg = session.createClientMessage(true);
            msg.getBody().writeBytes(new byte[512]);

            producerA.send(msg);
            producerB.send(msg);
         }

         session.commit(); // commit was called to clean the buffer only (making sure everything is on the server side)

         assertTrue(server.getPostOffice().getPagingManager().getPageStore(PAGED_ADDRESS_A).isPaging());
         assertFalse(server.getPostOffice().getPagingManager().getPageStore(PAGED_ADDRESS_B).isPaging());

         for (int i = 0; i < NUMBER_MESSAGES_BEFORE_PAGING; i++)
         {
            ClientMessage msg = session.createClientMessage(true);
            msg.getBody().writeBytes(new byte[512]);

            producerA.send(msg);
            producerB.send(msg);
         }

         session.commit(); // commit was called to clean the buffer only (making sure everything is on the server side)

         assertTrue(server.getPostOffice().getPagingManager().getPageStore(PAGED_ADDRESS_A).isPaging());
         assertTrue(server.getPostOffice().getPagingManager().getPageStore(PAGED_ADDRESS_B).isPaging());

         for (int i = NUMBER_MESSAGES_BEFORE_PAGING * 2; i < NUMBER_OF_MESSAGES; i++)
         {
            ClientMessage msg = session.createClientMessage(true);
            msg.getBody().writeBytes(new byte[512]);

            producerA.send(msg);
            producerB.send(msg);
         }

         session.close();

         assertTrue(server.getPostOffice().getPagingManager().getPageStore(PAGED_ADDRESS_A).isPaging());
         assertTrue(server.getPostOffice().getPagingManager().getPageStore(PAGED_ADDRESS_B).isPaging());

         session = sf.createSession(null, null, false, true, true, false, 0);

         session.start();

         ClientConsumer consumerA = session.createConsumer(PAGED_ADDRESS_A);

         ClientConsumer consumerB = session.createConsumer(PAGED_ADDRESS_B);

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
         {
            ClientMessage msg = consumerA.receive(5000);
            assertNotNull("Couldn't receive a message on consumerA, iteration = " + i, msg);
            msg.acknowledge();
         }

         assertNull(consumerA.receiveImmediate());

         consumerA.close();

         assertTrue(server.getPostOffice().getPagingManager().getPageStore(PAGED_ADDRESS_B).isPaging());

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
         {
            ClientMessage msg = consumerB.receive(5000);
            assertNotNull(msg);
            msg.acknowledge();
            session.commit();
         }

         assertNull(consumerB.receiveImmediate());

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

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected Configuration createDefaultConfig()
   {
      Configuration config = super.createDefaultConfig();
      config.setJournalSyncNonTransactional(false);
      return config;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
