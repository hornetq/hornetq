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

import java.util.HashMap;
import java.util.Map;

import junit.framework.AssertionFailedError;

import org.jboss.messaging.core.buffers.ChannelBuffers;
import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.utils.DataConstants;
import org.jboss.messaging.utils.SimpleString;

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

   // Constants -----------------------------------------------------
   private static final Logger log = Logger.getLogger(PagingTest.class);

   private static final int RECEIVE_TIMEOUT = 30000;

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   static final SimpleString ADDRESS = new SimpleString("SimpleAddress");

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testSendReceivePaging() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      config.setPagingMaxGlobalSizeBytes(100 * 1024);
      config.setPagingGlobalWatermarkSize(10 * 1024);

      MessagingServer server = createServer(true, config, new HashMap<String, AddressSettings>());

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

            MessagingBuffer bodyLocal = message.getBody();

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

         server.stop();

         server = createServer(true, config, new HashMap<String, AddressSettings>());
         server.start();

         sf = createInVMFactory();

         assertTrue(server.getPostOffice().getPagingManager().getGlobalSize() > 0);

         session = sf.createSession(null, null, false, true, true, false, 0);

         ClientConsumer consumer = session.createConsumer(ADDRESS);

         session.start();

         for (int i = 0; i < numberOfMessages; i++)
         {
            ClientMessage message2 = consumer.receive(RECEIVE_TIMEOUT);

            assertNotNull(message2);

            assertEquals(i, ((Integer)message2.getProperty(new SimpleString("id"))).intValue());

            message2.acknowledge();

            assertNotNull(message2);

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

         assertEquals(0, server.getPostOffice().getPagingManager().getGlobalSize());

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

      config.setPagingMaxGlobalSizeBytes(100 * 1024);
      config.setPagingGlobalWatermarkSize(10 * 1024);

      MessagingServer server = createServer(true, config, new HashMap<String, AddressSettings>());

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

         MessagingBuffer bodyLocal = ChannelBuffers.buffer(DataConstants.SIZE_INT * numberOfIntegers);

         ClientMessage message = null;

         int numberOfMessages = 0;
         while (true)
         {
            message = session.createClientMessage(true);
            message.setBody(bodyLocal);

            // Stop sending message as soon as we start paging
            if (server.getPostOffice().getPagingManager().isPaging(ADDRESS))
            {
               break;
            }
            numberOfMessages++;

            producer.send(message);
         }

         assertTrue(server.getPostOffice().getPagingManager().isPaging(ADDRESS));

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

               assertNull(consumer.receive(100));
               consumer.close();
            }

            Integer messageID = (Integer)message.getProperty(new SimpleString("id"));
            assertNotNull(messageID);
            assertEquals(messageID.intValue(), i);

            producerTransacted.send(message);
         }

         ClientConsumer consumer = session.createConsumer(ADDRESS);

         assertNull(consumer.receive(100));

         sessionTransacted.commit();

         sessionTransacted.close();

         for (int i = 0; i < 10; i++)
         {
            message = consumer.receive(RECEIVE_TIMEOUT);

            assertNotNull(message);

            Integer messageID = (Integer)message.getProperty(new SimpleString("id"));

            assertNotNull(messageID);
            assertEquals("message received out of order", messageID.intValue(), i);

            message.acknowledge();
         }

         assertNull(consumer.receive(100));

         consumer.close();

         session.close();

         assertEquals(0, server.getPostOffice().getPagingManager().getGlobalSize());

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

      config.setPagingMaxGlobalSizeBytes(100 * 1024);
      config.setPagingGlobalWatermarkSize(10 * 1024);

      MessagingServer server = createServer(true, config, new HashMap<String, AddressSettings>());

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

         long scheduledTime = System.currentTimeMillis() + 5000;

         for (int i = 0; i < numberOfMessages; i++)
         {
            message = session.createClientMessage(true);

            MessagingBuffer bodyLocal = message.getBody();

            for (int j = 1; j <= numberOfIntegers; j++)
            {
               bodyLocal.writeInt(j);
            }

            if (body == null)
            {
               body = bodyLocal.array();
            }

            message.setBody(bodyLocal);
            message.putIntProperty(new SimpleString("id"), i);

            // Worse scenario possible... only schedule what's on pages
            if (server.getPostOffice().getPagingManager().isPaging(ADDRESS))
            {
               message.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, scheduledTime);
            }

            producer.send(message);
         }

         if (restart)
         {
            session.close();

            server.stop();

            server = createServer(true, config, new HashMap<String, AddressSettings>());
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

            Long scheduled = (Long)message2.getProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME);
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

         assertEquals(0, server.getPostOffice().getPagingManager().getGlobalSize());

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

      config.setPagingMaxGlobalSizeBytes(100 * 1024);
      config.setPagingGlobalWatermarkSize(10 * 1024);

      MessagingServer server = createServer(true, config, new HashMap<String, AddressSettings>());

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

         long initialSize = server.getPostOffice().getPagingManager().getGlobalSize();

         ClientMessage message = null;

         for (int i = 0; i < numberOfMessages; i++)
         {
            message = session.createClientMessage(true);

            MessagingBuffer bodyLocal = message.getBody();

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

         assertNull(consumer.receive(100));

         session.close();

         assertEquals(initialSize, server.getPostOffice().getPagingManager().getGlobalSize());
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

      config.setPagingMaxGlobalSizeBytes(100 * 1024);
      config.setPagingGlobalWatermarkSize(10 * 1024);

      MessagingServer server = createServer(true, config, new HashMap<String, AddressSettings>());

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

         long initialSize = server.getPostOffice().getPagingManager().getGlobalSize();

         ClientMessage message = null;

         for (int i = 0; i < numberOfMessages; i++)
         {
            message = session.createClientMessage(true);

            MessagingBuffer bodyLocal = message.getBody();

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

         assertEquals(initialSize, server.getPostOffice().getPagingManager().getGlobalSize());
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

   public void testDropMessagesQueueMax() throws Exception
   {
      testDropMessages(false);
   }

   public void testDropMessagesGlobalMax() throws Exception
   {
      testDropMessages(true);
   }

   private void testDropMessages(boolean global) throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      HashMap<String, AddressSettings> settings = new HashMap<String, AddressSettings>();

      AddressSettings set = new AddressSettings();
      set.setDropMessagesWhenFull(true);

      settings.put(ADDRESS.toString(), set);

      if (global)
      {
         set.setMaxSizeBytes(-1);
         config.setPagingMaxGlobalSizeBytes(10 * 1024);
      }
      else
      {
         config.setPagingMaxGlobalSizeBytes(-1);
         set.setMaxSizeBytes(10 * 1024);
      }

      config.setPagingGlobalWatermarkSize(10 * 1024);

      MessagingServer server = createServer(true, config, settings);

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
            MessagingBuffer bodyLocal = ChannelBuffers.wrappedBuffer(new byte[1024]);

            message = session.createClientMessage(true);
            message.setBody(bodyLocal);

            producer.send(message);
         }

         session = sf.createSession(null, null, false, true, true, false, 0);

         ClientConsumer consumer = session.createConsumer(ADDRESS);

         session.start();

         for (int i = 0; i < 9; i++)
         {
            ClientMessage message2 = consumer.receive(RECEIVE_TIMEOUT);

            assertNotNull(message2);

            message2.acknowledge();
         }

         assertNull(consumer.receive(100));

         assertEquals(0, server.getPostOffice().getPagingManager().getGlobalSize());
         assertEquals(0, server
                                         .getPostOffice()
                                         .getPagingManager()
                                         .getPageStore(ADDRESS)
                                         .getAddressSize());

         for (int i = 0; i < numberOfMessages; i++)
         {
            MessagingBuffer bodyLocal = ChannelBuffers.wrappedBuffer(new byte[1024]);

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

         assertNull(consumer.receive(100));

         session.close();

         session = sf.createSession(false, true, true);

         producer = session.createProducer(ADDRESS);

         for (int i = 0; i < numberOfMessages; i++)
         {
            MessagingBuffer bodyLocal = ChannelBuffers.wrappedBuffer(new byte[1024]);

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

         assertNull(consumer.receive(100));

         session.close();

         assertEquals(0, server.getPostOffice().getPagingManager().getGlobalSize());
         assertEquals(0, server
                                         .getPostOffice()
                                         .getPagingManager()
                                         .getPageStore(ADDRESS)
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

   private void internalTestPageMultipleDestinations(boolean transacted) throws Exception
   {
      Configuration config = createDefaultConfig();

      final int NUMBER_OF_BINDINGS = 100;

      int NUMBER_OF_MESSAGES = 2;

      config.setPagingMaxGlobalSizeBytes(100 * 1024);
      config.setPagingGlobalWatermarkSize(10 * 1024);

      MessagingServer server = createServer(true, config, new HashMap<String, AddressSettings>());

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

         MessagingBuffer bodyLocal = ChannelBuffers.wrappedBuffer(new byte[1024]);

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

         server = createServer(true, config, new HashMap<String, AddressSettings>());
         server.start();

         sf = createInVMFactory();

         assertTrue(server.getPostOffice().getPagingManager().getGlobalSize() > 0);

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
            Queue queue = (Queue)server
                                                 .getPostOffice()
                                                 .getBinding(new SimpleString("someQueue" + i))
                                                 .getBindable();

            assertEquals("Queue someQueue" + i + " was supposed to be empty", 0, queue.getMessageCount());
            assertEquals("Queue someQueue" + i + " was supposed to be empty", 0, queue.getDeliveringCount());
         }

         assertEquals("There are pending messages on the server", 0, server
                                                                                     .getPostOffice()
                                                                                     .getPagingManager()
                                                                                     .getGlobalSize());

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

      configuration.setPagingMaxGlobalSizeBytes(0);
      configuration.setPagingGlobalWatermarkSize(0);

      Map<String, AddressSettings> addresses = new HashMap<String, AddressSettings>();

      addresses.put("#", new AddressSettings());

      AddressSettings pagedDestination = new AddressSettings();
      pagedDestination.setPageSizeBytes(1024);
      pagedDestination.setMaxSizeBytes(10 * 1024);

      addresses.put(PAGED_ADDRESS.toString(), pagedDestination);

      MessagingServer server = createServer(true, configuration, addresses);

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

         assertFalse(server.getPostOffice().getPagingManager().getPageStore(PAGED_ADDRESS).isPaging());
         assertFalse(server.getPostOffice().getPagingManager().getPageStore(NON_PAGED_ADDRESS).isPaging());

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

      configuration.setPagingMaxGlobalSizeBytes(0);
      configuration.setPagingGlobalWatermarkSize(0);

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

      MessagingServer server = createServer(true, configuration, addresses);

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

   public void testPagingDifferentSizesAndGlobal() throws Exception
   {
      SimpleString PAGED_ADDRESS_A = new SimpleString("paged-a");
      SimpleString PAGED_ADDRESS_B = new SimpleString("paged-b");
      SimpleString PAGED_ADDRESS_GLOBAL = new SimpleString("paged-global");

      Configuration configuration = createDefaultConfig();

      configuration.setPagingMaxGlobalSizeBytes(30 * 1024);
      configuration.setPagingGlobalWatermarkSize(1024);

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

      MessagingServer server = createServer(true, configuration, addresses);

      try
      {
         server.start();

         ClientSessionFactory sf = createInVMFactory();

         ClientSession session = sf.createSession(false, true, false);

         session.createQueue(PAGED_ADDRESS_A, PAGED_ADDRESS_A, true);

         session.createQueue(PAGED_ADDRESS_B, PAGED_ADDRESS_B, true);

         session.createQueue(PAGED_ADDRESS_GLOBAL, PAGED_ADDRESS_GLOBAL, true);

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

         session.commit();

         assertFalse(server.getPostOffice().getPagingManager().getPageStore(PAGED_ADDRESS_A).isPaging());
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

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
