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
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.paging.cursor.PageSubscription;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.wireformat.SessionContinuationMessage;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.QueueFactory;
import org.hornetq.core.server.ServerSession;
import org.hornetq.core.server.impl.HornetQServerImpl;
import org.hornetq.core.server.impl.QueueImpl;
import org.hornetq.core.server.impl.ServerSessionImpl;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.tests.integration.IntegrationTestLogger;
import org.hornetq.tests.integration.largemessage.LargeMessageTestBase;
import org.hornetq.utils.ExecutorFactory;
import org.junit.Before;
import org.junit.Test;

/**
 * A LargeMessageTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 * Created 29-Sep-08 4:04:10 PM
 *
 *
 */
public class InterruptedLargeMessageTest extends LargeMessageTestBase
{
   // Constants -----------------------------------------------------

   final static int RECEIVE_WAIT_TIME = 60000;

   private final int LARGE_MESSAGE_SIZE = HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE * 3;

   private final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   protected ServerLocator locator;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      LargeMessageTestInterceptorIgnoreLastPacket.clearInterrupt();
      clearData();
      locator = createFactory(isNetty());
   }

   protected boolean isNetty()
   {
      return false;
   }

   @Test
   public void testInterruptLargeMessageSend() throws Exception
   {

      ClientSession session = null;

      LargeMessageTestInterceptorIgnoreLastPacket.clearInterrupt();
      HornetQServer server = createServer(true, isNetty());

      server.getConfiguration()
         .getIncomingInterceptorClassNames()
         .add(LargeMessageTestInterceptorIgnoreLastPacket.class.getName());

      server.start();

      locator.setBlockOnNonDurableSend(false);
      locator.setBlockOnDurableSend(false);

      ClientSessionFactory sf = createSessionFactory(locator);

      session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, ADDRESS, true);

      ClientProducer producer = session.createProducer(ADDRESS);

      Message clientFile = createLargeClientMessage(session, LARGE_MESSAGE_SIZE, true);

      clientFile.setExpiration(System.currentTimeMillis());

      producer.send(clientFile);

      Thread.sleep(500);

      for (ServerSession srvSession : server.getSessions())
      {
         ((ServerSessionImpl)srvSession).clearLargeMessage();
      }

      server.stop(false);

      forceGC();

      server.start();

      server.stop();

      validateNoFilesOnLargeDir();
   }

   @Test
   public void testCloseConsumerDuringTransmission() throws Exception
   {
      HornetQServer server = createServer(true, isNetty());

      LargeMessageTestInterceptorIgnoreLastPacket.disableInterrupt();

      server.start();

      locator.setBlockOnNonDurableSend(false);
      locator.setBlockOnDurableSend(false);
      locator.addIncomingInterceptor(new LargeMessageTestInterceptorIgnoreLastPacket());

      ClientSessionFactory sf = createSessionFactory(locator);

      final ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, ADDRESS, true);

      ClientProducer producer = session.createProducer(ADDRESS);

      Message clientFile = createLargeClientMessage(session, LARGE_MESSAGE_SIZE, true);

      producer.send(clientFile);

      session.commit();

      LargeMessageTestInterceptorIgnoreLastPacket.clearInterrupt();

      final AtomicInteger unexpectedErrors = new AtomicInteger(0);
      final AtomicInteger expectedErrors = new AtomicInteger(0);
      final ClientConsumer cons = session.createConsumer(ADDRESS);

      session.start();

      Thread t = new Thread()
      {
         @Override
         public void run()
         {
            try
            {
               System.out.println("Receiving message");
               ClientMessage msg = cons.receive(5000);
               if (msg == null)
               {
                  System.err.println("Message not received");
                  unexpectedErrors.incrementAndGet();
                  return;
               }

               msg.checkCompletion();
            }
            catch (HornetQException e)
            {
               e.printStackTrace();
               expectedErrors.incrementAndGet();
            }
         }
      };

      t.start();

      LargeMessageTestInterceptorIgnoreLastPacket.awaitInterrupt();

      cons.close();

      t.join();

      assertEquals(0, unexpectedErrors.get());
      assertEquals(1, expectedErrors.get());

      session.close();

      server.stop();
   }

   @Test
   public void testSendNonPersistentQueue() throws Exception
   {


      ClientSession session = null;

      LargeMessageTestInterceptorIgnoreLastPacket.disableInterrupt();
      HornetQServer server = createServer(true, isNetty());

      server.start();

      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);

      ClientSessionFactory sf = createSessionFactory(locator);

      session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, ADDRESS, false);

      ClientProducer producer = session.createProducer(ADDRESS);

      for (int i = 0; i < 10; i++)
      {
         Message clientFile = createLargeClientMessage(session, LARGE_MESSAGE_SIZE, true);

         producer.send(clientFile);
      }
      session.commit();

      session.close();

      session = sf.createSession(false, false);

      ClientConsumer cons = session.createConsumer(ADDRESS);

      session.start();

      for (int h = 0; h < 5; h++)
      {
         for (int i = 0; i < 10; i++)
         {
            ClientMessage clientMessage = cons.receive(5000);
            assertNotNull(clientMessage);
            for (int countByte = 0; countByte < LARGE_MESSAGE_SIZE; countByte++)
            {
               assertEquals(getSamplebyte(countByte), clientMessage.getBodyBuffer().readByte());
            }
            clientMessage.acknowledge();
         }
         session.rollback();
      }

      server.stop(false);
      server.start();

      server.stop();

      validateNoFilesOnLargeDir();
   }

   @Test
   public void testSendPaging() throws Exception
   {


      ClientSession session = null;

      LargeMessageTestInterceptorIgnoreLastPacket.disableInterrupt();
      HornetQServer server =
         createServer(true, createDefaultConfig(isNetty()), 10000, 20000, new HashMap<String, AddressSettings>());

      // server.getConfiguration()
      // .getIncomingInterceptorClassNames()
      // .add(LargeMessageTestInterceptorIgnoreLastPacket.class.getName());

      server.start();

      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);

      ClientSessionFactory sf = createSessionFactory(locator);

      session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, ADDRESS, true);

      server.getPagingManager().getPageStore(ADDRESS).startPaging();

      ClientProducer producer = session.createProducer(ADDRESS);

      for (int i = 0; i < 10; i++)
      {
         Message clientFile = createLargeClientMessage(session, LARGE_MESSAGE_SIZE, true);

         producer.send(clientFile);
      }
      session.commit();

      validateNoFilesOnLargeDir(10);

      for (int h = 0; h < 5; h++)
      {
         session.close();

         sf.close();

         server.stop();

         server.start();

         sf = createSessionFactory(locator);

         session = sf.createSession(false, false);

         ClientConsumer cons = session.createConsumer(ADDRESS);

         session.start();

         for (int i = 0; i < 10; i++)
         {
            ClientMessage clientMessage = cons.receive(5000);
            assertNotNull(clientMessage);
            for (int countByte = 0; countByte < LARGE_MESSAGE_SIZE; countByte++)
            {
               assertEquals(getSamplebyte(countByte), clientMessage.getBodyBuffer().readByte());
            }
            clientMessage.acknowledge();
         }
         if (h == 4)
         {
            session.commit();
         }
         else
         {
            session.rollback();
         }

         session.close();
         sf.close();
      }

      server.stop(false);
      server.start();

      validateNoFilesOnLargeDir();

   }

   @Test
   public void testSendPreparedXA() throws Exception
   {


      ClientSession session = null;

      LargeMessageTestInterceptorIgnoreLastPacket.disableInterrupt();

      HornetQServer server =
         createServer(true, createDefaultConfig(isNetty()), 10000, 20000, new HashMap<String, AddressSettings>());

      server.getConfiguration()
       .getIncomingInterceptorClassNames()
       .add(LargeMessageTestInterceptorIgnoreLastPacket.class.getName());

      server.start();

      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);

      ClientSessionFactory sf = createSessionFactory(locator);

      session = sf.createSession(true, false, false);

      Xid xid1 = newXID();
      Xid xid2 = newXID();

      session.createQueue(ADDRESS, ADDRESS, true);

      ClientProducer producer = session.createProducer(ADDRESS);

      session.start(xid1, XAResource.TMNOFLAGS);

      for (int i = 0; i < 10; i++)
      {
         Message clientFile = createLargeClientMessage(session, LARGE_MESSAGE_SIZE, true);
         clientFile.putIntProperty("txid", 1);
         producer.send(clientFile);
      }
      session.end(xid1, XAResource.TMSUCCESS);

      session.prepare(xid1);

      session.start(xid2, XAResource.TMNOFLAGS);

      for (int i = 0; i < 10; i++)
      {
         Message clientFile = createLargeClientMessage(session, LARGE_MESSAGE_SIZE, true);
         clientFile.putIntProperty("txid", 2);
         clientFile.putIntProperty("i", i);
         producer.send(clientFile);
      }
      session.end(xid2, XAResource.TMSUCCESS);

      session.prepare(xid2);

      session.close();
      sf.close();

      server.stop(false);
      server.start();

      for (int start = 0; start < 2; start++)
      {
         System.out.println("Start " + start);

         sf = createSessionFactory(locator);

         if (start == 0)
         {
            session = sf.createSession(true, false, false);
            session.commit(xid1, false);
            session.close();
         }

         session = sf.createSession(false, false, false);
         ClientConsumer cons1 = session.createConsumer(ADDRESS);
         session.start();
         for (int i = 0; i < 10; i++)
         {
            log.info("I = " + i);
            ClientMessage msg = cons1.receive(5000);
            assertNotNull(msg);
            assertEquals(1, msg.getIntProperty("txid").intValue());
            msg.acknowledge();
         }

         if (start == 1)
         {
            session.commit();
         }
         else
         {
            session.rollback();
         }

         session.close();
         sf.close();

         server.stop();
         server.start();
      }
      server.stop();

      validateNoFilesOnLargeDir(10);

      server.start();

      sf = createSessionFactory(locator);

      session = sf.createSession(true, false, false);
      session.rollback(xid2);

      sf.close();

      server.stop();
      server.start();
      server.stop();

      validateNoFilesOnLargeDir();
   }

   @Test
   public void testRestartBeforeDelete() throws Exception
   {

      class NoPostACKQueue extends QueueImpl
      {

         public NoPostACKQueue(long id,
                               SimpleString address,
                               SimpleString name,
                               Filter filter,
                               PageSubscription pageSubscription,
                               boolean durable,
                               boolean temporary,
                               ScheduledExecutorService scheduledExecutor,
                               PostOffice postOffice,
                               StorageManager storageManager,
                               HierarchicalRepository<AddressSettings> addressSettingsRepository,
                               Executor executor)
         {
            super(id,
               address,
               name,
               filter,
               pageSubscription,
               durable,
               temporary,
               scheduledExecutor,
               postOffice,
               storageManager,
               addressSettingsRepository,
               executor);
         }

         @Override
         protected void postAcknowledge(final MessageReference ref)
         {
            System.out.println("Ignoring postACK on message " + ref);
         }
      }

      class NoPostACKQueueFactory implements QueueFactory
      {

         final StorageManager storageManager;

         final PostOffice postOffice;

         final ScheduledExecutorService scheduledExecutor;

         final HierarchicalRepository<AddressSettings> addressSettingsRepository;

         final ExecutorFactory execFactory;

         public NoPostACKQueueFactory(StorageManager storageManager,
                                      PostOffice postOffice,
                                      ScheduledExecutorService scheduledExecutor,
                                      HierarchicalRepository<AddressSettings> addressSettingsRepository,
                                      final ExecutorFactory execFactory)
         {
            this.storageManager = storageManager;
            this.postOffice = postOffice;
            this.scheduledExecutor = scheduledExecutor;
            this.addressSettingsRepository = addressSettingsRepository;
            this.execFactory = execFactory;
         }

         public Queue createQueue(long persistenceID,
                                  SimpleString address,
                                  SimpleString name,
                                  Filter filter,
                                  PageSubscription pageSubscription,
                                  boolean durable,
                                  boolean temporary)
         {

            return new NoPostACKQueue(persistenceID,
               address,
               name,
               filter,
               pageSubscription,
               durable,
               temporary,
               scheduledExecutor,
               postOffice,
               storageManager,
               addressSettingsRepository,
               execFactory.getExecutor());
         }

         /* (non-Javadoc)
          * @see org.hornetq.core.server.QueueFactory#setPostOffice(org.hornetq.core.postoffice.PostOffice)
          */
         public void setPostOffice(PostOffice postOffice)
         {
         }

      }


      ClientSession session = null;

      LargeMessageTestInterceptorIgnoreLastPacket.disableInterrupt();

      HornetQServer server = createServer(true, isNetty());
      server.start();

      QueueFactory original = server.getQueueFactory();

      ((HornetQServerImpl)server).replaceQueueFactory(new NoPostACKQueueFactory(server.getStorageManager(),
         server.getPostOffice(),
         server.getScheduledPool(),
         server.getAddressSettingsRepository(),
         server.getExecutorFactory()));

      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);

      ClientSessionFactory sf = createSessionFactory(locator);

      session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, ADDRESS, true);

      ClientProducer producer = session.createProducer(ADDRESS);

      for (int i = 0; i < 10; i++)
      {
         Message clientFile = createLargeClientMessage(session, LARGE_MESSAGE_SIZE, true);

         producer.send(clientFile);
      }
      session.commit();

      session.close();

      session = sf.createSession(false, false);

      ClientConsumer cons = session.createConsumer(ADDRESS);

      session.start();

      for (int i = 0; i < 10; i++)
      {
         ClientMessage msg = cons.receive(5000);
         assertNotNull(msg);
         msg.saveToOutputStream(new java.io.OutputStream()
         {
            @Override
            public void write(int b) throws IOException
            {
            }
         });
         msg.acknowledge();
         session.commit();
      }

      ((HornetQServerImpl)server).replaceQueueFactory(original);
      server.stop(false);
      server.start();

      server.stop();

      validateNoFilesOnLargeDir();
   }

   @Test
   public void testConsumeAfterRestart() throws Exception
   {
      ClientSession session = null;

      LargeMessageTestInterceptorIgnoreLastPacket.clearInterrupt();

      HornetQServer server = createServer(true, isNetty());
      server.start();

      QueueFactory original = server.getQueueFactory();

      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);

      ClientSessionFactory sf = createSessionFactory(locator);

      session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, ADDRESS, true);

      ClientProducer producer = session.createProducer(ADDRESS);

      for (int i = 0; i < 10; i++)
      {
         Message clientFile = createLargeClientMessage(session, LARGE_MESSAGE_SIZE, true);

         producer.send(clientFile);
      }
      session.commit();

      session.close();
      sf.close();

      server.stop();
      server.start();

      sf = createSessionFactory(locator);

      session = sf.createSession(false, false);

      ClientConsumer cons = session.createConsumer(ADDRESS);

      session.start();

      for (int i = 0; i < 10; i++)
      {
         ClientMessage msg = cons.receive(5000);
         assertNotNull(msg);
         msg.saveToOutputStream(new java.io.OutputStream()
         {
            @Override
            public void write(int b) throws IOException
            {
            }
         });
         msg.acknowledge();
         session.commit();
      }

      ((HornetQServerImpl)server).replaceQueueFactory(original);
      server.stop(false);
      server.start();

      server.stop();

      validateNoFilesOnLargeDir();
   }

   public static class LargeMessageTestInterceptorIgnoreLastPacket implements Interceptor
   {

      public static void clearInterrupt()
      {
         intMessages = true;
         latch = new CountDownLatch(1);
      }

      public static void disableInterrupt()
      {
         intMessages = false;
      }

      public static void awaitInterrupt() throws Exception
      {
         latch.await();
      }

      private static boolean intMessages = false;

      private static CountDownLatch latch = new CountDownLatch(1);

      @Override
      public boolean intercept(Packet packet, RemotingConnection connection) throws HornetQException
      {
         if (packet instanceof SessionContinuationMessage)
         {
            SessionContinuationMessage msg = (SessionContinuationMessage)packet;
            if (!msg.isContinues() && intMessages)
            {
               System.out.println("Ignored a message");
               latch.countDown();
               return false;
            }
         }
         return true;
      }
   }
}
