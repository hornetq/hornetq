/*
 * Copyright 2013 Red Hat, Inc.
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

import java.lang.management.ManagementFactory;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.core.journal.impl.NIOSequentialFileFactory;
import org.hornetq.core.paging.cursor.PageSubscription;
import org.hornetq.core.paging.cursor.impl.PageCursorProviderImpl;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.postoffice.impl.LocalQueueBinding;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.wireformat.SessionReceiveMessage;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.HornetQServerImpl;
import org.hornetq.core.server.impl.QueueFactoryImpl;
import org.hornetq.core.server.impl.QueueImpl;
import org.hornetq.core.server.impl.ServerSessionImpl;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.protocol.SessionCallback;
import org.hornetq.spi.core.remoting.ReadyListener;
import org.hornetq.spi.core.security.HornetQSecurityManager;
import org.hornetq.spi.core.security.HornetQSecurityManagerImpl;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.ExecutorFactory;
import org.hornetq.utils.ReusableLatch;

/**
 * This test will simulate a consumer hanging on the delivery packet due to unbehaved clients
 * and it will make sure we can still perform certain operations on the queue such as produce
 * and verify the counters
 */
public class HangConsumerTest extends ServiceTestBase
{

   private HornetQServer server;

   private final SimpleString QUEUE = new SimpleString("ConsumerTestQueue");

   private Queue queue;

   private ServerLocator locator;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      Configuration config = createDefaultConfig(false);
      
      config.setMessageExpiryScanPeriod(10);
      
      HornetQSecurityManager securityManager = new HornetQSecurityManagerImpl();

      config.setPersistenceEnabled(true);

      server = new MyHornetQServer(config, ManagementFactory.getPlatformMBeanServer(), securityManager);
      
      server.start();

      locator = createInVMNonHALocator();
   }
   
   protected void tearDown() throws Exception
   {
      server.stop();
      locator.close();
      super.tearDown();
   }

   public void testHangOnDelivery() throws Exception
   {
      queue = server.createQueue(QUEUE, QUEUE, null, true, false);
      try
      {

         ClientSessionFactory factory = locator.createSessionFactory();
         ClientSession sessionProducer = factory.createSession(false, false, false);
         
         ServerLocator consumerLocator = createInVMNonHALocator();
         ClientSessionFactory factoryConsumer = consumerLocator.createSessionFactory();
         ClientSession sessionConsumer = factoryConsumer.createSession();

         ClientProducer producer = sessionProducer.createProducer(QUEUE);

         ClientConsumer consumer = sessionConsumer.createConsumer(QUEUE);

         producer.send(sessionProducer.createMessage(true));
         
         blockConsumers();
         
         sessionProducer.commit();

         sessionConsumer.start();
         
         awaitBlocking();

         // this shouldn't lock
         producer.send(sessionProducer.createMessage(true));
         sessionProducer.commit();

         // These two operations should finish without the test hanging
         queue.getMessagesAdded(1);
         queue.getMessageCount(1);

         releaseConsumers();
         
         // a rollback to make sure everything will be reset on the deliveries
         // and that both consumers will receive each a message
         // this is to guarantee the server will have both consumers regsitered
         sessionConsumer.rollback();

         // a flush to guarantee any pending task is finished on flushing out delivery and pending msgs
         queue.flushExecutor();
         Assert.assertEquals(2, queue.getMessageCount());
         Assert.assertEquals(2, queue.getMessagesAdded());

         ClientMessage msg = consumer.receive(5000);
         Assert.assertNotNull(msg);
         msg.acknowledge();

         msg = consumer.receive(5000);
         Assert.assertNotNull(msg);
         msg.acknowledge();

         sessionProducer.commit();
         sessionConsumer.commit();
         
         sessionProducer.close();
         sessionConsumer.close();
      }
      finally
      {
         releaseConsumers();
      }
   }


   public void testHangOnPaging() throws Exception
   {
      AddressSettings setting = new AddressSettings();
      setting.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      setting.setMaxSizeBytes(100 * 1024);
      setting.setPageSizeBytes(10024);
      server.getAddressSettingsRepository().addMatch("#", setting);
      queue = server.createQueue(QUEUE, QUEUE, null, true, false);
      
      System.out.println("maxSize = " + queue.getPageSubscription().getPagingStore().getMaxSize());
      queue.getPageSubscription().getPagingStore().startPaging();
      try
      {

         locator.setBlockOnDurableSend(false);
         ClientSessionFactory factory = locator.createSessionFactory();
         ClientSession sessionProducer = factory.createSession(false, false, false);

         ServerLocator consumerLocator = createInVMNonHALocator();
         ClientSessionFactory factoryConsumer = consumerLocator.createSessionFactory();
         ClientSession sessionConsumer = factoryConsumer.createSession();

         ClientProducer producer = sessionProducer.createProducer(QUEUE);

         ClientConsumer consumer = sessionConsumer.createConsumer(QUEUE);

         for (int i = 0; i < 2000; i++)
         {
            ClientMessage msg = sessionProducer.createMessage(true);
            msg.getBodyBuffer().writeBytes(new byte[1024]);
            msg.setExpiration(System.currentTimeMillis() + 5000);
            producer.send(msg);
         }
         sessionProducer.commit();

         sessionConsumer.start();

         for (int i = 0; i < 500; i++)
         {
            ClientMessage msg = consumer.receive(5000);
            assertNotNull(msg);
            msg.acknowledge();
         }

         sessionConsumer.commit();
         for (int i = 0; i < 500; i++)
         {
            ClientMessage msg = consumer.receive(5000);
            assertNotNull(msg);
            msg.acknowledge();
         }

         sessionConsumer.rollback();
         
         PageCursorProviderImpl cursor = (PageCursorProviderImpl)queue.getPageSubscription().getPagingStore().getCursorProvier();
         
         cursor.clearCache();

         try
         {
            server.destroyQueue(QUEUE);
         }
         catch (Exception e)
         {
         }
         
         cursor.clearCache();
  
         consumer.close();
         server.stop();
         
         server.start();
      }
      finally
      {

      }
   }

   public void testHangOnPaging2() throws Exception
   {
      AddressSettings setting = new AddressSettings();
      setting.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      setting.setMaxSizeBytes(100 * 1024);
      setting.setPageSizeBytes(10024);
      server.getAddressSettingsRepository().addMatch("#", setting);
      queue = server.createQueue(QUEUE, QUEUE, null, true, false);
      server.createQueue(QUEUE, QUEUE.concat("hold-address"), new SimpleString("hq=0"), true, false);
      
      System.out.println("maxSize = " + queue.getPageSubscription().getPagingStore().getMaxSize());
      queue.getPageSubscription().getPagingStore().startPaging();
      try
      {

         locator.setBlockOnDurableSend(false);
         ClientSessionFactory factory = locator.createSessionFactory();
         ClientSession sessionProducer = factory.createSession(false, false, false);

         ServerLocator consumerLocator = createInVMNonHALocator();
         consumerLocator.setBlockOnAcknowledge(true);
         ClientSessionFactory factoryConsumer = consumerLocator.createSessionFactory();
         ClientSession sessionConsumer = factoryConsumer.createSession(false, false, 0);

         ClientProducer producer = sessionProducer.createProducer(QUEUE);

         ClientConsumer consumer = sessionConsumer.createConsumer(QUEUE);

         for (int i = 0; i < 2000; i++)
         {
            ClientMessage msg = sessionProducer.createMessage(true);
            msg.getBodyBuffer().writeBytes(new byte[1024]);
            msg.setExpiration(System.currentTimeMillis() + 5000);
            producer.send(msg);
         }
         sessionProducer.commit();

         sessionConsumer.start();
         
         ClientMessage msg = null;
         for (int i = 0; i < 500; i++)
         {
            msg = consumer.receive(5000);
            assertNotNull(msg);
         }

         PageCursorProviderImpl cursor = (PageCursorProviderImpl)queue.getPageSubscription().getPagingStore().getCursorProvier();
         
         boolean exceptionHappened = false;
         try
         {
            server.destroyQueue(QUEUE);
         }
         catch (Exception e)
         {
            exceptionHappened = true;
         }
         
         assertTrue(exceptionHappened);
         
         cursor.clearCache();
         
         Thread.sleep(500);
         
         msg.acknowledge();
         
         sessionConsumer.commit();
         
         consumer.close();

         server.destroyQueue(QUEUE);
         
         server.stop();
         server.start();
      }
      finally
      {

      }
   }
   
   /**
    * 
    */
   protected void releaseConsumers()
   {
      callbackSemaphore.release();
   }

   /**
    * @throws InterruptedException
    */
   protected void awaitBlocking() throws InterruptedException
   {
      assertTrue(this.inCall.await(5000));
   }

   /**
    * @throws InterruptedException
    */
   protected void blockConsumers() throws InterruptedException
   {
      this.callbackSemaphore.acquire();
   }

   /**
    * This would recreate the scenario where a queue was duplicated
    * @throws Exception
    */
   public void testHangDuplicateQueues() throws Exception
   {
      final Semaphore blocked = new Semaphore(1);
      final CountDownLatch latchDelete = new CountDownLatch(1);
      class MyQueueWithBlocking extends QueueImpl
      {

         /**
         * @param id
         * @param address
         * @param name
         * @param filter
         * @param pageSubscription
         * @param durable
         * @param temporary
         * @param scheduledExecutor
         * @param postOffice
         * @param storageManager
         * @param addressSettingsRepository
         * @param executor
         */
         public MyQueueWithBlocking(final long id,
                                    final SimpleString address,
                                    final SimpleString name,
                                    final Filter filter,
                                    final PageSubscription pageSubscription,
                                    final boolean durable,
                                    final boolean temporary,
                                    final ScheduledExecutorService scheduledExecutor,
                                    final PostOffice postOffice,
                                    final StorageManager storageManager,
                                    final HierarchicalRepository<AddressSettings> addressSettingsRepository,
                                    final Executor executor)
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
         public synchronized int deleteMatchingReferences(final int flushLimit, final Filter filter) throws Exception
         {
            latchDelete.countDown();
            blocked.acquire();
            blocked.release();
            return super.deleteMatchingReferences(flushLimit, filter);
         }
      }

      class LocalFactory extends QueueFactoryImpl
      {
         public LocalFactory(final ExecutorFactory executorFactory,
                             final ScheduledExecutorService scheduledExecutor,
                             final HierarchicalRepository<AddressSettings> addressSettingsRepository,
                             final StorageManager storageManager)
         {
            super(executorFactory, scheduledExecutor, addressSettingsRepository, storageManager);
         }

         @Override
         public Queue createQueue(final long persistenceID,
                                  final SimpleString address,
                                  final SimpleString name,
                                  final Filter filter,
                                  final PageSubscription pageSubscription,
                                  final boolean durable,
                                  final boolean temporary)
         {
            queue = new MyQueueWithBlocking(persistenceID,
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
                                            executorFactory.getExecutor());
            return queue;
         }

      }

      LocalFactory queueFactory = new LocalFactory(server.getExecutorFactory(),
                       server.getScheduledPool(),
                       server.getAddressSettingsRepository(),
                       server.getStorageManager());
      
      queueFactory.setPostOffice(server.getPostOffice());
      
      ((HornetQServerImpl)server).replaceQueueFactory(queueFactory);

      queue = server.createQueue(QUEUE, QUEUE, null, true, false);

      blocked.acquire();

      ClientSessionFactory factory = locator.createSessionFactory();
      ClientSession session = factory.createSession(false, false, false);

      ClientProducer producer = session.createProducer(QUEUE);

      producer.send(session.createMessage(true));
      session.commit();

      Thread tDelete = new Thread()
      {
         @Override
         public void run()
         {
            try
            {
               server.destroyQueue(QUEUE);
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }
      };

      tDelete.start();

      Assert.assertTrue(latchDelete.await(10, TimeUnit.SECONDS));

      try
      {
         server.createQueue(QUEUE, QUEUE, null, true, false);
      }
      catch (Exception expected)
      {
      }

      blocked.release();

      server.stop();
      
      tDelete.join();
      
      session.close();

      // a duplicate binding would impede the server from starting
      server.start();
      waitForServer(server);

      server.stop();

   }

   /**
    * This would force a journal duplication on bindings even with the scenario that generated fixed,
    * the server shouldn't hold of from starting
    * @throws Exception
    */
   public void testForceDuplicationOnBindings() throws Exception
   {
      queue = server.createQueue(QUEUE, QUEUE, null, true, false);

      ClientSessionFactory factory = locator.createSessionFactory();
      ClientSession session = factory.createSession(false, false, false);

      ClientProducer producer = session.createProducer(QUEUE);

      producer.send(session.createMessage(true));
      session.commit();
      
      long queueID = server.getStorageManager().generateUniqueID();
      long txID = server.getStorageManager().generateUniqueID();
      

      // Forcing a situation where the server would unexpectedly create a duplicated queue. The server should still start normally
      LocalQueueBinding newBinding = new LocalQueueBinding(QUEUE, new QueueImpl(queueID, QUEUE, QUEUE, null, true, false, null, null, null, null, null), server.getNodeID());
      server.getStorageManager().addQueueBinding(txID, newBinding);
      server.getStorageManager().commitBindings(txID);

      server.stop();

      // a duplicate binding would impede the server from starting
      server.start();
      waitForServer(server);

      server.stop();

   }

   // An exception during delivery shouldn't make the message disappear
   public void testExceptionWhileDelivering() throws Exception
   {
      queue = server.createQueue(QUEUE, QUEUE, null, true, false);

      HangInterceptor hangInt = new HangInterceptor();
      try
      {
         locator.addInterceptor(hangInt);

         ClientSessionFactory factory = locator.createSessionFactory();
         ClientSession session = factory.createSession(false, false, false);

         ClientProducer producer = session.createProducer(QUEUE);

         ClientConsumer consumer = session.createConsumer(QUEUE);

         producer.send(session.createMessage(true));
         session.commit();

         hangInt.close();

         session.start();

         Assert.assertTrue(hangInt.reusableLatch.await(10, TimeUnit.SECONDS));

         hangInt.pendingException = new HornetQException();

         hangInt.open();

         session.close();

         session = factory.createSession(false, false);
         session.start();

         consumer = session.createConsumer(QUEUE);

         ClientMessage msg = consumer.receive(5000);
         Assert.assertNotNull(msg);
         msg.acknowledge();

         session.commit();
      }
      finally
      {
         hangInt.open();
      }

   }
   
   /**
    * This will simulate what would happen with topic creationg where a single record is supposed to be created on the journal
    * @throws Exception
    */
   public void testDuplicateDestinationsOnTopic() throws Exception
   {
      for (int i = 0; i < 5; i++) 
      {
         if (server.locateQueue(SimpleString.toSimpleString("jms.topic.tt")) == null)
         {
            server.createQueue(SimpleString.toSimpleString("jms.topic.tt"), SimpleString.toSimpleString("jms.topic.tt"), SimpleString.toSimpleString(HornetQServerImpl.GENERIC_IGNORED_FILTER), true, false);
         }
         
         server.stop();
         
         SequentialFileFactory messagesFF = new NIOSequentialFileFactory(getBindingsDir(), null);

         JournalImpl messagesJournal = new JournalImpl(1024 * 1024,
                                                       2,
                                                       0,
                                                       0,
                                                       messagesFF,
                                                       "hornetq-bindings",
                                                       "bindings",
                                                       1);
         
         messagesJournal.start();
         
         LinkedList<RecordInfo> infos = new LinkedList<RecordInfo>();
         
         messagesJournal.load(infos, null, null);
         
         int bindings = 0;
         for (RecordInfo info: infos)
         {
            if (info.getUserRecordType() == JournalStorageManager.QUEUE_BINDING_RECORD)
            {
               bindings++;
            }
         }
         assertEquals(1, bindings);
         
         System.out.println("Bindings: " + bindings);
         messagesJournal.stop();
         if (i < 4) server.start();
      }
   }

   
   
   ReusableLatch inCall = new ReusableLatch(1);
   Semaphore callbackSemaphore = new Semaphore(1);
   
   
   class MyCallback implements SessionCallback
   {
      final SessionCallback targetCallback;
      
      MyCallback(SessionCallback parameter)
      {
         this.targetCallback = parameter;
      }

      /* (non-Javadoc)
       * @see org.hornetq.spi.core.protocol.SessionCallback#sendProducerCreditsMessage(int, org.hornetq.api.core.SimpleString)
       */
      @Override
      public void sendProducerCreditsMessage(int credits, SimpleString address)
      {
         targetCallback.sendProducerCreditsMessage(credits, address);
      }

      /* (non-Javadoc)
       * @see org.hornetq.spi.core.protocol.SessionCallback#sendMessage(org.hornetq.core.server.ServerMessage, long, int)
       */
      @Override
      public int sendMessage(ServerMessage message, long consumerID, int deliveryCount)
      {
         inCall.countDown();
         try
         {
            callbackSemaphore.acquire();
         }
         catch (InterruptedException e)
         {
            inCall.countUp();
            return -1;
         }

         try
         {
            return targetCallback.sendMessage(message, consumerID, deliveryCount);
         }
         finally
         {
            callbackSemaphore.release();
            inCall.countUp();
         }
      }

      /* (non-Javadoc)
       * @see org.hornetq.spi.core.protocol.SessionCallback#sendLargeMessage(org.hornetq.core.server.ServerMessage, long, long, int)
       */
      @Override
      public int sendLargeMessage(ServerMessage message, long consumerID, long bodySize, int deliveryCount)
      {
         return targetCallback.sendLargeMessage(message, consumerID, bodySize, deliveryCount);
      }

      /* (non-Javadoc)
       * @see org.hornetq.spi.core.protocol.SessionCallback#sendLargeMessageContinuation(long, byte[], boolean, boolean)
       */
      @Override
      public int sendLargeMessageContinuation(long consumerID, byte[] body, boolean continues, boolean requiresResponse)
      {
         return targetCallback.sendLargeMessageContinuation(consumerID, body, continues, requiresResponse);
      }

      /* (non-Javadoc)
       * @see org.hornetq.spi.core.protocol.SessionCallback#closed()
       */
      @Override
      public void closed()
      {
         targetCallback.closed();
      }

      /* (non-Javadoc)
       * @see org.hornetq.spi.core.protocol.SessionCallback#addReadyListener(org.hornetq.spi.core.remoting.ReadyListener)
       */
      @Override
      public void addReadyListener(ReadyListener listener)
      {
         targetCallback.addReadyListener(listener);
      }

      /* (non-Javadoc)
       * @see org.hornetq.spi.core.protocol.SessionCallback#removeReadyListener(org.hornetq.spi.core.remoting.ReadyListener)
       */
      @Override
      public void removeReadyListener(ReadyListener listener)
      {
         targetCallback.removeReadyListener(listener);
      }
      
      
   }
   
   class MyHornetQServer extends HornetQServerImpl
   {
      
      

      public MyHornetQServer(Configuration configuration,
                             MBeanServer mbeanServer,
                             HornetQSecurityManager securityManager)
      {
         super(configuration, mbeanServer, securityManager);
      }

      protected ServerSessionImpl internalCreateSession(final String name,
                                                        final String username,
                                                        final String password,
                                                        final int minLargeMessageSize,
                                                        final RemotingConnection connection,
                                                        final boolean autoCommitSends,
                                                        final boolean autoCommitAcks,
                                                        final boolean preAcknowledge,
                                                        final boolean xa,
                                                        final String defaultAddress,
                                                        final SessionCallback callback) throws Exception
      {
         final ServerSessionImpl session = new ServerSessionImpl(name,
                                                                 username,
                                                                 password,
                                                                 minLargeMessageSize,
                                                                 autoCommitSends,
                                                                 autoCommitAcks,
                                                                 preAcknowledge,
                                                                 getConfiguration().isPersistDeliveryCountBeforeDelivery(),
                                                                 xa,
                                                                 connection,
                                                                 getStorageManager(),
                                                                 getPostOffice(),
                                                                 getResourceManager(),
                                                                 getSecurityStore(),
                                                                 getManagementService(),
                                                                 this,
                                                                 getConfiguration().getManagementAddress(),
                                                                 defaultAddress == null ? null
                                                                                       : new SimpleString(defaultAddress),
                                                                 new MyCallback(callback));
         return session;
      }
   }

   class HangInterceptor implements Interceptor
   {
      Semaphore semaphore = new Semaphore(1);

      ReusableLatch reusableLatch = new ReusableLatch(1);

      volatile HornetQException pendingException = null;

      public void close() throws Exception
      {
         semaphore.acquire();
      }

      public void open() throws Exception
      {
         semaphore.release();
      }

      @Override
      public boolean intercept(final Packet packet, final RemotingConnection connection) throws HornetQException
      {
         if (packet instanceof SessionReceiveMessage)
         {
            System.out.println("Receiving message");
            try
            {
               reusableLatch.countDown();
               semaphore.acquire();
               semaphore.release();
               reusableLatch.countUp();
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }

         if (pendingException != null)
         {
            HornetQException exToThrow = pendingException;
            pendingException = null;
            throw exToThrow;
         }
         return true;
      }

   }
}
