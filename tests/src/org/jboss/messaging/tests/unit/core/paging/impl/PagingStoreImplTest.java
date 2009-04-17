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

package org.jboss.messaging.tests.unit.core.paging.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.buffers.ChannelBuffers;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.journal.SequentialFile;
import org.jboss.messaging.core.journal.SequentialFileFactory;
import org.jboss.messaging.core.journal.impl.NIOSequentialFileFactory;
import org.jboss.messaging.core.paging.Page;
import org.jboss.messaging.core.paging.PageTransactionInfo;
import org.jboss.messaging.core.paging.PagedMessage;
import org.jboss.messaging.core.paging.PagingManager;
import org.jboss.messaging.core.paging.PagingStore;
import org.jboss.messaging.core.paging.PagingStoreFactory;
import org.jboss.messaging.core.paging.impl.PageTransactionInfoImpl;
import org.jboss.messaging.core.paging.impl.PagedMessageImpl;
import org.jboss.messaging.core.paging.impl.PagingStoreImpl;
import org.jboss.messaging.core.paging.impl.TestSupportPageStore;
import org.jboss.messaging.core.persistence.QueueBindingInfo;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.Bindings;
import org.jboss.messaging.core.postoffice.DuplicateIDCache;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.core.server.LargeServerMessage;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.impl.ServerMessageImpl;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.core.transaction.ResourceManager;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.tests.unit.core.journal.impl.fakes.FakeSequentialFileFactory;
import org.jboss.messaging.tests.util.RandomUtil;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.utils.Pair;
import org.jboss.messaging.utils.SimpleString;
import org.jboss.messaging.utils.UUID;

/**
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class PagingStoreImplTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   private final static SimpleString destinationTestName = new SimpleString("test");

   // Attributes ----------------------------------------------------

   protected ExecutorService executor;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testAddAndRemoveMessages()
   {
      long id1 = RandomUtil.randomLong();
      long id2 = RandomUtil.randomLong();
      PageTransactionInfo trans = new PageTransactionInfoImpl(id2);

      trans.setRecordID(id1);

      // anything between 2 and 100
      int nr1 = RandomUtil.randomPositiveInt() % 98 + 2;

      for (int i = 0; i < nr1; i++)
      {
         trans.increment();
      }

      assertEquals(nr1, trans.getNumberOfMessages());

      MessagingBuffer buffer = ChannelBuffers.buffer(trans.getEncodeSize());

      trans.encode(buffer);

      PageTransactionInfo trans2 = new PageTransactionInfoImpl(id1);
      trans2.decode(buffer);

      assertEquals(id2, trans2.getTransactionID());

      assertEquals(nr1, trans2.getNumberOfMessages());

      for (int i = 0; i < nr1; i++)
      {
         trans.decrement();
      }

      assertEquals(0, trans.getNumberOfMessages());

      try
      {
         trans.decrement();
         fail("Exception expected!");
      }
      catch (Throwable ignored)
      {
      }

   }

   public void testDoubleStart() throws Exception
   {
      SequentialFileFactory factory = new FakeSequentialFileFactory();

      PagingStore storeImpl = new PagingStoreImpl(createMockManager(),
                                                  createStorageManagerMock(),
                                                  createPostOfficeMock(),
                                                  factory,
                                                  null,
                                                  destinationTestName,
                                                  new AddressSettings(),
                                                  executor);

      storeImpl.start();

      // this is not supposed to throw an exception.
      // As you could have start being called twice as Stores are dynamically
      // created, on a multi-thread environment
      storeImpl.start();

      storeImpl.stop();

   }

   public void testPageWithNIO() throws Exception
   {
      // This integration test could fail 1 in 100 due to race conditions.
      for (int i = 0; i < 100; i++)
      {
         recreateDirectory(getTestDir());
         testConcurrentPaging(new NIOSequentialFileFactory(getTestDir()), 1);
      }
   }

   public void testStore() throws Exception
   {
      SequentialFileFactory factory = new FakeSequentialFileFactory();

      PagingStoreFactory storeFactory = new FakeStoreFactory(factory);

      PagingStore storeImpl = new PagingStoreImpl(createMockManager(),
                                                  createStorageManagerMock(),
                                                  createPostOfficeMock(),
                                                  factory,
                                                  storeFactory,
                                                  destinationTestName,
                                                  new AddressSettings(),
                                                  executor);

      storeImpl.start();

      assertEquals(0, storeImpl.getNumberOfPages());

      storeImpl.startPaging();

      assertEquals(1, storeImpl.getNumberOfPages());

      List<MessagingBuffer> buffers = new ArrayList<MessagingBuffer>();

      MessagingBuffer buffer = createRandomBuffer(0, 10);

      buffers.add(buffer);
      SimpleString destination = new SimpleString("test");

      PagedMessageImpl msg = createMessage(destination, buffer);

      assertTrue(storeImpl.isPaging());

      assertTrue(storeImpl.page(msg, true, true));

      assertEquals(1, storeImpl.getNumberOfPages());

      storeImpl.sync();

      storeImpl = new PagingStoreImpl(createMockManager(),
                                      createStorageManagerMock(),
                                      createPostOfficeMock(),
                                      factory,
                                      null,
                                      destinationTestName,
                                      new AddressSettings(),
                                      executor);

      storeImpl.start();

      assertEquals(2, storeImpl.getNumberOfPages());

   }

   public void testDepageOnCurrentPage() throws Exception
   {
      SequentialFileFactory factory = new FakeSequentialFileFactory();

      SimpleString destination = new SimpleString("test");

      PagingStoreFactory storeFactory = new FakeStoreFactory(factory);

      TestSupportPageStore storeImpl = new PagingStoreImpl(createMockManager(),
                                                           createStorageManagerMock(),
                                                           createPostOfficeMock(),
                                                           factory,
                                                           storeFactory,
                                                           destinationTestName,
                                                           new AddressSettings(),
                                                           executor);

      storeImpl.start();

      assertEquals(0, storeImpl.getNumberOfPages());

      storeImpl.startPaging();

      List<MessagingBuffer> buffers = new ArrayList<MessagingBuffer>();

      for (int i = 0; i < 10; i++)
      {

         MessagingBuffer buffer = createRandomBuffer(i + 1l, 10);

         buffers.add(buffer);

         PagedMessageImpl msg = createMessage(destination, buffer);

         assertTrue(storeImpl.page(msg, true, true));
      }

      assertEquals(1, storeImpl.getNumberOfPages());

      storeImpl.sync();

      Page page = storeImpl.depage();

      page.open();

      List<PagedMessage> msg = page.read();

      assertEquals(10, msg.size());
      assertEquals(1, storeImpl.getNumberOfPages());

      page = storeImpl.depage();

      assertNull(page);

      assertEquals(0, storeImpl.getNumberOfPages());

      for (int i = 0; i < 10; i++)
      {
         assertEquals(0, msg.get(i).getMessage(null).getMessageID());
         assertEqualsByteArrays(buffers.get(i).array(), msg.get(i).getMessage(null).getBody().array());
      }

   }

   public void testDepageMultiplePages() throws Exception
   {
      SequentialFileFactory factory = new FakeSequentialFileFactory();
      SimpleString destination = new SimpleString("test");

      PagingStoreFactory storeFactory = new FakeStoreFactory(factory);

      TestSupportPageStore storeImpl = new PagingStoreImpl(createMockManager(),
                                                           createStorageManagerMock(),
                                                           createPostOfficeMock(),
                                                           factory,
                                                           storeFactory,
                                                           destinationTestName,
                                                           new AddressSettings(),
                                                           executor);

      storeImpl.start();

      assertEquals(0, storeImpl.getNumberOfPages());

      storeImpl.startPaging();

      assertEquals(1, storeImpl.getNumberOfPages());

      List<MessagingBuffer> buffers = new ArrayList<MessagingBuffer>();

      for (int i = 0; i < 10; i++)
      {

         MessagingBuffer buffer = createRandomBuffer(i + 1l, 10);

         buffers.add(buffer);

         if (i == 5)
         {
            storeImpl.forceAnotherPage();
         }

         PagedMessageImpl msg = createMessage(destination, buffer);

         assertTrue(storeImpl.page(msg, true, true));
      }

      assertEquals(2, storeImpl.getNumberOfPages());

      storeImpl.sync();

      for (int pageNr = 0; pageNr < 2; pageNr++)
      {
         Page page = storeImpl.depage();

         page.open();

         List<PagedMessage> msg = page.read();

         page.close();

         assertEquals(5, msg.size());

         for (int i = 0; i < 5; i++)
         {
            assertEquals(0, msg.get(i).getMessage(null).getMessageID());
            assertEqualsByteArrays(buffers.get(pageNr * 5 + i).array(), msg.get(i).getMessage(null).getBody().array());
         }
      }

      assertEquals(1, storeImpl.getNumberOfPages());

      assertTrue(storeImpl.isPaging());

      PagedMessageImpl msg = createMessage(destination, buffers.get(0));

      assertTrue(storeImpl.page(msg, true, true));

      Page newPage = storeImpl.depage();

      newPage.open();

      assertEquals(1, newPage.read().size());

      newPage.delete();

      assertEquals(1, storeImpl.getNumberOfPages());

      assertTrue(storeImpl.isPaging());

      assertNull(storeImpl.depage());

      assertFalse(storeImpl.isPaging());

      assertFalse(storeImpl.page(msg, true, true));

      storeImpl.startPaging();

      assertTrue(storeImpl.page(msg, true, true));

      Page page = storeImpl.depage();

      page.open();

      List<PagedMessage> msgs = page.read();

      assertEquals(1, msgs.size());

      assertEquals(0l, msgs.get(0).getMessage(null).getMessageID());

      assertEqualsByteArrays(buffers.get(0).array(), msgs.get(0).getMessage(null).getBody().array());

      assertEquals(1, storeImpl.getNumberOfPages());

      assertTrue(storeImpl.isPaging());

      assertNull(storeImpl.depage());

      assertEquals(0, storeImpl.getNumberOfPages());

      page.open();

   }

   public void testConcurrentDepage() throws Exception
   {
      SequentialFileFactory factory = new FakeSequentialFileFactory(1, false);

      testConcurrentPaging(factory, 10);
   }

   protected void testConcurrentPaging(final SequentialFileFactory factory, final int numberOfThreads) throws Exception,
                                                                                                      InterruptedException
   {

      PagingStoreFactory storeFactory = new FakeStoreFactory(factory);

      final int MAX_SIZE = 1024 * 10;

      final AtomicLong messageIdGenerator = new AtomicLong(0);

      final AtomicInteger aliveProducers = new AtomicInteger(numberOfThreads);

      final CountDownLatch latchStart = new CountDownLatch(numberOfThreads);

      final ConcurrentHashMap<Long, PagedMessageImpl> buffers = new ConcurrentHashMap<Long, PagedMessageImpl>();

      final ArrayList<Page> readPages = new ArrayList<Page>();

      AddressSettings settings = new AddressSettings();
      settings.setPageSizeBytes(MAX_SIZE);

      final TestSupportPageStore storeImpl = new PagingStoreImpl(createMockManager(),
                                                                 createStorageManagerMock(),
                                                                 createPostOfficeMock(),
                                                                 factory,
                                                                 storeFactory,
                                                                 new SimpleString("test"),
                                                                 settings,
                                                                 executor);

      storeImpl.start();

      assertEquals(0, storeImpl.getNumberOfPages());

      storeImpl.startPaging();

      assertEquals(1, storeImpl.getNumberOfPages());

      final SimpleString destination = new SimpleString("test");

      class ProducerThread extends Thread
      {

         Exception e;

         @Override
         public void run()
         {

            try
            {
               boolean firstTime = true;
               while (true)
               {
                  long id = messageIdGenerator.incrementAndGet();
                  PagedMessageImpl msg = createMessage(destination, createRandomBuffer(id, 5));
                  if (storeImpl.page(msg, false, true))
                  {
                     buffers.put(id, msg);
                  }
                  else
                  {
                     break;
                  }

                  if (firstTime)
                  {
                     latchStart.countDown();
                     firstTime = false;
                  }
               }
            }
            catch (Exception e)
            {
               e.printStackTrace();
               this.e = e;
            }
            finally
            {
               aliveProducers.decrementAndGet();
            }
         }
      }

      class ConsumerThread extends Thread
      {
         Exception e;

         @Override
         public void run()
         {
            try
            {
               // Wait every producer to produce at least one message
               latchStart.await();
               while (aliveProducers.get() > 0)
               {
                  Page page = storeImpl.depage();
                  if (page != null)
                  {
                     readPages.add(page);
                  }
               }
            }
            catch (Exception e)
            {
               e.printStackTrace();
               this.e = e;
            }
         }
      }

      ProducerThread producerThread[] = new ProducerThread[numberOfThreads];

      for (int i = 0; i < numberOfThreads; i++)
      {
         producerThread[i] = new ProducerThread();
         producerThread[i].start();
      }

      ConsumerThread consumer = new ConsumerThread();
      consumer.start();

      for (int i = 0; i < numberOfThreads; i++)
      {
         producerThread[i].join();
         if (producerThread[i].e != null)
         {
            throw producerThread[i].e;
         }
      }

      consumer.join();

      if (consumer.e != null)
      {
         throw consumer.e;
      }

      final ConcurrentHashMap<Long, PagedMessage> buffers2 = new ConcurrentHashMap<Long, PagedMessage>();

      for (Page page : readPages)
      {
         page.open();
         List<PagedMessage> msgs = page.read();
         page.close();

         for (PagedMessage msg : msgs)
         {
            long id = msg.getMessage(null).getBody().readLong();
            msg.getMessage(null).getBody().resetReaderIndex();

            PagedMessageImpl msgWritten = buffers.remove(id);
            buffers2.put(id, msg);
            assertNotNull(msgWritten);
            assertEquals(msg.getMessage(null).getDestination(), msgWritten.getMessage(null).getDestination());
            assertEqualsByteArrays(msgWritten.getMessage(null).getBody().array(), msg.getMessage(null)
                                                                                     .getBody()
                                                                                     .array());
         }
      }

      assertEquals(0, buffers.size());

      List<String> files = factory.listFiles("page");

      assertTrue(files.size() != 0);

      for (String file : files)
      {
         SequentialFile fileTmp = factory.createSequentialFile(file, 1);
         fileTmp.open();
         assertTrue(fileTmp.size() + " <= " + MAX_SIZE, fileTmp.size() <= MAX_SIZE);
         fileTmp.close();
      }

      TestSupportPageStore storeImpl2 = new PagingStoreImpl(createMockManager(),
                                                            createStorageManagerMock(),
                                                            createPostOfficeMock(),
                                                            factory,
                                                            storeFactory,
                                                            new SimpleString("test"),
                                                            settings,
                                                            executor);
      storeImpl2.start();

      int numberOfPages = storeImpl2.getNumberOfPages();
      assertTrue(numberOfPages != 0);

      storeImpl2.startPaging();

      storeImpl2.startPaging();

      assertEquals(numberOfPages, storeImpl2.getNumberOfPages());

      long lastMessageId = messageIdGenerator.incrementAndGet();
      PagedMessage lastMsg = createMessage(destination, createRandomBuffer(lastMessageId, 5));

      storeImpl2.page(lastMsg, false, true);
      buffers2.put(lastMessageId, lastMsg);

      Page lastPage = null;
      while (true)
      {
         Page page = storeImpl2.depage();
         if (page == null)
         {
            break;
         }

         lastPage = page;

         page.open();

         List<PagedMessage> msgs = page.read();

         page.close();

         for (PagedMessage msg : msgs)
         {

            long id = msg.getMessage(null).getBody().readLong();
            PagedMessage msgWritten = buffers2.remove(id);
            assertNotNull(msgWritten);
            assertEquals(msg.getMessage(null).getDestination(), msgWritten.getMessage(null).getDestination());
            assertEqualsByteArrays(msgWritten.getMessage(null).getBody().array(), msg.getMessage(null)
                                                                                     .getBody()
                                                                                     .array());
         }
      }

      lastPage.open();
      List<PagedMessage> lastMessages = lastPage.read();
      lastPage.close();
      assertEquals(1, lastMessages.size());

      lastMessages.get(0).getMessage(null).getBody().resetReaderIndex();
      assertEquals(lastMessages.get(0).getMessage(null).getBody().readLong(), lastMessageId);
      assertEqualsByteArrays(lastMessages.get(0).getMessage(null).getBody().array(), lastMsg.getMessage(null)
                                                                                            .getBody()
                                                                                            .array());

      assertEquals(0, buffers2.size());

      assertEquals(0, storeImpl.getAddressSize());

   }

   /**
   * @return
   */
   protected PagingManager createMockManager()
   {
      return new FakePagingManager();
   }

   private StorageManager createStorageManagerMock()
   {
      return new FakeStorageManager();
   }

   private PostOffice createPostOfficeMock()
   {
      return new FakePostOffice();
   }

   private PagedMessageImpl createMessage(final SimpleString destination, final MessagingBuffer buffer)
   {
      ServerMessage msg = new ServerMessageImpl((byte)1, true, 0, System.currentTimeMillis(), (byte)0, buffer);

      msg.setDestination(destination);
      return new PagedMessageImpl(msg);
   }

   private MessagingBuffer createRandomBuffer(final long id, final int size)
   {
      MessagingBuffer buffer = ChannelBuffers.buffer(size + 8);

      buffer.writeLong(id);

      for (int j = 8; j < buffer.capacity(); j++)
      {
         buffer.writeByte(RandomUtil.randomByte());
      }
      return buffer;
   }

   // Protected ----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      executor = Executors.newSingleThreadExecutor();
   }

   @Override
   protected void tearDown() throws Exception
   {
      executor.shutdown();
      super.tearDown();
   }

   // Inner classes -------------------------------------------------

   class FakePagingManager implements PagingManager
   {

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.paging.PagingManager#activate()
       */
      public void activate()
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.paging.PagingManager#addGlobalSize(long)
       */
      public long addGlobalSize(final long size)
      {
         return 0;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.paging.PagingManager#addTransaction(org.jboss.messaging.core.paging.PageTransactionInfo)
       */
      public void addTransaction(final PageTransactionInfo pageTransaction)
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.paging.PagingManager#createPageStore(org.jboss.messaging.utils.SimpleString)
       */
      public PagingStore createPageStore(final SimpleString destination) throws Exception
      {
         return null;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.paging.PagingManager#getGlobalDepageWatermarkBytes()
       */
      public long getGlobalDepageWatermarkBytes()
      {
         return ConfigurationImpl.DEFAULT_PAGE_WATERMARK_SIZE;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.paging.PagingManager#getGlobalSize()
       */
      public long getGlobalSize()
      {
         return 0;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.paging.PagingManager#getMaxGlobalSize()
       */
      public long getMaxGlobalSize()
      {
         return 0;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.paging.PagingManager#getPageStore(org.jboss.messaging.utils.SimpleString)
       */
      public PagingStore getPageStore(final SimpleString address) throws Exception
      {
         return null;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.paging.PagingManager#getTransaction(long)
       */
      public PageTransactionInfo getTransaction(final long transactionID)
      {
         return null;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.paging.PagingManager#isBackup()
       */
      public boolean isBackup()
      {
         return false;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.paging.PagingManager#isGlobalPageMode()
       */
      public boolean isGlobalPageMode()
      {
         return false;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.paging.PagingManager#isPaging(org.jboss.messaging.utils.SimpleString)
       */
      public boolean isPaging(final SimpleString destination) throws Exception
      {
         return false;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.paging.PagingManager#page(org.jboss.messaging.core.server.ServerMessage, boolean)
       */
      public boolean page(final ServerMessage message, final boolean duplicateDetection) throws Exception
      {
         return false;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.paging.PagingManager#page(org.jboss.messaging.core.server.ServerMessage, long, boolean)
       */
      public boolean page(final ServerMessage message, final long transactionId, final boolean duplicateDetection) throws Exception
      {
         return false;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.paging.PagingManager#reloadStores()
       */
      public void reloadStores() throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.paging.PagingManager#removeTransaction(long)
       */
      public void removeTransaction(final long transactionID)
      {

      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.paging.PagingManager#setGlobalPageMode(boolean)
       */
      public void setGlobalPageMode(final boolean globalMode)
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.paging.PagingManager#setPostOffice(org.jboss.messaging.core.postoffice.PostOffice)
       */
      public void setPostOffice(final PostOffice postOffice)
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.paging.PagingManager#startGlobalDepage()
       */
      public void startGlobalDepage()
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.paging.PagingManager#sync(java.util.Collection)
       */
      public void sync(final Collection<SimpleString> destinationsToSync) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.server.MessagingComponent#isStarted()
       */
      public boolean isStarted()
      {
         return false;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.server.MessagingComponent#start()
       */
      public void start() throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.server.MessagingComponent#stop()
       */
      public void stop() throws Exception
      {
      }

   }

   class FakeStorageManager implements StorageManager
   {

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.persistence.StorageManager#addQueueBinding(org.jboss.messaging.core.postoffice.Binding)
       */
      public void addQueueBinding(final Binding binding) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.persistence.StorageManager#commit(long)
       */
      public void commit(final long txID) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.persistence.StorageManager#createLargeMessage()
       */
      public LargeServerMessage createLargeMessage()
      {
         return null;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.persistence.StorageManager#deleteDuplicateID(long)
       */
      public void deleteDuplicateID(final long recordID) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.persistence.StorageManager#deleteDuplicateIDTransactional(long, long)
       */
      public void deleteDuplicateIDTransactional(final long txID, final long recordID) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.persistence.StorageManager#deleteMessage(long)
       */
      public void deleteMessage(final long messageID) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.persistence.StorageManager#deleteMessageTransactional(long, long, long)
       */
      public void deleteMessageTransactional(final long txID, final long queueID, final long messageID) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.persistence.StorageManager#deletePageTransactional(long, long)
       */
      public void deletePageTransactional(final long txID, final long recordID) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.persistence.StorageManager#deleteQueueBinding(long)
       */
      public void deleteQueueBinding(final long queueBindingID) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.persistence.StorageManager#generateUniqueID()
       */
      public long generateUniqueID()
      {
         return 0;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.persistence.StorageManager#getCurrentUniqueID()
       */
      public long getCurrentUniqueID()
      {
         return 0;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.persistence.StorageManager#getPersistentID()
       */
      public UUID getPersistentID()
      {
         return null;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.persistence.StorageManager#loadBindingJournal(java.util.List)
       */
      public void loadBindingJournal(final List<QueueBindingInfo> queueBindingInfos) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.persistence.StorageManager#loadMessageJournal(org.jboss.messaging.core.paging.PagingManager, java.util.Map, org.jboss.messaging.core.transaction.ResourceManager, java.util.Map)
       */
      public void loadMessageJournal(PagingManager pagingManager,
                                     ResourceManager resourceManager,
                                     Map<Long, Queue> queues,
                                     Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.persistence.StorageManager#prepare(long, javax.transaction.xa.Xid)
       */
      public void prepare(final long txID, final Xid xid) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.persistence.StorageManager#rollback(long)
       */
      public void rollback(final long txID) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.persistence.StorageManager#setPersistentID(org.jboss.messaging.utils.UUID)
       */
      public void setPersistentID(final UUID id) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.persistence.StorageManager#storeAcknowledge(long, long)
       */
      public void storeAcknowledge(final long queueID, final long messageID) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.persistence.StorageManager#storeAcknowledgeTransactional(long, long, long)
       */
      public void storeAcknowledgeTransactional(final long txID, final long queueID, final long messageID) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.persistence.StorageManager#storeDuplicateID(org.jboss.messaging.utils.SimpleString, byte[], long)
       */
      public void storeDuplicateID(final SimpleString address, final byte[] duplID, final long recordID) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.persistence.StorageManager#storeDuplicateIDTransactional(long, org.jboss.messaging.utils.SimpleString, byte[], long)
       */
      public void storeDuplicateIDTransactional(final long txID,
                                                final SimpleString address,
                                                final byte[] duplID,
                                                final long recordID) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.persistence.StorageManager#storeMessage(org.jboss.messaging.core.server.ServerMessage)
       */
      public void storeMessage(final ServerMessage message) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.persistence.StorageManager#storeMessageTransactional(long, org.jboss.messaging.core.server.ServerMessage)
       */
      public void storeMessageTransactional(final long txID, final ServerMessage message) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.persistence.StorageManager#storePageTransaction(long, org.jboss.messaging.core.paging.PageTransactionInfo)
       */
      public void storePageTransaction(final long txID, final PageTransactionInfo pageTransaction) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.persistence.StorageManager#storeReference(long, long)
       */
      public void storeReference(final long queueID, final long messageID) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.persistence.StorageManager#storeReferenceTransactional(long, long, long)
       */
      public void storeReferenceTransactional(final long txID, final long queueID, final long messageID) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.persistence.StorageManager#updateDeliveryCount(org.jboss.messaging.core.server.MessageReference)
       */
      public void updateDeliveryCount(final MessageReference ref) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.persistence.StorageManager#updateDuplicateID(org.jboss.messaging.utils.SimpleString, byte[], long)
       */
      public void updateDuplicateID(final SimpleString address, final byte[] duplID, final long recordID) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.persistence.StorageManager#updateDuplicateIDTransactional(long, org.jboss.messaging.utils.SimpleString, byte[], long)
       */
      public void updateDuplicateIDTransactional(final long txID,
                                                 final SimpleString address,
                                                 final byte[] duplID,
                                                 final long recordID) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.persistence.StorageManager#updateScheduledDeliveryTime(org.jboss.messaging.core.server.MessageReference)
       */
      public void updateScheduledDeliveryTime(final MessageReference ref) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.persistence.StorageManager#updateScheduledDeliveryTimeTransactional(long, org.jboss.messaging.core.server.MessageReference)
       */
      public void updateScheduledDeliveryTimeTransactional(final long txID, final MessageReference ref) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.server.MessagingComponent#isStarted()
       */
      public boolean isStarted()
      {
         return false;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.server.MessagingComponent#start()
       */
      public void start() throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.server.MessagingComponent#stop()
       */
      public void stop() throws Exception
      {
      }

   }

   class FakePostOffice implements PostOffice
   {

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.postoffice.PostOffice#activate()
       */
      public List<Queue> activate()
      {
         return null;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.postoffice.PostOffice#addBinding(org.jboss.messaging.core.postoffice.Binding)
       */
      public void addBinding(final Binding binding) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.postoffice.PostOffice#getBinding(org.jboss.messaging.utils.SimpleString)
       */
      public Binding getBinding(final SimpleString uniqueName)
      {
         return null;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.postoffice.PostOffice#getBindingsForAddress(org.jboss.messaging.utils.SimpleString)
       */
      public Bindings getBindingsForAddress(final SimpleString address) throws Exception
      {
         return null;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.postoffice.PostOffice#getDuplicateIDCache(org.jboss.messaging.utils.SimpleString)
       */
      public DuplicateIDCache getDuplicateIDCache(final SimpleString address)
      {
         return null;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.postoffice.PostOffice#getPagingManager()
       */
      public PagingManager getPagingManager()
      {
         return null;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.postoffice.PostOffice#redistribute(org.jboss.messaging.core.server.ServerMessage, org.jboss.messaging.utils.SimpleString, org.jboss.messaging.core.transaction.Transaction)
       */
      public boolean redistribute(final ServerMessage message, final SimpleString routingName, final Transaction tx) throws Exception
      {
         return false;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.postoffice.PostOffice#removeBinding(org.jboss.messaging.utils.SimpleString)
       */
      public Binding removeBinding(final SimpleString uniqueName) throws Exception
      {
         return null;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.postoffice.PostOffice#route(org.jboss.messaging.core.server.ServerMessage)
       */
      public void route(final ServerMessage message) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.postoffice.PostOffice#route(org.jboss.messaging.core.server.ServerMessage, org.jboss.messaging.core.transaction.Transaction)
       */
      public void route(final ServerMessage message, final Transaction tx) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.postoffice.PostOffice#sendQueueInfoToQueue(org.jboss.messaging.utils.SimpleString, org.jboss.messaging.utils.SimpleString)
       */
      public void sendQueueInfoToQueue(final SimpleString queueName, final SimpleString address) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.server.MessagingComponent#isStarted()
       */
      public boolean isStarted()
      {
         return false;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.server.MessagingComponent#start()
       */
      public void start() throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.server.MessagingComponent#stop()
       */
      public void stop() throws Exception
      {

      }

   }

   class FakeStoreFactory implements PagingStoreFactory
   {

      final SequentialFileFactory factory;

      public FakeStoreFactory()
      {
         factory = new FakeSequentialFileFactory();
      }

      public FakeStoreFactory(final SequentialFileFactory factory)
      {
         this.factory = factory;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.paging.PagingStoreFactory#getGlobalDepagerExecutor()
       */
      public Executor getGlobalDepagerExecutor()
      {
         return null;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.paging.PagingStoreFactory#newFileFactory(org.jboss.messaging.utils.SimpleString)
       */
      public SequentialFileFactory newFileFactory(final SimpleString destinationName) throws Exception
      {
         return factory;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.paging.PagingStoreFactory#newStore(org.jboss.messaging.utils.SimpleString, org.jboss.messaging.core.settings.impl.AddressSettings)
       */
      public PagingStore newStore(final SimpleString destinationName, final AddressSettings addressSettings) throws Exception
      {
         return null;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.paging.PagingStoreFactory#reloadStores(org.jboss.messaging.core.settings.HierarchicalRepository)
       */
      public List<PagingStore> reloadStores(final HierarchicalRepository<AddressSettings> addressSettingsRepository) throws Exception
      {
         return null;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.paging.PagingStoreFactory#setPagingManager(org.jboss.messaging.core.paging.PagingManager)
       */
      public void setPagingManager(final PagingManager manager)
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.paging.PagingStoreFactory#setPostOffice(org.jboss.messaging.core.postoffice.PostOffice)
       */
      public void setPostOffice(final PostOffice office)
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.paging.PagingStoreFactory#setStorageManager(org.jboss.messaging.core.persistence.StorageManager)
       */
      public void setStorageManager(final StorageManager storageManager)
      {
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.paging.PagingStoreFactory#stop()
       */
      public void stop() throws InterruptedException
      {
      }

   }

}
