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

package org.hornetq.tests.unit.core.paging.impl;

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

import org.hornetq.core.buffers.ChannelBuffers;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.journal.impl.NIOSequentialFileFactory;
import org.hornetq.core.paging.Page;
import org.hornetq.core.paging.PageTransactionInfo;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.paging.PagingStoreFactory;
import org.hornetq.core.paging.impl.PageTransactionInfoImpl;
import org.hornetq.core.paging.impl.PagedMessageImpl;
import org.hornetq.core.paging.impl.PagingStoreImpl;
import org.hornetq.core.paging.impl.TestSupportPageStore;
import org.hornetq.core.persistence.QueueBindingInfo;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.transaction.ResourceManager;
import org.hornetq.tests.unit.core.journal.impl.fakes.FakeSequentialFileFactory;
import org.hornetq.tests.unit.core.server.impl.fakes.FakePostOffice;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.Pair;
import org.hornetq.utils.SimpleString;
import org.hornetq.utils.UUID;

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

      HornetQBuffer buffer = ChannelBuffers.buffer(trans.getEncodeSize());

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

      List<HornetQBuffer> buffers = new ArrayList<HornetQBuffer>();

      HornetQBuffer buffer = createRandomBuffer(0, 10);

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

      List<HornetQBuffer> buffers = new ArrayList<HornetQBuffer>();

      for (int i = 0; i < 10; i++)
      {

         HornetQBuffer buffer = createRandomBuffer(i + 1l, 10);

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

      List<HornetQBuffer> buffers = new ArrayList<HornetQBuffer>();

      for (int i = 0; i < 10; i++)
      {

         HornetQBuffer buffer = createRandomBuffer(i + 1l, 10);

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

   private PagedMessageImpl createMessage(final SimpleString destination, final HornetQBuffer buffer)
   {
      ServerMessage msg = new ServerMessageImpl((byte)1, true, 0, System.currentTimeMillis(), (byte)0, buffer);

      msg.setDestination(destination);
      return new PagedMessageImpl(msg);
   }

   private HornetQBuffer createRandomBuffer(final long id, final int size)
   {
      HornetQBuffer buffer = ChannelBuffers.buffer(size + 8);

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

      public void activate()
      {
      }

      public long addSize(final long size)
      {
         return 0;
      }

      public void addTransaction(final PageTransactionInfo pageTransaction)
      {
      }

      public PagingStore createPageStore(final SimpleString destination) throws Exception
      {
         return null;
      }

      public long getTotalMemory()
      {
         return 0;
      }
      
      public SimpleString[] getStoreNames()
      {
         return null;
      }

      public long getMaxMemory()
      {
         return 0;
      }

      public PagingStore getPageStore(final SimpleString address) throws Exception
      {
         return null;
      }

      public PageTransactionInfo getTransaction(final long transactionID)
      {
         return null;
      }

      public boolean isBackup()
      {
         return false;
      }

      public boolean isGlobalPageMode()
      {
         return false;
      }

      public boolean isPaging(final SimpleString destination) throws Exception
      {
         return false;
      }

      public boolean page(final ServerMessage message, final boolean duplicateDetection) throws Exception
      {
         return false;
      }

      public boolean page(final ServerMessage message, final long transactionId, final boolean duplicateDetection) throws Exception
      {
         return false;
      }

      public void reloadStores() throws Exception
      {
      }

      public void removeTransaction(final long transactionID)
      {

      }

      public void setGlobalPageMode(final boolean globalMode)
      {
      }

      public void setPostOffice(final PostOffice postOffice)
      {
      }

      public void resumeDepages()
      {
      }

      public void sync(final Collection<SimpleString> destinationsToSync) throws Exception
      {
      }

      public boolean isStarted()
      {
         return false;
      }

      public void start() throws Exception
      {
      }

      public void stop() throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.paging.PagingManager#isGlobalFull()
       */
      public boolean isGlobalFull()
      {
         return false;
      }

   }

   class FakeStorageManager implements StorageManager
   {

      public void setUniqueIDSequence(long id)
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#addQueueBinding(org.hornetq.core.postoffice.Binding)
       */
      public void addQueueBinding(final Binding binding) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#commit(long)
       */
      public void commit(final long txID) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#createLargeMessage()
       */
      public LargeServerMessage createLargeMessage()
      {
         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#deleteDuplicateID(long)
       */
      public void deleteDuplicateID(final long recordID) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#deleteDuplicateIDTransactional(long, long)
       */
      public void deleteDuplicateIDTransactional(final long txID, final long recordID) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#deleteMessage(long)
       */
      public void deleteMessage(final long messageID) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#deleteMessageTransactional(long, long, long)
       */
      public void deleteMessageTransactional(final long txID, final long queueID, final long messageID) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#deletePageTransactional(long, long)
       */
      public void deletePageTransactional(final long txID, final long recordID) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#deleteQueueBinding(long)
       */
      public void deleteQueueBinding(final long queueBindingID) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#generateUniqueID()
       */
      public long generateUniqueID()
      {
         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#getCurrentUniqueID()
       */
      public long getCurrentUniqueID()
      {
         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#getPersistentID()
       */
      public UUID getPersistentID()
      {
         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#loadBindingJournal(java.util.List)
       */
      public void loadBindingJournal(final List<QueueBindingInfo> queueBindingInfos) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#loadMessageJournal(org.hornetq.core.paging.PagingManager, java.util.Map, org.hornetq.core.transaction.ResourceManager, java.util.Map)
       */
      public void loadMessageJournal(PagingManager pagingManager,
                                     ResourceManager resourceManager,
                                     Map<Long, Queue> queues,
                                     Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#prepare(long, javax.transaction.xa.Xid)
       */
      public void prepare(final long txID, final Xid xid) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#rollback(long)
       */
      public void rollback(final long txID) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#setPersistentID(org.hornetq.utils.UUID)
       */
      public void setPersistentID(final UUID id) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#storeAcknowledge(long, long)
       */
      public void storeAcknowledge(final long queueID, final long messageID) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#storeAcknowledgeTransactional(long, long, long)
       */
      public void storeAcknowledgeTransactional(final long txID, final long queueID, final long messageID) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#storeDuplicateID(org.hornetq.utils.SimpleString, byte[], long)
       */
      public void storeDuplicateID(final SimpleString address, final byte[] duplID, final long recordID) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#storeDuplicateIDTransactional(long, org.hornetq.utils.SimpleString, byte[], long)
       */
      public void storeDuplicateIDTransactional(final long txID,
                                                final SimpleString address,
                                                final byte[] duplID,
                                                final long recordID) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#storeMessage(org.hornetq.core.server.ServerMessage)
       */
      public void storeMessage(final ServerMessage message) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#storeMessageTransactional(long, org.hornetq.core.server.ServerMessage)
       */
      public void storeMessageTransactional(final long txID, final ServerMessage message) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#storePageTransaction(long, org.hornetq.core.paging.PageTransactionInfo)
       */
      public void storePageTransaction(final long txID, final PageTransactionInfo pageTransaction) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#storeReference(long, long)
       */
      public void storeReference(final long queueID, final long messageID) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#storeReferenceTransactional(long, long, long)
       */
      public void storeReferenceTransactional(final long txID, final long queueID, final long messageID) throws Exception
      {
      }

      public long storeHeuristicCompletion(Xid xid, boolean isCommit) throws Exception
      {
         return -1;
      }
      
      public void deleteHeuristicCompletion(long txID) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#updateDeliveryCount(org.hornetq.core.server.MessageReference)
       */
      public void updateDeliveryCount(final MessageReference ref) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#updateDuplicateID(org.hornetq.utils.SimpleString, byte[], long)
       */
      public void updateDuplicateID(final SimpleString address, final byte[] duplID, final long recordID) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#updateDuplicateIDTransactional(long, org.hornetq.utils.SimpleString, byte[], long)
       */
      public void updateDuplicateIDTransactional(final long txID,
                                                 final SimpleString address,
                                                 final byte[] duplID,
                                                 final long recordID) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#updateScheduledDeliveryTime(org.hornetq.core.server.MessageReference)
       */
      public void updateScheduledDeliveryTime(final MessageReference ref) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#updateScheduledDeliveryTimeTransactional(long, org.hornetq.core.server.MessageReference)
       */
      public void updateScheduledDeliveryTimeTransactional(final long txID, final MessageReference ref) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.HornetQComponent#isStarted()
       */
      public boolean isStarted()
      {
         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.HornetQComponent#start()
       */
      public void start() throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.HornetQComponent#stop()
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
       * @see org.hornetq.core.paging.PagingStoreFactory#getGlobalDepagerExecutor()
       */
      public Executor getGlobalDepagerExecutor()
      {
         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.paging.PagingStoreFactory#newFileFactory(org.hornetq.utils.SimpleString)
       */
      public SequentialFileFactory newFileFactory(final SimpleString destinationName) throws Exception
      {
         return factory;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.paging.PagingStoreFactory#newStore(org.hornetq.utils.SimpleString, org.hornetq.core.settings.impl.AddressSettings)
       */
      public PagingStore newStore(final SimpleString destinationName, final AddressSettings addressSettings) throws Exception
      {
         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.paging.PagingStoreFactory#reloadStores(org.hornetq.core.settings.HierarchicalRepository)
       */
      public List<PagingStore> reloadStores(final HierarchicalRepository<AddressSettings> addressSettingsRepository) throws Exception
      {
         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.paging.PagingStoreFactory#setPagingManager(org.hornetq.core.paging.PagingManager)
       */
      public void setPagingManager(final PagingManager manager)
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.paging.PagingStoreFactory#setPostOffice(org.hornetq.core.postoffice.PostOffice)
       */
      public void setPostOffice(final PostOffice office)
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.paging.PagingStoreFactory#setStorageManager(org.hornetq.core.persistence.StorageManager)
       */
      public void setStorageManager(final StorageManager storageManager)
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.paging.PagingStoreFactory#stop()
       */
      public void stop() throws InterruptedException
      {
      }

   }

}
