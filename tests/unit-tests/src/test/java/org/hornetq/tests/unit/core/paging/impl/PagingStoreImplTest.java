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
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.SimpleString;
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
import org.hornetq.core.paging.impl.PagingStoreImpl;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.persistence.impl.nullpm.NullStorageManager;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.RoutingContextImpl;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.unit.core.journal.impl.fakes.FakeSequentialFileFactory;
import org.hornetq.tests.unit.util.FakePagingManager;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.ExecutorFactory;

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
         trans.increment(true);
      }

      Assert.assertEquals(nr1, trans.getNumberOfMessages());

      HornetQBuffer buffer = HornetQBuffers.fixedBuffer(trans.getEncodeSize());

      trans.encode(buffer);

      PageTransactionInfo trans2 = new PageTransactionInfoImpl(id1);
      trans2.decode(buffer);

      Assert.assertEquals(id2, trans2.getTransactionID());

      Assert.assertEquals(nr1, trans2.getNumberOfMessages());

   }

   public void testDoubleStart() throws Exception
   {
      SequentialFileFactory factory = new FakeSequentialFileFactory();

      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);

      PagingStore storeImpl = new PagingStoreImpl(PagingStoreImplTest.destinationTestName,
                                                  null,
                                                  100,
                                                  createMockManager(),
                                                  createStorageManagerMock(),
                                                  factory,
                                                  null,
                                                  PagingStoreImplTest.destinationTestName,
                                                  addressSettings,
                                                  getExecutorFactory().getExecutor(),
                                                  true);

      storeImpl.start();

      // this is not supposed to throw an exception.
      // As you could have start being called twice as Stores are dynamically
      // created, on a multi-thread environment
      storeImpl.start();

      storeImpl.stop();

   }

   public void testPageWithNIO() throws Exception
   {
      recreateDirectory(getTestDir());
      testConcurrentPaging(new NIOSequentialFileFactory(getTestDir()), 1);
   }

   public void testStore() throws Exception
   {
      SequentialFileFactory factory = new FakeSequentialFileFactory();

      PagingStoreFactory storeFactory = new FakeStoreFactory(factory);

      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      PagingStore storeImpl =
               new PagingStoreImpl(PagingStoreImplTest.destinationTestName,
                                                           null,
                                                           100,
                                                           createMockManager(),
                                                           createStorageManagerMock(),
                                                           factory,
                                                           storeFactory,
                                                           PagingStoreImplTest.destinationTestName,
                                                           addressSettings,
                                                           getExecutorFactory().getExecutor(),
                                                           true);

      storeImpl.start();

      Assert.assertEquals(0, storeImpl.getNumberOfPages());

      storeImpl.startPaging();

      Assert.assertEquals(1, storeImpl.getNumberOfPages());

      List<HornetQBuffer> buffers = new ArrayList<HornetQBuffer>();

      HornetQBuffer buffer = createRandomBuffer(0, 10);

      buffers.add(buffer);
      SimpleString destination = new SimpleString("test");

      ServerMessage msg = createMessage(1, storeImpl, destination, buffer);

      Assert.assertTrue(storeImpl.isPaging());

      Assert.assertTrue(storeImpl.page(msg, new RoutingContextImpl(null)));

      Assert.assertEquals(1, storeImpl.getNumberOfPages());

      storeImpl.sync();

      storeImpl = new PagingStoreImpl(PagingStoreImplTest.destinationTestName,
                                      null,
                                      100,
                                      createMockManager(),
                                      createStorageManagerMock(),
                                      factory,
                                      null,
                                      PagingStoreImplTest.destinationTestName,
                                      addressSettings,
                                      getExecutorFactory().getExecutor(),
                                      true);

      storeImpl.start();

      Assert.assertEquals(1, storeImpl.getNumberOfPages());

   }

   public void testDepageOnCurrentPage() throws Exception
   {
      SequentialFileFactory factory = new FakeSequentialFileFactory();

      SimpleString destination = new SimpleString("test");

      PagingStoreFactory storeFactory = new FakeStoreFactory(factory);

      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      PagingStoreImpl storeImpl =
               new PagingStoreImpl(PagingStoreImplTest.destinationTestName,
                                                           null,
                                                           100,
                                                           createMockManager(),
                                                           createStorageManagerMock(),
                                                           factory,
                                                           storeFactory,
                                                           PagingStoreImplTest.destinationTestName,
                                                           addressSettings,
                                                           getExecutorFactory().getExecutor(),
                                                           true);

      storeImpl.start();

      Assert.assertEquals(0, storeImpl.getNumberOfPages());

      storeImpl.startPaging();

      List<HornetQBuffer> buffers = new ArrayList<HornetQBuffer>();

      int numMessages = 10;

      for (int i = 0; i < numMessages; i++)
      {

         HornetQBuffer buffer = createRandomBuffer(i + 1l, 10);

         buffers.add(buffer);

         ServerMessage msg = createMessage(i, storeImpl, destination, buffer);

         Assert.assertTrue(storeImpl.page(msg, new RoutingContextImpl(null)));
      }

      Assert.assertEquals(1, storeImpl.getNumberOfPages());

      storeImpl.sync();

      Page page = storeImpl.depage();

      page.open();

      List<PagedMessage> msg = page.read(new NullStorageManager());

      Assert.assertEquals(numMessages, msg.size());
      Assert.assertEquals(1, storeImpl.getNumberOfPages());

      page = storeImpl.depage();

      Assert.assertNull(page);

      Assert.assertEquals(0, storeImpl.getNumberOfPages());

      for (int i = 0; i < numMessages; i++)
      {
         HornetQBuffer horn1 = buffers.get(i);
         HornetQBuffer horn2 = msg.get(i).getMessage().getBodyBuffer();
         horn1.resetReaderIndex();
         horn2.resetReaderIndex();
         for (int j = 0; j < horn1.writerIndex(); j++)
         {
            Assert.assertEquals(horn1.readByte(), horn2.readByte());
         }
      }

   }

   public void testDepageMultiplePages() throws Exception
   {
      SequentialFileFactory factory = new FakeSequentialFileFactory();
      SimpleString destination = new SimpleString("test");

      PagingStoreFactory storeFactory = new FakeStoreFactory(factory);

      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      PagingStore storeImpl =
               new PagingStoreImpl(PagingStoreImplTest.destinationTestName,
                                                           null,
                                                           100,
                                                           createMockManager(),
                                                           createStorageManagerMock(),
                                                           factory,
                                                           storeFactory,
                                                           PagingStoreImplTest.destinationTestName,
                                                           addressSettings,
                                                           getExecutorFactory().getExecutor(),
                                                           true);

      storeImpl.start();

      Assert.assertEquals(0, storeImpl.getNumberOfPages());

      storeImpl.startPaging();

      Assert.assertEquals(1, storeImpl.getNumberOfPages());

      List<HornetQBuffer> buffers = new ArrayList<HornetQBuffer>();

      for (int i = 0; i < 10; i++)
      {

         HornetQBuffer buffer = createRandomBuffer(i + 1l, 10);

         buffers.add(buffer);

         if (i == 5)
         {
            storeImpl.forceAnotherPage();
         }

         ServerMessage msg = createMessage(i, storeImpl, destination, buffer);

         Assert.assertTrue(storeImpl.page(msg, new RoutingContextImpl(null)));
      }

      Assert.assertEquals(2, storeImpl.getNumberOfPages());

      storeImpl.sync();

      int sequence = 0;

      for (int pageNr = 0; pageNr < 2; pageNr++)
      {
         Page page = storeImpl.depage();

         System.out.println("numberOfPages = " + storeImpl.getNumberOfPages());

         page.open();

         List<PagedMessage> msg = page.read(new NullStorageManager());

         page.close();

         Assert.assertEquals(5, msg.size());

         for (int i = 0; i < 5; i++)
         {
            Assert.assertEquals(sequence++, msg.get(i).getMessage().getMessageID());
            UnitTestCase.assertEqualsBuffers(18, buffers.get(pageNr * 5 + i), msg.get(i).getMessage().getBodyBuffer());
         }
      }

      Assert.assertEquals(1, storeImpl.getNumberOfPages());

      Assert.assertTrue(storeImpl.isPaging());

      ServerMessage msg = createMessage(1, storeImpl, destination, buffers.get(0));

      Assert.assertTrue(storeImpl.page(msg, new RoutingContextImpl(null)));

      Page newPage = storeImpl.depage();

      newPage.open();

      Assert.assertEquals(1, newPage.read(new NullStorageManager()).size());

      newPage.delete(null);

      Assert.assertEquals(1, storeImpl.getNumberOfPages());

      Assert.assertTrue(storeImpl.isPaging());

      Assert.assertNull(storeImpl.depage());

      Assert.assertFalse(storeImpl.isPaging());

      Assert.assertFalse(storeImpl.page(msg, new RoutingContextImpl(null)));

      storeImpl.startPaging();

      Assert.assertTrue(storeImpl.page(msg, new RoutingContextImpl(null)));

      Page page = storeImpl.depage();

      page.open();

      List<PagedMessage> msgs = page.read(new NullStorageManager());

      Assert.assertEquals(1, msgs.size());

      Assert.assertEquals(1l, msgs.get(0).getMessage().getMessageID());

      UnitTestCase.assertEqualsBuffers(18, buffers.get(0), msgs.get(0).getMessage().getBodyBuffer());

      Assert.assertEquals(1, storeImpl.getNumberOfPages());

      Assert.assertTrue(storeImpl.isPaging());

      Assert.assertNull(storeImpl.depage());

      Assert.assertEquals(0, storeImpl.getNumberOfPages());

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

      final ConcurrentHashMap<Long, ServerMessage> buffers = new ConcurrentHashMap<Long, ServerMessage>();

      final ArrayList<Page> readPages = new ArrayList<Page>();

      AddressSettings settings = new AddressSettings();
      settings.setPageSizeBytes(MAX_SIZE);
      settings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);

      final PagingStore storeImpl =
               new PagingStoreImpl(PagingStoreImplTest.destinationTestName,
                                                                 null,
                                                                 100,
                                                                 createMockManager(),
                                                                 createStorageManagerMock(),
                                                                 factory,
                                                                 storeFactory,
                                                                 new SimpleString("test"),
                                                                 settings,
                                                                 getExecutorFactory().getExecutor(),
                                                                 true);

      storeImpl.start();

      Assert.assertEquals(0, storeImpl.getNumberOfPages());

      // Marked the store to be paged
      storeImpl.startPaging();

      Assert.assertEquals(1, storeImpl.getNumberOfPages());

      final SimpleString destination = new SimpleString("test");

      class WriterThread extends Thread
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

                  // Each thread will Keep paging until all the messages are depaged.
                  // This is possible because the depage thread is not actually reading the pages.
                  // Just using the internal API to remove it from the page file system
                  ServerMessage msg = createMessage(id, storeImpl, destination, createRandomBuffer(id, 5));
                  if (storeImpl.page(msg, new RoutingContextImpl(null)))
                  {
                     buffers.put(id, msg);
                  }
                  else
                  {
                     break;
                  }

                  if (firstTime)
                  {
                     // We have at least one data paged. So, we can start depaging now
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

      class ReaderThread extends Thread
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

      WriterThread producerThread[] = new WriterThread[numberOfThreads];

      for (int i = 0; i < numberOfThreads; i++)
      {
         producerThread[i] = new WriterThread();
         producerThread[i].start();
      }

      ReaderThread consumer = new ReaderThread();
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

      final ConcurrentMap<Long, ServerMessage> buffers2 = new ConcurrentHashMap<Long, ServerMessage>();

      for (Page page : readPages)
      {
         page.open();
         List<PagedMessage> msgs = page.read(new NullStorageManager());
         page.close();

         for (PagedMessage msg : msgs)
         {
            long id = msg.getMessage().getBodyBuffer().readLong();
            msg.getMessage().getBodyBuffer().resetReaderIndex();

            ServerMessage msgWritten = buffers.remove(id);
            buffers2.put(id, msg.getMessage());
            Assert.assertNotNull(msgWritten);
            Assert.assertEquals(msg.getMessage().getAddress(), msgWritten.getAddress());
            UnitTestCase.assertEqualsBuffers(10, msgWritten.getBodyBuffer(), msg.getMessage().getBodyBuffer());
         }
      }

      Assert.assertEquals(0, buffers.size());

      List<String> files = factory.listFiles("page");

      Assert.assertTrue(files.size() != 0);

      for (String file : files)
      {
         SequentialFile fileTmp = factory.createSequentialFile(file, 1);
         fileTmp.open();
         Assert.assertTrue("The page file size (" + fileTmp.size() + ") shouldn't be > " + MAX_SIZE,
                           fileTmp.size() <= MAX_SIZE);
         fileTmp.close();
      }

      PagingStore storeImpl2 =
               new PagingStoreImpl(PagingStoreImplTest.destinationTestName,
                                                            null,
                                                            100,
                                                            createMockManager(),
                                                            createStorageManagerMock(),
                                                            factory,
                                                            storeFactory,
                                                            new SimpleString("test"),
                                                            settings,
                                                            getExecutorFactory().getExecutor(),
                                                            true);
      storeImpl2.start();

      int numberOfPages = storeImpl2.getNumberOfPages();
      Assert.assertTrue(numberOfPages != 0);

      storeImpl2.startPaging();

      storeImpl2.startPaging();

      Assert.assertEquals(numberOfPages, storeImpl2.getNumberOfPages());

      long lastMessageId = messageIdGenerator.incrementAndGet();
      ServerMessage lastMsg = createMessage(lastMessageId, storeImpl, destination, createRandomBuffer(lastMessageId, 5));

      storeImpl2.forceAnotherPage();

      storeImpl2.page(lastMsg, new RoutingContextImpl(null));
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

         List<PagedMessage> msgs = page.read(new NullStorageManager());

         page.close();

         for (PagedMessage msg : msgs)
         {

            long id = msg.getMessage().getBodyBuffer().readLong();
            ServerMessage msgWritten = buffers2.remove(id);
            Assert.assertNotNull(msgWritten);
            Assert.assertEquals(msg.getMessage().getAddress(), msgWritten.getAddress());
            UnitTestCase.assertEqualsByteArrays(msgWritten.getBodyBuffer().writerIndex(),
                                                msgWritten.getBodyBuffer().toByteBuffer().array(),
                                                msg.getMessage().getBodyBuffer().toByteBuffer().array());
         }
      }

      lastPage.open();
      List<PagedMessage> lastMessages = lastPage.read(new NullStorageManager());
      lastPage.close();
      Assert.assertEquals(1, lastMessages.size());

      lastMessages.get(0).getMessage().getBodyBuffer().resetReaderIndex();
      Assert.assertEquals(lastMessages.get(0).getMessage().getBodyBuffer().readLong(), lastMessageId);

      Assert.assertEquals(0, buffers2.size());

      Assert.assertEquals(0, storeImpl.getAddressSize());
   }

   public void testRestartPage() throws Throwable
   {
      clearData();
      SequentialFileFactory factory = new NIOSequentialFileFactory(UnitTestCase.getPageDir());

      PagingStoreFactory storeFactory = new FakeStoreFactory(factory);

      final int MAX_SIZE = 1024 * 10;

      AddressSettings settings = new AddressSettings();
      settings.setPageSizeBytes(MAX_SIZE);
      settings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);

      final PagingStore storeImpl =
               new PagingStoreImpl(PagingStoreImplTest.destinationTestName,
                                                                 null,
                                                                 100,
                                                                 createMockManager(),
                                                                 createStorageManagerMock(),
                                                                 factory,
                                                                 storeFactory,
                                                                 new SimpleString("test"),
                                                                 settings,
                                                                 getExecutorFactory().getExecutor(),
                                                                 true);

      storeImpl.start();

      Assert.assertEquals(0, storeImpl.getNumberOfPages());

      // Marked the store to be paged
      storeImpl.startPaging();

      storeImpl.depage();

      assertNull(storeImpl.getCurrentPage());

      storeImpl.startPaging();

      assertNotNull(storeImpl.getCurrentPage());

      storeImpl.stop();
   }

   public void testOrderOnPaging() throws Throwable
   {
      clearData();
      SequentialFileFactory factory = new NIOSequentialFileFactory(UnitTestCase.getPageDir());

      PagingStoreFactory storeFactory = new FakeStoreFactory(factory);

      final int MAX_SIZE = 1024 * 10;

      AddressSettings settings = new AddressSettings();
      settings.setPageSizeBytes(MAX_SIZE);
      settings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);

      final PagingStore storeImpl =
               new PagingStoreImpl(PagingStoreImplTest.destinationTestName,
                                                                 null,
                                                                 100,
                                                                 createMockManager(),
                                                                 createStorageManagerMock(),
                                                                 factory,
                                                                 storeFactory,
                                                                 new SimpleString("test"),
                                                                 settings,
                                                                 getExecutorFactory().getExecutor(),
                                                                 false);

      storeImpl.start();

      Assert.assertEquals(0, storeImpl.getNumberOfPages());

      // Marked the store to be paged
      storeImpl.startPaging();

      final CountDownLatch producedLatch = new CountDownLatch(1);

      Assert.assertEquals(1, storeImpl.getNumberOfPages());

      final SimpleString destination = new SimpleString("test");

      final long NUMBER_OF_MESSAGES = 100000;

      final List<Throwable> errors = new ArrayList<Throwable>();

      class WriterThread extends Thread
      {

         public WriterThread()
         {
            super("PageWriter");
         }

         @Override
         public void run()
         {

            try
            {
               for (long i = 0; i < NUMBER_OF_MESSAGES; i++)
               {
                  // Each thread will Keep paging until all the messages are depaged.
                  // This is possible because the depage thread is not actually reading the pages.
                  // Just using the internal API to remove it from the page file system
                  ServerMessage msg = createMessage(i, storeImpl, destination, createRandomBuffer(i, 1024));
                  msg.putLongProperty("count", i);
                  while (!storeImpl.page(msg, new RoutingContextImpl(null)))
                  {
                     storeImpl.startPaging();
                  }

                  if (i == 0)
                  {
                     producedLatch.countDown();
                  }
               }
            }
            catch (Throwable e)
            {
               e.printStackTrace();
               errors.add(e);
            }
         }
      }

      class ReaderThread extends Thread
      {
         public ReaderThread()
         {
            super("PageReader");
         }

         @Override
         public void run()
         {
            try
            {

               long msgsRead = 0;

               while (msgsRead < NUMBER_OF_MESSAGES)
               {
                  Page page = storeImpl.depage();
                  if (page != null)
                  {
                     page.open();
                     List<PagedMessage> messages = page.read(new NullStorageManager());

                     for (PagedMessage pgmsg : messages)
                     {
                        ServerMessage msg = pgmsg.getMessage();

                        assertEquals(msgsRead++, msg.getMessageID());

                        assertEquals(msg.getMessageID(), msg.getLongProperty("count").longValue());
                     }

                     page.close();
                     page.delete(null);
                  }
                  else
                  {
                     System.out.println("Depaged!!!! numerOfMessages = " + msgsRead + " of " + NUMBER_OF_MESSAGES);
                     Thread.sleep(500);
                  }
               }

            }
            catch (Throwable e)
            {
               e.printStackTrace();
               errors.add(e);
            }
         }
      }

      WriterThread producerThread = new WriterThread();
      producerThread.start();
      ReaderThread consumer = new ReaderThread();
      consumer.start();

      producerThread.join();
      consumer.join();

      storeImpl.stop();

      for (Throwable e : errors)
      {
         throw e;
      }
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
      return new NullStorageManager();
   }

   private ExecutorFactory getExecutorFactory()
   {
      return new ExecutorFactory()
      {

         public Executor getExecutor()
         {
            return executor;
         }
      };
   }

   private ServerMessage createMessage(final long id,
                                       final PagingStore store,
                                       final SimpleString destination,
                                       final HornetQBuffer buffer)
   {
      ServerMessage msg = new ServerMessageImpl(id, 50 + buffer.capacity());

      msg.setAddress(destination);

      msg.setPagingStore(store);

      msg.getBodyBuffer().resetReaderIndex();
      msg.getBodyBuffer().resetWriterIndex();

      msg.getBodyBuffer().writeBytes(buffer, buffer.capacity());

      return msg;
   }

   protected HornetQBuffer createRandomBuffer(final long id, final int size)
   {
      return RandomUtil.randomBuffer(size, id);
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
      public PagingStore newStore(final SimpleString destinationName, final AddressSettings addressSettings)
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
