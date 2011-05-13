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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.transaction.xa.Xid;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.journal.JournalLoadInformation;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.journal.impl.NIOSequentialFileFactory;
import org.hornetq.core.message.impl.MessageInternal;
import org.hornetq.core.paging.Page;
import org.hornetq.core.paging.PageTransactionInfo;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.paging.PagingStoreFactory;
import org.hornetq.core.paging.cursor.PagePosition;
import org.hornetq.core.paging.impl.PageTransactionInfoImpl;
import org.hornetq.core.paging.impl.PagingStoreImpl;
import org.hornetq.core.paging.impl.TestSupportPageStore;
import org.hornetq.core.persistence.GroupingInfo;
import org.hornetq.core.persistence.OperationContext;
import org.hornetq.core.persistence.QueueBindingInfo;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.persistence.config.PersistedAddressSetting;
import org.hornetq.core.persistence.config.PersistedRoles;
import org.hornetq.core.persistence.impl.nullpm.NullStorageManager;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.replication.ReplicationManager;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.group.impl.GroupBinding;
import org.hornetq.core.server.impl.RoutingContextImpl;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.transaction.ResourceManager;
import org.hornetq.tests.unit.core.journal.impl.fakes.FakeSequentialFileFactory;
import org.hornetq.tests.unit.core.server.impl.fakes.FakePostOffice;
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
         trans.increment();
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
      TestSupportPageStore storeImpl = new PagingStoreImpl(PagingStoreImplTest.destinationTestName,
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
      TestSupportPageStore storeImpl = new PagingStoreImpl(PagingStoreImplTest.destinationTestName,
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
      TestSupportPageStore storeImpl = new PagingStoreImpl(PagingStoreImplTest.destinationTestName,
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

      final TestSupportPageStore storeImpl = new PagingStoreImpl(PagingStoreImplTest.destinationTestName,
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

      TestSupportPageStore storeImpl2 = new PagingStoreImpl(PagingStoreImplTest.destinationTestName,
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
      SequentialFileFactory factory = new NIOSequentialFileFactory(this.getPageDir());

      PagingStoreFactory storeFactory = new FakeStoreFactory(factory);

      final int MAX_SIZE = 1024 * 10;

      AddressSettings settings = new AddressSettings();
      settings.setPageSizeBytes(MAX_SIZE);
      settings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);

      final TestSupportPageStore storeImpl = new PagingStoreImpl(PagingStoreImplTest.destinationTestName,
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
      SequentialFileFactory factory = new NIOSequentialFileFactory(this.getPageDir());

      PagingStoreFactory storeFactory = new FakeStoreFactory(factory);

      final int MAX_SIZE = 1024 * 10;

      AddressSettings settings = new AddressSettings();
      settings.setPageSizeBytes(MAX_SIZE);
      settings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);

      final TestSupportPageStore storeImpl = new PagingStoreImpl(PagingStoreImplTest.destinationTestName,
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
      return new FakeStorageManager();
   }

   private PostOffice createPostOfficeMock()
   {
      return new FakePostOffice();
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

      public void deletePageStore(SimpleString storeName) throws Exception
      {
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

      /* (non-Javadoc)
       * @see org.hornetq.core.paging.PagingManager#getTransactions()
       */
      public Map<Long, PageTransactionInfo> getTransactions()
      {
         // TODO Auto-generated method stub
         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.paging.PagingManager#processReload()
       */
      public void processReload()
      {
         // TODO Auto-generated method stub

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.settings.HierarchicalRepositoryChangeListener#onChange()
       */
      public void onChange()
      {
      }

   }

   class FakeStorageManager implements StorageManager
   {

      public void setUniqueIDSequence(final long id)
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
       * @see org.hornetq.core.persistence.StorageManager#loadBindingJournal(java.util.List)
       */
      public JournalLoadInformation loadBindingJournal(final List<QueueBindingInfo> queueBindingInfos,
                                                       final List<GroupingInfo> groupingInfos) throws Exception
      {
         return new JournalLoadInformation();
      }

      public void addGrouping(final GroupBinding groupBinding) throws Exception
      {
         // To change body of implemented methods use File | Settings | File Templates.
      }

      public void deleteGrouping(final GroupBinding groupBinding) throws Exception
      {
         // To change body of implemented methods use File | Settings | File Templates.
      }

      public void sync()
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#loadMessageJournal(org.hornetq.core.paging.PagingManager, java.util.Map, org.hornetq.core.transaction.ResourceManager, java.util.Map)
       */
      public JournalLoadInformation loadMessageJournal(final PostOffice postOffice,
                                                       final PagingManager pagingManager,
                                                       final ResourceManager resourceManager,
                                                       final Map<Long, Queue> queues,
                                                       Map<Long, QueueBindingInfo> queueInfos,
                                                       final Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap) throws Exception
      {
         return new JournalLoadInformation();
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

      public long storeHeuristicCompletion(final Xid xid, final boolean isCommit) throws Exception
      {
         return -1;
      }

      public void deleteHeuristicCompletion(final long txID) throws Exception
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

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#afterReplicated(java.lang.Runnable)
       */
      public void afterCompleteOperations(final Runnable run)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#completeReplication()
       */
      public void completeOperations()
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#createLargeMessage(byte[])
       */
      public LargeServerMessage createLargeMessage(final long messageId, final MessageInternal msg)
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#isReplicated()
       */
      public boolean isReplicated()
      {

         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#loadInternalOnly()
       */
      public JournalLoadInformation[] loadInternalOnly() throws Exception
      {
         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#pageClosed(org.hornetq.utils.SimpleString, int)
       */
      public void pageClosed(final SimpleString storeName, final int pageNumber)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#pageDeleted(org.hornetq.utils.SimpleString, int)
       */
      public void pageDeleted(final SimpleString storeName, final int pageNumber)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#pageWrite(org.hornetq.core.paging.PagedMessage, int)
       */
      public void pageWrite(final PagedMessage message, final int pageNumber)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#blockOnReplication(long)
       */
      public boolean waitOnOperations(final long timeout) throws Exception
      {
         return true;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#setReplicator(org.hornetq.core.replication.ReplicationManager)
       */
      public void setReplicator(final ReplicationManager replicator)
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#afterCompleteOperations(org.hornetq.core.journal.IOCompletion)
       */
      public void afterCompleteOperations(final IOAsyncTask run)
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#waitOnOperations()
       */
      public void waitOnOperations() throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#getContext()
       */
      public OperationContext getContext()
      {
         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#newContext(java.util.concurrent.Executor)
       */
      public OperationContext newContext(final Executor executor)
      {
         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#clearContext()
       */
      public void clearContext()
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#setContext(org.hornetq.core.persistence.OperationContext)
       */
      public void setContext(final OperationContext context)
      {
      }

      public void storeReference(final long queueID, final long messageID, final boolean last) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#recoverAddressSettings()
       */
      public List<PersistedAddressSetting> recoverAddressSettings() throws Exception
      {
         return Collections.emptyList();
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#recoverPersistedRoles()
       */
      public List<PersistedRoles> recoverPersistedRoles() throws Exception
      {
         return Collections.emptyList();
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#storeAddressSetting(org.hornetq.core.persistconfig.PersistedAddressSetting)
       */
      public void storeAddressSetting(PersistedAddressSetting addressSetting) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#storeSecurityRoles(org.hornetq.core.persistconfig.PersistedRoles)
       */
      public void storeSecurityRoles(PersistedRoles persistedRoles) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#deleteAddressSetting(org.hornetq.api.core.SimpleString)
       */
      public void deleteAddressSetting(SimpleString addressMatch) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#deleteSecurityRoles(org.hornetq.api.core.SimpleString)
       */
      public void deleteSecurityRoles(SimpleString addressMatch) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#deletePageTransactional(long)
       */
      public void deletePageTransactional(long recordID) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#updatePageTransaction(long, org.hornetq.core.paging.PageTransactionInfo, int)
       */
      public void updatePageTransaction(long txID, PageTransactionInfo pageTransaction, int depage) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#storeCursorAcknowledge(long, org.hornetq.core.paging.cursor.PagePosition)
       */
      public void storeCursorAcknowledge(long queueID, PagePosition position)
      {
         // TODO Auto-generated method stub

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#storeCursorAcknowledgeTransactional(long, long, org.hornetq.core.paging.cursor.PagePosition)
       */
      public void storeCursorAcknowledgeTransactional(long txID, long queueID, PagePosition position)
      {
         // TODO Auto-generated method stub

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#deleteCursorAcknowledgeTransactional(long, long)
       */
      public void deleteCursorAcknowledgeTransactional(long txID, long ackID) throws Exception
      {
         // TODO Auto-generated method stub

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#updatePageTransaction(org.hornetq.core.paging.PageTransactionInfo, int)
       */
      public void updatePageTransaction(PageTransactionInfo pageTransaction, int depage) throws Exception
      {
         // TODO Auto-generated method stub

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#storePageCounter(long, long, long)
       */
      public long storePageCounter(long txID, long queueID, long value) throws Exception
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#deleteIncrementRecord(long, long)
       */
      public void deleteIncrementRecord(long txID, long recordID) throws Exception
      {
         // TODO Auto-generated method stub

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#deletePageCounter(long, long)
       */
      public void deletePageCounter(long txID, long recordID) throws Exception
      {
         // TODO Auto-generated method stub

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#storePageCounterInc(long, long, int)
       */
      public long storePageCounterInc(long txID, long queueID, int add) throws Exception
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#storePageCounterInc(long, int)
       */
      public long storePageCounterInc(long queueID, int add) throws Exception
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#newSingleThreadContext()
       */
      public OperationContext newSingleThreadContext()
      {
         return getContext();
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#commit(long, boolean)
       */
      public void commit(long txID, boolean lineUpContext) throws Exception
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.StorageManager#lineUpContext()
       */
      public void lineUpContext()
      {
         // TODO Auto-generated method stub
         
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
