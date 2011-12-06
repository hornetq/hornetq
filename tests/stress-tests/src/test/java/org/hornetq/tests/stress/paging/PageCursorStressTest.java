/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.tests.stress.paging;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.paging.cursor.PageCache;
import org.hornetq.core.paging.cursor.PageCursorProvider;
import org.hornetq.core.paging.cursor.PagePosition;
import org.hornetq.core.paging.cursor.PageSubscription;
import org.hornetq.core.paging.cursor.PagedReference;
import org.hornetq.core.paging.cursor.impl.PageCursorProviderImpl;
import org.hornetq.core.paging.cursor.impl.PagePositionImpl;
import org.hornetq.core.paging.impl.PagingStoreImpl;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.persistence.impl.journal.OperationContextImpl;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.RoutingContext;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.RoutingContextImpl;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.impl.TransactionImpl;
import org.hornetq.tests.unit.core.postoffice.impl.FakeQueue;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.LinkedListIterator;

/**
 * A PageCursorTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class PageCursorStressTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final SimpleString ADDRESS = new SimpleString("test-add");

   private HornetQServer server;

   private Queue queue;

   private List<Queue> queueList;

   private static final int PAGE_MAX = -1;

   private static final int PAGE_SIZE = 10 * 1024 * 1024;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Read more cache than what would fit on the memory, and validate if the memory would be cleared through soft-caches
   public void testReadCache() throws Exception
   {

      final int NUM_MESSAGES = 100;

      int numberOfPages = addMessages(NUM_MESSAGES, 1024 * 1024);

      System.out.println("NumberOfPages = " + numberOfPages);

      PageCursorProviderImpl cursorProvider = new PageCursorProviderImpl(lookupPageStore(ADDRESS),
                                                                         server.getStorageManager(),
                                                                         server.getExecutorFactory().getExecutor(),
                                                                         5);

      for (int i = 0; i < numberOfPages; i++)
      {
         PageCache cache = cursorProvider.getPageCache(new PagePositionImpl(i + 1, 0));
         System.out.println("Page " + i + " had " + cache.getNumberOfMessages() + " messages");

      }

      forceGC();

      assertTrue(cursorProvider.getCacheSize() < numberOfPages);

      System.out.println("Cache size = " + cursorProvider.getCacheSize());
   }

   public void testSimpleCursor() throws Exception
   {

      final int NUM_MESSAGES = 100;

      PageSubscription cursor = lookupPageStore(ADDRESS).getCursorProvider().getSubscription(queue.getID());

      Iterator<PagedReference> iterEmpty = cursor.iterator();

      int numberOfPages = addMessages(NUM_MESSAGES, 1024 * 1024);

      System.out.println("NumberOfPages = " + numberOfPages);

      PagedReference msg;

      LinkedListIterator<PagedReference> iterator = cursor.iterator();
      int key = 0;
      while ((msg = iterator.next()) != null)
      {
         assertEquals(key++, msg.getMessage().getIntProperty("key").intValue());
         cursor.confirmPosition(msg.getPosition());
      }
      assertEquals(NUM_MESSAGES, key);

      server.getStorageManager().waitOnOperations();

      waitCleanup();

      assertFalse(lookupPageStore(ADDRESS).isPaging());

      assertEquals(1, lookupPageStore(ADDRESS).getNumberOfPages());

      forceGC();

      server.stop();
      createServer();
      waitCleanup();
      assertEquals(1, lookupPageStore(ADDRESS).getNumberOfPages());

   }

   public void testSimpleCursorWithFilter() throws Exception
   {

      final int NUM_MESSAGES = 100;

      PageSubscription cursorEven = createNonPersistentCursor(new Filter()
      {

         public boolean match(ServerMessage message)
         {
            Boolean property = message.getBooleanProperty("even");
            if (property == null)
            {
               return false;
            }
            else
            {
               return property.booleanValue();
            }
         }

         public SimpleString getFilterString()
         {
            return new SimpleString("even=true");
         }

      });

      PageSubscription cursorOdd = createNonPersistentCursor(new Filter()
      {

         public boolean match(ServerMessage message)
         {
            Boolean property = message.getBooleanProperty("even");
            if (property == null)
            {
               return false;
            }
            else
            {
               return !property.booleanValue();
            }
         }

         public SimpleString getFilterString()
         {
            return new SimpleString("even=true");
         }

      });

      int numberOfPages = addMessages(NUM_MESSAGES, 1024 * 1024);

      System.out.println("NumberOfPages = " + numberOfPages);

      queue.getPageSubscription().close();

      PagedReference msg;

      LinkedListIterator<PagedReference> iteratorEven = cursorEven.iterator();

      LinkedListIterator<PagedReference> iteratorOdd = cursorOdd.iterator();

      int key = 0;
      while ((msg = iteratorEven.next()) != null)
      {
         System.out.println("Received" + msg);
         assertEquals(key, msg.getMessage().getIntProperty("key").intValue());
         assertTrue(msg.getMessage().getBooleanProperty("even").booleanValue());
         key += 2;
         cursorEven.confirmPosition(msg.getPosition());
      }
      assertEquals(NUM_MESSAGES, key);

      key = 1;
      while ((msg = iteratorOdd.next()) != null)
      {
         assertEquals(key, msg.getMessage().getIntProperty("key").intValue());
         assertFalse(msg.getMessage().getBooleanProperty("even").booleanValue());
         key += 2;
         cursorOdd.confirmPosition(msg.getPosition());
      }
      assertEquals(NUM_MESSAGES + 1, key);

      forceGC();

      // assertTrue(lookupCursorProvider().getCacheSize() < numberOfPages);

      server.stop();
      createServer();
      waitCleanup();
      assertEquals(1, lookupPageStore(ADDRESS).getNumberOfPages());

   }

   public void testReadNextPage() throws Exception
   {

      final int NUM_MESSAGES = 1;

      int numberOfPages = addMessages(NUM_MESSAGES, 1024);

      System.out.println("NumberOfPages = " + numberOfPages);

      PageCursorProvider cursorProvider = lookupCursorProvider();

      PageCache cache = cursorProvider.getPageCache(new PagePositionImpl(2, 0));

      assertNull(cache);
   }

   public void testRestart() throws Exception
   {
      final int NUM_MESSAGES = 1000;

      int numberOfPages = addMessages(NUM_MESSAGES, 100 * 1024);

      System.out.println("Number of pages = " + numberOfPages);

      PageCursorProvider cursorProvider = lookupCursorProvider();

      PageSubscription cursor = this.server.getPagingManager()
                                           .getPageStore(ADDRESS)
                                           .getCursorProvider()
                                           .getSubscription(queue.getID());

      PageCache firstPage = cursorProvider.getPageCache(new PagePositionImpl(server.getPagingManager()
                                                                                   .getPageStore(ADDRESS)
                                                                                   .getFirstPage(), 0));

      int firstPageSize = firstPage.getNumberOfMessages();

      firstPage = null;

      System.out.println("Cursor: " + cursor);
      cursorProvider.printDebug();

      LinkedListIterator<PagedReference> iterator = cursor.iterator();

      for (int i = 0; i < 1000; i++)
      {
         System.out.println("Reading Msg : " + i);
         PagedReference msg = iterator.next();
         assertNotNull(msg);
         assertEquals(i, msg.getMessage().getIntProperty("key").intValue());

         if (i < firstPageSize)
         {
            cursor.ack(msg);
         }
      }
      cursorProvider.printDebug();

      server.getStorageManager().waitOnOperations();
      lookupPageStore(ADDRESS).flushExecutors();

      // needs to clear the context since we are using the same thread over two distinct servers
      // otherwise we will get the old executor on the factory
      OperationContextImpl.clearContext();

      server.stop();

      server.start();

      cursor = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvider().getSubscription(queue.getID());

      iterator = cursor.iterator();

      for (int i = firstPageSize; i < NUM_MESSAGES; i++)
      {
         PagedReference msg = iterator.next();
         assertNotNull("Received " + i, msg);
         assertEquals(i, msg.getMessage().getIntProperty("key").intValue());

         cursor.ack(msg);

         OperationContextImpl.getContext(null).waitCompletion();

      }

      OperationContextImpl.getContext(null).waitCompletion();

      lookupPageStore(ADDRESS).flushExecutors();

      assertFalse(lookupPageStore(ADDRESS).isPaging());

      server.stop();
      createServer();
      assertFalse(lookupPageStore(ADDRESS).isPaging());
      waitCleanup();
      assertEquals(1, lookupPageStore(ADDRESS).getNumberOfPages());

   }

   public void testRestartWithHoleOnAck() throws Exception
   {

      final int NUM_MESSAGES = 1000;

      int numberOfPages = addMessages(NUM_MESSAGES, 10 * 1024);

      System.out.println("Number of pages = " + numberOfPages);

      PageCursorProvider cursorProvider = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvider();
      System.out.println("cursorProvider = " + cursorProvider);

      PageSubscription cursor = this.server.getPagingManager()
                                           .getPageStore(ADDRESS)
                                           .getCursorProvider()
                                           .getSubscription(queue.getID());

      System.out.println("Cursor: " + cursor);
      LinkedListIterator<PagedReference> iterator = cursor.iterator();
      for (int i = 0; i < 100; i++)
      {
         PagedReference msg = iterator.next();
         assertEquals(i, msg.getMessage().getIntProperty("key").intValue());
         if (i < 10 || i > 20)
         {
            cursor.ack(msg);
         }
      }

      server.getStorageManager().waitOnOperations();

      server.stop();

      OperationContextImpl.clearContext();

      server.start();

      cursor = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvider().getSubscription(queue.getID());
      iterator = cursor.iterator();

      for (int i = 10; i <= 20; i++)
      {
         PagedReference msg = iterator.next();
         assertEquals(i, msg.getMessage().getIntProperty("key").intValue());
         cursor.ack(msg);
      }

      for (int i = 100; i < NUM_MESSAGES; i++)
      {
         PagedReference msg = iterator.next();
         assertEquals(i, msg.getMessage().getIntProperty("key").intValue());
         cursor.ack(msg);
      }

      server.stop();
      createServer();
      waitCleanup();
      assertEquals(1, lookupPageStore(ADDRESS).getNumberOfPages());

   }

   public void testRestartWithHoleOnAckAndTransaction() throws Exception
   {
      final int NUM_MESSAGES = 1000;

      int numberOfPages = addMessages(NUM_MESSAGES, 10 * 1024);

      System.out.println("Number of pages = " + numberOfPages);

      PageCursorProvider cursorProvider = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvider();
      System.out.println("cursorProvider = " + cursorProvider);

      PageSubscription cursor = this.server.getPagingManager()
                                           .getPageStore(ADDRESS)
                                           .getCursorProvider()
                                           .getSubscription(queue.getID());

      System.out.println("Cursor: " + cursor);

      Transaction tx = new TransactionImpl(server.getStorageManager(), 60 * 1000);

      LinkedListIterator<PagedReference> iterator = cursor.iterator();

      for (int i = 0; i < 100; i++)
      {
         PagedReference msg = iterator.next();
         assertEquals(i, msg.getMessage().getIntProperty("key").intValue());
         if (i < 10 || i > 20)
         {
            cursor.ackTx(tx, msg);
         }
      }

      tx.commit();

      server.stop();

      OperationContextImpl.clearContext();

      server.start();

      cursor = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvider().getSubscription(queue.getID());

      tx = new TransactionImpl(server.getStorageManager(), 60 * 1000);
      iterator = cursor.iterator();

      for (int i = 10; i <= 20; i++)
      {
         PagedReference msg = iterator.next();
         assertEquals(i, msg.getMessage().getIntProperty("key").intValue());
         cursor.ackTx(tx, msg);
      }

      for (int i = 100; i < NUM_MESSAGES; i++)
      {
         PagedReference msg = iterator.next();
         assertEquals(i, msg.getMessage().getIntProperty("key").intValue());
         cursor.ackTx(tx, msg);
      }

      tx.commit();

      server.stop();
      createServer();
      waitCleanup();
      assertEquals(1, lookupPageStore(ADDRESS).getNumberOfPages());

   }

   public void testConsumeLivePage() throws Exception
   {
      PagingStoreImpl pageStore = lookupPageStore(ADDRESS);

      pageStore.startPaging();

      final int NUM_MESSAGES = 100;

      final int messageSize = 1024 * 1024;

      PageCursorProvider cursorProvider = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvider();
      System.out.println("cursorProvider = " + cursorProvider);

      PageSubscription cursor = this.server.getPagingManager()
                                           .getPageStore(ADDRESS)
                                           .getCursorProvider()
                                           .getSubscription(queue.getID());

      System.out.println("Cursor: " + cursor);

      RoutingContextImpl ctx = generateCTX();

      LinkedListIterator<PagedReference> iterator = cursor.iterator();

      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         // if (i % 100 == 0)
         System.out.println("read/written " + i);

         HornetQBuffer buffer = RandomUtil.randomBuffer(messageSize, i + 1l);

         ServerMessage msg = new ServerMessageImpl(i, buffer.writerIndex());
         msg.putIntProperty("key", i);

         msg.getBodyBuffer().writeBytes(buffer, 0, buffer.writerIndex());

         Assert.assertTrue(pageStore.page(msg, ctx, ctx.getContextListing(ADDRESS)));

         PagedReference readMessage = iterator.next();

         assertNotNull(readMessage);

         assertEquals(i, readMessage.getMessage().getIntProperty("key").intValue());

         assertNull(iterator.next());
      }

      OperationContextImpl.clearContext();

      ctx = generateCTX();

      pageStore = lookupPageStore(ADDRESS);

      cursor = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvider().getSubscription(queue.getID());
      iterator = cursor.iterator();

      for (int i = 0; i < NUM_MESSAGES * 2; i++)
      {
         if (i % 100 == 0)
            System.out.println("Paged " + i);

         if (i >= NUM_MESSAGES)
         {

            HornetQBuffer buffer = RandomUtil.randomBuffer(messageSize, i + 1l);

            ServerMessage msg = new ServerMessageImpl(i, buffer.writerIndex());
            msg.putIntProperty("key", i);

            msg.getBodyBuffer().writeBytes(buffer, 0, buffer.writerIndex());

            Assert.assertTrue(pageStore.page(msg, ctx, ctx.getContextListing(ADDRESS)));
         }

         PagedReference readMessage = iterator.next();

         assertNotNull(readMessage);

         assertEquals(i, readMessage.getMessage().getIntProperty("key").intValue());
      }

      OperationContextImpl.clearContext();

      pageStore = lookupPageStore(ADDRESS);

      cursor = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvider().getSubscription(queue.getID());
      iterator = cursor.iterator();

      for (int i = 0; i < NUM_MESSAGES * 3; i++)
      {
         if (i % 100 == 0)
            System.out.println("Paged " + i);

         if (i >= NUM_MESSAGES * 2 - 1)
         {

            HornetQBuffer buffer = RandomUtil.randomBuffer(messageSize, i + 1l);

            ServerMessage msg = new ServerMessageImpl(i, buffer.writerIndex());
            msg.putIntProperty("key", i + 1);

            msg.getBodyBuffer().writeBytes(buffer, 0, buffer.writerIndex());

            Assert.assertTrue(pageStore.page(msg, ctx, ctx.getContextListing(ADDRESS)));
         }

         PagedReference readMessage = iterator.next();

         assertNotNull(readMessage);

         cursor.ack(readMessage);

         assertEquals(i, readMessage.getMessage().getIntProperty("key").intValue());
      }

      PagedReference readMessage = iterator.next();

      assertEquals(NUM_MESSAGES * 3, readMessage.getMessage().getIntProperty("key").intValue());

      cursor.ack(readMessage);

      server.getStorageManager().waitOnOperations();

      pageStore.flushExecutors();

      assertFalse(pageStore.isPaging());

      server.stop();
      createServer();

      assertFalse(pageStore.isPaging());

      waitCleanup();

      assertFalse(lookupPageStore(ADDRESS).isPaging());

   }


   public void testConsumeLivePageMultiThread() throws Exception
   {
      final PagingStoreImpl pageStore = lookupPageStore(ADDRESS);

      pageStore.startPaging();

      final int NUM_TX = 100;

      final int MSGS_TX = 100;

      final int TOTAL_MSG = NUM_TX * MSGS_TX;

      final int messageSize = 1024;

      PageCursorProvider cursorProvider = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvider();
      System.out.println("cursorProvider = " + cursorProvider);

      PageSubscription cursor = this.server.getPagingManager()
                                           .getPageStore(ADDRESS)
                                           .getCursorProvider()
                                           .getSubscription(queue.getID());

      System.out.println("Cursor: " + cursor);

      final StorageManager storage = this.server.getStorageManager();

      final AtomicInteger exceptions = new AtomicInteger(0);

      Thread t1 = new Thread()
      {
         @Override
         public void run()
         {
            try
            {
               int count = 0;

               for (int txCount = 0; txCount < NUM_TX; txCount++)
               {

                  Transaction tx = null;

                  if (txCount % 2 == 0)
                  {
                     tx = new TransactionImpl(storage);
                  }

                  RoutingContext ctx = generateCTX(tx);

                  for (int i = 0 ; i < MSGS_TX; i++)
                  {
                     //System.out.println("Sending " + count);
                     HornetQBuffer buffer = RandomUtil.randomBuffer(messageSize, count);

                     ServerMessage msg = new ServerMessageImpl(i, buffer.writerIndex());
                     msg.putIntProperty("key", count++);

                     msg.getBodyBuffer().writeBytes(buffer, 0, buffer.writerIndex());

                     Assert.assertTrue(pageStore.page(msg, ctx, ctx.getContextListing(ADDRESS)));
                  }

                  if (tx != null)
                  {
                     tx.commit();
                  }

               }
            }
            catch (Throwable e)
            {
               e.printStackTrace();
               exceptions.incrementAndGet();
            }
         }
      };

      t1.start();


      LinkedListIterator<PagedReference> iterator = cursor.iterator();

      for (int i = 0 ; i < TOTAL_MSG; i++ )
      {
         assertEquals(0, exceptions.get());
         PagedReference ref = null;
         for (int repeat = 0 ; repeat < 5; repeat++)
         {
            ref = iterator.next();
            if (ref == null)
            {
               Thread.sleep(1000);
            }
            else
            {
               break;
            }
         }
         assertNotNull(ref);

         ref.acknowledge();
         assertNotNull(ref);

         System.out.println("Consuming " + ref.getMessage().getIntProperty("key"));
         //assertEquals(i, ref.getMessage().getIntProperty("key").intValue());
      }

      assertEquals(0, exceptions.get());
   }

   private RoutingContextImpl generateCTX()
   {
      return generateCTX(null);
   }

   private RoutingContextImpl generateCTX(Transaction tx)
   {
      RoutingContextImpl ctx = new RoutingContextImpl(tx);
      ctx.addQueue(ADDRESS, queue);

      for (Queue q : this.queueList)
      {
         ctx.addQueue(ADDRESS, q);
      }

      return ctx;
   }

   /**
    * @throws Exception
    * @throws InterruptedException
    */
   private void waitCleanup() throws Exception, InterruptedException
   {
      // The cleanup is done asynchronously, so we need to wait some time
      long timeout = System.currentTimeMillis() + 10000;

      while (System.currentTimeMillis() < timeout && lookupPageStore(ADDRESS).getNumberOfPages() != 1)
      {
         Thread.sleep(100);
      }

      assertTrue("expected " + lookupPageStore(ADDRESS).getNumberOfPages(),
                 lookupPageStore(ADDRESS).getNumberOfPages() <= 2);
   }

   public void testPrepareScenarios() throws Exception
   {
      PagingStoreImpl pageStore = lookupPageStore(ADDRESS);

      pageStore.startPaging();

      final int NUM_MESSAGES = 100;

      final int messageSize = 100 * 1024;

      PageCursorProvider cursorProvider = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvider();
      System.out.println("cursorProvider = " + cursorProvider);

      PageSubscription cursor = this.server.getPagingManager()
                                           .getPageStore(ADDRESS)
                                           .getCursorProvider()
                                           .getSubscription(queue.getID());
      LinkedListIterator<PagedReference> iterator = cursor.iterator();

      System.out.println("Cursor: " + cursor);

      StorageManager storage = this.server.getStorageManager();

      long pgtxRollback = storage.generateUniqueID();
      long pgtxForgotten = storage.generateUniqueID();
      long pgtxCommit = storage.generateUniqueID();

      Transaction txRollback = pgMessages(storage, pageStore, pgtxRollback, 0, NUM_MESSAGES, messageSize);
      pageStore.forceAnotherPage();
      Transaction txForgotten = pgMessages(storage, pageStore, pgtxForgotten, 100, NUM_MESSAGES, messageSize);
      pageStore.forceAnotherPage();
      Transaction txCommit = pgMessages(storage, pageStore, pgtxCommit, 200, NUM_MESSAGES, messageSize);
      pageStore.forceAnotherPage();

      addMessages(300, NUM_MESSAGES, messageSize);

      System.out.println("Number of pages - " + pageStore.getNumberOfPages());

      // First consume what's already there without any tx as nothing was committed
      for (int i = 300; i < 400; i++)
      {
         PagedReference pos = iterator.next();
         assertNotNull("Null at position " + i, pos);
         assertEquals(i, pos.getMessage().getIntProperty("key").intValue());
         cursor.ack(pos);
      }

      assertNull(iterator.next());

      cursor.printDebug();

      txCommit.commit();

      txRollback.rollback();

      storage.waitOnOperations();

      // Second:after pgtxCommit was done
      for (int i = 200; i < 300; i++)
      {
         PagedReference pos = iterator.next();
         assertNotNull(pos);
         assertEquals(i, pos.getMessage().getIntProperty("key").intValue());
         cursor.ack(pos);
      }

      assertNull(iterator.next());

      server.getStorageManager().waitOnOperations();

      server.stop();
      createServer();

      long timeout = System.currentTimeMillis() + 10000;

      while (System.currentTimeMillis() < timeout && lookupPageStore(ADDRESS).getNumberOfPages() != 1)
      {
         Thread.sleep(500);
      }
      assertEquals(1, lookupPageStore(ADDRESS).getNumberOfPages());

   }


   public void testLazyCommit() throws Exception
   {
      PagingStoreImpl pageStore = lookupPageStore(ADDRESS);

      pageStore.startPaging();

      final int NUM_MESSAGES = 100;

      final int messageSize = 100 * 1024;

      PageCursorProvider cursorProvider = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvider();
      System.out.println("cursorProvider = " + cursorProvider);

      PageSubscription cursor = this.server.getPagingManager()
                                           .getPageStore(ADDRESS)
                                           .getCursorProvider()
                                           .getSubscription(queue.getID());
      LinkedListIterator<PagedReference> iterator = cursor.iterator();

      System.out.println("Cursor: " + cursor);

      StorageManager storage = this.server.getStorageManager();

      long pgtxLazy = storage.generateUniqueID();

      Transaction txLazy = pgMessages(storage, pageStore, pgtxLazy, 0, NUM_MESSAGES, messageSize);

      addMessages(100, NUM_MESSAGES, messageSize);

      System.out.println("Number of pages - " + pageStore.getNumberOfPages());

      // First consume what's already there without any tx as nothing was committed
      for (int i = 100; i < 200; i++)
      {
         PagedReference pos = iterator.next();
         assertNotNull("Null at position " + i, pos);
         assertEquals(i, pos.getMessage().getIntProperty("key").intValue());
         cursor.ack(pos);
      }

      assertNull(iterator.next());

      txLazy.commit();

      storage.waitOnOperations();

      for (int i = 0; i < 100; i++)
      {
         PagedReference pos = iterator.next();
         assertNotNull("Null at position " + i, pos);
         assertEquals(i, pos.getMessage().getIntProperty("key").intValue());
         cursor.ack(pos);
      }

      assertNull(iterator.next());

      waitCleanup();

      server.stop();
      createServer();
      waitCleanup();
      assertEquals(1, lookupPageStore(ADDRESS).getNumberOfPages());

   }

   public void testNoCursors() throws Exception
   {

      final int NUM_MESSAGES = 100;

      int numberOfPages = addMessages(NUM_MESSAGES, 1024 * 1024);

      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory sf = locator.createSessionFactory();
      ClientSession session = sf.createSession();
      session.deleteQueue(ADDRESS);

      System.out.println("NumberOfPages = " + numberOfPages);

      session.close();
      sf.close();
      locator.close();
      server.stop();
      createServer();
      waitCleanup();
      assertEquals(0, lookupPageStore(ADDRESS).getNumberOfPages());

   }

   public void testFirstMessageInTheMiddlePersistent() throws Exception
   {

      final int NUM_MESSAGES = 100;

      int numberOfPages = addMessages(NUM_MESSAGES, 1024 * 1024);

      System.out.println("NumberOfPages = " + numberOfPages);

      PageCursorProvider cursorProvider = lookupCursorProvider();

      PageCache cache = cursorProvider.getPageCache(new PagePositionImpl(5, 0));

      PageSubscription cursor = cursorProvider.getSubscription(queue.getID());
      PagePosition startingPos = new PagePositionImpl(5, cache.getNumberOfMessages() / 2);
      cursor.bookmark(startingPos);

      // We can't proceed until the operation has finished
      server.getStorageManager().waitOnOperations();

      PagedMessage msg = cache.getMessage(startingPos.getMessageNr() + 1);
      msg.initMessage(server.getStorageManager());
      int initialKey = msg.getMessage().getIntProperty("key").intValue();
      int key = initialKey;

      msg = null;

      cache = null;

      LinkedListIterator<PagedReference> iterator = cursor.iterator();

      PagedReference msgCursor = null;
      while ((msgCursor = iterator.next()) != null)
      {
         assertEquals(key++, msgCursor.getMessage().getIntProperty("key").intValue());
      }
      assertEquals(NUM_MESSAGES, key);

      server.stop();

      OperationContextImpl.clearContext();

      createServer();

      cursorProvider = lookupCursorProvider();
      cursor = cursorProvider.getSubscription(queue.getID());
      key = initialKey;
      iterator = cursor.iterator();
      while ((msgCursor = iterator.next()) != null)
      {
         assertEquals(key++, msgCursor.getMessage().getIntProperty("key").intValue());
         cursor.ack(msgCursor);
      }

      forceGC();

      assertTrue(cursorProvider.getCacheSize() < numberOfPages);

      server.stop();
      createServer();
      waitCleanup();
      assertEquals(1, lookupPageStore(ADDRESS).getNumberOfPages());

   }

   private int tstProperty(ServerMessage msg)
   {
      return msg.getIntProperty("key").intValue();
   }

   public void testMultipleIterators() throws Exception
   {

      final int NUM_MESSAGES = 10;

      int numberOfPages = addMessages(NUM_MESSAGES, 1024 * 1024);

      System.out.println("NumberOfPages = " + numberOfPages);

      PageCursorProvider cursorProvider = lookupCursorProvider();

      PageSubscription cursor = cursorProvider.getSubscription(queue.getID());

      LinkedListIterator<PagedReference> iter = cursor.iterator();

      LinkedListIterator<PagedReference> iter2 = cursor.iterator();

      assertTrue(iter.hasNext());

      PagedReference msg1 = iter.next();

      PagedReference msg2 = iter2.next();

      assertEquals(tstProperty(msg1.getMessage()), tstProperty(msg2.getMessage()));

      System.out.println("property = " + tstProperty(msg1.getMessage()));

      msg1 = iter.next();

      assertEquals(1, tstProperty(msg1.getMessage()));

      iter.remove();

      msg2 = iter2.next();

      assertEquals(2, tstProperty(msg2.getMessage()));

      iter2.repeat();

      msg2 = iter2.next();

      assertEquals(2, tstProperty(msg2.getMessage()));

      iter2.repeat();

      assertEquals(2, tstProperty(msg2.getMessage()));

      msg1 = iter.next();

      assertEquals(2, tstProperty(msg1.getMessage()));

      iter.repeat();

      msg1 = iter.next();

      assertEquals(2, tstProperty(msg1.getMessage()));

      assertTrue(iter2.hasNext());


   }

   private int addMessages(final int numMessages, final int messageSize) throws Exception
   {
      return addMessages(0, numMessages, messageSize);
   }

   /**
    * @param numMessages
    * @param pageStore
    * @throws Exception
    */
   private int addMessages(final int start, final int numMessages, final int messageSize) throws Exception
   {
      PagingStoreImpl pageStore = lookupPageStore(ADDRESS);

      pageStore.startPaging();

      RoutingContext ctx = generateCTX();

      for (int i = start; i < start + numMessages; i++)
      {
         if (i % 100 == 0)
            System.out.println("Paged " + i);
         HornetQBuffer buffer = RandomUtil.randomBuffer(messageSize, i + 1l);

         ServerMessage msg = new ServerMessageImpl(i, buffer.writerIndex());
         msg.putIntProperty("key", i);
         // to be used on tests that are validating filters
         msg.putBooleanProperty("even", i % 2 == 0);

         msg.getBodyBuffer().writeBytes(buffer, 0, buffer.writerIndex());

         Assert.assertTrue(pageStore.page(msg, ctx, ctx.getContextListing(ADDRESS)));
      }

      return pageStore.getNumberOfPages();
   }

   /**
    * @return
    * @throws Exception
    */
   private PagingStoreImpl lookupPageStore(SimpleString address) throws Exception
   {
      return (PagingStoreImpl)server.getPagingManager().getPageStore(address);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void tearDown() throws Exception
   {
      queue = null;
      queueList = null;
      super.tearDown();
   }

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      OperationContextImpl.clearContext();
      queueList = new ArrayList<Queue>();

      createServer();
   }

   /**
    * @throws Exception
    */
   private void createServer() throws Exception
   {
      OperationContextImpl.clearContext();

      Configuration config = createDefaultConfig();

      config.setJournalSyncNonTransactional(true);

      server = createServer(true, config, PAGE_SIZE, PAGE_MAX, new HashMap<String, AddressSettings>());

      server.start();

      queueList.clear();

      try
      {
         queue = server.createQueue(ADDRESS, ADDRESS, null, true, false);
         queue.pause();
      }
      catch (Exception ignored)
      {
      }
   }

   /**
    * @return
    * @throws Exception
    */
   private PageSubscription createNonPersistentCursor(Filter filter) throws Exception
   {
      long id = server.getStorageManager().generateUniqueID();
      FakeQueue queue = new FakeQueue(new SimpleString(filter.toString()), id);
      queueList.add(queue);

      PageSubscription subs = lookupCursorProvider().createSubscription(id, filter, false);

      queue.setPageSubscription(subs);

      return subs;
   }

   /**
    * @return
    * @throws Exception
    */
   private PageCursorProvider lookupCursorProvider() throws Exception
   {
      return lookupPageStore(ADDRESS).getCursorProvider();
   }

   /**
    * @param storage
    * @param pageStore
    * @param pgParameter
    * @param start
    * @param NUM_MESSAGES
    * @param messageSize
    * @throws Exception
    */
   private Transaction pgMessages(StorageManager storage,
                           PagingStoreImpl pageStore,
                           long pgParameter,
                           int start,
                           final int NUM_MESSAGES,
                           final int messageSize) throws Exception
   {

      TransactionImpl txImpl = new TransactionImpl(pgParameter, null, storage);

      RoutingContext ctx = generateCTX(txImpl);

      for (int i = start; i < start + NUM_MESSAGES; i++)
      {
         HornetQBuffer buffer = RandomUtil.randomBuffer(messageSize, i + 1l);
         ServerMessage msg = new ServerMessageImpl(storage.generateUniqueID(), buffer.writerIndex());
         msg.getBodyBuffer().writeBytes(buffer, 0, buffer.writerIndex());
         msg.putIntProperty("key", i);
         pageStore.page(msg, ctx, ctx.getContextListing(ADDRESS));
      }

      return txImpl;

   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
