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

package org.hornetq.tests.integration.paging;
import org.junit.Before;

import org.junit.Test;

import javax.transaction.xa.Xid;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.paging.cursor.PageSubscription;
import org.hornetq.core.paging.cursor.PageSubscriptionCounter;
import org.hornetq.core.paging.cursor.impl.PageSubscriptionCounterImpl;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.persistence.impl.journal.OperationContextImpl;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.impl.TransactionImpl;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * A PagingCounterTest
 *
 * @author clebertsuconic
 *
 *
 */
public class PagingCounterTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private HornetQServer server;

   private ServerLocator sl;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testCounter() throws Exception
   {
      ClientSessionFactory sf = createSessionFactory(sl);
      ClientSession session = sf.createSession();

      try
      {
         Queue queue = server.createQueue(new SimpleString("A1"), new SimpleString("A1"), null, true, false);

         PageSubscriptionCounter counter = locateCounter(queue);

         StorageManager storage = server.getStorageManager();

         Transaction tx = new TransactionImpl(server.getStorageManager());

         counter.increment(tx, 1);

         assertEquals(0, counter.getValue());

         tx.commit();

         storage.waitOnOperations();

         assertEquals(1, counter.getValue());
      }
      finally
      {
         sf.close();
         session.close();
      }
   }

   @Test
   public void testCleanupCounter() throws Exception
   {
      ClientSessionFactory sf = createSessionFactory(sl);
      ClientSession session = sf.createSession();

      try
      {
         Queue queue = server.createQueue(new SimpleString("A1"), new SimpleString("A1"), null, true, false);

         PageSubscriptionCounter counter = locateCounter(queue);

         StorageManager storage = server.getStorageManager();

         Transaction tx = new TransactionImpl(server.getStorageManager());

         for (int i = 0 ; i < 2100; i++)
         {

            counter.increment(tx, 1);

            if (i % 200 == 0)
            {
               tx.commit();

               storage.waitOnOperations();

               assertEquals(i + 1, counter.getValue());

               tx = new TransactionImpl(server.getStorageManager());
            }
         }

         tx.commit();

         storage.waitOnOperations();

         assertEquals(2100, counter.getValue());

         server.stop();

         server = newHornetQServer();

         server.start();

         queue = server.locateQueue(new SimpleString("A1"));

         assertNotNull(queue);

         counter = locateCounter(queue);

         assertEquals(2100, counter.getValue());

      }
      finally
      {
         sf.close();
         session.close();
      }
   }


   @Test
   public void testCleanupCounterNonPersistent() throws Exception
   {
      ClientSessionFactory sf = createSessionFactory(sl);
      ClientSession session = sf.createSession();

      try
      {
         Queue queue = server.createQueue(new SimpleString("A1"), new SimpleString("A1"), null, true, false);

         PageSubscriptionCounter counter = locateCounter(queue);

         ((PageSubscriptionCounterImpl)counter).setPersistent(false);

         StorageManager storage = server.getStorageManager();

         Transaction tx = new TransactionImpl(server.getStorageManager());

         for (int i = 0 ; i < 2100; i++)
         {

            counter.increment(tx, 1);

            if (i % 200 == 0)
            {
               tx.commit();

               storage.waitOnOperations();

               assertEquals(i + 1, counter.getValue());

               tx = new TransactionImpl(server.getStorageManager());
            }
         }

         tx.commit();

         storage.waitOnOperations();

         assertEquals(2100, counter.getValue());

         server.stop();

         server = newHornetQServer();

         server.start();

         queue = server.locateQueue(new SimpleString("A1"));

         assertNotNull(queue);

         counter = locateCounter(queue);

         assertEquals(0, counter.getValue());

      }
      finally
      {
         sf.close();
         session.close();
      }
   }

   @Test
   public void testRestartCounter() throws Exception
   {
      Queue queue = server.createQueue(new SimpleString("A1"), new SimpleString("A1"), null, true, false);

      PageSubscriptionCounter counter = locateCounter(queue);

      StorageManager storage = server.getStorageManager();

      Transaction tx = new TransactionImpl(server.getStorageManager());

      counter.increment(tx, 1);

      assertEquals(0, counter.getValue());

      tx.commit();

      storage.waitOnOperations();

      assertEquals(1, counter.getValue());

      sl.close();

      server.stop();

      server = newHornetQServer();

      server.start();

      queue = server.locateQueue(new SimpleString("A1"));

      assertNotNull(queue);

      counter = locateCounter(queue);

      assertEquals(1, counter.getValue());

   }

   /**
    * @param queue
    * @return
    * @throws Exception
    */
   private PageSubscriptionCounter locateCounter(Queue queue) throws Exception
   {
      PageSubscription subscription = server.getPagingManager()
                                            .getPageStore(new SimpleString("A1"))
                                            .getCursorProvider()
                                            .getSubscription(queue.getID());

      PageSubscriptionCounter counter = subscription.getCounter();
      return counter;
   }

   @Test
   public void testPrepareCounter() throws Exception
   {
      Xid xid = newXID();

      Queue queue = server.createQueue(new SimpleString("A1"), new SimpleString("A1"), null, true, false);

      PageSubscriptionCounter counter = locateCounter(queue);

      StorageManager storage = server.getStorageManager();

      Transaction tx = new TransactionImpl(xid, server.getStorageManager(), 300);

      for (int i = 0 ; i < 2000; i++)
      {
         counter.increment(tx, 1);
      }

      assertEquals(0, counter.getValue());

      tx.prepare();

      storage.waitOnOperations();

      assertEquals(0, counter.getValue());

      server.stop();

      server = newHornetQServer();

      server.start();

      storage = server.getStorageManager();

      queue = server.locateQueue(new SimpleString("A1"));

      assertNotNull(queue);

      counter = locateCounter(queue);

      tx = server.getResourceManager().removeTransaction(xid);

      assertNotNull(tx);

      assertEquals(0, counter.getValue());

      tx.commit(false);

      storage.waitOnOperations();

      assertEquals(2000, counter.getValue());


   }


   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      server = newHornetQServer();
      server.start();
      sl = createInVMNonHALocator();
   }

   private HornetQServer newHornetQServer() throws Exception
   {

      OperationContextImpl.clearContext();

      HornetQServer server = super.createServer(true, false);

      AddressSettings defaultSetting = new AddressSettings();
      defaultSetting.setPageSizeBytes(10 * 1024);
      defaultSetting.setMaxSizeBytes(20 * 1024);

      server.getAddressSettingsRepository().addMatch("#", defaultSetting);

      return server;
   }

   // Inner classes -------------------------------------------------

}
