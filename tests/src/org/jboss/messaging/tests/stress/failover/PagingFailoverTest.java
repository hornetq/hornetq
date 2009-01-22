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

package org.jboss.messaging.tests.stress.failover;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.paging.PagingManager;
import org.jboss.messaging.core.paging.PagingStore;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.ByteBufferWrapper;
import org.jboss.messaging.core.remoting.impl.RemotingConnectionImpl;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnector;
import org.jboss.messaging.tests.integration.cluster.failover.FailoverTestBase;
import org.jboss.messaging.util.SimpleString;

/**
 * A PagingFailoverTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Dec 8, 2008 10:53:16 AM
 *
 *
 */
public class PagingFailoverTest extends FailoverTestBase
{

   // Constants -----------------------------------------------------
   private static final Logger log = Logger.getLogger(PagingFailoverTest.class);

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   private static final SimpleString ADDRESS = new SimpleString("FailoverTestAddress");

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testFailoverOnPaging() throws Exception
   {
      testPaging(true);
   }

   public void testReplicationOnPaging() throws Exception
   {
      testPaging(false);
   }

   private void testPaging(final boolean fail) throws Exception
   {
      setUpFileBased(100 * 1024);

      ClientSession session = null;
      try
      {
         ClientSessionFactory sf1 = createFailoverFactory();

         session = sf1.createSession(null, null, false, true, true, false, 0);

         session.createQueue(ADDRESS, ADDRESS, null, true, false);

         ClientProducer producer = session.createProducer(ADDRESS);

         final int numMessages = 50000;

         PagingManager pmLive = liveService.getServer().getPostOffice().getPagingManager();
         PagingStore storeLive = pmLive.getPageStore(ADDRESS);

         PagingManager pmBackup = backupService.getServer().getPostOffice().getPagingManager();
         PagingStore storeBackup = pmBackup.getPageStore(ADDRESS);

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = session.createClientMessage(true);
            ByteBuffer buffer = ByteBuffer.allocate(1000);

            buffer.putInt(i);

            buffer.rewind();

            message.setBody(new ByteBufferWrapper(buffer));

            producer.send(message);

            if (storeLive.isPaging())
            {
               assertTrue(storeBackup.isPaging());
            }
         }

         session.close();
         session = sf1.createSession(null, null, false, true, true, false, 0);
         session.start();

         final RemotingConnection conn = ((ClientSessionImpl)session).getConnection();

         assertEquals("GloblSize", pmLive.getGlobalSize(), pmBackup.getGlobalSize());

         assertEquals("PageSizeLive", storeLive.getAddressSize(), pmLive.getGlobalSize());

         assertEquals("PageSizeBackup", storeBackup.getAddressSize(), pmBackup.getGlobalSize());

         ClientConsumer consumer = session.createConsumer(ADDRESS);

         for (int i = 0; i < numMessages; i++)
         {

            if (fail && i == numMessages / 2)
            {
               conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));
            }

            ClientMessage message = consumer.receive(10000);


            assertNotNull(message);

            message.acknowledge();

            message.getBody().rewind();

            assertEquals(i, message.getBody().getInt());

         }

         session.close();
         session = null;

         if (!fail)
         {
            assertEquals(0, pmLive.getGlobalSize());
            assertEquals(0, storeLive.getAddressSize());
         }
         assertEquals(0, pmBackup.getGlobalSize());
         assertEquals(0, storeBackup.getAddressSize());

      }
      finally
      {
         if (session != null)
         {
            try
            {
               session.close();
            }
            catch (Exception ignored)
            {
               // eat it
            }
         }
      }

   }

   public void testMultithreadFailoverReplicationOnly() throws Throwable
   {
      setUpFileBased(100 * 1024, 20 * 1024);

      int numberOfProducedMessages = multiThreadProducer(false);

      System.out.println(numberOfProducedMessages + " messages produced");

      int numberOfConsumedMessages = multiThreadConsumer(false, false);

      assertEquals(numberOfProducedMessages, numberOfConsumedMessages);

   }

   public void testMultithreadFailoverOnProducing() throws Throwable
   {
      setUpFileBased(100 * 1024, 20 * 1024);

      int numberOfProducedMessages = multiThreadProducer(true);

      System.out.println(numberOfProducedMessages + " messages produced");

      int numberOfConsumedMessages = multiThreadConsumer(true, false);

      assertEquals(numberOfProducedMessages, numberOfConsumedMessages);

   }

   public void testMultithreadFailoverOnConsume() throws Throwable
   {
      setUpFileBased(100 * 1024, 20 * 1024);

      int numberOfProducedMessages = multiThreadProducer(false);

      System.out.println(numberOfProducedMessages + " messages produced");

      int numberOfConsumedMessages = multiThreadConsumer(false, true);

      assertEquals(numberOfProducedMessages, numberOfConsumedMessages);

   }

   /**
    * @throws Exception
    * @throws InterruptedException
    * @throws Throwable
    */
   private int multiThreadConsumer(final boolean connectedOnBackup, final boolean fail) throws Exception,
                                                                                       InterruptedException,
                                                                                       Throwable
   {
      ClientSession session = null;
      try
      {
         final AtomicInteger numberOfMessages = new AtomicInteger(0);

         final int RECEIVE_TIMEOUT = 10000;

         final ClientSessionFactory factory;
         final PagingStore store;

         if (connectedOnBackup)
         {
            factory = createBackupFactory();
            store = backupService.getServer().getPostOffice().getPagingManager().getPageStore(ADDRESS);
         }
         else
         {
            factory = createFailoverFactory();
            store = liveService.getServer().getPostOffice().getPagingManager().getPageStore(ADDRESS);
         }

         session = factory.createSession(false, true, true, false);

         final int initialNumberOfPages = store.getNumberOfPages();

         System.out.println("It has initially " + initialNumberOfPages);

         final int THREAD_CONSUMERS = 20;

         final CountDownLatch startFlag = new CountDownLatch(1);
         final CountDownLatch alignSemaphore = new CountDownLatch(THREAD_CONSUMERS);

         class Consumer extends Thread
         {
            volatile Throwable e;

            ClientSession session;

            public Consumer() throws Exception
            {
               session = factory.createSession(null, null, false, true, true, false, 0);
            }

            @Override
            public void run()
            {
               boolean started = false;

               try
               {

                  try
                  {
                     ClientConsumer consumer = session.createConsumer(ADDRESS);

                     session.start();

                     alignSemaphore.countDown();

                     started = true;

                     startFlag.await();

                     while (true)
                     {
                        ClientMessage msg = consumer.receive(RECEIVE_TIMEOUT);
                        if (msg == null)
                        {
                           break;
                        }

                        if (numberOfMessages.incrementAndGet() % 1000 == 0)
                        {
                           System.out.println(numberOfMessages + " messages read");
                        }

                        msg.acknowledge();
                     }

                  }
                  finally
                  {
                     session.close();
                  }
               }
               catch (Throwable e)
               {
                  log.error(e.getMessage(), e);
                  if (!started)
                  {
                     alignSemaphore.countDown();
                  }
                  this.e = e;
               }
            }
         }

         Consumer[] consumers = new Consumer[THREAD_CONSUMERS];

         for (int i = 0; i < THREAD_CONSUMERS; i++)
         {
            consumers[i] = new Consumer();
         }

         for (int i = 0; i < THREAD_CONSUMERS; i++)
         {
            consumers[i].start();
         }

         alignSemaphore.await();

         startFlag.countDown();

         if (fail)
         {
            Thread.sleep(1000);
            while (store.getNumberOfPages() == initialNumberOfPages)
            {
               Thread.sleep(100);
            }

            System.out.println("The system has already depaged " + (initialNumberOfPages - store.getNumberOfPages()) +
                               ", failing now");

            fail(session);
         }

         for (Thread t : consumers)
         {
            t.join();
         }

         for (Consumer p : consumers)
         {
            if (p.e != null)
            {
               throw p.e;
            }
         }

         return numberOfMessages.intValue();
      }
      finally
      {
         if (session != null)
         {
            try
            {
               session.close();
            }
            catch (Exception ignored)
            {
            }
         }
      }
   }

   /**
    * @throws Exception
    * @throws MessagingException
    * @throws InterruptedException
    * @throws Throwable
    */
   private int multiThreadProducer(final boolean failover) throws Exception,
                                                          MessagingException,
                                                          InterruptedException,
                                                          Throwable
   {

      final AtomicInteger numberOfMessages = new AtomicInteger(0);
      final PagingStore store = liveService.getServer().getPostOffice().getPagingManager().getPageStore(ADDRESS);

      final ClientSessionFactory factory = createFailoverFactory();

      ClientSession session = factory.createSession(false, true, true, false);
      try
      {
         session.createQueue(ADDRESS, ADDRESS, null, true, false);

         final int THREAD_PRODUCERS = 30;

         final CountDownLatch startFlag = new CountDownLatch(1);
         final CountDownLatch alignSemaphore = new CountDownLatch(THREAD_PRODUCERS);
         final CountDownLatch flagPaging = new CountDownLatch(THREAD_PRODUCERS);

         class Producer extends Thread
         {
            volatile Throwable e;

            @Override
            public void run()
            {
               boolean started = false;
               try
               {
                  ClientSession session = factory.createSession(false, true, true);
                  try
                  {
                     ClientProducer producer = session.createProducer(ADDRESS);

                     alignSemaphore.countDown();

                     started = true;
                     startFlag.await();

                     while (!store.isPaging())
                     {

                        ClientMessage msg = session.createClientMessage(true);

                        producer.send(msg);
                        numberOfMessages.incrementAndGet();
                     }

                     flagPaging.countDown();

                     for (int i = 0; i < 1000; i++)
                     {

                        ClientMessage msg = session.createClientMessage(true);

                        producer.send(msg);
                        numberOfMessages.incrementAndGet();

                     }

                  }
                  finally
                  {
                     session.close();
                  }
               }
               catch (Throwable e)
               {
                  log.error(e.getMessage(), e);
                  if (!started)
                  {
                     alignSemaphore.countDown();
                  }
                  flagPaging.countDown();
                  this.e = e;
               }
            }
         }

         Producer[] producers = new Producer[THREAD_PRODUCERS];

         for (int i = 0; i < THREAD_PRODUCERS; i++)
         {
            producers[i] = new Producer();
            producers[i].start();
         }

         alignSemaphore.await();

         // Start producing only when all the sessions are opened
         startFlag.countDown();

         if (failover)
         {
            flagPaging.await(); // for this test I want everybody on the paging part

            Thread.sleep(1500);

            fail(session);

         }

         for (Thread t : producers)
         {
            t.join();
         }

         for (Producer p : producers)
         {
            if (p.e != null)
            {
               throw p.e;
            }
         }

         return numberOfMessages.intValue();

      }
      finally
      {
         session.close();
         InVMConnector.resetFailures();
      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   protected void fail(final ClientSession session) throws Exception
   {
      RemotingConnectionImpl conn = (RemotingConnectionImpl)((ClientSessionImpl)session).getConnection();

      InVMConnector.numberOfFailures = 1;
      InVMConnector.failOnCreateConnection = true;
      System.out.println("Forcing a failure");
      conn.fail(new MessagingException(MessagingException.NOT_CONNECTED, "blah"));

   }

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
