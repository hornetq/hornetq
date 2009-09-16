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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.core.buffers.ChannelBuffers;
import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.message.Message;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.JournalType;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * A CompactingTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class CompactingTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private static final String AD1 = "ad1";

   private static final String AD2 = "ad2";

   private static final String AD3 = "ad3";

   private static final String Q1 = "q1";

   private static final String Q2 = "q2";

   private static final String Q3 = "q3";

   private static final int TOT_AD3 = 5000;

   private HornetQServer server;

   private ClientSessionFactory sf;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testCleanupAIO() throws Throwable
   {
      for (int i = 0; i < 3; i++)
      {
         System.out.println("Test # " + i);
         internalTestCleanup(JournalType.ASYNCIO);
         tearDown();
         setUp();
      }
   }

   public void testCleanupNIO() throws Throwable
   {
      for (int i = 0; i < 3; i++)
      {
         System.out.println("Test # " + i);
         internalTestCleanup(JournalType.NIO);
         tearDown();
         setUp();
      }
   }

   private void internalTestCleanup(JournalType journalType) throws Throwable
   {
      setupServer(journalType);

      ClientSession session = sf.createSession(false, true, true);

      ClientProducer prod = session.createProducer(AD1);

      for (int i = 0; i < 500; i++)
      {
         prod.send(session.createClientMessage(true));
      }

      session.commit();

      prod.close();

      ClientConsumer cons = session.createConsumer(Q2);
      prod = session.createProducer(AD2);

      session.start();

      for (int i = 0; i < 200; i++)
      {
         System.out.println("Iteration " + i);
         for (int j = 0; j < 1000; j++)
         {
            Message msg = session.createClientMessage(true);
            msg.getBody().writeBytes(new byte[1024]);

            prod.send(msg);
         }

         session.commit();

         for (int j = 0; j < 1000; j++)
         {
            ClientMessage msg = cons.receive(2000);
            assertNotNull(msg);
            msg.acknowledge();
         }

         session.commit();

      }

      assertNull(cons.receiveImmediate());

      session.close();

      server.stop();

      server.start();

      session = sf.createSession(false, true, true);
      cons = session.createConsumer(Q1);
      session.start();

      for (int i = 0; i < 500; i++)
      {
         ClientMessage msg = cons.receive(1000);
         assertNotNull(msg);
         msg.acknowledge();
      }

      assertNull(cons.receiveImmediate());

      prod = session.createProducer(AD2);

      session.close();

   }

   public void testMultiProducerAndCompactAIO() throws Throwable
   {
      internalTestMultiProducer(JournalType.ASYNCIO);
   }

   public void testMultiProducerAndCompactNIO() throws Throwable
   {
      internalTestMultiProducer(JournalType.NIO);
   }

   public void internalTestMultiProducer(JournalType journalType) throws Throwable
   {

      setupServer(journalType);

      ClientSession session = sf.createSession(false, false);

      try
      {
         ClientProducer producer = session.createProducer(AD3);

         byte[] buffer = new byte[10 * 1024];

         ClientMessage msg = session.createClientMessage(true);
         msg.setBody(ChannelBuffers.wrappedBuffer(buffer));
         for (int i = 0; i < TOT_AD3; i++)
         {
            producer.send(msg);
            if (i % 100 == 0)
            {
               session.commit();
            }
         }

         session.commit();
      }
      finally
      {
         session.close();
      }

      server.stop();

      setupServer(journalType);

      final AtomicInteger numberOfMessages = new AtomicInteger(0);
      final int NUMBER_OF_FAST_MESSAGES = 100000;
      final int SLOW_INTERVAL = 100;

      final CountDownLatch latchReady = new CountDownLatch(2);
      final CountDownLatch latchStart = new CountDownLatch(1);

      class FastProducer extends Thread
      {
         Throwable e;

         FastProducer()
         {
            super("Fast-Thread");
         }

         public void run()
         {
            ClientSession session = null;
            ClientSession sessionSlow = null;
            latchReady.countDown();
            try
            {
               latchStart.await();
               session = sf.createSession(true, true);
               sessionSlow = sf.createSession(false, false);
               ClientProducer prod = session.createProducer(AD2);
               ClientProducer slowProd = sessionSlow.createProducer(AD1);
               for (int i = 0; i < NUMBER_OF_FAST_MESSAGES; i++)
               {
                  if (i % SLOW_INTERVAL == 0)
                  {
                     if (numberOfMessages.incrementAndGet() % 5 == 0)
                     {
                        sessionSlow.commit();
                     }
                     slowProd.send(session.createClientMessage(true));
                  }
                  ClientMessage msg = session.createClientMessage(true);
                  msg.setBody(ChannelBuffers.wrappedBuffer(new byte[1024]));
                  prod.send(msg);
               }
               sessionSlow.commit();
            }
            catch (Throwable e)
            {
               this.e = e;
            }
            finally
            {
               try
               {
                  session.close();
               }
               catch (Throwable e)
               {
                  this.e = e;
               }
               try
               {
                  sessionSlow.close();
               }
               catch (Throwable e)
               {
                  this.e = e;
               }
            }
         }
      }

      class FastConsumer extends Thread
      {
         Throwable e;

         FastConsumer()
         {
            super("Fast-Consumer");
         }

         public void run()
         {
            ClientSession session = null;
            latchReady.countDown();
            try
            {
               latchStart.await();
               session = sf.createSession(true, true);
               session.start();
               ClientConsumer cons = session.createConsumer(Q2);
               for (int i = 0; i < NUMBER_OF_FAST_MESSAGES; i++)
               {
                  ClientMessage msg = cons.receive(60 * 1000);
                  msg.acknowledge();
               }

               assertNull(cons.receiveImmediate());
            }
            catch (Throwable e)
            {
               this.e = e;
            }
            finally
            {
               try
               {
                  session.close();
               }
               catch (Throwable e)
               {
                  this.e = e;
               }
            }
         }
      }

      FastConsumer f1 = new FastConsumer();
      f1.start();

      FastProducer p1 = new FastProducer();
      p1.start();

      latchReady.await();
      latchStart.countDown();

      p1.join();

      if (p1.e != null)
      {
         throw p1.e;
      }

      f1.join();

      if (f1.e != null)
      {
         throw f1.e;
      }

      sf.close();

      server.stop();

      setupServer(journalType);

      ClientSession sess = null;

      try
      {

         sess = sf.createSession(true, true);

         ClientConsumer cons = sess.createConsumer(Q1);

         sess.start();

         for (int i = 0; i < numberOfMessages.intValue(); i++)
         {
            ClientMessage msg = cons.receive(60000);
            assertNotNull(msg);
            msg.acknowledge();
         }

         assertNull(cons.receiveImmediate());

         cons.close();

         cons = sess.createConsumer(Q2);

         assertNull(cons.receive(100));

         cons.close();

         cons = sess.createConsumer(Q3);

         for (int i = 0; i < TOT_AD3; i++)
         {
            ClientMessage msg = cons.receive(60000);
            assertNotNull(msg);
            msg.acknowledge();
         }

         assertNull(cons.receiveImmediate());

      }
      finally
      {
         try
         {
            sess.close();
         }
         catch (Throwable ignored)
         {
         }
      }
   }

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      clearData();
   }

   /**
    * @throws Exception
    * @throws HornetQException
    */
   private void setupServer(JournalType journalType) throws Exception, HornetQException
   {
      Configuration config = createDefaultConfig();
      config.setJournalFileSize(ConfigurationImpl.DEFAULT_JOURNAL_FILE_SIZE);

      config.setJournalType(journalType);

      config.setJournalCompactMinFiles(10);
      config.setJournalCompactPercentage(50);

      server = createServer(true, config);

      server.start();

      sf = createInVMFactory();

      ClientSession sess = sf.createSession();

      try
      {
         sess.createQueue(AD1, Q1, true);
      }
      catch (Exception ignored)
      {
      }

      try
      {
         sess.createQueue(AD2, Q2, true);
      }
      catch (Exception ignored)
      {
      }

      try
      {
         sess.createQueue(AD3, Q3, true);
      }
      catch (Exception ignored)
      {
      }

      sess.close();
   }

   @Override
   protected void tearDown() throws Exception
   {
      try
      {
         if (sf != null)
         {
            sf.close();
         }

         if (server != null)
         {
            server.stop();
         }
      }
      catch (Exception e)
      {
         e.printStackTrace(); // system.out -> junit reports
      }

      server = null;

      sf = null;

      super.tearDown();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
