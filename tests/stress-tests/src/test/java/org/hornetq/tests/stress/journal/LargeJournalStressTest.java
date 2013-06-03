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

package org.hornetq.tests.stress.journal;
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;

import org.hornetq.api.config.HornetQDefaultConfiguration;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.JournalType;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.UnitTestCase;

/**
 * A LargeJournalStressTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class LargeJournalStressTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private static final String AD1 = "ad1";

   private static final String AD2 = "ad2";

   private static final String Q1 = "q1";

   private static final String Q2 = "q2";

   private HornetQServer server;

   private ClientSessionFactory sf;

   private ServerLocator locator;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testMultiProducerAndCompactAIO() throws Throwable
   {
      internalTestMultiProducer(JournalType.ASYNCIO);
   }

   @Test
   public void testMultiProducerAndCompactNIO() throws Throwable
   {
      internalTestMultiProducer(JournalType.NIO);
   }

   public void internalTestMultiProducer(final JournalType journalType) throws Throwable
   {

      setupServer(journalType);

      final AtomicInteger numberOfMessages = new AtomicInteger(0);
      final int SLOW_INTERVAL = 25000;
      final int NUMBER_OF_FAST_MESSAGES = SLOW_INTERVAL * 50;

      final CountDownLatch latchReady = new CountDownLatch(2);
      final CountDownLatch latchStart = new CountDownLatch(1);

      class FastProducer extends Thread
      {
         Throwable e;

         FastProducer()
         {
            super("Fast-Thread");
         }

         @Override
         public void run()
         {
            ClientSession session = null;
            ClientSession sessionSlow = null;
            latchReady.countDown();
            try
            {
               UnitTestCase.waitForLatch(latchStart);
               session = sf.createSession(true, true);
               sessionSlow = sf.createSession(false, false);
               ClientProducer prod = session.createProducer(LargeJournalStressTest.AD2);
               ClientProducer slowProd = sessionSlow.createProducer(LargeJournalStressTest.AD1);
               for (int i = 0; i < NUMBER_OF_FAST_MESSAGES; i++)
               {
                  if (i % SLOW_INTERVAL == 0)
                  {
                     System.out.println("Sending slow message, msgs = " + i +
                                        " slowMessages = " +
                                        numberOfMessages.get());

                     if (numberOfMessages.incrementAndGet() % 5 == 0)
                     {
                        sessionSlow.commit();
                     }
                     slowProd.send(session.createMessage(true));
                  }
                  ClientMessage msg = session.createMessage(true);
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

         @Override
         public void run()
         {
            ClientSession session = null;
            latchReady.countDown();
            try
            {
               UnitTestCase.waitForLatch(latchStart);
               session = sf.createSession(true, true);
               session.start();
               ClientConsumer cons = session.createConsumer(LargeJournalStressTest.Q2);
               for (int i = 0; i < NUMBER_OF_FAST_MESSAGES; i++)
               {
                  ClientMessage msg = cons.receive(60 * 1000);
                  msg.acknowledge();
               }

               Assert.assertNull(cons.receiveImmediate());
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

      UnitTestCase.waitForLatch(latchReady);
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

      ClientSession sess = sf.createSession(true, true);

      ClientConsumer cons = sess.createConsumer(LargeJournalStressTest.Q1);

      sess.start();

      for (int i = 0; i < numberOfMessages.intValue(); i++)
      {
         ClientMessage msg = cons.receive(10000);
         Assert.assertNotNull(msg);
         msg.acknowledge();
      }

      Assert.assertNull(cons.receiveImmediate());

      cons.close();

      cons = sess.createConsumer(LargeJournalStressTest.Q2);

      Assert.assertNull(cons.receiveImmediate());

      sess.close();

   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      clearData();

      locator = createInVMNonHALocator();

      locator.setBlockOnAcknowledge(false);
      locator.setBlockOnNonDurableSend(false);
      locator.setBlockOnDurableSend(false);

   }

   /**
    * @throws Exception
    * @throws HornetQException
    */
   private void setupServer(final JournalType journalType) throws Exception, HornetQException
   {
      Configuration config = createDefaultConfig();
      config.setJournalSyncNonTransactional(false);
      config.setJournalFileSize(HornetQDefaultConfiguration.getDefaultJournalFileSize());

      config.setJournalType(journalType);

      config.setJournalCompactMinFiles(0);
      config.setJournalCompactPercentage(50);

      server = createServer(true, config);

      server.start();

      sf = createSessionFactory(locator);

      ClientSession sess = sf.createSession();

      try
      {
         sess.createQueue(LargeJournalStressTest.AD1, LargeJournalStressTest.Q1, true);
      }
      catch (Exception ignored)
      {
      }

      try
      {
         sess.createQueue(LargeJournalStressTest.AD2, LargeJournalStressTest.Q2, true);
      }
      catch (Exception ignored)
      {
      }

      sess.close();

      sf = createSessionFactory(locator);
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      locator.close();

      if (sf != null)
      {
         sf.close();
      }

      if (server != null)
      {
         server.stop();
      }

      // We don't super.tearDown here because in case of failure, the data may be useful for debug
      // so, we only clear data on setup.
      // super.tearDown();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
