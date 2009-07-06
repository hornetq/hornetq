/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.integration.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.messaging.core.buffers.ChannelBuffers;
import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.server.JournalType;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.tests.util.ServiceTestBase;

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

   private static final String Q1 = "q1";

   private static final String Q2 = "q2";

   private MessagingServer server;

   private ClientSessionFactory sf;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testMultiProducerAndCompactNIO() throws Throwable
   {
      internalTestMultiProducer(JournalType.NIO);
   }

   public void testMultiProducerAndCompactAIO() throws Throwable
   {
      internalTestMultiProducer(JournalType.ASYNCIO);
   }

   public void internalTestMultiProducer(JournalType journalType) throws Throwable
   {

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

      ClientSession sess = sf.createSession(true, true);

      ClientConsumer cons = sess.createConsumer(Q1);

      sess.start();

      for (int i = 0; i < numberOfMessages.intValue(); i++)
      {
         ClientMessage msg = cons.receive(10000);
         assertNotNull(msg);
         msg.acknowledge();
      }

      assertNull(cons.receiveImmediate());

      cons.close();

      cons = sess.createConsumer(Q2);

      assertNull(cons.receive(100));

      sess.close();

   }

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      clearData();
   }

   /**
    * @throws Exception
    * @throws MessagingException
    */
   private void setupServer(JournalType journalType) throws Exception, MessagingException
   {
      Configuration config = createDefaultConfig();
      config.setJournalFileSize(ConfigurationImpl.DEFAULT_JOURNAL_FILE_SIZE);

      config.setJournalType(journalType);

      config.setJournalCompactMinFiles(ConfigurationImpl.DEFAULT_JOURNAL_COMPACT_MIN_FILES);
      config.setJournalCompactPercentage(ConfigurationImpl.DEFAULT_JOURNAL_COMPACT_PERCENTAGE);

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

      sess.close();

      sf = createInVMFactory();
   }

   @Override
   protected void tearDown() throws Exception
   {
      sf.close();

      server.stop();

      // super.tearDown();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
