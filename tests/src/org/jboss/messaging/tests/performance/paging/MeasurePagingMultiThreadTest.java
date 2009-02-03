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

package org.jboss.messaging.tests.performance.paging;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.impl.ByteBufferWrapper;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.util.SimpleString;

/**
 * A MeasurePagingMultiThreadTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Dec 1, 2008 1:02:39 PM
 *
 *
 */
public class MeasurePagingMultiThreadTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      clearData();
   }

   public void testPagingMultipleSenders() throws Throwable
   {

      final int NUMBER_OF_THREADS = 18;
      final int NUMBER_OF_MESSAGES = 50000;
      final int SIZE_OF_MESSAGE = 1024;

      Configuration config = createDefaultConfig();

      HashMap<String, AddressSettings> settings = new HashMap<String, AddressSettings>();

      config.setPagingMaxGlobalSizeBytes(20 * 1024);

      MessagingService messagingService = createService(true, config, settings);
      messagingService.start();
      try
      {

         final ClientSessionFactory factory = createInVMFactory();
         final SimpleString adr = new SimpleString("test-adr");

         createDestination(factory, adr);

         // Send some messages to make sure the destination is in page mode before we measure
         // And that will also help with VM optimizations
         sendInitialBatch(adr, NUMBER_OF_MESSAGES, SIZE_OF_MESSAGE, factory);

         final CountDownLatch latchAlign = new CountDownLatch(NUMBER_OF_THREADS);

         final CountDownLatch latchStart = new CountDownLatch(1);

         class Sender extends Thread
         {

            private final ClientSession session;

            private final ClientProducer producer;

            private final ClientMessage msg;

            Throwable e;

            public Sender() throws Exception
            {
               session = factory.createSession(false, true, true);
               producer = session.createProducer(adr);
               msg = session.createClientMessage(true);

               ByteBuffer buffer = ByteBuffer.allocate(SIZE_OF_MESSAGE);
               MessagingBuffer msgBuffer = new ByteBufferWrapper(buffer);
               msg.setBody(msgBuffer);

            }

            // run is not going to close sessions or anything, as we don't want to measure that time
            // so this will be done in a second time
            public void cleanUp() throws Exception
            {
               session.close();
            }

            @Override
            public void run()
            {
               try
               {
                  latchAlign.countDown();
                  latchStart.await();

                  long start = System.currentTimeMillis();
                  sendMessages(NUMBER_OF_MESSAGES, producer, msg);
                  long end = System.currentTimeMillis();

                  System.out.println("Thread " + Thread.currentThread().getName() +
                                     " finished sending in " +
                                     (end - start) +
                                     " milliseconds");
               }
               catch (Throwable e)
               {
                  this.e = e;
               }

            }
         }

         Sender senders[] = new Sender[NUMBER_OF_THREADS];

         for (int i = 0; i < NUMBER_OF_THREADS; i++)
         {
            senders[i] = new Sender();
            senders[i].start();
         }

         latchAlign.await();

         long timeStart = System.currentTimeMillis();

         latchStart.countDown();

         for (Thread t : senders)
         {
            t.join();
         }

         long timeEnd = System.currentTimeMillis();

         System.out.println("Total Time: " + (timeEnd - timeStart) +
                            " milliseconds what represented " +
                            (NUMBER_OF_MESSAGES * NUMBER_OF_THREADS * 1000 / (timeEnd - timeStart)) +
                            " per second");

         for (Sender s : senders)
         {
            if (s.e != null)
            {
               throw s.e;
            }
            s.cleanUp();
         }

      }
      finally
      {
         messagingService.stop();

      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   /**
    * @param adr
    * @param nMessages
    * @param messageSize
    * @param factory
    * @throws MessagingException
    */
   private void sendInitialBatch(final SimpleString adr,
                                 final int nMessages,
                                 final int messageSize,
                                 final ClientSessionFactory factory) throws MessagingException
   {
      ClientSession session = factory.createSession(false, true, true);
      ClientProducer producer = session.createProducer(adr);
      ClientMessage msg = session.createClientMessage(true);

      ByteBuffer buffer = ByteBuffer.allocate(messageSize);
      MessagingBuffer msgBuffer = new ByteBufferWrapper(buffer);
      msg.setBody(msgBuffer);

      sendMessages(nMessages, producer, msg);
   }

   /**
    * @param nMessages
    * @param producer
    * @param msg
    * @throws MessagingException
    */
   private void sendMessages(final int nMessages, final ClientProducer producer, final ClientMessage msg) throws MessagingException
   {
      for (int i = 0; i < nMessages; i++)
      {
         producer.send(msg);
      }
   }

   /**
    * @param factory
    * @param adr
    * @throws MessagingException
    */
   private void createDestination(final ClientSessionFactory factory, final SimpleString adr) throws MessagingException
   {
      {
         ClientSession session = factory.createSession(false, false, false);
         session.createQueue(adr, adr, null, true, false);
         session.close();
      }
   }

   // Inner classes -------------------------------------------------

}
