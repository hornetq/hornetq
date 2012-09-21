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

package org.hornetq.tests.integration.client;

import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * Multiple Threads producing Messages, with Multiple Consumers with different queues, each queue with a different filter
 * This is similar to MultipleThreadFilterTwoTest but it uses multiple queues
 *
 * @author clebertsuconic
 *
 *
 */
public class MultipleThreadFilterOneTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   final String ADDRESS = "ADDRESS";
   final int numberOfMessages = 1000;
   final int nThreads = 4;

   private boolean isNetty=false;

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------


   class SomeProducer extends Thread
   {
      final ClientSessionFactory factory;
      final ServerLocator locator;
      final ClientSession prodSession;
      public final AtomicInteger errors = new AtomicInteger(0);

      public SomeProducer() throws Exception
      {
         locator = createNonHALocator(isNetty);
         factory = locator.createSessionFactory();
         prodSession = factory.createSession(false, false);
      }

      public void run()
      {
         try
         {
            ClientProducer producer = prodSession.createProducer(ADDRESS);

            for (int i = 0 ; i < numberOfMessages; i++)
            {
               ClientMessage message = prodSession.createMessage(true);
               message.putIntProperty("prodNR", i % nThreads);
               producer.send(message);

               if (i % 100 == 0)
               {
                  prodSession.commit();
               }
            }
            prodSession.commit();
         }
         catch (Throwable e)
         {
            e.printStackTrace();
            errors.incrementAndGet();
         }
         finally
         {
            try
            {
               prodSession.close();
               locator.close();
            }
            catch (Throwable ignored)
            {
               ignored.printStackTrace();
            }

         }
      }
   }

   class SomeConsumer extends Thread
   {
      final ClientSessionFactory factory;
      final ServerLocator locator;
      final ClientSession consumerSession;
      ClientConsumer consumer;
      final int nr;
      final AtomicInteger errors = new AtomicInteger(0);

      public SomeConsumer(int nr) throws Exception
      {
         locator = createNonHALocator(isNetty);
         factory = locator.createSessionFactory();
         consumerSession = factory.createSession(false, false);
         consumerSession.createQueue(ADDRESS, "Q" + nr, "prodNR=" + nr, true);
         consumer = consumerSession.createConsumer("Q" + nr);
         consumerSession.start();
         this.nr = nr;
      }

      public void run()
      {
         try
         {
            consumerSession.start();

            for (int i = 0 ; i < numberOfMessages; i++)
            {
               ClientMessage msg = consumer.receive(5000);
               assertNotNull(msg);
               assertEquals(nr, msg.getIntProperty("prodNR").intValue());
               msg.acknowledge();

               if (i % 100 == 0)
               {
                  consumerSession.commit();
               }
            }

            assertNull(consumer.receiveImmediate());

            consumerSession.commit();
         }
         catch (Throwable e)
         {
            e.printStackTrace();
            errors.incrementAndGet();
         }
         finally
         {
            try
            {
               consumerSession.close();
               locator.close();
            }
            catch (Throwable ignored)
            {
               ignored.printStackTrace();
            }

         }
      }
   }


   public void testSendingNetty() throws Exception
   {
      testSending(true);
   }

   public void testSendingInVM() throws Exception
   {
      testSending(false);
   }


   private void testSending(boolean isNetty) throws Exception
   {
      this.isNetty = isNetty;
      HornetQServer server = createServer(true, isNetty);
      server.start();

      SomeConsumer[] consumers = new SomeConsumer[nThreads];
      SomeProducer[] producers = new SomeProducer[nThreads];
      try
      {

         for (int i = 0 ; i < nThreads; i++)
         {
            consumers[i] = new SomeConsumer(i);
            producers[i] = new SomeProducer();
         }

         for (int i = 0 ; i < nThreads; i++)
         {
            consumers[i].start();
            producers[i].start();
         }

         for (SomeProducer producer : producers)
         {
            producer.join();
            assertEquals(0, producer.errors.get());
         }


         for (SomeConsumer consumer : consumers)
         {
            consumer.join();
            assertEquals(0, consumer.errors.get());
         }

      }
      finally
      {
         server.stop();
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
