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
import org.junit.Before;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.MessageHandler;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * A MessageRateTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class MessageRateTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   private final SimpleString ADDRESS = new SimpleString("ADDRESS");

   private ServerLocator locator;

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testProduceRate() throws Exception
   {
      HornetQServer server = createServer(false);

         server.start();

         locator.setProducerMaxRate(10);
         ClientSessionFactory sf = createSessionFactory(locator);
         ClientSession session = sf.createSession(false, true, true);

         session.createQueue(ADDRESS, ADDRESS, true);

         ClientProducer producer = session.createProducer(ADDRESS);
         long start = System.currentTimeMillis();
         for (int i = 0; i < 10; i++)
         {
            producer.send(session.createMessage(false));
         }
         long end = System.currentTimeMillis();

         Assert.assertTrue("TotalTime = " + (end - start), end - start >= 1000);

         session.close();
   }

   @Test
   public void testConsumeRate() throws Exception
   {
      HornetQServer server = createServer(false);

      server.start();

         locator.setConsumerMaxRate(10);
         ClientSessionFactory sf = createSessionFactory(locator);

         ClientSession session = sf.createSession(false, true, true);

         session.createQueue(ADDRESS, ADDRESS, true);

         ClientProducer producer = session.createProducer(ADDRESS);

         for (int i = 0; i < 12; i++)
         {
            producer.send(session.createMessage(false));
         }

         session.start();

         ClientConsumer consumer = session.createConsumer(ADDRESS);

         long start = System.currentTimeMillis();

         for (int i = 0; i < 12; i++)
         {
            consumer.receive(1000);
         }

         long end = System.currentTimeMillis();

         Assert.assertTrue("TotalTime = " + (end - start), end - start >= 1000);

         session.close();
   }

   @Test
   public void testConsumeRateListener() throws Exception
   {
      HornetQServer server = createServer(false);

      server.start();

         locator.setConsumerMaxRate(10);
         ClientSessionFactory sf = createSessionFactory(locator);

         ClientSession session = sf.createSession(false, true, true);

         session.createQueue(ADDRESS, ADDRESS, true);

         ClientProducer producer = session.createProducer(ADDRESS);

         for (int i = 0; i < 12; i++)
         {
            producer.send(session.createMessage(false));
         }

         ClientConsumer consumer = session.createConsumer(ADDRESS);

         final AtomicInteger failures = new AtomicInteger(0);

         final CountDownLatch messages = new CountDownLatch(12);

         consumer.setMessageHandler(new MessageHandler()
         {

            public void onMessage(final ClientMessage message)
            {
               try
               {
                  message.acknowledge();
                  messages.countDown();
               }
               catch (Exception e)
               {
                  e.printStackTrace(); // Hudson report
                  failures.incrementAndGet();
               }
            }

         });

         long start = System.currentTimeMillis();
         session.start();
         Assert.assertTrue(messages.await(5, TimeUnit.SECONDS));
         long end = System.currentTimeMillis();

         Assert.assertTrue("TotalTime = " + (end - start), end - start >= 1000);

         session.close();
         }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      locator = createInVMNonHALocator();
   }

}
