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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.messaging.core.client.*;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.utils.SimpleString;

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

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testProduceRate() throws Exception
   {
      MessagingService service = createService(false);

      try
      {
         service.start();

         ClientSessionFactory sf = createInVMFactory();
         sf.setProducerMaxRate(10);
         ClientSession session = sf.createSession(false, true, true);
         
         session.createQueue(ADDRESS, ADDRESS, true);

         ClientProducer producer = session.createProducer(ADDRESS);
         long start = System.currentTimeMillis();
         for (int i = 0; i < 10; i++)
         {
            producer.send(session.createClientMessage(false));
         }
         long end = System.currentTimeMillis();

         assertTrue("TotalTime = " + (end - start), end - start >= 1000);
         

      }
      finally
      {
         if (service.isStarted())
         {
            service.stop();
         }
      }

   }


   public void testConsumeRate() throws Exception
   {
      MessagingService service = createService(false);

      try
      {
         service.start();

         ClientSessionFactory sf = createInVMFactory();
         sf.setConsumerMaxRate(10);
         
         ClientSession session = sf.createSession(false, true, true);
         
         session.createQueue(ADDRESS, ADDRESS, true);


         ClientProducer producer = session.createProducer(ADDRESS);
         
         for (int i = 0; i < 12; i++)
         {
            producer.send(session.createClientMessage(false));
         }

         session.start();
         
         ClientConsumer consumer = session.createConsumer(ADDRESS);

         long start = System.currentTimeMillis();
         
         for (int i = 0; i < 12; i++)
         {
            consumer.receive(1000);
         }
         
         long end = System.currentTimeMillis();

         assertTrue("TotalTime = " + (end - start), end - start >= 1000);
         

      }
      finally
      {
         if (service.isStarted())
         {
            service.stop();
         }
      }

   }


   public void testConsumeRateListener() throws Exception
   {
      MessagingService service = createService(false);

      try
      {
         service.start();

         ClientSessionFactory sf = createInVMFactory();
         sf.setConsumerMaxRate(10);
         
         ClientSession session = sf.createSession(false, true, true);
         
         session.createQueue(ADDRESS, ADDRESS, true);


         ClientProducer producer = session.createProducer(ADDRESS);
         
         for (int i = 0; i < 12; i++)
         {
            producer.send(session.createClientMessage(false));
         }
         
         ClientConsumer consumer = session.createConsumer(ADDRESS);
         
         final AtomicInteger failures = new AtomicInteger(0);
         
         final CountDownLatch messages = new CountDownLatch(12);
         
         consumer.setMessageHandler(new MessageHandler()
         {

            public void onMessage(ClientMessage message)
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
         assertTrue(messages.await(5, TimeUnit.SECONDS));
         long end = System.currentTimeMillis();
         
         assertTrue("TotalTime = " + (end - start), end - start >= 1000);
         

      }
      finally
      {
         if (service.isStarted())
         {
            service.stop();
         }
      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
