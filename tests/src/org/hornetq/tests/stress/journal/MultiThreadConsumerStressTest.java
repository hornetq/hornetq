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

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

import org.hornetq.core.buffers.HornetQBuffers;
import org.hornetq.core.client.*;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.JournalType;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.SimpleString;

/**
 * A MultiThreadConsumerStressTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class MultiThreadConsumerStressTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   final SimpleString ADDRESS = new SimpleString("SomeAddress");

   final SimpleString QUEUE = new SimpleString("SomeQueue");

   private HornetQServer server;

   private ClientSessionFactory sf;

   protected void setUp() throws Exception
   {
      super.setUp();
      setupServer(JournalType.ASYNCIO);
   }
   
   public void testProduceAndConsume() throws Throwable
   {
      int numberOfConsumers = 60;
      // this test assumes numberOfConsumers == numberOfProducers
      int numberOfProducers = numberOfConsumers;
      int produceMessage = 10000;
      int commitIntervalProduce = 100;
      int consumeMessage = (int) (produceMessage * 0.9);
      int commitIntervalConsume = 100;
      
      // Number of messages expected to be received after restart
      int numberOfMessagesExpected = (produceMessage - consumeMessage) * numberOfConsumers;

      CountDownLatch latchReady = new CountDownLatch(numberOfConsumers + numberOfProducers);

      CountDownLatch latchStart = new CountDownLatch(1);
      
      ArrayList<BaseThread> threads = new ArrayList<BaseThread>();

      ProducerThread[] prod = new ProducerThread[numberOfProducers];
      for (int i = 0; i < numberOfProducers; i++)
      {
         prod[i] = new ProducerThread(i, latchReady, latchStart, produceMessage, commitIntervalProduce);
         prod[i].start();
         threads.add(prod[i]);
      }
      
      ConsumerThread[] cons = new ConsumerThread[numberOfConsumers];

      for (int i = 0; i < numberOfConsumers; i++)
      {
         cons[i] = new ConsumerThread(i, latchReady, latchStart, consumeMessage, commitIntervalConsume);
         cons[i].start();
         threads.add(cons[i]);
      }
      
      latchReady.await();
      latchStart.countDown();
      
      for (BaseThread t : threads)
      {
         t.join();
         if (t.e != null)
         {
            throw t.e;
         }
      }
      
      server.stop();
      
      setupServer(JournalType.ASYNCIO);
      
      ClientSession sess = sf.createSession(true, true);
      
      ClientConsumer consumer = sess.createConsumer(QUEUE);
      
      sess.start();
      
      
      for (int i = 0 ; i < numberOfMessagesExpected; i++)
      {
         ClientMessage msg = consumer.receive(5000);
         assertNotNull(msg);
         
         if (i % 100 == 0)
         {
            System.out.println("Received #" + i + "  on thread after start");
         }
         msg.acknowledge();
      }
      
      assertNull(consumer.receiveImmediate());
      
      sess.close();
      
      
   }

   protected void tearDown() throws Exception
   {
      try
      {
         if (server != null && server.isStarted())
         {
            server.stop();
         }
      }
      catch (Throwable e)
      {
         e.printStackTrace(System.out); // System.out => junit reports
      }

      server = null;
      sf = null;
   }

   private void setupServer(JournalType journalType) throws Exception, HornetQException
   {
      Configuration config = createDefaultConfig(true);
      config.setJournalFileSize(ConfigurationImpl.DEFAULT_JOURNAL_FILE_SIZE);

      config.setJournalType(journalType);
      config.setJMXManagementEnabled(true);

      config.setJournalFileSize(ConfigurationImpl.DEFAULT_JOURNAL_FILE_SIZE);
      config.setJournalMinFiles(ConfigurationImpl.DEFAULT_JOURNAL_MIN_FILES);

      config.setJournalCompactMinFiles(0);
      config.setJournalCompactPercentage(50);

      server = createServer(true, config);

      server.start();

      sf = createNettyFactory();

      ClientSession sess = sf.createSession();

      try
      {
         sess.createQueue(ADDRESS, QUEUE, true);
      }
      catch (Exception ignored)
      {
      }

      sess.close();

      sf = createInVMFactory();
   }

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   class BaseThread extends Thread
   {
      Throwable e;

      final CountDownLatch latchReady;

      final CountDownLatch latchStart;

      final int numberOfMessages;

      final int commitInterval;

      BaseThread(String name,
                CountDownLatch latchReady,
                CountDownLatch latchStart,
                int numberOfMessages,
                int commitInterval)
      {
         super(name);
         this.latchReady = latchReady;
         this.latchStart = latchStart;
         this.commitInterval = commitInterval;
         this.numberOfMessages = numberOfMessages;
      }

   }

   class ProducerThread extends BaseThread
   {
      ProducerThread(int id,
                     CountDownLatch latchReady,
                     CountDownLatch latchStart,
                     int numberOfMessages,
                     int commitInterval)
      {
         super("ClientProducer:" + id, latchReady, latchStart, numberOfMessages, commitInterval);
      }

      public void run()
      {
         ClientSession session = null;
         latchReady.countDown();
         try
         {
            latchStart.await();
            session = sf.createSession(false, false);
            ClientProducer prod = session.createProducer(ADDRESS);
            for (int i = 0; i < numberOfMessages; i++)
            {
               if (i % commitInterval == 0)
               {
                  session.commit();
               }
               if (i % 100 == 0)
               {
                  System.out.println(Thread.currentThread().getName() + "::received #" + i);
               }
               ClientMessage msg = session.createClientMessage(true);               
               prod.send(msg);
            }

            session.commit();
            
            System.out.println("Thread " + Thread.currentThread().getName() + " sent " + numberOfMessages + "  messages");
         }
         catch (Throwable e)
         {
            e.printStackTrace();
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
               e.printStackTrace();
            }
         }
      }
   }

   class ConsumerThread extends BaseThread
   {
      ConsumerThread(int id,
                     CountDownLatch latchReady,
                     CountDownLatch latchStart,
                     int numberOfMessages,
                     int commitInterval)
      {
         super("ClientConsumer:" + id, latchReady, latchStart, numberOfMessages, commitInterval);
      }

      public void run()
      {
         ClientSession session = null;
         latchReady.countDown();
         try
         {
            latchStart.await();
            session = sf.createSession(false, false);
            session.start();
            ClientConsumer cons = session.createConsumer(QUEUE);
            for (int i = 0; i < numberOfMessages; i++)
            {
               ClientMessage msg = cons.receive(60 * 1000);
               msg.acknowledge();
               if (i % commitInterval == 0)
               {
                  session.commit();
               }
               if (i % 100 == 0)
               {
                  System.out.println(Thread.currentThread().getName() + "::sent #" + i);
               }
            }
            
            System.out.println("Thread " + Thread.currentThread().getName() + " received " + numberOfMessages + " messages");
            
            session.commit();
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

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
