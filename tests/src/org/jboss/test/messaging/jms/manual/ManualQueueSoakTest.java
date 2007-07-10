/*
   * JBoss, Home of Professional Open Source
   * Copyright 2005, JBoss Inc., and individual contributors as indicated
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

package org.jboss.test.messaging.jms.manual;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.logging.Logger;
import java.util.Properties;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.jms.ConnectionFactory;
import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.Queue;
import javax.jms.MessageProducer;
import javax.jms.MessageConsumer;
import javax.jms.Message;
import EDU.oswego.cs.dl.util.concurrent.SynchronizedInt;

/**
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *          $Id$
 */
public class ManualQueueSoakTest extends MessagingTestCase
{

   // Constants ------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------


   // Static ---------------------------------------------------------------------------------------

   static long PRODUCER_ALIVE_FOR=10000; // 1 minutes
   static long CONSUMER_ALIVE_FOR=10000; // 1 minutes
   static long TEST_ALIVE_FOR=600*1000; // 10 minutes
   static int NUMBER_OF_PRODUCERS=10;
   static int NUMBER_OF_CONSUMERS=10;

   static SynchronizedInt producedMessages = new SynchronizedInt(0);
   static SynchronizedInt readMessages = new SynchronizedInt(0);


   static Context createContext() throws Exception
   {
      Properties props = new Properties();

      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.jnp.interfaces.NamingContextFactory");
      props.put(Context.PROVIDER_URL, "jnp://localhost:1099");
      props.put(Context.URL_PKG_PREFIXES, "org.jnp.interfaces");

      return new InitialContext(props);
   }

   // Constructors ---------------------------------------------------------------------------------

   public ManualQueueSoakTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------

   public void testQueue() throws Exception
   {
      Context ctx = createContext();


      HashSet<Worker> threads = new HashSet<Worker>();

      // A chhanel of communication between workers and the test method
      SynchronousQueue<InternalMessage> testChannel = new SynchronousQueue<InternalMessage>();


      for (int i=0; i<NUMBER_OF_PRODUCERS; i++)
      {
         threads.add(new Producer(i, testChannel));
      }

      for (int i=0; i<NUMBER_OF_CONSUMERS; i++)
      {
         threads.add(new Consumer(i, testChannel));
      }


      for (Worker worker: threads)
      {
         worker.start();
      }

      long timeToFinish = System.currentTimeMillis() + TEST_ALIVE_FOR;

      int numberOfProducers = NUMBER_OF_PRODUCERS;
      int numberOfConsumers = NUMBER_OF_CONSUMERS;

      while (threads.size()>0)
      {
         InternalMessage msg = testChannel.poll(5, TimeUnit.SECONDS);

         if (msg!=null)
         {
            log.info("Received message " + msg);
            if (msg instanceof WorkerFailed)
            {
               fail("Worker " + msg.getWorker() + " has failed");
            }
            else
            if (msg instanceof WorkedFinishedMessages)
            {
               WorkedFinishedMessages finished = (WorkedFinishedMessages)msg;
               if (threads.remove(finished.getWorker()))
               {
                  if (System.currentTimeMillis() < timeToFinish)
                  {
                     if (finished.getWorker() instanceof Producer)
                     {
                        log.info("Scheduling new Producer");
                        Producer producer = new Producer(numberOfProducers++, testChannel);
                        threads.add(producer);
                        producer.start();
                     }
                     else
                     if (finished.getWorker() instanceof Consumer)
                     {
                        log.info("Scheduling new Consumer");
                        Consumer consumer = new Consumer(numberOfConsumers++, testChannel);
                        threads.add(consumer);
                        consumer.start();
                     }
                  }
               }
               else
               {
                  log.warn(finished.getWorker() + " was not available on threads HashSet");
               }
            }
         }
      }


      ctx = createContext();
      ConnectionFactory cf = (ConnectionFactory) ctx.lookup("/ClusteredConnectionFactory");
      Connection conn = cf.createConnection();
      Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
      Queue queue = (Queue )ctx.lookup("queue/testQueue");
      MessageConsumer consumer = sess.createConsumer(queue);

      conn.start();

      while (consumer.receive(1000)!=null)
      {
         readMessages.increment();
      }


      conn.close();

      assertEquals(producedMessages.get(), readMessages.get());
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   protected void setUp() throws Exception
   {
      super.setUp();
      producedMessages = new SynchronizedInt(0);
      readMessages = new SynchronizedInt(0);

   }

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------


   static class Worker extends Thread
   {

      protected Logger log = Logger.getLogger(getClass());

      private boolean failed=false;
      private int workerId;
      private Exception ex;

      SynchronousQueue<InternalMessage> messageQueue;


      public int getWorkerId()
      {
         return workerId;
      }


      public Exception getException()
      {
         return ex;
      }

      public boolean isFailed()
      {
         return failed;
      }

      protected synchronized void setFailed(boolean failed, Exception ex)
      {
         this.failed = failed;
         this.ex = ex;

         sendInternalMessage(new WorkerFailed(this));

      }

      protected void sendInternalMessage(InternalMessage msg)
      {
         log.info("Sending message " + msg);
         try
         {
            messageQueue.put(msg);
         }
         catch (Exception e)
         {
            log.error(e);
            setFailed(true, e);
         }
      }


      public Worker(String name, int workerId, SynchronousQueue<InternalMessage> messageQueue)
      {
         super(name);
         this.workerId = workerId;
         this.messageQueue = messageQueue;
         this.setDaemon(true);
      }

      public String toString()
      {
         return this.getClass().getName() + ":" + getWorkerId();
      }
   }

   static class Producer extends Worker
   {
      public Producer(int producerId, SynchronousQueue<InternalMessage> messageQueue)
      {
         super("Producer:" + producerId, producerId, messageQueue);
      }

      public void run()
      {
         try
         {
            Context ctx = createContext();

            ConnectionFactory cf = (ConnectionFactory) ctx.lookup("/ClusteredConnectionFactory");

            Queue queue = (Queue )ctx.lookup("queue/testQueue");

            log.info("Creating connection and producer");
            Connection conn = cf.createConnection();
            Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer prod = sess.createProducer(queue);
            log.info("Producer was created");

            long timeToFinish = System.currentTimeMillis() + PRODUCER_ALIVE_FOR;

            try
            {
               while(System.currentTimeMillis() < timeToFinish)
               {
                  prod.send(sess.createTextMessage("Message sent at " + System.currentTimeMillis()));
                  producedMessages.increment();
                  Thread.sleep(100);
                  log.info("Sending message");
               }
               sendInternalMessage(new WorkedFinishedMessages(this));
            }
            finally
            {
               conn.close();
            }

         }
         catch (Exception e)
         {
            log.error(e);
            setFailed(true, e);
         }
      }
   }

   static class Consumer extends Worker
   {
      public Consumer(int consumerId, SynchronousQueue<InternalMessage> messageQueue)
      {
         super("Consumer:" + consumerId, consumerId, messageQueue);
      }

      public void run()
      {
         try
         {
            Context ctx = createContext();

            ConnectionFactory cf = (ConnectionFactory) ctx.lookup("/ClusteredConnectionFactory");

            Queue queue = (Queue )ctx.lookup("queue/testQueue");

            log.info("Creating connection and consumer");
            Connection conn = cf.createConnection();
            Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer consumer = sess.createConsumer(queue);
            log.info("Consumer was created");

            conn.start();

            int msgs = 0;

            int transactions = 0;

            long timeToFinish = System.currentTimeMillis() + PRODUCER_ALIVE_FOR;

            try
            {
               while(System.currentTimeMillis() < timeToFinish)
               {
                  Message msg = consumer.receive(1000);
                  if (msg != null)
                  {
                     log.info("Received JMS message");
                     msgs ++;
                     if (msgs>=50)
                     {
                        transactions++;
                        if (transactions%2==0)
                        {
                           log.info("Commit transaction");
                           sess.commit();
                           readMessages.add(msgs);                           
                        }
                        else
                        {
                           log.info("Rollback transaction");
                           sess.rollback();
                        }
                        msgs=0;
                     }
                  }
                  else
                  {
                     readMessages.add(msgs);
                     sess.commit();
                     break;
                  }
               }
               sendInternalMessage(new WorkedFinishedMessages(this));
            }
            finally
            {
               conn.close();
            }

         }
         catch (Exception e)
         {
            log.error(e);
            setFailed(true, e);
         }
      }
   }

   // Objects used on the communication between  Workers and the test
   static class InternalMessage
   {
      Worker worker;


      public InternalMessage(Worker worker)
      {
         this.worker = worker;
      }


      public Worker getWorker()
      {
         return worker;
      }

      public String toString()
      {
         return this.getClass().getName() + " worker-> " + worker.toString();
      }
   }

   static class WorkedFinishedMessages extends InternalMessage
   {


      public WorkedFinishedMessages(Worker worker)
      {
         super(worker);
      }

   }

   static class WorkerFailed extends InternalMessage
   {
      public WorkerFailed(Worker worker)
      {
         super(worker);
      }
   }

}
