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
package org.jboss.jms.example;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.naming.InitialContext;

import org.jboss.jms.util.PerfParams;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.utils.TokenBucketLimiter;
import org.jboss.messaging.utils.TokenBucketLimiterImpl;

/**
 * A simple example that can be used to gather basic performance measurements.
 * 
 * It can be run against any JMS compliant provider, just by changing the jndi.properties file from
 * the examples/jms/config directory, and ensuring that the providers client libraries are on the classpath
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="clebert.suconic@jboss.com">Clebert Suconic</a>
 */
public class PerfExample
{
   private static final String THROUGHPUT_DATA_PROPERTY_NAME = "throughputData";

   private static Logger log = Logger.getLogger(PerfExample.class);

   private Queue queue;
   
   private Queue throughputQueue;

   private Connection connection;

   private Session session;

   private long start;

   public static void main(final String[] args)
   {
      PerfExample perfExample = new PerfExample();

      int noOfMessages = Integer.parseInt(args[1]);
      int noOfWarmupMessages = Integer.parseInt(args[2]);
      int messageSize = Integer.parseInt(args[3]);
      int deliveryMode = args[4].equalsIgnoreCase("persistent") ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
      boolean transacted = Boolean.parseBoolean(args[5]);
      int transactionBatchSize = Integer.parseInt(args[6]);
      boolean dupsok = "DUPS_OK".equalsIgnoreCase(args[7]);
      boolean drainQueue = Boolean.parseBoolean(args[8]);
      String queueLookup = args[9];
      String connectionFactoryLookup = args[10];
      int throttleRate = Integer.parseInt(args[11]);
      String throughputQueue = args[12];

      PerfParams perfParams = new PerfParams();
      perfParams.setNoOfMessagesToSend(noOfMessages);
      perfParams.setNoOfWarmupMessages(noOfWarmupMessages);
      perfParams.setMessageSize(messageSize);
      perfParams.setDeliveryMode(deliveryMode);
      perfParams.setSessionTransacted(transacted);
      perfParams.setTransactionBatchSize(transactionBatchSize);
      perfParams.setDupsOk(dupsok);
      perfParams.setDrainQueue(drainQueue);
      perfParams.setQueueLookup(queueLookup);
      perfParams.setConnectionFactoryLookup(connectionFactoryLookup);
      perfParams.setThrottleRate(throttleRate);
      perfParams.setThroughputQueue(throughputQueue);

      if (args[0].equalsIgnoreCase("-l"))
      {
         perfExample.runListener(perfParams);
      }
      else
      {
         perfExample.runSender(perfParams);
      }
   }

   private void init(final boolean transacted,
                     final String queueLookup,
                     final String throughputQueue,
                     final String connectionFactoryLookup,
                     final boolean dupsOk) throws Exception
   {
      InitialContext initialContext = new InitialContext();
      queue = (Queue)initialContext.lookup(queueLookup);
      if (!throughputQueue.equals("NONE"))
      {
         this.throughputQueue = (Queue)initialContext.lookup(throughputQueue);
      }
      ConnectionFactory cf = (ConnectionFactory)initialContext.lookup(connectionFactoryLookup);
      connection = cf.createConnection();
      session = connection.createSession(transacted, transacted ? Session.SESSION_TRANSACTED
                                                               : (dupsOk ? Session.DUPS_OK_ACKNOWLEDGE
                                                                        : Session.AUTO_ACKNOWLEDGE));
   }

   private void displayAverage(final long numberOfMessages, final long start, final long end)
   {
      double duration = (1.0 * end - start) / 1000; // in seconds
      double average = (1.0 * numberOfMessages / duration);
      log.info(String.format("average: %.2f msg/s (%d messages in %2.2fs)", average, numberOfMessages, duration));
   }

   public void runSender(final PerfParams perfParams)
   {
      try
      {
         log.info("params = " + perfParams);
         init(perfParams.isSessionTransacted(),
              perfParams.getQueueLookup(),
              perfParams.getThroughputQueue(),
              perfParams.getConnectionFactoryLookup(),
              perfParams.isDupsOk());
         start = System.currentTimeMillis();
         log.info("warming up by sending " + perfParams.getNoOfWarmupMessages() + " messages");
         sendMessages(perfParams.getNoOfWarmupMessages(),
                      perfParams.getTransactionBatchSize(),
                      perfParams.getDeliveryMode(),
                      perfParams.isSessionTransacted(),
                      false,
                      perfParams.getThrottleRate(),
                      perfParams.getMessageSize());
         log.info("warmed up");
         start = System.currentTimeMillis();
         sendMessages(perfParams.getNoOfMessagesToSend(),
                      perfParams.getTransactionBatchSize(),
                      perfParams.getDeliveryMode(),
                      perfParams.isSessionTransacted(),
                      true,
                      perfParams.getThrottleRate(),
                      perfParams.getMessageSize());
         long end = System.currentTimeMillis();

         displayAverage(perfParams.getNoOfMessagesToSend(), start, end);

         displayThrouput(perfParams, start);

      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
      finally
      {
         if (connection != null)
         {
            try
            {
               connection.close();
            }
            catch (JMSException e)
            {
               e.printStackTrace();
            }
         }
      }
   }

   private void displayThrouput(final PerfParams perfParams, final long start) throws JMSException
   {
      
      if (throughputQueue == null)
      {
         return;
      }
      
      MessageConsumer consumer = session.createConsumer(throughputQueue);

      connection.start();

      Message msg = consumer.receive(60000);

      if (perfParams.isSessionTransacted())
      {
         session.commit();
      }

      consumer.close();

      if (msg == null)
      {
         log.warn("Throughput Message wasn't received");
      }
      else
      {
         long lastMessageTime = msg.getLongProperty(THROUGHPUT_DATA_PROPERTY_NAME);

         if (lastMessageTime == 0l)
         {
            log.warn("invalid property Throughput-data on LastMessage");
         }

         double throughput = perfParams.getNoOfMessagesToSend() * 1000 / (lastMessageTime - start);

         log.info(String.format("Throughput: %.2f msg/s", throughput));
      }
   }

   private void sendMessages(final int numberOfMessages,
                             final int txBatchSize,
                             final int deliveryMode,
                             final boolean transacted,
                             final boolean display,
                             final int throttleRate,
                             final int messageSize) throws JMSException
   {
      MessageProducer producer = session.createProducer(queue);
      producer.setDisableMessageID(true);
      producer.setDisableMessageTimestamp(true);
      producer.setDeliveryMode(deliveryMode);
      BytesMessage bytesMessage = session.createBytesMessage();
      byte[] payload = new byte[messageSize];
      bytesMessage.writeBytes(payload);

      final int modulo = 2000;

      TokenBucketLimiter tbl = throttleRate != -1 ? new TokenBucketLimiterImpl(throttleRate, false) : null;

      boolean committed = false;
      for (int i = 1; i <= numberOfMessages; i++)
      {
         producer.send(bytesMessage);
         if (transacted)
         {
            if (i % txBatchSize == 0)
            {
               session.commit();
               committed = true;
            }
            else
            {
               committed = false;
            }
         }
         if (display && (i % modulo == 0))
         {
            double duration = (1.0 * System.currentTimeMillis() - start) / 1000;
            log.info(String.format("sent %6d messages in %2.2fs", i, duration));
         }

         if (tbl != null)
         {
            tbl.limit();
         }
      }
      if (transacted && !committed)
      {
         session.commit();
      }
   }

   public void runListener(final PerfParams perfParams)
   {
      try
      {
         init(perfParams.isSessionTransacted(),
              perfParams.getQueueLookup(),
              perfParams.getThroughputQueue(),
              perfParams.getConnectionFactoryLookup(),
              perfParams.isDupsOk());
         
         MessageConsumer messageConsumer = session.createConsumer(queue);
         connection.start();

         if (perfParams.isDrainQueue())
         {
            drainQueue(messageConsumer, perfParams);
            if (perfParams.isSessionTransacted())
            {
               session.commit();
            }
         }

         log.info("READY!!!");

         CountDownLatch countDownLatch = new CountDownLatch(1);
         messageConsumer.setMessageListener(new PerfListener(countDownLatch, perfParams));
         countDownLatch.await();
         long end = System.currentTimeMillis();

         messageConsumer.close();

         // start was set on the first received message
         displayAverage(perfParams.getNoOfMessagesToSend(), start, end);

         answerThroughput(perfParams, end);
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
      finally
      {
         if (connection != null)
         {
            try
            {
               connection.close();
            }
            catch (JMSException e)
            {
               e.printStackTrace();
            }
         }
      }
   }

   private void answerThroughput(final PerfParams perfParams, final long end) throws JMSException
   {
      if (throughputQueue != null)
      {
         MessageProducer producer = session.createProducer(throughputQueue);
         Message msg = session.createMessage();
         msg.setLongProperty(THROUGHPUT_DATA_PROPERTY_NAME, end);
         producer.send(msg);
         if (perfParams.isSessionTransacted())
         {
            session.commit();
         }
      }
   }

   private void drainQueue(final MessageConsumer consumer, final PerfParams perfParams) throws JMSException
   {
      log.info("draining queue");
      int msgs = 0;
      while (true)
      {
         Message m = consumer.receive(5000);
         if (m == null)
         {
            log.info("queue is drained(" + msgs + " messages)");
            break;
         }
         else
         {
            msgs++;
         }

         if (perfParams.isSessionTransacted() && msgs % perfParams.getTransactionBatchSize() == 0)
         {
            session.commit();
         }
      }
   }

   private class PerfListener implements MessageListener
   {
      private final CountDownLatch countDownLatch;

      private final PerfParams perfParams;

      private boolean warmingUp = true;

      private boolean started = false;

      private final int modulo;

      private final AtomicLong count = new AtomicLong(0);

      public PerfListener(final CountDownLatch countDownLatch, final PerfParams perfParams)
      {
         this.countDownLatch = countDownLatch;
         this.perfParams = perfParams;
         warmingUp = perfParams.getNoOfWarmupMessages() > 0;
         modulo = 2000;
      }

      public void onMessage(final Message message)
      {
         try
         {
            if (warmingUp)
            {
               boolean committed = checkCommit();
               if (count.incrementAndGet() == perfParams.getNoOfWarmupMessages())
               {
                  log.info("warmed up after receiving " + count.longValue() + " msgs");
                  if (!committed)
                  {
                     checkCommit();
                  }
                  warmingUp = false;
               }
               return;
            }

            if (!started)
            {
               started = true;
               // reset count to take stats
               count.set(0);
               start = System.currentTimeMillis();
            }

            long currentCount = count.incrementAndGet();
            boolean committed = checkCommit();
            if (currentCount == perfParams.getNoOfMessagesToSend())
            {
               if (!committed)
               {
                  checkCommit();
               }
               countDownLatch.countDown();
            }
            if (currentCount % modulo == 0)
            {
               double duration = (1.0 * System.currentTimeMillis() - start) / 1000;
               log.info(String.format("received %6d messages in %2.2fs", currentCount, duration));
            }
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }
      }

      private boolean checkCommit() throws Exception
      {
         if (perfParams.isSessionTransacted())
         {
            if (count.longValue() % perfParams.getTransactionBatchSize() == 0)
            {
               session.commit();

               return true;
            }
         }
         return false;
      }
   }
}
