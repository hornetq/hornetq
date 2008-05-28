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
import javax.naming.NamingException;

import org.jboss.jms.util.PerfParams;
import org.jboss.messaging.core.logging.Logger;

/**
 * a performance example that can be used to gather simple performance figures.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="clebert.suconic@jboss.com">Clebert Suconic</a>
 */
public class PerfExample
{
   private static Logger log = Logger.getLogger(PerfExample.class);
   private Queue queue;
   private Connection connection;
   private Session session;
   private long start;

   public static void main(String[] args)
   {
      PerfExample perfExample = new PerfExample();

      int noOfMessages = Integer.parseInt(args[1]);
      int noOfWarmupMessages = Integer.parseInt(args[2]);
      int deliveryMode = args[3].equalsIgnoreCase("persistent") ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
      boolean transacted = Boolean.parseBoolean(args[4]);
      int transactionBatchSize = Integer.parseInt(args[5]);
      boolean dupsok = "DUPS_OK".equalsIgnoreCase(args[6]);
      boolean drainQueue = Boolean.parseBoolean(args[7]);
      String queueLookup = args[8];
      String connectionFactoryLookup = args[9];

      PerfParams perfParams = new PerfParams();
      perfParams.setNoOfMessagesToSend(noOfMessages);
      perfParams.setNoOfWarmupMessages(noOfWarmupMessages);
      perfParams.setDeliveryMode(deliveryMode);
      perfParams.setSessionTransacted(transacted);
      perfParams.setTransactionBatchSize(transactionBatchSize);
      perfParams.setDupsOk(dupsok);
      perfParams.setDrainQueue(drainQueue);
      perfParams.setQueueLookup(queueLookup);
      perfParams.setConnectionFactoryLookup(connectionFactoryLookup);

      if (args[0].equalsIgnoreCase("-l"))
      {
         perfExample.runListener(perfParams);
      }
      else
      {
         perfExample.runSender(perfParams);
      }

   }

   private void init(boolean transacted, String queueLookup, String connectionFactoryLookup, boolean dupsOk)
           throws NamingException, JMSException
   {
      InitialContext initialContext = new InitialContext();
      queue = (Queue) initialContext.lookup(queueLookup);
      ConnectionFactory cf = (ConnectionFactory) initialContext.lookup(connectionFactoryLookup);
      connection = cf.createConnection();
      session = connection.createSession(transacted, transacted ? Session.SESSION_TRANSACTED : (dupsOk ? Session.DUPS_OK_ACKNOWLEDGE : Session.AUTO_ACKNOWLEDGE));
   }

   private void displayAverage(long numberOfMessages, long start, long end)
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
         init(perfParams.isSessionTransacted(), perfParams.getQueueLookup(), perfParams.getConnectionFactoryLookup(), perfParams.isDupsOk());
         start = System.currentTimeMillis();
         log.info("warming up by sending " + perfParams.getNoOfWarmupMessages() + " messages");
         sendMessages(perfParams.getNoOfWarmupMessages(), perfParams.getTransactionBatchSize(), perfParams.getDeliveryMode(), perfParams.isSessionTransacted(), false);
         log.info("warmed up");

         start = System.currentTimeMillis();
         sendMessages(perfParams.getNoOfMessagesToSend(), perfParams.getTransactionBatchSize(), perfParams.getDeliveryMode(), perfParams.isSessionTransacted(), true);
         long end = System.currentTimeMillis();

         displayAverage(perfParams.getNoOfMessagesToSend(), start, end);
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
      finally
      {
         if (connection != null)
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

   private void sendMessages(int numberOfMessages, int txBatchSize, int deliveryMode, boolean transacted, boolean display) throws JMSException
   {
      MessageProducer producer = session.createProducer(queue);
      producer.setDisableMessageID(true);
      producer.setDisableMessageTimestamp(true);
      producer.setDeliveryMode(deliveryMode);
      BytesMessage bytesMessage = session.createBytesMessage();
      byte[] payload = new byte[1024];
      bytesMessage.writeBytes(payload);
      
      int modulo = numberOfMessages / 10;

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
         init(perfParams.isSessionTransacted(), perfParams.getQueueLookup(), perfParams.getConnectionFactoryLookup(), perfParams.isDupsOk());
         MessageConsumer messageConsumer = session.createConsumer(queue);
         connection.start();

         if (perfParams.isDrainQueue())
         {
            drainQueue(messageConsumer);
         }

         log.info("READY!!!");

         CountDownLatch countDownLatch = new CountDownLatch(1);
         messageConsumer.setMessageListener(new PerfListener(countDownLatch, perfParams));
         countDownLatch.await();
         long end = System.currentTimeMillis();
         // start was set on the first received message
         displayAverage(perfParams.getNoOfMessagesToSend(), start, end);
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
      finally
      {
         if (connection != null)
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

   private void drainQueue(MessageConsumer consumer) throws JMSException
   {
      log.info("draining queue");
      while (true)
      {
         Message m = consumer.receive(5000);
         if (m == null)
         {
            log.info("queue is drained");
            break;
         }
      }
   }

   /**
    * a message listener
    */
   class PerfListener implements MessageListener
   {
      private CountDownLatch countDownLatch;

      private PerfParams perfParams;

      private boolean warmingUp = true;
      private boolean started = false;

      private int modulo;
      private AtomicLong count = new AtomicLong(0);

      public PerfListener(CountDownLatch countDownLatch, PerfParams perfParams)
      {
         this.countDownLatch = countDownLatch;
         this.perfParams = perfParams;
         warmingUp = perfParams.getNoOfWarmupMessages() > 0;
         this.modulo = perfParams.getNoOfMessagesToSend() / 10;
      }

      public void onMessage(Message message)
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
