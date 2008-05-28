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

import org.jboss.jms.util.PerfParams;
import org.jboss.messaging.core.logging.Logger;

import javax.jms.*;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
   private AtomicLong messageCount = new AtomicLong(0);
   private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
   private Session session;
   private Sampler command = new Sampler();

   public static void main(String[] args)
   {
      PerfExample perfExample = new PerfExample();

      int noOfMessages = Integer.parseInt(args[1]);
      int noOfWarmupMessages = Integer.parseInt(args[2]);
      int deliveryMode = args[3].equalsIgnoreCase("persistent") ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
      long samplePeriod = Long.parseLong(args[4]);
      boolean transacted = Boolean.parseBoolean(args[5]);
      log.info("Transacted:" + transacted);
      int transactionBatchSize = Integer.parseInt(args[6]);
      boolean drainQueue = Boolean.parseBoolean(args[7]);
      String queueLookup = args[8];
      String connectionFactoryLookup = args[9];
      boolean dupsok = "DUPS_OK".equalsIgnoreCase(args[10]);
      PerfParams perfParams = new PerfParams();
      perfParams.setNoOfMessagesToSend(noOfMessages);
      perfParams.setNoOfWarmupMessages(noOfWarmupMessages);
      perfParams.setDeliveryMode(deliveryMode);
      perfParams.setSamplePeriod(samplePeriod);
      perfParams.setSessionTransacted(transacted);
      perfParams.setTransactionBatchSize(transactionBatchSize);
      perfParams.setDrainQueue(drainQueue);
      perfParams.setQueueLookup(queueLookup);
      perfParams.setConnectionFactoryLookup(connectionFactoryLookup);
      perfParams.setDupsOk(dupsok);

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

   public void runSender(final PerfParams perfParams)
   {
      try
      {
         log.info("params = " + perfParams);
         init(perfParams.isSessionTransacted(), perfParams.getQueueLookup(), perfParams.getConnectionFactoryLookup(), perfParams.isDupsOk());
         log.info("warming up by sending " + perfParams.getNoOfWarmupMessages() + " messages");
         sendMessages(perfParams.getNoOfWarmupMessages(), perfParams.getTransactionBatchSize(), perfParams.getDeliveryMode(), perfParams.isSessionTransacted());
         log.info("warmed up");
         messageCount.set(0);

         scheduler.scheduleAtFixedRate(command, perfParams.getSamplePeriod(), perfParams.getSamplePeriod(), TimeUnit.SECONDS);
         sendMessages(perfParams.getNoOfMessagesToSend(), perfParams.getTransactionBatchSize(), perfParams.getDeliveryMode(), perfParams.isSessionTransacted());
         scheduler.shutdownNow();

         log.info(String.format("average: %.2f msg/s", (command.getAverage() / perfParams.getSamplePeriod())));
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

   private void sendMessages(int numberOfMessages, int txBatchSize, int deliveryMode, boolean transacted) throws JMSException
   {
      MessageProducer producer = session.createProducer(queue);
      producer.setDisableMessageID(true);
      producer.setDisableMessageTimestamp(true);
      producer.setDeliveryMode(deliveryMode);
      BytesMessage bytesMessage = session.createBytesMessage();
      byte[] payload = new byte[1024];
      bytesMessage.writeBytes(payload);

      boolean committed = false;
      for (int i = 1; i <= numberOfMessages; i++)
      {
         producer.send(bytesMessage);
         messageCount.incrementAndGet();
         if (transacted)
         {
            if (messageCount.longValue() % txBatchSize == 0)
            {
               session.commit();
               committed = true;
            }
            else
            {
               committed = false;
            }
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
         Message m = consumer.receive(500);
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

      public PerfListener(CountDownLatch countDownLatch, PerfParams perfParams)
      {
         this.countDownLatch = countDownLatch;
         this.perfParams = perfParams;
      }

      public void onMessage(Message message)
      {
         try
         {
            if (warmingUp)
            {
               boolean committed = checkCommit();
               if (messageCount.longValue() == perfParams.getNoOfWarmupMessages())
               {
                  log.info("warmed up after receiving " + messageCount.longValue() + " msgs");
                  if (!committed)
                  {
                     checkCommit();
                  }
                  warmingUp = false;
                  // reset messageCount to take stats
                  messageCount.set(0);
               } else {
                  messageCount.incrementAndGet();
               }
               return;
            }

            if (!started)
            {
               started = true;
               scheduler.scheduleAtFixedRate(command, 1, 1, TimeUnit.SECONDS);
            }

            messageCount.incrementAndGet();
            boolean committed = checkCommit();
            if (messageCount.longValue() == perfParams.getNoOfMessagesToSend())
            {
               if (!committed)
               {
                  checkCommit();
               }
               countDownLatch.countDown();
               scheduler.shutdownNow();
               log.info(String.format("average: %.2f msg/s", command.getAverage()));
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
            if (messageCount.longValue() % perfParams.getTransactionBatchSize() == 0)
            {
               session.commit();

               return true;
            }
         }
         return false;
      }
   }

   /**
    * simple class to gather performance figures
    */
   class Sampler implements Runnable
   {
      long sampleCount = 0;

      long startTime = 0;
      long samplesTaken = 0;

      public void run()
      {
         if (startTime == 0)
         {
            startTime = System.currentTimeMillis();
         }
         long elapsedTime = (System.currentTimeMillis() - startTime) / 1000; // in s
         long lastCount = sampleCount;
         sampleCount = messageCount.longValue();
         log.info(String.format("time elapsed: %2ds, message count: %7d, this period: %5d",
                 elapsedTime, sampleCount, sampleCount - lastCount));
         samplesTaken++;
      }

      public double getAverage()
      {
         return (1.0 * sampleCount)/samplesTaken;
      }

   }
}
