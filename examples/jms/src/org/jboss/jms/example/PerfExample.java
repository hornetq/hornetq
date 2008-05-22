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

import org.jboss.messaging.core.logging.Logger;
import org.jboss.jms.util.PerfParams;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.jms.*;
import java.util.concurrent.*;

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
   private int messageCount = 0;
   private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
   private Session session;
   private Sampler command = new Sampler();

   public static void main(String[] args)
   {
      PerfExample perfExample = new PerfExample();
      
      int noOfMessages = Integer.parseInt(args[1]);
      int deliveryMode = args[2].equalsIgnoreCase("persistent")? DeliveryMode.PERSISTENT: DeliveryMode.NON_PERSISTENT;
      long samplePeriod = Long.parseLong(args[3]);
      boolean transacted = Boolean.parseBoolean(args[4]);
      log.info("Transacted:" + transacted);
      int transactionBatchSize = Integer.parseInt(args[5]);
      PerfParams perfParams = new PerfParams();
      perfParams.setNoOfMessagesToSend(noOfMessages);
      perfParams.setDeliveryMode(deliveryMode);
      perfParams.setSamplePeriod(samplePeriod);
      perfParams.setSessionTransacted(transacted);
      perfParams.setTransactionBatchSize(transactionBatchSize);
      
      if (args[0].equalsIgnoreCase("-l"))
      {
         perfExample.runListener(perfParams);
      }
      else
      {
         
         perfExample.runSender(perfParams);
      }

   }

   private void init(boolean transacted)
           throws NamingException, JMSException
   {
      InitialContext initialContext = new InitialContext();
      queue = (Queue) initialContext.lookup("/queue/testPerfQueue");
      ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("/ConnectionFactory");
      connection = cf.createConnection();
      session = connection.createSession(transacted, transacted ? Session.SESSION_TRANSACTED : Session.DUPS_OK_ACKNOWLEDGE);
   }
   
   public void runSender(PerfParams perfParams)
   {
      try
      {
         log.info("params = " + perfParams);
         init(perfParams.isSessionTransacted());
         MessageProducer producer = session.createProducer(queue);
         producer.setDisableMessageID(true);
         producer.setDisableMessageTimestamp(true);
         producer.setDeliveryMode(perfParams.getDeliveryMode());
         scheduler.scheduleAtFixedRate(command, perfParams.getSamplePeriod(), perfParams.getSamplePeriod(), TimeUnit.SECONDS);
         BytesMessage bytesMessage = session.createBytesMessage();
         byte[] payload = new byte[1024];
         bytesMessage.writeBytes(payload);
         boolean committed = false;
         for (int i = 1; i <= perfParams.getNoOfMessagesToSend(); i++)
         {
            producer.send(bytesMessage);
            messageCount++;
            if (perfParams.isSessionTransacted())
            {
               if (messageCount % perfParams.getTransactionBatchSize() == 0)
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
         if (perfParams.isSessionTransacted() && !committed)
         {
            session.commit();
         }
         scheduler.shutdownNow();
         log.info("average: " + (command.getAverage() / perfParams.getSamplePeriod()) + " msg/s");
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

   public void runListener(final PerfParams perfParams)
   {
      try
      {
         init(perfParams.isSessionTransacted());
         MessageConsumer messageConsumer = session.createConsumer(queue);
         CountDownLatch countDownLatch = new CountDownLatch(1);
         connection.start();
         log.info("emptying queue");
         while (true)
         {
            Message m = messageConsumer.receive(500);
            if (m == null)
            {
               break;
            }
         }
         messageConsumer.setMessageListener(new PerfListener(countDownLatch, perfParams));
         log.info("READY!!!");
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



   /**
    * a message listener
    */
   class PerfListener implements MessageListener
   {
      private CountDownLatch countDownLatch;
      PerfParams perfParams;
      
      boolean started = false;


      public PerfListener(CountDownLatch countDownLatch, PerfParams perfParams)
      {
         this.countDownLatch = countDownLatch;
         this.perfParams = perfParams;
      }

      public void onMessage(Message message)
      {
         if (!started)
         {
            started = true;
            scheduler.scheduleAtFixedRate(command, 1, 1, TimeUnit.SECONDS);
         }

         try
         {
            BytesMessage bm = (BytesMessage) message;
            messageCount++;      
            boolean committed = checkCommit();
            if (messageCount == perfParams.getNoOfMessagesToSend())
            {
               if (!committed)
               {
                  checkCommit();
               }
               countDownLatch.countDown();
               scheduler.shutdownNow();
               log.info("average: " +  command.getAverage() + " msg/s" );
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
            if (messageCount % perfParams.getTransactionBatchSize() == 0)
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
      private static final int IGNORED_SAMPLES = 4;
      
      int sampleCount = 0;
      int ignoredCount = 0;
      
      long startTime = 0;

      long samplesTaken = 0;

      public void run()
      {         
         if(startTime == 0)
         {
            startTime = System.currentTimeMillis();
         }
         long elapsedTime = (System.currentTimeMillis() - startTime) / 1000; // in s
         int lastCount = sampleCount;
         sampleCount = messageCount;
         if (samplesTaken >= IGNORED_SAMPLES)
         {
            info(elapsedTime, sampleCount, sampleCount - lastCount, false);
         } else {
            info(elapsedTime, sampleCount, sampleCount - lastCount, true);
            ignoredCount += (sampleCount - lastCount);            
         }
         samplesTaken++;
      }

      public void info(long elapsedTime, int totalCount, int sampleCount, boolean ignored)
      {
         String message = String.format("time elapsed: %2ds, message count: %7d, this period: %5d %s", 
               elapsedTime, totalCount, sampleCount, ignored ? "[IGNORED]" : "");
         log.info(message);
      }
      
      public long getAverage()
      {
         return (sampleCount - ignoredCount)/(samplesTaken - IGNORED_SAMPLES);
      }

   }
}
