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
package org.jboss.core.example;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.MessageHandler;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.integration.transports.netty.NettyConnectorFactory;
import org.jboss.messaging.integration.transports.netty.TransportConstants;
import org.jboss.messaging.utils.TokenBucketLimiter;
import org.jboss.messaging.utils.TokenBucketLimiterImpl;

/**
 * 
 * A PerfBase
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public abstract class PerfBase
{
   private static final Logger log = Logger.getLogger(PerfSender.class.getName());

   protected static PerfParams getParams() throws Exception
   {
      Properties props = null;

      InputStream is = null;

      try
      {
         is = new FileInputStream("perf.properties");

         props = new Properties();

         props.load(is);
      }
      finally
      {
         if (is != null)
         {
            is.close();
         }
      }

      int noOfMessages = Integer.valueOf(props.getProperty("num-messages"));
      int noOfWarmupMessages = Integer.valueOf(props.getProperty("num-warmup-messages"));
      int messageSize = Integer.valueOf(props.getProperty("message-size"));
      boolean durable = Boolean.valueOf(props.getProperty("durable"));
      boolean transacted = Boolean.valueOf(props.getProperty("transacted"));
      int batchSize = Integer.valueOf(props.getProperty("batch-size"));
      boolean drainQueue = Boolean.valueOf(props.getProperty("drain-queue"));
      String queueName = props.getProperty("queue-name");
      String address = props.getProperty("address");
      int throttleRate = Integer.valueOf(props.getProperty("throttle-rate"));
      String host = props.getProperty("host");
      int port = Integer.valueOf(props.getProperty("port"));
      int tcpBufferSize = Integer.valueOf(props.getProperty("tcp-buffer"));
      boolean tcpNoDelay = Boolean.valueOf(props.getProperty("tcp-no-delay"));

      log.info("num-messages: " + noOfMessages);
      log.info("num-warmup-messages: " + noOfWarmupMessages);
      log.info("message-size: " + messageSize);
      log.info("durable: " + durable);
      log.info("transacted: " + transacted);
      log.info("batch-size: " + batchSize);
      log.info("drain-queue: " + drainQueue);
      log.info("address: " + address);
      log.info("throttle-rate: " + throttleRate);
      log.info("host:" + host);
      log.info("port: " + port);
      log.info("tcp buffer: " + tcpBufferSize);
      log.info("tcp no delay: " + tcpNoDelay);

      PerfParams perfParams = new PerfParams();
      perfParams.setNoOfMessagesToSend(noOfMessages);
      perfParams.setNoOfWarmupMessages(noOfWarmupMessages);
      perfParams.setMessageSize(messageSize);
      perfParams.setDurable(durable);
      perfParams.setSessionTransacted(transacted);
      perfParams.setBatchSize(batchSize);
      perfParams.setDrainQueue(drainQueue);
      perfParams.setQueueName(queueName);
      perfParams.setAddress(address);
      perfParams.setThrottleRate(throttleRate);
      perfParams.setHost(host);
      perfParams.setPort(port);
      perfParams.setTcpBufferSize(tcpBufferSize);
      perfParams.setTcpNoDelay(tcpNoDelay);

      return perfParams;
   }

   private final PerfParams perfParams;

   protected PerfBase(final PerfParams perfParams)
   {
      this.perfParams = perfParams;
   }

   private ClientSessionFactory factory;

   private ClientSession session;

   private long start;

   private void init(final boolean transacted, final String queueName) throws Exception
   {
      Map<String, Object> params = new HashMap<String, Object>();

      params.put(TransportConstants.TCP_NODELAY_PROPNAME, perfParams.isTcpNoDelay());
      params.put(TransportConstants.TCP_SENDBUFFER_SIZE_PROPNAME, perfParams.getTcpBufferSize());
      params.put(TransportConstants.TCP_RECEIVEBUFFER_SIZE_PROPNAME, perfParams.getTcpBufferSize());
      
      params.put(TransportConstants.HOST_PROP_NAME, perfParams.getHost());
      params.put(TransportConstants.PORT_PROP_NAME, perfParams.getPort());

      factory = new ClientSessionFactoryImpl(new TransportConfiguration(NettyConnectorFactory.class.getName(), params));
      factory.setPreAcknowledge(true);

      factory.setAckBatchSize(perfParams.getBatchSize());

      session = factory.createSession(!transacted, !transacted);
   }

   private void displayAverage(final long numberOfMessages, final long start, final long end)
   {
      double duration = (1.0 * end - start) / 1000; // in seconds
      double average = (1.0 * numberOfMessages / duration);
      log.info(String.format("average: %.2f msg/s (%d messages in %2.2fs)", average, numberOfMessages, duration));
   }

   protected void runSender()
   {
      try
      {
         log.info("params = " + perfParams);
         init(perfParams.isSessionTransacted(), perfParams.getQueueName());

         if (perfParams.isDrainQueue())
         {
            drainQueue();
         }

         start = System.currentTimeMillis();
         log.info("warming up by sending " + perfParams.getNoOfWarmupMessages() + " messages");
         sendMessages(perfParams.getNoOfWarmupMessages(),
                      perfParams.getBatchSize(),
                      perfParams.isDurable(),
                      perfParams.isSessionTransacted(),
                      false,
                      perfParams.getThrottleRate(),
                      perfParams.getMessageSize());
         log.info("warmed up");
         start = System.currentTimeMillis();
         sendMessages(perfParams.getNoOfMessagesToSend(),
                      perfParams.getBatchSize(),
                      perfParams.isDurable(),
                      perfParams.isSessionTransacted(),
                      true,
                      perfParams.getThrottleRate(),
                      perfParams.getMessageSize());
         long end = System.currentTimeMillis();
         displayAverage(perfParams.getNoOfMessagesToSend(), start, end);
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
      finally
      {
         if (session != null)
         {
            try
            {
               session.close();
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }
      }
   }
   
   protected void runListener()
   {
      try
      {
         
         init(perfParams.isSessionTransacted(), perfParams.getQueueName());

         if (perfParams.isDrainQueue())
         {
            drainQueue();
         }
                  
         ClientConsumer consumer = session.createConsumer(perfParams.getQueueName());
         
         session.start();

         log.info("READY!!!");

         CountDownLatch countDownLatch = new CountDownLatch(1);
         consumer.setMessageHandler(new PerfListener(countDownLatch, perfParams));
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
         if (session != null)
         {
            try
            {
               session.close();
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }
      }
   }

   private void drainQueue() throws Exception
   {
      log.info("Draining queue");
      ClientConsumer consumer = session.createConsumer(perfParams.getQueueName());

      session.start();

      ClientMessage message = null;

      int count = 0;
      do
      {
         message = consumer.receive(3000);

         if (message != null)
         {
            message.acknowledge();

            count++;
         }
      }
      while (message != null);
      
      consumer.close();
      
      log.info("Drained " + count + " messages");
   }

   private void sendMessages(final int numberOfMessages,
                             final int txBatchSize,
                             final boolean durable,
                             final boolean transacted,
                             final boolean display,
                             final int throttleRate,
                             final int messageSize) throws Exception
   {
      ClientProducer producer = session.createProducer(perfParams.getAddress());

      ClientMessage message = session.createClientMessage(durable);

      byte[] payload = new byte[messageSize];

      message.getBody().writeBytes(payload);

      final int modulo = 2000;

      TokenBucketLimiter tbl = throttleRate != -1 ? new TokenBucketLimiterImpl(throttleRate, false) : null;

      boolean committed = false;
      for (int i = 1; i <= numberOfMessages; i++)
      {
         producer.send(message);

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
   
   private class PerfListener implements MessageHandler
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
         this.modulo = 2000;
      }

      public void onMessage(final ClientMessage message)
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
            
            message.acknowledge();

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
            if (count.longValue() % perfParams.getBatchSize() == 0)
            {
               session.commit();

               return true;
            }
         }
         return false;
      }
   }

}
