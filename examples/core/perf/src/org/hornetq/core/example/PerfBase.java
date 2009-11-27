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
package org.hornetq.core.example;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.MessageHandler;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.integration.transports.netty.NettyConnectorFactory;
import org.hornetq.integration.transports.netty.TransportConstants;
import org.hornetq.utils.TokenBucketLimiter;
import org.hornetq.utils.TokenBucketLimiterImpl;

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
   
   private static final String DEFAULT_PERF_PROPERTIES_FILE_NAME = "perf.properties";
   
   private static byte[] randomByteArray(final int length)
   {
      byte[] bytes = new byte[length];
      
      Random random = new Random();
      
      for (int i = 0; i < length; i++)
      {
         bytes[i] = Integer.valueOf(random.nextInt()).byteValue();
      }
      
      return bytes;      
   }
   
   protected static String getPerfFileName(String[] args)
   {
      String fileName;
      
      if (args.length > 0)
      {
         fileName = args[0];
      }
      else
      {
         fileName = DEFAULT_PERF_PROPERTIES_FILE_NAME;
      }
      
      log.info("Using file name " + fileName);
      
      return fileName;
   }

   protected static PerfParams getParams(final String fileName) throws Exception
   {
      Properties props = null;

      InputStream is = null;

      try
      {
         is = new FileInputStream(fileName);

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
      boolean preAck = Boolean.valueOf(props.getProperty("pre-ack"));
      int confirmationWindowSize = Integer.valueOf(props.getProperty("confirmation-window"));
      int producerWindowSize = Integer.valueOf(props.getProperty("producer-window"));
      int consumerWindowSize = Integer.valueOf(props.getProperty("consumer-window"));
      boolean blockOnACK = Boolean.valueOf(props.getProperty("block-ack", "false"));
      boolean blockOnPersistent = Boolean.valueOf(props.getProperty("block-persistent", "false"));

      log.info("num-messages: " + noOfMessages);
      log.info("num-warmup-messages: " + noOfWarmupMessages);
      log.info("message-size: " + messageSize);
      log.info("durable: " + durable);
      log.info("transacted: " + transacted);
      log.info("batch-size: " + batchSize);
      log.info("drain-queue: " + drainQueue);
      log.info("address: " + address);
      log.info("queue name: " + queueName);
      log.info("throttle-rate: " + throttleRate);
      log.info("host:" + host);
      log.info("port: " + port);
      log.info("tcp buffer: " + tcpBufferSize);
      log.info("tcp no delay: " + tcpNoDelay);
      log.info("pre-ack: " + preAck);
      log.info("confirmation-window: " + confirmationWindowSize);
      log.info("producer-window: " + producerWindowSize);
      log.info("consumer-window: " + consumerWindowSize);
      log.info("block-ack:" + blockOnACK);
      log.info("block-persistent:" + blockOnPersistent);

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
      perfParams.setPreAck(preAck);
      perfParams.setConfirmationWindow(confirmationWindowSize);
      perfParams.setProducerWindow(producerWindowSize);
      perfParams.setConsumerWindow(consumerWindowSize);
      perfParams.setBlockOnACK(blockOnACK);
      perfParams.setBlockOnPersistent(blockOnPersistent);

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
      factory.setPreAcknowledge(perfParams.isPreAck());
      factory.setConfirmationWindowSize(perfParams.getConfirmationWindow());
      factory.setProducerWindowSize(perfParams.getProducerWindow());
      factory.setConsumerWindowSize(perfParams.getConsumerWindow());

      factory.setAckBatchSize(perfParams.getBatchSize());
      
      factory.setBlockOnAcknowledge(perfParams.isBlockOnACK());
      factory.setBlockOnPersistentSend(perfParams.isBlockOnPersistent());

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

      byte[] payload = randomByteArray(messageSize);

      message.getBodyBuffer().writeBytes(payload);

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
         
       //  log.info("sent message " + i);

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
