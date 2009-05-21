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
import java.util.Properties;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;

import org.jboss.common.example.JBMExample;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.integration.transports.netty.NettyConnectorFactory;
import org.jboss.messaging.utils.TokenBucketLimiter;
import org.jboss.messaging.utils.TokenBucketLimiterImpl;

/**
 * 
 * A PerfSender
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class PerfSender extends JBMExample
{
   public static void main(String[] args)
   {      
      try
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
         boolean drainQueue =  Boolean.valueOf(props.getProperty("drain-queue"));
         String queueName = props.getProperty("queue-name");
         int throttleRate = Integer.valueOf(props.getProperty("throttle-rate"));

         PerfParams perfParams = new PerfParams();
         perfParams.setNoOfMessagesToSend(noOfMessages);
         perfParams.setNoOfWarmupMessages(noOfWarmupMessages);
         perfParams.setMessageSize(messageSize);
         perfParams.setDurable(durable);
         perfParams.setSessionTransacted(transacted);
         perfParams.setBatchSize(batchSize);
         perfParams.setDrainQueue(drainQueue);
         perfParams.setQueueName(queueName);
         perfParams.setThrottleRate(throttleRate);

         new PerfSender(perfParams).run(args);
      }      
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

   private final PerfParams perfParams;

   private PerfSender(final PerfParams perfParams)
   {
      super();

      this.perfParams = perfParams;
   }

   private ClientSessionFactory factory;

   private ClientSession session;

   private long start;

   public boolean runExample() throws Exception
   {
      runSender(perfParams);

      return true;
   }

   private void init(final boolean transacted, final String queueName) throws Exception
   {
      factory = new ClientSessionFactoryImpl(new TransportConfiguration(NettyConnectorFactory.class.getName()));

      factory.setAckBatchSize(perfParams.getBatchSize());

      session = factory.createSession(!transacted, !transacted);

      session.createQueue(perfParams.getQueueName(), perfParams.getQueueName(), perfParams.isDurable());
   }

   private void displayAverage(final long numberOfMessages, final long start, final long end)
   {
      double duration = (1.0 * end - start) / 1000; // in seconds
      double average = (1.0 * numberOfMessages / duration);
      log.info(String.format("average: %.2f msg/s (%d messages in %2.2fs)", average, numberOfMessages, duration));
   }

   private void runSender(final PerfParams perfParams)
   {
      try
      {
         log.info("params = " + perfParams);
         init(perfParams.isSessionTransacted(), perfParams.getQueueName());
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

   private void sendMessages(final int numberOfMessages,
                             final int txBatchSize,
                             final boolean durable,
                             final boolean transacted,
                             final boolean display,
                             final int throttleRate,
                             final int messageSize) throws Exception
   {
      ClientProducer producer = session.createProducer(perfParams.getQueueName());

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

   private void drainQueue(final MessageConsumer consumer) throws JMSException
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

   // public boolean runExample() throws Exception
   // {
   // final String perfAddress = "perfAddress";
   //
   // final String perfQueueName = "perfQueue";
   //
   // ClientSessionFactory factory = null;
   //
   // ClientSession session = null;
   //
   // try
   // {
   // factory = new ClientSessionFactoryImpl(new TransportConfiguration(NettyConnectorFactory.class.getName()));
   //
   // session = factory.createSession();
   //
   // session.createQueue(perfAddress, perfQueueName, true);
   //
   // ClientProducer producer = session.createProducer(perfAddress);
   //
   // ClientMessage message = session.createClientMessage(true);
   //
   // message.getBody().writeString("Hello");
   //
   // producer.send(message);
   //
   // session.start();
   //
   // ClientConsumer consumer = session.createConsumer(perfQueueName);
   //
   // ClientMessage msgReceived = consumer.receive();
   //
   // System.out.println("message = " + msgReceived.getBody().readString());
   //
   // consumer.close();
   //
   // session.deleteQueue(perfQueueName);
   //
   // return true;
   // }
   // finally
   // {
   // if (session != null)
   // {
   // session.close();
   // }
   //
   // if (factory != null)
   // {
   // factory.close();
   // }
   // }
   // }

}
