/*
 * Copyright 2005-2014 Red Hat, Inc.
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
package org.hornetq.tests.performance.sends;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.util.JMSTestBase;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Clebert Suconic
 */

public class ACKTimeTest extends JMSTestBase
{
   private static final String Q_NAME = "test-queue-01";
   private Queue queue;


   // To avoid too many pending messages
   private final Semaphore pendingCredit = new Semaphore(3000);


   private void afterReceive()
   {
      pendingCredit.release();
   }

   private void beforeSend()
   {
      while (!receiverDone.get())
      {
         try
         {
            if (pendingCredit.tryAcquire(1, TimeUnit.SECONDS))
            {
               return;
            }
            else
            {
               System.out.println("Couldn't get credits!");
            }
         }
         catch (Throwable e)
         {
            throw new RuntimeException(e.getMessage(), e);
         }
      }

   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      jmsServer.createQueue(false, Q_NAME, null, true, Q_NAME);
      queue = HornetQJMSClient.createQueue(Q_NAME);

      AddressSettings settings = new AddressSettings();
      settings.setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK);
      settings.setMaxSizeBytes(Long.MAX_VALUE);
      server.getAddressSettingsRepository().clear();
      server.getAddressSettingsRepository().addMatch("#", settings);

   }


   @Override
   protected void registerConnectionFactory() throws Exception
   {
      List<TransportConfiguration> connectorConfigs = new ArrayList<TransportConfiguration>();
      connectorConfigs.add(new TransportConfiguration(NETTY_CONNECTOR_FACTORY));

      createCF(connectorConfigs, "/cf");

      cf = (ConnectionFactory) namingContext.lookup("/cf");
   }



   private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(ACKTimeTest.class.getName());
   private static final long MAX_BATCH_SIZE = Long.getLong("MAX_BATCH_SIZE", 2000);
   public AtomicBoolean receiverDone = new AtomicBoolean(false);


   @Test
   public void testSendReceive() throws Exception
   {
      long numberOfSamples = Long.getLong("HORNETQ_TEST_SAMPLES", 1000);


      MessageReceiver receiver = new MessageReceiver(Q_NAME, numberOfSamples);
      receiver.start();
      MessageSender sender = new MessageSender(Q_NAME);
      sender.start();

      receiver.join();
      receiverDone.set(true);
      sender.join();

   }

   private class MessageReceiver extends Thread
   {
      private final String qName;
      private final long numberOfSamples;

      public MessageReceiver(String qname, long numberOfSamples) throws Exception
      {
         super("Receiver " + qname);
         this.qName = qname;
         this.numberOfSamples = numberOfSamples;
      }

      @Override
      public void run()
      {
         try
         {
            LOGGER.info("Receiver: Connecting");
            Connection c = cf.createConnection();

            int mode = 0;
            mode = Session.CLIENT_ACKNOWLEDGE;
            LOGGER.info("Receiver: Using CLIENT_ACK mode");

            Session s = c.createSession(false, mode);
            Queue q = s.createQueue(this.qName);
            MessageConsumer consumer = s.createConsumer(q, null, false);

            c.start();

            Message m = null;

            // loop until we exhaust all batch sizes we wanted to test
            long currentBatchSize = 1000;
            while (currentBatchSize <= MAX_BATCH_SIZE)
            {
               // number of messages unacked messages since last acknowledgement
               long unackedMsgs = 0;
               // total ack time
               long ackTime = 0;
               // number of time acknowledge() called
               int samples = 0;

               // take the samples specified number of times
               while (samples < numberOfSamples)
               {
                  m = consumer.receive(5000);
                  if (m != null)
                  {
                     afterReceive();

                     unackedMsgs++;

                     // now ack all unacked messages
                     if (unackedMsgs == currentBatchSize)
                     {
                        long st = System.nanoTime();
                        m.acknowledge();
                        ackTime += System.nanoTime() - st;
                        // rested unacked messages count
                        unackedMsgs = 0;
                        // increase the count of number of samples taken
                        samples++;
                     }
                  }
               }
               System.out.println(String.format("%4d - %10d", currentBatchSize, ackTime / samples));

               // now try with different batch size
               currentBatchSize = nextBatchSize(currentBatchSize);
            }

            c.close();
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }
      }

      private long nextBatchSize(long prevBatchSize)
      {
         long newSize;

         if (prevBatchSize < 10)
         {
            newSize = prevBatchSize + 1;
         }
         else if (prevBatchSize < 100)
         {
            newSize = prevBatchSize + 10;
         }
         else
         {
            newSize = prevBatchSize + 100;
         }
         return newSize;
      }

   }

   private class MessageSender extends Thread
   {
      final String qName;

      public MessageSender(String qname) throws Exception
      {
         super("Sender " + qname);

         this.qName = qname;
      }

      @Override
      public void run()
      {
         try
         {
            LOGGER.info("Sender: Connecting");
            Connection c = cf.createConnection();
            Session s = null;
            s = c.createSession(true, Session.SESSION_TRANSACTED);
            LOGGER.info("Sender: Using TRANSACTED session");


            Queue q = s.createQueue(this.qName);
            MessageProducer producer = s.createProducer(null);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            c.start();
            long sent = 0L;
            while (!receiverDone.get())
            {
               sent++;

               beforeSend();
               producer.send(q, s.createTextMessage("Message_" + sent));
               if (sent % MAX_BATCH_SIZE == 0)
               {
                  s.commit();
               }
            }

         }
         catch (Exception e)
         {
            if (e instanceof InterruptedException)
            {
               LOGGER.info("Sender done.");
            }
            else
            {
               e.printStackTrace();
            }
         }
      }
   }
}