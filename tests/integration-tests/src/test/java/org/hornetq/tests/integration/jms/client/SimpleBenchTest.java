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

package org.hornetq.tests.integration.jms.client;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import java.util.ArrayList;
import java.util.List;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.tests.util.JMSTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Clebert Suconic
 */

public class SimpleBenchTest extends JMSTestBase
{


   private Queue queue;


   protected boolean usePersistence()
   {
      return false;
   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      queue = createQueue("queue1");
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      queue = null;
      super.tearDown();
   }


   protected int getNumberOfMessages()
   {
      return 200000;
   }

   protected Connection createConnection() throws Exception
   {
      return cf.createConnection();
   }


   @Test
   public void testMessagesReceivedInParallel() throws Throwable
   {
      final Connection connection = createConnection();
      final int numMessages = getNumberOfMessages();
      long time = System.currentTimeMillis();

      final ArrayList<Throwable> exceptions = new ArrayList<>();

      Thread t = new Thread(new Runnable()
      {
         @Override
         public void run()
         {
            Connection connectionConsumer = null;
            try
            {
               connectionConsumer = createConnection();
//               connectionConsumer = connection;
               connectionConsumer.start();
               Session sessionConsumer = connectionConsumer.createSession(false, Session.AUTO_ACKNOWLEDGE);
               final MessageConsumer consumer = sessionConsumer.createConsumer(queue);

               int count = numMessages;
               while (count > 0)
               {
                  try
                  {
                     BytesMessage m = (BytesMessage) consumer.receive(50000);
                     if (count % 1000 == 0)
                     {
                        System.out.println("Count = " + count + ", property=" + m.getStringProperty("XX"));
                     }
                     Assert.assertNotNull("Could not receive message count=" + count + " on consumer", m);
                     count--;
                  }
                  catch (JMSException e)
                  {
                     break;
                  }
               }
            }
            catch (Throwable e)
            {
               exceptions.add(e);
               e.printStackTrace();
            }
            finally
            {
               try
               {
                  // if the createconnecion wasn't commented out
                  if (connectionConsumer != connection)
                  {
                     connectionConsumer.close();
                  }
               }
               catch (Throwable ignored)
               {
                  // NO OP
               }
            }
         }
      });

      Session session = connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);

      t.start();

      MessageProducer p = session.createProducer(queue);
      p.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      for (int i = 0; i < numMessages; i++)
      {
         BytesMessage message = session.createBytesMessage();
         // TODO: this will break stuff if I use a large number
         message.writeBytes(new byte[5]);
         message.setIntProperty("count", i);
         message.setStringProperty("XX", "count" + i);
         p.send(message);
      }

      long taken = (System.currentTimeMillis() - time);


      System.out.println("taken on send = " + taken);
      t.join();

      for (Throwable e : exceptions)
      {
         throw e;
      }
      taken = (System.currentTimeMillis() - time);

      double messagesPerSecond = ((double) getNumberOfMessages() / (double) taken) * 1000;
      System.out.println("taken = " + taken + ", " + ((int) messagesPerSecond) + " messages per second");

      connection.close();
//      assertEquals(0, q.getMessageCount());
   }


   @Override
   protected void registerConnectionFactory() throws Exception
   {
      List<TransportConfiguration> connectorConfigs = new ArrayList<TransportConfiguration>();
      connectorConfigs.add(new TransportConfiguration(NETTY_CONNECTOR_FACTORY));

      createCF(connectorConfigs, "/cf");

      cf = (ConnectionFactory) namingContext.lookup("/cf");
   }

}
