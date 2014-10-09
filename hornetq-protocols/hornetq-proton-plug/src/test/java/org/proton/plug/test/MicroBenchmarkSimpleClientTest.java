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

package org.proton.plug.test;

import javax.jms.JMSException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.message.impl.MessageImpl;
import org.proton.plug.AMQPClientConnectionContext;
import org.proton.plug.AMQPClientReceiverContext;
import org.proton.plug.AMQPClientSenderContext;
import org.proton.plug.AMQPClientSessionContext;
import org.proton.plug.sasl.ClientSASLPlain;
import org.proton.plug.test.minimalclient.Connector;
import org.proton.plug.test.util.SimpleServerAbstractTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * @author Clebert Suconic
 */
@RunWith(Parameterized.class)
public class MicroBenchmarkSimpleClientTest extends SimpleServerAbstractTest
{

   final int timeout = 5;

   @Parameterized.Parameters(name = "sasl={0}, inVM={1}")
   public static Collection<Object[]> data()
   {
      List<Object[]> list = Arrays.asList(new Object[][]{
         {Boolean.TRUE, Boolean.FALSE},
         {Boolean.FALSE, Boolean.FALSE}});

      System.out.println("Size = " + list.size());
      return list;
   }

   public MicroBenchmarkSimpleClientTest(boolean useSASL, boolean useInVM)
   {
      super(useSASL, useInVM);
   }

   @Test
   public void testMessagesReceivedInParallel() throws Throwable
   {
      Connector connector1 = newConnector();
      connector1.start();
      final AMQPClientConnectionContext clientConnection = connector1.connect("127.0.0.1", Constants.PORT);
      clientConnection.clientOpen(useSASL ? new ClientSASLPlain("AA", "AA") : null);


      final AMQPClientConnectionContext connectionConsumer = connector1.connect("127.0.0.1", Constants.PORT);
      connectionConsumer.clientOpen(useSASL ? new ClientSASLPlain("AA", "AA") : null);


      final int numMessages = getNumberOfMessages();
      long time = System.currentTimeMillis();

      final ArrayList<Throwable> exceptions = new ArrayList<>();

      Thread t = new Thread(new Runnable()
      {
         @Override
         public void run()
         {
            try
            {
               AMQPClientSessionContext sessionConsumer = connectionConsumer.createClientSession();
               AMQPClientReceiverContext receiver = sessionConsumer.createReceiver("Test");
               receiver.flow(500);

               int received = 0;
               int count = numMessages;
               while (count > 0)
               {
                  if (received % 500 == 0 && received > 0)
                  {
                     receiver.flow(500);
                     if (received % 5000 == 0)
                     {
                        System.out.println("Received " + received);
                     }
                  }
                  received++;

                  try
                  {
                     MessageImpl m = (MessageImpl) receiver.receiveMessage(timeout, TimeUnit.SECONDS);
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
         }
      });

      AMQPClientSessionContext session = clientConnection.createClientSession();
      t.start();

      AMQPClientSenderContext sender = session.createSender("Test", true);
      for (int i = 0; i < numMessages; i++)
      {
         MessageImpl message = (MessageImpl) Message.Factory.create();
         message.setBody(new Data(new Binary(new byte[5])));
         sender.send(message);
      }

      t.join();

      for (Throwable e : exceptions)
      {
         throw e;
      }
      long taken = (System.currentTimeMillis() - time);
      System.out.println("Microbenchamrk ran in " + taken + " milliseconds, sending/receiving " + getNumberOfMessages() + " messages, while SASL = " + useSASL + ", inVM=" + this.useInVM);

      double messagesPerSecond = ((double) getNumberOfMessages() / (double) taken) * 1000;

      System.out.println(((int) messagesPerSecond) + " messages per second");

   }

   @Test
   public void testSimpleSend() throws Exception
   {
      Connector connector = newConnector();
      connector.start();
      AMQPClientConnectionContext clientConnection = connector.connect("127.0.0.1", Constants.PORT);

      clientConnection.clientOpen(useSASL ? new ClientSASLPlain("aa", "aa") : null);

      AMQPClientSessionContext session = clientConnection.createClientSession();
      AMQPClientSenderContext clientSender = session.createSender("Test", true);
      Properties props = new Properties();

      MessageImpl message = (MessageImpl) Message.Factory.create();

      Data value = new Data(new Binary(new byte[500]));

      message.setBody(value);
      clientSender.send(message);

      AMQPClientReceiverContext receiver = session.createReceiver("Test");

      receiver.flow(1000);

      message = (MessageImpl) receiver.receiveMessage(5, TimeUnit.SECONDS);

      System.out.println("Received message " + message.getBody());


   }



   protected int getNumberOfMessages()
   {
      return 200000;
   }

}
