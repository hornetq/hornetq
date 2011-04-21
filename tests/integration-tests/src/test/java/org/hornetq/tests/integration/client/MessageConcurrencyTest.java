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
package org.hornetq.tests.integration.client;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.*;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * 
 * A MessageConcurrencyTest
 *
 * @author Tim Fox
 *
 *
 */
public class MessageConcurrencyTest extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(ConsumerTest.class);

   private HornetQServer server;

   private final SimpleString ADDRESS = new SimpleString("MessageConcurrencyTestAddress");
   
   private final SimpleString QUEUE_NAME = new SimpleString("MessageConcurrencyTestQueue");

   private ServerLocator locator;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      server = createServer(false);

      server.start();

      locator = createInVMNonHALocator();
   }

   @Override
   protected void tearDown() throws Exception
   {
      locator.close();

      server.stop();

      server = null;

      super.tearDown();
   }

   // Test that a created message can be sent via multiple producers on different sessions concurrently
   public void testMessageConcurrency() throws Exception
   {
      ClientSessionFactory sf = locator.createSessionFactory();

      ClientSession createSession = sf.createSession();
      
      Set<ClientSession> sendSessions = new HashSet<ClientSession>();

      Set<Sender> senders = new HashSet<Sender>();

      final int numSessions = 100;
      
      final int numMessages = 1000;

      for (int i = 0; i < numSessions; i++)
      {
         ClientSession sendSession = sf.createSession();
         
         sendSessions.add(sendSession);

         ClientProducer producer = sendSession.createProducer(ADDRESS);
         
         Sender sender = new Sender(numMessages, producer);
         
         senders.add(sender);
         
         sender.start();
      }
      
      for (int i = 0; i < numMessages; i++)
      {
         byte[] body = RandomUtil.randomBytes(1000);

         ClientMessage message = createSession.createMessage(false);
         
         message.getBodyBuffer().writeBytes(body);
         
         for (Sender sender: senders)
         {
            sender.queue.add(message);
         }
      }

      for (Sender sender: senders)
      {
         sender.join();
         
         assertFalse(sender.failed);
      }
      
      for (ClientSession sendSession: sendSessions)
      {
         sendSession.close();
      }
       
      createSession.close();

      sf.close();      
   }
   
   // Test that a created message can be sent via multiple producers after being consumed from a single consumer
   public void testMessageConcurrencyAfterConsumption() throws Exception
   {
      ClientSessionFactory sf = locator.createSessionFactory();

      ClientSession consumeSession = sf.createSession();
      
      final ClientProducer mainProducer = consumeSession.createProducer(ADDRESS);
      
      consumeSession.createQueue(ADDRESS, QUEUE_NAME);
                  
      ClientConsumer consumer = consumeSession.createConsumer(QUEUE_NAME);
      
      
      
      consumeSession.start();

      Set<ClientSession> sendSessions = new HashSet<ClientSession>();

      final Set<Sender> senders = new HashSet<Sender>();

      final int numSessions = 100;
      
      final int numMessages = 1000;

      for (int i = 0; i < numSessions; i++)
      {
         ClientSession sendSession = sf.createSession();
         
         sendSessions.add(sendSession);

         ClientProducer producer = sendSession.createProducer(ADDRESS);
         
         Sender sender = new Sender(numMessages, producer);
         
         senders.add(sender);
         
         sender.start();
      }
      
      consumer.setMessageHandler(new MessageHandler()
      {
         public void onMessage(ClientMessage message)
         {
            for (Sender sender: senders)
            {
               sender.queue.add(message);
            }
         }
      });
      
      for (int i = 0; i < numMessages; i++)
      {
         byte[] body = RandomUtil.randomBytes(1000);

         ClientMessage message = consumeSession.createMessage(false);
         
         message.getBodyBuffer().writeBytes(body);
         
         mainProducer.send(message);
      }

      for (Sender sender: senders)
      {
         sender.join();
         
         assertFalse(sender.failed);
      }
      
      for (ClientSession sendSession: sendSessions)
      {
         sendSession.close();
      }
      
      consumer.close();
      
      consumeSession.deleteQueue(QUEUE_NAME);
      
      consumeSession.close();

      sf.close();      
   }
   
   private class Sender extends Thread
   {
      private final BlockingQueue<ClientMessage> queue = new LinkedBlockingQueue<ClientMessage>();

      private final ClientProducer producer;
      
      private final int numMessages;

      Sender(final int numMessages, final ClientProducer producer)
      {
         this.numMessages = numMessages;
         
         this.producer = producer;
      }
      
      volatile boolean failed;

      public void run()
      {
         try
         {
            for (int i = 0; i < numMessages; i++)
            {
               ClientMessage msg = queue.take();

               producer.send(msg);
            }
         }
         catch (Exception e)
         {
            log.error("Failed to send message", e);

            failed = true;
         }
      }
   }

}
