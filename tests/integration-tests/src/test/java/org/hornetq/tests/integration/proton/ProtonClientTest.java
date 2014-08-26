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
package org.hornetq.tests.integration.proton;

import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.protocol.proton.client.AMQPClientProtocolManagerFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;
import org.junit.Before;
import org.junit.Test;

/**
 * A NettyConsumerTest
 * <p/>
 * <p/>
 * Note: To better Debug this, define a System variable PN_TRACE_FRM=true
 * you can do that on either as a system property on your shell or on your IDE on the running settings
 * For some reason -DPN_TRACE_FRM=true doesn't work.. it has to be a system property
 *
 * @author clebertsuconic
 */
public class ProtonClientTest extends ServiceTestBase
{
   private HornetQServer server;

   private final SimpleString QUEUE = new SimpleString("ConsumerTestQueue");

   private ServerLocator locatorProton;

   private ServerLocator locatorCore;

   protected boolean isNetty()
   {
      return true;
   }

   @Before
   @Override
   public void setUp() throws Exception
   {
      super.setUp();

      server = createServer(true, isNetty());

      server.start();

      server.createQueue(SimpleString.toSimpleString("AD1"), SimpleString.toSimpleString("Q1"), null, true, false);
      server.createQueue(SimpleString.toSimpleString("AD1"), SimpleString.toSimpleString("AD1"), SimpleString.toSimpleString("IdontExist=false"), true, false);

      locatorProton = createFactory(isNetty());
      locatorProton.setProtocolManagerFactory(new AMQPClientProtocolManagerFactory());

      locatorCore = createFactory(isNetty());
   }


   @Test
   public void testSendProtonConsumeCore() throws Exception
   {
      final int numMessages = 1000;
      ClientSessionFactory factoryCore = locatorCore.createSessionFactory();
      ClientSession sessionCore = factoryCore.createSession();

      ClientSessionFactory factoryProton = locatorProton.createSessionFactory();
      ClientSession sessionProton = factoryProton.createSession();
      ClientProducer producerProton = sessionProton.createProducer("AD1");
      final ClientConsumer consumerCore = sessionCore.createConsumer("Q1");
      sessionCore.start();
      final AtomicInteger rec = new AtomicInteger(0);
      Thread t = new Thread(new Runnable()
      {
         @Override
         public void run()
         {
            for (int i = 0; i < numMessages; i++)
            {
               ClientMessage msg = null;
               try
               {
                  msg = consumerCore.receive(10000);
                  if (msg != null)
                  {
                     rec.incrementAndGet();
                  }

                  msg.acknowledge();
               }
               catch (Throwable e)
               {
                  e.printStackTrace();
               }
            }
            System.out.println("rec = " + rec);
         }
      });
      t.start();
      for (int i = 0; i < numMessages; i++)
      {
         producerProton.send(sessionProton.createMessage(true));
      }
      t.join();

      assertEquals(numMessages, rec.get());

      assertEquals(numMessages, rec.get());
      System.out.println("********* Received " + rec);


   }



   @Test
   public void testSendProtonConsumeProton() throws Exception
   {
      final int numMessages = 1000;
      ClientSessionFactory factoryConsumer = locatorProton.createSessionFactory();
      ClientSession sessionConsumer = factoryConsumer.createSession();

      ClientSessionFactory factoryProton = locatorProton.createSessionFactory();
      ClientSession sessionProton = factoryProton.createSession();
      ClientProducer producerProton = sessionProton.createProducer("AD1");

      final ClientConsumer consumerCore = sessionConsumer.createConsumer("Q1");
      sessionConsumer.start();
      final AtomicInteger rec = new AtomicInteger(0);
      Thread t = new Thread(new Runnable()
      {
         @Override
         public void run()
         {
            for (int i = 0; i < numMessages; i++)
            {
               ClientMessage msg = null;
               try
               {
                  msg = consumerCore.receive(10000);
                  if (msg != null)
                  {
                     rec.incrementAndGet();
                  }
                  msg.acknowledge();
               }
               catch (Throwable e)
               {
                  e.printStackTrace();
               }
            }
            System.out.println("rec = " + rec);
         }
      });
      t.start();
      for (int i = 0; i < numMessages; i++)
      {
         producerProton.send(sessionProton.createMessage(true));
      }
      t.join();

      assertEquals(rec.get(), numMessages);


   }


   @Test
   public void testSendProtonConsumeProton2() throws Exception
   {
      final int numMessages = 100;
      locatorProton.setConsumerWindowSize(10);
      ClientSessionFactory factoryConsumer = locatorProton.createSessionFactory();
      ClientSession sessionConsumer = factoryConsumer.createSession();

      ClientSessionFactory factoryProton = locatorProton.createSessionFactory();
      ClientSession sessionProton = factoryProton.createSession();
      ClientProducer producerProton = sessionProton.createProducer("AD1");

      for (int i = 0; i < numMessages; i++)
      {
         producerProton.send(sessionProton.createMessage(true));
      }
      factoryProton.close();
      final AtomicInteger rec = new AtomicInteger(0);

      final ClientConsumer consumerCore = sessionConsumer.createConsumer("Q1");
      sessionConsumer.start();
      Thread t = new Thread(new Runnable()
      {
         @Override
         public void run()
         {
            for (int i = 0; i < numMessages; i++)
            {
               ClientMessage msg = null;
               try
               {
                  msg = consumerCore.receive(10000);
                  System.out.println("xxxxxx  Msg " + msg);
                  if (msg != null)
                  {
                     rec.incrementAndGet();
                  }
                  msg.acknowledge();
               }
               catch (Throwable e)
               {
                  e.printStackTrace();
               }
            }
            System.out.println("rec = " + rec);
         }
      });
      t.start();
      t.join();

      assertEquals(numMessages, rec.get());


   }


   // Just send is useful for debugging the server
   @Test
   public void testJustSend() throws Exception
   {
      final int numMessages = 100;
      ClientSessionFactory factoryProton = locatorProton.createSessionFactory();
      ClientSession sessionDead = factoryProton.createSession();
      ClientSession sessionProton = factoryProton.createSession();
      ClientProducer producerProton = sessionProton.createProducer("AD1");

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage msg = sessionProton.createMessage(true);
         msg.putStringProperty("tst", "tstsdflkj sfdlkja d;fklaj salfjh asldkfhj alskdfhj alksjdfh alksjdfh alksjdf alkshjf klasjhdf lkasjhdf klahjsdf klasjhdf klajshdjkjjdlkjshdflkhsdf s;dflkja fd;ljas df;lkja sd;lfj asdf;lkj asd");
         producerProton.send(sessionProton.createMessage(true));
      }
   }


}
