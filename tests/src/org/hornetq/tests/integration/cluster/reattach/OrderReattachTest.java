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

package org.hornetq.tests.integration.cluster.reattach;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;
import junit.framework.TestSuite;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.MessageHandler;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.protocol.core.impl.RemotingConnectionImpl;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.jms.client.HornetQTextMessage;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.concurrent.LinkedBlockingDeque;

/**
 * A OrderReattachTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class OrderReattachTest extends ServiceTestBase
{

   // Disabled for now... under investigation (Clebert)
   public static TestSuite suite()
   {
      TestSuite suite = new TestSuite();

      return suite;
   }

   // Constants -----------------------------------------------------

   final SimpleString ADDRESS = new SimpleString("address");

   // Attributes ----------------------------------------------------
   private final Logger log = Logger.getLogger(this.getClass());

   private HornetQServer server;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testOrderOnSendInVM() throws Throwable
   {
      for (int i = 0; i < 500; i++)
      {
         log.info("#" + getName() + " # " + i);
         doTestOrderOnSend(false);
         tearDown();
         setUp();
      }
   }

   public void doTestOrderOnSend(final boolean isNetty) throws Throwable
   {
      server = createServer(false, isNetty);

      server.start();

      ClientSessionFactory sf = createFactory(isNetty);
      sf.setReconnectAttempts(-1);
      sf.setConfirmationWindowSize(100 * 1024 * 1024);
      sf.setBlockOnNonDurableSend(false);
      sf.setBlockOnAcknowledge(false);

      final ClientSession session = sf.createSession(false, true, true);

      final LinkedBlockingDeque<Boolean> failureQueue = new LinkedBlockingDeque<Boolean>();

      final CountDownLatch ready = new CountDownLatch(1);

      
      // this test will use a queue. Whenever the test wants a failure.. it can just send TRUE to failureQueue
      // This Thread will be reading the queue
      Thread failer = new Thread()
      {
         @Override
         public void run()
         {
            ready.countDown();
            while (true)
            {
               try
               {
                  Boolean poll = false;
                  try
                  {
                     poll = failureQueue.poll(60, TimeUnit.SECONDS);
                  }
                  catch (InterruptedException e)
                  {
                     e.printStackTrace();
                     break;
                  }

                  Thread.sleep(1);

                  final RemotingConnectionImpl conn = (RemotingConnectionImpl)((ClientSessionInternal)session).getConnection();

                  // True means... fail session
                  if (poll)
                  {
                     conn.fail(new HornetQException(HornetQException.NOT_CONNECTED, "poop"));
                  }
                  else
                  {
                     // false means... finish thread
                     break;
                  }
               }
               catch (Exception e)
               {
                  e.printStackTrace();
               }
            }
         }
      };

      failer.start();

      ready.await();

      try
      {
         doSend2(1, sf, failureQueue);
      }
      finally
      {
         try
         {
            session.close();
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }

         try
         {
            sf.close();
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }

         failureQueue.put(false);

         failer.join();
      }

   }

   public void doSend2(final int order, final ClientSessionFactory sf, final LinkedBlockingDeque<Boolean> failureQueue) throws Exception
   {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false);

      final int numMessages = 500;

      final int numSessions = 100;

      Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString("sub" + i);

         // failureQueue.push(true);

         ClientSession sessConsume = sf.createSession(false, true, true);

         sessConsume.createQueue(ADDRESS, subName, null, false);

         ClientConsumer consumer = sessConsume.createConsumer(subName);

         consumers.add(consumer);

         sessions.add(sessConsume);
      }

      ClientSession sessSend = sf.createSession(false, true, true);

      ClientProducer producer = sessSend.createProducer(ADDRESS);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = sessSend.createMessage(HornetQTextMessage.TYPE,
                                                        false,
                                                        0,
                                                        System.currentTimeMillis(),
                                                        (byte)1);

         if (i % 10 == 0)
         {
            // failureQueue.push(true);
         }
         message.putIntProperty(new SimpleString("count"), i);
         producer.send(message);
      }

      for (ClientSession session : sessions)
      {
         session.start();
      }

      class MyHandler implements MessageHandler
      {
         final CountDownLatch latch = new CountDownLatch(1);

         volatile int count;
         
         Exception failure;

         public void onMessage(final ClientMessage message)
         {
            if (count >= numMessages)
            {
               failure = new Exception("too many messages");
               latch.countDown();
            }
            
            if (message.getIntProperty("count") != count)
            {
               failure = new Exception("counter " + count + " was not as expected (" + message.getIntProperty("count") + ")");
               log.warn("Failure on receiving message ", failure);
               failure.printStackTrace();
               latch.countDown();
            }

            count++;

            if (count % 100 == 0)
            {
               failureQueue.push(true);
            }

            if (count == numMessages)
            {
               latch.countDown();
            }
         }
      }

      Set<MyHandler> handlers = new HashSet<MyHandler>();

      for (ClientConsumer consumer : consumers)
      {
         MyHandler handler = new MyHandler();

         consumer.setMessageHandler(handler);

         handlers.add(handler);
      }

      for (MyHandler handler : handlers)
      {
         boolean ok = handler.latch.await(60000, TimeUnit.MILLISECONDS);

         Assert.assertTrue(ok);
         
         if (handler.failure != null)
         {
            throw handler.failure;
         }
      }

      // failureQueue.push(true);

      sessSend.close();

      for (ClientSession session : sessions)
      {
         // failureQueue.push(true);
         session.close();
      }

      for (int i = 0; i < numSessions; i++)
      {

         failureQueue.push(true);

         SimpleString subName = new SimpleString("sub" + i);

         s.deleteQueue(subName);
      }

      s.close();

      long end = System.currentTimeMillis();

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
   }

   @Override
   protected void tearDown() throws Exception
   {
      if (server != null && server.isStarted())
      {
         server.stop();
      }

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}