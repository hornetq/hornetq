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

package org.hornetq.tests.integration.cluster.failover;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.*;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.client.impl.DelegatingSession;
import org.hornetq.core.logging.Logger;
import org.hornetq.spi.core.protocol.RemotingConnection;

/**
 * A MultiThreadFailoverTest
 * 
 * Test Failover where failure is prompted by another thread
 *
 * @author Tim Fox
 *
 *
 */
public class AsynchronousFailoverTest extends FailoverTestBase
{
   private static final Logger log = Logger.getLogger(AsynchronousFailoverTest.class);

   private volatile MyListener listener;

   private volatile ClientSessionFactoryInternal sf;

   private Object lockFail = new Object();

   class MyListener implements SessionFailureListener
   {
      CountDownLatch latch = new CountDownLatch(1);

      public void connectionFailed(final HornetQException me, boolean failedOver)
      {
         latch.countDown();
      }

      public void beforeReconnect(final HornetQException me)
      {
      }
   }

   public void testNonTransactional() throws Throwable
   {
      runTest(new TestRunner()
      {
         public void run()
         {
            try
            {
               doTestNonTransactional(this);
            }
            catch (Throwable e)
            {
               AsynchronousFailoverTest.log.error("Test failed", e);
               addException(e);
            }
         }
      });
   }

   public void testTransactional() throws Throwable
   {
      runTest(new TestRunner()
      {
         volatile boolean running = false;

         public void run()
         {
            try
            {
               assertFalse(running);
               running = true;
               try
               {
                  doTestTransactional(this);
               }
               finally
               {
                  running = false;
               }
            }
            catch (Throwable e)
            {
               AsynchronousFailoverTest.log.error("Test failed", e);
               addException(e);
            }
         }
      });
   }

   abstract class TestRunner implements Runnable
   {
      volatile boolean failed;

      ArrayList<Throwable> errors = new ArrayList<Throwable>();

      boolean isFailed()
      {
         return failed;
      }

      void setFailed()
      {
         failed = true;
      }

      void reset()
      {
         failed = false;
      }

      synchronized void addException(Throwable e)
      {
         errors.add(e);
      }

      void checkForExceptions() throws Throwable
      {
         if (errors.size() > 0)
         {
            log.warn("Exceptions on test:");
            for (Throwable e : errors)
            {
               log.warn(e.getMessage(), e);
            }
            // throwing the first error that happened on the Runnable
            throw errors.get(0);
         }

      }

   }

   private void runTest(final TestRunner runnable) throws Throwable
   {
      final int numIts = 1;

      DelegatingSession.debug = true;

      try
      {
         for (int i = 0; i < numIts; i++)
         {
            AsynchronousFailoverTest.log.info("Iteration " + i);
            ServerLocator locator = getServerLocator();
            locator.setBlockOnNonDurableSend(true);
            locator.setBlockOnDurableSend(true);
            locator.setReconnectAttempts(-1);
            sf = (ClientSessionFactoryInternal) createSessionFactoryAndWaitForTopology(locator, 2);


            ClientSession createSession = sf.createSession(true, true);

            createSession.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);

            RemotingConnection conn = ((ClientSessionInternal)createSession).getConnection();

            Thread t = new Thread(runnable);

            t.setName("MainTEST");

            t.start();

            long randomDelay = (long)(2000 * Math.random());

            AsynchronousFailoverTest.log.info("Sleeping " + randomDelay);

            Thread.sleep(randomDelay);

            AsynchronousFailoverTest.log.info("Failing asynchronously");

            MyListener listener = this.listener;

            // Simulate failure on connection
            synchronized (lockFail)
            {
               crash((ClientSession) createSession);
            }

            /*if (listener != null)
            {
               boolean ok = listener.latch.await(10000, TimeUnit.MILLISECONDS);

               Assert.assertTrue(ok);
            }*/

            runnable.setFailed();

            AsynchronousFailoverTest.log.info("Fail complete");

            t.join();

            runnable.checkForExceptions();

            createSession.close();

            if (sf.numSessions() != 0)
            {
               DelegatingSession.dumpSessionCreationStacks();
            }

            Assert.assertEquals(0, sf.numSessions());

            locator.close();
            
            Assert.assertEquals(0, sf.numConnections());

            if (i != numIts - 1)
            {
               tearDown();
               runnable.checkForExceptions();
               runnable.reset();
               setUp();
            }
         }
      }
      finally
      {
         DelegatingSession.debug = false;
      }
   }

   private void doTestNonTransactional(final TestRunner runner) throws Exception
   {
      while (!runner.isFailed())
      {
         AsynchronousFailoverTest.log.info("looping");

         ClientSession session = sf.createSession(true, true, 0);

         MyListener listener = new MyListener();

         session.addFailureListener(listener);

         this.listener = listener;

         ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

         final int numMessages = 1000;

         for (int i = 0; i < numMessages; i++)
         {
            boolean retry = false;
            do
            {
               try
               {
                  ClientMessage message = session.createMessage(true);

                  message.getBodyBuffer().writeString("message" + i);

                  message.putIntProperty("counter", i);

                  producer.send(message);

                  retry = false;
               }
               catch (HornetQException e)
               {
                  AsynchronousFailoverTest.log.info("exception when sending message with counter " + i);
                  if(e.getCode() != HornetQException.UNBLOCKED)
                  {
                     e.printStackTrace();
                  }
                  Assert.assertEquals(e.getCode(), HornetQException.UNBLOCKED);

                  retry = true;
               }
            }
            while (retry);
         }

         // create the consumer with retry if failover occurs during createConsumer call
         ClientConsumer consumer = null;
         boolean retry = false;
         do
         {
            try
            {
               consumer = session.createConsumer(FailoverTestBase.ADDRESS);

               retry = false;
            }
            catch (HornetQException e)
            {
               AsynchronousFailoverTest.log.info("exception when creating consumer");
               Assert.assertEquals(e.getCode(), HornetQException.UNBLOCKED);

               retry = true;
            }
         }
         while (retry);

         session.start();

         List<Integer> counts = new ArrayList<Integer>(1000);
         int lastCount = -1;
         boolean counterGap = false;
         while (true)
         {
            ClientMessage message = consumer.receive(500);

            if (message == null)
            {
               break;
            }

            // messages must remain ordered but there could be a "jump" if messages
            // are missing or duplicated
            int count = message.getIntProperty("counter");
            counts.add(count);
            if (count != lastCount + 1)
            {
               if (counterGap)
               {
                  Assert.fail("got a another counter gap at " + count + ": " + counts);
               }
               else
               {
                  if (lastCount != -1)
                  {
                     AsynchronousFailoverTest.log.info("got first counter gap at " + count);
                     counterGap = true;
                  }
               }
            }

            lastCount = count;

            message.acknowledge();
         }

         session.close();

         this.listener = null;
      }
   }

   private void doTestTransactional(final TestRunner runner) throws Exception
   {
      // For duplication detection
      int executionId = 0;

      while (!runner.isFailed())
      {
         ClientSession session = null;

         executionId++;

         try
         {

            MyListener listener = new MyListener();

            this.listener = listener;
            boolean retry = false;

            final int numMessages = 1000;

            session = sf.createSession(false, false);

            session.addFailureListener(listener);

            do
            {
               try
               {
                  ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

                  for (int i = 0; i < numMessages; i++)
                  {
                     ClientMessage message = session.createMessage(true);

                     message.getBodyBuffer().writeString("message" + i);

                     message.putIntProperty("counter", i);

                     message.putStringProperty(Message.HDR_DUPLICATE_DETECTION_ID, new SimpleString("id:" + i +
                                                                                                    ",exec:" +
                                                                                                    executionId));

                     producer.send(message);
                  }

                  session.commit();

                  retry = false;
               }
               catch (HornetQException e)
               {
                  if (e.getCode() == HornetQException.TRANSACTION_ROLLED_BACK || e.getCode() == HornetQException.UNBLOCKED)
                  {
                     // OK
                     retry = true;
                  }
                  else
                  {
                     throw e;
                  }
               }
            }
            while (retry);

            
            
            boolean blocked = false;

            retry = false;
            
            ClientConsumer consumer = null; 
            do
            {
               ArrayList<Integer> msgs = new ArrayList<Integer>();
               try
               {
                  if (consumer == null)
                  {
                     consumer = session.createConsumer(FailoverTestBase.ADDRESS);
                     session.start();
                  }

                  for (int i = 0; i < numMessages; i++)
                  {
                     ClientMessage message = consumer.receive(500);
                     if (message == null)
                     {
                        break;
                     }

                     int count = message.getIntProperty("counter");

                     msgs.add(count);

                     message.acknowledge();
                  }

                  session.commit();
                  
                  if (blocked)
                  {
                     assertTrue("msgs.size is expected to be 0 or "  + numMessages + " but it was " + msgs.size(), msgs.size() == 0 || msgs.size() == numMessages);
                  }
                  else
                  {
                     assertTrue("msgs.size is expected to be "  + numMessages  + " but it was " + msgs.size(), msgs.size() == numMessages);
                  }

                  int i = 0;
                  for (Integer msg : msgs)
                  {
                     assertEquals(i++, (int)msg);
                  }

                  retry = false;
                  blocked = false;
               }
               catch (HornetQException e)
               {
                  if (e.getCode() == HornetQException.TRANSACTION_ROLLED_BACK)
                  {
                     // TODO: https://jira.jboss.org/jira/browse/HORNETQ-369
                     // ATM RolledBack exception is being called with the transaction is committed.
                     // the test will fail if you remove this next line
                     blocked = true;
                  }
                  else if (e.getCode() == HornetQException.UNBLOCKED)
                  {
                     // TODO: https://jira.jboss.org/jira/browse/HORNETQ-369
                     // This part of the test is never being called.
                     blocked = true;
                  }

                  if (e.getCode() == HornetQException.UNBLOCKED || e.getCode() == HornetQException.TRANSACTION_ROLLED_BACK)
                  {
                     retry = true;
                  }
                  else
                  {
                     throw e;
                  }
               }
            }
            while (retry);
         }
         finally
         {
            if (session != null)
            {
               session.close();
            }
         }

         listener = null;
      }
   }

   @Override
   protected TransportConfiguration getAcceptorTransportConfiguration(final boolean live)
   {
      return getInVMTransportAcceptorConfiguration(live);
   }

   @Override
   protected TransportConfiguration getConnectorTransportConfiguration(final boolean live)
   {
      return getInVMConnectorTransportConfiguration(live);
   }

}
