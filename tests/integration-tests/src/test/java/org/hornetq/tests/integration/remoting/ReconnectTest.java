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

package org.hornetq.tests.integration.remoting;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.FailoverEventListener;
import org.hornetq.api.core.client.FailoverEventType;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.core.client.SessionFailureListener;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.ServerSession;
import org.hornetq.tests.util.ServiceTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 * A ReconnectSimpleTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ReconnectTest extends ServiceTestBase
{

   @Test
   public void testReconnectNetty() throws Exception
   {
      internalTestReconnect(true);
   }

   @Test
   public void testReconnectInVM() throws Exception
   {
      internalTestReconnect(false);
   }

   public void internalTestReconnect(final boolean isNetty) throws Exception
   {
      final int pingPeriod = 1000;

      HornetQServer server = createServer(false, isNetty);

      server.start();

      ClientSessionInternal session = null;

      try
      {
         ServerLocator locator = createFactory(isNetty);
         locator.setClientFailureCheckPeriod(pingPeriod);
         locator.setRetryInterval(500);
         locator.setRetryIntervalMultiplier(1d);
         locator.setReconnectAttempts(-1);
         locator.setConfirmationWindowSize(1024 * 1024);
         ClientSessionFactory factory = createSessionFactory(locator);


         session = (ClientSessionInternal)factory.createSession();

         final AtomicInteger count = new AtomicInteger(0);

         final CountDownLatch latch = new CountDownLatch(1);

         session.addFailureListener(new SessionFailureListener()
         {

            public void connectionFailed(final HornetQException me, boolean failedOver)
            {
               count.incrementAndGet();
               latch.countDown();
            }

            public void beforeReconnect(final HornetQException exception)
            {
            }

         });

         server.stop();

         Thread.sleep((pingPeriod * 2));

         server.start();

         Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));

         // Some time to let possible loops to occur
         Thread.sleep(500);

         Assert.assertEquals(1, count.get());

         locator.close();
      }
      finally
      {
         try
         {
            session.close();
         }
         catch (Throwable e)
         {
         }

         server.stop();
      }

   }

   @Test
   public void testMetadataAfterReconnectionNetty() throws Exception
   {
      internalMetadataAfterRetry(true);
   }

   @Test
   public void testMetadataAfterReconnectionInVM() throws Exception
   {
      internalMetadataAfterRetry(false);
   }

   public void internalMetadataAfterRetry(final boolean isNetty) throws Exception
   {
      final int pingPeriod = 1000;

      HornetQServer server = createServer(false, isNetty);

      server.start();

      ClientSessionInternal session = null;

      try
      {
         for (int i = 0; i < 100; i++)
         {
            ServerLocator locator = createFactory(isNetty);
            locator.setClientFailureCheckPeriod(pingPeriod);
            locator.setRetryInterval(1);
            locator.setRetryIntervalMultiplier(1d);
            locator.setReconnectAttempts(-1);
            locator.setConfirmationWindowSize(-1);
            ClientSessionFactory factory = createSessionFactory(locator);


            session = (ClientSessionInternal) factory.createSession();

            session.addMetaData("meta1", "meta1");

            ServerSession[] sessions = countMetadata(server, "meta1", 1);
            Assert.assertEquals(1, sessions.length);


            final AtomicInteger count = new AtomicInteger(0);

            final CountDownLatch latch = new CountDownLatch(1);

            session.addFailoverListener(new FailoverEventListener()
            {
               @Override
               public void failoverEvent(FailoverEventType eventType)
               {
                  if (eventType == FailoverEventType.FAILOVER_COMPLETED)
                  {
                     latch.countDown();
                  }
               }
            });

            sessions[0].getRemotingConnection().fail(new HornetQException("failure!"));

            Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));

            sessions = countMetadata(server, "meta1", 1);

            Assert.assertEquals(1, sessions.length);

            locator.close();
         }
      }
      finally
      {
         try
         {
            session.close();
         }
         catch (Throwable e)
         {
         }

         server.stop();
      }

   }

   private ServerSession[] countMetadata(HornetQServer server, String parameter, int expected) throws Exception
   {
      List<ServerSession> sessionList = new LinkedList<ServerSession>();

      for (int i = 0; i < 10 && sessionList.size() != expected; i++)
      {
         sessionList.clear();
         for (ServerSession sess : server.getSessions())
         {
            if (sess.getMetaData(parameter) != null)
            {
               sessionList.add(sess);
            }
         }

         if (sessionList.size() != expected)
         {
            Thread.sleep(100);
         }
      }

      return sessionList.toArray(new ServerSession[sessionList.size()]);
   }

   @Test
   public void testInterruptReconnectNetty() throws Exception
   {
      internalTestInterruptReconnect(true, false);
   }

   @Test
   public void testInterruptReconnectInVM() throws Exception
   {
      internalTestInterruptReconnect(false, false);
   }

   @Test
   public void testInterruptReconnectNettyInterruptMainThread() throws Exception
   {
      internalTestInterruptReconnect(true, true);
   }

   @Test
   public void testInterruptReconnectInVMInterruptMainThread() throws Exception
   {
      internalTestInterruptReconnect(false, true);
   }

   public void internalTestInterruptReconnect(final boolean isNetty, final boolean interruptMainThread) throws Exception
   {
      final int pingPeriod = 1000;

      HornetQServer server = createServer(false, isNetty);

      server.start();

      try
      {
         ServerLocator locator = createFactory(isNetty);
         locator.setClientFailureCheckPeriod(pingPeriod);
         locator.setRetryInterval(500);
         locator.setRetryIntervalMultiplier(1d);
         locator.setReconnectAttempts(-1);
         locator.setConfirmationWindowSize(1024 * 1024);
         ClientSessionFactoryInternal factory = (ClientSessionFactoryInternal)locator.createSessionFactory();

         // One for beforeReconnecto from the Factory, and one for the commit about to be done
         final CountDownLatch latchCommit = new CountDownLatch(2);

         final ArrayList<Thread> threadToBeInterrupted = new ArrayList<Thread>();

         factory.addFailureListener(new SessionFailureListener()
         {

            @Override
            public void connectionFailed(HornetQException exception, boolean failedOver)
            {
            }

            @Override
            public void beforeReconnect(HornetQException exception)
            {
               latchCommit.countDown();
               threadToBeInterrupted.add(Thread.currentThread());
               System.out.println("Thread " + Thread.currentThread() + " reconnecting now");
            }
         });


         final ClientSessionInternal session = (ClientSessionInternal)factory.createSession();

         final AtomicInteger count = new AtomicInteger(0);

         final CountDownLatch latch = new CountDownLatch(1);

         session.addFailureListener(new SessionFailureListener()
         {

            public void connectionFailed(final HornetQException me, boolean failedOver)
            {
               count.incrementAndGet();
               latch.countDown();
            }

            public void beforeReconnect(final HornetQException exception)
            {
            }

         });

         server.stop();

         Thread tcommitt = new Thread()
         {
            public void run()
            {
               latchCommit.countDown();
               try
               {
                  session.commit();
               }
               catch (HornetQException e)
               {
                  e.printStackTrace();
               }
            }
         };

         tcommitt.start();
         assertTrue(latchCommit.await(10, TimeUnit.SECONDS));

         // There should be only one thread
         assertEquals(1, threadToBeInterrupted.size());

         if (interruptMainThread)
         {
            tcommitt.interrupt();
         }
         else
         {
            for (Thread tint: threadToBeInterrupted)
            {
               tint.interrupt();
            }
         }
         tcommitt.join(5000);

         assertFalse(tcommitt.isAlive());

         locator.close();
      }
      finally
      {
      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
