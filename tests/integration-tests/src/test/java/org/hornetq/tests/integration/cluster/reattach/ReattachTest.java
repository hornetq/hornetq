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

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.*;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.impl.invm.InVMConnector;
import org.hornetq.core.remoting.impl.invm.InVMRegistry;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.jms.client.HornetQTextMessage;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * 
 * A ReattachTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 4 Nov 2008 16:54:50
 *
 *
 */
public class ReattachTest extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(ReattachTest.class);

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private static final SimpleString ADDRESS = new SimpleString("FailoverTestAddress");

   private HornetQServer service;

   private ServerLocator locator;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   /*
    * Test failure on connection, but server is still up so should immediately reconnect
    */
   public void testImmediateReattach() throws Exception
   {
      final long retryInterval = 500;

      final double retryMultiplier = 1d;

      final int reconnectAttempts = 1;

      locator.setRetryInterval(retryInterval);
      locator.setRetryIntervalMultiplier(retryMultiplier);
      locator.setReconnectAttempts(reconnectAttempts);
      locator.setConfirmationWindowSize(1024 * 1024);
      ClientSessionFactoryInternal sf = (ClientSessionFactoryInternal) locator.createSessionFactory();

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ReattachTest.ADDRESS, ReattachTest.ADDRESS, null, false);

      final int numIterations = 10;

      for (int j = 0; j < numIterations; j++)
      {
         ClientProducer producer = session.createProducer(ReattachTest.ADDRESS);

         final int numMessages = 100;

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = session.createMessage(HornetQTextMessage.TYPE,
                                                                false,
                                                                0,
                                                                System.currentTimeMillis(),
                                                                (byte)1);
            message.putIntProperty(new SimpleString("count"), i);
            message.getBodyBuffer().writeString("aardvarks");
            producer.send(message);
         }

         ClientConsumer consumer = session.createConsumer(ReattachTest.ADDRESS);

         RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

         conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

         session.start();

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = consumer.receive(500);

            Assert.assertNotNull(message);

            Assert.assertEquals("aardvarks", message.getBodyBuffer().readString());

            Assert.assertEquals(i, message.getObjectProperty(new SimpleString("count")));

            message.acknowledge();
         }

         ClientMessage message = consumer.receiveImmediate();

         Assert.assertNull(message);

         producer.close();

         consumer.close();
      }

      session.close();

      sf.close();
   }

   /*
    * Test failure on connection, simulate failure to create connection for a while, then 
    * allow connection to be recreated
    */
   public void testDelayedReattach() throws Exception
   {
      final long retryInterval = 500;

      final double retryMultiplier = 1d;

      final int reconnectAttempts = -1;

      locator.setRetryInterval(retryInterval);
      locator.setRetryIntervalMultiplier(retryMultiplier);
      locator.setReconnectAttempts(reconnectAttempts);
      locator.setConfirmationWindowSize(1024 * 1024);
      ClientSessionFactoryInternal sf = (ClientSessionFactoryInternal) locator.createSessionFactory();

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ReattachTest.ADDRESS, ReattachTest.ADDRESS, null, false);

      ClientProducer producer = session.createProducer(ReattachTest.ADDRESS);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(HornetQTextMessage.TYPE,
                                                             false,
                                                             0,
                                                             System.currentTimeMillis(),
                                                             (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBodyBuffer().writeString("aardvarks");
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(ReattachTest.ADDRESS);

      InVMConnector.failOnCreateConnection = true;

      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      Thread t = new Thread()
      {
         @Override
         public void run()
         {
            try
            {
               Thread.sleep(retryInterval * 3);
            }
            catch (InterruptedException ignore)
            {
            }

            InVMConnector.failOnCreateConnection = false;
         }
      };

      t.start();

      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(500);

         Assert.assertNotNull(message);

         Assert.assertEquals("aardvarks", message.getBodyBuffer().readString());

         Assert.assertEquals(i, message.getIntProperty("count").intValue());

         message.acknowledge();
      }

      ClientMessage message = consumer.receiveImmediate();

      Assert.assertNull(message);

      session.close();

      sf.close();

      t.join();
   }

   // Test an async (e.g. pinger) failure coming in while a connection manager is already reconnecting
   public void testAsyncFailureWhileReattaching() throws Exception
   {
      final long retryInterval = 500;

      final double retryMultiplier = 1d;

      final int reconnectAttempts = -1;

      final long asyncFailDelay = 2000;

      locator.setRetryInterval(retryInterval);
      locator.setRetryIntervalMultiplier(retryMultiplier);
      locator.setReconnectAttempts(reconnectAttempts);
      locator.setConfirmationWindowSize(1024 * 1024);
      ClientSessionFactoryInternal sf = (ClientSessionFactoryInternal) locator.createSessionFactory();

      ClientSession session = sf.createSession(false, true, true);

      ClientSession session2 = sf.createSession(false, true, true);

      class MyFailureListener implements SessionFailureListener
      {
         volatile boolean failed;

         public void connectionFailed(final HornetQException me, boolean failedOver)
         {
            failed = true;
         }

         public void beforeReconnect(final HornetQException exception)
         {
         }
      }

      MyFailureListener listener = new MyFailureListener();

      session2.addFailureListener(listener);

      session.createQueue(ReattachTest.ADDRESS, ReattachTest.ADDRESS, null, false);

      ClientProducer producer = session.createProducer(ReattachTest.ADDRESS);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(HornetQTextMessage.TYPE,
                                                             false,
                                                             0,
                                                             System.currentTimeMillis(),
                                                             (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBodyBuffer().writeString("aardvarks");
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(ReattachTest.ADDRESS);

      InVMConnector.numberOfFailures = 10;
      InVMConnector.failOnCreateConnection = true;

      final RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      final RemotingConnection conn2 = ((ClientSessionInternal)session2).getConnection();

      Thread t = new Thread()
      {
         @Override
         public void run()
         {
            try
            {
               Thread.sleep(asyncFailDelay);
            }
            catch (InterruptedException ignore)
            {
            }

            conn2.fail(new HornetQException(HornetQException.NOT_CONNECTED, "Did not receive pong from server"));
         }
      };

      t.start();

      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      Assert.assertTrue(listener.failed);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(500);

         Assert.assertNotNull(message);

         Assert.assertEquals("aardvarks", message.getBodyBuffer().readString());

         Assert.assertEquals(i, message.getIntProperty("count").intValue());

         message.acknowledge();
      }

      ClientMessage message = consumer.receiveImmediate();

      Assert.assertNull(message);

      session.close();

      session2.close();

      sf.close();

      t.join();
   }

   public void testReattachAttemptsFailsToReconnect() throws Exception
   {
      final long retryInterval = 500;

      final double retryMultiplier = 1d;

      final int reconnectAttempts = 3;

      locator.setRetryInterval(retryInterval);
      locator.setRetryIntervalMultiplier(retryMultiplier);
      locator.setReconnectAttempts(reconnectAttempts);
      locator.setConfirmationWindowSize(1024 * 1024);
      ClientSessionFactoryInternal sf = (ClientSessionFactoryInternal) locator.createSessionFactory();

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ReattachTest.ADDRESS, ReattachTest.ADDRESS, null, false);

      ClientProducer producer = session.createProducer(ReattachTest.ADDRESS);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(HornetQTextMessage.TYPE,
                                                             false,
                                                             0,
                                                             System.currentTimeMillis(),
                                                             (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBodyBuffer().writeString("aardvarks");
         producer.send(message);
      }

      session.createConsumer(ReattachTest.ADDRESS);

      InVMConnector.failOnCreateConnection = true;

      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      // Sleep for longer than max retries so should fail to reconnect

      Thread t = new Thread()
      {
         @Override
         public void run()
         {
            try
            {
               Thread.sleep(retryInterval * (reconnectAttempts + 1));
            }
            catch (InterruptedException ignore)
            {
            }

            InVMConnector.failOnCreateConnection = false;
         }
      };

      t.start();

      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      // Should throw exception since didn't reconnect

      try
      {
         session.start();

         Assert.fail("Should throw exception");
      }
      catch (HornetQException e)
      {
         Assert.assertEquals(HornetQException.OBJECT_CLOSED, e.getCode());
      }

      session.close();

      sf.close();

      t.join();
   }

   public void testCreateSessionFailAfterSendSeveralThreads() throws Throwable
   {

      Timer timer = new Timer();
      ClientSession session = null;

      try
      {

         final long retryInterval = 50;

         final double retryMultiplier = 1d;

         final int reconnectAttempts = -1;

         locator.setRetryInterval(retryInterval);
         locator.setRetryIntervalMultiplier(retryMultiplier);
         locator.setReconnectAttempts(reconnectAttempts);
         locator.setConfirmationWindowSize(1024 * 1024);
         final ClientSessionFactoryInternal sf = (ClientSessionFactoryInternal) locator.createSessionFactory();

         session = sf.createSession();

         final RemotingConnection connFailure = ((ClientSessionInternal)session).getConnection();

         int numberOfThreads = 100;
         final int numberOfSessionsToCreate = 10;

         final CountDownLatch alignLatch = new CountDownLatch(numberOfThreads);
         final CountDownLatch startFlag = new CountDownLatch(1);

         class CreateSessionThread extends Thread
         {
            Throwable failure;

            @Override
            public void run()
            {
               try
               {
                  alignLatch.countDown();
                  startFlag.await();
                  for (int i = 0; i < numberOfSessionsToCreate; i++)
                  {
                     Thread.yield();
                     ClientSession session = sf.createSession(false, true, true);

                     session.close();
                  }
               }
               catch (Throwable e)
               {
                  e.printStackTrace();
                  failure = e;
               }
            }
         }

         CreateSessionThread threads[] = new CreateSessionThread[numberOfThreads];
         for (int i = 0; i < numberOfThreads; i++)
         {
            threads[i] = new CreateSessionThread();
            threads[i].start();
         }

         alignLatch.await();

         timer.schedule(new TimerTask()
         {
            @Override
            public void run()
            {
               try
               {
                  connFailure.fail(new HornetQException(HornetQException.NOT_CONNECTED));
               }
               catch (Exception e)
               {
                  ReattachTest.log.warn("Error on the timer " + e);
               }
            }

         }, 10, 10);

         startFlag.countDown();

         Throwable failure = null;

         for (CreateSessionThread thread : threads)
         {
            thread.join();
            if (thread.failure != null)
            {
               System.out.println("Thread " + thread.getName() + " failed - " + thread.failure);
               failure = thread.failure;
            }
         }

         if (failure != null)
         {
            throw failure;
         }

         sf.close();

      }
      finally
      {
         timer.cancel();

         if (session != null)
         {
            session.close();
         }
      }
   }

   public void testCreateSessionFailBeforeSendSeveralThreads() throws Throwable
   {
      final long retryInterval = 500;

      final double retryMultiplier = 1d;

      final int reconnectAttempts = -1;

      locator.setRetryInterval(retryInterval);
      locator.setRetryIntervalMultiplier(retryMultiplier);
      locator.setReconnectAttempts(reconnectAttempts);
      locator.setConfirmationWindowSize(1024 * 1024);
      final ClientSessionFactoryInternal sf = (ClientSessionFactoryInternal) locator.createSessionFactory();

      InVMConnector.failOnCreateConnection = true;

      int numberOfThreads = 100;

      final CountDownLatch alignLatch = new CountDownLatch(numberOfThreads);
      final CountDownLatch startFlag = new CountDownLatch(1);

      class CreateSessionThread extends Thread
      {
         Throwable failure;

         @Override
         public void run()
         {
            try
            {
               alignLatch.countDown();
               startFlag.await();
               ClientSession session = sf.createSession(false, true, true);

               session.close();
            }
            catch (Throwable e)
            {
               e.printStackTrace();
               failure = e;
            }
         }
      }

      CreateSessionThread threads[] = new CreateSessionThread[numberOfThreads];
      for (int i = 0; i < numberOfThreads; i++)
      {
         threads[i] = new CreateSessionThread();
         threads[i].start();
      }

      // Sleep 3 times retryInterval, so it should at least have 3 retries

      Thread t = new Thread()
      {
         @Override
         public void run()
         {
            try
            {
               Thread.sleep(retryInterval * 3);
            }
            catch (InterruptedException ignore)
            {
            }

            InVMConnector.failOnCreateConnection = false;
         }
      };

      alignLatch.await();

      t.start();

      startFlag.countDown();

      Throwable failure = null;

      for (CreateSessionThread thread : threads)
      {
         thread.join();
         if (thread.failure != null)
         {
            System.out.println("Thread " + thread.getName() + " failed - " + thread.failure);
            failure = thread.failure;
         }
      }

      if (failure != null)
      {
         throw failure;
      }

      sf.close();

      t.join();
   }

   public void testCreateQueue() throws Exception
   {
      final long retryInterval = 50;

      final double retryMultiplier = 1d;

      final int reconnectAttempts = -1;

      locator.setRetryInterval(retryInterval);
      locator.setRetryIntervalMultiplier(retryMultiplier);
      locator.setReconnectAttempts(reconnectAttempts);
      locator.setConfirmationWindowSize(1024 * 1024);
      ClientSessionFactoryInternal sf = (ClientSessionFactoryInternal) locator.createSessionFactory();

      ClientSession session = sf.createSession(false, true, true);

      // Sleep 3 times retryInterval, so it should at least have 3 retries

      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      InVMConnector.failOnCreateConnection = false;

      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      Thread t = new Thread()
      {
         @Override
         public void run()
         {
            try
            {
               Thread.sleep(retryInterval * 3);
            }
            catch (InterruptedException ignore)
            {
            }

            InVMConnector.failOnCreateConnection = false;
         }
      };

      t.start();

      for (int i = 0; i < 10; i++)
      {
         session.createQueue("address", "queue" + i);
      }

      //
      // InVMConnector.failOnCreateConnection = true;

      //
      // //Should throw exception since didn't reconnect
      //      
      // try
      // {
      // session.start();
      //         
      // fail("Should throw exception");
      // }
      // catch (HornetQException e)
      // {
      // assertEquals(HornetQException.OBJECT_CLOSED, e.getCode());
      // }

      session.close();

      sf.close();

      t.join();
   }

   public void testReattachAttemptsSucceedsInReconnecting() throws Exception
   {
      final long retryInterval = 500;

      final double retryMultiplier = 1d;

      final int reconnectAttempts = 10;

      locator.setRetryInterval(retryInterval);
      locator.setRetryIntervalMultiplier(retryMultiplier);
      locator.setReconnectAttempts(reconnectAttempts);
      locator.setConfirmationWindowSize(1024 * 1024);
      ClientSessionFactoryInternal sf = (ClientSessionFactoryInternal) locator.createSessionFactory();

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ReattachTest.ADDRESS, ReattachTest.ADDRESS, null, false);

      ClientProducer producer = session.createProducer(ReattachTest.ADDRESS);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(HornetQTextMessage.TYPE,
                                                             false,
                                                             0,
                                                             System.currentTimeMillis(),
                                                             (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBodyBuffer().writeString("aardvarks");
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(ReattachTest.ADDRESS);

      InVMConnector.failOnCreateConnection = true;
      InVMConnector.numberOfFailures = reconnectAttempts - 1;

      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(500);

         Assert.assertNotNull(message);

         Assert.assertEquals("aardvarks", message.getBodyBuffer().readString());

         Assert.assertEquals(i, message.getIntProperty("count").intValue());

         message.acknowledge();
      }

      ClientMessage message = consumer.receiveImmediate();

      Assert.assertNull(message);

      session.close();

      sf.close();
   }

   public void testRetryInterval() throws Exception
   {
      final long retryInterval = 500;

      final double retryMultiplier = 1d;

      final int reconnectAttempts = -1;


      locator.setRetryInterval(retryInterval);
      locator.setRetryIntervalMultiplier(retryMultiplier);
      locator.setReconnectAttempts(reconnectAttempts);
      locator.setConfirmationWindowSize(1024 * 1024);
      ClientSessionFactoryInternal sf = (ClientSessionFactoryInternal) locator.createSessionFactory();

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ReattachTest.ADDRESS, ReattachTest.ADDRESS, null, false);

      ClientProducer producer = session.createProducer(ReattachTest.ADDRESS);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(HornetQTextMessage.TYPE,
                                                             false,
                                                             0,
                                                             System.currentTimeMillis(),
                                                             (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBodyBuffer().writeString("aardvarks");
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(ReattachTest.ADDRESS);

      InVMConnector.failOnCreateConnection = true;

      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      long start = System.currentTimeMillis();

      Thread t = new Thread()
      {
         @Override
         public void run()
         {
            try
            {
               Thread.sleep(retryInterval / 2);
            }
            catch (InterruptedException ignore)
            {
            }
            InVMConnector.failOnCreateConnection = false;
         }
      };

      t.start();

      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(500);

         Assert.assertNotNull(message);

         Assert.assertEquals("aardvarks", message.getBodyBuffer().readString());

         Assert.assertEquals(i, message.getIntProperty("count").intValue());

         message.acknowledge();
      }

      ClientMessage message = consumer.receiveImmediate();

      Assert.assertNull(message);

      long end = System.currentTimeMillis();

      Assert.assertTrue(end - start >= retryInterval);

      session.close();

      sf.close();

      t.join();
   }

   public void testExponentialBackoff() throws Exception
   {
      final long retryInterval = 500;

      final double retryMultiplier = 2d;

      final int reconnectAttempts = -1;


      locator.setRetryInterval(retryInterval);
      locator.setRetryIntervalMultiplier(retryMultiplier);
      locator.setReconnectAttempts(reconnectAttempts);
      locator.setConfirmationWindowSize(1024 * 1024);
      ClientSessionFactoryInternal sf = (ClientSessionFactoryInternal) locator.createSessionFactory();

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ReattachTest.ADDRESS, ReattachTest.ADDRESS, null, false);

      ClientProducer producer = session.createProducer(ReattachTest.ADDRESS);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(HornetQTextMessage.TYPE,
                                                             false,
                                                             0,
                                                             System.currentTimeMillis(),
                                                             (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBodyBuffer().writeString("aardvarks");
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(ReattachTest.ADDRESS);

      InVMConnector.failOnCreateConnection = true;
      InVMConnector.numberOfFailures = 3;

      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      long start = System.currentTimeMillis();

      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(500);

         Assert.assertNotNull(message);

         Assert.assertEquals("aardvarks", message.getBodyBuffer().readString());

         Assert.assertEquals(i, message.getObjectProperty(new SimpleString("count")));

         message.acknowledge();
      }

      ClientMessage message = consumer.receiveImmediate();

      Assert.assertNull(message);

      long end = System.currentTimeMillis();

      double wait = retryInterval + retryMultiplier * retryInterval + retryMultiplier * retryMultiplier * retryInterval;

      Assert.assertTrue(end - start >= wait);

      session.close();

      sf.close();
   }

   public void testExponentialBackoffMaxRetryInterval() throws Exception
   {
      final long retryInterval = 500;

      final double retryMultiplier = 2d;

      final int reconnectAttempts = -1;

      final long maxRetryInterval = 1000;

      locator.setRetryInterval(retryInterval);
      locator.setRetryIntervalMultiplier(retryMultiplier);
      locator.setReconnectAttempts(reconnectAttempts);
      locator.setMaxRetryInterval(maxRetryInterval);
      locator.setConfirmationWindowSize(1024 * 1024);
      ClientSessionFactoryInternal sf = (ClientSessionFactoryInternal) locator.createSessionFactory();

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ReattachTest.ADDRESS, ReattachTest.ADDRESS, null, false);

      ClientProducer producer = session.createProducer(ReattachTest.ADDRESS);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(HornetQTextMessage.TYPE,
                                                             false,
                                                             0,
                                                             System.currentTimeMillis(),
                                                             (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBodyBuffer().writeString("aardvarks");
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(ReattachTest.ADDRESS);

      InVMConnector.failOnCreateConnection = true;
      InVMConnector.numberOfFailures = 3;

      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      long start = System.currentTimeMillis();

      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(500);

         Assert.assertNotNull(message);

         Assert.assertEquals("aardvarks", message.getBodyBuffer().readString());

         Assert.assertEquals(i, message.getIntProperty("count").intValue());

         message.acknowledge();
      }

      ClientMessage message = consumer.receiveImmediate();

      Assert.assertNull(message);

      long end = System.currentTimeMillis();

      double wait = retryInterval + retryMultiplier * 2 * retryInterval + retryMultiplier;

      Assert.assertTrue(end - start >= wait);

      Assert.assertTrue(end - start < wait + 500);

      session.close();

      sf.close();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      service = createServer(false, false);

      service.start();

      locator =  createFactory(false);
   }

   @Override
   protected void tearDown() throws Exception
   {
      InVMConnector.resetFailures();

      locator.close();
      
      service.stop();

      Assert.assertEquals(0, InVMRegistry.instance.size());

      service = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
