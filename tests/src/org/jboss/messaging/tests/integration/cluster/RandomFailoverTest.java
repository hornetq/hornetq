/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors by
 * the @authors tag. See the copyright.txt in the distribution for a full listing of individual contributors. This is
 * free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of the License, or (at your option) any later version.
 * This software is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public License along with this software; if not,
 * write to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.tests.integration.cluster;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.MessageHandler;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.client.impl.ClientSessionImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.impl.ConnectionRegistryImpl;
import org.jboss.messaging.core.remoting.impl.RemotingConnectionImpl;
import org.jboss.messaging.core.remoting.impl.invm.InVMRegistry;
import org.jboss.messaging.core.remoting.impl.invm.TransportConstants;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;
import org.jboss.messaging.jms.client.JBossTextMessage;
import org.jboss.messaging.util.SimpleString;

/**
 * A RandomFailoverTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class RandomFailoverTest extends TestCase
{
   private static final Logger log = Logger.getLogger(SimpleAutomaticFailoverTest.class);

   // Constants -----------------------------------------------------

   private static final int RECEIVE_TIMEOUT = 5000;

   // Attributes ----------------------------------------------------

   private static final SimpleString ADDRESS = new SimpleString("FailoverTestAddress");

   private MessagingService liveService;

   private MessagingService backupService;

   private final Map<String, Object> backupParams = new HashMap<String, Object>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   private Timer timer = new Timer();

   private volatile Failer failer;

   private void startFailer(final long time, final ClientSession session)
   {
      failer = new Failer(session);

      timer.schedule(failer, (long)(time * Math.random()), 100);
   }

   private class Failer extends TimerTask
   {
      private final ClientSession session;

      private boolean executed;

      public Failer(final ClientSession session)
      {
         this.session = session;
      }

      public synchronized void run()
      {
         log.info("** Failing connection");

         RemotingConnectionImpl conn = (RemotingConnectionImpl)((ClientSessionImpl)session).getConnection();

         conn.fail(new MessagingException(MessagingException.NOT_CONNECTED, "blah"));

         log.info("** Fail complete");

         cancel();

         executed = true;
      }

      public synchronized boolean isExecuted()
      {
         return executed;
      }
   }
   
   private interface RunnableTest
   {
      void run(final ClientSessionFactory sf) throws Exception;      
   }
   
   public void testA() throws Exception
   {
      runTest(new RunnableTest()
      {
         public void run(final ClientSessionFactory sf) throws Exception
         {
            testA(sf);
         }
      });
   }
   
   public void testB() throws Exception
   {
      runTest(new RunnableTest()
      {
         public void run(final ClientSessionFactory sf) throws Exception
         {
            testB(sf);
         }
      });
   }
   
   public void testC() throws Exception
   {
      runTest(new RunnableTest()
      {
         public void run(final ClientSessionFactory sf) throws Exception
         {
            testC(sf);
         }
      });
   }
   
   public void testD() throws Exception
   {
      runTest(new RunnableTest()
      {
         public void run(final ClientSessionFactory sf) throws Exception
         {
            testD(sf);
         }
      });
   }
   
   public void testE() throws Exception
   {
      runTest(new RunnableTest()
      {
         public void run(final ClientSessionFactory sf) throws Exception
         {
            testE(sf);
         }
      });
   }
   
   public void testF() throws Exception
   {
      runTest(new RunnableTest()
      {
         public void run(final ClientSessionFactory sf) throws Exception
         {
            testF(sf);
         }
      });
   }
   
   public void testG() throws Exception
   {
      runTest(new RunnableTest()
      {
         public void run(final ClientSessionFactory sf) throws Exception
         {
            testG(sf);
         }
      });
   }
   
   public void testH() throws Exception
   {
      runTest(new RunnableTest()
      {
         public void run(final ClientSessionFactory sf) throws Exception
         {
            testH(sf);
         }
      });
   }
   
   public void testI() throws Exception
   {
      runTest(new RunnableTest()
      {
         public void run(final ClientSessionFactory sf) throws Exception
         {
            testI(sf);
         }
      });
   }
   
   public void testJ() throws Exception
   {
      runTest(new RunnableTest()
      {
         public void run(final ClientSessionFactory sf) throws Exception
         {
            testJ(sf);
         }
      });
   }
   
   public void testK() throws Exception
   {
      runTest(new RunnableTest()
      {
         public void run(final ClientSessionFactory sf) throws Exception
         {
            testK(sf);
         }
      });
   }
   
   public void testL() throws Exception
   {
      runTest(new RunnableTest()
      {
         public void run(final ClientSessionFactory sf) throws Exception
         {
            testK(sf);
         }
      });
   }

   public void runTest(final RunnableTest runnable) throws Exception
   {
      final int numIts = 100;
      
      for (int its = 0; its < numIts; its++)
      {
         start();

         ClientSessionFactoryImpl sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                    new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                               backupParams));

         ClientSession session = sf.createSession(false, false, false, false);

         startFailer(1000, session);

         do
         {
            runnable.run(sf);
         }
         while (!failer.isExecuted());

         session.close();

         assertEquals(0, sf.getSessionCount());

         stop();
      }
   }

   public void testA(final ClientSessionFactory sf) throws Exception
   {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false, false);

      final int numMessages = 100;

      final int numSessions = 50;

      Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString("sub" + i);

         ClientSession sessConsume = sf.createSession(false, true, true, false);

         sessConsume.start();

         sessConsume.createQueue(ADDRESS, subName, null, false, false);

         ClientConsumer consumer = sessConsume.createConsumer(subName);

         consumers.add(consumer);

         sessions.add(sessConsume);
      }

      ClientSession sessSend = sf.createSession(false, true, true, false);

      ClientProducer producer = sessSend.createProducer(ADDRESS);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = sessSend.createClientMessage(JBossTextMessage.TYPE,
                                                              false,
                                                              0,
                                                              System.currentTimeMillis(),
                                                              (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBody().flip();
         producer.send(message);
      }

      class MyHandler implements MessageHandler
      {
         final CountDownLatch latch = new CountDownLatch(1);

         volatile int count;

         public void onMessage(ClientMessage message)
         {
            if (count == numMessages)
            {
               fail("Too many messages");
            }

            assertEquals(count, message.getProperty(new SimpleString("count")));

            count++;

            try
            {
               message.processed();
            }
            catch (MessagingException me)
            {
               log.error("Failed to process", me);
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
         boolean ok = handler.latch.await(5000, TimeUnit.MILLISECONDS);

         assertTrue("Didn't receive all messages", ok);
      }

      sessSend.close();
      for (ClientSession session : sessions)
      {
         session.close();
      }

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString("sub" + i);

         s.deleteQueue(subName);
      }

      s.close();

      long end = System.currentTimeMillis();

      log.info("duration " + (end - start));
   }

   public void testB(final ClientSessionFactory sf) throws Exception
   {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false, false);

      final int numMessages = 100;

      final int numSessions = 50;

      Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString("sub" + i);

         ClientSession sessConsume = sf.createSession(false, true, true, false);

         sessConsume.createQueue(ADDRESS, subName, null, false, false);

         ClientConsumer consumer = sessConsume.createConsumer(subName);

         consumers.add(consumer);

         sessions.add(sessConsume);
      }

      ClientSession sessSend = sf.createSession(false, true, true, false);

      ClientProducer producer = sessSend.createProducer(ADDRESS);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = sessSend.createClientMessage(JBossTextMessage.TYPE,
                                                              false,
                                                              0,
                                                              System.currentTimeMillis(),
                                                              (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBody().flip();
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

         public void onMessage(ClientMessage message)
         {
            if (count == numMessages)
            {
               fail("Too many messages");
            }

            assertEquals(count, message.getProperty(new SimpleString("count")));

            count++;

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
         boolean ok = handler.latch.await(10000, TimeUnit.MILLISECONDS);

         assertTrue(ok);
      }

      sessSend.close();

      for (ClientSession session : sessions)
      {
         session.close();
      }

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString("sub" + i);

         s.deleteQueue(subName);
      }

      s.close();

      long end = System.currentTimeMillis();

      log.info("duration " + (end - start));

   }

   public void testC(final ClientSessionFactory sf) throws Exception
   {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false, false);

      final int numMessages = 100;

      final int numSessions = 50;

      Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString("sub" + i);

         ClientSession sessConsume = sf.createSession(false, false, false, false);

         sessConsume.start();

         sessConsume.createQueue(ADDRESS, subName, null, false, false);

         ClientConsumer consumer = sessConsume.createConsumer(subName);

         consumers.add(consumer);

         sessions.add(sessConsume);
      }

      ClientSession sessSend = sf.createSession(false, true, true, false);

      ClientProducer producer = sessSend.createProducer(ADDRESS);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = sessSend.createClientMessage(JBossTextMessage.TYPE,
                                                              false,
                                                              0,
                                                              System.currentTimeMillis(),
                                                              (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBody().flip();
         producer.send(message);
      }

      sessSend.rollback();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = sessSend.createClientMessage(JBossTextMessage.TYPE,
                                                              false,
                                                              0,
                                                              System.currentTimeMillis(),
                                                              (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBody().flip();
         producer.send(message);
      }

      sessSend.commit();

      class MyHandler implements MessageHandler
      {
         final CountDownLatch latch = new CountDownLatch(1);

         volatile int count;

         public void onMessage(ClientMessage message)
         {
            if (count == numMessages)
            {
               fail("Too many messages");
            }

            assertEquals(count, message.getProperty(new SimpleString("count")));

            count++;

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
         boolean ok = handler.latch.await(10000, TimeUnit.MILLISECONDS);

         assertTrue(ok);
      }

      handlers.clear();

      // New handlers
      for (ClientConsumer consumer : consumers)
      {
         MyHandler handler = new MyHandler();

         consumer.setMessageHandler(handler);

         handlers.add(handler);
      }

      for (ClientSession session : sessions)
      {
         session.rollback();
      }

      for (MyHandler handler : handlers)
      {
         boolean ok = handler.latch.await(10000, TimeUnit.MILLISECONDS);

         assertTrue(ok);
      }

      for (ClientSession session : sessions)
      {
         session.commit();
      }

      sessSend.close();
      for (ClientSession session : sessions)
      {
         session.close();
      }

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString("sub" + i);

         s.deleteQueue(subName);
      }

      s.close();

      long end = System.currentTimeMillis();

      log.info("duration " + (end - start));
   }

   public void testD(final ClientSessionFactory sf) throws Exception
   {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false, false);

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString("sub" + i);

         ClientSession sessConsume = sf.createSession(false, false, false, false);

         sessConsume.createQueue(ADDRESS, subName, null, false, false);

         ClientConsumer consumer = sessConsume.createConsumer(subName);

         consumers.add(consumer);

         sessions.add(sessConsume);
      }

      ClientSession sessSend = sf.createSession(false, true, true, false);

      ClientProducer producer = sessSend.createProducer(ADDRESS);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = sessSend.createClientMessage(JBossTextMessage.TYPE,
                                                              false,
                                                              0,
                                                              System.currentTimeMillis(),
                                                              (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBody().flip();
         producer.send(message);
      }

      sessSend.rollback();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = sessSend.createClientMessage(JBossTextMessage.TYPE,
                                                              false,
                                                              0,
                                                              System.currentTimeMillis(),
                                                              (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBody().flip();
         producer.send(message);
      }

      sessSend.commit();

      for (ClientSession session : sessions)
      {
         session.start();
      }

      class MyHandler implements MessageHandler
      {
         final CountDownLatch latch = new CountDownLatch(1);

         volatile int count;

         public void onMessage(ClientMessage message)
         {
            if (count == numMessages)
            {
               fail("Too many messages");
            }

            assertEquals(count, message.getProperty(new SimpleString("count")));

            count++;

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
         boolean ok = handler.latch.await(10000, TimeUnit.MILLISECONDS);

         assertTrue(ok);
      }

      handlers.clear();

      // New handlers
      for (ClientConsumer consumer : consumers)
      {
         MyHandler handler = new MyHandler();

         consumer.setMessageHandler(handler);

         handlers.add(handler);
      }

      for (ClientSession session : sessions)
      {
         session.rollback();
      }

      for (MyHandler handler : handlers)
      {
         boolean ok = handler.latch.await(10000, TimeUnit.MILLISECONDS);

         assertTrue(ok);
      }

      for (ClientSession session : sessions)
      {
         session.commit();
      }

      sessSend.close();
      for (ClientSession session : sessions)
      {
         session.close();
      }

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString("sub" + i);

         s.deleteQueue(subName);
      }

      s.close();

      long end = System.currentTimeMillis();

      log.info("duration " + (end - start));
   }

   // Now with synchronous receive()

   public void testE(final ClientSessionFactory sf) throws Exception
   {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false, false);

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString("sub" + i);

         ClientSession sessConsume = sf.createSession(false, true, true, false);

         sessConsume.start();

         sessConsume.createQueue(ADDRESS, subName, null, false, false);

         ClientConsumer consumer = sessConsume.createConsumer(subName);

         consumers.add(consumer);

         sessions.add(sessConsume);
      }

      ClientSession sessSend = sf.createSession(false, true, true, false);

      ClientProducer producer = sessSend.createProducer(ADDRESS);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = sessSend.createClientMessage(JBossTextMessage.TYPE,
                                                              false,
                                                              0,
                                                              System.currentTimeMillis(),
                                                              (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBody().flip();
         producer.send(message);
      }

      for (int i = 0; i < numMessages; i++)
      {
         for (ClientConsumer consumer : consumers)
         {
            ClientMessage msg = consumer.receive(RECEIVE_TIMEOUT);

            assertNotNull(msg);

            assertEquals(i, msg.getProperty(new SimpleString("count")));

            msg.processed();
         }
      }

      for (int i = 0; i < numMessages; i++)
      {
         for (ClientConsumer consumer : consumers)
         {
            ClientMessage msg = consumer.receiveImmediate();

            assertNull(msg);
         }
      }

      sessSend.close();
      for (ClientSession session : sessions)
      {
         session.close();
      }

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString("sub" + i);

         s.deleteQueue(subName);
      }

      s.close();

      long end = System.currentTimeMillis();

      log.info("duration " + (end - start));
   }

   public void testF(final ClientSessionFactory sf) throws Exception
   {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false, false);

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString("sub" + i);

         ClientSession sessConsume = sf.createSession(false, true, true, false);

         sessConsume.createQueue(ADDRESS, subName, null, false, false);

         ClientConsumer consumer = sessConsume.createConsumer(subName);

         consumers.add(consumer);

         sessions.add(sessConsume);
      }

      ClientSession sessSend = sf.createSession(false, true, true, false);

      ClientProducer producer = sessSend.createProducer(ADDRESS);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = sessSend.createClientMessage(JBossTextMessage.TYPE,
                                                              false,
                                                              0,
                                                              System.currentTimeMillis(),
                                                              (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBody().flip();
         producer.send(message);
      }

      for (ClientSession session : sessions)
      {
         session.start();
      }

      for (int i = 0; i < numMessages; i++)
      {
         for (ClientConsumer consumer : consumers)
         {
            ClientMessage msg = consumer.receive(RECEIVE_TIMEOUT);

            assertNotNull(msg);

            assertEquals(i, msg.getProperty(new SimpleString("count")));

            msg.processed();
         }
      }

      for (int i = 0; i < numMessages; i++)
      {
         for (ClientConsumer consumer : consumers)
         {
            ClientMessage msg = consumer.receiveImmediate();

            assertNull(msg);
         }
      }

      sessSend.close();
      for (ClientSession session : sessions)
      {
         session.close();
      }

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString("sub" + i);

         s.deleteQueue(subName);
      }

      s.close();

      long end = System.currentTimeMillis();

      log.info("duration " + (end - start));
   }

   public void testG(final ClientSessionFactory sf) throws Exception
   {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false, false);

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString("sub" + i);

         ClientSession sessConsume = sf.createSession(false, false, false, false);

         sessConsume.start();

         sessConsume.createQueue(ADDRESS, subName, null, false, false);

         ClientConsumer consumer = sessConsume.createConsumer(subName);

         consumers.add(consumer);

         sessions.add(sessConsume);
      }

      ClientSession sessSend = sf.createSession(false, false, false, false);

      ClientProducer producer = sessSend.createProducer(ADDRESS);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = sessSend.createClientMessage(JBossTextMessage.TYPE,
                                                              false,
                                                              0,
                                                              System.currentTimeMillis(),
                                                              (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBody().flip();
         producer.send(message);
      }

      sessSend.rollback();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = sessSend.createClientMessage(JBossTextMessage.TYPE,
                                                              false,
                                                              0,
                                                              System.currentTimeMillis(),
                                                              (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBody().flip();
         producer.send(message);
      }

      sessSend.commit();

      for (int i = 0; i < numMessages; i++)
      {
         for (ClientConsumer consumer : consumers)
         {
            ClientMessage msg = consumer.receive(RECEIVE_TIMEOUT);

            assertNotNull(msg);

            assertEquals(i, msg.getProperty(new SimpleString("count")));

            msg.processed();
         }
      }

      for (ClientConsumer consumer : consumers)
      {
         ClientMessage msg = consumer.receiveImmediate();

         assertNull(msg);
      }

      for (ClientSession session : sessions)
      {
         session.rollback();
      }

      for (int i = 0; i < numMessages; i++)
      {
         for (ClientConsumer consumer : consumers)
         {
            ClientMessage msg = consumer.receive(RECEIVE_TIMEOUT);

            assertNotNull(msg);

            assertEquals(i, msg.getProperty(new SimpleString("count")));

            msg.processed();
         }
      }

      for (int i = 0; i < numMessages; i++)
      {
         for (ClientConsumer consumer : consumers)
         {
            ClientMessage msg = consumer.receiveImmediate();

            assertNull(msg);
         }
      }

      for (ClientSession session : sessions)
      {
         session.commit();
      }

      sessSend.close();
      for (ClientSession session : sessions)
      {
         session.close();
      }

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString("sub" + i);

         s.deleteQueue(subName);
      }

      s.close();

      long end = System.currentTimeMillis();

      log.info("duration " + (end - start));
   }

   public void testH(final ClientSessionFactory sf) throws Exception
   {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false, false);

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString("sub" + i);

         ClientSession sessConsume = sf.createSession(false, false, false, false);

         sessConsume.createQueue(ADDRESS, subName, null, false, false);

         ClientConsumer consumer = sessConsume.createConsumer(subName);

         consumers.add(consumer);

         sessions.add(sessConsume);
      }

      ClientSession sessSend = sf.createSession(false, false, false, false);

      ClientProducer producer = sessSend.createProducer(ADDRESS);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = sessSend.createClientMessage(JBossTextMessage.TYPE,
                                                              false,
                                                              0,
                                                              System.currentTimeMillis(),
                                                              (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBody().flip();
         producer.send(message);
      }

      sessSend.rollback();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = sessSend.createClientMessage(JBossTextMessage.TYPE,
                                                              false,
                                                              0,
                                                              System.currentTimeMillis(),
                                                              (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBody().flip();
         producer.send(message);
      }

      sessSend.commit();

      for (ClientSession session : sessions)
      {
         session.start();
      }

      for (int i = 0; i < numMessages; i++)
      {
         for (ClientConsumer consumer : consumers)
         {
            ClientMessage msg = consumer.receive(RECEIVE_TIMEOUT);

            assertNotNull(msg);

            assertEquals(i, msg.getProperty(new SimpleString("count")));

            msg.processed();
         }
      }

      for (int i = 0; i < numMessages; i++)
      {
         for (ClientConsumer consumer : consumers)
         {
            ClientMessage msg = consumer.receiveImmediate();

            assertNull(msg);
         }
      }

      for (ClientSession session : sessions)
      {
         session.rollback();
      }

      for (int i = 0; i < numMessages; i++)
      {
         for (ClientConsumer consumer : consumers)
         {
            ClientMessage msg = consumer.receive(RECEIVE_TIMEOUT);

            assertNotNull(msg);

            assertEquals(i, msg.getProperty(new SimpleString("count")));

            msg.processed();
         }
      }

      for (int i = 0; i < numMessages; i++)
      {
         for (ClientConsumer consumer : consumers)
         {
            ClientMessage msg = consumer.receiveImmediate();

            assertNull(msg);
         }
      }

      for (ClientSession session : sessions)
      {
         session.commit();
      }

      sessSend.close();
      for (ClientSession session : sessions)
      {
         session.close();
      }

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString("sub" + i);

         s.deleteQueue(subName);
      }

      s.close();

      long end = System.currentTimeMillis();

      log.info("duration " + (end - start));
   }

   public void testI(final ClientSessionFactory sf) throws Exception
   {
      ClientSession sessCreate = sf.createSession(false, true, true, false);

      sessCreate.createQueue(ADDRESS, ADDRESS, null, false, false);

      ClientSession sess = sf.createSession(false, true, true, false);

      sess.start();

      ClientConsumer consumer = sess.createConsumer(ADDRESS);

      ClientProducer producer = sess.createProducer(ADDRESS);

      ClientMessage message = sess.createClientMessage(JBossTextMessage.TYPE,
                                                       false,
                                                       0,
                                                       System.currentTimeMillis(),
                                                       (byte)1);
      message.getBody().flip();

      producer.send(message);

      ClientMessage message2 = consumer.receive(RECEIVE_TIMEOUT);

      assertNotNull(message2);

      message2.processed();

      sess.close();

      sessCreate.deleteQueue(ADDRESS);

      sessCreate.close();     
   }

   public void testJ(final ClientSessionFactory sf) throws Exception
   {
      ClientSession sessCreate = sf.createSession(false, true, true, false);

      sessCreate.createQueue(ADDRESS, ADDRESS, null, false, false);

      ClientSession sess = sf.createSession(false, true, true, false);

      sess.start();

      ClientConsumer consumer = sess.createConsumer(ADDRESS);

      ClientProducer producer = sess.createProducer(ADDRESS);

      ClientMessage message = sess.createClientMessage(JBossTextMessage.TYPE,
                                                       false,
                                                       0,
                                                       System.currentTimeMillis(),
                                                       (byte)1);
      message.getBody().flip();

      producer.send(message);

      ClientMessage message2 = consumer.receive(RECEIVE_TIMEOUT);

      assertNotNull(message2);

      message2.processed();

      sess.close();

      sessCreate.deleteQueue(ADDRESS);

      sessCreate.close();
   }

   public void testK(final ClientSessionFactory sf) throws Exception
   {
      ClientSession s = sf.createSession(false, false, false, false);

      s.createQueue(ADDRESS, ADDRESS, null, false, false);

      final int numConsumers = 100;

      for (int i = 0; i < numConsumers; i++)
      {
         ClientConsumer consumer = s.createConsumer(ADDRESS);

         consumer.close();
      }

      s.deleteQueue(ADDRESS);

      s.close();
   }

   public void testL(final ClientSessionFactory sf) throws Exception
   {
      ClientSession s = sf.createSession(false, false, false, false);

      final int numSessions = 100;

      for (int i = 0; i < numSessions; i++)
      {
         ClientSession session = sf.createSession(false, false, false, false);

         session.close();
      }

      s.close();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   private void start() throws Exception
   {
      Configuration backupConf = new ConfigurationImpl();
      backupConf.setSecurityEnabled(false);
      backupConf.setPacketConfirmationBatchSize(10);
      backupParams.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      backupConf.getAcceptorConfigurations()
                .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory",
                                                backupParams));
      backupConf.setBackup(true);
      backupService = MessagingServiceImpl.newNullStorageMessagingServer(backupConf);
      backupService.start();

      // We need to sleep > 16 ms otherwise the id generators on live and backup could be initialised
      // with the same time component
      Thread.sleep(17);

      Configuration liveConf = new ConfigurationImpl();
      liveConf.setSecurityEnabled(false);
      liveConf.setPacketConfirmationBatchSize(10);
      liveConf.getAcceptorConfigurations()
              .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory"));
      liveConf.setBackupConnectorConfiguration(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                          backupParams));
      liveService = MessagingServiceImpl.newNullStorageMessagingServer(liveConf);
      liveService.start();
   }

   private void stop() throws Exception
   {
      ConnectionRegistryImpl.instance.dump();

      assertEquals(0, ConnectionRegistryImpl.instance.size());

      // ConnectionRegistryImpl.instance.clear();

      assertEquals(0, backupService.getServer().getRemotingService().getConnections().size());

      backupService.stop();

      assertEquals(0, liveService.getServer().getRemotingService().getConnections().size());

      liveService.stop();

      assertEquals(0, InVMRegistry.instance.size());
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
