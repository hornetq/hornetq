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

package org.jboss.messaging.tests.integration.cluster.failover;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.MessageHandler;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.client.impl.ClientSessionInternal;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.impl.invm.InVMRegistry;
import org.jboss.messaging.core.remoting.impl.invm.TransportConstants;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.jms.client.JBossTextMessage;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.utils.SimpleString;

/**
 * A RandomFailoverSoakTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class RandomFailoverTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(RandomFailoverTest.class);

   // Constants -----------------------------------------------------

   private static final int RECEIVE_TIMEOUT = 10000;

   // Attributes ----------------------------------------------------

   private static final SimpleString ADDRESS = new SimpleString("FailoverTestAddress");

   private MessagingService liveService;

   private MessagingService backupService;

   private final Map<String, Object> backupParams = new HashMap<String, Object>();

   private Timer timer;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testA() throws Exception
   {
      runTest(new RunnableT()
      {
         public void run(final ClientSessionFactory sf) throws Exception
         {
            doTestA(sf);
         }
      });
   }

   public void testB() throws Exception
   {
      runTest(new RunnableT()
      {
         public void run(final ClientSessionFactory sf) throws Exception
         {
            doTestB(sf);
         }
      });
   }

   public void testC() throws Exception
   {
      runTest(new RunnableT()
      {
         public void run(final ClientSessionFactory sf) throws Exception
         {
            doTestC(sf);
         }
      });
   }

   public void testD() throws Exception
   {
      runTest(new RunnableT()
      {
         public void run(final ClientSessionFactory sf) throws Exception
         {
            doTestD(sf);
         }
      });
   }

   public void testE() throws Exception
   {
      runTest(new RunnableT()
      {
         public void run(final ClientSessionFactory sf) throws Exception
         {
            doTestE(sf);
         }
      });
   }

   public void testF() throws Exception
   {
      runTest(new RunnableT()
      {
         public void run(final ClientSessionFactory sf) throws Exception
         {
            doTestF(sf);
         }
      });
   }

   public void testG() throws Exception
   {
      runTest(new RunnableT()
      {
         public void run(final ClientSessionFactory sf) throws Exception
         {
            doTestG(sf);
         }
      });
   }

   public void testH() throws Exception
   {
      runTest(new RunnableT()
      {
         public void run(final ClientSessionFactory sf) throws Exception
         {
            doTestH(sf);
         }
      });
   }

   public void testI() throws Exception
   {
      runTest(new RunnableT()
      {
         public void run(final ClientSessionFactory sf) throws Exception
         {
            doTestI(sf);
         }
      });
   }

   public void testJ() throws Exception
   {
      runTest(new RunnableT()
      {
         public void run(final ClientSessionFactory sf) throws Exception
         {
            doTestJ(sf);
         }
      });
   }

   public void testK() throws Exception
   {
      runTest(new RunnableT()
      {
         public void run(final ClientSessionFactory sf) throws Exception
         {
            doTestK(sf);
         }
      });
   }

   public void testL() throws Exception
   {
      runTest(new RunnableT()
      {
         public void run(final ClientSessionFactory sf) throws Exception
         {
            doTestL(sf);
         }
      });
   }

   public void testN() throws Exception
   {
      runTest(new RunnableT()
      {
         public void run(final ClientSessionFactory sf) throws Exception
         {
            doTestN(sf);
         }
      });
   }

   public void runTest(final RunnableT runnable) throws Exception
   {
      final int numIts = getNumIterations();

      for (int its = 0; its < numIts; its++)
      {
         start();

         ClientSessionFactoryImpl sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                    new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                               backupParams),
                                                                    0,
                                                                    1,
                                                                    ClientSessionFactoryImpl.DEFAULT_MAX_RETRIES_BEFORE_FAILOVER,
                                                                    ClientSessionFactoryImpl.DEFAULT_MAX_RETRIES_AFTER_FAILOVER);

         sf.setSendWindowSize(32 * 1024);

         ClientSession session = sf.createSession(false, false, false);

         Failer failer = startFailer(1000, session);

         log.info("***************Iteration: " + its);
         do
         {
            runnable.run(sf);
         }
         while (!failer.isExecuted());
         
         session.close();

         assertEquals(0, sf.numSessions());

         assertEquals(0, sf.numConnections());

         stop();
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void doTestA(final ClientSessionFactory sf) throws Exception
   {
      long start = System.currentTimeMillis();

      log.info("starting================");

      ClientSession s = sf.createSession(false, false, false);

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString("sub" + i);

         ClientSession sessConsume = sf.createSession(false, true, true);

         sessConsume.start();

         sessConsume.createQueue(ADDRESS, subName, null, false, false);

         ClientConsumer consumer = sessConsume.createConsumer(subName);

         consumers.add(consumer);

         sessions.add(sessConsume);
      }

      ClientSession sessSend = sf.createSession(false, true, true);

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
               message.acknowledge();
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

   protected void doTestB(final ClientSessionFactory sf) throws Exception
   {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false);

      final int numMessages = 100;

      final int numSessions = 50;

      Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString("sub" + i);

         ClientSession sessConsume = sf.createSession(false, true, true);

         sessConsume.createQueue(ADDRESS, subName, null, false, false);

         ClientConsumer consumer = sessConsume.createConsumer(subName);

         consumers.add(consumer);

         sessions.add(sessConsume);
      }

      ClientSession sessSend = sf.createSession(false, true, true);

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

   protected void doTestC(final ClientSessionFactory sf) throws Exception
   {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false);

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString("sub" + i);

         ClientSession sessConsume = sf.createSession(false, false, false);

         sessConsume.start();

         sessConsume.createQueue(ADDRESS, subName, null, false, false);

         ClientConsumer consumer = sessConsume.createConsumer(subName);

         consumers.add(consumer);

         sessions.add(sessConsume);
      }

      ClientSession sessSend = sf.createSession(false, true, true);

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

   protected void doTestD(final ClientSessionFactory sf) throws Exception
   {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false);

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString("sub" + i);

         ClientSession sessConsume = sf.createSession(false, false, false);

         sessConsume.createQueue(ADDRESS, subName, null, false, false);

         ClientConsumer consumer = sessConsume.createConsumer(subName);

         consumers.add(consumer);

         sessions.add(sessConsume);
      }

      ClientSession sessSend = sf.createSession(false, true, true);

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

   protected void doTestE(final ClientSessionFactory sf) throws Exception
   {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false);

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString("sub" + i);

         ClientSession sessConsume = sf.createSession(false, true, true);

         sessConsume.start();

         sessConsume.createQueue(ADDRESS, subName, null, false, false);

         ClientConsumer consumer = sessConsume.createConsumer(subName);

         consumers.add(consumer);

         sessions.add(sessConsume);
      }

      ClientSession sessSend = sf.createSession(false, true, true);

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

            msg.acknowledge();
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

   protected void doTestF(final ClientSessionFactory sf) throws Exception
   {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false);

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString("sub" + i);

         ClientSession sessConsume = sf.createSession(false, true, true);

         sessConsume.createQueue(ADDRESS, subName, null, false, false);

         ClientConsumer consumer = sessConsume.createConsumer(subName);

         consumers.add(consumer);

         sessions.add(sessConsume);
      }

      ClientSession sessSend = sf.createSession(false, true, true);

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

            if (msg == null)
            {
               throw new IllegalStateException("Failed to receive message " + i);
            }

            assertNotNull(msg);

            assertEquals(i, msg.getProperty(new SimpleString("count")));

            msg.acknowledge();
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

      assertEquals(1, ((ClientSessionFactoryImpl)sf).numSessions());

      long end = System.currentTimeMillis();

      log.info("duration " + (end - start));
   }

   protected void doTestG(final ClientSessionFactory sf) throws Exception
   {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false);

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString("sub" + i);

         ClientSession sessConsume = sf.createSession(false, false, false);

         sessConsume.start();

         sessConsume.createQueue(ADDRESS, subName, null, false, false);

         ClientConsumer consumer = sessConsume.createConsumer(subName);

         consumers.add(consumer);

         sessions.add(sessConsume);
      }

      ClientSession sessSend = sf.createSession(false, false, false);

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

      log.info("sent and committed");

      for (int i = 0; i < numMessages; i++)
      {
         for (ClientConsumer consumer : consumers)
         {
            ClientMessage msg = consumer.receive(RECEIVE_TIMEOUT);

            assertNotNull(msg);

            assertEquals(i, msg.getProperty(new SimpleString("count")));

            msg.acknowledge();
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

            msg.acknowledge();
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

   protected void doTestH(final ClientSessionFactory sf) throws Exception
   {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false);

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString("sub" + i);

         ClientSession sessConsume = sf.createSession(false, false, false);

         sessConsume.createQueue(ADDRESS, subName, null, false, false);

         ClientConsumer consumer = sessConsume.createConsumer(subName);

         consumers.add(consumer);

         sessions.add(sessConsume);
      }

      ClientSession sessSend = sf.createSession(false, false, false);

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

            msg.acknowledge();
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

            msg.acknowledge();
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

   protected void doTestI(final ClientSessionFactory sf) throws Exception
   {
      ClientSession sessCreate = sf.createSession(false, true, true);

      sessCreate.createQueue(ADDRESS, ADDRESS, null, false, false);

      ClientSession sess = sf.createSession(false, true, true);

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

      message2.acknowledge();

      sess.close();

      sessCreate.deleteQueue(ADDRESS);

      sessCreate.close();
   }

   protected void doTestJ(final ClientSessionFactory sf) throws Exception
   {
      ClientSession sessCreate = sf.createSession(false, true, true);

      sessCreate.createQueue(ADDRESS, ADDRESS, null, false, false);

      ClientSession sess = sf.createSession(false, true, true);

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

      message2.acknowledge();

      sess.close();

      sessCreate.deleteQueue(ADDRESS);

      sessCreate.close();
   }

   protected void doTestK(final ClientSessionFactory sf) throws Exception
   {
      ClientSession s = sf.createSession(false, false, false);

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

   protected void doTestL(final ClientSessionFactory sf) throws Exception
   {     
      final int numSessions = 10;

      for (int i = 0; i < numSessions; i++)
      {
         log.info("i " + i);
         
         ClientSession session = sf.createSession(false, false, false);

         session.close();
      }   
   }

   protected void doTestN(final ClientSessionFactory sf) throws Exception
   {
      ClientSession sessCreate = sf.createSession(false, true, true);

      sessCreate.createQueue(ADDRESS, new SimpleString(ADDRESS.toString()), null, false, false);

      ClientSession sess = sf.createSession(false, true, true);

      sess.stop();

      sess.start();

      sess.stop();

      ClientConsumer consumer = sess.createConsumer(new SimpleString(ADDRESS.toString()));

      ClientProducer producer = sess.createProducer(ADDRESS);

      ClientMessage message = sess.createClientMessage(JBossTextMessage.TYPE,
                                                       false,
                                                       0,
                                                       System.currentTimeMillis(),
                                                       (byte)1);
      message.getBody().flip();

      producer.send(message);

      sess.start();

      ClientMessage message2 = consumer.receive(RECEIVE_TIMEOUT);

      assertNotNull(message2);

      message2.acknowledge();

      sess.stop();

      sess.start();

      sess.close();

      sessCreate.deleteQueue(new SimpleString(ADDRESS.toString()));

      sessCreate.close();
   }

   protected int getNumIterations()
   {
      return 2;
   }

   protected void setUp() throws Exception
   {
      super.setUp();

      log.info("*********** created timer");
      timer = new Timer(true);

      log.info("************ Starting test " + this.getName());
   }

   protected void tearDown() throws Exception
   {
      timer.cancel();

      log.info("************ Ended test " + this.getName());

      InVMRegistry.instance.clear();

      super.tearDown();
   }

   // Private -------------------------------------------------------

   private Failer startFailer(final long time, final ClientSession session)
   {
      Failer failer = new Failer((ClientSessionInternal)session);

      timer.schedule(failer, (long)(time * Math.random()), 100);

      return failer;
   }

   private void start() throws Exception
   {
      Configuration backupConf = new ConfigurationImpl();
      backupConf.setSecurityEnabled(false);
      backupParams.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      backupConf.getAcceptorConfigurations()
                .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory",
                                                backupParams));
      backupConf.setBackup(true);
      backupService = Messaging.newNullStorageMessagingService(backupConf);
      backupService.start();

      // We need to sleep > 16 ms otherwise the id generators on live and backup could be initialised
      // with the same time component
      Thread.sleep(17);

      Configuration liveConf = new ConfigurationImpl();
      liveConf.setSecurityEnabled(false);
      liveConf.getAcceptorConfigurations()
              .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory"));
      Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();
      TransportConfiguration backupTC = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                   backupParams,
                                                                   "backup-connector");
      connectors.put(backupTC.getName(), backupTC);
      liveConf.setConnectorConfigurations(connectors);
      liveConf.setBackupConnectorName(backupTC.getName());
      liveService = Messaging.newNullStorageMessagingService(liveConf);
      liveService.start();
   }

   private void stop() throws Exception
   {
      backupService.stop();

      liveService.stop();

      assertEquals(0, InVMRegistry.instance.size());
   }

   // Inner classes -------------------------------------------------

   class Failer extends TimerTask
   {
      private final ClientSessionInternal session;

      private boolean executed;

      public Failer(final ClientSessionInternal session)
      {
         this.session = session;
      }

      public synchronized void run()
      {
         log.info("** Failing connection");

         session.getConnection().fail(new MessagingException(MessagingException.NOT_CONNECTED, "oops"));

         log.info("** Fail complete");

         cancel();

         executed = true;
      }

      public synchronized boolean isExecuted()
      {
         return executed;
      }
   }

   public abstract class RunnableT
   {
      abstract void run(final ClientSessionFactory sf) throws Exception;
   }
}
