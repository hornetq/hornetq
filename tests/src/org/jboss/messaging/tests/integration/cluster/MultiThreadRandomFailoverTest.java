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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
 * A MultiThreadRandomFailoverTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class MultiThreadRandomFailoverTest extends TestCase
{
   private static final Logger log = Logger.getLogger(MultiThreadRandomFailoverTest.class);

   // Constants -----------------------------------------------------

   private static final int RECEIVE_TIMEOUT = 5000;
   
   private static final int NUM_THREADS = 10;

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
      runTestMultipleThreads(new RunnableT()
      {
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception
         {
            doTestA(sf, threadNum);
         }
      }, NUM_THREADS);
   }
   
   public void testB() throws Exception
   {
      runTestMultipleThreads(new RunnableT()
      {
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception
         {
            doTestB(sf, threadNum);
         }
      }, NUM_THREADS);
   }
   
   public void testC() throws Exception
   {
      runTestMultipleThreads(new RunnableT()
      {
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception
         {
            doTestC(sf, threadNum);
         }
      }, NUM_THREADS);
   }
   
   public void testD() throws Exception
   {
      runTestMultipleThreads(new RunnableT()
      {
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception
         {
            doTestD(sf, threadNum);
         }
      }, NUM_THREADS);
   }

   public void testE() throws Exception
   {
      runTestMultipleThreads(new RunnableT()
      {
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception
         {
            doTestE(sf, threadNum);
         }
      }, NUM_THREADS);
   }

   public void testF() throws Exception
   {
      runTestMultipleThreads(new RunnableT()
      {
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception
         {
            doTestF(sf, threadNum);
         }
      }, NUM_THREADS);
   }

   public void testG() throws Exception
   {
      runTestMultipleThreads(new RunnableT()
      {
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception
         {
            doTestG(sf, threadNum);
         }
      }, NUM_THREADS);
   }
   
   public void testH() throws Exception
   {
      runTestMultipleThreads(new RunnableT()
      {
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception
         {
            doTestH(sf, threadNum);
         }
      }, NUM_THREADS);
   }
   
   public void testI() throws Exception
   {
      runTestMultipleThreads(new RunnableT()
      {
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception
         {
            doTestI(sf, threadNum);
         }
      }, NUM_THREADS);
   }
   
   public void testJ() throws Exception
   {
      runTestMultipleThreads(new RunnableT()
      {
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception
         {
            doTestJ(sf, threadNum);
         }
      }, NUM_THREADS);
   }
   
   public void testK() throws Exception
   {
      runTestMultipleThreads(new RunnableT()
      {
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception
         {
            doTestK(sf, threadNum);
         }
      }, NUM_THREADS);
   }
   
   public void testL() throws Exception
   {
      runTestMultipleThreads(new RunnableT()
      {
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception
         {
            doTestL(sf);
         }
      }, NUM_THREADS);
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
   protected void doTestA(final ClientSessionFactory sf, final int threadNum) throws Exception
   {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false, false);

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString(threadNum + "sub" + i);

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
            try
            {
               message.acknowledge();
            }
            catch (MessagingException me)
            {
               log.error("Failed to process", me);
            }
            
            if (count >= numMessages)
            {
               return;
            }

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
         SimpleString subName = new SimpleString(threadNum + "sub" + i);

         s.deleteQueue(subName);
      }

      s.close();

      long end = System.currentTimeMillis();

      log.info("duration " + (end - start));
   }

   protected void doTestB(final ClientSessionFactory sf, final int threadNum) throws Exception
   {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false, false);

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString(threadNum + "sub" + i);

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
            try
            {
               message.acknowledge();
            }
            catch (MessagingException me)
            {
               log.error("Failed to process", me);
            }
            
            if (count >= numMessages)
            {
               return;
            }

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
         SimpleString subName = new SimpleString(threadNum + "sub" + i);

         s.deleteQueue(subName);
      }

      s.close();

      long end = System.currentTimeMillis();

      log.info("duration " + (end - start));

   }

   protected void doTestC(final ClientSessionFactory sf, final int threadNum) throws Exception
   {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false, false);

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString(threadNum + "sub" + i);

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
            try
            {
               message.acknowledge();
            }
            catch (MessagingException me)
            {
               log.error("Failed to process", me);
            }
            
            if (count >= numMessages)
            {
               return;
            }

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
         SimpleString subName = new SimpleString(threadNum + "sub" + i);

         s.deleteQueue(subName);
      }

      s.close();

      long end = System.currentTimeMillis();

      log.info("duration " + (end - start));
   }

   protected void doTestD(final ClientSessionFactory sf, final int threadNum) throws Exception
   {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false, false);

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString(threadNum + " sub" + i);

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
            try
            {
               message.acknowledge();
            }
            catch (MessagingException me)
            {
               log.error("Failed to process", me);
            }
            
            if (count >= numMessages)
            {
               return;
            }

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
         SimpleString subName = new SimpleString(threadNum + " sub" + i);

         s.deleteQueue(subName);
      }

      s.close();

      long end = System.currentTimeMillis();

      log.info("duration " + (end - start));
   }

   // Now with synchronous receive()

   protected void doTestE(final ClientSessionFactory sf, final int threadNum) throws Exception
   {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false, false);

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString(threadNum + "sub" + i);

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

            msg.acknowledge();
         }
      }

      sessSend.close();
      for (ClientSession session : sessions)
      {
         session.close();
      }

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString(threadNum + "sub" + i);

         s.deleteQueue(subName);
      }

      s.close();

      long end = System.currentTimeMillis();

      log.info("duration " + (end - start));
   }

   protected void doTestF(final ClientSessionFactory sf, final int threadNum) throws Exception
   {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false, false);

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString(threadNum + "sub" + i);

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

            msg.acknowledge();
         }
      }

      sessSend.close();
      for (ClientSession session : sessions)
      {
         session.close();
      }

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString(threadNum + "sub" + i);

         s.deleteQueue(subName);
      }

      s.close();

      long end = System.currentTimeMillis();

      log.info("duration " + (end - start));
   }

   protected void doTestG(final ClientSessionFactory sf, final int threadNum) throws Exception
   {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false, false);

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString(threadNum + "sub" + i);

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

            msg.acknowledge();
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

            msg.acknowledge();
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
         SimpleString subName = new SimpleString(threadNum + "sub" + i);

         s.deleteQueue(subName);
      }

      s.close();

      long end = System.currentTimeMillis();

      log.info("duration " + (end - start));
   }

   protected void doTestH(final ClientSessionFactory sf, final int threadNum) throws Exception
   {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false, false);

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString(threadNum + "sub" + i);

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

            msg.acknowledge();
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

            msg.acknowledge();
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
         SimpleString subName = new SimpleString(threadNum + "sub" + i);

         s.deleteQueue(subName);
      }

      s.close();

      long end = System.currentTimeMillis();

      log.info("duration " + (end - start));
   }

   protected void doTestI(final ClientSessionFactory sf, final int threadNum) throws Exception
   {
      ClientSession sessCreate = sf.createSession(false, true, true, false);

      sessCreate.createQueue(ADDRESS, new SimpleString(threadNum + ADDRESS.toString()), null, false, false);

      ClientSession sess = sf.createSession(false, true, true, false);

      sess.start();

      ClientConsumer consumer = sess.createConsumer(new SimpleString(threadNum + ADDRESS.toString()));

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

      sessCreate.deleteQueue(new SimpleString(threadNum + ADDRESS.toString()));

      sessCreate.close();
   }

   protected void doTestJ(final ClientSessionFactory sf, final int threadNum) throws Exception
   {
      ClientSession sessCreate = sf.createSession(false, true, true, false);

      sessCreate.createQueue(ADDRESS, new SimpleString(threadNum + ADDRESS.toString()), null, false, false);

      ClientSession sess = sf.createSession(false, true, true, false);

      sess.start();

      ClientConsumer consumer = sess.createConsumer(new SimpleString(threadNum + ADDRESS.toString()));

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

      sessCreate.deleteQueue(new SimpleString(threadNum + ADDRESS.toString()));

      sessCreate.close();
   }

   protected void doTestK(final ClientSessionFactory sf, final int threadNum) throws Exception
   {
      ClientSession s = sf.createSession(false, false, false, false);

      s.createQueue(ADDRESS, new SimpleString(threadNum + ADDRESS.toString()), null, false, false);

      final int numConsumers = 100;

      for (int i = 0; i < numConsumers; i++)
      {
         ClientConsumer consumer = s.createConsumer(new SimpleString(threadNum + ADDRESS.toString()));

         consumer.close();
      }

      s.deleteQueue(new SimpleString(threadNum + ADDRESS.toString()));

      s.close();
   }

   protected void doTestL(final ClientSessionFactory sf) throws Exception
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

   protected int getNumIterations()
   {
      return 20;
   }
   
   protected void setUp() throws Exception
   {
      super.setUp();
      
      log.info("************ Starting test " + this.getName());
      
      timer = new Timer();
      
   }
   
   protected void tearDown() throws Exception
   {
      log.info("************* Ending test " + this.getName());
      
      timer.cancel();
      
      super.tearDown();
   }

   // Private -------------------------------------------------------

   private void runTestMultipleThreads(final RunnableT runnable, final int numThreads) throws Exception
   {
      final int numIts = getNumIterations();

      for (int its = 0; its < numIts; its++)
      {
         log.info("************ ITERATION: " + its);
         start();

         final ClientSessionFactoryImpl sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                          new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                     backupParams));

         ClientSession session = sf.createSession(false, false, false, false);

         Failer failer = startFailer(1000, session);

         class Runner extends Thread
         {
            private volatile Throwable throwable;

            private final RunnableT test;

            private final int threadNum;

            Runner(final RunnableT test, final int threadNum)
            {
               this.test = test;

               this.threadNum = threadNum;
            }

            public void run()
            {
               try
               {
                  test.run(sf, threadNum);
               }
               catch (Throwable t)
               {
                  throwable = t;

                  log.error("Failed to run test", t);
               }
            }
         }
         
         do
         {
            List<Runner> threads = new ArrayList<Runner>();
            
            for (int i = 0; i < numThreads; i++)
            {
               Runner runner = new Runner(runnable, i);

               threads.add(runner);

               runner.start();
            }
            
            for (Runner thread : threads)
            {
               thread.join();               

               assertNull(thread.throwable);
            }
            
            runnable.checkFail();
         }
         while (!failer.isExecuted());
         
         session.close();

         assertEquals(0, sf.getSessionCount());

         stop();
      }
   }
   
   private Failer startFailer(final long time, final ClientSession session)
   {
      Failer failer = new Failer(session);

      timer.schedule(failer, (long)(time * Math.random()), 100);
      
      return failer;
   }

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

   // Inner classes -------------------------------------------------

   class Failer extends TimerTask
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

   public abstract class RunnableT extends Thread
   {
      private volatile String failReason;
      private volatile Throwable throwable;
      
      public void setFailed(final String reason, final Throwable throwable)
      {
         this.failReason = reason;
         this.throwable = throwable;
      }
      
      public void checkFail()
      {
         if (throwable != null)
         {
            log.error("Test failed: " + failReason, throwable);
         }
         if (failReason != null)
         {
            fail(failReason);
         }
      }
      public abstract void run(final ClientSessionFactory sf, final int threadNum) throws Exception;
   }
}
