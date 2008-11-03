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
import org.jboss.messaging.core.client.impl.ClientSessionFactoryInternal;
import org.jboss.messaging.core.client.impl.ClientSessionImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
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

   private static final int RECEIVE_TIMEOUT = 30000;

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

   // public void testM() throws Exception
   // {
   // runTestMultipleThreads(new RunnableT()
   // {
   // public void run(final ClientSessionFactory sf, final int threadNum) throws Exception
   // {
   // doTestM(sf, threadNum);
   // }
   // }, NUM_THREADS);
   // }

   public void testN() throws Exception
   {
      runTestMultipleThreads(new RunnableT()
      {
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception
         {
            doTestN(sf, threadNum);
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

      sendMessages(sessSend, producer, numMessages, threadNum);

      Set<MyHandler> handlers = new HashSet<MyHandler>();

      for (ClientConsumer consumer : consumers)
      {
         MyHandler handler = new MyHandler(threadNum, numMessages);

         consumer.setMessageHandler(handler);

         handlers.add(handler);
      }

      for (MyHandler handler : handlers)
      {
         boolean ok = handler.latch.await(5000, TimeUnit.MILLISECONDS);

         if (!ok)
         {
            throw new Exception("Timed out waiting for messages on handler " + System.identityHashCode(handler) + " threadnum " + threadNum);
         }

         if (handler.failure != null)
         {
            throw new Exception("Handler failed: " + handler.failure);
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

      sendMessages(sessSend, producer, numMessages, threadNum);

      for (ClientSession session : sessions)
      {
         session.start();
      }

      Set<MyHandler> handlers = new HashSet<MyHandler>();

      for (ClientConsumer consumer : consumers)
      {
         MyHandler handler = new MyHandler(threadNum, numMessages);

         consumer.setMessageHandler(handler);

         handlers.add(handler);
      }

      for (MyHandler handler : handlers)
      {
         boolean ok = handler.latch.await(10000, TimeUnit.MILLISECONDS);

         if (!ok)
         {
            throw new Exception("Timed out waiting for messages on handler " + System.identityHashCode(handler) + " threadnum " + threadNum);
         }

         if (handler.failure != null)
         {
            throw new Exception("Handler failed: " + handler.failure);
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

      ClientSession sessSend = sf.createSession(false, false, false, false);

      ClientProducer producer = sessSend.createProducer(ADDRESS);

      sendMessages(sessSend, producer, numMessages, threadNum);

      sessSend.rollback();

      sendMessages(sessSend, producer, numMessages, threadNum);

      sessSend.commit();

      Set<MyHandler> handlers = new HashSet<MyHandler>();

      for (ClientConsumer consumer : consumers)
      {
         MyHandler handler = new MyHandler(threadNum, numMessages);

         consumer.setMessageHandler(handler);

         handlers.add(handler);
      }

      for (MyHandler handler : handlers)
      {
         boolean ok = handler.latch.await(10000, TimeUnit.MILLISECONDS);

         if (!ok)
         {
            throw new Exception("Timed out waiting for messages on handler " + System.identityHashCode(handler) + " threadnum " + threadNum);
         }

         if (handler.failure != null)
         {
            throw new Exception("Handler failed: " + handler.failure);
         }
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
      
      ClientSession sessSend = sf.createSession(false, false, false, false);

      ClientProducer producer = sessSend.createProducer(ADDRESS);

      sendMessages(sessSend, producer, numMessages, threadNum);

      sessSend.rollback();
      
      sendMessages(sessSend, producer, numMessages, threadNum);
      
      sessSend.commit();
      

      for (ClientSession session : sessions)
      {
         session.start();
      }
       
      Set<MyHandler> handlers = new HashSet<MyHandler>();

      for (ClientConsumer consumer : consumers)
      {
         MyHandler handler = new MyHandler(threadNum, numMessages);

         consumer.setMessageHandler(handler);

         handlers.add(handler);
      }

      for (MyHandler handler : handlers)
      {
         boolean ok = handler.latch.await(10000, TimeUnit.MILLISECONDS);

         if (!ok)
         {
            throw new Exception("Timed out waiting for messages on handler " + System.identityHashCode(handler) + " threadnum " + threadNum);
         }

         if (handler.failure != null)
         {
            throw new Exception("Handler failed: " + handler.failure);
         }
      }
      
      handlers.clear();
      
      // Set handlers to null
      for (ClientConsumer consumer : consumers)
      {
         consumer.setMessageHandler(null);
      }

      for (ClientSession session : sessions)
      {
         session.rollback();
      }
      
      // New handlers
      for (ClientConsumer consumer : consumers)
      {
         MyHandler handler = new MyHandler(threadNum, numMessages);

         consumer.setMessageHandler(handler);

         handlers.add(handler);
      }
      
      for (MyHandler handler : handlers)
      {
         boolean ok = handler.latch.await(10000, TimeUnit.MILLISECONDS);

         if (!ok)
         {
            throw new Exception("Timed out waiting for messages on handler " + System.identityHashCode(handler) + " threadnum " + threadNum);
         }
         
         if (handler.failure != null)
         {
            throw new Exception("Handler failed on rollback: " + handler.failure);
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

      sendMessages(sessSend, producer, numMessages, threadNum);

      consumeMessages(consumers, numMessages, threadNum);

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

      sendMessages(sessSend, producer, numMessages, threadNum);

      for (ClientSession session : sessions)
      {
         session.start();
      }

      consumeMessages(consumers, numMessages, threadNum);

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

      sendMessages(sessSend, producer, numMessages, threadNum);

      sessSend.rollback();

      sendMessages(sessSend, producer, numMessages, threadNum);

      sessSend.commit();

      consumeMessages(consumers, numMessages, threadNum);

      for (ClientSession session : sessions)
      {
         session.rollback();
      }

      consumeMessages(consumers, numMessages, threadNum);

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

      sendMessages(sessSend, producer, numMessages, threadNum);

      sessSend.rollback();

      sendMessages(sessSend, producer, numMessages, threadNum);

      sessSend.commit();

      for (ClientSession session : sessions)
      {
         session.start();
      }

      consumeMessages(consumers, numMessages, threadNum);

      for (ClientSession session : sessions)
      {
         session.rollback();
      }

      consumeMessages(consumers, numMessages, threadNum);

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

   // Browsers
   // FIXME - this test won't work until we use a proper iterator for browsing a queue.
   // Making a copy of the queue for a browser consumer doesn't work well with replication since
   // When replicating the create consumer (browser) to the backup, when executed on the backup the
   // backup may have different messages in its queue since been added on different threads.
   // So when replicating deliveries they may not be found.
   // https://jira.jboss.org/jira/browse/JBMESSAGING-1433
   // protected void doTestM(final ClientSessionFactory sf, final int threadNum) throws Exception
   // {
   // long start = System.currentTimeMillis();
   //
   // ClientSession sessSend = sf.createSession(false, true, true, false);
   //      
   // ClientSession sessConsume = sf.createSession(false, true, true, false);
   //      
   // sessConsume.createQueue(ADDRESS, new SimpleString(threadNum + "sub"), null, false, false);
   //
   // final int numMessages = 100;
   //
   // ClientProducer producer = sessSend.createProducer(ADDRESS);
   //
   // sendMessages(sessSend, producer, numMessages, threadNum);
   //      
   // ClientConsumer browser = sessConsume.createConsumer(new SimpleString(threadNum + "sub"),
   // null, false, true);
   //      
   // Map<Integer, Integer> consumerCounts = new HashMap<Integer, Integer>();
   //      
   // for (int i = 0; i < numMessages; i++)
   // {
   // ClientMessage msg = browser.receive(RECEIVE_TIMEOUT);
   //
   // assertNotNull(msg);
   //
   // int tn = (Integer)msg.getProperty(new SimpleString("threadnum"));
   // int cnt = (Integer)msg.getProperty(new SimpleString("count"));
   //
   // Integer c = consumerCounts.get(tn);
   // if (c == null)
   // {
   // c = new Integer(cnt);
   // }
   //
   // if (cnt != c.intValue())
   // {
   // throw new Exception("Invalid count, expected " + c + " got " + cnt);
   // }
   //         
   // c++;
   //         
   // //Wrap
   // if (c == numMessages)
   // {
   // c = 0;
   // }
   //         
   // consumerCounts.put(tn, c);
   //
   // msg.acknowledge();
   // }
   //
   // sessConsume.close();
   //      
   // sessConsume = sf.createSession(false, true, true, false);
   //      
   // browser = sessConsume.createConsumer(new SimpleString(threadNum + "sub"),
   // null, false, true);
   //      
   // //Messages should still be there
   //      
   // consumerCounts.clear();
   //      
   // for (int i = 0; i < numMessages; i++)
   // {
   // ClientMessage msg = browser.receive(RECEIVE_TIMEOUT);
   //
   // assertNotNull(msg);
   //
   // int tn = (Integer)msg.getProperty(new SimpleString("threadnum"));
   // int cnt = (Integer)msg.getProperty(new SimpleString("count"));
   //
   // Integer c = consumerCounts.get(tn);
   // if (c == null)
   // {
   // c = new Integer(cnt);
   // }
   //
   // if (cnt != c.intValue())
   // {
   // throw new Exception("Invalid count, expected " + c + " got " + cnt);
   // }
   //         
   // c++;
   //         
   // //Wrap
   // if (c == numMessages)
   // {
   // c = 0;
   // }
   //         
   // consumerCounts.put(tn, c);
   //
   // msg.acknowledge();
   // }
   //      
   // sessConsume.close();
   //      
   // sessSend.deleteQueue(new SimpleString(threadNum + "sub"));
   //      
   // sessSend.close();
   //
   // long end = System.currentTimeMillis();
   //
   // log.info("duration " + (end - start));
   // }

   protected void doTestN(final ClientSessionFactory sf, final int threadNum) throws Exception
   {
      ClientSession sessCreate = sf.createSession(false, true, true, false);

      sessCreate.createQueue(ADDRESS, new SimpleString(threadNum + ADDRESS.toString()), null, false, false);

      ClientSession sess = sf.createSession(false, true, true, false);

      sess.stop();

      sess.start();

      sess.stop();

      ClientConsumer consumer = sess.createConsumer(new SimpleString(threadNum + ADDRESS.toString()));

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

      sessCreate.deleteQueue(new SimpleString(threadNum + ADDRESS.toString()));

      sessCreate.close();
   }

   protected int getNumIterations()
   {
      return 50;
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

      if (liveService != null && liveService.isStarted())
      {
         liveService.stop();
      }
      if (backupService != null && backupService.isStarted())
      {
         backupService.stop();
      }
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

         final ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                              new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                         backupParams));
         
         sf.setSendWindowSize(32 * 1024);

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

         assertEquals(0, sf.numSessions());

         assertEquals(0, sf.numConnections());

         assertEquals(0, sf.numBackupConnections());

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
      liveConf.getAcceptorConfigurations()
              .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory"));
      liveConf.setBackupConnectorConfiguration(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                          backupParams));
      liveService = MessagingServiceImpl.newNullStorageMessagingServer(liveConf);
      liveService.start();
   }

   private void stop() throws Exception
   {
      assertEquals(0, backupService.getServer().getRemotingService().getConnections().size());

      backupService.stop();

      assertEquals(0, liveService.getServer().getRemotingService().getConnections().size());

      liveService.stop();

      assertEquals(0, InVMRegistry.instance.size());
   }

   private void sendMessages(final ClientSession sessSend,
                             final ClientProducer producer,
                             final int numMessages,
                             final int threadNum) throws Exception
   {
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = sessSend.createClientMessage(JBossTextMessage.TYPE,
                                                              false,
                                                              0,
                                                              System.currentTimeMillis(),
                                                              (byte)1);
         message.putIntProperty(new SimpleString("threadnum"), threadNum);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBody().flip();
         producer.send(message);
      }
   }

   private void consumeMessages(final Set<ClientConsumer> consumers, final int numMessages, final int threadNum) throws Exception
   {
      // We make sure the messages arrive in the order they were sent from a particular producer
      Map<ClientConsumer, Map<Integer, Integer>> counts = new HashMap<ClientConsumer, Map<Integer, Integer>>();

      for (int i = 0; i < numMessages; i++)
      {
         for (ClientConsumer consumer : consumers)
         {
            Map<Integer, Integer> consumerCounts = counts.get(consumer);

            if (consumerCounts == null)
            {
               consumerCounts = new HashMap<Integer, Integer>();
               counts.put(consumer, consumerCounts);
            }

            ClientMessage msg = consumer.receive(RECEIVE_TIMEOUT);

            assertNotNull(msg);

            int tn = (Integer)msg.getProperty(new SimpleString("threadnum"));
            int cnt = (Integer)msg.getProperty(new SimpleString("count"));
            
           // log.info("Got message " + tn + ":" + cnt);

            Integer c = consumerCounts.get(tn);
            if (c == null)
            {
               c = new Integer(cnt);
            }

            if (tn == threadNum && cnt != c.intValue())
            {
               throw new Exception("Invalid count, expected " + tn + ": " + c + " got " + cnt);
            }

            c++;

            // Wrap
            if (c == numMessages)
            {
               c = 0;
            }

            consumerCounts.put(tn, c);

            msg.acknowledge();
         }
      }
   }

   // Inner classes -------------------------------------------------

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

   private abstract class RunnableT extends Thread
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

   private class MyHandler implements MessageHandler
   {
      final CountDownLatch latch = new CountDownLatch(1);

      private Map<Integer, Integer> counts = new HashMap<Integer, Integer>();

      volatile String failure;

      final int tn;

      final int numMessages;

      volatile boolean done;

      MyHandler(final int threadNum, final int numMessages)
      {
         this.tn = threadNum;

         this.numMessages = numMessages;
      }

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

         if (done)
         {
            return;
         }

         int threadNum = (Integer)message.getProperty(new SimpleString("threadnum"));
         int cnt = (Integer)message.getProperty(new SimpleString("count"));

         Integer c = counts.get(threadNum);
         if (c == null)
         {
            c = new Integer(cnt);
         }

         //log.info(System.identityHashCode(this) + " consumed message " + threadNum + ":" + cnt);
         
         if (tn == threadNum && cnt != c.intValue())
         {
            failure = "Invalid count, expected " + threadNum + ":" + c + " got " + cnt;            
            log.error(failure);

            latch.countDown();
         }

         if (tn == threadNum && c == numMessages - 1)
         {
            done = true;
            latch.countDown();
         }

         c++;
         // Wrap around at numMessages
         if (c == numMessages)
         {
            c = 0;
         }

         counts.put(threadNum, c);

      }
   }
}
