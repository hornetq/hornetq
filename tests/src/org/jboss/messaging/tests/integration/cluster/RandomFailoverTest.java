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
import org.jboss.messaging.core.remoting.RemotingConnection;
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

   //private volatile Thread failThread;
   
   private Timer timer = new Timer();
   
   private volatile Failer failer;
   
   private void startFailer(final long time)
   {      
      failer = new Failer();
      
      timer.schedule(failer, (long)(time * Math.random()), Long.MAX_VALUE);
   }

   private static class Failer extends TimerTask
   {
      volatile ClientSession session;
                    
      public void run()
      {
         if (session != null)
         {
            log.info("** Failing connection");
            
            RemotingConnectionImpl conn = (RemotingConnectionImpl)((ClientSessionImpl)session).getConnection();
            
            conn.fail(new MessagingException(MessagingException.NOT_CONNECTED, "blah"));
            
            log.info("** Fail complete");
            
            session = null;
            
            cancel();
         }
      }
   }
   
   public void testFailureA() throws Exception
   {
      for (int its = 0; its < 1000; its++)
      {      
         start();                  
                           
         startFailer(3000);
         
         ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                           backupParams));         
         do
         {         
            testA(sf);
         }
         while (failer.session != null);
               
         stop();
      }
   }
   
   public void testAA(final ClientSessionFactory sf) throws Exception
   {                       
      ClientSession s = sf.createSession(false, false, false, false);
      
      s.createQueue(ADDRESS, ADDRESS, null, false, false);
      
      failer.session = s;
                              
      final int numConsumers = 100;
         
      for (int i = 0; i < numConsumers; i++)
      {
         ClientConsumer consumer = s.createConsumer(ADDRESS);
         
         consumer.close();
      }
      
      s.deleteQueue(ADDRESS);
      
      s.close();
      
      log.info("done");
   }
   
   public void testA(final ClientSessionFactory sf) throws Exception
   {      
      long start = System.currentTimeMillis();
                        
      ClientSession s = sf.createSession(false, false, false, false);
      
      failer.session = s;
                              
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
      
     // log.info("sent messages");
            
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
      for (ClientSession session: sessions)
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
   
   public void testB() throws Exception
   {
      start();

      long start = System.currentTimeMillis();

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                             new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                        backupParams));

      final int numMessages = 1000;

      final int numSessions = 100;

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
      
      for (ClientSession session: sessions)
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
      
      for (ClientSession session: sessions)
      {
         session.close();
      }

      long end = System.currentTimeMillis();

      log.info("duration " + (end - start));

      // this.waitForFailThread();

      stop();
   }
   
   public void testC() throws Exception
   {
      start();

      long start = System.currentTimeMillis();

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                             new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                        backupParams));

      final int numMessages = 1000;

      final int numSessions = 100;

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
      
      //New handlers
      for (ClientConsumer consumer : consumers)
      {
         MyHandler handler = new MyHandler();

         consumer.setMessageHandler(handler);

         handlers.add(handler);
      }
      
      for (ClientSession session: sessions)
      {
         session.rollback();
      }
      
      for (MyHandler handler : handlers)
      {
         boolean ok = handler.latch.await(10000, TimeUnit.MILLISECONDS);

         assertTrue(ok);
      }
      
      for (ClientSession session: sessions)
      {
         session.commit();
      }

      sessSend.close();
      for (ClientSession session: sessions)
      {
         session.close();
      }

      long end = System.currentTimeMillis();

      log.info("duration " + (end - start));

      // this.waitForFailThread();

      stop();
   }
   
   public void testD() throws Exception
   {
      start();

      long start = System.currentTimeMillis();

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                             new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                        backupParams));

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
      
      for (ClientSession session: sessions)
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
      
      //New handlers
      for (ClientConsumer consumer : consumers)
      {
         MyHandler handler = new MyHandler();

         consumer.setMessageHandler(handler);

         handlers.add(handler);
      }
      
      for (ClientSession session: sessions)
      {
         session.rollback();
      }
      
      for (MyHandler handler : handlers)
      {
         boolean ok = handler.latch.await(10000, TimeUnit.MILLISECONDS);

         assertTrue(ok);
      }
      
      for (ClientSession session: sessions)
      {
         session.commit();
      }

      sessSend.close();
      for (ClientSession session: sessions)
      {
         session.close();
      }

      long end = System.currentTimeMillis();

      log.info("duration " + (end - start));

      // this.waitForFailThread();

      stop();
   }
   
   // Now with synchronous receive()
   
   
   public void testE() throws Exception
   {
      start();

      long start = System.currentTimeMillis();

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                             new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                        backupParams));

      final int numMessages = 1000;

      final int numSessions = 100;

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
      for (ClientSession session: sessions)
      {
         session.close();
      }

      long end = System.currentTimeMillis();

      log.info("duration " + (end - start));

      // this.waitForFailThread();

      stop();
   }
   
   public void testF() throws Exception
   {
      start();

      long start = System.currentTimeMillis();

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                             new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                        backupParams));

      final int numMessages = 1000;

      final int numSessions = 100;

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
      
      for (ClientSession session: sessions)
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
      for (ClientSession session: sessions)
      {
         session.close();
      }

      long end = System.currentTimeMillis();

      log.info("duration " + (end - start));

      // this.waitForFailThread();

      stop();
   }
   
   public void testG() throws Exception
   {
      start();

      long start = System.currentTimeMillis();

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                             new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                        backupParams));

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
                  
      for (int i = 0; i < numMessages; i++)
      {
         for (ClientConsumer consumer : consumers)
         {
            ClientMessage msg = consumer.receiveImmediate();
            
            assertNull(msg);
         }
      }
      
      for (ClientSession session: sessions)
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
         
      for (ClientSession session: sessions)
      {
         session.commit();
      }
      
      sessSend.close();
      for (ClientSession session: sessions)
      {
         session.close();
      }

      long end = System.currentTimeMillis();

      log.info("duration " + (end - start));

      // this.waitForFailThread();

      stop();
   }
   
      
   public void testH() throws Exception
   {
      start();

      long start = System.currentTimeMillis();

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                             new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                        backupParams));

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
      
      for (ClientSession session: sessions)
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
      
      for (ClientSession session: sessions)
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
         
      for (ClientSession session: sessions)
      {
         session.commit();
      }
      
      sessSend.close();
      for (ClientSession session: sessions)
      {
         session.close();
      }

      long end = System.currentTimeMillis();

      log.info("duration " + (end - start));

      // this.waitForFailThread();

      stop();
   }
   
   public void testI() throws Exception
   {
      start();

      long start = System.currentTimeMillis();

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                             new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                        backupParams));

      final int numIts = 1000;
            
      for (int i = 0; i < numIts; i++)
      {      
         //log.info("iteration " + i);
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

      // this.waitForFailThread();

      stop();
   }
   
   public void testJ() throws Exception
   {
      start();

      long start = System.currentTimeMillis();

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                             new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                        backupParams));

      ClientSession sessHold = sf.createSession(false, true, true, false);
      
      final int numIts = 1000;
            
      for (int i = 0; i < numIts; i++)
      {      
         //log.info("iteration " + i);
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
      
      sessHold.close();

      // this.waitForFailThread();

      stop();
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

      assertEquals(0, backupService.getServer().getRemotingService().getConnections().size());

      backupService.stop();

      assertEquals(0, liveService.getServer().getRemotingService().getConnections().size());

      liveService.stop();

      assertEquals(0, InVMRegistry.instance.size());
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
