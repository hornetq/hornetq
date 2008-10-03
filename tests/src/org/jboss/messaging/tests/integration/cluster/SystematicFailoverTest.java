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
 * A SystematicFailoverTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class SystematicFailoverTest extends TestCase
{
   private static final Logger log = Logger.getLogger(SimpleAutomaticFailoverTest.class);

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private static final SimpleString ADDRESS = new SimpleString("FailoverTestAddress");

   private MessagingService liveService;

   private MessagingService backupService;

   private final Map<String, Object> backupParams = new HashMap<String, Object>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   private volatile Thread failThread;

   private void setFailAfterRandomTime(final RemotingConnection conn)
   {
      failThread = new Thread()
      {
         public void run()
         {
            try
            {
               Thread.sleep((long)(300 * Math.random()));

               log.info("Failing");
               conn.fail(new MessagingException(MessagingException.NOT_CONNECTED, "blah"));
            }
            catch (InterruptedException ignore)
            {
            }
         }
      };

      failThread.start();
   }

   private void waitForFailThread() throws Exception
   {
      if (failThread != null)
      {
         failThread.join();
      }
   }

   public void testExerciseAPI() throws Exception
   {
      for (int its = 0; its < 1000; its++)
      {         
         start();

         long start = System.currentTimeMillis();

         ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                           backupParams));

         // Non transacted
         ClientSession session1 = sf.createSession(false, true, true, false);

         RemotingConnectionImpl conn = (RemotingConnectionImpl)((ClientSessionImpl)session1).getConnection();

         setFailAfterRandomTime(conn);

         session1.createQueue(ADDRESS, ADDRESS, null, false, false);

         final int numMessages = 1000;

         ClientProducer producer1 = session1.createProducer(ADDRESS);

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = session1.createClientMessage(JBossTextMessage.TYPE,
                                                                 false,
                                                                 0,
                                                                 System.currentTimeMillis(),
                                                                 (byte)1);
            message.putIntProperty(new SimpleString("count"), i);
            message.getBody().putString("aardvarks");
            message.getBody().flip();
            producer1.send(message);
         }

         ClientSession session2 = sf.createSession(false, true, true, false);

         session2.start();

         ClientConsumer consumer1 = session2.createConsumer(ADDRESS);

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = consumer1.receive();

            assertEquals("aardvarks", message.getBody().getString());
            assertEquals(i, message.getProperty(new SimpleString("count")));

            message.processed();
         }

         producer1.close();

         ClientProducer producer2 = session1.createProducer(ADDRESS);

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = session2.createClientMessage(JBossTextMessage.TYPE,
                                                                 false,
                                                                 0,
                                                                 System.currentTimeMillis(),
                                                                 (byte)1);
            message.putIntProperty(new SimpleString("count"), i);
            message.getBody().putString("aardvarks");
            message.getBody().flip();
            producer2.send(message);
         }

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = consumer1.receive();

            assertEquals("aardvarks", message.getBody().getString());
            assertEquals(i, message.getProperty(new SimpleString("count")));

            message.processed();
         }

         consumer1.close();

         // Transacted
         ClientSession session3 = sf.createSession(false, false, false, false);

         ClientProducer producer3 = session3.createProducer(ADDRESS);

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = session3.createClientMessage(JBossTextMessage.TYPE,
                                                                 false,
                                                                 0,
                                                                 System.currentTimeMillis(),
                                                                 (byte)1);
            message.putIntProperty(new SimpleString("count"), i);
            message.getBody().putString("aardvarks");
            message.getBody().flip();
            producer3.send(message);

            session3.rollback();

            message = session3.createClientMessage(JBossTextMessage.TYPE, false, 0, System.currentTimeMillis(), (byte)1);
            message.putIntProperty(new SimpleString("count"), i);
            message.getBody().putString("aardvarks");
            message.getBody().flip();
            producer3.send(message);

            session3.commit();
         }

         ClientConsumer consumer2 = session3.createConsumer(ADDRESS);

         session3.start();

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = consumer2.receive();

            assertEquals("aardvarks", message.getBody().getString());
            assertEquals(i, message.getProperty(new SimpleString("count")));

            message.processed();
         }

         session3.rollback();

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = consumer2.receive();

            assertEquals("aardvarks", message.getBody().getString());
            assertEquals(i, message.getProperty(new SimpleString("count")));

            message.processed();
         }

         session3.commit();

         session1.close();

         session2.close();

         final int numConsumers = 10;

         Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();

         final ClientSession session4 = sf.createSession(false, true, true, false);

         session4.start();

         for (int i = 0; i < numConsumers; i++)
         {
            SimpleString wibble = new SimpleString("wibble");

            session4.addDestination(wibble, false, false);

            session4.removeDestination(wibble, false);

            SimpleString subName = new SimpleString("sub" + i);

            session4.createQueue(ADDRESS, subName, null, false, false);

            ClientConsumer consumer = session4.createConsumer(subName);

            consumers.add(consumer);
         }

         ClientProducer producer4 = session4.createProducer(ADDRESS);

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = session4.createClientMessage(JBossTextMessage.TYPE,
                                                                 false,
                                                                 0,
                                                                 System.currentTimeMillis(),
                                                                 (byte)1);
            message.putIntProperty(new SimpleString("count"), i);
            message.getBody().putString("aardvarks");
            message.getBody().flip();
            producer4.send(message);
         }

         // Consume synchronously
         for (ClientConsumer consumer : consumers)
         {
            for (int i = 0; i < numMessages; i++)
            {
               ClientMessage message = consumer.receive();

               assertEquals("aardvarks", message.getBody().getString());
               assertEquals(i, message.getProperty(new SimpleString("count")));

               message.processed();
            }
         }

         class MyHandler implements MessageHandler
         {
            final CountDownLatch latch = new CountDownLatch(1);

            volatile int count;

            public void onMessage(ClientMessage message)
            {
               assertEquals(count, message.getProperty(new SimpleString("count")));

               // log.info(this + " got message " + count);

               try
               {
                  message.processed();
               }
               catch (MessagingException me)
               {
                  fail();
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

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = session4.createClientMessage(JBossTextMessage.TYPE,
                                                                 false,
                                                                 0,
                                                                 System.currentTimeMillis(),
                                                                 (byte)1);
            message.putIntProperty(new SimpleString("count"), i);
            message.getBody().putString("aardvarks");
            message.getBody().flip();
            producer4.send(message);
         }

         for (MyHandler handler : handlers)
         {
            boolean ok = handler.latch.await(1000, TimeUnit.MILLISECONDS);

            assertTrue(ok);
         }

         session3.close();

         session4.close();

         long end = System.currentTimeMillis();

         log.info("iteration" + its + " duration " + (end - start));

         this.waitForFailThread();

         stop();
      }
   }

   public void testReplicateWithHandlers() throws Exception
   {
      for (int its = 0; its < 100; its++)
      {
         log.info("Starting iteration " + its);

         start();

         long start = System.currentTimeMillis();

         ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                           backupParams));

         final int numMessages = 100;

         final int numConsumers = 10;

         Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();

         ClientSession sessConsume = sf.createSession(false, true, true, false);
         
         sessConsume.start();
         
         for (int i = 0; i < numConsumers; i++)
         {
            SimpleString subName = new SimpleString("sub" + i);

            sessConsume.createQueue(ADDRESS, subName, null, false, false);

            ClientConsumer consumer = sessConsume.createConsumer(subName);

            consumers.add(consumer);
         }
         
         ClientSession sessSend = sf.createSession(false, true, true, false);         

         ClientProducer producer4 = sessSend.createProducer(ADDRESS);

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = sessSend.createClientMessage(JBossTextMessage.TYPE,
                                                                false,
                                                                0,
                                                                System.currentTimeMillis(),
                                                                (byte)1);
            message.putIntProperty(new SimpleString("count"), i);
            message.getBody().putString("aardvarks");
            message.getBody().flip();
            producer4.send(message);
         }
         
         
         class MyHandler implements MessageHandler
         {
            final CountDownLatch latch = new CountDownLatch(1);

            volatile int count;

            public void onMessage(ClientMessage message)
            {
               //log.info("got message " + message.getMessageID());
               
               assertEquals(count, message.getProperty(new SimpleString("count")));

               //log.info(this + " got message " + count);

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
            boolean ok = handler.latch.await(1000, TimeUnit.MILLISECONDS);

            assertTrue(ok);
         }

         sessSend.close();
         sessConsume.close();

         long end = System.currentTimeMillis();

         log.info("duration " + (end - start));

         // this.waitForFailThread();

         stop();
      }
   }

   // public void testHangOnCreateSession(final byte type) throws Exception
   // {
   // for (int j = 0; j < 1000; j++)
   // {
   // start();
   //
   // ClientSessionFactory sf = new ClientSessionFactoryImpl(new
   // TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
   // new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
   // backupParams));
   //
   // final int numSessions = 50;
   //
   // List<ClientSession> sessions = new ArrayList<ClientSession>();
   //
   // for (int i = 0; i < numSessions; i++)
   // {
   // ClientSession session = sf.createSession(false, true, true, -1, false);
   //
   // sessions.add(session);
   //
   // if (i == 0)
   // {
   // RemotingConnectionImpl conn = (RemotingConnectionImpl)((ClientSessionImpl)session).getConnection();
   //
   // //conn.failOnPacketType(type);
   // }
   // }
   //
   // for (ClientSession session : sessions)
   // {
   // session.close();
   // }
   //
   // stop();
   // }
   //
   // }

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
