/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.jboss.messaging.tests.integration.basic;

import junit.framework.TestCase;
import org.jboss.messaging.core.client.*;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.GroupingRoundRobinDistributionPolicy;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.jms.client.JBossTextMessage;
import org.jboss.messaging.util.SimpleString;

import java.util.concurrent.CountDownLatch;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class AutoGroupClientTest extends TestCase
{
   public void testGroupIdAutomaticallySet() throws Exception
   {
      final SimpleString QUEUE = new SimpleString("testGroupQueue");
      QueueSettings qs = new QueueSettings();
      qs.setDistributionPolicyClass(GroupingRoundRobinDistributionPolicy.class.getName());

      Configuration conf = new ConfigurationImpl();

      conf.setSecurityEnabled(false);

      conf.getAcceptorConfigurations().add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory"));

      MessagingService messagingService = MessagingServiceImpl.newNullStorageMessagingServer(conf);

      messagingService.getServer().getQueueSettingsRepository().addMatch("testGroupQueue", qs);
      messagingService.start();

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"));
      sf.setAutoGroupId(true);
      ClientSession session = sf.createSession(false, true, true, false);

      session.createQueue(QUEUE, QUEUE, null, false, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final CountDownLatch latch = new CountDownLatch(100);

      MyMessageHandler myMessageHandler = new MyMessageHandler(latch);
      MyMessageHandler myMessageHandler2 = new MyMessageHandler(latch);

      ClientConsumer consumer = session.createConsumer(QUEUE);
      consumer.setMessageHandler(myMessageHandler);
      ClientConsumer consumer2 = session.createConsumer(QUEUE);
      consumer2.setMessageHandler(myMessageHandler2);

      session.start();

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE, false, 0,
               System.currentTimeMillis(), (byte) 1);
         message.getBody().putString("testINVMCoreClient");
         message.getBody().flip();
         message.setDurable(false);
         producer.send(message);
      }
      latch.await();

      session.close();

      messagingService.stop();

      assertEquals(myMessageHandler.messagesReceived, 100);
      assertEquals(myMessageHandler2.messagesReceived, 0);
   }

   public void testGroupIdAutomaticallySetMultipleProducers() throws Exception
   {
      final SimpleString QUEUE = new SimpleString("testGroupQueue");
      QueueSettings qs = new QueueSettings();
      qs.setDistributionPolicyClass(GroupingRoundRobinDistributionPolicy.class.getName());

      Configuration conf = new ConfigurationImpl();

      conf.setSecurityEnabled(false);

      conf.getAcceptorConfigurations().add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory"));

      MessagingService messagingService = MessagingServiceImpl.newNullStorageMessagingServer(conf);

      messagingService.getServer().getQueueSettingsRepository().addMatch("testGroupQueue", qs);
      messagingService.start();

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"));
      sf.setAutoGroupId(true);
      ClientSession session = sf.createSession(false, true, true, false);

      session.createQueue(QUEUE, QUEUE, null, false, false);

      ClientProducer producer = session.createProducer(QUEUE);
      ClientProducer producer2 = session.createProducer(QUEUE);

      final CountDownLatch latch = new CountDownLatch(200);

      MyMessageHandler myMessageHandler = new MyMessageHandler(latch);
      MyMessageHandler myMessageHandler2 = new MyMessageHandler(latch);
      MyMessageHandler myMessageHandler3 = new MyMessageHandler(latch);

      ClientConsumer consumer = session.createConsumer(QUEUE);
      consumer.setMessageHandler(myMessageHandler);
      ClientConsumer consumer2 = session.createConsumer(QUEUE);
      consumer2.setMessageHandler(myMessageHandler2);
      ClientConsumer consumer3 = session.createConsumer(QUEUE);
      consumer3.setMessageHandler(myMessageHandler3);

      session.start();

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE, false, 0,
               System.currentTimeMillis(), (byte) 1);
         message.getBody().putString("testINVMCoreClient");
         message.getBody().flip();
         producer.send(message);
      }
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE, false, 0,
               System.currentTimeMillis(), (byte) 1);
         message.getBody().putString("testINVMCoreClient");
         message.getBody().flip();
         producer2.send(message);
      }
      latch.await();

      session.close();

      messagingService.stop();

      assertEquals(myMessageHandler.messagesReceived, 100);
      assertEquals(myMessageHandler2.messagesReceived, 100);
      assertEquals(myMessageHandler3.messagesReceived, 0);
   }

   public void testGroupIdAutomaticallyNotSet() throws Exception
   {
      final SimpleString QUEUE = new SimpleString("testGroupQueue");
      QueueSettings qs = new QueueSettings();
      qs.setDistributionPolicyClass(GroupingRoundRobinDistributionPolicy.class.getName());
      Configuration conf = new ConfigurationImpl();

      conf.setSecurityEnabled(false);

      conf.getAcceptorConfigurations().add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory"));

      MessagingService messagingService = MessagingServiceImpl.newNullStorageMessagingServer(conf);
      messagingService.getServer().getQueueSettingsRepository().addMatch("testGroupQueue", qs);
      messagingService.start();

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"));

      ClientSession session = sf.createSession(false, true, true, false);

      session.createQueue(QUEUE, QUEUE, null, false, false);
      
      ClientProducer producer = session.createProducer(QUEUE);

      final CountDownLatch latch = new CountDownLatch(100);

      MyMessageHandler myMessageHandler = new MyMessageHandler(latch);
      MyMessageHandler myMessageHandler2 = new MyMessageHandler(latch);

      ClientConsumer consumer = session.createConsumer(QUEUE);
      consumer.setMessageHandler(myMessageHandler);
      ClientConsumer consumer2 = session.createConsumer(QUEUE);
      consumer2.setMessageHandler(myMessageHandler2);
      
      session.start();

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE, false, 0,
               System.currentTimeMillis(), (byte) 1);
         message.getBody().putString("testINVMCoreClient");
         message.getBody().flip();
         message.setDurable(false);
         producer.send(message);
      }
      latch.await();
      
      session.close();

      messagingService.stop();

      assertEquals(myMessageHandler.messagesReceived, 50);
      assertEquals(myMessageHandler2.messagesReceived, 50);
   }


   private static class MyMessageHandler implements MessageHandler
   {
      volatile int messagesReceived = 0;

      private final CountDownLatch latch;

      public MyMessageHandler(CountDownLatch latch)
      {
         this.latch = latch;
      }

      public void onMessage(ClientMessage message)
      {
         messagesReceived++;
         try
         {
            message.processed();
         }
         catch (MessagingException e)
         {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
         }
         latch.countDown();
      }
   }
}
