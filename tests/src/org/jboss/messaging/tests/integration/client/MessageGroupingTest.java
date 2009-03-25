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
package org.jboss.messaging.tests.integration.client;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.MessageHandler;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.GroupingRoundRobinDistributor;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.core.transaction.impl.XidImpl;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.utils.SimpleString;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class MessageGroupingTest extends UnitTestCase
{
   private MessagingService messagingService;

   private ClientSession clientSession;

   private SimpleString qName = new SimpleString("MessageGroupingTestQueue");

   public void testBasicGrouping() throws Exception
   {
      ClientProducer clientProducer = clientSession.createProducer(qName);
      ClientConsumer consumer = clientSession.createConsumer(qName);
      ClientConsumer consumer2 = clientSession.createConsumer(qName);
      clientSession.start();
      SimpleString groupId = new SimpleString("grp1");
      int numMessages = 100;
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, clientSession);
         message.putStringProperty(MessageImpl.HDR_GROUP_ID, groupId);
         clientProducer.send(message);
      }
      CountDownLatch latch = new CountDownLatch(numMessages);
      DummyMessageHandler dummyMessageHandler = new DummyMessageHandler(latch, true);
      consumer.setMessageHandler(dummyMessageHandler);
      DummyMessageHandler dummyMessageHandler2 = new DummyMessageHandler(latch, true);
      consumer2.setMessageHandler(dummyMessageHandler2);
      assertTrue(latch.await(10, TimeUnit.SECONDS));
      assertTrue(dummyMessageHandler.list.size() == 100);
      assertTrue(dummyMessageHandler2.list.size() == 0);
      consumer.close();
      consumer2.close();
   }

   public void testMultipleGrouping() throws Exception
   {
      ClientProducer clientProducer = clientSession.createProducer(qName);
      ClientConsumer consumer = clientSession.createConsumer(qName);
      ClientConsumer consumer2 = clientSession.createConsumer(qName);
      clientSession.start();
      SimpleString groupId = new SimpleString("grp1");
      SimpleString groupId2 = new SimpleString("grp2");
      int numMessages = 100;
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, clientSession);
         if( i % 2 == 0 || i == 0)
         {
            message.putStringProperty(MessageImpl.HDR_GROUP_ID, groupId);
         }
         else
         {
             message.putStringProperty(MessageImpl.HDR_GROUP_ID, groupId2);
         }
         clientProducer.send(message);
      }
      CountDownLatch latch = new CountDownLatch(numMessages);
      DummyMessageHandler dummyMessageHandler = new DummyMessageHandler(latch, true);
      consumer.setMessageHandler(dummyMessageHandler);
      DummyMessageHandler dummyMessageHandler2 = new DummyMessageHandler(latch, true);
      consumer2.setMessageHandler(dummyMessageHandler2);
      assertTrue(latch.await(10, TimeUnit.SECONDS));
      assertEquals(dummyMessageHandler.list.size(), 50);
      int i = 0;
      for (ClientMessage message : dummyMessageHandler.list)
      {
         assertEquals(message.getBody().readString(), "m" + i);
         i += 2;
      }
      assertEquals(dummyMessageHandler2.list.size(), 50);
      i = 1;
      for (ClientMessage message : dummyMessageHandler2.list)
      {
         assertEquals(message.getBody().readString(), "m" + i);
         i += 2;
      }
      consumer.close();
      consumer2.close();
   }

   public void testMultipleGroupingStartConsumersAfterMessagesSent() throws Exception
   {
      ClientProducer clientProducer = clientSession.createProducer(qName);
      ClientConsumer consumer = clientSession.createConsumer(qName);
      ClientConsumer consumer2 = clientSession.createConsumer(qName);
      SimpleString groupId = new SimpleString("grp1");
      SimpleString groupId2 = new SimpleString("grp2");
      int numMessages = 100;
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, clientSession);
         if( i % 2 == 0 || i == 0)
         {
            message.putStringProperty(MessageImpl.HDR_GROUP_ID, groupId);
         }
         else
         {
             message.putStringProperty(MessageImpl.HDR_GROUP_ID, groupId2);
         }
         clientProducer.send(message);
      }

      clientSession.start();
      CountDownLatch latch = new CountDownLatch(numMessages);
      DummyMessageHandler dummyMessageHandler = new DummyMessageHandler(latch, true);
      consumer.setMessageHandler(dummyMessageHandler);
      DummyMessageHandler dummyMessageHandler2 = new DummyMessageHandler(latch, true);
      consumer2.setMessageHandler(dummyMessageHandler2);
      assertTrue(latch.await(10, TimeUnit.SECONDS));
      assertEquals(dummyMessageHandler.list.size(), 50);
      int i = 0;
      for (ClientMessage message : dummyMessageHandler.list)
      {
         assertEquals(message.getBody().readString(), "m" + i);
         i += 2;
      }
      assertEquals(dummyMessageHandler2.list.size(), 50);
      i = 1;
      for (ClientMessage message : dummyMessageHandler2.list)
      {
         assertEquals(message.getBody().readString(), "m" + i);
         i += 2;
      }
      consumer.close();
      consumer2.close();
   }

   public void testMultipleGroupingConsumeHalf() throws Exception
   {
      ClientProducer clientProducer = clientSession.createProducer(qName);
      ClientConsumer consumer = clientSession.createConsumer(qName);
      ClientConsumer consumer2 = clientSession.createConsumer(qName);
      clientSession.start();
      SimpleString groupId = new SimpleString("grp1");
      SimpleString groupId2 = new SimpleString("grp2");
      int numMessages = 100;
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, clientSession);
         if( i % 2 == 0 || i == 0)
         {
            message.putStringProperty(MessageImpl.HDR_GROUP_ID, groupId);
         }
         else
         {
             message.putStringProperty(MessageImpl.HDR_GROUP_ID, groupId2);
         }
         clientProducer.send(message);
      }

      for(int i = 0; i < numMessages/2; i++)
      {
         ClientMessage cm = consumer.receive(500);
         assertNotNull(cm);
         assertEquals(cm.getBody().readString(), "m" + i);
         i++;
         cm = consumer2.receive(500);
         assertNotNull(cm);
         assertEquals(cm.getBody().readString(), "m" + i);
      }

      consumer2.close();
      consumer.close();
      //check that within their groups the messages are still in the correct order
      consumer = clientSession.createConsumer(qName);
      for(int i = 0; i < numMessages; i+=2)
      {
         ClientMessage cm = consumer.receive(500);
         assertNotNull(cm);
         assertEquals(cm.getBody().readString(), "m" + i);
      }
      for(int i = 1; i < numMessages; i+=2)
      {
         ClientMessage cm = consumer.receive(500);
         assertNotNull(cm);
         assertEquals(cm.getBody().readString(), "m" + i);
      }
      consumer.close();
   }

   public void testMultipleGroupingSingleConsumer() throws Exception
   {
      ClientProducer clientProducer = clientSession.createProducer(qName);
      ClientConsumer consumer = clientSession.createConsumer(qName);
      clientSession.start();
      SimpleString groupId = new SimpleString("grp1");
      SimpleString groupId2 = new SimpleString("grp2");
      int numMessages = 100;
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, clientSession);
         if( i % 2 == 0 || i == 0)
         {
            message.putStringProperty(MessageImpl.HDR_GROUP_ID, groupId);
         }
         else
         {
             message.putStringProperty(MessageImpl.HDR_GROUP_ID, groupId2);
         }
         clientProducer.send(message);
      }
      CountDownLatch latch = new CountDownLatch(numMessages);
      DummyMessageHandler dummyMessageHandler = new DummyMessageHandler(latch, true);
      consumer.setMessageHandler(dummyMessageHandler);
      assertTrue(latch.await(10, TimeUnit.SECONDS));
      assertEquals(dummyMessageHandler.list.size(), 100);
      int i = 0;
      for (ClientMessage message : dummyMessageHandler.list)
      {
         assertEquals(message.getBody().readString(), "m" + i);
         i += 1;
      }
      consumer.close();
   }

   public void testMultipleGroupingTXCommit() throws Exception
   {
      ClientSessionFactory sessionFactory = new ClientSessionFactoryImpl(new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      ClientSession clientSession = sessionFactory.createSession(false, false, false);
      ClientProducer clientProducer = this.clientSession.createProducer(qName);
      clientSession.start();
      ClientConsumer consumer = clientSession.createConsumer(qName);
      ClientConsumer consumer2 = clientSession.createConsumer(qName);

      SimpleString groupId = new SimpleString("grp1");
      SimpleString groupId2 = new SimpleString("grp2");
      int numMessages = 100;
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, clientSession);
         if( i % 2 == 0 || i == 0)
         {
            message.putStringProperty(MessageImpl.HDR_GROUP_ID, groupId);
         }
         else
         {
             message.putStringProperty(MessageImpl.HDR_GROUP_ID, groupId2);
         }
         clientProducer.send(message);
      }
      CountDownLatch latch = new CountDownLatch(numMessages);
      DummyMessageHandler dummyMessageHandler = new DummyMessageHandler(latch, true);
      consumer.setMessageHandler(dummyMessageHandler);
      DummyMessageHandler dummyMessageHandler2 = new DummyMessageHandler(latch, true);
      consumer2.setMessageHandler(dummyMessageHandler2);
      assertTrue(latch.await(10, TimeUnit.SECONDS));
      clientSession.commit();
      assertEquals(dummyMessageHandler.list.size(), 50);
      int i = 0;
      for (ClientMessage message : dummyMessageHandler.list)
      {
         assertEquals(message.getBody().readString(), "m" + i);
         i += 2;
      }
      assertEquals(dummyMessageHandler2.list.size(), 50);
      i = 1;
      for (ClientMessage message : dummyMessageHandler2.list)
      {
         assertEquals(message.getBody().readString(), "m" + i);
         i += 2;
      }
      consumer.close();
      consumer2.close();
      consumer = this.clientSession.createConsumer(qName);
      assertNull(consumer.receive(500));
   }

   public void testMultipleGroupingTXRollback() throws Exception
   {
      ClientSessionFactory sessionFactory = new ClientSessionFactoryImpl(new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      sessionFactory.setBlockOnAcknowledge(true);
      ClientSession clientSession = sessionFactory.createSession(false, false, false);
      ClientProducer clientProducer = this.clientSession.createProducer(qName);
      ClientConsumer consumer = clientSession.createConsumer(qName);
      ClientConsumer consumer2 = clientSession.createConsumer(qName);
      clientSession.start();
      SimpleString groupId = new SimpleString("grp1");
      SimpleString groupId2 = new SimpleString("grp2");
      int numMessages = 100;
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, clientSession);
         if( i % 2 == 0 || i == 0)
         {
            message.putStringProperty(MessageImpl.HDR_GROUP_ID, groupId);
         }
         else
         {
             message.putStringProperty(MessageImpl.HDR_GROUP_ID, groupId2);
         }
         clientProducer.send(message);
      }
      CountDownLatch latch = new CountDownLatch(numMessages);
      DummyMessageHandler dummyMessageHandler = new DummyMessageHandler(latch, true);
      consumer.setMessageHandler(dummyMessageHandler);
      DummyMessageHandler dummyMessageHandler2 = new DummyMessageHandler(latch, true);
      consumer2.setMessageHandler(dummyMessageHandler2);
      assertTrue(latch.await(10, TimeUnit.SECONDS));
      assertEquals(dummyMessageHandler.list.size(), 50);
      int i = 0;
      for (ClientMessage message : dummyMessageHandler.list)
      {
         assertEquals(message.getBody().readString(), "m" + i);
         i += 2;
      }
      assertEquals(dummyMessageHandler2.list.size(), 50);
      i = 1;
      for (ClientMessage message : dummyMessageHandler2.list)
      {
         assertEquals(message.getBody().readString(), "m" + i);
         i += 2;
      }
      latch = new CountDownLatch(numMessages);
      dummyMessageHandler.reset(latch);
      dummyMessageHandler2.reset(latch);
      clientSession.rollback();
      assertTrue(latch.await(10, TimeUnit.SECONDS));
      assertEquals(dummyMessageHandler.list.size(), 50);
      i = 0;
      for (ClientMessage message : dummyMessageHandler.list)
      {
         assertEquals(message.getBody().readString(), "m" + i);
         i += 2;
      }
      assertEquals(dummyMessageHandler2.list.size(), 50);
      i = 1;
      for (ClientMessage message : dummyMessageHandler2.list)
      {
         assertEquals(message.getBody().readString(), "m" + i);
         i += 2;
      }
      consumer = this.clientSession.createConsumer(qName);
      assertNull(consumer.receive(500));
   }

   public void testMultipleGroupingXACommit() throws Exception
   {
      ClientSessionFactory sessionFactory = new ClientSessionFactoryImpl(new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      ClientSession clientSession = sessionFactory.createSession(true, false, false);
      ClientProducer clientProducer = this.clientSession.createProducer(qName);
      ClientConsumer consumer = clientSession.createConsumer(qName);
      ClientConsumer consumer2 = clientSession.createConsumer(qName);
      clientSession.start();
      Xid xid = new XidImpl("bq".getBytes(), 4, "gtid".getBytes());
      clientSession.start(xid, XAResource.TMNOFLAGS);

      SimpleString groupId = new SimpleString("grp1");
      SimpleString groupId2 = new SimpleString("grp2");
      int numMessages = 100;
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, clientSession);
         if( i % 2 == 0 || i == 0)
         {
            message.putStringProperty(MessageImpl.HDR_GROUP_ID, groupId);
         }
         else
         {
             message.putStringProperty(MessageImpl.HDR_GROUP_ID, groupId2);
         }
         clientProducer.send(message);
      }
      CountDownLatch latch = new CountDownLatch(numMessages);
      DummyMessageHandler dummyMessageHandler = new DummyMessageHandler(latch, true);
      consumer.setMessageHandler(dummyMessageHandler);
      DummyMessageHandler dummyMessageHandler2 = new DummyMessageHandler(latch, true);
      consumer2.setMessageHandler(dummyMessageHandler2);
      assertTrue(latch.await(10, TimeUnit.SECONDS));
      clientSession.end(xid, XAResource.TMSUCCESS);
      clientSession.prepare(xid);
      clientSession.commit(xid, true);
      assertEquals(dummyMessageHandler.list.size(), 50);
      int i = 0;
      for (ClientMessage message : dummyMessageHandler.list)
      {
         assertEquals(message.getBody().readString(), "m" + i);
         i += 2;
      }
      assertEquals(dummyMessageHandler2.list.size(), 50);
      i = 1;
      for (ClientMessage message : dummyMessageHandler2.list)
      {
         assertEquals(message.getBody().readString(), "m" + i);
         i += 2;
      }
      consumer.close();
      consumer2.close();
      consumer = this.clientSession.createConsumer(qName);
      assertNull(consumer.receive(500));
   }

   public void testMultipleGroupingXARollback() throws Exception
   {
      ClientSessionFactory sessionFactory = new ClientSessionFactoryImpl(new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      sessionFactory.setBlockOnAcknowledge(true);
      ClientSession clientSession = sessionFactory.createSession(true, false, false);
      ClientProducer clientProducer = this.clientSession.createProducer(qName);
      clientSession.start();
      ClientConsumer consumer = clientSession.createConsumer(qName);
      ClientConsumer consumer2 = clientSession.createConsumer(qName);
      Xid xid = new XidImpl("bq".getBytes(), 4, "gtid".getBytes());
      clientSession.start(xid, XAResource.TMNOFLAGS);

      SimpleString groupId = new SimpleString("grp1");
      SimpleString groupId2 = new SimpleString("grp2");
      int numMessages = 100;
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, clientSession);
         if( i % 2 == 0 || i == 0)
         {
            message.putStringProperty(MessageImpl.HDR_GROUP_ID, groupId);
         }
         else
         {
             message.putStringProperty(MessageImpl.HDR_GROUP_ID, groupId2);
         }
         clientProducer.send(message);
      }
      CountDownLatch latch = new CountDownLatch(numMessages);
      DummyMessageHandler dummyMessageHandler = new DummyMessageHandler(latch, true);
      consumer.setMessageHandler(dummyMessageHandler);
      DummyMessageHandler dummyMessageHandler2 = new DummyMessageHandler(latch, true);
      consumer2.setMessageHandler(dummyMessageHandler2);
      assertTrue(latch.await(10, TimeUnit.SECONDS));
      clientSession.end(xid, XAResource.TMSUCCESS);
      assertEquals(dummyMessageHandler.list.size(), 50);
      int i = 0;
      for (ClientMessage message : dummyMessageHandler.list)
      {
         assertEquals(message.getBody().readString(), "m" + i);
         i += 2;
      }
      assertEquals(dummyMessageHandler2.list.size(), 50);
      i = 1;
      for (ClientMessage message : dummyMessageHandler2.list)
      {
         assertEquals(message.getBody().readString(), "m" + i);
         i += 2;
      }
      latch = new CountDownLatch(numMessages);
      dummyMessageHandler.reset(latch);
      dummyMessageHandler2.reset(latch);
      clientSession.rollback(xid);
      clientSession.start(xid, XAResource.TMNOFLAGS);
      assertTrue(latch.await(10, TimeUnit.SECONDS));
      clientSession.end(xid, XAResource.TMSUCCESS);
      clientSession.prepare(xid);
      clientSession.commit(xid, false);
      assertEquals(dummyMessageHandler.list.size(), 50);
      i = 0;
      for (ClientMessage message : dummyMessageHandler.list)
      {
         assertEquals(message.getBody().readString(), "m" + i);
         i += 2;
      }
      assertEquals(dummyMessageHandler2.list.size(), 50);
      i = 1;
      for (ClientMessage message : dummyMessageHandler2.list)
      {
         assertEquals(message.getBody().readString(), "m" + i);
         i += 2;
      }
      consumer = this.clientSession.createConsumer(qName);
      assertNull(consumer.receive(500));
   }

   protected void tearDown() throws Exception
   {
      if (clientSession != null)
      {
         try
         {
            clientSession.close();
         }
         catch (MessagingException e1)
         {
            //
         }
      }
      if (messagingService != null && messagingService.isStarted())
      {
         try
         {
            messagingService.stop();
         }
         catch (Exception e1)
         {
            //
         }
      }
      messagingService = null;
      clientSession = null;
      
      super.tearDown();
   }

   protected void setUp() throws Exception
   {
      super.setUp();
      
      ConfigurationImpl configuration = new ConfigurationImpl();
      configuration.setSecurityEnabled(false);
      TransportConfiguration transportConfig = new TransportConfiguration(INVM_ACCEPTOR_FACTORY);
      configuration.getAcceptorConfigurations().add(transportConfig);
      messagingService = Messaging.newNullStorageMessagingService(configuration);
      // start the server
      messagingService.start();

      AddressSettings qs = new AddressSettings();
      qs.setDistributionPolicyClass(GroupingRoundRobinDistributor.class.getName());
      messagingService.getServer().getAddressSettingsRepository().addMatch(qName.toString(), qs);
      // then we create a client as normal
      ClientSessionFactory sessionFactory = new ClientSessionFactoryImpl(new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      clientSession = sessionFactory.createSession(false, true, true);
      clientSession.createQueue(qName, qName, null, false, false);
   }

   private static class DummyMessageHandler implements MessageHandler
   {
      ArrayList<ClientMessage> list = new ArrayList<ClientMessage>();

      private CountDownLatch latch;

      private final boolean acknowledge;

      public DummyMessageHandler(CountDownLatch latch, boolean acknowledge)
      {
         this.latch = latch;
         this.acknowledge = acknowledge;
      }

      public void onMessage(ClientMessage message)
      {
         list.add(message);
         if (acknowledge)
         {
            try
         {
            message.acknowledge();
         }
         catch (MessagingException e)
            {
               //ignore
            }
         }
         latch.countDown();
      }

      public void reset(CountDownLatch latch)
      {
         list.clear();
         this.latch = latch;
      }
   }
}
