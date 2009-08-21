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
package org.hornetq.tests.integration.client;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.MessageHandler;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.transaction.impl.XidImpl;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.SimpleString;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class MessageGroupingTest extends UnitTestCase
{
   private HornetQServer server;

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
      assertEquals(100, dummyMessageHandler.list.size());
      assertEquals(0, dummyMessageHandler2.list.size());
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
      assertEquals(50, dummyMessageHandler.list.size());
      int i = 0;
      for (ClientMessage message : dummyMessageHandler.list)
      {
         assertEquals(message.getBody().readString(), "m" + i);
         i += 2;
      }
      assertEquals(50, dummyMessageHandler2.list.size());
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
      assertEquals(50, dummyMessageHandler.list.size());
      int i = 0;
      for (ClientMessage message : dummyMessageHandler.list)
      {
         assertEquals(message.getBody().readString(), "m" + i);
         i += 2;
      }
      assertEquals(50, dummyMessageHandler2.list.size());
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
      clientSession.close();
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
      clientSession.close();
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
      clientSession.close();
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
      clientSession.close();
   }

   protected void tearDown() throws Exception
   {
      if (clientSession != null)
      {
         try
         {
            clientSession.close();
         }
         catch (HornetQException e1)
         {
            //
         }
      }
      if (server != null && server.isStarted())
      {
         try
         {
            server.stop();
         }
         catch (Exception e1)
         {
            //
         }
      }
      server = null;
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
      server = HornetQ.newHornetQServer(configuration, false);
      // start the server
      server.start();

      // then we create a client as normal
      ClientSessionFactory sessionFactory = new ClientSessionFactoryImpl(new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      clientSession = sessionFactory.createSession(false, true, true);
      clientSession.createQueue(qName, qName, null, false);
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
         catch (HornetQException e)
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
