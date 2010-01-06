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

import junit.framework.Assert;

import org.hornetq.api.SimpleString;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.*;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.core.transaction.impl.XidImpl;
import org.hornetq.tests.util.UnitTestCase;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class MessageGroupingTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(MessageGroupingTest.class);

   private HornetQServer server;

   private ClientSession clientSession;

   private final SimpleString qName = new SimpleString("MessageGroupingTestQueue");

   public void testBasicGroupingWithDirectDelivery() throws Exception
   {
      doTestBasicGrouping(true);
   }

   public void testBasicGroupingWithoutDirectDelivery() throws Exception
   {
      doTestBasicGrouping(false);
   }

   public void testMultipleGroupingWithDirectDelivery() throws Exception
   {
      doTestMultipleGrouping(true);
   }

   public void testMultipleGroupingWithoutDirectDelivery() throws Exception
   {
      doTestMultipleGrouping(false);
   }

   public void testMultipleGroupingSingleConsumerWithDirectDelivery() throws Exception
   {
      doTestMultipleGroupingSingleConsumer(true);
   }

   public void testMultipleGroupingSingleConsumerWithoutDirectDelivery() throws Exception
   {
      doTestMultipleGroupingSingleConsumer(false);
   }

   public void testMultipleGroupingTXCommitWithDirectDelivery() throws Exception
   {
      doTestMultipleGroupingTXCommit(true);
   }

   public void testMultipleGroupingTXCommitWithoutDirectDelivery() throws Exception
   {
      doTestMultipleGroupingTXCommit(false);
   }

   public void testMultipleGroupingTXRollbackWithDirectDelivery() throws Exception
   {
      doTestMultipleGroupingTXRollback(true);
   }

   public void testMultipleGroupingTXRollbackWithoutDirectDelivery() throws Exception
   {
      doTestMultipleGroupingTXRollback(false);
   }

   public void testMultipleGroupingXACommitWithDirectDelivery() throws Exception
   {
      dotestMultipleGroupingXACommit(true);
   }

   public void testMultipleGroupingXACommitWithoutDirectDelivery() throws Exception
   {
      dotestMultipleGroupingXACommit(false);
   }

   public void testMultipleGroupingXARollbackWithDirectDelivery() throws Exception
   {
      doTestMultipleGroupingXARollback(true);
   }

   public void testMultipleGroupingXARollbackWithoutDirectDelivery() throws Exception
   {
      doTestMultipleGroupingXARollback(false);
   }

   private void doTestBasicGrouping(final boolean directDelivery) throws Exception
   {
      ClientProducer clientProducer = clientSession.createProducer(qName);
      ClientConsumer consumer = clientSession.createConsumer(qName);
      ClientConsumer consumer2 = clientSession.createConsumer(qName);
      if (directDelivery)
      {
         clientSession.start();
      }
      SimpleString groupId = new SimpleString("grp1");
      int numMessages = 100;
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, clientSession);
         message.putStringProperty(Message.HDR_GROUP_ID, groupId);
         clientProducer.send(message);
      }
      if (!directDelivery)
      {
         clientSession.start();
      }
      CountDownLatch latch = new CountDownLatch(numMessages);
      DummyMessageHandler dummyMessageHandler = new DummyMessageHandler(latch, true);
      consumer.setMessageHandler(dummyMessageHandler);
      DummyMessageHandler dummyMessageHandler2 = new DummyMessageHandler(latch, true);
      consumer2.setMessageHandler(dummyMessageHandler2);
      Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
      Assert.assertEquals(100, dummyMessageHandler.list.size());
      Assert.assertEquals(0, dummyMessageHandler2.list.size());
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
         if (i % 2 == 0 || i == 0)
         {
            message.putStringProperty(Message.HDR_GROUP_ID, groupId);
         }
         else
         {
            message.putStringProperty(Message.HDR_GROUP_ID, groupId2);
         }
         clientProducer.send(message);
      }

      for (int i = 0; i < numMessages / 2; i++)
      {
         ClientMessage cm = consumer.receive(500);
         Assert.assertNotNull(cm);
         Assert.assertEquals(cm.getBodyBuffer().readString(), "m" + i);
         i++;
         cm = consumer2.receive(500);
         Assert.assertNotNull(cm);
         Assert.assertEquals(cm.getBodyBuffer().readString(), "m" + i);
      }

      MessageGroupingTest.log.info("closing consumers");

      consumer2.close();

      MessageGroupingTest.log.info("closed consumer 2");

      consumer.close();

      MessageGroupingTest.log.info("closed consuemrs");
      // check that within their groups the messages are still in the correct order
      consumer = clientSession.createConsumer(qName);
      for (int i = 0; i < numMessages; i += 2)
      {
         ClientMessage cm = consumer.receive(500);
         Assert.assertNotNull(cm);
         Assert.assertEquals(cm.getBodyBuffer().readString(), "m" + i);
      }
      for (int i = 1; i < numMessages; i += 2)
      {
         ClientMessage cm = consumer.receive(500);
         Assert.assertNotNull(cm);
         Assert.assertEquals(cm.getBodyBuffer().readString(), "m" + i);
      }
      consumer.close();
   }

   private void doTestMultipleGroupingSingleConsumer(final boolean directDelivery) throws Exception
   {
      ClientProducer clientProducer = clientSession.createProducer(qName);
      ClientConsumer consumer = clientSession.createConsumer(qName);
      if (directDelivery)
      {
         clientSession.start();
      }
      SimpleString groupId = new SimpleString("grp1");
      SimpleString groupId2 = new SimpleString("grp2");
      int numMessages = 100;
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, clientSession);
         if (i % 2 == 0 || i == 0)
         {
            message.putStringProperty(Message.HDR_GROUP_ID, groupId);
         }
         else
         {
            message.putStringProperty(Message.HDR_GROUP_ID, groupId2);
         }
         clientProducer.send(message);
      }
      if (!directDelivery)
      {
         clientSession.start();
      }
      CountDownLatch latch = new CountDownLatch(numMessages);
      DummyMessageHandler dummyMessageHandler = new DummyMessageHandler(latch, true);
      consumer.setMessageHandler(dummyMessageHandler);
      Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
      Assert.assertEquals(dummyMessageHandler.list.size(), 100);
      int i = 0;
      for (ClientMessage message : dummyMessageHandler.list)
      {
         Assert.assertEquals(message.getBodyBuffer().readString(), "m" + i);
         i += 1;
      }
      consumer.close();
   }

   private void doTestMultipleGroupingTXCommit(final boolean directDelivery) throws Exception
   {
      ClientSessionFactory sessionFactory = HornetQClient.createClientSessionFactory(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));
      ClientSession clientSession = sessionFactory.createSession(false, false, false);
      ClientProducer clientProducer = this.clientSession.createProducer(qName);
      if (directDelivery)
      {
         clientSession.start();
      }
      ClientConsumer consumer = clientSession.createConsumer(qName);
      ClientConsumer consumer2 = clientSession.createConsumer(qName);

      SimpleString groupId = new SimpleString("grp1");
      SimpleString groupId2 = new SimpleString("grp2");
      int numMessages = 100;
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, clientSession);
         if (i % 2 == 0 || i == 0)
         {
            message.putStringProperty(Message.HDR_GROUP_ID, groupId);
         }
         else
         {
            message.putStringProperty(Message.HDR_GROUP_ID, groupId2);
         }
         clientProducer.send(message);
      }
      if (!directDelivery)
      {
         clientSession.start();
      }
      CountDownLatch latch = new CountDownLatch(numMessages);
      DummyMessageHandler dummyMessageHandler = new DummyMessageHandler(latch, true);
      consumer.setMessageHandler(dummyMessageHandler);
      DummyMessageHandler dummyMessageHandler2 = new DummyMessageHandler(latch, true);
      consumer2.setMessageHandler(dummyMessageHandler2);
      Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
      clientSession.commit();
      Assert.assertEquals(dummyMessageHandler.list.size(), 50);
      int i = 0;
      for (ClientMessage message : dummyMessageHandler.list)
      {
         Assert.assertEquals(message.getBodyBuffer().readString(), "m" + i);
         i += 2;
      }
      Assert.assertEquals(dummyMessageHandler2.list.size(), 50);
      i = 1;
      for (ClientMessage message : dummyMessageHandler2.list)
      {
         Assert.assertEquals(message.getBodyBuffer().readString(), "m" + i);
         i += 2;
      }
      consumer.close();
      consumer2.close();
      consumer = this.clientSession.createConsumer(qName);
      Assert.assertNull(consumer.receiveImmediate());
      clientSession.close();
   }

   private void doTestMultipleGroupingTXRollback(final boolean directDelivery) throws Exception
   {
      ClientSessionFactory sessionFactory = HornetQClient.createClientSessionFactory(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));
      sessionFactory.setBlockOnAcknowledge(true);
      ClientSession clientSession = sessionFactory.createSession(false, false, false);
      ClientProducer clientProducer = this.clientSession.createProducer(qName);
      ClientConsumer consumer = clientSession.createConsumer(qName);
      ClientConsumer consumer2 = clientSession.createConsumer(qName);
      if (directDelivery)
      {
         clientSession.start();
      }
      SimpleString groupId = new SimpleString("grp1");
      SimpleString groupId2 = new SimpleString("grp2");
      int numMessages = 100;
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, clientSession);
         if (i % 2 == 0 || i == 0)
         {
            message.putStringProperty(Message.HDR_GROUP_ID, groupId);
         }
         else
         {
            message.putStringProperty(Message.HDR_GROUP_ID, groupId2);
         }
         clientProducer.send(message);
      }
      if (!directDelivery)
      {
         clientSession.start();
      }
      CountDownLatch latch = new CountDownLatch(numMessages);
      DummyMessageHandler dummyMessageHandler = new DummyMessageHandler(latch, true);
      consumer.setMessageHandler(dummyMessageHandler);
      DummyMessageHandler dummyMessageHandler2 = new DummyMessageHandler(latch, true);
      consumer2.setMessageHandler(dummyMessageHandler2);
      Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
      Assert.assertEquals(dummyMessageHandler.list.size(), 50);
      int i = 0;
      for (ClientMessage message : dummyMessageHandler.list)
      {
         Assert.assertEquals(message.getBodyBuffer().readString(), "m" + i);
         i += 2;
      }
      Assert.assertEquals(dummyMessageHandler2.list.size(), 50);
      i = 1;
      for (ClientMessage message : dummyMessageHandler2.list)
      {
         Assert.assertEquals(message.getBodyBuffer().readString(), "m" + i);
         i += 2;
      }
      latch = new CountDownLatch(numMessages);
      dummyMessageHandler.reset(latch);
      dummyMessageHandler2.reset(latch);
      clientSession.rollback();
      Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
      Assert.assertEquals(dummyMessageHandler.list.size(), 50);
      i = 0;
      for (ClientMessage message : dummyMessageHandler.list)
      {
         Assert.assertEquals(message.getBodyBuffer().readString(), "m" + i);
         i += 2;
      }
      Assert.assertEquals(dummyMessageHandler2.list.size(), 50);
      i = 1;
      for (ClientMessage message : dummyMessageHandler2.list)
      {
         Assert.assertEquals(message.getBodyBuffer().readString(), "m" + i);
         i += 2;
      }
      consumer = this.clientSession.createConsumer(qName);
      Assert.assertNull(consumer.receiveImmediate());
      clientSession.close();
   }

   private void dotestMultipleGroupingXACommit(final boolean directDelivery) throws Exception
   {
      ClientSessionFactory sessionFactory = HornetQClient.createClientSessionFactory(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));
      ClientSession clientSession = sessionFactory.createSession(true, false, false);
      ClientProducer clientProducer = this.clientSession.createProducer(qName);
      ClientConsumer consumer = clientSession.createConsumer(qName);
      ClientConsumer consumer2 = clientSession.createConsumer(qName);
      if (directDelivery)
      {
         clientSession.start();
      }
      Xid xid = new XidImpl("bq".getBytes(), 4, "gtid".getBytes());
      clientSession.start(xid, XAResource.TMNOFLAGS);

      SimpleString groupId = new SimpleString("grp1");
      SimpleString groupId2 = new SimpleString("grp2");
      int numMessages = 100;
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, clientSession);
         if (i % 2 == 0 || i == 0)
         {
            message.putStringProperty(Message.HDR_GROUP_ID, groupId);
         }
         else
         {
            message.putStringProperty(Message.HDR_GROUP_ID, groupId2);
         }
         clientProducer.send(message);
      }
      if (!directDelivery)
      {
         clientSession.start();
      }
      CountDownLatch latch = new CountDownLatch(numMessages);
      DummyMessageHandler dummyMessageHandler = new DummyMessageHandler(latch, true);
      consumer.setMessageHandler(dummyMessageHandler);
      DummyMessageHandler dummyMessageHandler2 = new DummyMessageHandler(latch, true);
      consumer2.setMessageHandler(dummyMessageHandler2);
      Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
      clientSession.end(xid, XAResource.TMSUCCESS);
      clientSession.prepare(xid);
      clientSession.commit(xid, false);
      Assert.assertEquals(dummyMessageHandler.list.size(), 50);
      int i = 0;
      for (ClientMessage message : dummyMessageHandler.list)
      {
         Assert.assertEquals(message.getBodyBuffer().readString(), "m" + i);
         i += 2;
      }
      Assert.assertEquals(dummyMessageHandler2.list.size(), 50);
      i = 1;
      for (ClientMessage message : dummyMessageHandler2.list)
      {
         Assert.assertEquals(message.getBodyBuffer().readString(), "m" + i);
         i += 2;
      }
      consumer.close();
      consumer2.close();
      consumer = this.clientSession.createConsumer(qName);
      Assert.assertNull(consumer.receiveImmediate());
      clientSession.close();
   }

   private void doTestMultipleGroupingXARollback(final boolean directDelivery) throws Exception
   {
      ClientSessionFactory sessionFactory = HornetQClient.createClientSessionFactory(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));
      sessionFactory.setBlockOnAcknowledge(true);
      ClientSession clientSession = sessionFactory.createSession(true, false, false);
      ClientProducer clientProducer = this.clientSession.createProducer(qName);
      if (directDelivery)
      {
         clientSession.start();
      }
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
         if (i % 2 == 0 || i == 0)
         {
            message.putStringProperty(Message.HDR_GROUP_ID, groupId);
         }
         else
         {
            message.putStringProperty(Message.HDR_GROUP_ID, groupId2);
         }
         clientProducer.send(message);
      }
      if (!directDelivery)
      {
         clientSession.start();
      }
      CountDownLatch latch = new CountDownLatch(numMessages);
      DummyMessageHandler dummyMessageHandler = new DummyMessageHandler(latch, true);
      consumer.setMessageHandler(dummyMessageHandler);
      DummyMessageHandler dummyMessageHandler2 = new DummyMessageHandler(latch, true);
      consumer2.setMessageHandler(dummyMessageHandler2);
      Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
      clientSession.end(xid, XAResource.TMSUCCESS);
      Assert.assertEquals(dummyMessageHandler.list.size(), 50);
      int i = 0;
      for (ClientMessage message : dummyMessageHandler.list)
      {
         Assert.assertEquals(message.getBodyBuffer().readString(), "m" + i);
         i += 2;
      }
      Assert.assertEquals(dummyMessageHandler2.list.size(), 50);
      i = 1;
      for (ClientMessage message : dummyMessageHandler2.list)
      {
         Assert.assertEquals(message.getBodyBuffer().readString(), "m" + i);
         i += 2;
      }
      latch = new CountDownLatch(numMessages);
      dummyMessageHandler.reset(latch);
      dummyMessageHandler2.reset(latch);
      clientSession.rollback(xid);
      clientSession.start(xid, XAResource.TMNOFLAGS);
      Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
      clientSession.end(xid, XAResource.TMSUCCESS);
      clientSession.prepare(xid);
      clientSession.commit(xid, false);
      Assert.assertEquals(dummyMessageHandler.list.size(), 50);
      i = 0;
      for (ClientMessage message : dummyMessageHandler.list)
      {
         Assert.assertEquals(message.getBodyBuffer().readString(), "m" + i);
         i += 2;
      }
      Assert.assertEquals(dummyMessageHandler2.list.size(), 50);
      i = 1;
      for (ClientMessage message : dummyMessageHandler2.list)
      {
         Assert.assertEquals(message.getBodyBuffer().readString(), "m" + i);
         i += 2;
      }
      consumer = this.clientSession.createConsumer(qName);
      Assert.assertNull(consumer.receiveImmediate());
      clientSession.close();
   }

   private void doTestMultipleGrouping(final boolean directDelivery) throws Exception
   {
      ClientProducer clientProducer = clientSession.createProducer(qName);
      ClientConsumer consumer = clientSession.createConsumer(qName);
      ClientConsumer consumer2 = clientSession.createConsumer(qName);
      if (directDelivery)
      {
         clientSession.start();
      }
      SimpleString groupId = new SimpleString("grp1");
      SimpleString groupId2 = new SimpleString("grp2");
      int numMessages = 100;
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, clientSession);
         if (i % 2 == 0 || i == 0)
         {
            message.putStringProperty(Message.HDR_GROUP_ID, groupId);
         }
         else
         {
            message.putStringProperty(Message.HDR_GROUP_ID, groupId2);
         }
         clientProducer.send(message);
      }
      if (!directDelivery)
      {
         clientSession.start();
      }
      CountDownLatch latch = new CountDownLatch(numMessages);
      DummyMessageHandler dummyMessageHandler = new DummyMessageHandler(latch, true);
      consumer.setMessageHandler(dummyMessageHandler);
      DummyMessageHandler dummyMessageHandler2 = new DummyMessageHandler(latch, true);
      consumer2.setMessageHandler(dummyMessageHandler2);
      Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
      Assert.assertEquals(50, dummyMessageHandler.list.size());
      int i = 0;
      for (ClientMessage message : dummyMessageHandler.list)
      {
         Assert.assertEquals(message.getBodyBuffer().readString(), "m" + i);
         i += 2;
      }
      Assert.assertEquals(50, dummyMessageHandler2.list.size());
      i = 1;
      for (ClientMessage message : dummyMessageHandler2.list)
      {
         Assert.assertEquals(message.getBodyBuffer().readString(), "m" + i);
         i += 2;
      }
      consumer.close();
      consumer2.close();
   }

   @Override
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

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      ConfigurationImpl configuration = new ConfigurationImpl();
      configuration.setSecurityEnabled(false);
      TransportConfiguration transportConfig = new TransportConfiguration(UnitTestCase.INVM_ACCEPTOR_FACTORY);
      configuration.getAcceptorConfigurations().add(transportConfig);
      server = HornetQServers.newHornetQServer(configuration, false);
      // start the server
      server.start();

      // then we create a client as normal
      ClientSessionFactory sessionFactory = HornetQClient.createClientSessionFactory(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));
      clientSession = sessionFactory.createSession(false, true, true);
      clientSession.createQueue(qName, qName, null, false);
   }

   private static class DummyMessageHandler implements MessageHandler
   {
      ArrayList<ClientMessage> list = new ArrayList<ClientMessage>();

      private CountDownLatch latch;

      private final boolean acknowledge;

      public DummyMessageHandler(final CountDownLatch latch, final boolean acknowledge)
      {
         this.latch = latch;
         this.acknowledge = acknowledge;
      }

      public void onMessage(final ClientMessage message)
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
               // ignore
            }
         }
         latch.countDown();
      }

      public void reset(final CountDownLatch latch)
      {
         list.clear();
         this.latch = latch;
      }
   }
}
