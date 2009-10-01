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
package org.hornetq.tests.integration.xa;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.TransactionOperation;
import org.hornetq.core.transaction.impl.XidImpl;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.SimpleString;
import org.hornetq.utils.UUIDGenerator;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class XaTimeoutTest extends UnitTestCase
{

   private Map<String, AddressSettings> addressSettings = new HashMap<String, AddressSettings>();

   private HornetQServer messagingService;
   
   private ClientSession clientSession;

   private ClientProducer clientProducer;

   private ClientConsumer clientConsumer;

   private ClientSessionFactory sessionFactory;

   private ConfigurationImpl configuration;

   private SimpleString atestq = new SimpleString("atestq");

   protected void setUp() throws Exception
   {
      super.setUp();
      
      addressSettings.clear();
      configuration = new ConfigurationImpl();
      configuration.setSecurityEnabled(false);
      configuration.setTransactionTimeoutScanPeriod(500);
      TransportConfiguration transportConfig = new TransportConfiguration(INVM_ACCEPTOR_FACTORY);
      configuration.getAcceptorConfigurations().add(transportConfig);
      messagingService = HornetQ.newHornetQServer(configuration, false);
      //start the server
      messagingService.start();
      //then we create a client as normal
      sessionFactory = new ClientSessionFactoryImpl(new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      clientSession = sessionFactory.createSession(true, false, false);
      clientSession.createQueue(atestq, atestq, null, true);
      clientProducer = clientSession.createProducer(atestq);
      clientConsumer = clientSession.createConsumer(atestq);
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
      

      clientProducer = null;;

      clientConsumer = null;

      sessionFactory = null;

      configuration = null;;
      
      
      super.tearDown();
   }

   public void testSimpleTimeoutOnSendOnCommit() throws Exception
   {
      Xid xid = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      ClientMessage m1 = createTextMessage("m1", clientSession);
      ClientMessage m2 = createTextMessage("m2", clientSession);
      ClientMessage m3 = createTextMessage("m3", clientSession);
      ClientMessage m4 = createTextMessage("m4", clientSession);
      clientSession.setTransactionTimeout(1);
      clientSession.start(xid, XAResource.TMNOFLAGS);
      clientProducer.send(m1);
      clientProducer.send(m2);
      clientProducer.send(m3);
      clientProducer.send(m4);
      clientSession.end(xid, XAResource.TMSUCCESS);
      CountDownLatch latch = new CountDownLatch(1);
      messagingService.getResourceManager().getTransaction(xid).addOperation(new RollbackCompleteOperation(latch));
      assertTrue(latch.await(5, TimeUnit.SECONDS));
      try
      {
         clientSession.commit(xid, true);
      }
      catch (XAException e)
      {
         assertTrue(e.errorCode == XAException.XAER_NOTA);
      }
      clientSession.start();
      ClientMessage m = clientConsumer.receive(500);
      assertNull(m);
   }

   public void testSimpleTimeoutOnReceive() throws Exception
   {
      Xid xid = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      ClientMessage m1 = createTextMessage("m1", clientSession);
      ClientMessage m2 = createTextMessage("m2", clientSession);
      ClientMessage m3 = createTextMessage("m3", clientSession);
      ClientMessage m4 = createTextMessage("m4", clientSession);
      ClientSession clientSession2 = sessionFactory.createSession(false, true, true);
      ClientProducer clientProducer2 = clientSession2.createProducer(atestq);
      clientProducer2.send(m1);
      clientProducer2.send(m2);
      clientProducer2.send(m3);
      clientProducer2.send(m4);
      clientSession2.close();
      clientSession.setTransactionTimeout(2);
      clientSession.start(xid, XAResource.TMNOFLAGS);
      clientSession.start();
      ClientMessage m = clientConsumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "m1");
      m = clientConsumer.receive(500);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m2");
      m = clientConsumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "m3");
      m = clientConsumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "m4");
      clientSession.end(xid, XAResource.TMSUCCESS);
      CountDownLatch latch = new CountDownLatch(1);
      messagingService.getResourceManager().getTransaction(xid).addOperation(new RollbackCompleteOperation(latch));
      assertTrue(latch.await(5, TimeUnit.SECONDS));
      try
      {
         clientSession.commit(xid, true);
      }
      catch (XAException e)
      {
         assertTrue(e.errorCode == XAException.XAER_NOTA);
      }
      clientSession.setTransactionTimeout(0);
      clientConsumer.close();
      clientSession2 = sessionFactory.createSession(false, true, true);
      ClientConsumer consumer = clientSession2.createConsumer(atestq);
      clientSession2.start();
      m = consumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "m1");
      m = consumer.receive(500);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m2");
      m = consumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "m3");
      m = consumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "m4");
      clientSession2.close();
   }

   public void testSimpleTimeoutOnSendAndReceive() throws Exception
   {
      Xid xid = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      ClientMessage m1 = createTextMessage("m1", clientSession);
      ClientMessage m2 = createTextMessage("m2", clientSession);
      ClientMessage m3 = createTextMessage("m3", clientSession);
      ClientMessage m4 = createTextMessage("m4", clientSession);
      ClientMessage m5 = createTextMessage("m5", clientSession);
      ClientMessage m6 = createTextMessage("m6", clientSession);
      ClientMessage m7 = createTextMessage("m7", clientSession);
      ClientMessage m8 = createTextMessage("m8", clientSession);
      ClientSession clientSession2 = sessionFactory.createSession(false, true, true);
      ClientProducer clientProducer2 = clientSession2.createProducer(atestq);
      clientProducer2.send(m1);
      clientProducer2.send(m2);
      clientProducer2.send(m3);
      clientProducer2.send(m4);
      clientSession2.close();
      clientSession.setTransactionTimeout(2);
      clientSession.start(xid, XAResource.TMNOFLAGS);
      clientSession.start();
      clientProducer.send(m5);
      clientProducer.send(m6);
      clientProducer.send(m7);
      clientProducer.send(m8);
      ClientMessage m = clientConsumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "m1");
      m = clientConsumer.receive(500);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m2");
      m = clientConsumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "m3");
      m = clientConsumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "m4");
      clientSession.end(xid, XAResource.TMSUCCESS);
      CountDownLatch latch = new CountDownLatch(1);
      messagingService.getResourceManager().getTransaction(xid).addOperation(new RollbackCompleteOperation(latch));
      assertTrue(latch.await(5, TimeUnit.SECONDS));
      try
      {
         clientSession.commit(xid, true);
      }
      catch (XAException e)
      {
         assertTrue(e.errorCode == XAException.XAER_NOTA);
      }
      clientSession.setTransactionTimeout(0);
      clientConsumer.close();
      clientSession2 = sessionFactory.createSession(false, true, true);
      ClientConsumer consumer = clientSession2.createConsumer(atestq);
      clientSession2.start();
      m = consumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "m1");
      m = consumer.receive(500);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m2");
      m = consumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "m3");
      m = consumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "m4");
      m = consumer.receive(500);
      assertNull(m);
      clientSession2.close();
   }

   public void testPreparedTransactionNotTimedOut() throws Exception
   {
      Xid xid = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      ClientMessage m1 = createTextMessage("m1", clientSession);
      ClientMessage m2 = createTextMessage("m2", clientSession);
      ClientMessage m3 = createTextMessage("m3", clientSession);
      ClientMessage m4 = createTextMessage("m4", clientSession);
      ClientMessage m5 = createTextMessage("m5", clientSession);
      ClientMessage m6 = createTextMessage("m6", clientSession);
      ClientMessage m7 = createTextMessage("m7", clientSession);
      ClientMessage m8 = createTextMessage("m8", clientSession);
      ClientSession clientSession2 = sessionFactory.createSession(false, true, true);
      ClientProducer clientProducer2 = clientSession2.createProducer(atestq);
      clientProducer2.send(m1);
      clientProducer2.send(m2);
      clientProducer2.send(m3);
      clientProducer2.send(m4);
      clientSession2.close();
      clientSession.setTransactionTimeout(2);
      clientSession.start(xid, XAResource.TMNOFLAGS);
      clientSession.start();
      clientProducer.send(m5);
      clientProducer.send(m6);
      clientProducer.send(m7);
      clientProducer.send(m8);
      ClientMessage m = clientConsumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "m1");
      m = clientConsumer.receive(500);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m2");
      m = clientConsumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "m3");
      m = clientConsumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "m4");
      clientSession.end(xid, XAResource.TMSUCCESS);
      clientSession.prepare(xid);
      CountDownLatch latch = new CountDownLatch(1);
      messagingService.getResourceManager().getTransaction(xid).addOperation(new RollbackCompleteOperation(latch));
      assertFalse(latch.await(2600, TimeUnit.MILLISECONDS));
      clientSession.commit(xid, true);

      clientSession.setTransactionTimeout(0);
      clientConsumer.close();
      clientSession2 = sessionFactory.createSession(false, true, true);
      ClientConsumer consumer = clientSession2.createConsumer(atestq);
      clientSession2.start();
      m = consumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "m5");
      m = consumer.receive(500);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m6");
      m = consumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "m7");
      m = consumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "m8");
      m = consumer.receive(500);
      assertNull(m);
      clientSession2.close();
   }

   public void testChangingTimeoutGetsPickedUp() throws Exception
   {
      Xid xid = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      ClientMessage m1 = createTextMessage("m1", clientSession);
      ClientMessage m2 = createTextMessage("m2", clientSession);
      ClientMessage m3 = createTextMessage("m3", clientSession);
      ClientMessage m4 = createTextMessage("m4", clientSession);
      clientSession.start(xid, XAResource.TMNOFLAGS);
      clientProducer.send(m1);
      clientProducer.send(m2);
      clientProducer.send(m3);
      clientProducer.send(m4);
      clientSession.end(xid, XAResource.TMSUCCESS);
      clientSession.setTransactionTimeout(1);
      CountDownLatch latch = new CountDownLatch(1);
      messagingService.getResourceManager().getTransaction(xid).addOperation(new RollbackCompleteOperation(latch));
      assertTrue(latch.await(5, TimeUnit.SECONDS));

      try
      {
         clientSession.commit(xid, true);
      }
      catch (XAException e)
      {
         assertTrue(e.errorCode == XAException.XAER_NOTA);
      }
      clientSession.start();
      ClientMessage m = clientConsumer.receive(500);
      assertNull(m);
   }

   public void testChangingTimeoutGetsPickedUpCommit() throws Exception
   {
      Xid xid = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      ClientMessage m1 = createTextMessage("m1", clientSession);
      ClientMessage m2 = createTextMessage("m2", clientSession);
      ClientMessage m3 = createTextMessage("m3", clientSession);
      ClientMessage m4 = createTextMessage("m4", clientSession);
      clientSession.setTransactionTimeout(2);
      clientSession.start(xid, XAResource.TMNOFLAGS);
      clientProducer.send(m1);
      clientProducer.send(m2);
      clientProducer.send(m3);
      clientProducer.send(m4);
      clientSession.end(xid, XAResource.TMSUCCESS);
      clientSession.setTransactionTimeout(10000);
      CountDownLatch latch = new CountDownLatch(1);
      messagingService.getResourceManager().getTransaction(xid).addOperation(new RollbackCompleteOperation(latch));
      assertFalse(latch.await(2600, TimeUnit.MILLISECONDS));
      clientSession.prepare(xid);
      clientSession.commit(xid, true);
      ClientSession clientSession2 = sessionFactory.createSession(false, true, true);
      ClientConsumer consumer = clientSession2.createConsumer(atestq);
      clientSession2.start();
      ClientMessage m = consumer.receive(500);
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "m1");
      m = consumer.receive(500);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m2");
      m = consumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "m3");
      m = consumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "m4");
      clientSession2.close();
   }

   public void testMultipleTransactionsTimedOut() throws Exception
   {
      Xid[] xids = new XidImpl[100];
      for (int i = 0; i < xids.length; i++)
      {
         xids[i] = new XidImpl(("xa" + i).getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());
      }
      ClientSession[] clientSessions = new ClientSession[xids.length];
      for (int i = 0; i < clientSessions.length; i++)
      {
         clientSessions[i] = sessionFactory.createSession(true, false, false);
      }

      ClientProducer[] clientProducers = new ClientProducer[xids.length];
      for (int i = 0; i < clientProducers.length; i++)
      {
         clientProducers[i] = clientSessions[i].createProducer(atestq);
      }

      ClientMessage[] messages = new ClientMessage[xids.length];

      for (int i = 0; i < messages.length; i++)
      {
         messages[i] = createTextMessage("m" + i, clientSession);
      }
      clientSession.setTransactionTimeout(2);
      for (int i = 0; i < clientSessions.length; i++)
      {
         clientSessions[i].start(xids[i], XAResource.TMNOFLAGS);
      }
      for (int i = 0; i < clientProducers.length; i++)
      {
         clientProducers[i].send(messages[i]);
      }
      for (int i = 0; i < clientSessions.length; i++)
      {
         clientSessions[i].end(xids[i], XAResource.TMSUCCESS);
      }
      CountDownLatch latch = new CountDownLatch(1);
      messagingService.getResourceManager().getTransaction(xids[clientSessions.length - 1]).addOperation(new RollbackCompleteOperation(latch));
      assertTrue(latch.await(5, TimeUnit.SECONDS));
      for (int i = 0; i < clientSessions.length; i++)
      {
         try
         {
            clientSessions[i].commit(xids[i], true);
         }
         catch (XAException e)
         {
            assertTrue(e.errorCode == XAException.XAER_NOTA);
         }
      }
      for (ClientSession session : clientSessions)
      {
         session.close();
      }
      clientSession.start();
      ClientMessage m = clientConsumer.receive(500);
      assertNull(m);
   }

   class RollbackCompleteOperation implements TransactionOperation
   {
      final CountDownLatch latch;

      public RollbackCompleteOperation(CountDownLatch latch)
      {
         this.latch = latch;
      }

      public void beforePrepare(Transaction tx) throws Exception
      {
      }

      public void beforeCommit(Transaction tx) throws Exception
      {
      }

      public void beforeRollback(Transaction tx) throws Exception
      {
      }

      public void afterPrepare(Transaction tx) throws Exception
      {
      }

      public void afterCommit(Transaction tx) throws Exception
      {
      }

      public void afterRollback(Transaction tx) throws Exception
      {
         latch.countDown();
      }

      public Collection<Queue> getDistinctQueues()
      {
         return Collections.emptySet();
      }
   }
}
