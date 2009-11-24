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

package org.hornetq.tests.integration.cluster.failover;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
import org.hornetq.core.client.SessionFailureListener;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.remoting.Interceptor;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.invm.TransportConstants;
import org.hornetq.core.transaction.impl.XidImpl;
import org.hornetq.jms.client.HornetQTextMessage;
import org.hornetq.utils.SimpleString;

/**
 * 
 * A FailoverTest
 * 
 * Tests:
 * 
 * Failover via shared storage manager:
 * 
 * 
 * 5) Failover due to failure on create session
 * 
 * 6) Replicate above tests on JMS API
 * 
 * 7) Repeat above tests using replicated journal
 * 
 * 8) Test with different values of auto commit acks and autocomit sends
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class FailoverTest extends FailoverTestBase
{
   private static final Logger log = Logger.getLogger(FailoverTest.class);

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   /**
    * @param name
    */
   public FailoverTest(String name)
   {
      super(name);
   }

   public FailoverTest()
   {
   }

   abstract class BaseListener implements SessionFailureListener
   {
      public void beforeReconnect(HornetQException me)
      {
      }
   }

   public void testNonTransacted() throws Exception
   {
      ClientSessionFactoryInternal sf = getSessionFactory();

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      ClientSession session = sf.createSession(true, true);

      session.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener extends BaseListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      session.addFailureListener(new MyListener());

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(i % 2 == 0);

         setBody(i, message);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      fail(session, latch);

      log.info("got here 1");

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         // Only the persistent messages will survive

         if (i % 2 == 0)
         {
            ClientMessage message = consumer.receive(1000);

            assertNotNull(message);

            assertMessageBody(i, message);

            assertEquals(i, message.getIntProperty("counter").intValue());

            message.acknowledge();
         }
      }

      log.info("closing session");
      session.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   /** It doesn't fail, but it restart both servers, live and backup, and the data should be received after the restart,
    *  and the servers should be able to connect without any problems. */
   public void testRestartServers() throws Exception
   {
      ClientSessionFactoryInternal sf = getSessionFactory();

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      ClientSession session = sf.createSession(true, true);

      session.createQueue(ADDRESS, ADDRESS, null, true);

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(true);

         setBody(i, message);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      session.commit();

      session.close();

      server0Service.stop();
      server1Service.stop();

      server1Service.start();
      server0Service.start();

      sf = getSessionFactory();

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      session = sf.createSession(true, true);

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);

         assertNotNull(message);

         assertMessageBody(i, message);

         assertEquals(i, message.getIntProperty("counter").intValue());

         message.acknowledge();
      }

      log.info("closing session");
      session.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   /**
    * @param session
    * @param latch
    * @throws InterruptedException
    */
   private void fail(ClientSession session, final CountDownLatch latch) throws InterruptedException
   {

      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      // Simulate failure on connection
      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      // Wait to be informed of failure

      boolean ok = latch.await(1000, TimeUnit.MILLISECONDS);

      assertTrue(ok);
   }

   public void testTransactedMessagesSentSoRollback() throws Exception
   {
      ClientSessionFactoryInternal sf = getSessionFactory();

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      ClientSession session = sf.createSession(false, false);

      session.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener extends BaseListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }

      }

      session.addFailureListener(new MyListener());

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(i % 2 == 0);

         setBody(i, message);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      fail(session, latch);

      assertTrue(session.isRollbackOnly());

      try
      {
         session.commit();

         fail("Should throw exception");
      }
      catch (HornetQException e)
      {
         assertEquals(HornetQException.TRANSACTION_ROLLED_BACK, e.getCode());
      }

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      ClientMessage message = consumer.receiveImmediate();

      assertNull(message);

      session.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testTransactedMessagesNotSentSoNoRollback() throws Exception
   {
      ClientSessionFactoryInternal sf = getSessionFactory();

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      ClientSession session = sf.createSession(false, false);

      session.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener extends BaseListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      session.addFailureListener(new MyListener());

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(i % 2 == 0);

         setBody(i, message);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      session.commit();

      fail(session, latch);

      // committing again should work since didn't send anything since last commit

      assertFalse(session.isRollbackOnly());

      session.commit();

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         // Only the persistent messages will survive

         if (i % 2 == 0)
         {
            ClientMessage message = consumer.receive(1000);

            assertNotNull(message);

            assertMessageBody(i, message);

            assertEquals(i, message.getIntProperty("counter").intValue());

            message.acknowledge();
         }
      }

      assertNull(consumer.receiveImmediate());

      session.commit();

      session.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testTransactedMessagesWithConsumerStartedBedoreFailover() throws Exception
   {
      ClientSessionFactoryInternal sf = getSessionFactory();

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      ClientSession session = sf.createSession(false, false);

      session.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener extends BaseListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      session.addFailureListener(new MyListener());

      // create a consumer and start the session before failover
      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(i % 2 == 0);

         setBody(i, message);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      // messages will be delivered to the consumer when the session is committed
      session.commit();

      assertFalse(session.isRollbackOnly());

      fail(session, latch);

      session.commit();

      session.close();

      session = sf.createSession(false, false);

      consumer = session.createConsumer(ADDRESS);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         // Only the persistent messages will survive

         if (i % 2 == 0)
         {
            ClientMessage message = consumer.receive(1000);

            assertNotNull(message);

            assertMessageBody(i, message);

            assertEquals(i, message.getIntProperty("counter").intValue());

            message.acknowledge();
         }
      }

      assertNull(consumer.receiveImmediate());

      session.commit();

      session.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testTransactedMessagesConsumedSoRollback() throws Exception
   {
      ClientSessionFactoryInternal sf = getSessionFactory();

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      ClientSession session1 = sf.createSession(false, false);

      session1.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener extends BaseListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      session1.addFailureListener(new MyListener());

      ClientProducer producer = session1.createProducer(ADDRESS);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session1.createClientMessage(i % 2 == 0);

         setBody(i, message);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      session1.commit();

      ClientSession session2 = sf.createSession(false, false);

      ClientConsumer consumer = session2.createConsumer(ADDRESS);

      session2.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);

         assertNotNull(message);

         assertMessageBody(i, message);

         assertEquals(i, message.getIntProperty("counter").intValue());

         message.acknowledge();
      }

      fail(session2, latch);

      assertTrue(session2.isRollbackOnly());

      try
      {
         session2.commit();

         fail("Should throw exception");
      }
      catch (HornetQException e)
      {
         assertEquals(HornetQException.TRANSACTION_ROLLED_BACK, e.getCode());
      }

      session1.close();

      session2.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testTransactedMessagesNotConsumedSoNoRollback() throws Exception
   {
      ClientSessionFactoryInternal sf = getSessionFactory();

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      ClientSession session1 = sf.createSession(false, false);

      session1.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener extends BaseListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      session1.addFailureListener(new MyListener());

      ClientProducer producer = session1.createProducer(ADDRESS);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session1.createClientMessage(true);

         setBody(i, message);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      session1.commit();

      ClientSession session2 = sf.createSession(false, false);

      ClientConsumer consumer = session2.createConsumer(ADDRESS);

      session2.start();

      for (int i = 0; i < numMessages / 2; i++)
      {
         ClientMessage message = consumer.receive(1000);

         assertNotNull(message);

         assertMessageBody(i, message);

         assertEquals(i, message.getIntProperty("counter").intValue());

         message.acknowledge();
      }

      session2.commit();

      consumer.close();

      fail(session2, latch);

      assertFalse(session2.isRollbackOnly());

      consumer = session2.createConsumer(ADDRESS);

      for (int i = numMessages / 2; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);

         assertNotNull(message);

         assertMessageBody(i, message);

         assertEquals(i, message.getIntProperty("counter").intValue());

         message.acknowledge();
      }

      session2.commit();

      assertNull(consumer.receiveImmediate());

      session1.close();

      session2.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testXAMessagesSentSoRollbackOnEnd() throws Exception
   {
      ClientSessionFactoryInternal sf = getSessionFactory();

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      ClientSession session = sf.createSession(true, false, false);

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener extends BaseListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      session.addFailureListener(new MyListener());

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 100;

      session.start(xid, XAResource.TMNOFLAGS);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(i % 2 == 0);

         setBody(i, message);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      fail(session, latch);

      try
      {
         session.end(xid, XAResource.TMSUCCESS);

         fail("Should throw exception");
      }
      catch (XAException e)
      {
         assertEquals(XAException.XA_RBOTHER, e.errorCode);
      }

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      ClientMessage message = consumer.receiveImmediate();

      assertNull(message);

      session.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testXAMessagesSentSoRollbackOnPrepare() throws Exception
   {
      ClientSessionFactoryInternal sf = getSessionFactory();

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      ClientSession session = sf.createSession(true, false, false);

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener extends BaseListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      session.addFailureListener(new MyListener());

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 100;

      session.start(xid, XAResource.TMNOFLAGS);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(i % 2 == 0);

         setBody(i, message);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      session.end(xid, XAResource.TMSUCCESS);

      fail(session, latch);

      try
      {
         session.prepare(xid);

         fail("Should throw exception");
      }
      catch (XAException e)
      {
         assertEquals(XAException.XA_RBOTHER, e.errorCode);
      }

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      ClientMessage message = consumer.receiveImmediate();

      assertNull(message);

      session.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   // This might happen if 1PC optimisation kicks in
   public void testXAMessagesSentSoRollbackOnCommit() throws Exception
   {
      ClientSessionFactoryInternal sf = getSessionFactory();

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      ClientSession session = sf.createSession(true, false, false);

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener extends BaseListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      session.addFailureListener(new MyListener());

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 100;

      session.start(xid, XAResource.TMNOFLAGS);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(i % 2 == 0);

         setBody(i, message);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      session.end(xid, XAResource.TMSUCCESS);

      session.prepare(xid);

      fail(session, latch);

      try
      {
         session.commit(xid, true);

         fail("Should throw exception");
      }
      catch (XAException e)
      {
         assertEquals(XAException.XA_RBOTHER, e.errorCode);
      }

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      ClientMessage message = consumer.receiveImmediate();

      assertNull(message);

      session.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testXAMessagesNotSentSoNoRollbackOnCommit() throws Exception
   {
      ClientSessionFactoryInternal sf = getSessionFactory();

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      ClientSession session = sf.createSession(true, false, false);

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener extends BaseListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      session.addFailureListener(new MyListener());

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 100;

      session.start(xid, XAResource.TMNOFLAGS);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(i % 2 == 0);

         setBody(i, message);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      session.end(xid, XAResource.TMSUCCESS);

      session.prepare(xid);

      session.commit(xid, false);

      fail(session, latch);

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      Xid xid2 = new XidImpl("tfytftyf".getBytes(), 54654, "iohiuohiuhgiu".getBytes());

      session.start(xid2, XAResource.TMNOFLAGS);

      for (int i = 0; i < numMessages; i++)
      {
         // Only the persistent messages will survive

         if (i % 2 == 0)
         {
            ClientMessage message = consumer.receive(1000);

            assertNotNull(message);

            assertMessageBody(i, message);

            assertEquals(i, message.getIntProperty("counter").intValue());

            message.acknowledge();
         }
      }

      session.end(xid2, XAResource.TMSUCCESS);

      session.prepare(xid2);

      session.commit(xid2, false);

      session.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testXAMessagesConsumedSoRollbackOnEnd() throws Exception
   {
      ClientSessionFactoryInternal sf = getSessionFactory();

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      ClientSession session1 = sf.createSession(false, false);

      session1.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener extends BaseListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      session1.addFailureListener(new MyListener());

      ClientProducer producer = session1.createProducer(ADDRESS);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session1.createClientMessage(i % 2 == 0);

         setBody(i, message);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      session1.commit();

      ClientSession session2 = sf.createSession(true, false, false);

      ClientConsumer consumer = session2.createConsumer(ADDRESS);

      session2.start();

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session2.start(xid, XAResource.TMNOFLAGS);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);

         assertNotNull(message);

         assertMessageBody(i, message);

         assertEquals(i, message.getIntProperty("counter").intValue());

         message.acknowledge();
      }

      fail(session2, latch);

      try
      {
         session2.end(xid, XAResource.TMSUCCESS);

         fail("Should throw exception");
      }
      catch (XAException e)
      {
         assertEquals(XAException.XA_RBOTHER, e.errorCode);
      }

      session1.close();

      session2.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testXAMessagesConsumedSoRollbackOnPrepare() throws Exception
   {
      ClientSessionFactoryInternal sf = getSessionFactory();

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      ClientSession session1 = sf.createSession(false, false);

      session1.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener extends BaseListener
      {
         public void connectionFailed(HornetQException me)
         {
            log.info("calling listener");
            latch.countDown();
         }
      }

      session1.addFailureListener(new MyListener());

      ClientProducer producer = session1.createProducer(ADDRESS);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session1.createClientMessage(i % 2 == 0);

         setBody(i, message);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      session1.commit();

      ClientSession session2 = sf.createSession(true, false, false);

      ClientConsumer consumer = session2.createConsumer(ADDRESS);

      session2.start();

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session2.start(xid, XAResource.TMNOFLAGS);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);

         assertNotNull(message);

         assertMessageBody(i, message);

         assertEquals(i, message.getIntProperty("counter").intValue());

         message.acknowledge();
      }

      session2.end(xid, XAResource.TMSUCCESS);

      RemotingConnection conn = ((ClientSessionInternal)session2).getConnection();

      // Simulate failure on connection
      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      // Wait to be informed of failure

      boolean ok = latch.await(1000, TimeUnit.MILLISECONDS);

      log.info("waited for latch");

      assertTrue(ok);

      try
      {
         session2.prepare(xid);

         fail("Should throw exception");
      }
      catch (XAException e)
      {
         assertEquals(XAException.XA_RBOTHER, e.errorCode);
      }

      // Thread.sleep(30000);

      session1.close();

      session2.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   // 1PC optimisation
   public void testXAMessagesConsumedSoRollbackOnCommit() throws Exception
   {
      ClientSessionFactoryInternal sf = getSessionFactory();

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      ClientSession session1 = sf.createSession(false, false);

      session1.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener extends BaseListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      session1.addFailureListener(new MyListener());

      ClientProducer producer = session1.createProducer(ADDRESS);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session1.createClientMessage(i % 2 == 0);

         setBody(i, message);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      session1.commit();

      ClientSession session2 = sf.createSession(true, false, false);

      ClientConsumer consumer = session2.createConsumer(ADDRESS);

      session2.start();

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session2.start(xid, XAResource.TMNOFLAGS);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);

         assertNotNull(message);

         assertMessageBody(i, message);

         assertEquals(i, message.getIntProperty("counter").intValue());

         message.acknowledge();
      }

      session2.end(xid, XAResource.TMSUCCESS);

      session2.prepare(xid);

      fail(session2, latch);

      try
      {
         session2.commit(xid, true);

         fail("Should throw exception");
      }
      catch (XAException e)
      {
         assertEquals(XAException.XA_RBOTHER, e.errorCode);
      }

      session1.close();

      session2.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testCreateNewFactoryAfterFailover() throws Exception
   {
      ClientSessionFactoryInternal sf = getSessionFactory();

      ClientSession session = sendAndConsume(sf);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener extends BaseListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      conn.addFailureListener(new MyListener());

      // Simulate failure on connection
      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      // Wait to be informed of failure

      boolean ok = latch.await(1000, TimeUnit.MILLISECONDS);

      assertTrue(ok);

      session.close();

      sf = new ClientSessionFactoryImpl(getConnectorTransportConfiguration(false));

      session = sendAndConsume(sf);

      session.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testFailoverMultipleSessionsWithConsumers() throws Exception
   {
      ClientSessionFactoryInternal sf = getSessionFactory();

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      final int numSessions = 10;

      final int numConsumersPerSession = 5;

      Map<ClientSession, List<ClientConsumer>> sessionConsumerMap = new HashMap<ClientSession, List<ClientConsumer>>();

      class MyListener extends BaseListener
      {
         CountDownLatch latch = new CountDownLatch(1);

         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      List<MyListener> listeners = new ArrayList<MyListener>();

      for (int i = 0; i < numSessions; i++)
      {
         ClientSession session = sf.createSession(true, true);

         List<ClientConsumer> consumers = new ArrayList<ClientConsumer>();

         for (int j = 0; j < numConsumersPerSession; j++)
         {
            SimpleString queueName = new SimpleString("queue" + i + "-" + j);

            session.createQueue(ADDRESS, queueName, null, true);

            ClientConsumer consumer = session.createConsumer(queueName);

            consumers.add(consumer);
         }

         sessionConsumerMap.put(session, consumers);
      }

      ClientSession sendSession = sf.createSession(true, true);

      ClientProducer producer = sendSession.createProducer(ADDRESS);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = sendSession.createClientMessage(true);

         setBody(i, message);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      RemotingConnection conn = ((ClientSessionInternal)sendSession).getConnection();

      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      // Wait to be informed of failure

      for (MyListener listener : listeners)
      {
         boolean ok = listener.latch.await(1000, TimeUnit.MILLISECONDS);

         assertTrue(ok);
      }

      for (ClientSession session : sessionConsumerMap.keySet())
      {
         session.start();
      }

      for (List<ClientConsumer> consumerList : sessionConsumerMap.values())
      {
         for (ClientConsumer consumer : consumerList)
         {
            for (int i = 0; i < numMessages; i++)
            {
               ClientMessage message = consumer.receive(1000);

               assertNotNull(message);

               assertMessageBody(i, message);

               assertEquals(i, message.getIntProperty("counter").intValue());

               message.acknowledge();
            }
         }
      }

      for (ClientSession session : sessionConsumerMap.keySet())
      {
         session.close();
      }

      sendSession.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   /*
    * Browser will get reset to beginning after failover
    */
   public void testFailWithBrowser() throws Exception
   {
      ClientSessionFactoryInternal sf = this.getSessionFactory();

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      ClientSession session = sf.createSession(true, true);

      session.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener extends BaseListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      session.addFailureListener(new MyListener());

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(i % 2 == 0);

         setBody(i, message);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(ADDRESS, true);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);

         assertNotNull(message);

         assertMessageBody(i, message);

         assertEquals(i, message.getIntProperty("counter").intValue());
      }

      fail(session, latch);

      for (int i = 0; i < numMessages; i++)
      {
         // Only the persistent messages will survive

         if (i % 2 == 0)
         {
            ClientMessage message = consumer.receive(1000);

            assertNotNull(message);

            assertMessageBody(i, message);

            assertEquals(i, message.getIntProperty("counter").intValue());

            message.acknowledge();
         }
      }

      session.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testFailThenReceiveMoreMessagesAfterFailover() throws Exception
   {
      ClientSessionFactoryInternal sf = getSessionFactory();

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      ClientSession session = sf.createSession(true, true);

      session.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener extends BaseListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      session.addFailureListener(new MyListener());

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(i % 2 == 0);

         setBody(i, message);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);

         assertNotNull(message);

         assertMessageBody(i, message);

         assertEquals(i, message.getIntProperty("counter").intValue());
      }

      fail(session, latch);

      // Should get the same ones after failover since we didn't ack

      for (int i = 0; i < numMessages; i++)
      {
         // Only the persistent messages will survive

         if (i % 2 == 0)
         {
            ClientMessage message = consumer.receive(1000);

            assertNotNull(message);

            assertMessageBody(i, message);

            assertEquals(i, message.getIntProperty("counter").intValue());

            message.acknowledge();
         }
      }

      session.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testFailThenReceiveMoreMessagesAfterFailover2() throws Exception
   {
      ClientSessionFactoryInternal sf = getSessionFactory();

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);
      sf.setBlockOnAcknowledge(true);

      ClientSession session = sf.createSession(true, true, 0);

      session.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener extends BaseListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      session.addFailureListener(new MyListener());

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(i % 2 == 0);

         setBody(i, message);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);

         assertNotNull(message);

         assertMessageBody(i, message);

         assertEquals(i, message.getIntProperty("counter").intValue());

         message.acknowledge();
      }

      fail(session, latch);

      // Send some more

      for (int i = numMessages; i < numMessages * 2; i++)
      {
         ClientMessage message = session.createClientMessage(i % 2 == 0);

         setBody(i, message);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      // Should get the same ones after failover since we didn't ack

      for (int i = numMessages; i < numMessages * 2; i++)
      {
         ClientMessage message = consumer.receive(1000);

         assertNotNull(message);

         assertMessageBody(i, message);

         assertEquals(i, message.getIntProperty("counter").intValue());

         message.acknowledge();
      }

      session.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testSimpleSendAfterFailover() throws Exception
   {
      ClientSessionFactoryInternal sf = getSessionFactory();

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);
      sf.setBlockOnAcknowledge(true);

      ClientSession session = sf.createSession(true, true, 0);

      session.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener extends BaseListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      session.addFailureListener(new MyListener());

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 100;

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      fail(session, latch);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(i % 2 == 0);

         setBody(i, message);
         
         System.out.println("Durable = " + message.isDurable());

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);

         assertNotNull(message);

         assertMessageBody(i, message);

         assertEquals(i, message.getIntProperty("counter").intValue());

         message.acknowledge();
      }

      session.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testForceBlockingReturn() throws Exception
   {
      ClientSessionFactoryInternal sf = this.getSessionFactory();

      // Add an interceptor to delay the send method so we can get time to cause failover before it returns

      server0Service.getRemotingService().addInterceptor(new DelayInterceptor());

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);
      sf.setBlockOnAcknowledge(true);

      final ClientSession session = sf.createSession(true, true, 0);

      session.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener extends BaseListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      session.addFailureListener(new MyListener());

      final ClientProducer producer = session.createProducer(ADDRESS);

      class Sender extends Thread
      {
         public void run()
         {
            ClientMessage message = session.createClientMessage(true);

            message.getBody().writeString("message");

            try
            {
               producer.send(message);
            }
            catch (HornetQException e)
            {
               this.e = e;
            }
         }

         volatile HornetQException e;
      }

      Sender sender = new Sender();

      sender.start();

      Thread.sleep(500);

      fail(session, latch);

      sender.join();

      assertNotNull(sender.e);

      assertEquals(sender.e.getCode(), HornetQException.UNBLOCKED);

      session.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testCommitOccurredUnblockedAndResendNoDuplicates() throws Exception
   {
      final ClientSessionFactoryInternal sf = this.getSessionFactory();

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);
      sf.setBlockOnAcknowledge(true);

      final ClientSession session = sf.createSession(false, false);

      session.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener extends BaseListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      session.addFailureListener(new MyListener());

      final int numMessages = 100;

      ClientProducer producer = session.createProducer(ADDRESS);

      String txID = "my-tx-id";

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(true);

         if (i == 0)
         {
            // Only need to add it on one message per tx
            message.putStringProperty(MessageImpl.HDR_DUPLICATE_DETECTION_ID, new SimpleString(txID));
         }

         setBody(i, message);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      class Committer extends Thread
      {
         public void run()
         {
            Interceptor interceptor = new DelayInterceptor2();

            try
            {
               sf.addInterceptor(interceptor);

               session.commit();
            }
            catch (HornetQException e)
            {
               if (e.getCode() == HornetQException.UNBLOCKED)
               {
                  // Ok - now we retry the commit after removing the interceptor

                  sf.removeInterceptor(interceptor);

                  try
                  {
                     session.commit();
                     fail("commit succeeded");
                  }
                  catch (HornetQException e2)
                  {
                     if (e2.getCode() == HornetQException.TRANSACTION_ROLLED_BACK)
                     {
                        // Ok

                        failed = false;
                     }
                  }
               }
            }
         }

         volatile boolean failed = true;
      }

      Committer committer = new Committer();

      committer.start();

      Thread.sleep(500);

      fail(session, latch);

      committer.join();

      assertFalse(committer.failed);

      session.close();

      ClientSession session2 = sf.createSession(false, false);

      producer = session2.createProducer(ADDRESS);

      // We now try and resend the messages since we get a transaction rolled back exception
      // but the commit actually succeeded, duplicate detection should kick in and prevent dups

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session2.createClientMessage(true);

         if (i == 0)
         {
            // Only need to add it on one message per tx
            message.putStringProperty(MessageImpl.HDR_DUPLICATE_DETECTION_ID, new SimpleString(txID));
         }

         setBody(i, message);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      session2.commit();

      ClientConsumer consumer = session2.createConsumer(ADDRESS);

      session2.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);

         assertNotNull(message);

         assertMessageBody(i, message);

         assertEquals(i, message.getIntProperty("counter").intValue());

         message.acknowledge();
      }

      ClientMessage message = consumer.receiveImmediate();

      assertNull(message);

      session2.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testCommitDidNotOccurUnblockedAndResend() throws Exception
   {
      ClientSessionFactoryInternal sf = this.getSessionFactory();

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);
      sf.setBlockOnAcknowledge(true);

      final ClientSession session = sf.createSession(false, false);

      session.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener extends BaseListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      session.addFailureListener(new MyListener());

      final int numMessages = 100;

      ClientProducer producer = session.createProducer(ADDRESS);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(true);

         setBody(i, message);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      class Committer extends Thread
      {
         public void run()
         {
            Interceptor interceptor = new DelayInterceptor3();

            try
            {
               server0Service.getRemotingService().addInterceptor(interceptor);

               session.commit();
            }
            catch (HornetQException e)
            {
               if (e.getCode() == HornetQException.UNBLOCKED)
               {
                  // Ok - now we retry the commit after removing the interceptor

                  server0Service.getRemotingService().removeInterceptor(interceptor);

                  try
                  {
                     session.commit();
                  }
                  catch (HornetQException e2)
                  {
                     if (e2.getCode() == HornetQException.TRANSACTION_ROLLED_BACK)
                     {
                        // Ok

                        failed = false;
                     }
                  }
               }
            }
         }

         volatile boolean failed = true;
      }

      Committer committer = new Committer();

      committer.start();

      Thread.sleep(500);

      fail(session, latch);

      committer.join();

      assertFalse(committer.failed);

      session.close();

      ClientSession session2 = sf.createSession(false, false);

      producer = session2.createProducer(ADDRESS);

      // We now try and resend the messages since we get a transaction rolled back exception

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session2.createClientMessage(true);

         setBody(i, message);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      session2.commit();

      ClientConsumer consumer = session2.createConsumer(ADDRESS);

      session2.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);

         assertNotNull(message);

         assertMessageBody(i, message);

         assertEquals(i, message.getIntProperty("counter").intValue());

         message.acknowledge();
      }

      ClientMessage message = consumer.receiveImmediate();

      assertNull(message);

      session2.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected TransportConfiguration getAcceptorTransportConfiguration(boolean live)
   {
      if (live)
      {
         return new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory");
      }
      else
      {
         Map<String, Object> server1Params = new HashMap<String, Object>();

         server1Params.put(TransportConstants.SERVER_ID_PROP_NAME, 1);

         return new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory", server1Params);
      }
   }

   @Override
   protected TransportConfiguration getConnectorTransportConfiguration(final boolean live)
   {
      if (live)
      {
         return new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory");
      }
      else
      {
         Map<String, Object> server1Params = new HashMap<String, Object>();

         server1Params.put(TransportConstants.SERVER_ID_PROP_NAME, 1);

         return new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory", server1Params);
      }
   }

   /**
    * @param i
    * @param message
    */
   protected void assertMessageBody(int i, ClientMessage message)
   {
      assertEquals("message" + i, message.getBody().readString());
   }

   /**
    * @param i
    * @param message
    * @throws Exception 
    */
   protected void setBody(int i, ClientMessage message) throws Exception
   {
      message.getBody().writeString("message" + i);
   }

   // Private -------------------------------------------------------

   private ClientSession sendAndConsume(final ClientSessionFactory sf) throws Exception
   {
      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, ADDRESS, null, false);

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(HornetQTextMessage.TYPE,
                                                             false,
                                                             0,
                                                             System.currentTimeMillis(),
                                                             (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBody().writeString("aardvarks");
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive();

         assertEquals("aardvarks", message2.getBody().readString());

         assertEquals(i, message2.getObjectProperty(new SimpleString("count")));

         message2.acknowledge();
      }

      ClientMessage message3 = consumer.receiveImmediate();

      assertNull(message3);

      return session;
   }

   // Inner classes -------------------------------------------------
}
