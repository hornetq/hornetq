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
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.hornetq.api.core.HornetQDuplicateIdException;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQExceptionType;
import org.hornetq.api.core.HornetQObjectClosedException;
import org.hornetq.api.core.HornetQTransactionOutcomeUnknownException;
import org.hornetq.api.core.HornetQTransactionRolledBackException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.MessageHandler;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.server.impl.InVMNodeManager;
import org.hornetq.core.transaction.impl.XidImpl;
import org.hornetq.jms.client.HornetQTextMessage;
import org.hornetq.tests.integration.IntegrationTestLogger;
import org.hornetq.tests.integration.cluster.util.TestableServer;
import org.hornetq.tests.util.CountDownSessionFailureListener;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.TransportConfigurationUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * A FailoverTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class FailoverTest extends FailoverTestBase
{

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   private static final int NUM_MESSAGES = 100;

   private ServerLocator locator;
   protected ClientSessionFactoryInternal sf;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      locator = getServerLocator();
   }

   protected ClientSession createSession(ClientSessionFactory sf1,
                                         boolean autoCommitSends,
                                         boolean autoCommitAcks,
                                         int ackBatchSize) throws Exception
   {
      return addClientSession(sf1.createSession(autoCommitSends, autoCommitAcks, ackBatchSize));
   }

   protected ClientSession
            createSession(ClientSessionFactory sf1, boolean autoCommitSends, boolean autoCommitAcks) throws Exception
   {
      return addClientSession(sf1.createSession(autoCommitSends, autoCommitAcks));
   }

   protected ClientSession createSession(ClientSessionFactory sf1) throws Exception
   {
      return addClientSession(sf1.createSession());
   }

   protected ClientSession createSession(ClientSessionFactory sf1,
                                         boolean xa,
                                         boolean autoCommitSends,
                                         boolean autoCommitAcks) throws Exception
   {
      return addClientSession(sf1.createSession(xa, autoCommitSends, autoCommitAcks));
   }

   // https://issues.jboss.org/browse/HORNETQ-685
   @Test
   public void testTimeoutOnFailover() throws Exception
   {
      locator.setCallTimeout(1000);
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setAckBatchSize(0);
      locator.setReconnectAttempts(-1);
      ((InVMNodeManager)nodeManager).failoverPause = 500;

      ClientSessionFactoryInternal sf1 = (ClientSessionFactoryInternal)createSessionFactory(locator);

      final ClientSession session = createSession(sf1, true, true);

      session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);

      final ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      final CountDownLatch latch = new CountDownLatch(10);
      final CountDownLatch latchFailed = new CountDownLatch(1);

      Runnable r = new Runnable()
      {
         public void run()
         {
            for (int i = 0; i < 500; i++)
            {
               ClientMessage message = session.createMessage(true);
               message.putIntProperty("counter", i);
               try
               {
                  System.out.println("Sent " + i);
                  producer.send(message);
                  if (i < 10)
                  {
                     latch.countDown();
                     if (latch.getCount() == 0)
                     {
                        latchFailed.await(10, TimeUnit.SECONDS);
                     }
                  }
               }
               catch (Exception e)
               {
                  // this is our retry
                  try
                  {
                     if (!producer.isClosed())
                        producer.send(message);
                  }
                  catch (HornetQException e1)
                  {
                     e1.printStackTrace();
                  }
               }
            }
         }
      };
      Thread t = new Thread(r);
      t.start();
      assertTrue("latch released", latch.await(10, TimeUnit.SECONDS));
      crash(session);
      latchFailed.countDown();
      t.join(30000);
      if (t.isAlive())
      {
         t.interrupt();
         fail("Thread still alive");
      }
      assertTrue(backupServer.getServer().waitForActivation(5, TimeUnit.SECONDS));
      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);
      session.start();
      for (int i = 0; i < 500; i++)
      {
         ClientMessage m = consumer.receive(1000);
         assertNotNull("message #=" + i, m);
         // assertEquals(i, m.getIntProperty("counter").intValue());
      }
   }

   // https://issues.jboss.org/browse/HORNETQ-685
   @Test
   public void testTimeoutOnFailoverConsume() throws Exception
   {
      locator.setCallTimeout(5000);
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setAckBatchSize(0);
      locator.setBlockOnAcknowledge(true);
      locator.setReconnectAttempts(-1);
      locator.setAckBatchSize(0);
      ((InVMNodeManager)nodeManager).failoverPause = 5000l;

      ClientSessionFactoryInternal sf1 = (ClientSessionFactoryInternal)createSessionFactory(locator);

      final ClientSession session = createSession(sf1, true, true);

      session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);

      final ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      for (int i = 0; i < 500; i++)
      {
         ClientMessage message = session.createMessage(true);
         message.putIntProperty("counter", i);
         producer.send(message);
      }

      final CountDownLatch latch = new CountDownLatch(1);
      final CountDownLatch endLatch = new CountDownLatch(1);

      final ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);
      session.start();

      final Map<Integer, ClientMessage> received = new HashMap<Integer, ClientMessage>();

      consumer.setMessageHandler(new MessageHandler()
      {

         public void onMessage(ClientMessage message)
         {
            Integer counter = message.getIntProperty("counter");
            received.put(counter, message);
            try
            {
               log.debug("acking message = id = " + message.getMessageID() + ", counter = " +
                     message.getIntProperty("counter"));
               message.acknowledge();
            } catch (HornetQException e)
            {
               e.printStackTrace();
               return;
            }
            log.debug("Acked counter = " + counter);
            if (counter.equals(10))
            {
               latch.countDown();
            }
            if (received.size() == 500)
            {
               endLatch.countDown();
            }
         }

      });
      latch.await(10, TimeUnit.SECONDS);
      log.info("crashing session");
      crash(session);
      endLatch.await(60, TimeUnit.SECONDS);
      assertTrue("received only " + received.size(), received.size() == 500);

      session.close();
   }

   @Test
   public void testTimeoutOnFailoverConsumeBlocked() throws Exception
   {
      locator.setCallTimeout(5000);
      locator.setBlockOnNonDurableSend(true);
      locator.setConsumerWindowSize(0);
      locator.setBlockOnDurableSend(true);
      locator.setAckBatchSize(0);
      locator.setBlockOnAcknowledge(true);
      locator.setReconnectAttempts(-1);
      locator.setAckBatchSize(0);
      ((InVMNodeManager)nodeManager).failoverPause = 5000l;

      ClientSessionFactoryInternal sf1 = (ClientSessionFactoryInternal)createSessionFactory(locator);

      final ClientSession session = createSession(sf1, true, true);

      session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);

      final ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      for (int i = 0; i < 500; i++)
      {
         ClientMessage message = session.createMessage(true);
         message.putIntProperty("counter", i);
         message.putBooleanProperty("end", i == 499);
         producer.send(message);
      }

      final CountDownLatch latch = new CountDownLatch(1);
      final CountDownLatch endLatch = new CountDownLatch(1);

      final ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);
      session.start();

      final Map<Integer, ClientMessage> received = new HashMap<Integer, ClientMessage>();

      Thread t = new Thread()
      {
         @Override
         public void run()
         {
            ClientMessage message = null;
            try
            {
               while ((message = getMessage()) != null)
               {
                  Integer counter = message.getIntProperty("counter");
                  received.put(counter, message);
                  try
                  {
                     log.info("acking message = id = " + message.getMessageID() +
                        ", counter = " +
                        message.getIntProperty("counter"));
                     message.acknowledge();
                  }
                  catch (HornetQException e)
                  {
                     e.printStackTrace();
                     continue;
                  }
                  log.info("Acked counter = " + counter);
                  if (counter.equals(10))
                  {
                     latch.countDown();
                  }
                  if (received.size() == 500)
                  {
                     endLatch.countDown();
                  }

                  if (message.getBooleanProperty("end"))
                  {
                     break;
                  }
               }
            }
            catch (Exception e)
            {
               fail("failing due to exception " + e);
            }

         }

         private ClientMessage getMessage()
         {
            while (true)
            {
               try
               {
                  ClientMessage msg = consumer.receive(20000);
                  if (msg == null)
                  {
                     log.info("Returning null message on consuming");
                  }
                  return msg;
               }
               catch(HornetQObjectClosedException oce)
               {
                  throw new RuntimeException(oce);
               }
               catch (HornetQException ignored)
               {
                  // retry
                  ignored.printStackTrace();
               }
            }
         }
      };
      t.start();
      latch.await(10, TimeUnit.SECONDS);
      log.info("crashing session");
      crash(session);
      endLatch.await(60, TimeUnit.SECONDS);
      t.join();
      assertTrue("received only " + received.size(), received.size() == 500);

      session.close();
   }

   // https://issues.jboss.org/browse/HORNETQ-685
   @Test
   public void testTimeoutOnFailoverTransactionCommit() throws Exception
   {
      locator.setCallTimeout(2000);
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setAckBatchSize(0);
      locator.setReconnectAttempts(-1);
      ((InVMNodeManager)nodeManager).failoverPause = 5000l;

      ClientSessionFactoryInternal sf1 = (ClientSessionFactoryInternal)createSessionFactory(locator);

      final ClientSession session = createSession(sf1, true, false, false);

      session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);

      final ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session.start(xid, XAResource.TMNOFLAGS);

      for (int i = 0; i < 500; i++)
      {
         ClientMessage message = session.createMessage(true);
         message.putIntProperty("counter", i);

         producer.send(message);

      }
      session.end(xid, XAResource.TMSUCCESS);
      session.prepare(xid);
      crash(false, session);

      try
      {
         session.commit(xid, false);
      }
      catch (XAException e)
      {
         //there is still an edge condition that we must deal with
         session.commit(xid, false);
      }

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);
      session.start();
      for (int i = 0; i < 500; i++)
      {
         ClientMessage m = consumer.receive(1000);
         assertNotNull(m);
         assertEquals(i, m.getIntProperty("counter").intValue());
      }
   }

   // https://issues.jboss.org/browse/HORNETQ-685
   @Test
   public void testTimeoutOnFailoverTransactionRollback() throws Exception
   {
      locator.setCallTimeout(2000);
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setAckBatchSize(0);
      locator.setReconnectAttempts(-1);
      ((InVMNodeManager)nodeManager).failoverPause = 5000l;

      ClientSessionFactoryInternal sf1 = (ClientSessionFactoryInternal)createSessionFactory(locator);

      final ClientSession session = createSession(sf1, true, false, false);

      session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);

      final ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session.start(xid, XAResource.TMNOFLAGS);

      for (int i = 0; i < 500; i++)
      {
         ClientMessage message = session.createMessage(true);
         message.putIntProperty("counter", i);

         producer.send(message);
      }

      session.end(xid, XAResource.TMSUCCESS);
      session.prepare(xid);
      crash(false, session);

      try
      {
         session.rollback(xid);
      }
      catch (XAException e)
      {
         //there is still an edge condition that we must deal with
         session.rollback(xid);
      }

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);
      session.start();

      ClientMessage m = consumer.receive(1000);
      assertNull(m);

   }

   /**
    * see http://jira.jboss.org/browse/HORNETQ-522
    * @throws Exception
    */
   @Test
   public void testNonTransactedWithZeroConsumerWindowSize() throws Exception
   {
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setAckBatchSize(0);
      locator.setReconnectAttempts(-1);

      createClientSessionFactory();

      ClientSession session = createSession(sf, true, true);

      session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         ClientMessage message = session.createMessage(true);

         setBody(i, message);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      int winSize = 0;
      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS, null, winSize, 100, false);

      final List<ClientMessage> received = new ArrayList<ClientMessage>();

      consumer.setMessageHandler(new MessageHandler()
      {

         public void onMessage(ClientMessage message)
         {
            received.add(message);
         }

      });

      session.start();

      crash(session);

      int retry = 0;
      while (received.size() < NUM_MESSAGES)
      {
         Thread.sleep(100);
         retry++;
         if (retry > 50)
         {
            break;
         }
      }
      session.close();
      final int retryLimit = 5;
      Assert.assertTrue("Number of retries (" + retry + ") should be <= " + retryLimit, retry <= retryLimit);
   }

   private void createClientSessionFactory() throws Exception
   {
      sf = (ClientSessionFactoryInternal)createSessionFactory(locator);
   }

   @Test
   public void testNonTransacted() throws Exception
   {
      createSessionFactory();

      ClientSession session = createSession(sf, true, true);

      session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      sendMessagesSomeDurable(session, producer);

      crash(session);

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      receiveDurableMessages(consumer);

      session.close();

      sf.close();

      Assert.assertEquals(0, sf.numSessions());

      Assert.assertEquals(0, sf.numConnections());
   }

   /**
    * Basic fail-back test.
    * @throws Exception
    */
   @Test
   public void testFailBack() throws Exception
   {
      boolean doFailBack = true;
      simpleReplication(doFailBack);
   }

   @Test
   public void testFailBackLiveRestartsBackupIsGone() throws Exception
   {
      locator.setFailoverOnInitialConnection(true);
      createSessionFactory();
      ClientSession session = createSessionAndQueue();

      ClientProducer producer = addClientProducer(session.createProducer(FailoverTestBase.ADDRESS));

      sendMessages(session, producer, NUM_MESSAGES);
      producer.close();
      session.commit();
      SimpleString liveId = liveServer.getServer().getNodeID();
      crash(session);

      session.start();
      ClientConsumer consumer = addClientConsumer(session.createConsumer(FailoverTestBase.ADDRESS));
      receiveMessages(consumer);
      assertNoMoreMessages(consumer);
      consumer.close();
      session.commit();

      assertEquals("backup must be running with the same nodeID", liveId, backupServer.getServer().getNodeID());
      sf.close();

      backupServer.crash();
      Thread.sleep(100);
      assertFalse("backup is not running", backupServer.isStarted());

      assertFalse("must NOT be a backup", liveServer.getServer().getConfiguration().isBackup());
      adaptLiveConfigForReplicatedFailBack(liveServer.getServer().getConfiguration());
      beforeRestart(liveServer);
      liveServer.start();
      assertTrue("live initialized...", liveServer.getServer().waitForActivation(15, TimeUnit.SECONDS));

      sf = (ClientSessionFactoryInternal)createSessionFactory(locator);

      ClientSession session2 = createSession(sf, false, false);
      session2.start();
      ClientConsumer consumer2 = session2.createConsumer(FailoverTestBase.ADDRESS);
      boolean replication = !liveServer.getServer().getConfiguration().isSharedStore();
      if (replication)
         receiveMessages(consumer2, 0, NUM_MESSAGES, true);
      assertNoMoreMessages(consumer2);
      session2.commit();
   }

   @Test
   public void testSimpleReplication() throws Exception
   {
      boolean doFailBack = false;
      simpleReplication(doFailBack);
   }

   @Test
   public void testWithoutUsingTheBackup() throws Exception
   {
      locator.setFailoverOnInitialConnection(true);
      createSessionFactory();
      ClientSession session = createSessionAndQueue();

      ClientProducer producer = addClientProducer(session.createProducer(FailoverTestBase.ADDRESS));

      sendMessages(session, producer, NUM_MESSAGES);
      producer.close();
      session.commit();

      backupServer.stop(); // Backup stops!
      backupServer.start();
      assertTrue(backupServer.getServer().waitForBackupSync(10, TimeUnit.SECONDS));

      session.start();
      ClientConsumer consumer = addClientConsumer(session.createConsumer(FailoverTestBase.ADDRESS));
      receiveMessages(consumer);
      assertNoMoreMessages(consumer);
      consumer.close();
      session.commit();

      session.start();
      producer = addClientProducer(session.createProducer(FailoverTestBase.ADDRESS));
      sendMessages(session, producer, NUM_MESSAGES);
      producer.close();
      session.commit();
      backupServer.stop(); // Backup stops!
      beforeRestart(backupServer);
      backupServer.start();
      assertTrue(backupServer.getServer().waitForBackupSync(10, TimeUnit.SECONDS));
      backupServer.stop(); // Backup stops!

      liveServer.stop();
      beforeRestart(liveServer);
      liveServer.start();
      liveServer.getServer().waitForActivation(10, TimeUnit.SECONDS);

      ClientSession session2 = createSession(sf, false, false);
      session2.start();
      ClientConsumer consumer2 = session2.createConsumer(FailoverTestBase.ADDRESS);
      receiveMessages(consumer2, 0, NUM_MESSAGES, true);
      assertNoMoreMessages(consumer2);
      session2.commit();
   }

   /**
    * @param doFailBack
    * @throws Exception
    * @throws HornetQException
    * @throws InterruptedException
    */
   private void simpleReplication(boolean doFailBack) throws Exception, HornetQException, InterruptedException
   {
      locator.setFailoverOnInitialConnection(true);
      createSessionFactory();
      ClientSession session = createSessionAndQueue();

      ClientProducer producer = addClientProducer(session.createProducer(FailoverTestBase.ADDRESS));

      sendMessages(session, producer, NUM_MESSAGES);
      producer.close();
      session.commit();
      SimpleString liveId = liveServer.getServer().getNodeID();
      crash(session);

      session.start();
      ClientConsumer consumer = addClientConsumer(session.createConsumer(FailoverTestBase.ADDRESS));
      receiveMessages(consumer);
      assertNoMoreMessages(consumer);
      consumer.close();

      producer = addClientProducer(session.createProducer(FailoverTestBase.ADDRESS));
      sendMessages(session, producer, NUM_MESSAGES);
      producer.close();
      session.commit();

      assertEquals("backup must be running with the same nodeID", liveId, backupServer.getServer().getNodeID());
      if (doFailBack)
      {
         assertFalse("must NOT be a backup", liveServer.getServer().getConfiguration().isBackup());
         adaptLiveConfigForReplicatedFailBack(liveServer.getServer().getConfiguration());
         beforeRestart(liveServer);
         liveServer.start();
         assertTrue("live initialized...", liveServer.getServer().waitForActivation(15, TimeUnit.SECONDS));
         int i = 0;
         while (backupServer.isStarted() && i++ < 100)
         {
            Thread.sleep(100);
         }
         assertFalse("Backup should stop!", backupServer.isStarted());
      }
      else
      {
         backupServer.stop();
         beforeRestart(backupServer);
         backupServer.start();
         assertTrue(backupServer.getServer().waitForActivation(10, TimeUnit.SECONDS));
      }

      ClientSession session2 = createSession(sf, false, false);
      session2.start();
      ClientConsumer consumer2 = session2.createConsumer(FailoverTestBase.ADDRESS);
      receiveMessages(consumer2, 0, NUM_MESSAGES, true);
      assertNoMoreMessages(consumer2);
      session2.commit();
   }

   /**
    * @param consumer
    * @throws HornetQException
    */
   private void assertNoMoreMessages(ClientConsumer consumer) throws HornetQException
   {
      ClientMessage msg = consumer.receiveImmediate();
      assertNull("there should be no more messages to receive! " + msg, msg);
   }

   protected void createSessionFactory() throws Exception
   {
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setReconnectAttempts(-1);

      sf = createSessionFactoryAndWaitForTopology(locator, 2);
   }

   @Test
   public void testConsumeTransacted() throws Exception
   {
      createSessionFactory();

      ClientSession session = createSessionAndQueue();

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      final int numMessages = 10;

      sendMessages(session, producer, numMessages);

      session.commit();

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);
         assertNotNull("Just crashed? " + (i == 6) + " " + i, message);

         message.acknowledge();

         // TODO: The test won't pass if you uncomment this line
         // assertEquals(i, (int)message.getIntProperty("counter"));

         if (i == 5)
         {
            crash(session);
         }
      }

      try
      {
         session.commit();
         fail("session must have rolled back on failover");
      }
      catch(HornetQTransactionRolledBackException trbe)
      {
         //ok
      }
      catch (HornetQException e)
      {
         fail("Invalid Exception type:" + e.getType());
      }

      consumer.close();

      consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);

         assertNotNull("Expecting message #" + i, message);

         message.acknowledge();
      }

      session.commit();

      session.close();
   }

   /**
    * @return
    * @throws Exception
    * @throws HornetQException
    */
   private ClientSession createSessionAndQueue() throws Exception, HornetQException
   {
      ClientSession session = createSession(sf, false, false);

      session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);
      return session;
   }

   // https://jira.jboss.org/jira/browse/HORNETQ-285
   @Test
   public void testFailoverOnInitialConnection() throws Exception
   {
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setFailoverOnInitialConnection(true);
      locator.setReconnectAttempts(-1);

      sf = createSessionFactoryAndWaitForTopology(locator, 2);

      // Crash live server
      crash();

      ClientSession session = createSession(sf);

      session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);


      sendMessages(session, producer, NUM_MESSAGES);

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      receiveMessages(consumer);

      session.close();
   }

   @Test
   public void testTransactedMessagesSentSoRollback() throws Exception
   {
      createSessionFactory();

      ClientSession session = createSessionAndQueue();

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      sendMessagesSomeDurable(session, producer);

      crash(session);

      Assert.assertTrue(session.isRollbackOnly());

      try
      {
         session.commit();

         Assert.fail("Should throw exception");
      }
      catch(HornetQTransactionRolledBackException trbe)
      {
         //ok
      }
      catch (HornetQException e)
      {
         fail("Invalid Exception type:" + e.getType());
      }

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      ClientMessage message = consumer.receiveImmediate();

      Assert.assertNull("message should be null! Was: " + message, message);

      session.close();
   }

   /**
    * Test that once the transacted session has throw a TRANSACTION_ROLLED_BACK exception,
    * it can be reused again
    */
   @Test
   public void testTransactedMessagesSentSoRollbackAndContinueWork() throws Exception
   {
      createSessionFactory();

      ClientSession session = createSessionAndQueue();

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      sendMessagesSomeDurable(session, producer);

      crash(session);

      Assert.assertTrue(session.isRollbackOnly());

      try
      {
         session.commit();

         Assert.fail("Should throw exception");
      }
      catch(HornetQTransactionRolledBackException trbe)
      {
         //ok
      }
      catch (HornetQException e)
      {
         fail("Invalid Exception type:" + e.getType());
      }

      ClientMessage message = session.createMessage(false);
      int counter = RandomUtil.randomInt();
      message.putIntProperty("counter", counter);

      producer.send(message);

      // session is working again
      session.commit();

      session.start();

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();
      message = consumer.receive(1000);

      Assert.assertNotNull("expecting a message", message);
      Assert.assertEquals(counter, message.getIntProperty("counter").intValue());

      session.close();
   }

   @Test
   public void testTransactedMessagesNotSentSoNoRollback() throws Exception
   {
      createSessionFactory();

      ClientSession session = createSessionAndQueue();

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      sendMessagesSomeDurable(session, producer);

      session.commit();

      crash(session);

      // committing again should work since didn't send anything since last commit

      Assert.assertFalse(session.isRollbackOnly());

      session.commit();

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      receiveDurableMessages(consumer);

      Assert.assertNull(consumer.receiveImmediate());

      session.commit();

      session.close();
   }

   @Test
   public void testTransactedMessagesWithConsumerStartedBeforeFailover() throws Exception
   {
      createSessionFactory();

      ClientSession session = createSessionAndQueue();

      // create a consumer and start the session before failover
      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      sendMessagesSomeDurable(session, producer);

      // messages will be delivered to the consumer when the session is committed
      session.commit();

      Assert.assertFalse(session.isRollbackOnly());

      crash(session);

      session.commit();

      session.close();

      session = createSession(sf, false, false);

      consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      receiveDurableMessages(consumer);

      Assert.assertNull(consumer.receiveImmediate());

      session.commit();
   }

   @Test
   public void testTransactedMessagesConsumedSoRollback() throws Exception
   {
      createSessionFactory();

      ClientSession session1 = createSessionAndQueue();

      ClientProducer producer = session1.createProducer(FailoverTestBase.ADDRESS);

      sendMessagesSomeDurable(session1, producer);

      session1.commit();

      ClientSession session2 = createSession(sf, false, false);

      ClientConsumer consumer = session2.createConsumer(FailoverTestBase.ADDRESS);

      session2.start();

      receiveMessages(consumer);

      crash(session2);

      Assert.assertTrue(session2.isRollbackOnly());

      try
      {
         session2.commit();

         Assert.fail("Should throw exception");
      }
      catch(HornetQTransactionRolledBackException trbe)
      {
         //ok
      }
      catch (HornetQException e)
      {
         fail("Invalid Exception type:" + e.getType());
      }
   }

   @Test
   public void testTransactedMessagesNotConsumedSoNoRollback() throws Exception
   {
      createSessionFactory();

      ClientSession session1 = createSessionAndQueue();

      ClientProducer producer = session1.createProducer(FailoverTestBase.ADDRESS);

      sendMessages(session1, producer, NUM_MESSAGES);
      session1.commit();

      ClientSession session2 = createSession(sf, false, false);

      ClientConsumer consumer = session2.createConsumer(FailoverTestBase.ADDRESS);

      session2.start();

      receiveMessages(consumer, 0, NUM_MESSAGES / 2, true);

      session2.commit();

      consumer.close();

      crash(session2);

      Assert.assertFalse(session2.isRollbackOnly());

      consumer = session2.createConsumer(FailoverTestBase.ADDRESS);

      for (int i = NUM_MESSAGES / 2; i < NUM_MESSAGES; i++)
      {
         ClientMessage message = consumer.receive(1000);

         Assert.assertNotNull("expecting message " + i, message);

         assertMessageBody(i, message);

         Assert.assertEquals(i, message.getIntProperty("counter").intValue());

         message.acknowledge();
      }

      session2.commit();

      Assert.assertNull(consumer.receiveImmediate());
   }

   @Test
   public void testXAMessagesSentSoRollbackOnEnd() throws Exception
   {
      createSessionFactory();

      ClientSession session = createSession(sf, true, false, false);

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      session.start(xid, XAResource.TMNOFLAGS);

      sendMessagesSomeDurable(session, producer);

      crash(session);

      try
      {
         session.end(xid, XAResource.TMSUCCESS);

         Assert.fail("Should throw exception");
      }
      catch (XAException e)
      {
         Assert.assertEquals(XAException.XA_RBOTHER, e.errorCode);
      }

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      ClientMessage message = consumer.receiveImmediate();

      Assert.assertNull(message);
   }


   //start a tx but sending messages after crash
   @Test
   public void testXAMessagesSentSoRollbackOnEnd2() throws Exception
   {
      createSessionFactory();

      ClientSession session = createSession(sf, true, false, false);

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      session.start(xid, XAResource.TMNOFLAGS);

      crash(session);

      // sendMessagesSomeDurable(session, producer);

      producer.send(createMessage(session, 1, true));

      try
      {
         session.end(xid, XAResource.TMSUCCESS);

         Assert.fail("Should throw exception");
      }
      catch (XAException e)
      {
//         Assert.assertEquals(XAException.XAER_NOTA, e.errorCode);
      }

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      ClientMessage message = consumer.receiveImmediate();

      Assert.assertNull(message);
   }

   @Test
   public void testXAMessagesSentSoRollbackOnPrepare() throws Exception
   {
      createSessionFactory();

      final ClientSession session = createSession(sf, true, false, false);

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);

      final ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      session.start(xid, XAResource.TMNOFLAGS);

      sendMessagesSomeDurable(session, producer);

      session.end(xid, XAResource.TMSUCCESS);

      crash(session);

      try
      {
         session.prepare(xid);

         Assert.fail("Should throw exception");
      }
      catch (XAException e)
      {
         Assert.assertEquals(XAException.XA_RBOTHER, e.errorCode);
         // XXXX  session.rollback();
      }

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      ClientMessage message = consumer.receiveImmediate();

      Assert.assertNull(message);

      producer.close();
      consumer.close();
   }

   // This might happen if 1PC optimisation kicks in
   @Test
   public void testXAMessagesSentSoRollbackOnCommit() throws Exception
   {
      createSessionFactory();

      ClientSession session = createSession(sf, true, false, false);

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);



      session.start(xid, XAResource.TMNOFLAGS);

      sendMessagesSomeDurable(session, producer);

      session.end(xid, XAResource.TMSUCCESS);

      crash(session);

      try
      {
         session.commit(xid, true);

         Assert.fail("Should throw exception");
      }
      catch (XAException e)
      {
         Assert.assertEquals(XAException.XAER_NOTA, e.errorCode);
      }

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      ClientMessage message = consumer.receiveImmediate();

      Assert.assertNull(message);
   }

   @Test
   public void testXAMessagesNotSentSoNoRollbackOnCommit() throws Exception
   {
      createSessionFactory();

      ClientSession session = createSession(sf, true, false, false);

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);



      session.start(xid, XAResource.TMNOFLAGS);

      sendMessagesSomeDurable(session, producer);

      session.end(xid, XAResource.TMSUCCESS);

      session.prepare(xid);

      session.commit(xid, false);

      crash(session);

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      Xid xid2 = new XidImpl("tfytftyf".getBytes(), 54654, "iohiuohiuhgiu".getBytes());

      session.start(xid2, XAResource.TMNOFLAGS);

      receiveDurableMessages(consumer);

      session.end(xid2, XAResource.TMSUCCESS);

      session.prepare(xid2);

      session.commit(xid2, false);
   }

   @Test
   public void testXAMessagesConsumedSoRollbackOnEnd() throws Exception
   {
      createSessionFactory();

      ClientSession session1 = createSessionAndQueue();

      ClientProducer producer = session1.createProducer(FailoverTestBase.ADDRESS);

      sendMessagesSomeDurable(session1, producer);

      session1.commit();

      ClientSession session2 = createSession(sf, true, false, false);

      ClientConsumer consumer = session2.createConsumer(FailoverTestBase.ADDRESS);

      session2.start();

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session2.start(xid, XAResource.TMNOFLAGS);

      receiveMessages(consumer);

      crash(session2);

      try
      {
         session2.end(xid, XAResource.TMSUCCESS);

         Assert.fail("Should throw exception");
      }
      catch (XAException e)
      {
         Assert.assertEquals(XAException.XA_RBOTHER, e.errorCode);
      }
   }

   @Test
   public void testXAMessagesConsumedSoRollbackOnEnd2() throws Exception
   {
      createSessionFactory();

      ClientSession session1 = createSessionAndQueue();

      ClientProducer producer = session1.createProducer(FailoverTestBase.ADDRESS);

      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         // some are durable, some are not!
         producer.send(createMessage(session1, i, true));
      }

      session1.commit();

      ClientSession session2 = createSession(sf, true, false, false);

      ClientConsumer consumer = session2.createConsumer(FailoverTestBase.ADDRESS);

      session2.start();

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session2.start(xid, XAResource.TMNOFLAGS);

      crash(session2);

      receiveMessages(consumer);

      try
      {
         session2.end(xid, XAResource.TMSUCCESS);

         Assert.fail("Should throw exception");
      }
      catch (XAException e)
      {
      }

      // Since the end was not accepted, the messages should be redelivered
      receiveMessages(consumer);
   }

   @Test
   public void testXAMessagesConsumedSoRollbackOnPrepare() throws Exception
   {
      createSessionFactory();

      ClientSession session1 = createSessionAndQueue();

      ClientProducer producer = session1.createProducer(FailoverTestBase.ADDRESS);

      sendMessagesSomeDurable(session1, producer);

      session1.commit();

      ClientSession session2 = createSession(sf, true, false, false);

      ClientConsumer consumer = session2.createConsumer(FailoverTestBase.ADDRESS);

      session2.start();

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session2.start(xid, XAResource.TMNOFLAGS);

      receiveMessages(consumer);

      session2.end(xid, XAResource.TMSUCCESS);

      crash(session2);

      try
      {
         session2.prepare(xid);

         Assert.fail("Should throw exception");
      }
      catch (XAException e)
      {
         Assert.assertEquals(XAException.XA_RBOTHER, e.errorCode);
      }
   }

   // 1PC optimisation
   @Test
   public void testXAMessagesConsumedSoRollbackOnCommit() throws Exception
   {
      createSessionFactory();
      ClientSession session1 = createSessionAndQueue();

      ClientProducer producer = session1.createProducer(FailoverTestBase.ADDRESS);

      sendMessagesSomeDurable(session1, producer);

      session1.commit();

      ClientSession session2 = createSession(sf, true, false, false);

      ClientConsumer consumer = session2.createConsumer(FailoverTestBase.ADDRESS);

      session2.start();

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session2.start(xid, XAResource.TMNOFLAGS);

      receiveMessages(consumer);

      session2.end(xid, XAResource.TMSUCCESS);

      // session2.prepare(xid);

      crash(session2);

      try
      {
         session2.commit(xid, true);

         Assert.fail("Should throw exception");
      }
      catch (XAException e)
      {
         // it should be rolled back
         Assert.assertEquals(XAException.XAER_NOTA, e.errorCode);
      }

      session1.close();

      session2.close();
   }

   @Test
   public void testCreateNewFactoryAfterFailover() throws Exception
   {
      this.disableCheckThread();
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setFailoverOnInitialConnection(true);
      sf = createSessionFactoryAndWaitForTopology(locator, 2);

      ClientSession session = sendAndConsume(sf, true);

      crash(true, session);

      session.close();


      long timeout;
      timeout = System.currentTimeMillis() + 5000;
      while (timeout > System.currentTimeMillis())
      {
         try
         {
            createClientSessionFactory();
            break;
         }
         catch (Exception e)
         {
            // retrying
            Thread.sleep(100);
         }
      }

      session = sendAndConsume(sf, true);
   }

   @Test
   public void testFailoverMultipleSessionsWithConsumers() throws Exception
   {
      createSessionFactory();

      final int numSessions = 5;

      final int numConsumersPerSession = 5;

      Map<ClientSession, List<ClientConsumer>> sessionConsumerMap = new HashMap<ClientSession, List<ClientConsumer>>();

      for (int i = 0; i < numSessions; i++)
      {
         ClientSession session = createSession(sf, true, true);

         List<ClientConsumer> consumers = new ArrayList<ClientConsumer>();

         for (int j = 0; j < numConsumersPerSession; j++)
         {
            SimpleString queueName = new SimpleString("queue" + i + "-" + j);

            session.createQueue(FailoverTestBase.ADDRESS, queueName, null, true);

            ClientConsumer consumer = session.createConsumer(queueName);

            consumers.add(consumer);
         }

         sessionConsumerMap.put(session, consumers);
      }

      ClientSession sendSession = createSession(sf, true, true);

      ClientProducer producer = sendSession.createProducer(FailoverTestBase.ADDRESS);


      sendMessages(sendSession, producer, NUM_MESSAGES);

      Set<ClientSession> sessionSet = sessionConsumerMap.keySet();
      ClientSession[] sessions = new ClientSession[sessionSet.size()];
      sessionSet.toArray(sessions);
      crash(sessions);

      for (ClientSession session : sessionConsumerMap.keySet())
      {
         session.start();
      }

      for (List<ClientConsumer> consumerList : sessionConsumerMap.values())
      {
         for (ClientConsumer consumer : consumerList)
         {
            receiveMessages(consumer);
         }
      }
   }

   /*
    * Browser will get reset to beginning after failover
    */
   @Test
   public void testFailWithBrowser() throws Exception
   {
      createSessionFactory();
      ClientSession session = createSession(sf, true, true);

      session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      sendMessagesSomeDurable(session, producer);

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS, true);

      session.start();

      receiveMessages(consumer, 0, NUM_MESSAGES, false);

      crash(session);

      receiveDurableMessages(consumer);
   }

   protected void sendMessagesSomeDurable(ClientSession session, ClientProducer producer) throws Exception
   {
      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         // some are durable, some are not!
         producer.send(createMessage(session, i, isDurable(i)));
      }
   }

   @Test
   public void testFailThenReceiveMoreMessagesAfterFailover() throws Exception
   {
      createSessionFactory();

      ClientSession session = createSession(sf, true, true);

      session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      sendMessagesSomeDurable(session, producer);

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      // Receive MSGs but don't ack!
      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         ClientMessage message = consumer.receive(1000);

         Assert.assertNotNull(message);

         assertMessageBody(i, message);

         Assert.assertEquals(i, message.getIntProperty("counter").intValue());
      }

      crash(session);

      // Should get the same ones after failover since we didn't ack

      receiveDurableMessages(consumer);
   }

   protected void receiveDurableMessages(ClientConsumer consumer) throws HornetQException
   {
      // During failover non-persistent messages may disappear but in certain cases they may survive.
      // For that reason the test is validating all the messages but being permissive with non-persistent messages
      // The test will just ack any non-persistent message, however when arriving it must be in order
      ClientMessage repeatMessage=null;
      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         ClientMessage message;

         if (repeatMessage != null)
         {
            message = repeatMessage;
            repeatMessage = null;
         }
         else
         {
            message = consumer.receive(1000);
         }

         if (message != null)
         {
            int msgInternalCounter = message.getIntProperty("counter").intValue();

            if (msgInternalCounter == i + 1)
            {
               // The test can only jump to the next message if the current iteration is meant for non-durable
               assertFalse("a message on counter=" + i + " was expected", isDurable(i));
               // message belongs to the next iteration.. lets just ignore it
               repeatMessage = message;
               continue;
            }
         }

         if (isDurable(i))
         {
            Assert.assertNotNull(message);
         }

         if (message != null)
         {
            assertMessageBody(i, message);
            Assert.assertEquals(i, message.getIntProperty("counter").intValue());
            message.acknowledge();
         }
      }
   }

   private boolean isDurable(int i)
   {
      return i % 2 == 0;
   }

   @Test
   public void testFailThenReceiveMoreMessagesAfterFailover2() throws Exception
   {
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setBlockOnAcknowledge(true);
      locator.setReconnectAttempts(-1);
      sf = createSessionFactoryAndWaitForTopology(locator, 2);

      ClientSession session = createSession(sf, true, true, 0);

      session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      sendMessagesSomeDurable(session, producer);

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();
      receiveMessages(consumer);

      crash(session);

      // Send some more

      for (int i = NUM_MESSAGES; i < NUM_MESSAGES * 2; i++)
      {
         producer.send(createMessage(session, i, isDurable(i)));
      }
      receiveMessages(consumer, NUM_MESSAGES, NUM_MESSAGES * 2, true);
   }

   private void receiveMessages(ClientConsumer consumer) throws HornetQException
   {
      receiveMessages(consumer, 0, NUM_MESSAGES, true);
   }

   @Test
   public void testSimpleSendAfterFailoverDurableTemporary() throws Exception
   {
      doSimpleSendAfterFailover(true, true);
   }

   @Test
   public void testSimpleSendAfterFailoverNonDurableTemporary() throws Exception
   {
      doSimpleSendAfterFailover(false, true);
   }

   @Test
   public void testSimpleSendAfterFailoverDurableNonTemporary() throws Exception
   {
      doSimpleSendAfterFailover(true, false);
   }

   @Test
   public void testSimpleSendAfterFailoverNonDurableNonTemporary() throws Exception
   {
      doSimpleSendAfterFailover(false, false);
   }

   private void doSimpleSendAfterFailover(final boolean durable, final boolean temporary) throws Exception
   {
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setBlockOnAcknowledge(true);
      locator.setReconnectAttempts(-1);
      sf = createSessionFactoryAndWaitForTopology(locator, 2);

      ClientSession session = createSession(sf, true, true, 0);

      if (temporary)
      {
         session.createTemporaryQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null);
      }
      else
      {
         session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, durable);
      }

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      crash(session);

      sendMessagesSomeDurable(session, producer);

      receiveMessages(consumer);
   }

   public void _testForceBlockingReturn() throws Exception
   {
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setBlockOnAcknowledge(true);
      locator.setReconnectAttempts(-1);
      createClientSessionFactory();

      // Add an interceptor to delay the send method so we can get time to cause failover before it returns

      // liveServer.getRemotingService().addIncomingInterceptor(new DelayInterceptor());

      final ClientSession session = createSession(sf, true, true, 0);

      session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);

      final ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      class Sender extends Thread
      {
         @Override
         public void run()
         {
            ClientMessage message = session.createMessage(true);

            message.getBodyBuffer().writeString("message");

            try
            {
               producer.send(message);
            }
            catch (HornetQException e1)
            {
               this.e = e1;
            }
         }

         volatile HornetQException e;
      }

      Sender sender = new Sender();

      sender.start();

      crash(session);

      sender.join();

      Assert.assertNotNull(sender.e);

      Assert.assertEquals(sender.e.getType(), HornetQExceptionType.UNBLOCKED);

      session.close();
   }

   @Test
   public void testCommitOccurredUnblockedAndResendNoDuplicates() throws Exception
   {
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setReconnectAttempts(-1);

      locator.setBlockOnAcknowledge(true);

      sf = createSessionFactoryAndWaitForTopology(locator, 2);

      final ClientSession session = createSession(sf, false, false);

      session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      String txID = "my-tx-id";

      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         ClientMessage message = session.createMessage(true);

         if (i == 0)
         {
            // Only need to add it on one message per tx
            message.putStringProperty(Message.HDR_DUPLICATE_DETECTION_ID, new SimpleString(txID));
         }

         setBody(i, message);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      class Committer extends Thread
      {
         DelayInterceptor2 interceptor = new DelayInterceptor2();

         @Override
         public void run()
         {
            try
            {
               sf.getServerLocator().addIncomingInterceptor(interceptor);

               session.commit();
            }
            catch(HornetQTransactionRolledBackException trbe)
            {
               // Ok - now we retry the commit after removing the interceptor

               sf.getServerLocator().removeIncomingInterceptor(interceptor);

               try
               {
                  session.commit();

                  failed = false;
               }
               catch (HornetQException e2)
               {
                  throw new RuntimeException(e2);
               }
            }
            catch(HornetQTransactionOutcomeUnknownException toue)
            {
               // Ok - now we retry the commit after removing the interceptor

               sf.getServerLocator().removeIncomingInterceptor(interceptor);

               try
               {
                  session.commit();

                  failed = false;
               }
               catch (HornetQException e2)
               {
                  throw new RuntimeException(e2);
               }
            }
            catch (HornetQException e)
            {
               //ignore
            }
         }

         volatile boolean failed = true;
      }

      Committer committer = new Committer();

      // Commit will occur, but response will never get back, connection is failed, and commit
      // should be unblocked with transaction rolled back

      committer.start();

      // Wait for the commit to occur and the response to be discarded
      assertTrue(committer.interceptor.await());

      crash(session);

      committer.join();

      Assert.assertFalse("second attempt succeed?", committer.failed);

      session.close();

      ClientSession session2 = createSession(sf, false, false);

      producer = session2.createProducer(FailoverTestBase.ADDRESS);

      // We now try and resend the messages since we get a transaction rolled back exception
      // but the commit actually succeeded, duplicate detection should kick in and prevent dups

      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         ClientMessage message = session2.createMessage(true);

         if (i == 0)
         {
            // Only need to add it on one message per tx
            message.putStringProperty(Message.HDR_DUPLICATE_DETECTION_ID, new SimpleString(txID));
         }

         setBody(i, message);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      try
      {
         session2.commit();
         fail("expecting DUPLICATE_ID_REJECTED exception");
      }
      catch(HornetQDuplicateIdException dide)
      {
         //ok
      }
      catch (HornetQException e)
      {
         fail("Invalid Exception type:" + e.getType());
      }

      ClientConsumer consumer = session2.createConsumer(FailoverTestBase.ADDRESS);

      session2.start();

      receiveMessages(consumer);

      ClientMessage message = consumer.receiveImmediate();

      Assert.assertNull(message);
   }

   @Test
   public void testCommitDidNotOccurUnblockedAndResend() throws Exception
   {
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setBlockOnAcknowledge(true);
      locator.setReconnectAttempts(-1);
      sf = createSessionFactoryAndWaitForTopology(locator, 2);

      final ClientSession session = createSession(sf, false, false);

      session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);



      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);
      sendMessages(session, producer,NUM_MESSAGES);

      class Committer extends Thread
      {
         @Override
         public void run()
         {
            Interceptor interceptor = new DelayInterceptor3();

            try
            {
               liveServer.addInterceptor(interceptor);

               session.commit();
            }
            catch(HornetQTransactionRolledBackException trbe)
            {
               // Ok - now we retry the commit after removing the interceptor

               liveServer.removeInterceptor(interceptor);

               try
               {
                  session.commit();

                  failed = false;
               }
               catch (HornetQException e2)
               {
               }
            }
            catch(HornetQTransactionOutcomeUnknownException toue)
            {
               // Ok - now we retry the commit after removing the interceptor

               liveServer.removeInterceptor(interceptor);

               try
               {
                  session.commit();

                  failed = false;
               }
               catch (HornetQException e2)
               {
               }
            }
            catch (HornetQException e)
            {
               //ignore
            }
         }

         volatile boolean failed = true;
      }

      Committer committer = new Committer();

      committer.start();

      crash(session);

      committer.join();

      Assert.assertFalse("commiter failed should be false", committer.failed);

      session.close();

      ClientSession session2 = createSession(sf, false, false);

      producer = session2.createProducer(FailoverTestBase.ADDRESS);

      // We now try and resend the messages since we get a transaction rolled back exception
      sendMessages(session2, producer,NUM_MESSAGES);

      session2.commit();

      ClientConsumer consumer = session2.createConsumer(FailoverTestBase.ADDRESS);

      session2.start();

      receiveMessages(consumer);

      ClientMessage message = consumer.receiveImmediate();

      Assert.assertNull("expecting null message", message);
   }

   @Test
   public void testBackupServerNotRemoved() throws Exception
   {
      // HORNETQ-720 Disabling test for replicating backups.
      if (!backupServer.getServer().getConfiguration().isSharedStore())
      {
         waitForComponent(backupServer, 1);
         return;
      }
      locator.setFailoverOnInitialConnection(true);
      createSessionFactory();

      ClientSession session = sendAndConsume(sf, true);
      CountDownSessionFailureListener listener = new CountDownSessionFailureListener(session);

      session.addFailureListener(listener);

      backupServer.stop();

      liveServer.crash();

      // To reload security or other settings that are read during startup
      beforeRestart(backupServer);

      backupServer.start();

      assertTrue("session failure listener", listener.getLatch().await(5, TimeUnit.SECONDS));

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      ClientMessage message = session.createMessage(true);

      setBody(0, message);

      producer.send(message);
   }

   @Test
   public void testLiveAndBackupLiveComesBack() throws Exception
   {
      locator.setFailoverOnInitialConnection(true);
      createSessionFactory();
      final CountDownLatch latch = new CountDownLatch(1);

      ClientSession session = sendAndConsume(sf, true);

      session.addFailureListener(new CountDownSessionFailureListener(latch, session));

      backupServer.stop();

      liveServer.crash();

      beforeRestart(liveServer);

      // To reload security or other settings that are read during startup
      beforeRestart(liveServer);

      liveServer.start();

      assertTrue(latch.await(5, TimeUnit.SECONDS));

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      ClientMessage message = session.createMessage(true);

      setBody(0, message);

      producer.send(message);
   }

   @Test
   public void testLiveAndBackupLiveComesBackNewFactory() throws Exception
   {
      locator.setFailoverOnInitialConnection(true);
      createSessionFactory();

      final CountDownLatch latch = new CountDownLatch(1);

      ClientSession session = sendAndConsume(sf, true);

      session.addFailureListener(new CountDownSessionFailureListener(latch, session));

      backupServer.stop();

      liveServer.crash();

      // To reload security or other settings that are read during startup
      beforeRestart(liveServer);

      liveServer.start();

      assertTrue(latch.await(5, TimeUnit.SECONDS));

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      ClientMessage message = session.createMessage(true);

      setBody(0, message);

      producer.send(message);

      session.close();

      sf.close();

      createClientSessionFactory();

      session = createSession(sf);

      ClientConsumer cc = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      ClientMessage cm = cc.receive(5000);

      assertNotNull(cm);

      Assert.assertEquals("message0", cm.getBodyBuffer().readString());
   }

   @Test
   public void testLiveAndBackupBackupComesBackNewFactory() throws Exception
   {
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setFailoverOnInitialConnection(true);
      locator.setReconnectAttempts(-1);
      sf = createSessionFactoryAndWaitForTopology(locator, 2);

      ClientSession session = sendAndConsume(sf, true);

      CountDownSessionFailureListener listener = new CountDownSessionFailureListener(session);

      session.addFailureListener(listener);

      backupServer.stop();

      liveServer.crash();

      // To reload security or other settings that are read during startup
      beforeRestart(backupServer);

      if (!backupServer.getServer().getConfiguration().isSharedStore())
      {
         // XXX
         // this test would not make sense in the remote replication use case, without the following
         backupServer.getServer().getConfiguration().setBackup(false);
      }

      backupServer.start();

      assertTrue("session failure listener", listener.getLatch().await(5, TimeUnit.SECONDS));

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      ClientMessage message = session.createMessage(true);

      setBody(0, message);

      producer.send(message);

      session.close();

      sf.close();

      createClientSessionFactory();

      session = createSession(sf);

      ClientConsumer cc = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      ClientMessage cm = cc.receive(5000);

      assertNotNull(cm);

      Assert.assertEquals("message0", cm.getBodyBuffer().readString());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected TransportConfiguration getAcceptorTransportConfiguration(final boolean live)
   {
      return TransportConfigurationUtils.getInVMAcceptor(live);
   }

   @Override
   protected TransportConfiguration getConnectorTransportConfiguration(final boolean live)
   {
      return TransportConfigurationUtils.getInVMConnector(live);
   }

   protected void beforeRestart(TestableServer liveServer1)
   {
      // no-op
   }

   private ClientSession sendAndConsume(final ClientSessionFactory sf1, final boolean createQueue) throws Exception
   {
      ClientSession session = createSession(sf1, false, true, true);

      if (createQueue)
      {
         session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, false);
      }

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         ClientMessage message = session.createMessage(HornetQTextMessage.TYPE,
            false,
            0,
            System.currentTimeMillis(),
            (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBodyBuffer().writeString("aardvarks");
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         ClientMessage message2 = consumer.receive();

         Assert.assertEquals("aardvarks", message2.getBodyBuffer().readString());

         Assert.assertEquals(i, message2.getObjectProperty(new SimpleString("count")));

         message2.acknowledge();
      }

      ClientMessage message3 = consumer.receiveImmediate();

      Assert.assertNull(message3);

      return session;
   }

   // Inner classes -------------------------------------------------

}
