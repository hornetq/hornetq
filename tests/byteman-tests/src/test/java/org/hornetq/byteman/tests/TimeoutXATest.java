/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hornetq.byteman.tests;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.XAConnection;
import javax.jms.XASession;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.concurrent.CountDownLatch;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.tests.util.ServiceTestBase;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
public class TimeoutXATest extends ServiceTestBase
{


   protected HornetQServer server = null;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      server = createServer(createDefaultConfig(true));
      server.getConfiguration().setTransactionTimeout(1000);
      server.getConfiguration().setTransactionTimeoutScanPeriod(1100);
      server.getConfiguration().setJournalSyncNonTransactional(false);
      server.start();
      server.createQueue(SimpleString.toSimpleString("jms.queue.Queue1"), SimpleString.toSimpleString("jms.queue.Queue1"), null, true, false);

      removingTXEntered0 = new CountDownLatch(1);
      removingTXAwait0 = new CountDownLatch(1);
      removingTXEntered1 = new CountDownLatch(1);
      removingTXAwait1 = new CountDownLatch(1);
      entered = 0;

      enteredRollback = 0;
      enteredRollbackLatch = new CountDownLatch(1);
      waitingRollbackLatch = new CountDownLatch(1);
   }

   static int entered;
   static CountDownLatch removingTXEntered0;
   static CountDownLatch removingTXAwait0;
   static CountDownLatch removingTXEntered1;
   static CountDownLatch removingTXAwait1;

   static int enteredRollback;
   static CountDownLatch enteredRollbackLatch;
   static CountDownLatch waitingRollbackLatch;


   @Test
   @BMRules(
      rules = {@BMRule(
         name = "removing TX",
         targetClass = "org.hornetq.core.transaction.impl.ResourceManagerImpl",
         targetMethod = "removeTransaction",
         targetLocation = "ENTRY",
         action = "org.hornetq.byteman.tests.TimeoutXATest.removingTX();"),
         @BMRule(
            name = "afterRollback TX",
            targetClass = "org.hornetq.core.transaction.impl.TransactionImpl",
            targetMethod = "afterRollback",
            targetLocation = "ENTRY",
            action = "org.hornetq.byteman.tests.TimeoutXATest.afterRollback();")})
   public void testTimeoutOnTX2() throws Exception
   {
      HornetQConnectionFactory connectionFactory = HornetQJMSClient.createConnectionFactoryWithHA(JMSFactoryType.CF, new TransportConfiguration(NETTY_CONNECTOR_FACTORY));
      XAConnection connection = (XAConnection) connectionFactory.createXAConnection();

      Connection connction2 = connectionFactory.createConnection();
      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
      Queue queue = session.createQueue("Queue1");

      MessageProducer producer = session.createProducer(queue);
      for (int i = 0; i < 10; i++)
      {
         producer.send(session.createTextMessage("hello " + 1));
      }

      session.commit();

      final XASession xaSession = connection.createXASession();
      final Xid xid = newXID();

      xaSession.getXAResource().start(xid, XAResource.TMNOFLAGS);
      MessageConsumer consumer = xaSession.createConsumer(queue);
      connection.start();
      for (int i = 0; i < 10; i++)
      {
         Assert.assertNotNull(consumer.receive(5000));
      }
      xaSession.getXAResource().end(xid, XAResource.TMSUCCESS);

      final CountDownLatch latchStore = new CountDownLatch(1000);

      Thread storingThread = new Thread()
      {
         public void run()
         {
            try
            {
               for (int i = 0; i < 100000; i++)
               {
                  latchStore.countDown();
                  server.getStorageManager().storeDuplicateID(SimpleString.toSimpleString("crebis"), new byte[]{1}, server.getStorageManager().generateUniqueID());
               }
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }
      };

      removingTXEntered0.await();

      Thread t = new Thread()
      {
         public void run()
         {
            try
            {
               xaSession.getXAResource().rollback(xid);
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }

         }
      };

      t.start();


      removingTXEntered1.await();

      storingThread.start();
      latchStore.await();

      removingTXAwait1.countDown();

      Thread.sleep(1000);
      removingTXAwait0.countDown();

      enteredRollbackLatch.await();

      waitingRollbackLatch.countDown();

      t.join();

      consumer.close();
//
//      connction2.start();
//
      consumer = session.createConsumer(queue);
      for (int i = 0; i < 10; i++)
      {
         Assert.assertNotNull(consumer.receive(5000));
      }
      Assert.assertNull(consumer.receiveNoWait());
//      session.commit();
//      session.close();
      connection.close();
      connction2.close();

   }

   public static void afterRollback()
   {
      new Exception().printStackTrace();
      if (enteredRollback++ == 0)
      {
         enteredRollbackLatch.countDown();
         try
         {
            waitingRollbackLatch.await();
         }
         catch (Throwable e)
         {

         }
      }

   }

   public static void removingTX()
   {
      new Exception().printStackTrace();
      int xent = entered++;

      if (xent == 0)
      {
         removingTXEntered0.countDown();
         try
         {
            removingTXAwait0.await();
         }
         catch (Throwable ignored)
         {
            ignored.printStackTrace();
         }
      }
      else if (xent == 1)
      {
         removingTXEntered1.countDown();
         try
         {
            removingTXAwait1.await();
         }
         catch (Throwable ignored)
         {
            ignored.printStackTrace();
         }
      }

   }
}
