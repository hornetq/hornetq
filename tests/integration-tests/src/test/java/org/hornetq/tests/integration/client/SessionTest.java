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

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSession.BindingQuery;
import org.hornetq.api.core.client.ClientSession.QueueQuery;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.core.client.SessionFailureListener;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * This test covers the API for ClientSession altho XA tests are tested seperately.
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class SessionTest extends ServiceTestBase
{
   private final String queueName = "ClientSessionTestQ";

   private ServerLocator locator;
   private HornetQServer server;
   private ClientSessionFactory cf;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      locator = createInVMNonHALocator();
      server = createServer(false);
      server.start();
   }

   public void testFailureListener() throws Exception
   {

      cf = createSessionFactory(locator);
         ClientSession clientSession = cf.createSession(false, true, true);
         final CountDownLatch latch = new CountDownLatch(1);
         clientSession.addFailureListener(new SessionFailureListener()
         {
            public void connectionFailed(final HornetQException me, boolean failedOver)
            {
               latch.countDown();
            }

            public void beforeReconnect(final HornetQException me)
            {
            }
         });

         // Make sure failure listener is called if server is stopped without session being closed first
         server.stop();
         Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));

         // not really part of the test,
         // we still clean up resources left in the VM
         clientSession.close();
         }

   public void testFailureListenerRemoved() throws Exception
   {
      cf = createSessionFactory(locator);
         ClientSession clientSession = cf.createSession(false, true, true);
         class MyFailureListener implements SessionFailureListener
         {
            boolean called = false;

            public void connectionFailed(final HornetQException me, boolean failedOver)
            {
               called = true;
            }

            public void beforeReconnect(final HornetQException me)
            {
            }
         }

         MyFailureListener listener = new MyFailureListener();
         clientSession.addFailureListener(listener);

         Assert.assertTrue(clientSession.removeFailureListener(listener));
         clientSession.close();
         server.stop();
         Assert.assertFalse(listener.called);
   }

   // Closing a session if the underlying remoting connection is deaad should cleanly
   // release all resources
   public void testCloseSessionOnDestroyedConnection() throws Exception
   {
         // Make sure we have a short connection TTL so sessions will be quickly closed on the server
      server.stop();
      long ttl = 500;
         server.getConfiguration().setConnectionTTLOverride(ttl);
         server.start();
      cf = createSessionFactory(locator);
         ClientSessionInternal clientSession = (ClientSessionInternal)cf.createSession(false, true, true);
         clientSession.createQueue(queueName, queueName, false);
         ClientProducer producer = clientSession.createProducer();
         ClientConsumer consumer = clientSession.createConsumer(queueName);

         Assert.assertEquals(1, server.getRemotingService().getConnections().size());

         RemotingConnection rc = clientSession.getConnection();

         rc.fail(new HornetQException(HornetQException.INTERNAL_ERROR));

         clientSession.close();

         long start = System.currentTimeMillis();

         while (true)
         {
            int cons = server.getRemotingService().getConnections().size();

            if (cons == 0)
            {
               break;
            }

            long now = System.currentTimeMillis();

            if (now - start > 10000)
            {
               throw new Exception("Timed out waiting for connections to close");
            }

            Thread.sleep(50);
         }
   }

   public void testBindingQuery() throws Exception
   {
      cf = createSessionFactory(locator);
         ClientSession clientSession = cf.createSession(false, true, true);
         clientSession.createQueue("a1", "q1", false);
         clientSession.createQueue("a1", "q2", false);
         clientSession.createQueue("a2", "q3", false);
         clientSession.createQueue("a2", "q4", false);
         clientSession.createQueue("a2", "q5", false);
         BindingQuery resp = clientSession.bindingQuery(new SimpleString("a"));
         List<SimpleString> queues = resp.getQueueNames();
         Assert.assertTrue(queues.isEmpty());
         resp = clientSession.bindingQuery(new SimpleString("a1"));
         queues = resp.getQueueNames();
         Assert.assertEquals(queues.size(), 2);
         Assert.assertTrue(queues.contains(new SimpleString("q1")));
         Assert.assertTrue(queues.contains(new SimpleString("q2")));
         resp = clientSession.bindingQuery(new SimpleString("a2"));
         queues = resp.getQueueNames();
         Assert.assertEquals(queues.size(), 3);
         Assert.assertTrue(queues.contains(new SimpleString("q3")));
         Assert.assertTrue(queues.contains(new SimpleString("q4")));
         Assert.assertTrue(queues.contains(new SimpleString("q5")));
         clientSession.close();
   }

   public void testQueueQuery() throws Exception
   {
      cf = createSessionFactory(locator);
         ClientSession clientSession = cf.createSession(false, true, true);
         clientSession.createQueue("a1", queueName, false);
         clientSession.createConsumer(queueName);
         clientSession.createConsumer(queueName);
         ClientProducer cp = clientSession.createProducer("a1");
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         QueueQuery resp = clientSession.queueQuery(new SimpleString(queueName));
         Assert.assertEquals(new SimpleString("a1"), resp.getAddress());
         Assert.assertEquals(2, resp.getConsumerCount());
         Assert.assertEquals(2, resp.getMessageCount());
         Assert.assertEquals(null, resp.getFilterString());
         clientSession.close();
   }

   public void testQueueQueryWithFilter() throws Exception
   {
      cf = createSessionFactory(locator);
         ClientSession clientSession = cf.createSession(false, true, true);
         clientSession.createQueue("a1", queueName, "foo=bar", false);
         clientSession.createConsumer(queueName);
         clientSession.createConsumer(queueName);
         QueueQuery resp = clientSession.queueQuery(new SimpleString(queueName));
         Assert.assertEquals(new SimpleString("a1"), resp.getAddress());
         Assert.assertEquals(2, resp.getConsumerCount());
         Assert.assertEquals(0, resp.getMessageCount());
         Assert.assertEquals(new SimpleString("foo=bar"), resp.getFilterString());
         clientSession.close();
   }

   public void testQueueQueryNoQ() throws Exception
   {
      cf = createSessionFactory(locator);
         ClientSession clientSession = cf.createSession(false, true, true);
         QueueQuery resp = clientSession.queueQuery(new SimpleString(queueName));
         Assert.assertFalse(resp.isExists());
         Assert.assertEquals(null, resp.getAddress());
         clientSession.close();
   }

   public void testClose() throws Exception
   {
      cf = createSessionFactory(locator);
         ClientSession clientSession = cf.createSession(false, true, true);
         clientSession.createQueue(queueName, queueName, false);
         ClientProducer p = clientSession.createProducer();
         ClientProducer p1 = clientSession.createProducer(queueName);
         ClientConsumer c = clientSession.createConsumer(queueName);
         ClientConsumer c1 = clientSession.createConsumer(queueName);
         clientSession.close();
         Assert.assertTrue(clientSession.isClosed());
         Assert.assertTrue(p.isClosed());
         Assert.assertTrue(p1.isClosed());
         Assert.assertTrue(c.isClosed());
         Assert.assertTrue(c1.isClosed());
   }

   public void testCreateMessageNonDurable() throws Exception
   {
      cf = createSessionFactory(locator);
         ClientSession clientSession = cf.createSession(false, true, true);
         ClientMessage clientMessage = clientSession.createMessage(false);
         Assert.assertFalse(clientMessage.isDurable());
         clientSession.close();
   }

   public void testCreateMessageDurable() throws Exception
   {
      cf = createSessionFactory(locator);
         ClientSession clientSession = cf.createSession(false, true, true);
         ClientMessage clientMessage = clientSession.createMessage(true);
         Assert.assertTrue(clientMessage.isDurable());
         clientSession.close();
         }

   public void testCreateMessageType() throws Exception
   {
      cf = createSessionFactory(locator);
         ClientSession clientSession = cf.createSession(false, true, true);
         ClientMessage clientMessage = clientSession.createMessage((byte)99, false);
         Assert.assertEquals((byte)99, clientMessage.getType());
         clientSession.close();
   }

   public void testCreateMessageOverrides() throws Exception
   {
      cf = createSessionFactory(locator);
         ClientSession clientSession = cf.createSession(false, true, true);
         ClientMessage clientMessage = clientSession.createMessage((byte)88, false, 100l, 300l, (byte)33);
         Assert.assertEquals((byte)88, clientMessage.getType());
         Assert.assertEquals(100l, clientMessage.getExpiration());
         Assert.assertEquals(300l, clientMessage.getTimestamp());
         Assert.assertEquals((byte)33, clientMessage.getPriority());
         clientSession.close();
   }

   public void testGetVersion() throws Exception
   {
      cf = createSessionFactory(locator);
         ClientSession clientSession = cf.createSession(false, true, true);
         Assert.assertEquals(server.getVersion().getIncrementingVersion(), clientSession.getVersion());
         clientSession.close();
   }

   public void testStart() throws Exception
   {
      cf = createSessionFactory(locator);
         ClientSession clientSession = cf.createSession(false, true, true);
         clientSession.createQueue(queueName, queueName, false);
         clientSession.start();
         clientSession.close();
   }

   public void testStop() throws Exception
   {
      cf = createSessionFactory(locator);
         ClientSession clientSession = cf.createSession(false, true, true);
         clientSession.createQueue(queueName, queueName, false);
         clientSession.start();
         clientSession.stop();
         clientSession.close();
   }

   public void testCommitWithSend() throws Exception
   {
      cf = createSessionFactory(locator);
         ClientSession clientSession = cf.createSession(false, false, true);
         clientSession.createQueue(queueName, queueName, false);
         ClientProducer cp = clientSession.createProducer(queueName);
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         Queue q = (Queue)server.getPostOffice().getBinding(new SimpleString(queueName)).getBindable();
         Assert.assertEquals(0, q.getMessageCount());
         clientSession.commit();
         Assert.assertEquals(10, q.getMessageCount());
         clientSession.close();
   }

   public void testRollbackWithSend() throws Exception
   {
      cf = createSessionFactory(locator);
         ClientSession clientSession = cf.createSession(false, false, true);
         clientSession.createQueue(queueName, queueName, false);
         ClientProducer cp = clientSession.createProducer(queueName);
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         Queue q = (Queue)server.getPostOffice().getBinding(new SimpleString(queueName)).getBindable();
         Assert.assertEquals(0, q.getMessageCount());
         clientSession.rollback();
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         clientSession.commit();
         Assert.assertEquals(2, q.getMessageCount());
         clientSession.close();
         }

   public void testCommitWithReceive() throws Exception
   {
      locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);
      cf = createSessionFactory(locator);
         ClientSession sendSession = cf.createSession(false, true, true);
         ClientProducer cp = sendSession.createProducer(queueName);
         ClientSession clientSession = cf.createSession(false, true, false);
         clientSession.createQueue(queueName, queueName, false);
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         Queue q = (Queue)server.getPostOffice().getBinding(new SimpleString(queueName)).getBindable();
         Assert.assertEquals(10, q.getMessageCount());
         ClientConsumer cc = clientSession.createConsumer(queueName);
         clientSession.start();
         ClientMessage m = cc.receive(5000);
         Assert.assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         Assert.assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         Assert.assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         Assert.assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         Assert.assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         Assert.assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         Assert.assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         Assert.assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         Assert.assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         Assert.assertNotNull(m);
         m.acknowledge();
         clientSession.commit();
         Assert.assertEquals(0, q.getMessageCount());
         clientSession.close();
         sendSession.close();
   }

   public void testRollbackWithReceive() throws Exception
   {
         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);
      cf = createSessionFactory(locator);
         ClientSession sendSession = cf.createSession(false, true, true);
         ClientProducer cp = sendSession.createProducer(queueName);
         ClientSession clientSession = cf.createSession(false, true, false);
         clientSession.createQueue(queueName, queueName, false);
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         cp.send(clientSession.createMessage(false));
         Queue q = (Queue)server.getPostOffice().getBinding(new SimpleString(queueName)).getBindable();
         Assert.assertEquals(10, q.getMessageCount());
         ClientConsumer cc = clientSession.createConsumer(queueName);
         clientSession.start();
         ClientMessage m = cc.receive(5000);
         Assert.assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         Assert.assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         Assert.assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         Assert.assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         Assert.assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         Assert.assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         Assert.assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         Assert.assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         Assert.assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         Assert.assertNotNull(m);
         m.acknowledge();
         clientSession.rollback();
         Assert.assertEquals(10, q.getMessageCount());
         clientSession.close();
         sendSession.close();
      }
}
