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

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionQueueQueryResponseMessage;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.SimpleString;

/**
 * This test covers the API for ClientSession altho XA tests are tested seperately.
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class SessionTest extends ServiceTestBase
{
   private String queueName = "ClientSessionTestQ";

   public void testFailureListener() throws Exception
   {
      HornetQServer server = createServer(false);
      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession clientSession = cf.createSession(false, true, true);
         final CountDownLatch latch = new CountDownLatch(1);
         clientSession.addFailureListener(new FailureListener()
         {
            public void connectionFailed(HornetQException me)
            {
               latch.countDown();
            }
         });
         
         //Make sure failure listener is called if server is stopped without session being closed first
         server.stop();
         assertTrue(latch.await(5, TimeUnit.SECONDS));

         // not really part of the test,
         // we still clean up resources left in the VM
         clientSession.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testFailureListenerRemoved() throws Exception
   {
      HornetQServer server = createServer(false);
      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession clientSession = cf.createSession(false, true, true);
         class MyFailureListener implements FailureListener
         {
            boolean called = false;

            public void connectionFailed(HornetQException me)
            {
               called = true;
            }
         }

         MyFailureListener listener = new MyFailureListener();
         clientSession.addFailureListener(listener);

         assertTrue(clientSession.removeFailureListener(listener));
         clientSession.close();
         server.stop();
         assertFalse(listener.called);
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }
   
   //Closing a session if the underlying remoting connection is deaad should cleanly
   //release all resources
   public void testCloseSessionOnDestroyedConnection() throws Exception
   {
      HornetQServer server = createServer(false);
      try
      {
         //Make sure we have a short connection TTL so sessions will be quickly closed on the server
         long ttl = 500;
         server.getConfiguration().setConnectionTTLOverride(ttl);
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSessionInternal clientSession = (ClientSessionInternal)cf.createSession(false, true, true);
         clientSession.createQueue(queueName, queueName, false);
         ClientProducer producer = clientSession.createProducer();
         ClientConsumer consumer = clientSession.createConsumer(queueName);   
         
         assertEquals(1, server.getRemotingService().getConnections().size());
         
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
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testBindingQuery() throws Exception
   {
      HornetQServer server = createServer(false);
      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession clientSession = cf.createSession(false, true, true);
         clientSession.createQueue("a1", "q1", false);
         clientSession.createQueue("a1", "q2", false);
         clientSession.createQueue("a2", "q3", false);
         clientSession.createQueue("a2", "q4", false);
         clientSession.createQueue("a2", "q5", false);
         SessionBindingQueryResponseMessage resp = clientSession.bindingQuery(new SimpleString("a"));
         List<SimpleString> queues = resp.getQueueNames();
         assertTrue(queues.isEmpty());
         resp = clientSession.bindingQuery(new SimpleString("a1"));
         queues = resp.getQueueNames();
         assertEquals(queues.size(), 2);
         assertTrue(queues.contains(new SimpleString("q1")));
         assertTrue(queues.contains(new SimpleString("q2")));
         resp = clientSession.bindingQuery(new SimpleString("a2"));
         queues = resp.getQueueNames();
         assertEquals(queues.size(), 3);
         assertTrue(queues.contains(new SimpleString("q3")));
         assertTrue(queues.contains(new SimpleString("q4")));
         assertTrue(queues.contains(new SimpleString("q5")));
         clientSession.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testQueueQuery() throws Exception
   {
      HornetQServer server = createServer(false);
      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession clientSession = cf.createSession(false, true, true);
         clientSession.createQueue("a1", queueName, false);
         clientSession.createConsumer(queueName);
         clientSession.createConsumer(queueName);
         ClientProducer cp = clientSession.createProducer("a1");
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         SessionQueueQueryResponseMessage resp = clientSession.queueQuery(new SimpleString(queueName));
         assertEquals(new SimpleString("a1"), resp.getAddress());
         assertEquals(2, resp.getConsumerCount());
         assertEquals(2, resp.getMessageCount());
         assertEquals(null, resp.getFilterString());
         clientSession.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testQueueQueryWithFilter() throws Exception
   {
      HornetQServer server = createServer(false);
      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession clientSession = cf.createSession(false, true, true);
         clientSession.createQueue("a1", queueName, "foo=bar", false);
         clientSession.createConsumer(queueName);
         clientSession.createConsumer(queueName);
         SessionQueueQueryResponseMessage resp = clientSession.queueQuery(new SimpleString(queueName));
         assertEquals(new SimpleString("a1"), resp.getAddress());
         assertEquals(2, resp.getConsumerCount());
         assertEquals(0, resp.getMessageCount());
         assertEquals(new SimpleString("foo=bar"), resp.getFilterString());
         clientSession.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

    public void testQueueQueryNoQ() throws Exception
   {
      HornetQServer server = createServer(false);
      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession clientSession = cf.createSession(false, true, true);
         SessionQueueQueryResponseMessage resp = clientSession.queueQuery(new SimpleString(queueName));
         assertFalse(resp.isExists());
         assertEquals(null, resp.getAddress());
         clientSession.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testClose() throws Exception
   {
      HornetQServer server = createServer(false);
      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession clientSession = cf.createSession(false, true, true);
         clientSession.createQueue(queueName, queueName, false);
         ClientProducer p = clientSession.createProducer();
         ClientProducer p1 = clientSession.createProducer(queueName);
         ClientConsumer c = clientSession.createConsumer(queueName);
         ClientConsumer c1 = clientSession.createConsumer(queueName);
         clientSession.close();
         assertTrue(clientSession.isClosed());
         assertTrue(p.isClosed());
         assertTrue(p1.isClosed());
         assertTrue(c.isClosed());
         assertTrue(c1.isClosed());
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testCreateClientMessageNonDurable() throws Exception
   {
      HornetQServer server = createServer(false);
      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession clientSession = cf.createSession(false, true, true);
         ClientMessage clientMessage = clientSession.createClientMessage(false);
         assertFalse(clientMessage.isDurable());
         clientSession.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testCreateClientMessageDurable() throws Exception
   {
      HornetQServer server = createServer(false);
      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession clientSession = cf.createSession(false, true, true);
         ClientMessage clientMessage = clientSession.createClientMessage(true);
         assertTrue(clientMessage.isDurable());
         clientSession.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testCreateClientMessageType() throws Exception
   {
      HornetQServer server = createServer(false);
      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession clientSession = cf.createSession(false, true, true);
         ClientMessage clientMessage = clientSession.createClientMessage((byte) 99, false);
         assertEquals((byte) 99, clientMessage.getType());
         clientSession.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testCreateClientMessageOverrides() throws Exception
   {
      HornetQServer server = createServer(false);
      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession clientSession = cf.createSession(false, true, true);
         ClientMessage clientMessage = clientSession.createClientMessage((byte) 88, false, 100l, 300l, (byte) 33);
         assertEquals((byte) 88, clientMessage.getType());
         assertEquals(100l, clientMessage.getExpiration());
         assertEquals(300l, clientMessage.getTimestamp());
         assertEquals((byte) 33, clientMessage.getPriority());
         clientSession.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testGetVersion() throws Exception
   {
      HornetQServer server = createServer(false);
      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession clientSession = cf.createSession(false, true, true);
         assertEquals(server.getVersion().getIncrementingVersion(), clientSession.getVersion());
         clientSession.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testStart() throws Exception
   {
      HornetQServer server = createServer(false);
      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession clientSession = cf.createSession(false, true, true);
         clientSession.createQueue(queueName, queueName, false);
         clientSession.start();
         clientSession.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testStop() throws Exception
   {
      HornetQServer server = createServer(false);
      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession clientSession = cf.createSession(false, true, true);
         clientSession.createQueue(queueName, queueName, false);
         clientSession.start();
         clientSession.stop();
         clientSession.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testCommitWithSend() throws Exception
   {
      HornetQServer server = createServer(false);
      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession clientSession = cf.createSession(false, false, true);
         clientSession.createQueue(queueName, queueName, false);
         ClientProducer cp = clientSession.createProducer(queueName);
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         Queue q = (Queue) server.getPostOffice().getBinding(new SimpleString(queueName)).getBindable();
         assertEquals(0, q.getMessageCount());
         clientSession.commit();
         assertEquals(10, q.getMessageCount());
         clientSession.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testRollbackWithSend() throws Exception
   {
      HornetQServer server = createServer(false);
      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession clientSession = cf.createSession(false, false, true);
         clientSession.createQueue(queueName, queueName, false);
         ClientProducer cp = clientSession.createProducer(queueName);
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         Queue q = (Queue) server.getPostOffice().getBinding(new SimpleString(queueName)).getBindable();
         assertEquals(0, q.getMessageCount());
         clientSession.rollback();
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         clientSession.commit();
         assertEquals(2, q.getMessageCount());
         clientSession.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testCommitWithReceive() throws Exception
   {
      HornetQServer server = createServer(false);
      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         cf.setBlockOnNonPersistentSend(true);
         cf.setBlockOnPersistentSend(true);
         ClientSession sendSession = cf.createSession(false, true, true);
         ClientProducer cp = sendSession.createProducer(queueName);
         ClientSession clientSession = cf.createSession(false, true, false);
         clientSession.createQueue(queueName, queueName, false);
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         Queue q = (Queue) server.getPostOffice().getBinding(new SimpleString(queueName)).getBindable();
         assertEquals(10, q.getMessageCount());
         ClientConsumer cc = clientSession.createConsumer(queueName);
         clientSession.start();
         ClientMessage m = cc.receive(5000);
         assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         assertNotNull(m);
         m.acknowledge();
         clientSession.commit();
         assertEquals(0, q.getMessageCount());
         clientSession.close();
         sendSession.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testRollbackWithReceive() throws Exception
   {
      HornetQServer server = createServer(false);
      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         cf.setBlockOnNonPersistentSend(true);
         cf.setBlockOnPersistentSend(true);
         ClientSession sendSession = cf.createSession(false, true, true);
         ClientProducer cp = sendSession.createProducer(queueName);
         ClientSession clientSession = cf.createSession(false, true, false);
         clientSession.createQueue(queueName, queueName, false);
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         cp.send(clientSession.createClientMessage(false));
         Queue q = (Queue) server.getPostOffice().getBinding(new SimpleString(queueName)).getBindable();
         assertEquals(10, q.getMessageCount());
         ClientConsumer cc = clientSession.createConsumer(queueName);
         clientSession.start();
         ClientMessage m = cc.receive(5000);
         assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         assertNotNull(m);
         m.acknowledge();
         m = cc.receive(5000);
         assertNotNull(m);
         m.acknowledge();
         clientSession.rollback();
         assertEquals(10, q.getMessageCount());
         clientSession.close();
         sendSession.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }
}
