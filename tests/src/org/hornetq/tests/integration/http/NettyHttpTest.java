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
package org.hornetq.tests.integration.http;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.hornetq.core.buffers.HornetQBuffer;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.remoting.impl.AbstractBufferHandler;
import org.hornetq.core.remoting.spi.BufferHandler;
import org.hornetq.core.remoting.spi.Connection;
import org.hornetq.core.remoting.spi.ConnectionLifeCycleListener;
import org.hornetq.integration.transports.netty.NettyAcceptor;
import org.hornetq.integration.transports.netty.NettyConnector;
import org.hornetq.integration.transports.netty.TransportConstants;
import org.hornetq.tests.util.UnitTestCase;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class NettyHttpTest extends UnitTestCase
{
   private NettyAcceptor acceptor;

   private NettyConnector connector;
   
   private ExecutorService threadPool;

   private ScheduledExecutorService scheduledThreadPool;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      
      checkFreePort(TransportConstants.DEFAULT_PORT);

      threadPool = Executors.newCachedThreadPool();

      scheduledThreadPool = Executors.newScheduledThreadPool(ConfigurationImpl.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE);
   }
   
   @Override
   protected void tearDown() throws Exception
   {
      if (connector != null)
      {
         connector.close();
         connector = null;
      }
      if (acceptor != null)
      {
         acceptor.stop();
         acceptor = null;
      }
      
      threadPool.shutdownNow();

      scheduledThreadPool.shutdownNow();
      
      threadPool = null;
      
      scheduledThreadPool = null;

      checkFreePort(TransportConstants.DEFAULT_PORT);

      super.tearDown();
   }
   

   public void testSendAndReceiveAtSameTime() throws Exception
   {

      int numPackets = 1000;
      CountDownLatch connCreatedLatch = new CountDownLatch(1);
      CountDownLatch acceptorLatch = new CountDownLatch(numPackets);
      CountDownLatch connectorLatch = new CountDownLatch(numPackets);
      HashMap<String, Object> conf = new HashMap<String, Object>();
      conf.put(TransportConstants.HTTP_ENABLED_PROP_NAME, true);
      conf.put(TransportConstants.HTTP_CLIENT_IDLE_SCAN_PERIOD, -1l);
      DummyConnectionLifeCycleListener acceptorListener = new DummyConnectionLifeCycleListener(connCreatedLatch);
      SimpleBufferHandler acceptorHandler = new SimpleBufferHandler(acceptorLatch);
      acceptor = new NettyAcceptor(conf, acceptorHandler, acceptorListener, threadPool, scheduledThreadPool);
      acceptor.start();

      SimpleBufferHandler2 connectorHandler = new SimpleBufferHandler2(connectorLatch);
      connector = new NettyConnector(conf, connectorHandler, new DummyConnectionLifeCycleListener(null), threadPool, scheduledThreadPool);
      connector.start();
      Connection conn = connector.createConnection();
      connCreatedLatch.await(5, TimeUnit.SECONDS);
      for (int i = 0; i < numPackets; i++)
      {
         HornetQBuffer buff = conn.createBuffer(8);
         buff.writeInt(4);
         buff.writeInt(i);
         conn.write(buff);
         HornetQBuffer buff2 = conn.createBuffer(8);
         buff2.writeInt(4);
         buff2.writeInt(i);
         acceptorListener.connection.write(buff2);
      }
      assertTrue(acceptorLatch.await(10, TimeUnit.SECONDS));
      assertTrue(connectorLatch.await(10, TimeUnit.SECONDS));
      conn.close();
      assertEquals(acceptorHandler.messagesReceieved, numPackets);
      assertEquals(connectorHandler.messagesReceieved, numPackets);
      int i = 0;
      for (Integer j : acceptorHandler.messages)
      {
         assertTrue(i == j);
         i++;
      }
      i = 0;
      for (Integer j : connectorHandler.messages)
      {
         assertTrue(i == j);
         i++;
      }
   }


   public void testSendThenReceive() throws Exception
   {
      int numPackets = 1000;
      CountDownLatch connCreatedLatch = new CountDownLatch(1);
      CountDownLatch acceptorLatch = new CountDownLatch(numPackets);
      CountDownLatch connectorLatch = new CountDownLatch(numPackets);
      HashMap<String, Object> conf = new HashMap<String, Object>();
      conf.put(TransportConstants.HTTP_ENABLED_PROP_NAME, true);
      conf.put(TransportConstants.HTTP_CLIENT_IDLE_SCAN_PERIOD, -1l);
      DummyConnectionLifeCycleListener acceptorListener = new DummyConnectionLifeCycleListener(connCreatedLatch);
      SimpleBufferHandler acceptorHandler = new SimpleBufferHandler(acceptorLatch);
      acceptor = new NettyAcceptor(conf, acceptorHandler, acceptorListener, threadPool, scheduledThreadPool);
      acceptor.start();

      SimpleBufferHandler2 connectorHandler = new SimpleBufferHandler2(connectorLatch);
      connector = new NettyConnector(conf, connectorHandler, new DummyConnectionLifeCycleListener(null), threadPool, scheduledThreadPool);
      connector.start();
      Connection conn = connector.createConnection();
      connCreatedLatch.await(5, TimeUnit.SECONDS);
      for (int i = 0; i < numPackets; i++)
      {
         HornetQBuffer buff = conn.createBuffer(8);
         buff.writeInt(4);
         buff.writeInt(i);
         conn.write(buff);
      }
      for (int i = 0; i < numPackets; i++)
      {
         HornetQBuffer buff = conn.createBuffer(8);
         buff.writeInt(4);
         buff.writeInt(i);
         acceptorListener.connection.write(buff);
      }
      assertTrue(acceptorLatch.await(10, TimeUnit.SECONDS));
      assertTrue(connectorLatch.await(10, TimeUnit.SECONDS));
      conn.close();
      assertEquals(acceptorHandler.messagesReceieved, numPackets);
      assertEquals(connectorHandler.messagesReceieved, numPackets);
      int i = 0;
      for (Integer j : acceptorHandler.messages)
      {
         assertTrue(i == j);
         i++;
      }
      i = 0;
      for (Integer j : connectorHandler.messages)
      {
         assertTrue(i == j);
         i++;
      }
   }

   public void testReceiveThenSend() throws Exception
   {

      int numPackets = 1000;
      CountDownLatch connCreatedLatch = new CountDownLatch(1);
      CountDownLatch acceptorLatch = new CountDownLatch(numPackets);
      CountDownLatch connectorLatch = new CountDownLatch(numPackets);
      HashMap<String, Object> conf = new HashMap<String, Object>();
      conf.put(TransportConstants.HTTP_ENABLED_PROP_NAME, true);
      conf.put(TransportConstants.HTTP_CLIENT_IDLE_SCAN_PERIOD, -1l);
      DummyConnectionLifeCycleListener acceptorListener = new DummyConnectionLifeCycleListener(connCreatedLatch);
      SimpleBufferHandler acceptorHandler = new SimpleBufferHandler(acceptorLatch);
      acceptor = new NettyAcceptor(conf, acceptorHandler, acceptorListener, threadPool, scheduledThreadPool);
      acceptor.start();

      SimpleBufferHandler connectorHandler = new SimpleBufferHandler(connectorLatch);
      connector = new NettyConnector(conf, connectorHandler, new DummyConnectionLifeCycleListener(null), threadPool, scheduledThreadPool);
      connector.start();
      Connection conn = connector.createConnection();
      connCreatedLatch.await(5, TimeUnit.SECONDS);
      for (int i = 0; i < numPackets; i++)
      {
         HornetQBuffer buff = conn.createBuffer(8);
         buff.writeInt(4);
         buff.writeInt(i);
         acceptorListener.connection.write(buff);
      }

      for (int i = 0; i < numPackets; i++)
      {
         HornetQBuffer buff = conn.createBuffer(8);
         buff.writeInt(4);
         buff.writeInt(i);
         conn.write(buff);
      }
      acceptorLatch.await(10, TimeUnit.SECONDS);
      connectorLatch.await(10, TimeUnit.SECONDS);
      conn.close();
      assertEquals(acceptorHandler.messagesReceieved, numPackets);
      assertEquals(connectorHandler.messagesReceieved, numPackets);
      int i = 0;
      for (Integer j : acceptorHandler.messages)
      {
         assertTrue(i == j);
         i++;
      }
      i = 0;
      for (Integer j : connectorHandler.messages)
      {
         assertTrue(i == j);
         i++;
      }
   }

   public void testReceivePiggyBackOnOneResponse() throws Exception
   {

      int numPackets = 1000;
      CountDownLatch connCreatedLatch = new CountDownLatch(1);
      CountDownLatch acceptorLatch = new CountDownLatch(1);
      CountDownLatch connectorLatch = new CountDownLatch(numPackets);
      HashMap<String, Object> conf = new HashMap<String, Object>();
      conf.put(TransportConstants.HTTP_ENABLED_PROP_NAME, true);
      conf.put(TransportConstants.HTTP_CLIENT_IDLE_SCAN_PERIOD, -1l);
      DummyConnectionLifeCycleListener acceptorListener = new DummyConnectionLifeCycleListener(connCreatedLatch);
      SimpleBufferHandler acceptorHandler = new SimpleBufferHandler(acceptorLatch);
      acceptor = new NettyAcceptor(conf, acceptorHandler, acceptorListener, threadPool, scheduledThreadPool);
      acceptor.start();

      SimpleBufferHandler connectorHandler = new SimpleBufferHandler(connectorLatch);
      connector = new NettyConnector(conf, connectorHandler, new DummyConnectionLifeCycleListener(null), threadPool, scheduledThreadPool);
      connector.start();
      Connection conn = connector.createConnection();
      connCreatedLatch.await(5, TimeUnit.SECONDS);
      for (int i = 0; i < numPackets; i++)
      {
         HornetQBuffer buff = conn.createBuffer(8);
         buff.writeInt(4);
         buff.writeInt(i);
         acceptorListener.connection.write(buff);
      }

      HornetQBuffer buff = conn.createBuffer(8);
      buff.writeInt(4);
      buff.writeInt(0);
      conn.write(buff);

      acceptorLatch.await(10, TimeUnit.SECONDS);
      connectorLatch.await(10, TimeUnit.SECONDS);
      conn.close();
      assertEquals(acceptorHandler.messagesReceieved, 1);
      assertEquals(connectorHandler.messagesReceieved, numPackets);
      int i = 0;
      for (Integer j : acceptorHandler.messages)
      {
         assertTrue(i == j);
         i++;
      }
      i = 0;
      for (Integer j : connectorHandler.messages)
      {
         assertTrue(i == j);
         i++;
      }
   }

   public void testReceivePiggyBackOnIdleClient() throws Exception
   {

      int numPackets = 1000;
      CountDownLatch connCreatedLatch = new CountDownLatch(1);
      CountDownLatch acceptorLatch = new CountDownLatch(0);
      CountDownLatch connectorLatch = new CountDownLatch(numPackets);
      HashMap<String, Object> conf = new HashMap<String, Object>();
      conf.put(TransportConstants.HTTP_ENABLED_PROP_NAME, true);
      conf.put(TransportConstants.HTTP_CLIENT_IDLE_SCAN_PERIOD, 500l);
      conf.put(TransportConstants.HTTP_CLIENT_IDLE_PROP_NAME, 500l);
      DummyConnectionLifeCycleListener acceptorListener = new DummyConnectionLifeCycleListener(connCreatedLatch);
      SimpleBufferHandler acceptorHandler = new SimpleBufferHandler(acceptorLatch);
      acceptor = new NettyAcceptor(conf, acceptorHandler, acceptorListener, threadPool, scheduledThreadPool);
      acceptor.start();

      SimpleBufferHandler connectorHandler = new SimpleBufferHandler(connectorLatch);
      connector = new NettyConnector(conf, connectorHandler, new DummyConnectionLifeCycleListener(null), threadPool, scheduledThreadPool);
      connector.start();
      Connection conn = connector.createConnection();
      connCreatedLatch.await(5, TimeUnit.SECONDS);
      for (int i = 0; i < numPackets; i++)
      {
         HornetQBuffer buff = conn.createBuffer(8);
         buff.writeInt(4);
         buff.writeInt(i);
         acceptorListener.connection.write(buff);
      }

      acceptorLatch.await(10, TimeUnit.SECONDS);
      connectorLatch.await(10, TimeUnit.SECONDS);
      conn.close();
      assertEquals(acceptorHandler.messagesReceieved, 0);
      assertEquals(connectorHandler.messagesReceieved, numPackets);
      int i = 0;
      for (Integer j : acceptorHandler.messages)
      {
         assertTrue(i == j);
         i++;
      }
      i = 0;
      for (Integer j : connectorHandler.messages)
      {
         assertTrue(i == j);
         i++;
      }
   }

   public void testSendWithNoReceive() throws Exception
   {

      int numPackets = 1000;
      CountDownLatch connCreatedLatch = new CountDownLatch(1);
      CountDownLatch acceptorLatch = new CountDownLatch(numPackets);
      CountDownLatch connectorLatch = new CountDownLatch(numPackets);
      HashMap<String, Object> conf = new HashMap<String, Object>();
      conf.put(TransportConstants.HTTP_ENABLED_PROP_NAME, true);
      conf.put(TransportConstants.HTTP_CLIENT_IDLE_SCAN_PERIOD, -1l);
      conf.put(TransportConstants.HTTP_RESPONSE_TIME_PROP_NAME, 500l);
      conf.put(TransportConstants.HTTP_SERVER_SCAN_PERIOD_PROP_NAME, 5000l);
      DummyConnectionLifeCycleListener acceptorListener = new DummyConnectionLifeCycleListener(connCreatedLatch);
      SimpleBufferHandler acceptorHandler = new SimpleBufferHandler(acceptorLatch);
      acceptor = new NettyAcceptor(conf, acceptorHandler, acceptorListener, threadPool, scheduledThreadPool);
      acceptor.start();

      BogusResponseHandler connectorHandler = new BogusResponseHandler(connectorLatch);
      connector = new NettyConnector(conf, connectorHandler, new DummyConnectionLifeCycleListener(null), threadPool, scheduledThreadPool);
      connector.start();
      Connection conn = connector.createConnection();
      connCreatedLatch.await(5, TimeUnit.SECONDS);
      for (int i = 0; i < numPackets; i++)
      {
         HornetQBuffer buff = conn.createBuffer(8);
         buff.writeInt(4);
         buff.writeInt(i);
         conn.write(buff);
      }
      acceptorLatch.await(100, TimeUnit.SECONDS);
      connectorLatch.await(0, TimeUnit.SECONDS);
      conn.close();
      assertEquals(acceptorHandler.messagesReceieved, numPackets);
      assertEquals(connectorHandler.messagesReceieved, 0);
      int i = 0;
      for (Integer j : acceptorHandler.messages)
      {
         assertTrue(i == j);
         i++;
      }
   }

   public void testSendOnly() throws Exception
   {

      int numPackets = 1000;
      CountDownLatch connCreatedLatch = new CountDownLatch(1);
      CountDownLatch acceptorLatch = new CountDownLatch(numPackets);
      CountDownLatch connectorLatch = new CountDownLatch(numPackets);
      HashMap<String, Object> conf = new HashMap<String, Object>();
      conf.put(TransportConstants.HTTP_ENABLED_PROP_NAME, true);
      conf.put(TransportConstants.HTTP_CLIENT_IDLE_SCAN_PERIOD, -1l);
      conf.put(TransportConstants.HTTP_RESPONSE_TIME_PROP_NAME, 500l);
      conf.put(TransportConstants.HTTP_SERVER_SCAN_PERIOD_PROP_NAME, 5000l);
      DummyConnectionLifeCycleListener acceptorListener = new DummyConnectionLifeCycleListener(connCreatedLatch);
      SimpleBufferHandler2 acceptorHandler = new SimpleBufferHandler2(acceptorLatch);
      acceptor = new NettyAcceptor(conf, acceptorHandler, acceptorListener, threadPool, scheduledThreadPool);
      acceptor.start();

      BogusResponseHandler connectorHandler = new BogusResponseHandler(connectorLatch);
      connector = new NettyConnector(conf, connectorHandler, new DummyConnectionLifeCycleListener(null), threadPool, scheduledThreadPool);
      connector.start();
      Connection conn = connector.createConnection();
      connCreatedLatch.await(5, TimeUnit.SECONDS);
      for (int i = 0; i < numPackets; i++)
      {
         HornetQBuffer buff = conn.createBuffer(8);
         buff.writeInt(4);
         buff.writeInt(i);
         conn.write(buff);
      }
      acceptorLatch.await(10, TimeUnit.SECONDS);
      connectorLatch.await(0, TimeUnit.SECONDS);
      conn.close();
      assertEquals(acceptorHandler.messagesReceieved, numPackets);
      assertEquals(connectorHandler.messagesReceieved, 0);
      int i = 0;
      for (Integer j : acceptorHandler.messages)
      {
         assertTrue(i == j);
         i++;
      }
   }


   class SimpleBufferHandler extends AbstractBufferHandler
   {
      int messagesReceieved = 0;

      ArrayList<Integer> messages = new ArrayList<Integer>();

      private CountDownLatch latch;

      public SimpleBufferHandler(CountDownLatch latch)
      {
         this.latch = latch;
      }

      public void bufferReceived(Object connectionID, HornetQBuffer buffer)
      {
         int i = buffer.readInt();
         messages.add(i);
         messagesReceieved++;
         latch.countDown();
      }
   }
   class SimpleBufferHandler2 extends AbstractBufferHandler
   {
      int messagesReceieved = 0;

      ArrayList<Integer> messages = new ArrayList<Integer>();

      private CountDownLatch latch;

      public SimpleBufferHandler2(CountDownLatch latch)
      {
         this.latch = latch;
      }

      public void bufferReceived(Object connectionID, HornetQBuffer buffer)
      {
         int i = buffer.readInt();

         if( messagesReceieved == 0 && messagesReceieved != i)
         {
            System.out.println("first message not received = " + i);
         }
         messages.add(i);
         messagesReceieved++;
         latch.countDown();
      }

      
   }
   class BogusResponseHandler implements BufferHandler
   {
      int messagesReceieved = 0;

      ArrayList<Integer> messages = new ArrayList<Integer>();

      private CountDownLatch latch;

      public BogusResponseHandler(CountDownLatch latch)
      {
         this.latch = latch;
      }

      public int isReadyToHandle(HornetQBuffer buffer)
      {
         return 0;
      }

      public void bufferReceived(Object connectionID, HornetQBuffer buffer)
      {
         int i = buffer.readInt();
         messages.add(i);
         messagesReceieved++;
         latch.countDown();
      }
   }

   class DummyConnectionLifeCycleListener implements ConnectionLifeCycleListener
   {
      Connection connection;

      private CountDownLatch latch;

      public DummyConnectionLifeCycleListener(CountDownLatch connCreatedLatch)
      {
         this.latch = connCreatedLatch;
      }

      public void connectionCreated(Connection connection)
      {
         this.connection = connection;
         if (latch != null)
         {
            latch.countDown();
         }
      }

      public void connectionDestroyed(Object connectionID)
      {
      }

      public void connectionException(Object connectionID, HornetQException me)
      {
         me.printStackTrace();
      }
   }
}
