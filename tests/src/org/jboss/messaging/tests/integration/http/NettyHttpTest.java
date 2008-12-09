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
package org.jboss.messaging.tests.integration.http;

import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.integration.transports.netty.NettyAcceptor;
import org.jboss.messaging.integration.transports.netty.TransportConstants;
import org.jboss.messaging.integration.transports.netty.NettyConnector;
import org.jboss.messaging.core.remoting.spi.BufferHandler;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.core.remoting.spi.Connection;
import org.jboss.messaging.core.remoting.spi.ConnectionLifeCycleListener;
import org.jboss.messaging.core.remoting.impl.AbstractBufferHandler;
import org.jboss.messaging.core.exception.MessagingException;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class NettyHttpTest extends UnitTestCase
{
   private NettyAcceptor acceptor;

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
      acceptor = new NettyAcceptor(conf, acceptorHandler, acceptorListener);
      acceptor.start();

      SimpleBufferHandler connectorHandler = new SimpleBufferHandler(connectorLatch);
      NettyConnector connector = new NettyConnector(conf, connectorHandler, new DummyConnectionLifeCycleListener(null));
      connector.start();
      Connection conn = connector.createConnection();
      connCreatedLatch.await(5, TimeUnit.SECONDS);
      for (int i = 0; i < numPackets; i++)
      {
         MessagingBuffer buff = conn.createBuffer(8);
         buff.putInt(4);
         buff.putInt(i);
         buff.flip();
         conn.write(buff);
         buff = conn.createBuffer(8);
         buff.putInt(4);
         buff.putInt(i);
         buff.flip();
         acceptorListener.connection.write(buff);
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
      acceptor = new NettyAcceptor(conf, acceptorHandler, acceptorListener);
      acceptor.start();

      SimpleBufferHandler connectorHandler = new SimpleBufferHandler(connectorLatch);
      NettyConnector connector = new NettyConnector(conf, connectorHandler, new DummyConnectionLifeCycleListener(null));
      connector.start();
      Connection conn = connector.createConnection();
      connCreatedLatch.await(5, TimeUnit.SECONDS);
      for (int i = 0; i < numPackets; i++)
      {
         MessagingBuffer buff = conn.createBuffer(8);
         buff.putInt(4);
         buff.putInt(i);
         buff.flip();
         conn.write(buff);
      }
      for (int i = 0; i < numPackets; i++)
      {
         MessagingBuffer buff = conn.createBuffer(8);
         buff.putInt(4);
         buff.putInt(i);
         buff.flip();
         acceptorListener.connection.write(buff);
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
      acceptor = new NettyAcceptor(conf, acceptorHandler, acceptorListener);
      acceptor.start();

      SimpleBufferHandler connectorHandler = new SimpleBufferHandler(connectorLatch);
      NettyConnector connector = new NettyConnector(conf, connectorHandler, new DummyConnectionLifeCycleListener(null));
      connector.start();
      Connection conn = connector.createConnection();
      connCreatedLatch.await(5, TimeUnit.SECONDS);
      for (int i = 0; i < numPackets; i++)
      {
         MessagingBuffer buff = conn.createBuffer(8);
         buff.putInt(4);
         buff.putInt(i);
         buff.flip();
         acceptorListener.connection.write(buff);
      }

      for (int i = 0; i < numPackets; i++)
      {
         MessagingBuffer buff = conn.createBuffer(8);
         buff.putInt(4);
         buff.putInt(i);
         buff.flip();
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
      acceptor = new NettyAcceptor(conf, acceptorHandler, acceptorListener);
      acceptor.start();

      SimpleBufferHandler connectorHandler = new SimpleBufferHandler(connectorLatch);
      NettyConnector connector = new NettyConnector(conf, connectorHandler, new DummyConnectionLifeCycleListener(null));
      connector.start();
      Connection conn = connector.createConnection();
      connCreatedLatch.await(5, TimeUnit.SECONDS);
      for (int i = 0; i < numPackets; i++)
      {
         MessagingBuffer buff = conn.createBuffer(8);
         buff.putInt(4);
         buff.putInt(i);
         buff.flip();
         acceptorListener.connection.write(buff);
      }


      MessagingBuffer buff = conn.createBuffer(8);
      buff.putInt(4);
      buff.putInt(0);
      buff.flip();
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
      acceptor = new NettyAcceptor(conf, acceptorHandler, acceptorListener);
      acceptor.start();

      SimpleBufferHandler connectorHandler = new SimpleBufferHandler(connectorLatch);
      NettyConnector connector = new NettyConnector(conf, connectorHandler, new DummyConnectionLifeCycleListener(null));
      connector.start();
      Connection conn = connector.createConnection();
      connCreatedLatch.await(5, TimeUnit.SECONDS);
      for (int i = 0; i < numPackets; i++)
      {
         MessagingBuffer buff = conn.createBuffer(8);
         buff.putInt(4);
         buff.putInt(i);
         buff.flip();
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
      acceptor = new NettyAcceptor(conf, acceptorHandler, acceptorListener);
      acceptor.start();

      BogusResponseHandler connectorHandler = new BogusResponseHandler(connectorLatch);
      NettyConnector connector = new NettyConnector(conf, connectorHandler, new DummyConnectionLifeCycleListener(null));
      connector.start();
      Connection conn = connector.createConnection();
      connCreatedLatch.await(5, TimeUnit.SECONDS);
      for (int i = 0; i < numPackets; i++)
      {
         MessagingBuffer buff = conn.createBuffer(8);
         buff.putInt(4);
         buff.putInt(i);
         buff.flip();
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
   protected void setUp() throws Exception
   {
   }

   @Override
   protected void tearDown() throws Exception
   {
      
      if(acceptor != null)
      {
         acceptor.stop();
         acceptor = null;
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


      public void bufferReceived(Object connectionID, MessagingBuffer buffer)
      {
         int i = buffer.getInt();
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

      public int isReadyToHandle(MessagingBuffer buffer)
      {
         return 0;
      }

      public void bufferReceived(Object connectionID, MessagingBuffer buffer)
      {
         int i = buffer.getInt();
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

      public void connectionException(Object connectionID, MessagingException me)
      {
      }
   }
}
