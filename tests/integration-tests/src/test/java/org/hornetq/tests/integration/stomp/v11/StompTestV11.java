/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hornetq.tests.integration.stomp.v11;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.BytesMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import junit.framework.Assert;

import org.hornetq.core.logging.Logger;
import org.hornetq.core.protocol.stomp.Stomp;
import org.hornetq.tests.integration.stomp.util.ClientStompFrame;
import org.hornetq.tests.integration.stomp.util.StompClientConnection;
import org.hornetq.tests.integration.stomp.util.StompClientConnectionFactory;
import org.hornetq.tests.integration.stomp.util.StompClientConnectionV11;


public class StompTestV11 extends StompTestBase2
{
   private static final transient Logger log = Logger.getLogger(StompTestV11.class);
   
   private StompClientConnection connV11;
   
   protected void setUp() throws Exception
   {
      super.setUp();
      connV11 = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
   }
   
   protected void tearDown() throws Exception
   {
      if (connV11.isConnected())
      {
         connV11.disconnect();
      }
      super.tearDown();
   }
   
   public void testConnection() throws Exception
   {
      StompClientConnection connection = StompClientConnectionFactory.createClientConnection("1.0", hostname, port);
      
      connection.connect(defUser, defPass);
      
      assertTrue(connection.isConnected());
      
      assertEquals("1.0", connection.getVersion());
      
      connection.disconnect();

      connection = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      
      connection.connect(defUser, defPass);
      
      assertTrue(connection.isConnected());
      
      assertEquals("1.1", connection.getVersion());
      
      connection.disconnect();
      
      connection = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      
      connection.connect();
      
      assertFalse(connection.isConnected());
      
      //new way of connection
      StompClientConnectionV11 conn = (StompClientConnectionV11) StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      conn.connect1(defUser, defPass);
      
      assertTrue(conn.isConnected());
      
      conn.disconnect();
   }
   
   public void testNegotiation() throws Exception
   {
      // case 1 accept-version absent. It is a 1.0 connect
      ClientStompFrame frame = connV11.createFrame("CONNECT");
      frame.addHeader("host", "127.0.0.1");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);
      
      ClientStompFrame reply = connV11.sendFrame(frame);
      
      assertEquals("CONNECTED", reply.getCommand());
      
      //reply headers: version, session, server
      assertEquals(null, reply.getHeader("version"));

      connV11.disconnect();

      // case 2 accept-version=1.0, result: 1.0
      connV11 = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      frame = connV11.createFrame("CONNECT");
      frame.addHeader("accept-version", "1.0");
      frame.addHeader("host", "127.0.0.1");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);
      
      reply = connV11.sendFrame(frame);
      
      assertEquals("CONNECTED", reply.getCommand());
      
      //reply headers: version, session, server
      assertEquals("1.0", reply.getHeader("version"));
      
      connV11.disconnect();

      // case 3 accept-version=1.1, result: 1.1
      connV11 = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      frame = connV11.createFrame("CONNECT");
      frame.addHeader("accept-version", "1.1");
      frame.addHeader("host", "127.0.0.1");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);
      
      reply = connV11.sendFrame(frame);
      
      assertEquals("CONNECTED", reply.getCommand());
      
      //reply headers: version, session, server
      assertEquals("1.1", reply.getHeader("version"));
      
      connV11.disconnect();

      // case 4 accept-version=1.0,1.1,1.2, result 1.1
      connV11 = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      frame = connV11.createFrame("CONNECT");
      frame.addHeader("accept-version", "1.0,1.1,1.2");
      frame.addHeader("host", "127.0.0.1");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);
      
      reply = connV11.sendFrame(frame);
      
      assertEquals("CONNECTED", reply.getCommand());
      
      //reply headers: version, session, server
      assertEquals("1.1", reply.getHeader("version"));
      
      connV11.disconnect();

      // case 5 accept-version=1.2, result error
      connV11 = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      frame = connV11.createFrame("CONNECT");
      frame.addHeader("accept-version", "1.2");
      frame.addHeader("host", "127.0.0.1");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);
      
      reply = connV11.sendFrame(frame);
      
      assertEquals("ERROR", reply.getCommand());
      
      System.out.println("Got error frame " + reply);
      
   }
   
   public void testSendAndReceive() throws Exception
   {
      connV11.connect(defUser, defPass);
      ClientStompFrame frame = connV11.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-type", "text/plain");
      frame.setBody("Hello World 1!");
      
      ClientStompFrame response = connV11.sendFrame(frame);
      
      assertNull(response);
      
      frame.addHeader("receipt", "1234");
      frame.setBody("Hello World 2!");
      
      response = connV11.sendFrame(frame);
      
      assertNotNull(response);
      
      assertEquals("RECEIPT", response.getCommand());
      
      assertEquals("1234", response.getHeader("receipt-id"));
      
      //subscribe
      StompClientConnection newConn = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      newConn.connect(defUser, defPass);
      
      ClientStompFrame subFrame = newConn.createFrame("SUBSCRIBE");
      subFrame.addHeader("id", "a-sub");
      subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      subFrame.addHeader("ack", "auto");
      
      newConn.sendFrame(subFrame);
      
      frame = newConn.receiveFrame();
      
      System.out.println("received " + frame);
      
      assertEquals("MESSAGE", frame.getCommand());
      
      assertEquals("a-sub", frame.getHeader("subscription"));
      
      assertNotNull(frame.getHeader("message-id"));
      
      assertEquals(getQueuePrefix() + getQueueName(), frame.getHeader("destination"));
      
      assertEquals("Hello World 1!", frame.getBody());
      
      frame = newConn.receiveFrame();
      
      System.out.println("received " + frame);      
      
      //unsub
      ClientStompFrame unsubFrame = newConn.createFrame("UNSUBSCRIBE");
      unsubFrame.addHeader("id", "a-sub");
      
      newConn.disconnect();
   }

   public void testHeaderContentType() throws Exception
   {
      connV11.connect(defUser, defPass);
      ClientStompFrame frame = connV11.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-type", "application/xml");
      frame.setBody("Hello World 1!");
      
      connV11.sendFrame(frame);
      
      //subscribe
      StompClientConnection newConn = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      newConn.connect(defUser, defPass);
      
      ClientStompFrame subFrame = newConn.createFrame("SUBSCRIBE");
      subFrame.addHeader("id", "a-sub");
      subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      subFrame.addHeader("ack", "auto");
      
      newConn.sendFrame(subFrame);
      
      frame = newConn.receiveFrame();
      
      System.out.println("received " + frame);
      
      assertEquals("MESSAGE", frame.getCommand());
      
      assertEquals("application/xml", frame.getHeader("content-type"));
      
      //unsub
      ClientStompFrame unsubFrame = newConn.createFrame("UNSUBSCRIBE");
      unsubFrame.addHeader("id", "a-sub");
      
      newConn.disconnect();
   }

   public void testHeaderContentLength() throws Exception
   {
      connV11.connect(defUser, defPass);
      ClientStompFrame frame = connV11.createFrame("SEND");
      
      String body = "Hello World 1!";
      String cLen = String.valueOf(body.getBytes("UTF-8").length);
      
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-type", "application/xml");
      frame.addHeader("content-length", cLen);
      frame.setBody(body + "extra");
      
      connV11.sendFrame(frame);
      
      //subscribe
      StompClientConnection newConn = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      newConn.connect(defUser, defPass);
      
      ClientStompFrame subFrame = newConn.createFrame("SUBSCRIBE");
      subFrame.addHeader("id", "a-sub");
      subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      subFrame.addHeader("ack", "auto");
      
      newConn.sendFrame(subFrame);
      
      frame = newConn.receiveFrame();
      
      System.out.println("received " + frame);
      
      assertEquals("MESSAGE", frame.getCommand());
      
      assertEquals(cLen, frame.getHeader("content-length"));
      
      //unsub
      ClientStompFrame unsubFrame = newConn.createFrame("UNSUBSCRIBE");
      unsubFrame.addHeader("id", "a-sub");
      
      newConn.disconnect();
   }

   public void testHeaderEncoding() throws Exception
   {
      connV11.connect(defUser, defPass);
      ClientStompFrame frame = connV11.createFrame("SEND");
      
      String body = "Hello World 1!";
      String cLen = String.valueOf(body.getBytes("UTF-8").length);
      
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-type", "application/xml");
      frame.addHeader("content-length", cLen);
      String hKey = "special-header\\\\\\n\\:";
      String hVal = "\\:\\\\\\ngood";
      frame.addHeader(hKey, hVal);
      
      System.out.println("key: |" + hKey + "| val: |" + hVal);
      
      frame.setBody(body);
      
      connV11.sendFrame(frame);
      
      //subscribe
      StompClientConnection newConn = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      newConn.connect(defUser, defPass);
      
      ClientStompFrame subFrame = newConn.createFrame("SUBSCRIBE");
      subFrame.addHeader("id", "a-sub");
      subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      subFrame.addHeader("ack", "auto");
      
      newConn.sendFrame(subFrame);
      
      frame = newConn.receiveFrame();
      
      System.out.println("received " + frame);
      
      assertEquals("MESSAGE", frame.getCommand());
      
      String value = frame.getHeader("special-header" + "\\" + "\n" + ":");
      
      assertEquals(":" + "\\" + "\n" + "good", value);
      
      //unsub
      ClientStompFrame unsubFrame = newConn.createFrame("UNSUBSCRIBE");
      unsubFrame.addHeader("id", "a-sub");
      
      newConn.disconnect();
   }
   
   public void testHeartBeat() throws Exception
   {
      //no heart beat at all if heat-beat absent
      ClientStompFrame frame = connV11.createFrame("CONNECT");
      frame.addHeader("host", "127.0.0.1");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);
      
      ClientStompFrame reply = connV11.sendFrame(frame);
      
      assertEquals("CONNECTED", reply.getCommand());
      
      Thread.sleep(5000);
      
      assertEquals(0, connV11.getFrameQueueSize());
      
      connV11.disconnect();
      
      //no heart beat for (0,0)
      connV11 = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      frame = connV11.createFrame("CONNECT");
      frame.addHeader("host", "127.0.0.1");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);
      frame.addHeader("heart-beat", "0,0");
      frame.addHeader("accept-version", "1.0,1.1");
      
      reply = connV11.sendFrame(frame);
      
      assertEquals("CONNECTED", reply.getCommand());
      
      assertEquals("0,0", reply.getHeader("heart-beat"));
      
      Thread.sleep(5000);
      
      assertEquals(0, connV11.getFrameQueueSize());
      
      connV11.disconnect();

      //heart-beat (1,0), should receive a min client ping accepted by server
      connV11 = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      frame = connV11.createFrame("CONNECT");
      frame.addHeader("host", "127.0.0.1");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);
      frame.addHeader("heart-beat", "1,0");
      frame.addHeader("accept-version", "1.0,1.1");
      
      reply = connV11.sendFrame(frame);
      
      assertEquals("CONNECTED", reply.getCommand());
      
      assertEquals("0,500", reply.getHeader("heart-beat"));
      
      Thread.sleep(2000);
      
      //now server side should be disconnected because we didn't send ping for 2 sec
      frame = connV11.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-type", "text/plain");
      frame.setBody("Hello World");

      //send will fail
      try
      {
         connV11.sendFrame(frame);
         fail("connection should have been destroyed by now");
      }
      catch (IOException e)
      {
         //ignore
      }
      
      //heart-beat (1,0), start a ping, then send a message, should be ok.
      connV11 = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      frame = connV11.createFrame("CONNECT");
      frame.addHeader("host", "127.0.0.1");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);
      frame.addHeader("heart-beat", "1,0");
      frame.addHeader("accept-version", "1.0,1.1");
      
      reply = connV11.sendFrame(frame);
      
      assertEquals("CONNECTED", reply.getCommand());
      
      assertEquals("0,500", reply.getHeader("heart-beat"));
      
      System.out.println("========== start pinger!");
      
      connV11.startPinger(500);
      
      Thread.sleep(2000);
      
      //now server side should be disconnected because we didn't send ping for 2 sec
      frame = connV11.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-type", "text/plain");
      frame.setBody("Hello World");

      //send will be ok
      connV11.sendFrame(frame);
      
      connV11.stopPinger();
      
      connV11.disconnect();

   }
   
   //server ping
   public void testHeartBeat2() throws Exception
   {
      //heart-beat (1,1)
      ClientStompFrame frame = connV11.createFrame("CONNECT");
      frame.addHeader("host", "127.0.0.1");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);
      frame.addHeader("heart-beat", "1,1");
      frame.addHeader("accept-version", "1.0,1.1");
      
      ClientStompFrame reply = connV11.sendFrame(frame);
      
      assertEquals("CONNECTED", reply.getCommand());
      
      assertEquals("500,500", reply.getHeader("heart-beat"));
      
      connV11.disconnect();
      
      //heart-beat (500,1000)
      connV11 = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      frame = connV11.createFrame("CONNECT");
      frame.addHeader("host", "127.0.0.1");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);
      frame.addHeader("heart-beat", "500,1000");
      frame.addHeader("accept-version", "1.0,1.1");
      
      reply = connV11.sendFrame(frame);
      
      assertEquals("CONNECTED", reply.getCommand());
      
      assertEquals("1000,500", reply.getHeader("heart-beat"));
      
      System.out.println("========== start pinger!");
      
      connV11.startPinger(500);
      
      Thread.sleep(10000);
      
      //now check the frame size
      int size = connV11.getFrameQueueSize();
      
      System.out.println("ping received: " + size);
      
      assertTrue(size > 5);
      
      //now server side should be disconnected because we didn't send ping for 2 sec
      frame = connV11.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-type", "text/plain");
      frame.setBody("Hello World");

      //send will be ok
      connV11.sendFrame(frame);
      
      connV11.stopPinger();
      
      connV11.disconnect();

   }
   
   public void testNack() throws Exception
   {
      connV11.connect(defUser, defPass);

      subscribe(connV11, "sub1", "client");

      sendMessage(getName());

      ClientStompFrame frame = connV11.receiveFrame();

      String messageID = frame.getHeader("message-id");
      
      System.out.println("Received message with id " + messageID);
      
      nack(connV11, "sub1", messageID);
      
      unsubscribe(connV11, "sub1");
      
      connV11.disconnect();

      //Nack makes the message be dropped.
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNull(message);
   }

   public void testNackWithWrongSubId() throws Exception
   {
      connV11.connect(defUser, defPass);

      subscribe(connV11, "sub1", "client");

      sendMessage(getName());

      ClientStompFrame frame = connV11.receiveFrame();

      String messageID = frame.getHeader("message-id");
      
      System.out.println("Received message with id " + messageID);
      
      nack(connV11, "sub2", messageID);
      
      ClientStompFrame error = connV11.receiveFrame();
      
      System.out.println("Receiver error: " + error);
      
      unsubscribe(connV11, "sub1");
      
      connV11.disconnect();

      //message should be still there
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNotNull(message);
   }

   public void testNackWithWrongMessageId() throws Exception
   {
      connV11.connect(defUser, defPass);

      subscribe(connV11, "sub1", "client");

      sendMessage(getName());

      ClientStompFrame frame = connV11.receiveFrame();

      String messageID = frame.getHeader("message-id");
      
      System.out.println("Received message with id " + messageID);
      
      nack(connV11, "sub2", "someother");
      
      ClientStompFrame error = connV11.receiveFrame();
      
      System.out.println("Receiver error: " + error);
      
      unsubscribe(connV11, "sub1");
      
      connV11.disconnect();

      //message should still there
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNotNull(message);
   }
   
   
   public void testAck() throws Exception
   {
      connV11.connect(defUser, defPass);

      subscribe(connV11, "sub1", "client");

      sendMessage(getName());

      ClientStompFrame frame = connV11.receiveFrame();

      String messageID = frame.getHeader("message-id");
      
      System.out.println("Received message with id " + messageID);
      
      ack(connV11, "sub1", messageID, null);
      
      unsubscribe(connV11, "sub1");
      
      connV11.disconnect();

      //Nack makes the message be dropped.
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNull(message);
   }

   public void testAckWithWrongSubId() throws Exception
   {
      connV11.connect(defUser, defPass);

      subscribe(connV11, "sub1", "client");

      sendMessage(getName());

      ClientStompFrame frame = connV11.receiveFrame();

      String messageID = frame.getHeader("message-id");
      
      System.out.println("Received message with id " + messageID);
      
      ack(connV11, "sub2", messageID, null);
      
      ClientStompFrame error = connV11.receiveFrame();
      
      System.out.println("Receiver error: " + error);
      
      unsubscribe(connV11, "sub1");
      
      connV11.disconnect();

      //message should be still there
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNotNull(message);
   }

   public void testAckWithWrongMessageId() throws Exception
   {
      connV11.connect(defUser, defPass);

      subscribe(connV11, "sub1", "client");

      sendMessage(getName());

      ClientStompFrame frame = connV11.receiveFrame();

      String messageID = frame.getHeader("message-id");
      
      System.out.println("Received message with id " + messageID);
      
      ack(connV11, "sub2", "someother", null);
      
      ClientStompFrame error = connV11.receiveFrame();
      
      System.out.println("Receiver error: " + error);
      
      unsubscribe(connV11, "sub1");
      
      connV11.disconnect();

      //message should still there
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNotNull(message);
   }
   
   public void testErrorWithReceipt() throws Exception
   {
      connV11.connect(defUser, defPass);

      subscribe(connV11, "sub1", "client");

      sendMessage(getName());

      ClientStompFrame frame = connV11.receiveFrame();

      String messageID = frame.getHeader("message-id");
      
      System.out.println("Received message with id " + messageID);
      
      ClientStompFrame ackFrame = connV11.createFrame("ACK");
      //give it a wrong sub id
      ackFrame.addHeader("subscription", "sub2");
      ackFrame.addHeader("message-id", messageID);
      ackFrame.addHeader("receipt", "answer-me");
      
      ClientStompFrame error = connV11.sendFrame(ackFrame);
      
      System.out.println("Receiver error: " + error);
      
      assertEquals("ERROR", error.getCommand());
      
      assertEquals("answer-me", error.getHeader("receipt-id"));
      
      unsubscribe(connV11, "sub1");
      
      connV11.disconnect();

      //message should still there
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNotNull(message);      
   }
   
   public void testErrorWithReceipt2() throws Exception
   {
      connV11.connect(defUser, defPass);

      subscribe(connV11, "sub1", "client");

      sendMessage(getName());

      ClientStompFrame frame = connV11.receiveFrame();

      String messageID = frame.getHeader("message-id");
      
      System.out.println("Received message with id " + messageID);
      
      ClientStompFrame ackFrame = connV11.createFrame("ACK");
      //give it a wrong sub id
      ackFrame.addHeader("subscription", "sub1");
      ackFrame.addHeader("message-id", String.valueOf(Long.valueOf(messageID) + 1));
      ackFrame.addHeader("receipt", "answer-me");
      
      ClientStompFrame error = connV11.sendFrame(ackFrame);
      
      System.out.println("Receiver error: " + error);
      
      assertEquals("ERROR", error.getCommand());
      
      assertEquals("answer-me", error.getHeader("receipt-id"));
      
      unsubscribe(connV11, "sub1");
      
      connV11.disconnect();

      //message should still there
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNotNull(message);      
   }
   
   public void testAckModeClient() throws Exception
   {
      connV11.connect(defUser, defPass);

      subscribe(connV11, "sub1", "client");
      
      int num = 50;
      //send a bunch of messages
      for (int i = 0; i < num; i++)
      {
         this.sendMessage("client-ack" + i);
      }
      
      ClientStompFrame frame = null;
      
      for (int i = 0; i < num; i++)
      {
         frame = connV11.receiveFrame();
         assertNotNull(frame);
      }
      
      //ack the last
      this.ack(connV11, "sub1", frame);
      
      unsubscribe(connV11, "sub1");
      
      connV11.disconnect();
      
      //no messages can be received.
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNull(message);
   }
   
   public void testAckModeClient2() throws Exception
   {
      connV11.connect(defUser, defPass);

      subscribe(connV11, "sub1", "client");
      
      int num = 50;
      //send a bunch of messages
      for (int i = 0; i < num; i++)
      {
         this.sendMessage("client-ack" + i);
      }
      
      ClientStompFrame frame = null;
      
      for (int i = 0; i < num; i++)
      {
         frame = connV11.receiveFrame();
         assertNotNull(frame);

         //ack the 49th
         if (i == num - 2)
         {
            this.ack(connV11, "sub1", frame);
         }
      }
      
      unsubscribe(connV11, "sub1");
      
      connV11.disconnect();
      
      //no messages can be received.
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNotNull(message);
      message = consumer.receive(1000);
      Assert.assertNull(message);
   }
   
   public void testAckModeAuto() throws Exception
   {
      connV11.connect(defUser, defPass);

      subscribe(connV11, "sub1", "auto");
      
      int num = 50;
      //send a bunch of messages
      for (int i = 0; i < num; i++)
      {
         this.sendMessage("auto-ack" + i);
      }
      
      ClientStompFrame frame = null;
      
      for (int i = 0; i < num; i++)
      {
         frame = connV11.receiveFrame();
         assertNotNull(frame);
      }
      
      unsubscribe(connV11, "sub1");
      
      connV11.disconnect();
      
      //no messages can be received.
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNull(message);
   }
   
   public void testAckModeClientIndividual() throws Exception
   {
      connV11.connect(defUser, defPass);

      subscribe(connV11, "sub1", "client-individual");
      
      int num = 50;
      //send a bunch of messages
      for (int i = 0; i < num; i++)
      {
         this.sendMessage("client-individual-ack" + i);
      }
      
      ClientStompFrame frame = null;
      
      for (int i = 0; i < num; i++)
      {
         frame = connV11.receiveFrame();
         assertNotNull(frame);
         
         System.out.println(i + " == received: " + frame);
         //ack on even numbers
         if (i%2 == 0)
         {
            this.ack(connV11, "sub1", frame);
         }
      }
      
      unsubscribe(connV11, "sub1");
      
      connV11.disconnect();
      
      //no messages can be received.
      MessageConsumer consumer = session.createConsumer(queue);
      
      TextMessage message = null;
      for (int i = 0; i < num/2; i++)
      {
         message = (TextMessage) consumer.receive(1000);
         Assert.assertNotNull(message);
         System.out.println("Legal: " + message.getText());
      }
      
      message = (TextMessage) consumer.receive(1000);
      
      Assert.assertNull(message);
   }

   //tests below are adapted from StompTest
   public void testBeginSameTransactionTwice() throws Exception
   {
      connV11.connect(defUser, defPass);

      beginTransaction(connV11, "tx1");
      
      beginTransaction(connV11, "tx1");
      
      ClientStompFrame f = connV11.receiveFrame();
      Assert.assertTrue(f.getCommand().equals("ERROR"));
   }

   public void testBodyWithUTF8() throws Exception
   {
      connV11.connect(defUser, defPass);

      this.subscribe(connV11, getName(), "auto");

      String text = "A" + "\u00ea" + "\u00f1" + "\u00fc" + "C";
      System.out.println(text);
      sendMessage(text);

      ClientStompFrame frame = connV11.receiveFrame();
      System.out.println(frame);
      Assert.assertTrue(frame.getCommand().equals("MESSAGE"));
      Assert.assertNotNull(frame.getHeader("destination"));
      Assert.assertTrue(frame.getBody().equals(text));
      
      connV11.disconnect();
   }
   
   public void testClientAckNotPartOfTransaction() throws Exception
   {
      connV11.connect(defUser, defPass);

      this.subscribe(connV11, getName(), "client");

      sendMessage(getName());

      ClientStompFrame frame = connV11.receiveFrame();

      Assert.assertTrue(frame.getCommand().equals("MESSAGE"));
      Assert.assertNotNull(frame.getHeader("destination"));
      Assert.assertTrue(frame.getBody().equals(getName()));
      Assert.assertNotNull(frame.getHeader("message-id"));

      String messageID = frame.getHeader("message-id");

      beginTransaction(connV11, "tx1");

      this.ack(connV11, getName(), messageID, "tx1");

      abortTransaction(connV11, "tx1");

      frame = connV11.receiveFrame();
      
      assertNull(frame);

      this.unsubscribe(connV11, getName());

      connV11.disconnect();
   }

   public void testDisconnectAndError() throws Exception
   {
      connV11.connect(defUser, defPass);

      this.subscribe(connV11, getName(), "client");

      ClientStompFrame frame = connV11.createFrame("DISCONNECT");
      frame.addHeader("receipt", "1");
      
      ClientStompFrame result = connV11.sendFrame(frame);
      
      if (result == null || (!"RECEIPT".equals(result.getCommand())) || (!"1".equals(result.getHeader("receipt-id"))))
      {
         fail("Disconnect failed! " + result);
      }

      // sending a message will result in an error
      ClientStompFrame sendFrame = connV11.createFrame("SEND");
      sendFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      sendFrame.setBody("Hello World");
      
      try
      {
         connV11.sendFrame(sendFrame);
         fail("connection should have been closed by server.");
      }
      catch (ClosedChannelException e)
      {
         //ok.
      }
      
      connV11.destroy();
   }

   public void testDurableSubscriber() throws Exception
   {
      connV11.connect(defUser, defPass);

      this.subscribe(connV11, "sub1", "client", getName());

      this.subscribe(connV11, "sub1", "client", getName());

      ClientStompFrame frame = connV11.receiveFrame();
      Assert.assertTrue(frame.getCommand().equals("ERROR"));

      connV11.disconnect();
   }

   public void testDurableSubscriberWithReconnection() throws Exception
   {
      connV11.connect(defUser, defPass, "myclientid");

      this.subscribeTopic(connV11, "sub1", "auto", getName());

      ClientStompFrame frame = connV11.createFrame("DISCONNECT");
      frame.addHeader("receipt", "1");
      
      ClientStompFrame result = connV11.sendFrame(frame);
      
      if (result == null || (!"RECEIPT".equals(result.getCommand())) || (!"1".equals(result.getHeader("receipt-id"))))
      {
         fail("Disconnect failed! " + result);
      }

      // send the message when the durable subscriber is disconnected
      sendMessage(getName(), topic);

      connV11.destroy();
      connV11 = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      connV11.connect(defUser, defPass, "myclientid");

      this.subscribeTopic(connV11, "sub1", "auto", getName());

      // we must have received the message
      frame = connV11.receiveFrame();

      Assert.assertTrue(frame.getCommand().equals("MESSAGE"));
      Assert.assertNotNull(frame.getHeader("destination"));
      Assert.assertEquals(getName(), frame.getBody());

      this.unsubscribe(connV11, "sub1");
      
      connV11.disconnect();
   }

   public void testJMSXGroupIdCanBeSet() throws Exception
   {
      MessageConsumer consumer = session.createConsumer(queue);

      connV11.connect(defUser, defPass);

      ClientStompFrame frame = connV11.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("JMSXGroupID", "TEST");
      frame.setBody("Hello World");
      
      connV11.sendFrame(frame);

      TextMessage message = (TextMessage)consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertEquals("Hello World", message.getText());
      // differ from StompConnect
      Assert.assertEquals("TEST", message.getStringProperty("JMSXGroupID"));
   }

   public void testMessagesAreInOrder() throws Exception
   {
      int ctr = 10;
      String[] data = new String[ctr];

      connV11.connect(defUser, defPass);
      
      this.subscribe(connV11, "sub1", "auto");

      for (int i = 0; i < ctr; ++i)
      {
         data[i] = getName() + i;
         sendMessage(data[i]);
      }

      ClientStompFrame frame = null;
      
      for (int i = 0; i < ctr; ++i)
      {
         frame = connV11.receiveFrame();
         Assert.assertTrue("Message not in order", frame.getBody().equals(data[i]));
      }

      for (int i = 0; i < ctr; ++i)
      {
         data[i] = getName() + ":second:" + i;
         sendMessage(data[i]);
      }

      for (int i = 0; i < ctr; ++i)
      {
         frame = connV11.receiveFrame();
         Assert.assertTrue("Message not in order", frame.getBody().equals(data[i]));
      }

      connV11.disconnect();
   }

   public void testSubscribeWithAutoAckAndSelector() throws Exception
   {
      connV11.connect(defUser, defPass);
      
      this.subscribe(connV11, "sub1", "auto", null, "foo = 'zzz'");

      sendMessage("Ignored message", "foo", "1234");
      sendMessage("Real message", "foo", "zzz");

      ClientStompFrame frame = connV11.receiveFrame();

      Assert.assertTrue("Should have received the real message but got: " + frame, frame.getBody().equals("Real message"));

      connV11.disconnect();
   }

   public void testRedeliveryWithClientAck() throws Exception
   {
      connV11.connect(defUser, defPass);

      this.subscribe(connV11, "subId", "client");

      sendMessage(getName());

      ClientStompFrame frame = connV11.receiveFrame();
      
      assertTrue(frame.getCommand().equals("MESSAGE"));

      connV11.disconnect();

      // message should be received since message was not acknowledged
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertTrue(message.getJMSRedelivered());
   }

   public void testSendManyMessages() throws Exception
   {
      MessageConsumer consumer = session.createConsumer(queue);

      connV11.connect(defUser, defPass);

      int count = 1000;
      final CountDownLatch latch = new CountDownLatch(count);
      consumer.setMessageListener(new MessageListener()
      {
         public void onMessage(Message arg0)
         {
            latch.countDown();
         }
      });

      ClientStompFrame frame = connV11.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.setBody("Hello World");

      for (int i = 1; i <= count; i++)
      {
         connV11.sendFrame(frame);
      }

      assertTrue(latch.await(60, TimeUnit.SECONDS));
      
      connV11.disconnect();
   }

   public void testSendMessage() throws Exception
   {
      MessageConsumer consumer = session.createConsumer(queue);

      connV11.connect(defUser, defPass);

      ClientStompFrame frame = connV11.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.setBody("Hello World");
      
      connV11.sendFrame(frame);

      TextMessage message = (TextMessage)consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertEquals("Hello World", message.getText());
      // Assert default priority 4 is used when priority header is not set
      Assert.assertEquals("getJMSPriority", 4, message.getJMSPriority());

      // Make sure that the timestamp is valid - should
      // be very close to the current time.
      long tnow = System.currentTimeMillis();
      long tmsg = message.getJMSTimestamp();
      Assert.assertTrue(Math.abs(tnow - tmsg) < 1000);
   }

   public void testSendMessageWithContentLength() throws Exception
   {
      MessageConsumer consumer = session.createConsumer(queue);

      connV11.connect(defUser, defPass);

      byte[] data = new byte[] { 1, 0, 0, 4 };
      
      ClientStompFrame frame = connV11.createFrame("SEND");
      
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.setBody(new String(data, "UTF-8"));
      
      frame.addHeader("content-length", String.valueOf(data.length));

      connV11.sendFrame(frame);
      
      BytesMessage message = (BytesMessage)consumer.receive(10000);
      Assert.assertNotNull(message);
      //there is one extra null byte
      assertEquals(data.length + 1, message.getBodyLength());
      assertEquals(data[0], message.readByte());
      assertEquals(data[1], message.readByte());
      assertEquals(data[2], message.readByte());
      assertEquals(data[3], message.readByte());
   }

   public void testSendMessageWithCustomHeadersAndSelector() throws Exception
   {
      MessageConsumer consumer = session.createConsumer(queue, "foo = 'abc'");

      connV11.connect(defUser, defPass);

      ClientStompFrame frame = connV11.createFrame("SEND");
      frame.addHeader("foo", "abc");
      frame.addHeader("bar", "123");
      
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.setBody("Hello World");
      
      connV11.sendFrame(frame);

      TextMessage message = (TextMessage)consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertEquals("Hello World", message.getText());
      Assert.assertEquals("foo", "abc", message.getStringProperty("foo"));
      Assert.assertEquals("bar", "123", message.getStringProperty("bar"));
   }
   
   public void testSendMessageWithLeadingNewLine() throws Exception
   {
      MessageConsumer consumer = session.createConsumer(queue);

      connV11.connect(defUser, defPass);
      
      ClientStompFrame frame = connV11.createFrame("SEND");

      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.setBody("Hello World");

      connV11.sendWickedFrame(frame);

      TextMessage message = (TextMessage)consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertEquals("Hello World", message.getText());

      // Make sure that the timestamp is valid - should
      // be very close to the current time.
      long tnow = System.currentTimeMillis();
      long tmsg = message.getJMSTimestamp();
      Assert.assertTrue(Math.abs(tnow - tmsg) < 1000);
   }

   //-----------------private help methods
   
   private void abortTransaction(StompClientConnection conn, String txID) throws IOException, InterruptedException
   {
      ClientStompFrame abortFrame = conn.createFrame("ABORT");
      abortFrame.addHeader("transaction", txID);

      conn.sendFrame(abortFrame);
   }

   private void beginTransaction(StompClientConnection conn, String txID) throws IOException, InterruptedException
   {
      ClientStompFrame beginFrame = conn.createFrame("BEGIN");
      beginFrame.addHeader("transaction", txID);
      
      conn.sendFrame(beginFrame);
   }
   
   private void ack(StompClientConnection conn, String subId,
         ClientStompFrame frame) throws IOException, InterruptedException
   {
      String messageID = frame.getHeader("message-id");
      
      ClientStompFrame ackFrame = conn.createFrame("ACK");

      ackFrame.addHeader("subscription", subId);
      ackFrame.addHeader("message-id", messageID);
      
      ClientStompFrame response = conn.sendFrame(ackFrame);
      if (response != null)
      {
         throw new IOException("failed to ack " + response);
      }
   }

   private void ack(StompClientConnection conn, String subId, String mid, String txID) throws IOException, InterruptedException
   {
      ClientStompFrame ackFrame = conn.createFrame("ACK");
      ackFrame.addHeader("subscription", subId);
      ackFrame.addHeader("message-id", mid);
      if (txID != null)
      {
         ackFrame.addHeader("transaction", txID);
      }
      
      conn.sendFrame(ackFrame);
   }

   private void nack(StompClientConnection conn, String subId, String mid) throws IOException, InterruptedException
   {
      ClientStompFrame ackFrame = conn.createFrame("NACK");
      ackFrame.addHeader("subscription", subId);
      ackFrame.addHeader("message-id", mid);
      
      conn.sendFrame(ackFrame);
   }
   
   private void subscribe(StompClientConnection conn, String subId, String ack) throws IOException, InterruptedException
   {
      subscribe(conn, subId, ack, null, null);
   }

   private void subscribe(StompClientConnection conn, String subId,
         String ack, String durableId) throws IOException, InterruptedException
   {
      subscribe(conn, subId, ack, durableId, null);
   }
   
   private void subscribe(StompClientConnection conn, String subId,
         String ack, String durableId, String selector) throws IOException, InterruptedException
   {
      ClientStompFrame subFrame = conn.createFrame("SUBSCRIBE");
      subFrame.addHeader("id", subId);
      subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      subFrame.addHeader("ack", ack);
      if (durableId != null)
      {
         subFrame.addHeader("durable-subscriber-name", durableId);
      }
      if (selector != null)
      {
         subFrame.addHeader("selector", selector);
      }
      conn.sendFrame(subFrame);
   }

   private void subscribeTopic(StompClientConnection conn, String subId,
         String ack, String durableId) throws IOException, InterruptedException
   {
      ClientStompFrame subFrame = conn.createFrame("SUBSCRIBE");
      subFrame.addHeader("id", subId);
      subFrame.addHeader("destination", getTopicPrefix() + getTopicName());
      subFrame.addHeader("ack", ack);
      subFrame.addHeader("durable-subscriber-name", durableId);
      
      conn.sendFrame(subFrame);
   }

   private void unsubscribe(StompClientConnection conn, String subId) throws IOException, InterruptedException
   {
      ClientStompFrame subFrame = conn.createFrame("UNSUBSCRIBE");
      subFrame.addHeader("id", subId);
      
      conn.sendFrame(subFrame);
   }

}





