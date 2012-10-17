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
package org.hornetq.tests.integration;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.client.impl.ClientConsumerImpl;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.PacketImpl;
import org.hornetq.core.protocol.core.impl.wireformat.SessionReceiveMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionSendMessage;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.tests.util.ServiceTestBase;

/**
 *
 * A InterceptorTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a> fox
 *
 *
 */
public class InterceptorTest extends ServiceTestBase
{
   private HornetQServer server;

   private final SimpleString QUEUE = new SimpleString("InterceptorTestQueue");

   private ServerLocator locator;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      server = createServer(false, true);

      server.start();

      locator = createNettyNonHALocator();
   }

   private static final String key = "fruit";

   private class MyInterceptor1 implements Interceptor
   {
      public boolean intercept(final Packet packet, final RemotingConnection connection) throws HornetQException
      {
         if (packet.getType() == PacketImpl.SESS_SEND)
         {
            SessionSendMessage p = (SessionSendMessage)packet;

            ServerMessage sm = (ServerMessage)p.getMessage();

            sm.putStringProperty(InterceptorTest.key, "orange");
         }

         return true;
      }

   }

   private class MyOutgoingInterceptor1 implements Interceptor
   {
      public boolean intercept(final Packet packet, final RemotingConnection connection) throws HornetQException
      {
         if (packet.getType() == PacketImpl.SESS_RECEIVE_MSG)
         {
            SessionReceiveMessage p = (SessionReceiveMessage)packet;

            ServerMessage sm = (ServerMessage)p.getMessage();

            sm.putStringProperty(InterceptorTest.key, "orange");
         }

         return true;
      }

   }

   private class MyInterceptor2 implements Interceptor
   {
      public boolean intercept(final Packet packet, final RemotingConnection connection) throws HornetQException
      {
         if (packet.getType() == PacketImpl.SESS_SEND)
         {
            return false;
         }

         return true;
      }

   }

   private class MyOutgoingInterceptor2 implements Interceptor
   {
      public boolean intercept(final Packet packet, final RemotingConnection connection) throws HornetQException
      {
         if (isForceDeliveryResponse(packet))
         {
            return true;
         }

         if (packet.getType() == PacketImpl.SESS_RECEIVE_MSG)
         {
            return false;
         }

         return true;
      }
   }

   private class MyInterceptor3 implements Interceptor
   {
      public boolean intercept(final Packet packet, final RemotingConnection connection) throws HornetQException
      {
         if (packet.getType() == PacketImpl.SESS_RECEIVE_MSG)
         {
            SessionReceiveMessage p = (SessionReceiveMessage)packet;

            ClientMessage cm = (ClientMessage)p.getMessage();

            cm.putStringProperty(InterceptorTest.key, "orange");
         }

         return true;
      }

   }

   private class MyOutgoingInterceptor3 implements Interceptor
   {
      public boolean intercept(final Packet packet, final RemotingConnection connection) throws HornetQException
      {
         if (packet.getType() == PacketImpl.SESS_SEND)
         {
            SessionSendMessage p = (SessionSendMessage)packet;

            ClientMessage cm = (ClientMessage)p.getMessage();

            cm.putStringProperty(InterceptorTest.key, "orange");
         }

         return true;
      }

   }

   private class MyInterceptor4 implements Interceptor
   {
      public boolean intercept(final Packet packet, final RemotingConnection connection) throws HornetQException
      {
         if (isForceDeliveryResponse(packet))
         {
            return true;
         }

         if (packet.getType() == PacketImpl.SESS_RECEIVE_MSG)
         {
            return false;
         }

         return true;
      }

   }

   private class MyOutgoingInterceptor4 implements Interceptor
   {
      public boolean intercept(final Packet packet, final RemotingConnection connection) throws HornetQException
      {
         if (isForceDeliveryResponse(packet))
         {
            return true;
         }

         if (packet.getType() == PacketImpl.SESS_SEND)
         {
            return false;
         }

         return true;
      }

   }
   /**
    * @param packet
    */
   private boolean isForceDeliveryResponse(final Packet packet)
   {
      if (packet.getType() == PacketImpl.SESS_RECEIVE_MSG)
      {
         SessionReceiveMessage msg = (SessionReceiveMessage) packet;
         if (msg.getMessage().containsProperty(ClientConsumerImpl.FORCED_DELIVERY_MESSAGE))
         {
            return true;
         }
      }

      return false;
   }

   private class MyInterceptor5 implements Interceptor
   {
      private final String key;

      private final int num;

      private volatile boolean reject;

      private volatile boolean wasCalled;

      MyInterceptor5(final String key, final int num)
      {
         this.key = key;

         this.num = num;
      }

      public void setReject(final boolean reject)
      {
         this.reject = reject;
      }

      public boolean wasCalled()
      {
         return wasCalled;
      }

      public void setWasCalled(final boolean wasCalled)
      {
         this.wasCalled = wasCalled;
      }

      public boolean intercept(final Packet packet, final RemotingConnection connection) throws HornetQException
      {
         if (packet.getType() == PacketImpl.SESS_SEND)
         {
            SessionSendMessage p = (SessionSendMessage)packet;

            ServerMessage sm = (ServerMessage)p.getMessage();

            sm.putIntProperty(key, num);

            wasCalled = true;

            return !reject;
         }

         return true;

      }

   }

   private class MyInterceptor6 implements Interceptor
   {
      private final String key;

      private final int num;

      private volatile boolean reject;

      private volatile boolean wasCalled;

      MyInterceptor6(final String key, final int num)
      {
         this.key = key;

         this.num = num;
      }

      public void setReject(final boolean reject)
      {
         this.reject = reject;
      }

      public boolean wasCalled()
      {
         return wasCalled;
      }

      public void setWasCalled(final boolean wasCalled)
      {
         this.wasCalled = wasCalled;
      }

      public boolean intercept(final Packet packet, final RemotingConnection connection) throws HornetQException
      {

         if (isForceDeliveryResponse(packet))
         {
            return true;
         }

         if (packet.getType() == PacketImpl.SESS_RECEIVE_MSG)
         {
            SessionReceiveMessage p = (SessionReceiveMessage)packet;

            Message sm = p.getMessage();

            sm.putIntProperty(key, num);

            wasCalled = true;

            return !reject;
         }

         return true;

      }

   }

   public void testServerInterceptorChangeProperty() throws Exception
   {
      MyInterceptor1 interceptor = new MyInterceptor1();

      server.getRemotingService().addInterceptor(interceptor);

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 10;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(false);

         message.putIntProperty("count", i);

         message.putStringProperty(InterceptorTest.key, "apple");

         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);

         assertNotNull(message);

         assertEquals(i, message.getIntProperty("count").intValue());

         Assert.assertEquals("orange", message.getStringProperty(InterceptorTest.key));
      }

      server.getRemotingService().removeInterceptor(interceptor);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(false);

         message.putStringProperty(InterceptorTest.key, "apple");

         producer.send(message);
      }

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);

         Assert.assertEquals("apple", message.getStringProperty(InterceptorTest.key));
      }

      session.close();
   }

   public void testServerInterceptorRejectPacket() throws Exception
   {
      MyInterceptor2 interceptor = new MyInterceptor2();

      server.getRemotingService().addInterceptor(interceptor);


      locator.setBlockOnNonDurableSend(false);

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 10;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(false);

         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      ClientMessage message = consumer.receiveImmediate();

      Assert.assertNull(message);

      session.close();
   }

   public void testClientInterceptorChangeProperty() throws Exception
   {
      ClientSessionFactory sf = createSessionFactory(locator);

      MyInterceptor3 interceptor = new MyInterceptor3();

      sf.getServerLocator().addInterceptor(interceptor);

      ClientSession session = sf.createSession(false, true, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 10;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(false);

         message.putStringProperty(InterceptorTest.key, "apple");

         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);

         Assert.assertEquals("orange", message.getStringProperty(InterceptorTest.key));
      }

      sf.getServerLocator().removeInterceptor(interceptor);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(false);

         message.putStringProperty(InterceptorTest.key, "apple");

         producer.send(message);
      }

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);

         Assert.assertEquals("apple", message.getStringProperty(InterceptorTest.key));
      }

      session.close();
   }

   public void testClientOutgoingInterceptorChangeProperty() throws Exception
   {
      ClientSessionFactory sf = createSessionFactory(locator);

      MyOutgoingInterceptor3 interceptor = new MyOutgoingInterceptor3();

      sf.getServerLocator().addOutgoingInterceptor(interceptor);

      ClientSession session = sf.createSession(false, true, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 10;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(false);

         message.putStringProperty(InterceptorTest.key, "apple");

         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);

         Assert.assertEquals("orange", message.getStringProperty(InterceptorTest.key));
      }

      sf.getServerLocator().removeOutgoingInterceptor(interceptor);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(false);

         message.putStringProperty(InterceptorTest.key, "apple");

         producer.send(message);
      }

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);

         Assert.assertEquals("apple", message.getStringProperty(InterceptorTest.key));
      }

      session.close();
   }

   public void testClientInterceptorRejectPacket() throws Exception
   {
      ClientSessionFactory sf = createSessionFactory(locator);

      MyInterceptor4 interceptor = new MyInterceptor4();

      sf.getServerLocator().addInterceptor(interceptor);

      ClientSession session = sf.createSession(false, true, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 10;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(false);

         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      ClientMessage message = consumer.receive(100);

      Assert.assertNull(message);

      session.close();
   }

   public void testClientOutgoingInterceptorRejectPacketOnNonBlockingSend() throws Exception
   {
      locator.setBlockOnNonDurableSend(false);
      ClientSessionFactory sf = createSessionFactory(locator);

      MyOutgoingInterceptor4 interceptor = new MyOutgoingInterceptor4();

      sf.getServerLocator().addOutgoingInterceptor(interceptor);

      ClientSession session = sf.createSession(false, true, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 10;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(false);

         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      ClientMessage message = consumer.receive(100);

      Assert.assertNull(message);

      session.close();
   }

   public void testClientOutgoingInterceptorRejectPacketOnBlockingSend() throws Exception
   {
      // must make the call block to exercise the right logic
      locator.setBlockOnNonDurableSend(true);
      ClientSessionFactory sf = createSessionFactory(locator);

      MyOutgoingInterceptor4 interceptor = new MyOutgoingInterceptor4();

      sf.getServerLocator().addOutgoingInterceptor(interceptor);

      ClientSession session = sf.createSession(false, true, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      ClientMessage message = session.createMessage(false);

      try
      {
         producer.send(message);
         Assert.fail();
      }
      catch (HornetQException e)
      {
         // expected exception
      }
   }

   public void testServerMultipleInterceptors() throws Exception
   {
      MyInterceptor5 interceptor1 = new MyInterceptor5("a", 1);
      MyInterceptor5 interceptor2 = new MyInterceptor5("b", 2);
      MyInterceptor5 interceptor3 = new MyInterceptor5("c", 3);
      MyInterceptor5 interceptor4 = new MyInterceptor5("d", 4);

      server.getRemotingService().addInterceptor(interceptor1);
      server.getRemotingService().addInterceptor(interceptor2);
      server.getRemotingService().addInterceptor(interceptor3);
      server.getRemotingService().addInterceptor(interceptor4);

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 10;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(false);

         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);

         Assert.assertEquals(1, message.getIntProperty("a").intValue());
         Assert.assertEquals(2, message.getIntProperty("b").intValue());
         Assert.assertEquals(3, message.getIntProperty("c").intValue());
         Assert.assertEquals(4, message.getIntProperty("d").intValue());
      }

      server.getRemotingService().removeInterceptor(interceptor2);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(false);

         producer.send(message);
      }

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);

         Assert.assertEquals(1, message.getIntProperty("a").intValue());
         Assert.assertFalse(message.containsProperty("b"));
         Assert.assertEquals(3, message.getIntProperty("c").intValue());
         Assert.assertEquals(4, message.getIntProperty("d").intValue());

      }

      interceptor3.setReject(true);

      interceptor1.setWasCalled(false);
      interceptor2.setWasCalled(false);
      interceptor3.setWasCalled(false);
      interceptor4.setWasCalled(false);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(false);

         producer.send(message);
      }

      ClientMessage message = consumer.receiveImmediate();

      Assert.assertNull(message);

      Assert.assertTrue(interceptor1.wasCalled());
      Assert.assertFalse(interceptor2.wasCalled());
      Assert.assertTrue(interceptor3.wasCalled());
      Assert.assertFalse(interceptor4.wasCalled());

      session.close();
   }

   public void testClientMultipleInterceptors() throws Exception
   {
      MyInterceptor6 interceptor1 = new MyInterceptor6("a", 1);
      MyInterceptor6 interceptor2 = new MyInterceptor6("b", 2);
      MyInterceptor6 interceptor3 = new MyInterceptor6("c", 3);
      MyInterceptor6 interceptor4 = new MyInterceptor6("d", 4);

      ClientSessionFactory sf = createSessionFactory(locator);

      sf.getServerLocator().addInterceptor(interceptor1);
      sf.getServerLocator().addInterceptor(interceptor2);
      sf.getServerLocator().addInterceptor(interceptor3);
      sf.getServerLocator().addInterceptor(interceptor4);

      ClientSession session = sf.createSession(false, true, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 10;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(false);

         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);

         Assert.assertEquals(1, message.getIntProperty("a").intValue());
         Assert.assertEquals(2, message.getIntProperty("b").intValue());
         Assert.assertEquals(3, message.getIntProperty("c").intValue());
         Assert.assertEquals(4, message.getIntProperty("d").intValue());
      }

      sf.getServerLocator().removeInterceptor(interceptor2);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(false);

         producer.send(message);
      }

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);

         Assert.assertEquals(1, message.getIntProperty("a").intValue());
         Assert.assertFalse(message.containsProperty("b"));
         Assert.assertEquals(3, message.getIntProperty("c").intValue());
         Assert.assertEquals(4, message.getIntProperty("d").intValue());

      }

      interceptor3.setReject(true);

      interceptor1.setWasCalled(false);
      interceptor2.setWasCalled(false);
      interceptor3.setWasCalled(false);
      interceptor4.setWasCalled(false);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(false);

         producer.send(message);
      }

      ClientMessage message = consumer.receive(100);

      Assert.assertNull(message);

      Assert.assertTrue(interceptor1.wasCalled());
      Assert.assertFalse(interceptor2.wasCalled());
      Assert.assertTrue(interceptor3.wasCalled());
      Assert.assertFalse(interceptor4.wasCalled());

      session.close();
   }

   public void testServerOutgoingInterceptorChangeProperty() throws Exception
   {
      MyOutgoingInterceptor1 interceptor = new MyOutgoingInterceptor1();

      server.getRemotingService().addOutgoingInterceptor(interceptor);

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 10;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(false);

         message.putIntProperty("count", i);

         message.putStringProperty(InterceptorTest.key, "apple");

         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);

         assertNotNull(message);

         assertEquals(i, message.getIntProperty("count").intValue());

         Assert.assertEquals("orange", message.getStringProperty(InterceptorTest.key));
      }

      server.getRemotingService().removeOutgoingInterceptor(interceptor);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(false);

         message.putStringProperty(InterceptorTest.key, "apple");

         producer.send(message);
      }

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);

         Assert.assertEquals("apple", message.getStringProperty(InterceptorTest.key));
      }

      session.close();
   }

   public void testServerOutgoingInterceptorRejectMessage() throws Exception
   {
      MyOutgoingInterceptor2 interceptor = new MyOutgoingInterceptor2();

      server.getRemotingService().addOutgoingInterceptor(interceptor);

      locator.setBlockOnNonDurableSend(false);

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 10;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(false);

         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      ClientMessage message = consumer.receiveImmediate();

      Assert.assertNull(message);

      session.close();
   }
}
