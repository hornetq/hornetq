/*
 * Copyright 2005-2014 Red Hat, Inc.
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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQPropertyConversionException;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.MessageHandler;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.protocol.core.impl.HornetQConsumerContext;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.spi.core.remoting.ConsumerContext;
import org.hornetq.tests.integration.IntegrationTestLogger;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.UUID;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class AcknowledgeTest extends ServiceTestBase
{
   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   public final SimpleString addressA = new SimpleString("addressA");

   public final SimpleString queueA = new SimpleString("queueA");

   public final SimpleString queueB = new SimpleString("queueB");

   public final SimpleString queueC = new SimpleString("queueC");

   @Test
   public void testReceiveAckLastMessageOnly() throws Exception
   {
      HornetQServer server = createServer(false);
      server.start();
      ServerLocator locator = createInVMNonHALocator();
      locator.setAckBatchSize(0);
      locator.setBlockOnAcknowledge(true);
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession sendSession = cf.createSession(false, true, true);
      ClientSession session = cf.createSession(false, true, true);
      sendSession.createQueue(addressA, queueA, false);
      ClientProducer cp = sendSession.createProducer(addressA);
      ClientConsumer cc = session.createConsumer(queueA);
      int numMessages = 100;
      for (int i = 0; i < numMessages; i++)
      {
         cp.send(sendSession.createMessage(false));
      }
      session.start();
      ClientMessage cm = null;
      for (int i = 0; i < numMessages; i++)
      {
         cm = cc.receive(5000);
         Assert.assertNotNull(cm);
      }
      cm.acknowledge();
      Queue q = (Queue) server.getPostOffice().getBinding(queueA).getBindable();

      Assert.assertEquals(0, q.getDeliveringCount());
      session.close();
      sendSession.close();
   }

   @Test
   public void testAsyncConsumerNoAck() throws Exception
   {
      HornetQServer server = createServer(false);

      server.start();
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession sendSession = cf.createSession(false, true, true);
      ClientSession session = cf.createSession(false, true, true);
      sendSession.createQueue(addressA, queueA, false);
      ClientProducer cp = sendSession.createProducer(addressA);
      ClientConsumer cc = session.createConsumer(queueA);
      int numMessages = 3;
      for (int i = 0; i < numMessages; i++)
      {
         cp.send(sendSession.createMessage(false));
      }

      Thread.sleep(500);
      log.info("woke up");

      final CountDownLatch latch = new CountDownLatch(numMessages);
      session.start();
      cc.setMessageHandler(new MessageHandler()
      {
         int c = 0;

         public void onMessage(final ClientMessage message)
         {
            log.info("Got message " + c++);
            latch.countDown();
         }
      });
      Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
      Queue q = (Queue) server.getPostOffice().getBinding(queueA).getBindable();
      Assert.assertEquals(numMessages, q.getDeliveringCount());
      sendSession.close();
      session.close();
   }

   @Test
   public void testAsyncConsumerAck() throws Exception
   {
      HornetQServer server = createServer(false);
      server.start();
      ServerLocator locator = createInVMNonHALocator();
      locator.setBlockOnAcknowledge(true);
      locator.setAckBatchSize(0);
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession sendSession = cf.createSession(false, true, true);
      final ClientSession session = cf.createSession(false, true, true);
      sendSession.createQueue(addressA, queueA, false);
      ClientProducer cp = sendSession.createProducer(addressA);
      ClientConsumer cc = session.createConsumer(queueA);
      int numMessages = 100;
      for (int i = 0; i < numMessages; i++)
      {
         cp.send(sendSession.createMessage(false));
      }
      final CountDownLatch latch = new CountDownLatch(numMessages);
      session.start();
      cc.setMessageHandler(new MessageHandler()
      {
         public void onMessage(final ClientMessage message)
         {
            try
            {
               message.acknowledge();
            }
            catch (HornetQException e)
            {
               try
               {
                  session.close();
               }
               catch (HornetQException e1)
               {
                  e1.printStackTrace();
               }
            }
            latch.countDown();
         }
      });
      Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
      Queue q = (Queue) server.getPostOffice().getBinding(queueA).getBindable();
      Assert.assertEquals(0, q.getDeliveringCount());
      sendSession.close();
      session.close();
   }


   /**
    * This is validating a case where a consumer will try to ack a message right after failover, but the consumer at the target server didn't
    * receive the message yet.
    * on that case the system should rollback any acks done and redeliver any messages
    */
   @Test
   public void testInvalidACK() throws Exception
   {
      HornetQServer server = createServer(false);
      server.start();

      ServerLocator locator = createInVMNonHALocator();

      locator.setAckBatchSize(0);

      locator.setBlockOnAcknowledge(true);

      ClientSessionFactory cf = createSessionFactory(locator);

      int numMessages = 100;

      ClientSession sessionConsumer = cf.createSession(true, true, 0);

      sessionConsumer.start();

      sessionConsumer.createQueue(addressA, queueA, true);

      ClientConsumer consumer = sessionConsumer.createConsumer(queueA);

      // sending message
      {
         ClientSession sendSession = cf.createSession(false, true, true);

         ClientProducer cp = sendSession.createProducer(addressA);

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage msg = sendSession.createMessage(true);
            msg.putIntProperty("seq", i);
            cp.send(msg);
         }

         sendSession.close();
      }

      {

         ClientMessage msg = consumer.receive(5000);

         // need to way some time before all the possible references are sent to the consumer
         // as we need to guarantee the order on cancellation on this test
         Thread.sleep(1000);

         try
         {
            // pretending to be an unbehaved client doing an invalid ack right after failover
            ((ClientSessionInternal) sessionConsumer).acknowledge(new FakeConsumerWithID(0), new FakeMessageWithID(12343));
            fail("supposed to throw an exception here");
         }
         catch (Exception e)
         {
         }

         try
         {
            // pretending to be an unbehaved client doing an invalid ack right after failover
            ((ClientSessionInternal) sessionConsumer).acknowledge(new FakeConsumerWithID(3), new FakeMessageWithID(12343));
            fail("supposed to throw an exception here");
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }

         consumer.close();

         consumer = sessionConsumer.createConsumer(queueA);


         for (int i = 0; i < numMessages; i++)
         {
            msg = consumer.receive(5000);
            assertNotNull(msg);
            assertEquals(i, msg.getIntProperty("seq").intValue());
            msg.acknowledge();
         }
      }
   }


   @Test
   public void testAsyncConsumerAckLastMessageOnly() throws Exception
   {
      HornetQServer server = createServer(false);
      server.start();
      ServerLocator locator = createInVMNonHALocator();
      locator.setBlockOnAcknowledge(true);
      locator.setAckBatchSize(0);
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession sendSession = cf.createSession(false, true, true);
      final ClientSession session = cf.createSession(false, true, true);
      sendSession.createQueue(addressA, queueA, false);
      ClientProducer cp = sendSession.createProducer(addressA);
      ClientConsumer cc = session.createConsumer(queueA);
      int numMessages = 100;
      for (int i = 0; i < numMessages; i++)
      {
         cp.send(sendSession.createMessage(false));
      }
      final CountDownLatch latch = new CountDownLatch(numMessages);
      session.start();
      cc.setMessageHandler(new MessageHandler()
      {
         public void onMessage(final ClientMessage message)
         {
            if (latch.getCount() == 1)
            {
               try
               {
                  message.acknowledge();
               }
               catch (HornetQException e)
               {
                  try
                  {
                     session.close();
                  }
                  catch (HornetQException e1)
                  {
                     e1.printStackTrace();
                  }
               }
            }
            latch.countDown();
         }
      });
      Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
      Queue q = (Queue) server.getPostOffice().getBinding(queueA).getBindable();
      Assert.assertEquals(0, q.getDeliveringCount());
      sendSession.close();
      session.close();
   }


   class FakeConsumerWithID implements ClientConsumer
   {
      final long id;

      FakeConsumerWithID(long id)
      {
         this.id = id;
      }

      @Override
      public ConsumerContext getConsumerContext()
      {
         return new HornetQConsumerContext(this.id);
      }

      @Override
      public ClientMessage receive() throws HornetQException
      {
         return null;
      }

      @Override
      public ClientMessage receive(long timeout) throws HornetQException
      {
         return null;
      }

      @Override
      public ClientMessage receiveImmediate() throws HornetQException
      {
         return null;
      }

      @Override
      public MessageHandler getMessageHandler() throws HornetQException
      {
         return null;
      }

      @Override
      public void setMessageHandler(MessageHandler handler) throws HornetQException
      {

      }

      @Override
      public void close() throws HornetQException
      {

      }

      @Override
      public boolean isClosed()
      {
         return false;
      }

      @Override
      public Exception getLastException()
      {
         return null;
      }
   }

   class FakeMessageWithID implements Message
   {

      final long id;

      FakeMessageWithID(final long id)
      {
         this.id = id;
      }

      @Override
      public long getMessageID()
      {
         return id;
      }

      @Override
      public UUID getUserID()
      {
         return null;
      }

      @Override
      public void setUserID(UUID userID)
      {

      }

      @Override
      public SimpleString getAddress()
      {
         return null;
      }

      @Override
      public Message setAddress(SimpleString address)
      {
         return null;
      }

      @Override
      public byte getType()
      {
         return 0;
      }

      @Override
      public boolean isDurable()
      {
         return false;
      }

      @Override
      public void setDurable(boolean durable)
      {

      }

      @Override
      public long getExpiration()
      {
         return 0;
      }

      @Override
      public boolean isExpired()
      {
         return false;
      }

      @Override
      public void setExpiration(long expiration)
      {

      }

      @Override
      public long getTimestamp()
      {
         return 0;
      }

      @Override
      public void setTimestamp(long timestamp)
      {

      }

      @Override
      public byte getPriority()
      {
         return 0;
      }

      @Override
      public void setPriority(byte priority)
      {

      }

      @Override
      public int getEncodeSize()
      {
         return 0;
      }

      @Override
      public boolean isLargeMessage()
      {
         return false;
      }

      @Override
      public HornetQBuffer getBodyBuffer()
      {
         return null;
      }

      @Override
      public HornetQBuffer getBodyBufferCopy()
      {
         return null;
      }

      @Override
      public Message putBooleanProperty(SimpleString key, boolean value)
      {
         return null;
      }

      @Override
      public Message putBooleanProperty(String key, boolean value)
      {
         return null;
      }

      @Override
      public Message putByteProperty(SimpleString key, byte value)
      {
         return null;
      }

      @Override
      public Message putByteProperty(String key, byte value)
      {
         return null;
      }

      @Override
      public Message putBytesProperty(SimpleString key, byte[] value)
      {
         return null;
      }

      @Override
      public Message putBytesProperty(String key, byte[] value)
      {
         return null;
      }

      @Override
      public Message putShortProperty(SimpleString key, short value)
      {
         return null;
      }

      @Override
      public Message putShortProperty(String key, short value)
      {
         return null;
      }

      @Override
      public Message putCharProperty(SimpleString key, char value)
      {
         return null;
      }

      @Override
      public Message putCharProperty(String key, char value)
      {
         return null;
      }

      @Override
      public Message putIntProperty(SimpleString key, int value)
      {
         return null;
      }

      @Override
      public Message putIntProperty(String key, int value)
      {
         return null;
      }

      @Override
      public Message putLongProperty(SimpleString key, long value)
      {
         return null;
      }

      @Override
      public Message putLongProperty(String key, long value)
      {
         return null;
      }

      @Override
      public Message putFloatProperty(SimpleString key, float value)
      {
         return null;
      }

      @Override
      public Message putFloatProperty(String key, float value)
      {
         return null;
      }

      @Override
      public Message putDoubleProperty(SimpleString key, double value)
      {
         return null;
      }

      @Override
      public Message putDoubleProperty(String key, double value)
      {
         return null;
      }

      @Override
      public Message putStringProperty(SimpleString key, SimpleString value)
      {
         return null;
      }

      @Override
      public Message putStringProperty(String key, String value)
      {
         return null;
      }

      @Override
      public Message putObjectProperty(SimpleString key, Object value) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public Message putObjectProperty(String key, Object value) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public Object removeProperty(SimpleString key)
      {
         return null;
      }

      @Override
      public Object removeProperty(String key)
      {
         return null;
      }

      @Override
      public boolean containsProperty(SimpleString key)
      {
         return false;
      }

      @Override
      public boolean containsProperty(String key)
      {
         return false;
      }

      @Override
      public Boolean getBooleanProperty(SimpleString key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public Boolean getBooleanProperty(String key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public Byte getByteProperty(SimpleString key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public Byte getByteProperty(String key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public Double getDoubleProperty(SimpleString key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public Double getDoubleProperty(String key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public Integer getIntProperty(SimpleString key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public Integer getIntProperty(String key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public Long getLongProperty(SimpleString key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public Long getLongProperty(String key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public Object getObjectProperty(SimpleString key)
      {
         return null;
      }

      @Override
      public Object getObjectProperty(String key)
      {
         return null;
      }

      @Override
      public Short getShortProperty(SimpleString key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public Short getShortProperty(String key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public Float getFloatProperty(SimpleString key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public Float getFloatProperty(String key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public String getStringProperty(SimpleString key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public String getStringProperty(String key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public SimpleString getSimpleStringProperty(SimpleString key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public SimpleString getSimpleStringProperty(String key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public byte[] getBytesProperty(SimpleString key) throws HornetQPropertyConversionException
      {
         return new byte[0];
      }

      @Override
      public byte[] getBytesProperty(String key) throws HornetQPropertyConversionException
      {
         return new byte[0];
      }

      @Override
      public Set<SimpleString> getPropertyNames()
      {
         return null;
      }

      @Override
      public Map<String, Object> toMap()
      {
         return null;
      }
   }
}
