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

package org.hornetq.tests.integration.jms.client;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.Assert;

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.jms.HornetQJMSConstants;
import org.hornetq.jms.server.impl.JMSFactoryType;
import org.hornetq.tests.util.JMSTestBase;
import org.hornetq.tests.util.UnitTestCase;

/**
 * Receive Messages and resend them, like the bridge would do
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ReSendMessageTest extends JMSTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private Queue queue;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testResendWithLargeMessage() throws Exception
   {
      Connection conn = cf.createConnection();
      try
      {
         conn.start();

         Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
         ArrayList<Message> msgs = new ArrayList<Message>();

         for (int i = 0; i < 10; i++)
         {
            BytesMessage bm = sess.createBytesMessage();
            bm.setObjectProperty(HornetQJMSConstants.JMS_HORNETQ_INPUT_STREAM,
                                 UnitTestCase.createFakeLargeStream(2 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE));
            msgs.add(bm);

            MapMessage mm = sess.createMapMessage();
            mm.setBoolean("boolean", true);
            mm.setByte("byte", (byte)3);
            mm.setBytes("bytes", new byte[] { (byte)3, (byte)4, (byte)5 });
            mm.setChar("char", (char)6);
            mm.setDouble("double", 7.0);
            mm.setFloat("float", 8.0f);
            mm.setInt("int", 9);
            mm.setLong("long", 10l);
            mm.setObject("object", new String("this is an object"));
            mm.setShort("short", (short)11);
            mm.setString("string", "this is a string");

            msgs.add(mm);
            msgs.add(sess.createTextMessage("hello" + i));
            msgs.add(sess.createObjectMessage(new SomeSerializable("hello" + i)));
         }

         internalTestResend(msgs, sess);

      }
      finally
      {
         conn.close();
      }

   }

   public void testResendWithMapMessagesOnly() throws Exception
   {
      Connection conn = cf.createConnection();
      try
      {
         conn.start();

         Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
         ArrayList<Message> msgs = new ArrayList<Message>();

         for (int i = 0; i < 1; i++)
         {
            MapMessage mm = sess.createMapMessage();
            mm.setBoolean("boolean", true);
            mm.setByte("byte", (byte)3);
            mm.setBytes("bytes", new byte[] { (byte)3, (byte)4, (byte)5 });
            mm.setChar("char", (char)6);
            mm.setDouble("double", 7.0);
            mm.setFloat("float", 8.0f);
            mm.setInt("int", 9);
            mm.setLong("long", 10l);
            mm.setObject("object", new String("this is an object"));
            mm.setShort("short", (short)11);
            mm.setString("string", "this is a string");

            msgs.add(mm);

            MapMessage emptyMap = sess.createMapMessage();
            msgs.add(emptyMap);
         }

         internalTestResend(msgs, sess);

      }
      finally
      {
         conn.close();
      }

   }

   public void internalTestResend(final ArrayList<Message> msgs, final Session sess) throws Exception
   {
      MessageProducer prod = sess.createProducer(queue);

      for (Message msg : msgs)
      {
         prod.send(msg);
      }

      sess.commit();

      MessageConsumer cons = sess.createConsumer(queue);

      for (int i = 0; i < msgs.size(); i++)
      {
         Message msg = cons.receive(5000);
         Assert.assertNotNull(msg);

         prod.send(msg);
      }

      Assert.assertNull(cons.receiveNoWait());

      sess.commit();

      for (Message originalMessage : msgs)
      {
         Message copiedMessage = cons.receive(5000);
         Assert.assertNotNull(copiedMessage);

         Assert.assertEquals(copiedMessage.getClass(), originalMessage.getClass());

         sess.commit();

         if (copiedMessage instanceof BytesMessage)
         {
            BytesMessage copiedBytes = (BytesMessage)copiedMessage;

            for (int i = 0; i < copiedBytes.getBodyLength(); i++)
            {
               Assert.assertEquals(UnitTestCase.getSamplebyte(i), copiedBytes.readByte());
            }
         }
         else if (copiedMessage instanceof MapMessage)
         {
            MapMessage copiedMap = (MapMessage)copiedMessage;
            MapMessage originalMap = (MapMessage)originalMessage;
            if (originalMap.getString("str") != null)
            {
               Assert.assertEquals(originalMap.getString("str"), copiedMap.getString("str"));
            }
            if (originalMap.getObject("long") != null)
            {
               Assert.assertEquals(originalMap.getLong("long"), copiedMap.getLong("long"));
            }
            if (originalMap.getObject("int") != null)
            {
               Assert.assertEquals(originalMap.getInt("int"), copiedMap.getInt("int"));
            }
            if (originalMap.getObject("object") != null)
            {
               Assert.assertEquals(originalMap.getObject("object"), copiedMap.getObject("object"));
            }
         }
         else if (copiedMessage instanceof ObjectMessage)
         {
            Assert.assertNotSame(((ObjectMessage)originalMessage).getObject(),
                                 ((ObjectMessage)copiedMessage).getObject());
            Assert.assertEquals(((ObjectMessage)originalMessage).getObject(),
                                ((ObjectMessage)copiedMessage).getObject());
         }
         else if (copiedMessage instanceof TextMessage)
         {
            Assert.assertEquals(((TextMessage)originalMessage).getText(), ((TextMessage)copiedMessage).getText());
         }
      }

   }

   public static class SomeSerializable implements Serializable
   {
      /**
       * 
       */
      private static final long serialVersionUID = -8576054940441747312L;

      final String txt;

      /* (non-Javadoc)
       * @see java.lang.Object#hashCode()
       */
      @Override
      public int hashCode()
      {
         final int prime = 31;
         int result = 1;
         result = prime * result + (txt == null ? 0 : txt.hashCode());
         return result;
      }

      /* (non-Javadoc)
       * @see java.lang.Object#equals(java.lang.Object)
       */
      @Override
      public boolean equals(final Object obj)
      {
         if (this == obj)
         {
            return true;
         }
         if (obj == null)
         {
            return false;
         }
         if (getClass() != obj.getClass())
         {
            return false;
         }
         SomeSerializable other = (SomeSerializable)obj;
         if (txt == null)
         {
            if (other.txt != null)
            {
               return false;
            }
         }
         else if (!txt.equals(other.txt))
         {
            return false;
         }
         return true;
      }

      SomeSerializable(final String txt)
      {
         this.txt = txt;
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   @Override
   protected void createCF(final List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs,
                           final String ... jndiBindings) throws Exception
   {
      int retryInterval = 1000;
      double retryIntervalMultiplier = 1.0;
      int reconnectAttempts = -1;
      boolean failoverOnServerShutdown = true;
      int callTimeout = 30000;

      jmsServer.createConnectionFactory("ManualReconnectionToSingleServerTest",
                                        connectorConfigs,
                                        null,
                                        HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                        HornetQClient.DEFAULT_CONNECTION_TTL,
                                        callTimeout,
                                        true,
                                        HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                        HornetQClient.DEFAULT_CONSUMER_WINDOW_SIZE,
                                        HornetQClient.DEFAULT_CONSUMER_MAX_RATE,
                                        HornetQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE,
                                        HornetQClient.DEFAULT_PRODUCER_WINDOW_SIZE,
                                        HornetQClient.DEFAULT_PRODUCER_MAX_RATE,
                                        HornetQClient.DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                                        HornetQClient.DEFAULT_BLOCK_ON_DURABLE_SEND,
                                        HornetQClient.DEFAULT_BLOCK_ON_NON_DURABLE_SEND,
                                        HornetQClient.DEFAULT_AUTO_GROUP,
                                        HornetQClient.DEFAULT_PRE_ACKNOWLEDGE,
                                        HornetQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                                        HornetQClient.DEFAULT_ACK_BATCH_SIZE,
                                        HornetQClient.DEFAULT_ACK_BATCH_SIZE,
                                        HornetQClient.DEFAULT_USE_GLOBAL_POOLS,
                                        HornetQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,
                                        HornetQClient.DEFAULT_THREAD_POOL_MAX_SIZE,
                                        retryInterval,
                                        retryIntervalMultiplier,
                                        HornetQClient.DEFAULT_MAX_RETRY_INTERVAL,
                                        reconnectAttempts,
                                        HornetQClient.DEFAULT_FAILOVER_ON_INITIAL_CONNECTION,
                                        failoverOnServerShutdown,
                                        null,
                                        JMSFactoryType.CF,
                                        jndiBindings);
   }

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      queue = createQueue("queue1");
   }

   @Override
   protected void tearDown() throws Exception
   {
      queue = null;
      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
