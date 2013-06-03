/*
 * Copyright 2010 Red Hat, Inc.
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
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.hornetq.api.core.HornetQNotConnectedException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.core.client.impl.DelegatingSession;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.client.HornetQMessage;
import org.hornetq.jms.client.HornetQSession;
import org.hornetq.jms.client.HornetQTextMessage;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.tests.util.JMSTestBase;

import java.util.Iterator;

/**
 * GroupingTest
 *
 * @author Tim Fox
 *
 */
public class GroupingTest extends JMSTestBase
{
   private Queue queue;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      queue = createQueue("TestQueue");
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      jmsServer.destroyQueue("TestQueue");

      super.tearDown();
   }

   protected void setProperty(Message message)
   {
      ((HornetQMessage)message).getCoreMessage().putStringProperty(org.hornetq.api.core.Message.HDR_GROUP_ID, new SimpleString("foo"));
   }

   protected ConnectionFactory getCF() throws Exception
   {
      return cf;
   }

   @Test
   public void testGrouping() throws Exception
   {
      ConnectionFactory fact = getCF();

      Connection connection = fact.createConnection();

      Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

      MessageProducer producer = session.createProducer(queue);

      MessageConsumer consumer1 = session.createConsumer(queue);
      MessageConsumer consumer2 = session.createConsumer(queue);
      MessageConsumer consumer3 = session.createConsumer(queue);

      connection.start();

      String jmsxgroupID = null;

      for (int j = 0; j < 100; j++)
      {
         TextMessage message = session.createTextMessage();

         message.setText("Message" + j);

         setProperty(message);

         producer.send(message);

         String prop = message.getStringProperty("JMSXGroupID");

         assertNotNull(prop);

         if (jmsxgroupID != null)
         {
            assertEquals(jmsxgroupID, prop);
         }
         else
         {
            jmsxgroupID = prop;
         }
      }

      //All msgs should go to the first consumer
      for (int j = 0; j < 100; j++)
      {
         TextMessage tm = (TextMessage)consumer1.receive(10000);

         assertNotNull(tm);

         assertEquals("Message" + j, tm.getText());

         assertEquals(tm.getStringProperty("JMSXGroupID"), jmsxgroupID);
      }

      connection.close();


   }


   @Test
   public void testManyGroups() throws Exception
   {
      ConnectionFactory fact = getCF();

      Connection connection = fact.createConnection();

      Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

      MessageProducer producer = session.createProducer(queue);

      MessageConsumer consumer1 = session.createConsumer(queue);
      MessageConsumer consumer2 = session.createConsumer(queue);
      MessageConsumer consumer3 = session.createConsumer(queue);

      connection.start();

      for (int j = 0; j < 1000; j++)
      {
         TextMessage message = session.createTextMessage();

         message.setText("Message" + j);

         message.setStringProperty("_HQ_GROUP_ID", "" + (j % 10));

         producer.send(message);

         String prop = message.getStringProperty("JMSXGroupID");

         assertNotNull(prop);

      }

      int msg1 = flushMessages(consumer1);
      int msg2 = flushMessages(consumer2);
      int msg3 = flushMessages(consumer3);


      assertNotSame(0, msg1);
      assertNotSame(0, msg2);
      assertNotSame(0, msg2);

      consumer1.close();
      consumer2.close();
      consumer3.close();


      connection.close();


   }

   @Test
   public void testGroupingRollbackOnClose() throws Exception
   {
      HornetQConnectionFactory fact = (HornetQConnectionFactory) getCF();
      fact.setConsumerWindowSize(1000);
      fact.setTransactionBatchSize(0);
      Connection connection = fact.createConnection();
      RemotingConnection rc = server.getRemotingService().getConnections().iterator().next();
      Connection connection2 = fact.createConnection();

      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
      Session session2 = connection2.createSession(true, Session.SESSION_TRANSACTED);

      MessageProducer producer = session.createProducer(queue);

      MessageConsumer consumer1 = session.createConsumer(queue);
      MessageConsumer consumer2 = session2.createConsumer(queue);

      connection.start();
      connection2.start();

      String jmsxgroupID = null;

      for (int j = 0; j < 100; j++)
      {
         TextMessage message = session.createTextMessage();

         message.setText("Message" + j);

         setProperty(message);

         producer.send(message);

         String prop = message.getStringProperty("JMSXGroupID");

         assertNotNull(prop);

         if (jmsxgroupID != null)
         {
            assertEquals(jmsxgroupID, prop);
         }
         else
         {
            jmsxgroupID = prop;
         }
      }
      session.commit();
      //consume 5 msgs from 1st first consumer
      for (int j = 0; j < 1; j++)
      {
         TextMessage tm = (TextMessage)consumer1.receive(10000);

         assertNotNull(tm);

         assertEquals("Message" + j, tm.getText());

         assertEquals(tm.getStringProperty("JMSXGroupID"), jmsxgroupID);
      }
      Thread.sleep(2000);
      //session.rollback();
      //session.close();
      //consume all msgs from 2nd first consumer
     // ClientSession hqs = ((HornetQSession) session).getCoreSession();
    //  ((DelegatingSession) hqs).getChannel().close();
      rc.fail(new HornetQNotConnectedException());
      for (int j = 0; j < 10; j++)
      {
         TextMessage tm = (TextMessage)consumer2.receive(10000);

         assertNotNull(tm);

         long text = ((HornetQTextMessage) tm).getCoreMessage().getMessageID();
         System.out.println(tm.getJMSMessageID() + " text = " + text);
         //assertEquals("Message" + j, text);

         assertEquals(tm.getStringProperty("JMSXGroupID"), jmsxgroupID);
      }

      connection.close();
      connection2.close();
   }

   private int flushMessages(MessageConsumer consumer) throws JMSException
   {
      int received = 0;
      while (true)
      {
         TextMessage msg = (TextMessage)consumer.receiveNoWait();
         if (msg == null)
         {
            break;
         }
         msg.acknowledge();
         received++;
      }
      return received;
   }

}
