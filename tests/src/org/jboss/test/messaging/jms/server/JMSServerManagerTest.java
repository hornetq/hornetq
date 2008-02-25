/*
   * JBoss, Home of Professional Open Source
   * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.test.messaging.jms.server;

import org.apache.tools.ant.taskdefs.Sleep;
import org.jboss.test.messaging.JBMServerTestCase;
import org.jboss.messaging.core.impl.server.SubscriptionInfo;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.jms.server.ConnectionInfo;
import org.jboss.messaging.jms.server.JMSServerManager;

import javax.jms.*;
import javax.naming.NameNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class JMSServerManagerTest extends JBMServerTestCase
{
   JMSServerManager jmsServerManager;

   protected void setUp() throws Exception
   {
      super.setUp();
      jmsServerManager = getJmsServerManager();
   }

   public void testIsStarted()
   {
      assertTrue(jmsServerManager.isStarted());
   }


   public void testCreateAndDestroyQueue() throws Exception
   {
      jmsServerManager.createQueue("anewtestqueue", "anewtestqueue");
      Queue q = (Queue) getInitialContext().lookup("anewtestqueue");
      assertNotNull(q);
      jmsServerManager.destroyQueue("anewtestqueue");
      try
      {
         getInitialContext().lookup("anewtestqueue");
         fail("should throw eception");
      }
      catch (NameNotFoundException e)
      {
         //pass
      }
      jmsServerManager.createQueue("anewtestqueue", "/anewtestqueue");
      q = (Queue) getInitialContext().lookup("/anewtestqueue");
      assertNotNull(q);
      jmsServerManager.destroyQueue("anewtestqueue");
      try
      {
         getInitialContext().lookup("/anewtestqueue");
         fail("should throw eception");
      }
      catch (NameNotFoundException e)
      {
         //pass
      }

      jmsServerManager.createQueue("anewtestqueue", "/queues/anewtestqueue");
      getInitialContext().lookup("/queues/anewtestqueue");
      assertNotNull(q);
      jmsServerManager.destroyQueue("anewtestqueue");
      try
      {
         getInitialContext().lookup("/queues/newtestqueue");
         fail("should throw eception");
      }
      catch (NameNotFoundException e)
      {
         //pass
      }

      jmsServerManager.createQueue("anewtestqueue", "/queues/and/anewtestqueue");
      q = (Queue) getInitialContext().lookup("/queues/and/anewtestqueue");
      assertNotNull(q);
      jmsServerManager.destroyQueue("anewtestqueue");
      try
      {
         getInitialContext().lookup("/queues/and/anewtestqueue");
         fail("should throw eception");
      }
      catch (NameNotFoundException e)
      {
         //pass
      }
   }

   public void testCreateAndDestroyTopic() throws Exception
   {
      jmsServerManager.createTopic("anewtesttopic", "anewtesttopic");
      Topic q = (Topic) getInitialContext().lookup("anewtesttopic");
      assertNotNull(q);
      jmsServerManager.destroyTopic("anewtesttopic");
      try
      {
         q = (Topic) getInitialContext().lookup("anewtesttopic");
         fail("should throw eception");
      }
      catch (NameNotFoundException e)
      {
         //pass
      }
      jmsServerManager.createTopic("anewtesttopic", "/anewtesttopic");
      q = (Topic) getInitialContext().lookup("/anewtesttopic");
      assertNotNull(q);
      jmsServerManager.destroyTopic("anewtesttopic");
      try
      {
         q = (Topic) getInitialContext().lookup("/anewtesttopic");
         fail("should throw eception");
      }
      catch (NameNotFoundException e)
      {
         //pass
      }

      jmsServerManager.createTopic("anewtesttopic", "/topics/anewtesttopic");
      q = (Topic) getInitialContext().lookup("/topics/anewtesttopic");
      assertNotNull(q);
      jmsServerManager.destroyTopic("anewtesttopic");
      try
      {
         q = (Topic) getInitialContext().lookup("/topics/newtesttopic");
         fail("should throw eception");
      }
      catch (NameNotFoundException e)
      {
         //pass
      }

      jmsServerManager.createTopic("anewtesttopic", "/topics/and/anewtesttopic");
      q = (Topic) getInitialContext().lookup("/topics/and/anewtesttopic");
      assertNotNull(q);
      jmsServerManager.destroyTopic("anewtesttopic");
      try
      {
         q = (Topic) getInitialContext().lookup("/topics/and/anewtesttopic");
         fail("should throw eception");
      }
      catch (NameNotFoundException e)
      {
         //pass
      }
   }

   public void testListAllQueues()
   {
      Set<String> queueNames = jmsServerManager.listAllQueues();
      for (String queueName : queueNames)
      {
         System.out.println("queueName = " + queueName);
      }
   }

   public void testListAllTopics()
   {
      Set<String> topics = jmsServerManager.listAllTopics();
      for (String queueName : topics)
      {
         System.out.println("queueName = " + queueName);
      }
   }

   public void testCreateAndDestroyConectionFactory() throws Exception
   {
      jmsServerManager.createConnectionFactory("newtestcf", "anid", 100, true, 100, "newtestcf");
      JBossConnectionFactory jbcf = (JBossConnectionFactory) getInitialContext().lookup("newtestcf");
      assertNotNull(jbcf);
      assertNotNull(jbcf.getDelegate());
      jmsServerManager.destroyConnectionFactory("newtestcf");
      try
      {
         getInitialContext().lookup("newtestcf");
         fail("should throw exception");
      }
      catch (NameNotFoundException e)
      {
         //pass
      }
      ArrayList<String> bindings = new ArrayList<String>();
      bindings.add("oranewtestcf");
      bindings.add("newtestcf");
      jmsServerManager.createConnectionFactory("newtestcf", "anid", 100, true, 100, bindings);
      jbcf = (JBossConnectionFactory) getInitialContext().lookup("newtestcf");
      assertNotNull(jbcf);
      assertNotNull(jbcf.getDelegate());
      jbcf = (JBossConnectionFactory) getInitialContext().lookup("oranewtestcf");
      assertNotNull(jbcf);
      assertNotNull(jbcf.getDelegate());
      jmsServerManager.destroyConnectionFactory("newtestcf");
      try
      {
         getInitialContext().lookup("newtestcf");
         fail("should throw exception");
      }
      catch (NameNotFoundException e)
      {
         //pass
      }
      try
      {
         getInitialContext().lookup("oranewtestcf");
         fail("should throw exception");
      }
      catch (NameNotFoundException e)
      {
         //pass
      }
   }

   public void testGetConnections() throws Exception
   {
      Connection conn = getConnectionFactory().createConnection("guest", "guest");
      List<ConnectionInfo> connectionInfos = jmsServerManager.getConnections();
      assertNotNull(connectionInfos);
      assertEquals(1, connectionInfos.size());
      ConnectionInfo connectionInfo = connectionInfos.get(0);
      assertEquals("guest", connectionInfo.getUser());
      assertEquals(ConnectionInfo.status.STOPPED, connectionInfo.getStatus());
      conn.start();
      // starting a connection is a remoting async operation
      // wait a little before querying clients infos from the server
      sleepIfRemoting(250);
      connectionInfos = jmsServerManager.getConnections();
      assertNotNull(connectionInfos);
      assertEquals(1, connectionInfos.size());
      connectionInfo = connectionInfos.get(0);
      assertEquals(ConnectionInfo.status.STARTED, connectionInfo.getStatus());
      connectionInfo.getAddress();
      connectionInfo.getTimeCreated();
      connectionInfo.getAliveTime();
      conn.close();
      connectionInfos = jmsServerManager.getConnections();
      assertNotNull(connectionInfos);
      assertEquals(0, connectionInfos.size());
      Connection conn2 = getConnectionFactory().createConnection("guest", "guest");
      Connection conn3 = getConnectionFactory().createConnection("guest", "guest");
      connectionInfos = jmsServerManager.getConnections();
      assertNotNull(connectionInfos);
      assertEquals(2, connectionInfos.size());
      conn2.close();
      connectionInfos = jmsServerManager.getConnections();
      assertNotNull(connectionInfos);
      assertEquals(1, connectionInfos.size());
      conn3.close();
      connectionInfos = jmsServerManager.getConnections();
      assertNotNull(connectionInfos);
      assertEquals(0, connectionInfos.size());
   }

   public void testGetConnectionsForUser() throws Exception
   {
      Connection conn = getConnectionFactory().createConnection("guest", "guest");
      Connection conn2 = getConnectionFactory().createConnection();
      Connection conn3 = getConnectionFactory().createConnection();
      Connection conn4 = getConnectionFactory().createConnection("guest", "guest");
      Connection conn5 = getConnectionFactory().createConnection("guest", "guest");

      try
      {
         List<ConnectionInfo> connectionInfos = jmsServerManager.getConnectionsForUser("guest");
         assertNotNull(connectionInfos);
         assertEquals(connectionInfos.size(),3);
         for (ConnectionInfo connectionInfo : connectionInfos)
         {
            assertEquals(connectionInfo.getUser(), "guest");
         }
      }
      finally
      {
         if(conn != null)
         {
            conn.close();
         }
         if(conn2 != null)
         {
            conn2.close();
         }
         if(conn3 != null)
         {
            conn3.close();
         }
         if(conn4 != null)
         {
            conn4.close();
         }
         if(conn5 != null)
         {
            conn5.close();
         }
      }

   }

   public void testDropConnectionForId() throws Exception
   {
      Connection conn = getConnectionFactory().createConnection("guest", "guest");
      Connection conn2 = getConnectionFactory().createConnection();
      Connection conn3 = getConnectionFactory().createConnection();
      Connection conn4 = getConnectionFactory().createConnection("john", "needle");
      Connection conn5 = getConnectionFactory().createConnection("guest", "guest");
      String id = conn4.getClientID();
      try
      {

         List<ConnectionInfo> connectionInfos = jmsServerManager.getConnectionsForUser("john");
         assertEquals(connectionInfos.size(), 1);
         jmsServerManager.dropConnection(connectionInfos.get(0).getId());
         connectionInfos = jmsServerManager.getConnections();
         assertNotNull(connectionInfos);
         assertEquals(connectionInfos.size(),4);
         for (ConnectionInfo connectionInfo : connectionInfos)
         {
            assertNotSame(connectionInfo.getUser(), "john");
         }
         try
         {
            conn4.createSession(false, Session.AUTO_ACKNOWLEDGE);
            fail("Should throw exception");
         }
         catch (JMSException e)
         {
            //pass
         }
      }
      finally
      {
         if(conn != null)
         {
            conn.close();
         }
         if(conn2 != null)
         {
            conn2.close();
         }
         if(conn3 != null)
         {
            conn3.close();
         }
         if(conn5 != null)
         {
            conn5.close();
         }
      }

   }

   public void testDropConnectionForUser() throws Exception
   {
      Connection conn = getConnectionFactory().createConnection("guest", "guest");
      Connection conn2 = getConnectionFactory().createConnection();
      Connection conn3 = getConnectionFactory().createConnection();
      Connection conn4 = getConnectionFactory().createConnection("john", "needle");
      Connection conn5 = getConnectionFactory().createConnection("guest", "guest");
      String id = conn4.getClientID();
      try
      {
         jmsServerManager.dropConnectionForUser("guest");
         List<ConnectionInfo> connectionInfos = jmsServerManager.getConnections();
         assertNotNull(connectionInfos);
         assertEquals(connectionInfos.size(),3);
         for (ConnectionInfo connectionInfo : connectionInfos)
         {
            assertNotSame(connectionInfo.getUser(), "guest");
         }
         try
         {
            conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            fail("Should throw exception");
         }
         catch (JMSException e)
         {
            //pass
         }
         try
         {
            conn5.createSession(false, Session.AUTO_ACKNOWLEDGE);
            fail("Should throw exception");
         }
         catch (JMSException e)
         {
            //pass
         }
      }
      finally
      {
         if(conn2 != null)
         {
            conn2.close();
         }
         if(conn3 != null)
         {
            conn3.close();
         }
         if(conn4 != null)
         {
            conn4.close();
         }
      }

   }
   public void test() throws Exception
   {
      Connection conn = getConnectionFactory().createConnection("guest", "guest");
      Session sess = conn.createSession(true, Session.AUTO_ACKNOWLEDGE);
      sess.createConsumer(queue1);
      jmsServerManager.createQueue("Queue1", "binding");
      jmsServerManager.getConsumerCountForQueue("Queue1");
      sess.createConsumer(queue1);
      sess.createConsumer(queue1);
      sess.createConsumer(queue1);
      sess.createConsumer(queue1);
      sess.createConsumer(queue1);
      assertEquals(jmsServerManager.getConsumerCountForQueue("Queue1"), 6);
      conn.close();
      assertEquals(jmsServerManager.getConsumerCountForQueue("Queue1"), 0);
      conn = getConnectionFactory().createConnection("guest", "guest");
      sess = conn.createSession(true, Session.AUTO_ACKNOWLEDGE);
      sess.createConsumer(topic1);
      conn.close();
   }

   public void testListMessagesForQueue() throws Exception
   {
      Connection conn = getConnectionFactory().createConnection("guest", "guest");
      try
      {
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = sess.createProducer(queue1);
         producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         for (int i = 0; i < 10; i++)
         {
            TextMessage message = sess.createTextMessage();
            message.setIntProperty("count", i);
            producer.send(message);
         }
         producer.setDeliveryMode(DeliveryMode.PERSISTENT);
         for (int i = 10; i < 20; i++)
         {
            TextMessage message = sess.createTextMessage();
            message.setIntProperty("count", i);
            producer.send(message);
         }
         List<Message> messageList = jmsServerManager.listMessagesForQueue("Queue1");
         assertEquals(messageList.size(), 20);
         for (int i = 0; i < messageList.size(); i++)
         {
            Message message = messageList.get(i);
            assertEquals(message.getIntProperty("count"), i);
            assertTrue(message instanceof TextMessage);
         }
         messageList = jmsServerManager.listMessagesForQueue("Queue1", JMSServerManager.ListType.NON_DURABLE);
         assertEquals(messageList.size(), 10);
         for (int i = 0; i < messageList.size(); i++)
         {
            Message message = messageList.get(i);
            assertEquals(message.getIntProperty("count"), i);
            assertTrue(message instanceof TextMessage);
            assertTrue(message.getJMSDeliveryMode() == DeliveryMode.NON_PERSISTENT);
         }
         messageList = jmsServerManager.listMessagesForQueue("Queue1", JMSServerManager.ListType.DURABLE);
         assertEquals(messageList.size(), 10);
         for (int i = 10; i < messageList.size() + 10; i++)
         {
            Message message = messageList.get(i - 10);
            assertEquals(message.getIntProperty("count"), i);
            assertTrue(message instanceof TextMessage);
            assertTrue(message.getJMSDeliveryMode() == DeliveryMode.PERSISTENT);
         }
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void testListMessagesForSubscription() throws Exception
   {
      Connection conn = getConnectionFactory().createConnection("guest", "guest");
      try
      {
         String cid = "myclientid";
         String id = "mysubid";
         conn.setClientID(cid);
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         TopicSubscriber subscriber = sess.createDurableSubscriber(topic1, id);
         MessageProducer producer = sess.createProducer(topic1);

         producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         for (int i = 0; i < 10; i++)
         {
            TextMessage message = sess.createTextMessage();
            message.setIntProperty("count", i);
            producer.send(message);
         }
         producer.setDeliveryMode(DeliveryMode.PERSISTENT);
         for (int i = 10; i < 20; i++)
         {
            TextMessage message = sess.createTextMessage();
            message.setIntProperty("count", i);
            producer.send(message);
         }

         assertEquals(20, jmsServerManager.listMessagesForSubscription(cid + "." + id).size());
         assertEquals(10, jmsServerManager.listMessagesForSubscription(cid + "." + id, JMSServerManager.ListType.DURABLE).size());
         assertEquals(10, jmsServerManager.listMessagesForSubscription(cid + "." + id, JMSServerManager.ListType.NON_DURABLE).size());
         subscriber.close();
         sess.unsubscribe(id);
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void testRemoveMessageFromQueue() throws Exception
   {
      Connection conn = getConnectionFactory().createConnection("guest", "guest");
      try
      {
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = sess.createProducer(queue1);
         Message messageToDelete = null;
         for (int i = 0; i < 10; i++)
         {
            TextMessage message = sess.createTextMessage();
            message.setIntProperty("pos", i);
            producer.send(message);
            if (i == 5)
            {
               messageToDelete = message;
            }
         }
         jmsServerManager.removeMessageFromQueue("Queue1", messageToDelete.getJMSMessageID());
         sess = conn.createSession(true, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer consumer = sess.createConsumer(queue1);
         conn.start();
         int lastPos = -1;
         for (int i = 0; i < 9; i++)
         {
            Message message = consumer.receive();
            assertNotSame(messageToDelete.getJMSMessageID(), message.getJMSMessageID());
            int pos = message.getIntProperty("pos");
            assertTrue("returned in wrong order", pos > lastPos);
            lastPos = pos;
         }
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void testRemoveMessageFromTopic() throws Exception
   {
      Connection conn = getConnectionFactory().createConnection("guest", "guest");
      try
      {
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = sess.createProducer(topic1);
         MessageConsumer consumer = sess.createConsumer(topic1);
         MessageConsumer consumer2 = sess.createConsumer(topic1);
         Message messageToDelete = null;
         for (int i = 0; i < 10; i++)
         {
            TextMessage message = sess.createTextMessage();
            producer.send(message);
            if (i == 5)
            {
               messageToDelete = message;
            }
         }
         jmsServerManager.removeMessageFromTopic("Topic1", messageToDelete.getJMSMessageID());
         conn.start();
         for (int i = 0; i < 9; i++)
         {
            Message message = consumer.receive();
            assertNotSame(messageToDelete.getJMSMessageID(), message.getJMSMessageID());
            message = consumer2.receive();
            assertNotSame(messageToDelete.getJMSMessageID(), message.getJMSMessageID());
         }
      }
      finally
      {
         if(conn != null)
         {
            conn.close();
         }
      }

   }

   public void testRemoveAllMessagesFromQueue() throws Exception
   {
      Connection conn = getConnectionFactory().createConnection("guest", "guest");
      try
      {
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = sess.createProducer(queue1);
         for (int i = 0; i < 10; i++)
         {
            TextMessage message = sess.createTextMessage();
            producer.send(message);
         }
         jmsServerManager.removeAllMessagesForQueue("Queue1");
         sess = conn.createSession(true, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer consumer = sess.createConsumer(queue1);
         assertEquals("messages still exist", 0, jmsServerManager.getMessageCountForQueue("Queue1"));

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void testRemoveAllMessagesFromTopic() throws Exception
   {
      Connection conn = getConnectionFactory().createConnection("guest", "guest");
      try
      {
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = sess.createProducer(topic1);
         MessageConsumer consumer = sess.createConsumer(topic1);
         MessageConsumer consumer2 = sess.createConsumer(topic1);
         Message messageToDelete = null;
         for (int i = 0; i < 10; i++)
         {
            TextMessage message = sess.createTextMessage();
            producer.send(message);
            if (i == 5)
            {
               messageToDelete = message;
            }
         }
         jmsServerManager.removeAllMessagesForTopic("Topic1");
         List<SubscriptionInfo> subscriptionInfos = jmsServerManager.listSubscriptions("Topic1");
         for (SubscriptionInfo subscriptionInfo : subscriptionInfos)
         {
            assertEquals(0, jmsServerManager.listMessagesForSubscription(subscriptionInfo.getId()).size());
         }
      }
      finally
      {
         if(conn != null)
         {
            conn.close();
         }
      }

   }

   public void testMoveMessage() throws Exception
   {
      Connection conn = getConnectionFactory().createConnection("guest", "guest");
      try
      {
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = sess.createProducer(queue1);
         Message messageToMove = null;
         for (int i = 0; i < 10; i++)
         {
            TextMessage message = sess.createTextMessage();
            producer.send(message);
            if (i == 5)
            {
               messageToMove = message;
            }
         }
         jmsServerManager.moveMessage("Queue1", "Queue2", messageToMove.getJMSMessageID());
         MessageConsumer consumer = sess.createConsumer(queue1);
         conn.start();
         for (int i = 0; i < 9; i++)
         {
            Message message = consumer.receive();
            assertNotSame(messageToMove.getJMSMessageID(), message.getJMSMessageID());
         }
         consumer.close();
         consumer = sess.createConsumer(queue2);
         Message message = consumer.receive();
         assertEquals(messageToMove.getJMSMessageID(), message.getJMSMessageID());
      }
      finally
      {
         if(conn != null)
         {
            conn.close();
         }
      }

   }

   public void testExpireMessage() throws Exception
   {
      Connection conn = getConnectionFactory().createConnection("guest", "guest");
      try
      {
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue q = (Queue) getInitialContext().lookup("/queue/QueueWithOwnDLQAndExpiryQueue");
         MessageProducer producer = sess.createProducer(q);
         Message messageToMove = null;
         for (int i = 0; i < 10; i++)
         {
            TextMessage message = sess.createTextMessage();
            producer.send(message);
            if (i == 5)
            {
               messageToMove = message;
            }
         }
         jmsServerManager.expireMessage("QueueWithOwnDLQAndExpiryQueue", messageToMove.getJMSMessageID());
         MessageConsumer consumer = sess.createConsumer(q);
         conn.start();
         for (int i = 0; i < 9; i++)
         {
            Message message = consumer.receive();
            assertNotSame(messageToMove.getJMSMessageID(), message.getJMSMessageID());
         }
         consumer.close();
         Queue expQueue = (Queue) getInitialContext().lookup("/queue/PrivateExpiryQueue");
         consumer = sess.createConsumer(expQueue);
         Message message = consumer.receive();
         assertEquals(messageToMove.getJMSMessageID(), message.getJMSMessageID());
      }
      finally
      {
         if(conn != null)
         {
            conn.close();
         }
      }

   }
}
