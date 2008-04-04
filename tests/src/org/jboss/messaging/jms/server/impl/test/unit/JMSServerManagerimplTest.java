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
package org.jboss.messaging.jms.server.impl.test.unit;

import junit.framework.TestCase;
import org.jboss.messaging.jms.server.JMSServerManager;
import org.jboss.messaging.jms.server.ConnectionInfo;
import org.jboss.messaging.jms.server.SubscriptionInfo;
import org.jboss.messaging.jms.server.MessageStatistics;
import org.jboss.messaging.jms.server.impl.JMSServerManagerImpl;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.impl.MessagingServerImpl;
import org.jboss.messaging.core.management.impl.MessagingServerManagementImpl;
import static org.jboss.messaging.core.remoting.TransportType.INVM;
import org.jboss.messaging.core.security.JBMUpdateableSecurityManager;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.test.messaging.tools.container.Constants;
import org.jboss.test.messaging.tools.container.InVMInitialContextFactory;

import javax.jms.*;
import javax.naming.NameNotFoundException;
import javax.naming.InitialContext;
import java.util.Hashtable;
import java.util.Set;
import java.util.ArrayList;
import java.util.List;

/**
 * JMSServerManagerImpl tests
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class JMSServerManagerimplTest extends TestCase
{
   private JMSServerManagerImpl jmsServerManager;
   private InitialContext initialContext;
   private MessagingServer messagingServer;

   protected void setUp() throws Exception
   {
      jmsServerManager = new JMSServerManagerImpl();
      MessagingServerManagementImpl messagingServerManagement = new MessagingServerManagementImpl();
      ConfigurationImpl conf = new ConfigurationImpl();
      conf.setInvmDisabled(false);
      conf.setTransport(INVM);
      messagingServer = new MessagingServerImpl(conf);
      messagingServer.start();
      jmsServerManager.setMessagingServerManagement(messagingServerManagement);
      messagingServerManagement.setMessagingServer(messagingServer);
      Hashtable env = new Hashtable();
      env.put("java.naming.factory.initial",
              "org.jboss.test.messaging.tools.container.InVMSingleInitialContextFactory");
      initialContext = new InitialContext(env);
      jmsServerManager.setInitialContext(initialContext);
   }

   protected void tearDown() throws Exception
   {
      //InVMInitialContextFactory.reset();
      messagingServer.stop();
      jmsServerManager = null;
      messagingServer = null;
   }

   public void testIsStarted()
   {
      assertTrue(jmsServerManager.isStarted());
   }

   public void testCreateAndDestroyQueue() throws Exception
   {
      jmsServerManager.createQueue("anewtestqueue", "anewtestqueue");
      Queue q = (Queue) initialContext.lookup("anewtestqueue");
      assertNotNull(q);
      jmsServerManager.destroyQueue("anewtestqueue");
      try
      {
         initialContext.lookup("anewtestqueue");
         fail("should throw eception");
      }
      catch (NameNotFoundException e)
      {
         //pass
      }
      jmsServerManager.createQueue("anewtestqueue", "/anewtestqueue");
      q = (Queue) initialContext.lookup("/anewtestqueue");
      assertNotNull(q);
      jmsServerManager.destroyQueue("anewtestqueue");
      try
      {
         initialContext.lookup("/anewtestqueue");
         fail("should throw eception");
      }
      catch (NameNotFoundException e)
      {
         //pass
      }

      jmsServerManager.createQueue("anewtestqueue", "/queues/anewtestqueue");
      initialContext.lookup("/queues/anewtestqueue");
      assertNotNull(q);
      jmsServerManager.destroyQueue("anewtestqueue");
      try
      {
         initialContext.lookup("/queues/newtestqueue");
         fail("should throw eception");
      }
      catch (NameNotFoundException e)
      {
         //pass
      }

      jmsServerManager.createQueue("anewtestqueue", "/queues/and/anewtestqueue");
      q = (Queue) initialContext.lookup("/queues/and/anewtestqueue");
      assertNotNull(q);
      jmsServerManager.destroyQueue("anewtestqueue");
      try
      {
         initialContext.lookup("/queues/and/anewtestqueue");
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
      Topic q = (Topic) initialContext.lookup("anewtesttopic");
      assertNotNull(q);
      jmsServerManager.destroyTopic("anewtesttopic");
      try
      {
         q = (Topic) initialContext.lookup("anewtesttopic");
         fail("should throw eception");
      }
      catch (NameNotFoundException e)
      {
         //pass
      }
      jmsServerManager.createTopic("anewtesttopic", "/anewtesttopic");
      q = (Topic) initialContext.lookup("/anewtesttopic");
      assertNotNull(q);
      jmsServerManager.destroyTopic("anewtesttopic");
      try
      {
         q = (Topic) initialContext.lookup("/anewtesttopic");
         fail("should throw eception");
      }
      catch (NameNotFoundException e)
      {
         //pass
      }

      jmsServerManager.createTopic("anewtesttopic", "/topics/anewtesttopic");
      q = (Topic) initialContext.lookup("/topics/anewtesttopic");
      assertNotNull(q);
      jmsServerManager.destroyTopic("anewtesttopic");
      try
      {
         q = (Topic) initialContext.lookup("/topics/newtesttopic");
         fail("should throw eception");
      }
      catch (NameNotFoundException e)
      {
         //pass
      }

      jmsServerManager.createTopic("anewtesttopic", "/topics/and/anewtesttopic");
      q = (Topic) initialContext.lookup("/topics/and/anewtesttopic");
      assertNotNull(q);
      jmsServerManager.destroyTopic("anewtesttopic");
      try
      {
         q = (Topic) initialContext.lookup("/topics/and/anewtesttopic");
         fail("should throw eception");
      }
      catch (NameNotFoundException e)
      {
         //pass
      }
   }

   public void testListAllQueues() throws Exception
   {
      ArrayList queuesAdded = new ArrayList();
      for(int i = 0; i < 100; i++)
      {
         jmsServerManager.createQueue("aq" + i, "/aq"+ i);
         queuesAdded.add("aq" + i);
      }
      Set<String> queueNames = jmsServerManager.listAllQueues();
      for (Object o : queuesAdded)
      {
         assertTrue(queueNames.remove(o));
      }
      assertTrue(queueNames.isEmpty());
   }

   public void testListAllTopics() throws Exception
   {
      ArrayList topicsAdded = new ArrayList();
      for(int i = 0; i < 100; i++)
      {
         jmsServerManager.createTopic("at" + i, "/at"+ i);
         topicsAdded.add("at" + i);
      }
      Set<String> topicNames = jmsServerManager.listAllTopics();
      for (Object o : topicsAdded)
      {
         assertTrue(topicNames.remove(o));
      }
      assertTrue(topicNames.isEmpty());
   }

   public void testCreateAndDestroyConnectionFactory() throws Exception
   {
      jmsServerManager.createConnectionFactory("newtestcf", "anid", 100, true, 1000, -1, 1000, -1, "newtestcf");
      JBossConnectionFactory jbcf = (JBossConnectionFactory) initialContext.lookup("newtestcf");
      assertNotNull(jbcf);
      assertNotNull(jbcf.getDelegate());
      jmsServerManager.destroyConnectionFactory("newtestcf");
      try
      {
         initialContext.lookup("newtestcf");
         fail("should throw exception");
      }
      catch (NameNotFoundException e)
      {
         //pass
      }
      ArrayList<String> bindings = new ArrayList<String>();
      bindings.add("oranewtestcf");
      bindings.add("newtestcf");
      jmsServerManager.createConnectionFactory("newtestcf", "anid", 100, true, 1000, -1, 1000, -1, bindings);
      jbcf = (JBossConnectionFactory) initialContext.lookup("newtestcf");
      assertNotNull(jbcf);
      assertNotNull(jbcf.getDelegate());
      jbcf = (JBossConnectionFactory) initialContext.lookup("oranewtestcf");
      assertNotNull(jbcf);
      assertNotNull(jbcf.getDelegate());
      jmsServerManager.destroyConnectionFactory("newtestcf");
      try
      {
         initialContext.lookup("newtestcf");
         fail("should throw exception");
      }
      catch (NameNotFoundException e)
      {
         //pass
      }
      try
      {
         initialContext.lookup("oranewtestcf");
         fail("should throw exception");
      }
      catch (NameNotFoundException e)
      {
         //pass
      }
   }

   public void testGetConnections() throws Exception
   {
      ArrayList<String> bindings = new ArrayList<String>();
      bindings.add("cf");
      jmsServerManager.createConnectionFactory("newtestcf", "anid", 100, true, 1000, -1, 1000, -1, bindings );
      JBossConnectionFactory connectionFactory = (JBossConnectionFactory) initialContext.lookup("cf");

      Connection conn = connectionFactory.createConnection("guest", "guest");
      List<ConnectionInfo> connectionInfos = jmsServerManager.getConnections();
      assertNotNull(connectionInfos);
      assertEquals(1, connectionInfos.size());
      ConnectionInfo connectionInfo = connectionInfos.get(0);
      assertEquals("guest", connectionInfo.getUser());
      assertEquals(ConnectionInfo.status.STOPPED, connectionInfo.getStatus());
      conn.start();
      // starting a connection is a remoting async operation
      // wait a little before querying clients infos from the server
      //sleepIfRemoting(250);
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
      Connection conn2 = connectionFactory.createConnection("guest", "guest");
      Connection conn3 = connectionFactory.createConnection("guest", "guest");
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
      ArrayList<String> bindings = new ArrayList<String>();
      bindings.add("cf");
      jmsServerManager.createConnectionFactory("newtestcf", "anid", 100, true, 1000, -1, 1000, -1, bindings );
      JBossConnectionFactory connectionFactory = (JBossConnectionFactory) initialContext.lookup("cf");

      Connection conn = connectionFactory.createConnection("guest", "guest");
      Connection conn2 = connectionFactory.createConnection();
      Connection conn3 = connectionFactory.createConnection();
      Connection conn4 = connectionFactory.createConnection("guest", "guest");
      Connection conn5 = connectionFactory.createConnection("guest", "guest");

      try
      {
         List<ConnectionInfo> connectionInfos = jmsServerManager.getConnectionsForUser("guest");
         assertNotNull(connectionInfos);
         assertEquals(connectionInfos.size(), 3);
         for (ConnectionInfo connectionInfo : connectionInfos)
         {
            assertEquals(connectionInfo.getUser(), "guest");
         }
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
         if (conn3 != null)
         {
            conn3.close();
         }
         if (conn4 != null)
         {
            conn4.close();
         }
         if (conn5 != null)
         {
            conn5.close();
         }
      }

   }

   public void testDropConnectionForId() throws Exception
   {
      JBMUpdateableSecurityManager securityManager = (JBMUpdateableSecurityManager) messagingServer.getSecurityManager();
      securityManager.addUser("john", "needle");
      ArrayList<String> bindings = new ArrayList<String>();
      bindings.add("cf");
      jmsServerManager.createConnectionFactory("newtestcf", "anid", 100, true, 1000, -1, 1000, -1, bindings );
      JBossConnectionFactory connectionFactory = (JBossConnectionFactory) initialContext.lookup("cf");

      Connection conn = connectionFactory.createConnection("guest", "guest");
      Connection conn2 = connectionFactory.createConnection();
      Connection conn3 = connectionFactory.createConnection();
      Connection conn4 = connectionFactory.createConnection("john", "needle");
      Connection conn5 = connectionFactory.createConnection("guest", "guest");
      String id = conn4.getClientID();
      try
      {

         List<ConnectionInfo> connectionInfos = jmsServerManager.getConnectionsForUser("john");
         assertEquals(connectionInfos.size(), 1);
         jmsServerManager.dropConnection(connectionInfos.get(0).getId());
         connectionInfos = jmsServerManager.getConnections();
         assertNotNull(connectionInfos);
         assertEquals(connectionInfos.size(), 4);
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
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
         if (conn3 != null)
         {
            conn3.close();
         }
         if (conn5 != null)
         {
            conn5.close();
         }
      }

   }

   public void testDropConnectionForUser() throws Exception
   {
      JBMUpdateableSecurityManager securityManager = (JBMUpdateableSecurityManager) messagingServer.getSecurityManager();
      securityManager.addUser("john", "needle");
      ArrayList<String> bindings = new ArrayList<String>();
      bindings.add("cf");
      jmsServerManager.createConnectionFactory("newtestcf", "anid", 100, true, 1000, -1, 1000, -1, bindings );
      JBossConnectionFactory connectionFactory = (JBossConnectionFactory) initialContext.lookup("cf");

      Connection conn = connectionFactory.createConnection("guest", "guest");
      Connection conn2 = connectionFactory.createConnection();
      Connection conn3 = connectionFactory.createConnection();
      Connection conn4 = connectionFactory.createConnection("john", "needle");
      Connection conn5 = connectionFactory.createConnection("guest", "guest");
      String id = conn4.getClientID();
      try
      {
         jmsServerManager.dropConnectionsForUser("guest");
         List<ConnectionInfo> connectionInfos = jmsServerManager.getConnections();
         assertNotNull(connectionInfos);
         assertEquals(connectionInfos.size(), 3);
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
         if (conn2 != null)
         {
            conn2.close();
         }
         if (conn3 != null)
         {
            conn3.close();
         }
         if (conn4 != null)
         {
            conn4.close();
         }
      }

   }

   public void testListMessagesForQueue() throws Exception
   {
      ArrayList<String> bindings = new ArrayList<String>();
      bindings.add("cf");
      jmsServerManager.createConnectionFactory("newtestcf", "anid", 100, true, 1000, -1, 1000, -1, bindings );
      JBossConnectionFactory connectionFactory = (JBossConnectionFactory) initialContext.lookup("cf");
      jmsServerManager.createQueue("Queue1", "/queue1");
      Queue queue1 = (Queue) initialContext.lookup("/queue1");
      Connection conn = connectionFactory.createConnection("guest", "guest");
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

   //   public void testRemoveMessageFromQueue() throws Exception
//   {
//      Connection conn = getConnectionFactory().createConnection("guest", "guest");
//      try
//      {
//         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
//         MessageProducer producer = sess.createProducer(queue1);
//         Message messageToDelete = null;
//         for (int i = 0; i < 10; i++)
//         {
//            TextMessage message = sess.createTextMessage();
//            message.setIntProperty("pos", i);
//            producer.send(message);
//            if (i == 5)
//            {
//               messageToDelete = message;
//            }
//         }
//         jmsServerManager.removeMessageFromQueue("Queue1", messageToDelete.getJMSMessageID());
//         sess = conn.createSession(true, Session.AUTO_ACKNOWLEDGE);
//         MessageConsumer consumer = sess.createConsumer(queue1);
//         conn.start();
//         int lastPos = -1;
//         for (int i = 0; i < 9; i++)
//         {
//            Message message = consumer.receive();
//            assertNotSame(messageToDelete.getJMSMessageID(), message.getJMSMessageID());
//            int pos = message.getIntProperty("pos");
//            assertTrue("returned in wrong order", pos > lastPos);
//            lastPos = pos;
//         }
//      }
//      finally
//      {
//         if (conn != null)
//         {
//            conn.close();
//         }
//      }
//   }

//   public void testRemoveMessageFromTopic() throws Exception
//   {
//      Connection conn = getConnectionFactory().createConnection("guest", "guest");
//      try
//      {
//         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
//         MessageProducer producer = sess.createProducer(topic1);
//         MessageConsumer consumer = sess.createConsumer(topic1);
//         MessageConsumer consumer2 = sess.createConsumer(topic1);
//         Message messageToDelete = null;
//         for (int i = 0; i < 10; i++)
//         {
//            TextMessage message = sess.createTextMessage();
//            producer.send(message);
//            if (i == 5)
//            {
//               messageToDelete = message;
//            }
//         }
//         jmsServerManager.removeMessageFromTopic("Topic1", messageToDelete.getJMSMessageID());
//         conn.start();
//         for (int i = 0; i < 9; i++)
//         {
//            Message message = consumer.receive();
//            assertNotSame(messageToDelete.getJMSMessageID(), message.getJMSMessageID());
//            message = consumer2.receive();
//            assertNotSame(messageToDelete.getJMSMessageID(), message.getJMSMessageID());
//         }
//      }
//      finally
//      {
//         if (conn != null)
//         {
//            conn.close();
//         }
//      }
//
//   }

//   public void testRemoveAllMessagesFromQueue() throws Exception
//   {
//      Connection conn = getConnectionFactory().createConnection("guest", "guest");
//
//      ServerManagement.getServer(0).createQueue("myQueue", null);
//
//      Queue queue = (Queue)this.getInitialContext().lookup("/queue/myQueue");
//
//      try
//      {
//         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
//         MessageProducer producer = sess.createProducer(queue);
//         for (int i = 0; i < 10; i++)
//         {
//            TextMessage message = sess.createTextMessage();
//            producer.send(message);
//         }
//         jmsServerManager.removeAllMessagesForQueue("myQueue");
//         sess = conn.createSession(true, Session.AUTO_ACKNOWLEDGE);
//         MessageConsumer consumer = sess.createConsumer(queue);
//         assertEquals("messages still exist", 0, jmsServerManager.getMessageCountForQueue("myQueue"));
//
//      }
//      finally
//      {
//         if (conn != null)
//         {
//            conn.close();
//         }
//
//         try
//         {
//         	ServerManagement.getServer(0).destroyQueue("myQueue", null);
//         }
//         catch (Exception ignore)
//         {
//         }
//      }
//   }

   public void testListMessagesForSubscription() throws Exception
   {
      ArrayList<String> bindings = new ArrayList<String>();
      bindings.add("cf");
      jmsServerManager.createConnectionFactory("newtestcf", null, 100, true, 1000, -1, 1000, -1, bindings );
      JBossConnectionFactory connectionFactory = (JBossConnectionFactory) initialContext.lookup("cf");
      jmsServerManager.createTopic("topic1", "/topic1");
      Topic topic1 = (Topic) initialContext.lookup("/topic1");
      Connection conn = connectionFactory.createConnection("guest", "guest");
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

//   public void testMoveMessage() throws Exception
//   {
//      Connection conn = getConnectionFactory().createConnection("guest", "guest");
//      try
//      {
//         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
//         MessageProducer producer = sess.createProducer(queue1);
//         Message messageToMove = null;
//         for (int i = 0; i < 10; i++)
//         {
//            TextMessage message = sess.createTextMessage();
//            producer.send(message);
//            if (i == 5)
//            {
//               messageToMove = message;
//            }
//         }
//         jmsServerManager.moveMessage("Queue1", "Queue2", messageToMove.getJMSMessageID());
//         MessageConsumer consumer = sess.createConsumer(queue1);
//         conn.start();
//         for (int i = 0; i < 9; i++)
//         {
//            Message message = consumer.receive();
//            assertNotSame(messageToMove.getJMSMessageID(), message.getJMSMessageID());
//         }
//         consumer.close();
//         consumer = sess.createConsumer(queue2);
//         Message message = consumer.receive();
//         assertEquals(messageToMove.getJMSMessageID(), message.getJMSMessageID());
//      }
//      finally
//      {
//         if (conn != null)
//         {
//            conn.close();
//         }
//      }
//
//   }


   public void testRemoveAllMessagesFromTopic() throws Exception
   {
      ArrayList<String> bindings = new ArrayList<String>();
      bindings.add("cf");
      jmsServerManager.createConnectionFactory("newtestcf", "anid", 100, true, 1000, -1, 1000, -1, bindings );
      JBossConnectionFactory connectionFactory = (JBossConnectionFactory) initialContext.lookup("cf");
      Connection conn = connectionFactory.createConnection("guest", "guest");
      jmsServerManager.createTopic("topic1", "/topic1");
      Topic topic1 = (Topic) initialContext.lookup("/topic1");
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
         if (conn != null)
         {
            conn.close();
         }
      }

   }

//   public void testChangeMessagePriority() throws Exception
//   {
//      Connection conn = getConnectionFactory().createConnection("guest", "guest");
//      try
//      {
//         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
//         MessageProducer producer = sess.createProducer(queue1);
//         producer.setPriority(9);
//         Message messageToMove = null;
//         for (int i = 0; i < 10; i++)
//         {
//            TextMessage message = sess.createTextMessage();
//
//            producer.send(message);
//            if (i == 5)
//            {
//               messageToMove = message;
//            }
//         }
//         jmsServerManager.changeMessagePriority("Queue1", messageToMove.getJMSMessageID(), 8);
//         MessageConsumer consumer = sess.createConsumer(queue1);
//         conn.start();
//         for (int i = 0; i < 9; i++)
//         {
//            Message message = consumer.receive();
//            assertNotSame(messageToMove.getJMSMessageID(), message.getJMSMessageID());
//            System.out.println("message.getJMSPriority() = " + message.getJMSPriority());
//            assertEquals(9, message.getJMSPriority());
//         }
//         Message message = consumer.receive();
//         assertEquals(8, message.getJMSPriority());
//         assertEquals(messageToMove.getJMSMessageID(), message.getJMSMessageID());
//
//         consumer.close();
//
//      }
//      finally
//      {
//         if (conn != null)
//         {
//            conn.close();
//         }
//      }
//
//   }

   
   public void testExpireMessage() throws Exception
   {
      ArrayList<String> bindings = new ArrayList<String>();
      bindings.add("cf");
      jmsServerManager.createConnectionFactory("newtestcf", "anid", 100, true, 1000, -1, 1000, -1, bindings );
      JBossConnectionFactory connectionFactory = (JBossConnectionFactory) initialContext.lookup("cf");
      Connection conn = connectionFactory.createConnection("guest", "guest");
      jmsServerManager.createQueue("QueueWithOwnDLQAndExpiryQueue", "/queue/QueueWithOwnDLQAndExpiryQueue");
      jmsServerManager.createQueue("PrivateExpiryQueue", "/queue/PrivateExpiryQueue");
      
      try
      {
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue q = (Queue) initialContext.lookup("/queue/QueueWithOwnDLQAndExpiryQueue");
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
         Queue expQueue = (Queue) initialContext.lookup("/queue/PrivateExpiryQueue");
         consumer = sess.createConsumer(expQueue);
         Message message = consumer.receive();
         assertEquals(messageToMove.getJMSMessageID(), message.getJMSMessageID());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }

   }

   public void testMessageStatistics() throws Exception
   {
      ArrayList<String> bindings = new ArrayList<String>();
      bindings.add("cf");
      jmsServerManager.createConnectionFactory("newtestcf", "anid", 100, true, 1000, -1, 1000, -1, bindings );
      JBossConnectionFactory connectionFactory = (JBossConnectionFactory) initialContext.lookup("cf");
      Connection conn = connectionFactory.createConnection("guest", "guest");
      try
      {
         jmsServerManager.createQueue("CountQueue", "/queue/CountQueue");
         Queue queue1 = (Queue) initialContext.lookup("/queue/CountQueue");
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = sess.createProducer(queue1);

         TextMessage message = sess.createTextMessage();
         for(int i = 0; i < 100; i++)
         {
            producer.send(message);
         }
         jmsServerManager.startGatheringStatisticsForQueue("CountQueue");
         for(int i = 0; i < 100; i++)
         {
            producer.send(message);
         }
         List<MessageStatistics> messageStatistics = jmsServerManager.getStatistics();
         assertTrue(messageStatistics != null && messageStatistics.size() ==1);
         assertEquals(messageStatistics.get(0).getCount(), 100);
         assertEquals(messageStatistics.get(0).getTotalMessageCount(), 200);
         assertEquals(messageStatistics.get(0).getCurrentMessageCount(), 200);
         MessageConsumer consumer = sess.createConsumer(queue1);
         conn.start();
         for(int i = 0; i < 50; i++)
         {
            consumer.receive();
         }
         messageStatistics = jmsServerManager.getStatistics();
         assertEquals(messageStatistics.get(0).getCount(), 100);
         assertEquals(messageStatistics.get(0).getTotalMessageCount(), 200);
         assertEquals(messageStatistics.get(0).getCurrentMessageCount(), 150);
         consumer.close();
         for(int i = 0; i < 50; i++)
         {
            producer.send(message);
         }
         messageStatistics = jmsServerManager.getStatistics();
         assertEquals(messageStatistics.get(0).getCount(), 150);
         assertEquals(messageStatistics.get(0).getTotalMessageCount(), 250);
         assertEquals(messageStatistics.get(0).getCurrentMessageCount(), 200);

         consumer = sess.createConsumer(queue1);
         conn.start();
         for(int i = 0; i < 200; i++)
         {
            consumer.receive();
         }
         messageStatistics = jmsServerManager.getStatistics();
         assertEquals(messageStatistics.get(0).getCount(), 150);
         assertEquals(messageStatistics.get(0).getTotalMessageCount(), 250);
         assertEquals(messageStatistics.get(0).getCurrentMessageCount(), 0);
         consumer.close();
         jmsServerManager.stopGatheringStatisticsForQueue("CountQueue");
         messageStatistics = jmsServerManager.getStatistics();
         assertTrue(messageStatistics != null && messageStatistics.size() == 0);
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }

   }
}
