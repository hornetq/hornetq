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
package org.jboss.test.messaging.jms.server.destination;

import java.util.List;

import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.management.ObjectName;
import javax.naming.InitialContext;

import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.test.messaging.jms.server.destination.base.DestinationManagementTestBase;
import org.jboss.test.messaging.tools.ServerManagement;


/**
 * Tests a topic's management interface.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class TopicManagementTest extends DestinationManagementTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public TopicManagementTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
   }

   /**
    * Test subscriptionCount() and subscriptionCount(boolean durable).
    * @throws Exception
    */
   public void testSubscriptionCount() throws Exception
   {
      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      TopicConnectionFactory cf = (JBossConnectionFactory)ic.lookup("/ConnectionFactory");
 
      ServerManagement.deployTopic("TopicSubscription");
      Topic topic = (Topic)ic.lookup("/topic/TopicSubscription");

      TopicConnection conn = cf.createTopicConnection();

      conn.setClientID("Client1");

      TopicSession s = conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = s.createProducer(topic);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);

      // Create 1 durable subscription and 2 non-durable subscription
      s.createDurableSubscriber(topic, "SubscriberA");
      
      s.createSubscriber(topic);
      s.createSubscriber(topic);

      // There should be 3 subscriptions
      ObjectName destObjectName = 
         new ObjectName("jboss.messaging.destination:service=Topic,name=TopicSubscription");
      Integer count = (Integer)ServerManagement.invoke(destObjectName, "subscriptionCount", null, null);
      assertEquals(3, count.intValue());
      
      // There should be 1 durable subscription
      Integer duraCount = (Integer)ServerManagement.invoke(
            destObjectName, 
            "subscriptionCount", 
            new Object[] {Boolean.TRUE}, 
            new String[] {"boolean"});
      assertEquals(1, duraCount.intValue());
      
      // There should be 2 non-durable subscription
      Integer nonduraCount = (Integer)ServerManagement.invoke(
            destObjectName, 
            "subscriptionCount", 
            new Object[] {Boolean.FALSE}, 
            new String[] {"boolean"});
      assertEquals(2, nonduraCount.intValue());
      
      // Now disconnect
      conn.close();
      
      // There should be only 1 subscription totally
      count = (Integer)ServerManagement.invoke(destObjectName, "subscriptionCount", null, null);
      assertEquals(1, count.intValue());
      
      // There should be 1 durable subscription
      duraCount = (Integer)ServerManagement.invoke(
            destObjectName, 
            "subscriptionCount", 
            new Object[] {Boolean.TRUE}, 
            new String[] {"boolean"});
      assertEquals(1, duraCount.intValue());
      
      // There should be 0 non-durable subscription
      nonduraCount = (Integer)ServerManagement.invoke(
            destObjectName, 
            "subscriptionCount", 
            new Object[] {Boolean.FALSE}, 
            new String[] {"boolean"});
      assertEquals(0, nonduraCount.intValue());
      
      // Now connect again and restore the durable subscription
      conn = cf.createTopicConnection();
      conn.setClientID("Client1");
      s = conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
      s.createDurableSubscriber(topic, "SubscriberA");
      
      // There should be still 1 subscription
      count = (Integer)ServerManagement.invoke(destObjectName, "subscriptionCount", null, null);
      assertEquals(1, count.intValue());

      // Now create another durable subscription
      s.createDurableSubscriber(topic, "SubscriberB");
      
      // There should be 2 durable subscription
      duraCount = (Integer)ServerManagement.invoke(
            destObjectName, 
            "subscriptionCount", 
            new Object[] {Boolean.TRUE}, 
            new String[] {"boolean"});
      assertEquals(2, duraCount.intValue());
      
      // There should be 0 non-durable subscription
      nonduraCount = (Integer)ServerManagement.invoke(
            destObjectName, 
            "subscriptionCount", 
            new Object[] {Boolean.FALSE}, 
            new String[] {"boolean"});
      assertEquals(0, nonduraCount.intValue());

      // And close the connection
      conn.close();
      
      // There should be 2 subscription still
      count = (Integer)ServerManagement.invoke(destObjectName, "subscriptionCount", null, null);
      assertEquals(2, count.intValue());

      // There should be 2 durable subscription
      duraCount = (Integer)ServerManagement.invoke(
            destObjectName, 
            "subscriptionCount", 
            new Object[] {Boolean.TRUE}, 
            new String[] {"boolean"});
      assertEquals(2, duraCount.intValue());
      
      // There should be 0 non-durable subscription
      nonduraCount = (Integer)ServerManagement.invoke(
            destObjectName, 
            "subscriptionCount", 
            new Object[] {Boolean.FALSE}, 
            new String[] {"boolean"});
      assertEquals(0, nonduraCount.intValue());
      
      ServerManagement.undeployTopic("TopicSubscription");
   }

   /**
    * Test listSubscriptions() and listSubscriptions(boolean durable).
    * @throws Exception
    */
   public void testListSubscriptions() throws Exception
   {
      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      TopicConnectionFactory cf = (JBossConnectionFactory)ic.lookup("/ConnectionFactory");
 
      ServerManagement.deployTopic("TopicSubscriptionList");
      Topic topic = (Topic)ic.lookup("/topic/TopicSubscriptionList");

      TopicConnection conn = cf.createTopicConnection();

      conn.setClientID("Client1");

      TopicSession s = conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = s.createProducer(topic);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);

      // Create 1 durable subscription and 2 non-durable subscription
      s.createDurableSubscriber(topic, "SubscriberA");
      
      s.createSubscriber(topic);
      s.createSubscriber(topic);

      // There should be 3 subscriptions
      ObjectName destObjectName = 
         new ObjectName("jboss.messaging.destination:service=Topic,name=TopicSubscriptionList");
      List list = (List)ServerManagement.invoke(destObjectName, "listSubscriptions", null, null);
      assertEquals(3, list.size());
      
      // There should be 1 durable subscription
      List duraList = (List)ServerManagement.invoke(
            destObjectName, 
            "listSubscriptions", 
            new Object[] {Boolean.TRUE}, 
            new String[] {"boolean"});
      assertEquals(1, duraList.size());
      // String[] {ChannelID, ClientID, SubscriptionName}
      String[] strs = (String[])duraList.get(0);
      assertEquals("Client1", strs[1]);
      assertEquals("SubscriberA", strs[2]);
      
      // There should be 2 non-durable subscription
      List nonduraList = (List)ServerManagement.invoke(
            destObjectName, 
            "listSubscriptions", 
            new Object[] {Boolean.FALSE}, 
            new String[] {"boolean"});
      assertEquals(2, nonduraList.size());
      // String[] {ChannelID, ClientID, SubscriptionName}
      strs = (String[])nonduraList.get(0);
      assertEquals("", strs[1]);
      assertEquals("", strs[2]);
      // String[] {ChannelID, ClientID, SubscriptionName}
      strs = (String[])nonduraList.get(1);
      assertEquals("", strs[1]);
      assertEquals("", strs[2]);
      
      // Now disconnect
      conn.close();
      
      // There should be only 1 subscription totally
      list = (List)ServerManagement.invoke(destObjectName, "listSubscriptions", null, null);
      assertEquals(1, list.size());
      
      // There should be 1 durable subscription
      duraList = (List)ServerManagement.invoke(
            destObjectName, 
            "listSubscriptions", 
            new Object[] {Boolean.TRUE}, 
            new String[] {"boolean"});
      assertEquals(1, duraList.size());
       
      // There should be 0 non-durable subscription
      nonduraList = (List)ServerManagement.invoke(
            destObjectName, 
            "listSubscriptions", 
            new Object[] {Boolean.FALSE}, 
            new String[] {"boolean"});
      assertEquals(0, nonduraList.size());
      ServerManagement.undeployTopic("TopicSubscriptionList");
   }
   
   /**
    * Test removeAllMessages().
    * @throws Exception
    */
   public void testRemoveAllMessages() throws Exception
   {
      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      TopicConnectionFactory cf = (JBossConnectionFactory)ic.lookup("/ConnectionFactory");
 
      ServerManagement.deployTopic("TopicRemoveAllMessages");
      Topic topic = (Topic)ic.lookup("/topic/TopicRemoveAllMessages");

      TopicConnection conn = cf.createTopicConnection();

      conn.setClientID("Client1");

      TopicSession s = conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = s.createProducer(topic);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);

      // Create 1 durable subscription and 1 non-durable subscription
      TopicSubscriber tsDurable = s.createDurableSubscriber(topic, "Durable1");
      TopicSubscriber tsNonDurable = s.createSubscriber(topic);
      
      // Send 1 message
      prod.send(s.createTextMessage("First one"));
      
      // Start the connection for delivery
      conn.start();

      // Remove all messages from the topic
      ObjectName destObjectName = 
         new ObjectName("jboss.messaging.destination:service=Topic,name=TopicRemoveAllMessages");
      ServerManagement.invoke(destObjectName, "removeAllMessages", null, null);

      // Try to receive messages from the two subscriptions, should be null
      assertNull(tsDurable.receiveNoWait());
      assertNull(tsNonDurable.receiveNoWait());
      
      // Now close the connection
      conn.close();
      
      // Connect again to the same topic
      conn = cf.createTopicConnection();
      conn.setClientID("Client1");
      s = conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
      prod = s.createProducer(topic);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);

      // Send another message
      prod.send(s.createTextMessage("Second one"));
      
      // Start the connection for delivery
      conn.start();
      
      // Remove all messages from the topic
      ServerManagement.invoke(destObjectName, "removeAllMessages", null, null);

      // Restore the durable subscription now, the message should be already gone
      tsDurable = s.createDurableSubscriber(topic, "Durable1");
      assertNull(tsDurable.receiveNoWait());
      
      // Clean-up
      conn.close();
      ServerManagement.undeployTopic("TopicRemoveAllMessages");
   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   protected boolean isQueue()
   {
      return false;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
