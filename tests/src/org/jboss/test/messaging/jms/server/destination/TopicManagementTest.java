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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicSession;
import javax.management.ObjectName;
import javax.naming.InitialContext;

import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.jms.selector.Selector;
import org.jboss.jms.server.destination.SubscriptionInfo;
import org.jboss.test.messaging.jms.server.destination.base.DestinationManagementTestBase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * Tests a topic's management interface.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
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

   public void testReloadTopic() throws Exception
   {      
      String config =
         "<mbean code=\"org.jboss.jms.server.destination.TopicService\" " +
         "       name=\"somedomain:service=Topic,name=ReloadTopic\"" +
         "       xmbean-dd=\"xmdesc/Topic-xmbean.xml\">" +
         "    <depends optional-attribute-name=\"ServerPeer\">jboss.messaging:service=ServerPeer</depends>" +
         "</mbean>";
      
      ObjectName destObjectName = deploy(config);
      
      try
      {
   
         assertEquals("ReloadTopic", ServerManagement.getAttribute(destObjectName, "Name"));
   
         String jndiName = "/topic/ReloadTopic";
         String s = (String)ServerManagement.getAttribute(destObjectName, "JNDIName");
         assertEquals(jndiName, s);
         
         //Send some messages to durable sub
         
         InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
         
         ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
         
         Topic topic = (Topic)ic.lookup("/topic/ReloadTopic");
   
         Connection conn = cf.createConnection();
         
         conn.start();
         
         conn.setClientID("wibble765");
         
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons = sess.createDurableSubscriber(topic, "subxyz");
         
         MessageProducer prod = sess.createProducer(topic);
         prod.setDeliveryMode(DeliveryMode.PERSISTENT);
     
         for (int i = 0; i < 10; i++)
         {
            TextMessage tm = sess.createTextMessage();
            
            tm.setText("message:" + i);
            
            prod.send(tm);
         }
         
         conn.close();
         
         //Receive half of them
         
         conn = cf.createConnection();
         
         conn.setClientID("wibble765");
         
         sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         cons = sess.createDurableSubscriber(topic, "subxyz");
         
         conn.start();
         
         for (int i = 0; i < 5; i++)
         {
            TextMessage tm = (TextMessage)cons.receive(1000);
            
            assertNotNull(tm);
            
            assertEquals("message:" + i, tm.getText());
         }
         
         conn.close();
         
         //Undeploy and redeploy the queue
         //The last 5 persistent messages should still be there
         
         undeployDestination("ReloadTopic");
         
         deploy(config);
         
         topic = (Topic)ic.lookup("/topic/ReloadTopic");
         
         conn = cf.createConnection();
         
         conn.setClientID("wibble765");      
         
         sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         cons = sess.createDurableSubscriber(topic, "subxyz");
         
         conn.start();
         
         for (int i = 5; i < 10; i++)
         {
            TextMessage tm = (TextMessage)cons.receive(1000);
            
            assertNotNull(tm);
            
            assertEquals("message:" + i, tm.getText());
         }
         
         conn.close();      
      }
      finally
      {      
         undeployDestination("ReloadTopic");
      }
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
      
      try
      {
         Topic topic = (Topic)ic.lookup("/topic/TopicRemoveAllMessages");
   
         TopicConnection conn = cf.createTopicConnection();
   
         conn.setClientID("Client1");
   
         TopicSession s = conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = s.createProducer(topic);
         prod.setDeliveryMode(DeliveryMode.PERSISTENT);
   
         // Create 1 durable subscription and 1 non-durable subscription
         s.createDurableSubscriber(topic, "Durable1");
         s.createSubscriber(topic);
         
         // Send 1 message
         prod.send(s.createTextMessage("First one"));         
         
         ObjectName destObjectName = 
            new ObjectName("jboss.messaging.destination:service=Topic,name=TopicRemoveAllMessages");
         
         int count = ((Integer)ServerManagement.getAttribute(destObjectName, "AllMessageCount")).intValue();
         
         assertEquals(2, count);
         
         // Start the connection for delivery
         conn.start();
         
         // Remove all messages from the topic
         
         //Need to pause since delivery may still be in progress
         Thread.sleep(2000);
         
         ServerManagement.invoke(destObjectName, "removeAllMessages", null, null);
   
         count = ((Integer)ServerManagement.getAttribute(destObjectName, "AllMessageCount")).intValue();
         
         assertEquals(0, count);
                  
         // Now close the connection
         conn.close();
         
         count = ((Integer)ServerManagement.getAttribute(destObjectName, "AllMessageCount")).intValue();
         
         assertEquals(0, count);
         
         // Connect again to the same topic
         conn = cf.createTopicConnection();
         conn.setClientID("Client1");
         s = conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
         prod = s.createProducer(topic);
         prod.setDeliveryMode(DeliveryMode.PERSISTENT);
   
         // Send another message
         prod.send(s.createTextMessage("Second one"));
         
         count = ((Integer)ServerManagement.getAttribute(destObjectName, "AllMessageCount")).intValue();
         
         assertEquals(1, count);

         // Start the connection for delivery
         conn.start();
         
         // Remove all messages from the topic
         ServerManagement.invoke(destObjectName, "removeAllMessages", null, null);
   
         count = ((Integer)ServerManagement.getAttribute(destObjectName, "AllMessageCount")).intValue();
         
         assertEquals(0, count);
         
         // Clean-up
         conn.close();
      }
      finally
      {         
         ServerManagement.undeployTopic("TopicRemoveAllMessages");
      }
   }
  
   public void testMessageCount() throws Exception
   {
      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      TopicConnectionFactory cf = (JBossConnectionFactory)ic.lookup("/ConnectionFactory");
 
      ServerManagement.deployTopic("TopicGetAllMessageCount");
      
      TopicConnection conn = null;
      
      
      try
      {                  
         Topic topic = (Topic)ic.lookup("/topic/TopicGetAllMessageCount");
   
         conn = cf.createTopicConnection();
   
         conn.setClientID("Client1");
   
         TopicSession s = conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = s.createProducer(topic);
         prod.setDeliveryMode(DeliveryMode.PERSISTENT);
   
         // Create 1 durable subscription and 2 non-durable subscription
         s.createDurableSubscriber(topic, "SubscriberA");
         
         s.createSubscriber(topic);
         s.createSubscriber(topic);
         
         //Send a couple of messages
         TextMessage tm1 = s.createTextMessage("message1");
         TextMessage tm2 = s.createTextMessage("message2");
         
         prod.send(tm1);
         prod.send(tm2);
   
         // There should be 3 subscriptions
         ObjectName destObjectName = 
            new ObjectName("jboss.messaging.destination:service=Topic,name=TopicGetAllMessageCount");
         
         Integer count = (Integer)ServerManagement.getAttribute(destObjectName, "AllMessageCount");
         assertEquals(6, count.intValue());
         
         count = (Integer)ServerManagement.getAttribute(destObjectName, "DurableMessageCount");
         assertEquals(2, count.intValue());
         
         count = (Integer)ServerManagement.getAttribute(destObjectName, "NonDurableMessageCount");
         assertEquals(4, count.intValue());
                  
         // Now disconnect
         conn.close();
         
         // Only the durable should survive
         count = (Integer)ServerManagement.getAttribute(destObjectName, "AllMessageCount");
         assertEquals(2, count.intValue());
         
         count = (Integer)ServerManagement.getAttribute(destObjectName, "DurableMessageCount");
         assertEquals(2, count.intValue());
         
         count = (Integer)ServerManagement.getAttribute(destObjectName, "NonDurableMessageCount");
         assertEquals(0, count.intValue());
         
         // Now connect again and restore the durable subscription
         conn = cf.createTopicConnection();
         conn.setClientID("Client1");
         s = conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = s.createDurableSubscriber(topic, "SubscriberA");
         
         count = (Integer)ServerManagement.getAttribute(destObjectName, "AllMessageCount");
         assertEquals(2, count.intValue());
   
         // Now create another durable subscription
         s.createDurableSubscriber(topic, "SubscriberB");
         
         //Now consume
         
         conn.start();
         
         TextMessage rm1 = (TextMessage)cons.receive(500);
         assertNotNull(rm1);
         assertEquals(tm1.getText(), rm1.getText());
         
         TextMessage rm2 = (TextMessage)cons.receive(500);
         assertNotNull(rm2);
         assertEquals(tm2.getText(), rm2.getText());
         
         Message m = cons.receive(500);
         assertNull(m);
         
         count = (Integer)ServerManagement.getAttribute(destObjectName, "AllMessageCount");
         assertEquals(0, count.intValue());
         
         count = (Integer)ServerManagement.getAttribute(destObjectName, "DurableMessageCount");
         assertEquals(0, count.intValue());
         
         count = (Integer)ServerManagement.getAttribute(destObjectName, "NonDurableMessageCount");
         assertEquals(0, count.intValue());
         
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         ServerManagement.undeployTopic("TopicGetAllMessageCount");
      }
   }
   

   public void testSubscriptionsCount() throws Exception
   {
      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      TopicConnectionFactory cf = (JBossConnectionFactory)ic.lookup("/ConnectionFactory");
 
      ServerManagement.deployTopic("TopicSubscriptionsCount");
      
      TopicConnection conn = null;
            
      try
      {
         Topic topic = (Topic)ic.lookup("/topic/TopicSubscriptionsCount");
   
         conn = cf.createTopicConnection();
   
         conn.setClientID("Client1");
   
         TopicSession s = conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

         // Create 2 durable subscription and 4 non-durable subscription
         s.createDurableSubscriber(topic, "SubscriberA");
         s.createDurableSubscriber(topic, "SubscriberB");
         
         s.createSubscriber(topic);
         s.createSubscriber(topic);
         s.createSubscriber(topic);
         s.createSubscriber(topic);
   
         // There should be 6 subscriptions
         ObjectName destObjectName = 
            new ObjectName("jboss.messaging.destination:service=Topic,name=TopicSubscriptionsCount");
         
         Integer count = (Integer)ServerManagement.getAttribute(destObjectName, "AllSubscriptionsCount");
         assertEquals(6, count.intValue());
         
         count = (Integer)ServerManagement.getAttribute(destObjectName, "DurableSubscriptionsCount");
         assertEquals(2, count.intValue());
         
         count = (Integer)ServerManagement.getAttribute(destObjectName, "NonDurableSubscriptionsCount");
         assertEquals(4, count.intValue());
         
                
         // Now disconnect
         conn.close();
         
         // Only the durable should survive
         count = (Integer)ServerManagement.getAttribute(destObjectName, "AllSubscriptionsCount");
         assertEquals(2, count.intValue());
         
         count = (Integer)ServerManagement.getAttribute(destObjectName, "DurableSubscriptionsCount");
         assertEquals(2, count.intValue());
         
         count = (Integer)ServerManagement.getAttribute(destObjectName, "NonDurableSubscriptionsCount");
         assertEquals(0, count.intValue());
         
         
         // Now connect again and restore the durable subscription
         conn = cf.createTopicConnection();
         conn.setClientID("Client1");
         s = conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
         s.createDurableSubscriber(topic, "SubscriberA");
         
         count = (Integer)ServerManagement.getAttribute(destObjectName, "AllSubscriptionsCount");
         assertEquals(2, count.intValue());
         
         count = (Integer)ServerManagement.getAttribute(destObjectName, "DurableSubscriptionsCount");
         assertEquals(2, count.intValue());
         
         count = (Integer)ServerManagement.getAttribute(destObjectName, "NonDurableSubscriptionsCount");
         assertEquals(0, count.intValue());
         
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         ServerManagement.undeployTopic("TopicSubscriptionsCount");
      }
   }

   
   public void testListSubscriptions() throws Exception
   {
      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      TopicConnectionFactory cf = (JBossConnectionFactory)ic.lookup("/ConnectionFactory");
 
      ServerManagement.deployTopic("TopicSubscriptionList");
      Topic topic = (Topic)ic.lookup("/topic/TopicSubscriptionList");

      TopicConnection conn = null;
      
      try
      {
         conn = cf.createTopicConnection();
   
         conn.setClientID("Client1");
   
         TopicSession s = conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

         // Create 2 durable subscription and 2 non-durable subscription
         s.createDurableSubscriber(topic, "SubscriberA");
         
         s.createDurableSubscriber(topic, "SubscriberB", "wibble is null", false);
         
         s.createSubscriber(topic);
         
         s.createSubscriber(topic);
         
         MessageProducer prod = s.createProducer(topic);
         prod.setDeliveryMode(DeliveryMode.PERSISTENT);
         
         // Send a couple of messages
         TextMessage tm1 = s.createTextMessage("message1");
         TextMessage tm2 = s.createTextMessage("message2");
         
         prod.send(tm1);
         prod.send(tm2);
      
         // There should be 4 subscriptions
         ObjectName destObjectName = 
            new ObjectName("jboss.messaging.destination:service=Topic,name=TopicSubscriptionList");
         
         List list = (List)ServerManagement.invoke(destObjectName, "listAllSubscriptions", null, null);
         
         assertEquals(4, list.size());
         
         SubscriptionInfo sub1 = (SubscriptionInfo)list.get(0);
         SubscriptionInfo sub2 = (SubscriptionInfo)list.get(1);
         SubscriptionInfo sub3 = (SubscriptionInfo)list.get(2);
         SubscriptionInfo sub4 = (SubscriptionInfo)list.get(3);
         
         assertEquals("Client1", sub1.getClientID());
         assertEquals(-1, sub1.getMaxSize());
         assertEquals(2, sub1.getMessageCount());
         assertEquals(null, sub1.getSelector());
         assertEquals("SubscriberA", sub1.getName());
         
         assertEquals("Client1", sub2.getClientID());
         assertEquals(-1, sub2.getMaxSize());
         assertEquals(2, sub2.getMessageCount());
         assertEquals("wibble is null", sub2.getSelector());
         assertEquals("SubscriberB", sub2.getName());
         

         assertEquals(null, sub3.getClientID());
         assertEquals(-1, sub3.getMaxSize());
         assertEquals(2, sub3.getMessageCount());
         assertEquals(null, sub3.getSelector());
         assertEquals(null, sub3.getName());
         
         assertEquals(null, sub4.getClientID());
         assertEquals(-1, sub4.getMaxSize());
         assertEquals(2, sub4.getMessageCount());
         assertEquals(null, sub4.getSelector());
         assertEquals(null, sub4.getName());
         
         //Now the durable
         
         list = (List)ServerManagement.invoke(destObjectName, "listDurableSubscriptions", null, null);
         
         assertEquals(2, list.size());
         
         sub1 = (SubscriptionInfo)list.get(0);
         sub2 = (SubscriptionInfo)list.get(1);

         
         assertEquals("Client1", sub1.getClientID());
         assertEquals(-1, sub1.getMaxSize());
         assertEquals(2, sub1.getMessageCount());
         assertEquals(null, sub1.getSelector());
         assertEquals("SubscriberA", sub1.getName());
         
         assertEquals("Client1", sub2.getClientID());
         assertEquals(-1, sub2.getMaxSize());
         assertEquals(2, sub2.getMessageCount());
         assertEquals("wibble is null", sub2.getSelector());
         assertEquals("SubscriberB", sub2.getName());
         
         //and the non durable
         
         list = (List)ServerManagement.invoke(destObjectName, "listNonDurableSubscriptions", null, null);
         
         assertEquals(2, list.size());
         
         sub3 = (SubscriptionInfo)list.get(0);
         sub4 = (SubscriptionInfo)list.get(1);
         
         assertEquals(null, sub3.getClientID());
         assertEquals(-1, sub3.getMaxSize());
         assertEquals(2, sub3.getMessageCount());
         assertEquals(null, sub3.getSelector());
         assertEquals(null, sub3.getName());
         
         assertEquals(null, sub4.getClientID());
         assertEquals(-1, sub4.getMaxSize());
         assertEquals(2, sub4.getMessageCount());
         assertEquals(null, sub4.getSelector());
         assertEquals(null, sub4.getName());
         
         
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      
         ServerManagement.undeployTopic("TopicSubscriptionList");
      }
   }
   

   public void testListSubscriptionsAsHTML() throws Exception
   {
      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      TopicConnectionFactory cf = (JBossConnectionFactory)ic.lookup("/ConnectionFactory");
 
      ServerManagement.deployTopic("TopicSubscriptionList");
      Topic topic = (Topic)ic.lookup("/topic/TopicSubscriptionList");

      TopicConnection conn = null;
      
      try
      {
         conn = cf.createTopicConnection();
   
         conn.setClientID("Client1");
   
         TopicSession s = conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

         // Create 2 durable subscription and 2 non-durable subscription
         s.createDurableSubscriber(topic, "SubscriberA");
         
         s.createDurableSubscriber(topic, "SubscriberB", "wibble is null", false);
         
         s.createSubscriber(topic);
         
         s.createSubscriber(topic);
         
         MessageProducer prod = s.createProducer(topic);
         prod.setDeliveryMode(DeliveryMode.PERSISTENT);
         
         // Send a couple of messages
         TextMessage tm1 = s.createTextMessage("message1");
         TextMessage tm2 = s.createTextMessage("message2");
         
         prod.send(tm1);
         prod.send(tm2);
      
         // There should be 4 subscriptions
         ObjectName destObjectName = 
            new ObjectName("jboss.messaging.destination:service=Topic,name=TopicSubscriptionList");
         
         String html = (String)ServerManagement.invoke(destObjectName, "listAllSubscriptionsAsHTML", null, null);
         
         assertTrue(html.startsWith("<table width=\"100%\" border=\"1\" cellpadding=\"1\" cellspacing=\"1\">"));
         assertTrue(html.endsWith("</table>"));
         
         assertTrue(html.indexOf("SubscriberA") != -1);
         assertTrue(html.indexOf("Client1") != -1);
         assertTrue(html.indexOf("SubscriberB") != -1);
         assertTrue(html.indexOf("wibble is null") != -1);
         
         
         //Now the durable
         
         html = (String)ServerManagement.invoke(destObjectName, "listDurableSubscriptionsAsHTML", null, null);
         
         assertTrue(html.startsWith("<table width=\"100%\" border=\"1\" cellpadding=\"1\" cellspacing=\"1\">"));
         assertTrue(html.endsWith("</table>"));
         
         assertTrue(html.indexOf("SubscriberA") != -1);
         assertTrue(html.indexOf("Client1") != -1);
         assertTrue(html.indexOf("SubscriberB") != -1);
         assertTrue(html.indexOf("wibble is null") != -1);
         
         // Now the non durable
         
         html = (String)ServerManagement.invoke(destObjectName, "listNonDurableSubscriptionsAsHTML", null, null);
         
         assertTrue(html.startsWith("<table width=\"100%\" border=\"1\" cellpadding=\"1\" cellspacing=\"1\">"));
         assertTrue(html.endsWith("</table>"));
         
         assertFalse(html.indexOf("SubscriberA") != -1);
         assertFalse(html.indexOf("Client1") != -1);
         assertFalse(html.indexOf("SubscriberB") != -1);
         assertFalse(html.indexOf("wibble is null") != -1);
                
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      
         ServerManagement.undeployTopic("TopicSubscriptionList");
      }
   }
   
  
   public void testListMessages() throws Exception
   {   
      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      TopicConnectionFactory cf = (JBossConnectionFactory)ic.lookup("/ConnectionFactory");
 
      ServerManagement.deployTopic("TopicMessageList");
      
      TopicConnection conn = null;
      
      try
      {
         Topic topic = (Topic)ic.lookup("/topic/TopicMessageList");
   
         conn = cf.createTopicConnection();
   
         conn.setClientID("Client1");
   
         TopicSession s = conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = s.createProducer(topic);
         prod.setDeliveryMode(DeliveryMode.PERSISTENT);
   
         MessageConsumer cons = s.createDurableSubscriber(topic, "SubscriberA");
         
         // Send some persistent message
         TextMessage tm1 = s.createTextMessage("message1");
         tm1.setStringProperty("vegetable", "parsnip");
         TextMessage tm2 = s.createTextMessage("message2");
         tm2.setStringProperty("vegetable", "parsnip");
         TextMessage tm3 = s.createTextMessage("message3");
         tm3.setStringProperty("vegetable", "parsnip");
         prod.send(tm1);
         prod.send(tm2);
         prod.send(tm3);
         
         // and some non persistent with a selector
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         
         TextMessage tm4 = s.createTextMessage("message4");
         tm4.setStringProperty("vegetable", "artichoke");
         TextMessage tm5 = s.createTextMessage("message5");
         tm5.setStringProperty("vegetable", "artichoke");
         TextMessage tm6 = s.createTextMessage("message6");
         tm6.setStringProperty("vegetable", "artichoke");
         prod.send(tm4);
         prod.send(tm5);
         prod.send(tm6);
         
         // Start the connection for delivery
         conn.start();
   
         // There should be 1 subscription
         ObjectName destObjectName = 
            new ObjectName("jboss.messaging.destination:service=Topic,name=TopicMessageList");
         
         List durableSubs = (List)ServerManagement.invoke(destObjectName, "listDurableSubscriptions", null, null);
         
         assertNotNull(durableSubs);
         assertEquals(1, durableSubs.size());
         
         
         //Note that listing messages DOES NOT list messages that are in the process of being delivered
         //or scheduled
         
         //so we need to close the consumers otherwise the messages will be buffered in them and not
         //visible
         
         cons.close();
         
         //Give time for cancel to occur
         Thread.sleep(500);
         
         String sub1Id = ((SubscriptionInfo)durableSubs.get(0)).getId();
                
         List allMsgs = (List)ServerManagement.invoke(destObjectName, "listAllMessages", new Object[] { sub1Id }, new String[] { "java.lang.String" });
         
         assertNotNull(allMsgs);         
         assertEquals(6, allMsgs.size());
         
         TextMessage rm1 = (TextMessage)allMsgs.get(0);
         TextMessage rm2 = (TextMessage)allMsgs.get(1);
         TextMessage rm3 = (TextMessage)allMsgs.get(2);
         TextMessage rm4 = (TextMessage)allMsgs.get(3);
         TextMessage rm5 = (TextMessage)allMsgs.get(4);
         TextMessage rm6 = (TextMessage)allMsgs.get(5);
         
         assertEquals(tm1.getText(), rm1.getText());
         assertEquals(tm2.getText(), rm2.getText());
         assertEquals(tm3.getText(), rm3.getText());
         assertEquals(tm4.getText(), rm4.getText());
         assertEquals(tm5.getText(), rm5.getText());
         assertEquals(tm6.getText(), rm6.getText());
         
         assertTrue(rm1.getJMSDeliveryMode() == DeliveryMode.PERSISTENT);
         assertTrue(rm2.getJMSDeliveryMode() == DeliveryMode.PERSISTENT);
         assertTrue(rm3.getJMSDeliveryMode() == DeliveryMode.PERSISTENT);         
         assertTrue(rm4.getJMSDeliveryMode() == DeliveryMode.NON_PERSISTENT);
         assertTrue(rm5.getJMSDeliveryMode() == DeliveryMode.NON_PERSISTENT);
         assertTrue(rm6.getJMSDeliveryMode() == DeliveryMode.NON_PERSISTENT);
         
         List durMsgs = (List)ServerManagement.invoke(destObjectName, "listDurableMessages", new Object[] { sub1Id }, new String[] { "java.lang.String" });
         
         assertNotNull(durMsgs);         
         assertEquals(3, durMsgs.size());
         
         rm1 = (TextMessage)durMsgs.get(0);
         rm2 = (TextMessage)durMsgs.get(1);
         rm3 = (TextMessage)durMsgs.get(2);
                
         assertEquals(tm1.getText(), rm1.getText());
         assertEquals(tm2.getText(), rm2.getText());
         assertEquals(tm3.getText(), rm3.getText());
         
         assertTrue(rm1.getJMSDeliveryMode() == DeliveryMode.PERSISTENT);
         assertTrue(rm2.getJMSDeliveryMode() == DeliveryMode.PERSISTENT);
         assertTrue(rm3.getJMSDeliveryMode() == DeliveryMode.PERSISTENT);                 
         
         
         List nondurMsgs = (List)ServerManagement.invoke(destObjectName, "listNonDurableMessages", new Object[] { sub1Id }, new String[] { "java.lang.String" });
         
         assertNotNull(nondurMsgs);         
         assertEquals(3, nondurMsgs.size());
               
         rm4 = (TextMessage)nondurMsgs.get(0);
         rm5 = (TextMessage)nondurMsgs.get(1);
         rm6 = (TextMessage)nondurMsgs.get(2);
                 
         assertEquals(tm4.getText(), rm4.getText());
         assertEquals(tm5.getText(), rm5.getText());
         assertEquals(tm6.getText(), rm6.getText());
             
         assertTrue(rm4.getJMSDeliveryMode() == DeliveryMode.NON_PERSISTENT);
         assertTrue(rm5.getJMSDeliveryMode() == DeliveryMode.NON_PERSISTENT);
         assertTrue(rm6.getJMSDeliveryMode() == DeliveryMode.NON_PERSISTENT);
         
         //Now with a selector
         
         String sel = "vegetable='artichoke'";
         
         allMsgs = (List)ServerManagement.invoke(destObjectName, "listAllMessages", new Object[] { sub1Id, sel }, new String[] { "java.lang.String", "java.lang.String" });
         
         assertNotNull(allMsgs);         
         assertEquals(3, allMsgs.size());
         
         rm4 = (TextMessage)allMsgs.get(0);
         rm5 = (TextMessage)allMsgs.get(1);
         rm6 = (TextMessage)allMsgs.get(2);
         
         assertEquals(tm4.getText(), rm4.getText());
         assertEquals(tm5.getText(), rm5.getText());
         assertEquals(tm6.getText(), rm6.getText());
            
         assertTrue(rm4.getJMSDeliveryMode() == DeliveryMode.NON_PERSISTENT);
         assertTrue(rm5.getJMSDeliveryMode() == DeliveryMode.NON_PERSISTENT);
         assertTrue(rm6.getJMSDeliveryMode() == DeliveryMode.NON_PERSISTENT);
                  
         durMsgs = (List)ServerManagement.invoke(destObjectName, "listDurableMessages", new Object[] { sub1Id, sel }, new String[] { "java.lang.String" , "java.lang.String" });
         
         assertNotNull(durMsgs);         
         assertEquals(0, durMsgs.size());
         
         
         nondurMsgs = (List)ServerManagement.invoke(destObjectName, "listNonDurableMessages", new Object[] { sub1Id, sel }, new String[] { "java.lang.String", "java.lang.String" });
         
         assertNotNull(nondurMsgs);         
         assertEquals(3, nondurMsgs.size());
               
         rm4 = (TextMessage)nondurMsgs.get(0);
         rm5 = (TextMessage)nondurMsgs.get(1);
         rm6 = (TextMessage)nondurMsgs.get(2);
                 
         assertEquals(tm4.getText(), rm4.getText());
         assertEquals(tm5.getText(), rm5.getText());
         assertEquals(tm6.getText(), rm6.getText());
             
         assertTrue(rm4.getJMSDeliveryMode() == DeliveryMode.NON_PERSISTENT);
         assertTrue(rm5.getJMSDeliveryMode() == DeliveryMode.NON_PERSISTENT);
         assertTrue(rm6.getJMSDeliveryMode() == DeliveryMode.NON_PERSISTENT);                  
         
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         
         ServerManagement.undeployTopic("TopicMessageList");
      
      }
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
