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
    * Test subscriptionCount() and subscriptionCount(boolean durable).
    * @throws Exception
    */
   public void testSubscriptionCount() throws Exception
   {
      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      TopicConnectionFactory cf = (JBossConnectionFactory)ic.lookup("/ConnectionFactory");
 
      ServerManagement.deployTopic("TopicSubscription");
      
      try
      {
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
         
      }
      finally
      {
      
         ServerManagement.undeployTopic("TopicSubscription");
      }
   }

   /**
    * XXX Placeholder
    * Test listSubscriptions() and listSubscriptions(boolean durable).
    * @throws Exception
    */
   /*
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
   */

   /**
    * Test listSubscriptionsAsText().
    * @throws Exception
    */
   public void testListSubscriptionsAsText() throws Exception
   {
      ServerManagement.deployTopic("TopicSubscription");
      
      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      TopicConnectionFactory cf = (JBossConnectionFactory)ic.lookup("/ConnectionFactory");
 
      ServerManagement.deployTopic("TopicSubscriptionListAsText");
      
      try
      {
         Topic topic = (Topic)ic.lookup("/topic/TopicSubscriptionListAsText");
   
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
            new ObjectName("jboss.messaging.destination:service=Topic,name=TopicSubscriptionListAsText");
         String text = (String)ServerManagement.invoke(destObjectName, "listSubscriptionsAsText", null, null);
         //System.out.println("Text: \n" + text);
         
         // Find the location of durable
         int durableStart = text.indexOf("Durable");
         assertTrue (durableStart != -1);
         assertTrue (text.indexOf("SubscriberA", durableStart) != -1);
         assertTrue (text.indexOf("Client1", durableStart) != -1);
         // Find the first location of Non-durable
         int nonDurableStart = text.indexOf("Non-durable");
         assertTrue(nonDurableStart != -1);
         // Find the 2nd Non-durable
         int nonDurableSecond = text.substring(nonDurableStart).indexOf("Non-durable", 1);
         assertTrue(nonDurableSecond != -1);
         
         // Test durable subscriptions
         text = (String)ServerManagement.invoke(
               destObjectName, 
               "listSubscriptionsAsText", 
               new Object[] {Boolean.TRUE},
               new String[] {"boolean"});
         //System.out.println("Durable Text: \n" + text);
         
         durableStart = text.indexOf("Durable");
         assertTrue (durableStart != -1);
         assertTrue (text.indexOf("SubscriberA", durableStart) != -1);
         assertTrue (text.indexOf("Client1", durableStart) != -1);
         
         // Test non-durable subscriptions
         text = (String)ServerManagement.invoke(
               destObjectName, 
               "listSubscriptionsAsText", 
               new Object[] {Boolean.FALSE},
               new String[] {"boolean"});
         //System.out.println("Non-durable Text: \n" + text);
         nonDurableStart = text.indexOf("Non-durable");
         assertTrue(nonDurableStart != -1);
         // Find the 2nd Non-durable
         nonDurableSecond = text.substring(nonDurableStart).indexOf("Non-durable", 1);
         assertTrue(nonDurableSecond != -1);     
         
         // Now disconnect
         conn.close();
         
         // There should be only 1 subscription totally
         text = (String)ServerManagement.invoke(destObjectName, "listSubscriptionsAsText", null, null);
         durableStart = text.indexOf("Durable");
         assertTrue (durableStart != -1);
         assertTrue (text.indexOf("SubscriberA", durableStart) != -1);
         assertTrue (text.indexOf("Client1", durableStart) != -1);
         
         // There should be 1 durable subscription
         text = (String)ServerManagement.invoke(
               destObjectName, 
               "listSubscriptionsAsText", 
               new Object[] {Boolean.TRUE}, 
               new String[] {"boolean"});
         durableStart = text.indexOf("Durable");
         assertTrue (durableStart != -1);
         assertTrue (text.indexOf("SubscriberA", durableStart) != -1);
         assertTrue (text.indexOf("Client1", durableStart) != -1);
          
         // There should be 0 non-durable subscription
         text = (String)ServerManagement.invoke(
               destObjectName, 
               "listSubscriptionsAsText", 
               new Object[] {Boolean.FALSE}, 
               new String[] {"boolean"});
         nonDurableStart = text.indexOf("Non-durable");
         assertTrue(-1 == nonDurableStart);
      }
      finally
      {      
         ServerManagement.undeployTopic("TopicSubscriptionListAsText");
         ServerManagement.undeployTopic("TopicSubscription");
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
         List listMsg = (List)ServerManagement.invoke(destObjectName, 
                  "listMessagesDurableSub",
                  new Object[] {"Durable1", "Client1", null},
                  new String[] {"java.lang.String", "java.lang.String", "java.lang.String"});         
         assertEquals(1, listMsg.size());
         
         //TODO - Why the heck isn't there a messageCount method on the subscription
         //like there is on queue??????????
         
         // Start the connection for delivery
         conn.start();
         
         // Remove all messages from the topic
         
         //Need to pause since delivery may still be in progress
         Thread.sleep(2000);
         
         ServerManagement.invoke(destObjectName, "removeAllMessages", null, null);
   
         // Try to receive messages from the two subscriptions, should be null
         
         listMsg = (List)ServerManagement.invoke(destObjectName, 
                  "listMessagesDurableSub",
                  new Object[] {"Durable1", "Client1", null},
                  new String[] {"java.lang.String", "java.lang.String", "java.lang.String"});         
         assertEquals(0, listMsg.size());
                  
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
         
         listMsg = (List)ServerManagement.invoke(destObjectName, 
                  "listMessagesDurableSub",
                  new Object[] {"Durable1", "Client1", null},
                  new String[] {"java.lang.String", "java.lang.String", "java.lang.String"});   
         
         assertEquals(2, listMsg.size());

         // Start the connection for delivery
         conn.start();
         
         // Remove all messages from the topic
         ServerManagement.invoke(destObjectName, "removeAllMessages", null, null);
   
         listMsg = (List)ServerManagement.invoke(destObjectName, 
                  "listMessagesDurableSub",
                  new Object[] {"Durable1", "Client1", null},
                  new String[] {"java.lang.String", "java.lang.String", "java.lang.String"});         
         assertEquals(0, listMsg.size());
         
         // Clean-up
         conn.close();
      }
      finally
      {         
         ServerManagement.undeployTopic("TopicRemoveAllMessages");
      }
   }
  
   public void testListMessages() throws Exception
   {
      ServerManagement.deployTopic("TopicSubscriptionListAsText");
      
      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      TopicConnectionFactory cf = (JBossConnectionFactory)ic.lookup("/ConnectionFactory");
 
      ServerManagement.deployTopic("TopicMessageList");
      
      try
      {
         Topic topic = (Topic)ic.lookup("/topic/TopicMessageList");
   
         TopicConnection conn = cf.createTopicConnection();
   
         conn.setClientID("Client1");
   
         TopicSession s = conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = s.createProducer(topic);
         prod.setDeliveryMode(DeliveryMode.PERSISTENT);
   
         // Create 1 durable subscription and 2 non-durable subscription
         s.createDurableSubscriber(topic, "SubscriberA");
         
         s.createSubscriber(topic);
         s.createSubscriber(topic);
         
         // Send 1 message
         prod.send(s.createTextMessage("First one"));
         
         // Start the connection for delivery
         conn.start();
   
         // There should be 3 subscriptions
         ObjectName destObjectName = 
            new ObjectName("jboss.messaging.destination:service=Topic,name=TopicMessageList");
         
         ServerManagement.invoke(destObjectName, "listSubscriptionsAsText", null, null);
         
         
         // Note that listing the messages ONLY list those ones not in the process of delivery
         // Therefore the following is invalid
         
         
         // Send another message
         prod.send(s.createTextMessage("Second one"));
                  
         
         List listMsg = (List)ServerManagement.invoke(destObjectName, 
                  "listMessagesDurableSub",
                  new Object[] {"SubscriberA", "", null},
                  new String[] {"java.lang.String", "java.lang.String", "java.lang.String"});
         assertEquals(0, listMsg.size());
            
         conn.close();
         
         listMsg = (List)ServerManagement.invoke(destObjectName, 
               "listMessagesDurableSub",
               new Object[] {"SubscriberA", "", null},
               new String[] {"java.lang.String", "java.lang.String", "java.lang.String"});
         assertEquals(2, listMsg.size());
         assertTrue(listMsg.get(0) instanceof TextMessage);
         assertTrue(listMsg.get(1) instanceof TextMessage);
         assertEquals(((TextMessage)listMsg.get(0)).getText(), "First one");
         assertEquals(((TextMessage)listMsg.get(1)).getText(), "Second one");
         
      }
      finally
      {
         ServerManagement.undeployTopic("TopicMessageList");
      
         ServerManagement.undeployTopic("TopicSubscriptionListAsText");
      }
   }

//   /*
//    * XXX Placeholder
//    * 
//   public void testListMessages() throws Exception
//   {
//      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
//      TopicConnectionFactory cf = (JBossConnectionFactory)ic.lookup("/ConnectionFactory");
// 
//      ServerManagement.deployTopic("TopicMessageList");
//      Topic topic = (Topic)ic.lookup("/topic/TopicMessageList");
//
//      TopicConnection conn = cf.createTopicConnection();
//
//      conn.setClientID("Client1");
//
//      TopicSession s = conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
//      MessageProducer prod = s.createProducer(topic);
//      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
//
//      // Create 1 durable subscription and 2 non-durable subscription
//      s.createDurableSubscriber(topic, "SubscriberA");
//      
//      s.createSubscriber(topic);
//      s.createSubscriber(topic);
//      
//      // Send 1 message
//      prod.send(s.createTextMessage("First one"));
//      
//      // Start the connection for delivery
//      conn.start();
//
//      // There should be 3 subscriptions
//      ObjectName destObjectName = 
//         new ObjectName("jboss.messaging.destination:service=Topic,name=TopicMessageList");
//      List listSub = (List)ServerManagement.invoke(destObjectName, "listSubscriptions", null, null);
//      assertEquals(3, listSub.size());
//      // Each subscription will have the same message
//      for (int i = 0; i < 3; i++)
//      {
//         String[] strs = (String[])listSub.get(i);
//         List listMsg = (List)ServerManagement.invoke(destObjectName, 
//               "listMessages",
//               new Object[] {new Long(strs[0]), strs[1], strs[2], null},
//               new String[] {"long", "java.lang.String", "java.lang.String", "java.lang.String"});
//         assertEquals(1, listMsg.size());
//         assertTrue(listMsg.get(0) instanceof TextMessage);
//         assertEquals(((TextMessage)listMsg.get(0)).getText(), "First one");
//      }
//
//      // Send another message
//      prod.send(s.createTextMessage("Second one"));
//
//      // There should be 1 durable subscription
//      List duraListSub = (List)ServerManagement.invoke(
//            destObjectName, 
//            "listSubscriptions", 
//            new Object[] {Boolean.TRUE}, 
//            new String[] {"boolean"});
//      assertEquals(1, duraListSub.size());
//      String[] strs = (String[])duraListSub.get(0);
//      // The durable subscription has 2 messages
//      List listMsg = (List)ServerManagement.invoke(destObjectName, 
//            "listMessages",
//            new Object[] {new Long(strs[0]), strs[1], strs[2], null},
//            new String[] {"long", "java.lang.String", "java.lang.String", "java.lang.String"});
//      assertEquals(2, listMsg.size());
//      assertTrue(listMsg.get(0) instanceof TextMessage);
//      assertTrue(listMsg.get(1) instanceof TextMessage);
//      assertEquals(((TextMessage)listMsg.get(0)).getText(), "First one");
//      assertEquals(((TextMessage)listMsg.get(1)).getText(), "Second one");
//      
//      // Clean-up
//      conn.close();
//      ServerManagement.undeployTopic("TopicMessageList");
//   }
//   */
//   
   /**
    * The jmx-console has the habit of sending an empty string if no argument is specified, so
    * we test this eventuality.
    */
   public void testListMessagesEmptySelector() throws Exception
   {
      ServerManagement.deployTopic("TopicMessageList");
      
      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      TopicConnectionFactory cf = (JBossConnectionFactory)ic.lookup("/ConnectionFactory");

      ServerManagement.deployTopic("TopicMessageList2");
      
      try
      {
         Topic topic = (Topic)ic.lookup("/topic/TopicMessageList2");
   
         TopicConnection conn = cf.createTopicConnection();
         conn.setClientID("Client1");
         TopicSession s = conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
   
         s.createDurableSubscriber(topic, "SubscriberA");
   
         ObjectName topicON =
            new ObjectName("jboss.messaging.destination:service=Topic,name=TopicMessageList2");
   
         List messages = (List)ServerManagement.invoke(topicON,
               "listMessagesDurableSub",
               new Object[] {"SubscriberA", "Client1", ""},
               new String[] {"java.lang.String", "java.lang.String", "java.lang.String"});
         assertTrue(messages.isEmpty());
         
         messages = (List)ServerManagement.invoke(topicON,
               "listMessagesDurableSub",
               new Object[] {"SubscriberA", "Client1", "                   "},
               new String[] {"java.lang.String", "java.lang.String", "java.lang.String"});
         assertTrue(messages.isEmpty());
   
         // Clean-up
         conn.close();
      }
      finally
      {
         ServerManagement.undeployTopic("TopicMessageList2");
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
