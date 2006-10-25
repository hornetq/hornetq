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
package org.jboss.test.messaging.jms.manual;

import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.Context;
import javax.naming.InitialContext;

import org.jboss.test.messaging.MessagingTestCase;

/**
 * 
 * A ManualClusteringTest
 * 
 * Nodes must be started up in order node1, node2, node3
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class ManualClusteringTest extends MessagingTestCase
{
   protected Context ic1;
   
   protected Context ic2;
   
   protected Context ic3;
   
   protected Queue queue1;
   
   protected Topic topic1;
   
   protected Queue queue2;
   
   protected Topic topic2;
   
   protected Queue queue3;
   
   protected Topic topic3;
   
   protected ConnectionFactory cf1;
   
   protected ConnectionFactory cf2;
   
   protected ConnectionFactory cf3;
     
   public ManualClusteringTest(String name)
   {
      super(name);
   }

   protected void setUp() throws Exception
   {
      super.setUp();
      
      Properties props1 = new Properties();
      
      props1.put(Context.INITIAL_CONTEXT_FACTORY, "org.jnp.interfaces.NamingContextFactory");
      props1.put(Context.PROVIDER_URL, "jnp://localhost:1199");
      props1.put(Context.URL_PKG_PREFIXES, "org.jnp.interfaces");
      
      ic1 = new InitialContext(props1);
      
      Properties props2 = new Properties();
      
      props2.put(Context.INITIAL_CONTEXT_FACTORY, "org.jnp.interfaces.NamingContextFactory");
      props2.put(Context.PROVIDER_URL, "jnp://localhost:1299");
      props2.put(Context.URL_PKG_PREFIXES, "org.jnp.interfaces");
      
      ic2 = new InitialContext(props2);
      
      Properties props3 = new Properties();
      
      props3.put(Context.INITIAL_CONTEXT_FACTORY, "org.jnp.interfaces.NamingContextFactory");
      props3.put(Context.PROVIDER_URL, "jnp://localhost:1399");
      props3.put(Context.URL_PKG_PREFIXES, "org.jnp.interfaces");
      
      ic3 = new InitialContext(props3);
      
      queue1 = (Queue)ic1.lookup("queue/testDistributedQueue");
      
      queue2 = (Queue)ic2.lookup("queue/testDistributedQueue");
      
      queue3 = (Queue)ic3.lookup("queue/testDistributedQueue");
            
      topic1 = (Topic)ic1.lookup("topic/testDistributedTopic");
      
      topic2 = (Topic)ic2.lookup("topic/testDistributedTopic");
      
      topic3 = (Topic)ic3.lookup("topic/testDistributedTopic");
      
      cf1 = (ConnectionFactory)ic1.lookup("/ConnectionFactory");
      
      cf2 = (ConnectionFactory)ic2.lookup("/ConnectionFactory");
      
      cf3 = (ConnectionFactory)ic3.lookup("/ConnectionFactory");
      
      drainStuff();
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
      
      ic1.close();
      
      ic2.close();
   }
   
   protected void drainStuff() throws Exception
   {
      Connection conn1 = null;
      
      Connection conn2 = null;
      
      Connection conn3 = null;
            
      try
      {
         conn1 = cf1.createConnection();
         
         conn2 = cf2.createConnection();
         
         conn3 = cf3.createConnection();
           
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         Session sess3 = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons1 = sess1.createConsumer(queue1);
         
         MessageConsumer cons2 = sess2.createConsumer(queue2);
         
         MessageConsumer cons3 = sess3.createConsumer(queue2);
         
         conn1.start();
         
         conn2.start();
         
         conn3.start();
         
         Message msg = null;
         
         do
         {
            msg = cons1.receive(1000);
         }
         while (msg != null);
         
         do
         {
            msg = cons2.receive(1000);
         }
         while (msg != null);
         
         do
         {
            msg = cons3.receive(1000);
         }
         while (msg != null);
      }
      finally
      {      
         if (conn1 != null) conn1.close();
         
         if (conn2 != null) conn2.close();
         
         if (conn3 != null) conn3.close();
      }
   }
   
   public void testClusteredQueueLocalConsumerNonPersistent() throws Exception
   {
      clusteredQueueLocalConsumer(false);
   }
   
   public void testClusteredQueueLocalConsumerPersistent() throws Exception
   {
      clusteredQueueLocalConsumer(true);
   }
   
   public void testClusteredQueueNoLocalConsumerNonPersistent() throws Exception
   {
      clusteredQueueNoLocalConsumer(false);
   }
   
   public void testClusteredQueueNoLocalConsumerPersistent() throws Exception
   {
      clusteredQueueNoLocalConsumer(true);
   }
   
   
   public void testClusteredTopicNonDurableNonPersistent() throws Exception
   {
      clusteredTopicNonDurable(false);
   }
   
   public void testClusteredTopicNonDurablePersistent() throws Exception
   {
      clusteredTopicNonDurable(true);
   }
   
   
   public void testClusteredTopicNonDurableWithSelectorsNonPersistent() throws Exception
   {
      clusteredTopicNonDurableWithSelectors(false);
   }
   
   public void testClusteredTopicNonDurableWithSelectorsPersistent() throws Exception
   {
      clusteredTopicNonDurableWithSelectors(true);
   }
   
   public void testClusteredTopicDurableNonPersistent() throws Exception
   {
      clusteredTopicDurable(false);
   }
   
   public void testClusteredTopicDurablePersistent() throws Exception
   {
      clusteredTopicDurable(true);
   }
   
   public void testClusteredTopicSharedDurableLocalConsumerNonPersistent() throws Exception
   {
      clusteredTopicSharedDurableLocalConsumer(false);
   }
   
   public void testClusteredTopicSharedDurableLocalConsumerPersistent() throws Exception
   {
      clusteredTopicSharedDurableLocalConsumer(true);
   }
   
   public void testClusteredTopicSharedDurableNoLocalConsumerNonPersistent() throws Exception
   {
      clusteredTopicSharedDurableNoLocalConsumer(false);
   }
   
   public void testClusteredTopicSharedDurableNoLocalConsumerPersistent() throws Exception
   {
      clusteredTopicSharedDurableNoLocalConsumer(true);
   }
   
   public void testClusteredTopicSharedDurableNoLocalSubNonPersistent() throws Exception
   {
      clusteredTopicSharedDurableNoLocalSub(false);
   }
   
   public void testClusteredTopicSharedDurableNoLocalSubPersistent() throws Exception
   {
      clusteredTopicSharedDurableNoLocalSub(true);
   }
   
   
   
   
   /*
    * Create a consumer on each queue on each node.
    * Send messages in turn from all nodes.
    * Ensure that the local consumer gets the message
    */
   protected void clusteredQueueLocalConsumer(boolean persistent) throws Exception
   {
      Connection conn1 = null;
      
      Connection conn2 = null;
      
      Connection conn3 = null;
      try
      {
         conn1 = cf1.createConnection();
         
         conn2 = cf2.createConnection();
         
         conn3 = cf3.createConnection();
           
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         Session sess3 = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons1 = sess1.createConsumer(queue1);
         
         MessageConsumer cons2 = sess2.createConsumer(queue2);
         
         MessageConsumer cons3 = sess3.createConsumer(queue3);
         
         conn1.start();
         
         conn2.start();
         
         conn3.start();
         
         //Send at node1
         
         MessageProducer prod1 = sess1.createProducer(queue1);
         
         prod1.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
         
         final int NUM_MESSAGES = 100;
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess1.createTextMessage("message" + i);
            
            prod1.send(tm);
         }
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons1.receive(1000);
            
            assertNotNull(tm);
            
            assertEquals("message" + i, tm.getText());
         }
         
         Message m = cons2.receive(2000);
         
         assertNull(m);
         
         m = cons3.receive(2000);
         
         assertNull(m);
         
         // Send at node2
         
         MessageProducer prod2 = sess2.createProducer(queue2);
         
         prod2.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess2.createTextMessage("message" + i);
            
            prod2.send(tm);
         }
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons2.receive(1000);
            
            assertNotNull(tm);
            
            assertEquals("message" + i, tm.getText());
         }
         
         m = cons1.receive(2000);
         
         assertNull(m);
         
         m = cons3.receive(2000);
         
         assertNull(m);
         
         // Send at node3
         
         MessageProducer prod3 = sess3.createProducer(queue3);
         
         prod3.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess3.createTextMessage("message" + i);
            
            prod3.send(tm);
         }
            
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons3.receive(1000);
            
            assertNotNull(tm);
            
            assertEquals("message" + i, tm.getText());
         }
         
         m = cons1.receive(2000);
         
         assertNull(m);
         
         m = cons2.receive(2000);
         
         assertNull(m);         
      }
      finally
      {      
         if (conn1 != null) conn1.close();
         
         if (conn2 != null) conn2.close();
         
         if (conn3 != null) conn3.close();
      }
   }
   
   
   
   
   /*
    * Create a consumer on two nodes out of three
    * Send messages from the third node
    * Ensure that the messages are received from the other two nodes in 
    * round robin order.
    * (Note that this test depends on us using the default router which has
    * this round robin behaviour)
    */
   protected void clusteredQueueNoLocalConsumer(boolean persistent) throws Exception
   {
      Connection conn1 = null;
      
      Connection conn2 = null;
      
      Connection conn3 = null;
      try
      {
         conn1 = cf1.createConnection();
         
         conn2 = cf2.createConnection();
         
         conn3 = cf3.createConnection();
           
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         Session sess3 = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons2 = sess2.createConsumer(queue2);
         
         MessageConsumer cons3 = sess3.createConsumer(queue3);
         
         conn2.start();
         
         conn3.start();
         
         //Send at node1
         
         MessageProducer prod1 = sess1.createProducer(queue1);
         
         prod1.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
         
         final int NUM_MESSAGES = 100;
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess1.createTextMessage("message" + i);
            
            prod1.send(tm);
         }
         
         for (int i = 0; i < NUM_MESSAGES / 2; i++)
         {
            TextMessage tm = (TextMessage)cons2.receive(1000);
            
            assertNotNull(tm);
            
            assertEquals("message" + i * 2, tm.getText());
         }
         
         for (int i = 0; i < NUM_MESSAGES / 2; i++)
         {
            TextMessage tm = (TextMessage)cons3.receive(1000);
            
            assertNotNull(tm);
            
            assertEquals("message" + (i * 2 + 1), tm.getText());
         }
      
      }
      finally
      {      
         if (conn1 != null) conn1.close();
         
         if (conn2 != null) conn2.close();
         
         if (conn3 != null) conn3.close();
      }
   }
   
   
   
   /*
    * Create non durable subscriptions on all nodes of the cluster.
    * Ensure all messages are receive as appropriate
    */
   public void clusteredTopicNonDurable(boolean persistent) throws Exception
   {
      Connection conn1 = null;
      
      Connection conn2 = null;
      
      Connection conn3 = null;
      try
      {
         conn1 = cf1.createConnection();
         
         conn2 = cf2.createConnection();
         
         conn3 = cf3.createConnection();
           
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         Session sess3 = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons1 = sess1.createConsumer(topic1);
         
         MessageConsumer cons2 = sess2.createConsumer(topic2);
         
         MessageConsumer cons3 = sess3.createConsumer(topic3);
         
         MessageConsumer cons4 = sess1.createConsumer(topic1);
         
         MessageConsumer cons5 = sess2.createConsumer(topic2);
            
         conn1.start();
         
         conn2.start();
         
         conn3.start();
         
         //Send at node1
         
         MessageProducer prod1 = sess1.createProducer(topic1);
         
         prod1.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
         
         final int NUM_MESSAGES = 100;
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess1.createTextMessage("message" + i);
            
            prod1.send(tm);
         }
            
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons1.receive(1000);
            
            assertNotNull(tm);
                        
            assertEquals("message" + i, tm.getText());                        
         }
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons2.receive(1000);
                      
            assertNotNull(tm);
            
            assertEquals("message" + i, tm.getText());
         }
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons3.receive(1000);
                        
            assertNotNull(tm);
             
            assertEquals("message" + i, tm.getText());
         } 
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons4.receive(1000);
                        
            assertNotNull(tm);
             
            assertEquals("message" + i, tm.getText());
         } 
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons5.receive(1000);
                        
            assertNotNull(tm);
             
            assertEquals("message" + i, tm.getText());
         } 
      }
      finally
      {      
         if (conn1 != null) conn1.close();
         
         if (conn2 != null) conn2.close();
         
         if (conn3 != null) conn3.close();
      }
   }
   
   
   
   
   /*
    * Create non durable subscriptions on all nodes of the cluster.
    * Include some with selectors
    * Ensure all messages are receive as appropriate
    */
   public void clusteredTopicNonDurableWithSelectors(boolean persistent) throws Exception
   {
      Connection conn1 = null;
      
      Connection conn2 = null;
      
      Connection conn3 = null;
      try
      {
         conn1 = cf1.createConnection();
         
         conn2 = cf2.createConnection();
         
         conn3 = cf3.createConnection();
                             
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         Session sess3 = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons1 = sess1.createConsumer(topic1);
         
         MessageConsumer cons2 = sess2.createConsumer(topic2);
         
         MessageConsumer cons3 = sess3.createConsumer(topic3);
         
         MessageConsumer cons4 = sess1.createConsumer(topic1, "COLOUR='red'");
         
         MessageConsumer cons5 = sess2.createConsumer(topic2, "COLOUR='blue'");
            
         conn1.start();
         
         conn2.start();
         
         conn3.start();
         
         //Send at node1
         
         MessageProducer prod1 = sess1.createProducer(topic1);
         
         prod1.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
         
         final int NUM_MESSAGES = 100;
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess1.createTextMessage("message" + i);
            
            int c = i % 3;
            if (c == 0)
            {
               tm.setStringProperty("COLOUR", "red");
            }
            else if (c == 1)
            {
               tm.setStringProperty("COLOUR", "blue");
            }
            
            prod1.send(tm);
         }
            
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons1.receive(1000);
            
            assertNotNull(tm);
                        
            assertEquals("message" + i, tm.getText());                        
         }
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons2.receive(1000);
                      
            assertNotNull(tm);
            
            assertEquals("message" + i, tm.getText());
         }
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons3.receive(1000);
                        
            assertNotNull(tm);
             
            assertEquals("message" + i, tm.getText());
         } 
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            int c = i % 3;
            
            if (c == 0)
            {
               TextMessage tm = (TextMessage)cons4.receive(1000);
                           
               assertNotNull(tm);
                
               assertEquals("message" + i, tm.getText());
            }
         } 
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            int c = i % 3;
            
            if (c == 1)
            {
               TextMessage tm = (TextMessage)cons5.receive(1000);
                           
               assertNotNull(tm);
                
               assertEquals("message" + i, tm.getText());
            }
         } 
      }
      finally
      {      
         if (conn1 != null) conn1.close();
         
         if (conn2 != null) conn2.close();
         
         if (conn3 != null) conn3.close();
      }
   }
   
   
   
   /*
    * Create durable subscriptions on all nodes of the cluster.
    * Include a couple with selectors
    * Ensure all messages are receive as appropriate
    * None of the durable subs are shared
    */
   public void clusteredTopicDurable(boolean persistent) throws Exception
   {
      Connection conn1 = null;
      
      Connection conn2 = null;
      
      Connection conn3 = null;
      try
      {
         conn1 = cf1.createConnection();
         
         conn2 = cf2.createConnection();
         
         conn3 = cf3.createConnection();
         
         conn1.setClientID("wib1");
         
         conn2.setClientID("wib1");
         
         conn3.setClientID("wib1");
           
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         Session sess3 = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons1 = sess1.createDurableSubscriber(topic1, "sub1");
         
         MessageConsumer cons2 = sess2.createDurableSubscriber(topic2, "sub2");
         
         MessageConsumer cons3 = sess3.createDurableSubscriber(topic3, "sub3");
         
         MessageConsumer cons4 = sess1.createDurableSubscriber(topic1, "sub4");
         
         MessageConsumer cons5 = sess2.createDurableSubscriber(topic2, "sub5");
            
         conn1.start();
         
         conn2.start();
         
         conn3.start();
         
         //Send at node1
         
         MessageProducer prod1 = sess1.createProducer(topic1);
         
         prod1.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
         
         final int NUM_MESSAGES = 100;
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess1.createTextMessage("message" + i);
            
            prod1.send(tm);
         }
            
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons1.receive(1000);
            
            assertNotNull(tm);
                        
            assertEquals("message" + i, tm.getText());                        
         }
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons2.receive(1000);
                      
            assertNotNull(tm);
            
            assertEquals("message" + i, tm.getText());
         }
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons3.receive(1000);
                        
            assertNotNull(tm);
             
            assertEquals("message" + i, tm.getText());
         } 
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons4.receive(1000);
                        
            assertNotNull(tm);
             
            assertEquals("message" + i, tm.getText());
         } 
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons5.receive(1000);
                        
            assertNotNull(tm);
             
            assertEquals("message" + i, tm.getText());
         } 
         
         cons1.close();
         
         cons2.close();
         
         cons3.close();
         
         cons4.close();
         
         cons5.close();
         
         sess1.unsubscribe("sub1");
         
         sess2.unsubscribe("sub2");
         
         sess3.unsubscribe("sub3");
         
         sess1.unsubscribe("sub4");
         
         sess2.unsubscribe("sub5");
         
      }
      finally
      {      
         if (conn1 != null) conn1.close();
         
         if (conn2 != null) conn2.close();
         
         if (conn3 != null) conn3.close();
      }
   }
   
   
   
   
   /*
    * Create shared durable subs on multiple nodes, the local instance should always get the message
    */
   protected void clusteredTopicSharedDurableLocalConsumer(boolean persistent) throws Exception
   {
      Connection conn1 = null;
      
      Connection conn2 = null;
      
      Connection conn3 = null;
      try
      {
         conn1 = cf1.createConnection();
         
         conn2 = cf2.createConnection();
         
         conn3 = cf3.createConnection();
         
         conn1.setClientID("wib1");
         
         conn2.setClientID("wib1");
         
         conn3.setClientID("wib1");
           
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         Session sess3 = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons1 = sess1.createDurableSubscriber(topic1, "sub1");
         
         MessageConsumer cons2 = sess2.createDurableSubscriber(topic2, "sub1");
         
         MessageConsumer cons3 = sess3.createDurableSubscriber(topic3, "sub1");
         
         conn1.start();
         
         conn2.start();
         
         conn3.start();
         
         //Send at node1
         
         MessageProducer prod1 = sess1.createProducer(topic1);
         
         prod1.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
         
         final int NUM_MESSAGES = 100;
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess1.createTextMessage("message" + i);
            
            prod1.send(tm);
         }
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons1.receive(1000);
            
            assertNotNull(tm);
            
            assertEquals("message" + i, tm.getText());
         }
         
         Message m = cons2.receive(2000);
         
         assertNull(m);
         
         m = cons3.receive(2000);
         
         assertNull(m);
         
         // Send at node2
         
         MessageProducer prod2 = sess2.createProducer(topic2);
         
         prod2.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess2.createTextMessage("message" + i);
            
            prod2.send(tm);
         }
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons2.receive(1000);
            
            assertNotNull(tm);
               
            assertEquals("message" + i, tm.getText());
         }
         
         m = cons1.receive(2000);
         
         assertNull(m);
         
         m = cons3.receive(2000);
         
         assertNull(m);
         
         // Send at node3
         
         MessageProducer prod3 = sess3.createProducer(topic3);
         
         prod3.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess3.createTextMessage("message" + i);
            
            prod3.send(tm);
         }
           
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons3.receive(1000);
            
            assertNotNull(tm);
            
            assertEquals("message" + i, tm.getText());
         }
         
         m = cons1.receive(2000);
         
         assertNull(m);
         
         m = cons2.receive(2000);
         
         assertNull(m);         
         
         cons1.close();
         
         cons2.close();
         
         cons3.close();
         
         //Need to unsubscribe on any node that the durable sub was created on
         
         sess1.unsubscribe("sub1");
         
         sess2.unsubscribe("sub1");
         
         sess3.unsubscribe("sub1");
      }
      finally
      {      
         if (conn1 != null) conn1.close();
         
         if (conn2 != null) conn2.close();
         
         if (conn3 != null) conn3.close();
      }
   }
   
   
   /*
    * Create shared durable subs on multiple nodes, but without consumer on local node
    * even thought there is durable sub
    * should round robin
    * note that this test assumes round robin
    */
   protected void clusteredTopicSharedDurableNoLocalConsumer(boolean persistent) throws Exception
   {
      Connection conn1 = null;
      
      Connection conn2 = null;
      
      Connection conn3 = null;
      try
      {
         conn1 = cf1.createConnection();
         
         conn2 = cf2.createConnection();
         
         conn3 = cf3.createConnection();
         
         conn1.setClientID("wib1");
         
         conn2.setClientID("wib1");
         
         conn3.setClientID("wib1");
           
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         Session sess3 = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons1 = sess1.createDurableSubscriber(topic1, "sub1");
         
         //Now close it on node 1
         conn1.close();
         
         conn1 = cf1.createConnection();
         
         conn1.setClientID("wib1");         
         
         sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         //This means the durable sub is inactive on node1
         
         MessageConsumer cons2 = sess2.createDurableSubscriber(topic2, "sub1");
         
         MessageConsumer cons3 = sess3.createDurableSubscriber(topic3, "sub1");
         
         conn2.start();
         
         conn3.start();
         
         //Send at node1
         
         //Should round robin between the other 2 since there is no active consumer on sub1 on node1
         
         MessageProducer prod1 = sess1.createProducer(topic1);
         
         prod1.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
         
         final int NUM_MESSAGES = 100;
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess1.createTextMessage("message" + i);
            
            prod1.send(tm);
         }
         
         for (int i = 0; i < NUM_MESSAGES / 2; i++)
         {
            TextMessage tm = (TextMessage)cons2.receive(1000);
            
            assertNotNull(tm);
            
            assertEquals("message" + i * 2, tm.getText());
         }
         
         for (int i = 0; i < NUM_MESSAGES / 2; i++)
         {
            TextMessage tm = (TextMessage)cons3.receive(1000);
            
            assertNotNull(tm);
            
            assertEquals("message" + (i * 2 + 1), tm.getText());
         }
         
         cons2.close();
         
         cons3.close();
         
         sess1.unsubscribe("sub1");
         
         sess2.unsubscribe("sub1");
         
         sess3.unsubscribe("sub1");
      
      }
      finally
      {      
         if (conn1 != null) conn1.close();
         
         if (conn2 != null) conn2.close();
         
         if (conn3 != null) conn3.close();
      }
   }
   
   
   
   /*
    * Create shared durable subs on multiple nodes, but without sub on local node
    * should round robin
    * note that this test assumes round robin
    */
   protected void clusteredTopicSharedDurableNoLocalSub(boolean persistent) throws Exception
   {
      Connection conn1 = null;
      
      Connection conn2 = null;
      
      Connection conn3 = null;
      try
      {
         conn1 = cf1.createConnection();
         
         conn2 = cf2.createConnection();
         
         conn3 = cf3.createConnection();
         
         conn2.setClientID("wib1");
         
         conn3.setClientID("wib1");
           
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         Session sess3 = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);
                  
         MessageConsumer cons2 = sess2.createDurableSubscriber(topic2, "sub1");
         
         MessageConsumer cons3 = sess3.createDurableSubscriber(topic3, "sub1");
         
         conn2.start();
         
         conn3.start();
         
         //Send at node1
         
         //Should round robin between the other 2 since there is no active consumer on sub1 on node1
         
         MessageProducer prod1 = sess1.createProducer(topic1);
         
         prod1.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
         
         final int NUM_MESSAGES = 100;
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess1.createTextMessage("message" + i);
            
            prod1.send(tm);
         }
         
         for (int i = 0; i < NUM_MESSAGES / 2; i++)
         {
            TextMessage tm = (TextMessage)cons2.receive(1000);
            
            assertNotNull(tm);
            
            assertEquals("message" + i * 2, tm.getText());
         }
         
         for (int i = 0; i < NUM_MESSAGES / 2; i++)
         {
            TextMessage tm = (TextMessage)cons3.receive(1000);
            
            assertNotNull(tm);
            
            assertEquals("message" + (i * 2 + 1), tm.getText());
         }
         
         cons2.close();
         
         cons3.close();
         
         sess2.unsubscribe("sub1");
         
         sess3.unsubscribe("sub1");
      
      }
      finally
      {      
         if (conn1 != null) conn1.close();
         
         if (conn2 != null) conn2.close();
         
         if (conn3 != null) conn3.close();
      }
   }

   class MyListener implements MessageListener
   {
      private int i;
      
      MyListener(int i)
      {
         this.i = i;
      }

      public void onMessage(Message m)
      {
         try
         {
            int count = m.getIntProperty("count");
            
            log.info("Listener " + i + " received message " + count);
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }
      }
      
   }
   
}
