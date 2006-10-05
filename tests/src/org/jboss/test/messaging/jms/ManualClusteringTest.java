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
package org.jboss.test.messaging.jms;

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
      
//      Properties props3 = new Properties();
//      
//      props3.put(Context.INITIAL_CONTEXT_FACTORY, "org.jnp.interfaces.NamingContextFactory");
//      props3.put(Context.PROVIDER_URL, "jnp://localhost:1399");
//      props3.put(Context.URL_PKG_PREFIXES, "org.jnp.interfaces");
//      
//      ic3 = new InitialContext(props3);
      
      queue1 = (Queue)ic1.lookup("queue/testDistributedQueue");
      
      queue2 = (Queue)ic2.lookup("queue/testDistributedQueue");
      
      //queue3 = (Queue)ic3.lookup("queue/ClusteredQueue1");
            
      topic1 = (Topic)ic1.lookup("topic/testDistributedTopic");
      
      topic2 = (Topic)ic2.lookup("topic/testDistributedTopic");
      
      //topic3 = (Topic)ic3.lookup("topic/ClusteredTopic1");
      
      cf1 = (ConnectionFactory)ic1.lookup("/ConnectionFactory");
      
      cf2 = (ConnectionFactory)ic2.lookup("/ConnectionFactory");
      
      //cf3 = (ConnectionFactory)ic3.lookup("/ConnectionFactory");

      log.info("setup done");
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
      
      ic1.close();
      
      ic2.close();
   }
   
   /*
    * Each node had consumers, send message at node, make sure local consumer gets message
    */
   public void testClusteredQueueLocalConsumerNonPersistent() throws Exception
   {
      log.info("starting test");

      Connection conn1 = null;
      
      Connection conn2 = null;
      try
      {
         conn1 = cf1.createConnection();
         
         conn2 = cf2.createConnection();
           
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons1 = sess1.createConsumer(queue1);
         
         MessageConsumer cons2 = sess2.createConsumer(queue2);
         
         conn1.start();
         
         conn2.start();
         
         MessageProducer prod1 = sess1.createProducer(queue1);
         
         prod1.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         
         final int NUM_MESSAGES = 100;
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess1.createTextMessage("message" + i);
            
            prod1.send(tm);
         }
         
         log.info("sent messages");
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            log.info("i is " + i);
            
            TextMessage tm = (TextMessage)cons1.receive(1000);
            
            assertNotNull(tm);
            
            log.info("Got message:" + tm);
            
            assertEquals("message" + i, tm.getText());
         }
         
         Message m = cons2.receive(2000);
         
         assertNull(m);
      }
      finally
      {      
         try
         {
            if (conn1 != null) conn1.close();
            
            if (conn2 != null) conn2.close();
         }
         catch (Exception ignore)
         {
            
         }
      }
   }
   
   public void testClusteredQueueLocalConsumerPersistent() throws Exception
   {
      Connection conn1 = null;
      
      Connection conn2 = null;
      try
      {
         conn1 = cf1.createConnection();
         
         conn2 = cf2.createConnection();
           
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons1 = sess1.createConsumer(queue1);
         
         MessageConsumer cons2 = sess2.createConsumer(queue2);
         
         conn1.start();
         
         conn2.start();
         
         MessageProducer prod1 = sess1.createProducer(queue1);
         
         prod1.setDeliveryMode(DeliveryMode.PERSISTENT);
         
         final int NUM_MESSAGES = 100;
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess1.createTextMessage("message" + i);
            
            prod1.send(tm);
         }
         
         log.info("sent messages");
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            log.info("i is " + i);
            
            TextMessage tm = (TextMessage)cons1.receive(1000);
            
            assertNotNull(tm);
            
            log.info("Got message:" + tm);
            
            assertEquals("message" + i, tm.getText());
         }
         
         Message m = cons2.receive(2000);
         
         assertNull(m);
      }
      finally
      {      
         try
         {
            if (conn1 != null) conn1.close();
            
            if (conn2 != null) conn2.close();
         }
         catch (Exception ignore)
         {
            
         }
      }
   }
   
//   /*
//    * No consumer on local node, send message at node, make sure remote consumer gets messages
//    */
//   public void testClusteredQueueNoLocalConsumerNonPersistent() throws Exception
//   {
//      Connection conn1 = null;
//      
//      Connection conn2 = null;
//      try
//      {
//         conn1 = cf1.createConnection();
//         
//         conn2 = cf2.createConnection();
//           
//         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
//         
//         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
//         
//         MessageConsumer cons2 = sess2.createConsumer(queue2);
//         
//         conn1.start();
//         
//         conn2.start();
//         
//         MessageProducer prod1 = sess1.createProducer(queue1);
//         
//         prod1.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
//         
//         final int NUM_MESSAGES = 100;
//         
//         for (int i = 0; i < NUM_MESSAGES; i++)
//         {
//            TextMessage tm = sess1.createTextMessage("message" + i);
//            
//            prod1.send(tm);
//         }
//         
//         log.info("sent messages");
//         
//         for (int i = 0; i < NUM_MESSAGES; i++)
//         {
//            log.info("i is " + i);
//            
//            TextMessage tm = (TextMessage)cons2.receive(10000);
//            
//            assertNotNull(tm);
//            
//            log.info("Got message:" + tm);
//            
//            assertEquals("message" + i, tm.getText());
//         }
//
//      }
//      finally
//      {      
//         try
//         {
//            if (conn1 != null) conn1.close();
//            
//            if (conn2 != null) conn2.close();
//         }
//         catch (Exception ignore)
//         {
//            
//         }
//      }
//   }
//   
//   
//   
//   public void testClusteredQueueNoLocalConsumerPersistent() throws Exception
//   {
//      Connection conn1 = null;
//      
//      Connection conn2 = null;
//      try
//      {
//         conn1 = cf1.createConnection();
//         
//         conn2 = cf2.createConnection();
//           
//         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
//         
//         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
//         
//         MessageConsumer cons2 = sess2.createConsumer(queue2);
//         
//         conn1.start();
//         
//         conn2.start();
//         
//         MessageProducer prod1 = sess1.createProducer(queue1);
//         
//         prod1.setDeliveryMode(DeliveryMode.PERSISTENT);
//         
//         final int NUM_MESSAGES = 100;
//         
//         for (int i = 0; i < NUM_MESSAGES; i++)
//         {
//            TextMessage tm = sess1.createTextMessage("message" + i);
//            
//            prod1.send(tm);
//         }
//         
//         log.info("sent messages");
//         
//         for (int i = 0; i < NUM_MESSAGES; i++)
//         {
//            log.info("i is " + i);
//            
//            TextMessage tm = (TextMessage)cons2.receive(1000);
//            
//            assertNotNull(tm);
//            
//            log.info("Got message:" + tm);
//            
//            assertEquals("message" + i, tm.getText());
//         }
//
//      }
//      finally
//      {      
//         try
//         {
//            if (conn1 != null) conn1.close();
//            
//            if (conn2 != null) conn2.close();
//         }
//         catch (Exception ignore)
//         {
//            
//         }
//      }
//   }
//   

   public void testClusteredTopicNonDurableNonPersistent() throws Exception
   {
      Connection conn1 = null;
      
      Connection conn2 = null;
      try
      {
         conn1 = cf1.createConnection();
         
         conn2 = cf2.createConnection();
           
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons1 = sess1.createConsumer(topic1);
         
         MessageConsumer cons2 = sess2.createConsumer(topic2);
         
         conn1.start();
         
         conn2.start();
         
         MessageProducer prod1 = sess1.createProducer(topic1);
         
         prod1.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         
         final int NUM_MESSAGES = 100;
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess1.createTextMessage("message" + i);
            
            prod1.send(tm);
         }
         
         log.info("sent messages");
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            log.info("i is " + i);
            
            TextMessage tm = (TextMessage)cons1.receive(1000);
            
            assertNotNull(tm);
            
            log.info("Got message:" + tm);
            
            assertEquals("message" + i, tm.getText());
         }
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            log.info("i is " + i);
            
            TextMessage tm = (TextMessage)cons2.receive(1000);
            
            assertNotNull(tm);
            
            log.info("Got message:" + tm);
            
            assertEquals("message" + i, tm.getText());
         }
         

      }
      finally
      {      
         try
         {
            if (conn1 != null) conn1.close();
            
            if (conn2 != null) conn2.close();
         }
         catch (Exception ignore)
         {
            
         }
      }
   }
   
   
   public void testClusteredTopicNonDurablePersistent() throws Exception
   {
      Connection conn1 = null;
      
      Connection conn2 = null;
      try
      {
         conn1 = cf1.createConnection();
         
         conn2 = cf2.createConnection();
           
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons1 = sess1.createConsumer(topic1);
         
         MessageConsumer cons2 = sess2.createConsumer(topic2);
         
         conn1.start();
         
         conn2.start();
         
         MessageProducer prod1 = sess1.createProducer(topic1);
         
         prod1.setDeliveryMode(DeliveryMode.PERSISTENT);
         
         final int NUM_MESSAGES = 100;
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess1.createTextMessage("message" + i);
            
            prod1.send(tm);
         }
         
         log.info("sent messages");
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            log.info("i is " + i);
            
            TextMessage tm = (TextMessage)cons1.receive(1000);
            
            assertNotNull(tm);
            
            log.info("Got message:" + tm);
            
            assertEquals("message" + i, tm.getText());
         }
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            log.info("i is " + i);
            
            TextMessage tm = (TextMessage)cons2.receive(1000);
            
            assertNotNull(tm);
            
            log.info("Got message:" + tm);
            
            assertEquals("message" + i, tm.getText());
         }
         

      }
      finally
      {      
         try
         {
            if (conn1 != null) conn1.close();
            
            if (conn2 != null) conn2.close();
         }
         catch (Exception ignore)
         {
            
         }
      }
   }
   
   
   public void testClusteredTopicDurableNonPersistentLocal() throws Exception
   {
      Connection conn1 = null;
      
      Connection conn2 = null;
      try
      {
         conn1 = cf1.createConnection();
         
         conn1.setClientID("id1");
         
         conn2 = cf2.createConnection();
         
         conn2.setClientID("id1");
           
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
               
         MessageConsumer durable1 = sess1.createDurableSubscriber(topic1, "sub1");
         
         MessageConsumer durable2 = sess2.createDurableSubscriber(topic2, "sub1");
         
         conn1.start();
         
         conn2.start();
         
         MessageProducer prod1 = sess1.createProducer(topic1);
         
         prod1.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         
         final int NUM_MESSAGES = 100;
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess1.createTextMessage("message" + i);
            
            prod1.send(tm);
         }
         
         log.info("sent messages");
         
         //All the messages should be on the local sub
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            log.info("i is " + i);
            
            TextMessage tm = (TextMessage)durable1.receive(1000);
            
            assertNotNull(tm);
            
            log.info("Got message:" + tm);
            
            assertEquals("message" + i, tm.getText());
         }
         
         Message m = durable2.receive(2000);
         
         assertNull(m);
         
         durable1.close();
         
         durable2.close();
         
         sess1.unsubscribe("sub1");
         
         sess2.unsubscribe("sub1");

      }
      finally
      {      
         try
         {
            if (conn1 != null) conn1.close();
            
            if (conn2 != null) conn2.close();
         }
         catch (Exception ignore)
         {
            
         }
      }
   }
   
   public void testClusteredTopicDurablePersistentLocal() throws Exception
   {
      Connection conn1 = null;
      
      Connection conn2 = null;
      try
      {
         conn1 = cf1.createConnection();
         
         conn1.setClientID("id1");
         
         conn2 = cf2.createConnection();
         
         conn2.setClientID("id1");
           
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         try
         {
            sess1.unsubscribe("sub1");
                  
            sess2.unsubscribe("sub1");
         }
         catch (Exception ignore)
         {            
         }
         
         MessageConsumer durable1 = sess1.createDurableSubscriber(topic1, "sub1");
         
         MessageConsumer durable2 = sess2.createDurableSubscriber(topic2, "sub1");
         
         conn1.start();
         
         conn2.start();
         
         MessageProducer prod1 = sess1.createProducer(topic1);
         
         prod1.setDeliveryMode(DeliveryMode.PERSISTENT);
         
         final int NUM_MESSAGES = 100;
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess1.createTextMessage("message" + i);
            
            prod1.send(tm);
         }
         
         log.info("sent messages");
         
         //All the messages should be on the local sub
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            log.info("i is " + i);
            
            TextMessage tm = (TextMessage)durable1.receive(1000);
            
            assertNotNull(tm);
            
            log.info("Got message:" + tm);
            
            assertEquals("message" + i, tm.getText());
         }
         
         Message m = durable2.receive(2000);
         
         assertNull(m);
         
         sess1.unsubscribe("sub1");
         
         sess2.unsubscribe("sub1");
         
      }
      finally
      {      
         try
         {
            if (conn1 != null) conn1.close();
            
            if (conn2 != null) conn2.close();
         }
         catch (Exception ignore)
         {
            
         }
      }
   }
   
     
   public void testClusteredTopicDurableNonPersistentNotLocal() throws Exception
   {
      Connection conn1 = null;
      
      Connection conn2 = null;
      try
      {
         conn1 = cf1.createConnection();
         
         conn1.setClientID("id1");
         
         conn2 = cf2.createConnection();
         
         conn2.setClientID("id1");
           
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer durable2 = sess2.createDurableSubscriber(topic2, "sub1");
         
         conn1.start();
         
         conn2.start();
         
         MessageProducer prod1 = sess1.createProducer(topic1);
         
         prod1.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         
         final int NUM_MESSAGES = 100;
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess1.createTextMessage("message" + i);
            
            prod1.send(tm);
         }
         
         log.info("sent messages");
         
         //All the messages should be on the non local sub
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            log.info("i is " + i);
            
            TextMessage tm = (TextMessage)durable2.receive(1000);
            
            assertNotNull(tm);
            
            log.info("Got message:" + tm);
            
            assertEquals("message" + i, tm.getText());
         }
         
         durable2.close();
          
         sess2.unsubscribe("sub1");
         
      }
      finally
      {      
         try
         {
            if (conn1 != null) conn1.close();
            
            if (conn2 != null) conn2.close();
         }
         catch (Exception ignore)
         {
            
         }
      }
   }
   
   public void testClusteredTopicDurablePersistentNotLocal() throws Exception
   {
      Connection conn1 = null;
      
      Connection conn2 = null;
      try
      {
         conn1 = cf1.createConnection();
         
         conn1.setClientID("id1");
         
         conn2 = cf2.createConnection();
         
         conn2.setClientID("id1");
           
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
          
         MessageConsumer durable2 = sess2.createDurableSubscriber(topic2, "sub1");
         
         conn1.start();
         
         conn2.start();
         
         MessageProducer prod1 = sess1.createProducer(topic1);
         
         prod1.setDeliveryMode(DeliveryMode.PERSISTENT);
         
         final int NUM_MESSAGES = 100;
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess1.createTextMessage("message" + i);
            
            prod1.send(tm);
         }
         
         log.info("sent messages");
         
         //All the messages should be on the non local sub
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            log.info("i is " + i);
            
            TextMessage tm = (TextMessage)durable2.receive(1000);
            
            assertNotNull(tm);
            
            log.info("Got message:" + tm);
            
            assertEquals("message" + i, tm.getText());
         }
         
         durable2.close();
         
         sess2.unsubscribe("sub1");
         
      }
      finally
      {      
         try
         {
            if (conn1 != null) conn1.close();
            
            if (conn2 != null) conn2.close();
         }
         catch (Exception ignore)
         {
            
         }
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
