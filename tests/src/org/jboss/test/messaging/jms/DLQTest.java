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

import java.util.HashMap;
import java.util.Map;

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
import javax.management.ObjectName;
import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;

import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.server.endpoint.ServerSessionEndpoint;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * A DLQTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class DLQTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected InitialContext ic;
   protected ConnectionFactory cf;
   protected Queue queue;

   // Constructors --------------------------------------------------

   public DLQTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testDLQAlreadyDeployed() throws Exception
   {
      if (ServerManagement.isRemote())
      {
         return;
      }
      
      ServerManagement.deployQueue("DLQ");
      
      ObjectName serverPeerObjectName = ServerManagement.getServerPeerObjectName();
      
      ObjectName dlqObjectName = (ObjectName)ServerManagement.getAttribute(serverPeerObjectName, "DefaultDLQ");
      
      assertNotNull(dlqObjectName);
            
      String name = (String)ServerManagement.getAttribute(dlqObjectName, "Name");
      
      assertNotNull(name);
      
      assertEquals("DLQ", name);

      String jndiName = (String)ServerManagement.getAttribute(dlqObjectName, "JNDIName");
      
      assertNotNull(jndiName);
      
      assertEquals("/queue/DLQ", jndiName);
      
      org.jboss.messaging.core.Queue dlq = ServerManagement.getServer().getServerPeer().getDefaultDLQInstance();

      assertNotNull(dlq);

      InitialContext ic = null;

      try
      {
         ic = new InitialContext(ServerManagement.getJNDIEnvironment());

         JBossQueue q = (JBossQueue)ic.lookup("/queue/DLQ");

         assertNotNull(q);

         assertEquals("DLQ", q.getName());
      }
      finally
      {
         if (ic != null) ic.close();

         ServerManagement.undeployQueue("DLQ");

      }
   }

   public void testDLQNotAlreadyDeployed() throws Exception
   {
      if (ServerManagement.isRemote())
      {
         return;
      }
      
      org.jboss.messaging.core.Queue dlq = ServerManagement.getServer().getServerPeer().getDefaultDLQInstance();

      assertNull(dlq);

      InitialContext ic = null;

      try
      {
         ic = new InitialContext(ServerManagement.getJNDIEnvironment());

         try
         {
            ic.lookup("/queue/DLQ");

            fail();
         }
         catch (NameNotFoundException e)
         {
            //Ok
         }
      }
      finally
      {
         if (ic != null) ic.close();
      }
   }
   
   public void testDefaultAndOverrideDLQ() throws Exception
   {
      if (ServerManagement.isRemote())
      {
         return;
      }
      
      final int NUM_MESSAGES = 5;
      
      final int MAX_DELIVERIES = 8;
      
      ServerManagement.deployQueue("DefaultDLQ");
      
      ServerManagement.deployQueue("OverrideDLQ");
      
      ServerManagement.deployQueue("TestQueue");
      
      String defaultDLQObjectName = "jboss.messaging.destination:service=Queue,name=DefaultDLQ";
      
      String overrideDLQObjectName = "jboss.messaging.destination:service=Queue,name=OverrideDLQ";
      
      String testQueueObjectName = "jboss.messaging.destination:service=Queue,name=TestQueue";
      
      ObjectName serverPeerObjectName = ServerManagement.getServerPeerObjectName();
      
      ServerManagement.setAttribute(serverPeerObjectName, "DefaultMaxDeliveryAttempts", String.valueOf(MAX_DELIVERIES));
            
      ServerManagement.setAttribute(serverPeerObjectName, "DefaultDLQ", defaultDLQObjectName);
      
      ServerManagement.setAttribute(new ObjectName(testQueueObjectName), "DLQ", "");
      
      Queue testQueue = (Queue)ic.lookup("/queue/TestQueue");
      
      Queue defaultDLQ = (Queue)ic.lookup("/queue/DefaultDLQ");
      
      Queue overrideDLQ = (Queue)ic.lookup("/queue/OverrideDLQ");
      
      drainDestination(cf, testQueue);
            
      drainDestination(cf, defaultDLQ);
            
      drainDestination(cf, overrideDLQ);
            
      Connection conn = null;
      
      try
      {      
         conn = cf.createConnection();
         
         {         
            Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
   
            MessageProducer prod = sess.createProducer(testQueue);
   
            for (int i = 0; i < NUM_MESSAGES; i++)
            {
               TextMessage tm = sess.createTextMessage("Message:" + i);
   
               prod.send(tm);
            }
   
            Session sess2 = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            
            MessageConsumer cons = sess2.createConsumer(testQueue);
            
            conn.start();
   
            for (int i = 0; i < MAX_DELIVERIES; i++) 
            {
               for (int j = 0; j < NUM_MESSAGES; j++)
               {
                  TextMessage tm = (TextMessage)cons.receive(1000);
   
                  assertNotNull(tm);
   
                  assertEquals("Message:" + j, tm.getText());
               }
   
               sess2.recover();
            }
            
            //At this point all the messages have been delivered exactly MAX_DELIVERIES times 
            
            Message m = cons.receive(1000);
            
            assertNull(m);
            
            //Now should be in default dlq
            
            MessageConsumer cons3 = sess.createConsumer(defaultDLQ);
            
            for (int i = 0; i < NUM_MESSAGES; i++)
            {
               TextMessage tm = (TextMessage)cons3.receive(1000);
   
               assertNotNull(tm);
   
               assertEquals("Message:" + i, tm.getText());
            }
            
            conn.close();
         }
         
         
         {
            //Now try with overriding the default dlq
            
            conn = cf.createConnection();
            
            ServerManagement.setAttribute(new ObjectName(testQueueObjectName), "DLQ", overrideDLQObjectName);
            
            Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
   
            MessageProducer prod = sess.createProducer(testQueue);
   
            for (int i = 0; i < NUM_MESSAGES; i++)
            {
               TextMessage tm = sess.createTextMessage("Message:" + i);
   
               prod.send(tm);
            }
   
            Session sess2 = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            
            MessageConsumer cons = sess2.createConsumer(testQueue);
            
            conn.start();
   
            for (int i = 0; i < MAX_DELIVERIES; i++) 
            {
               for (int j = 0; j < NUM_MESSAGES; j++)
               {
                  TextMessage tm = (TextMessage)cons.receive(1000);
   
                  assertNotNull(tm);
   
                  assertEquals("Message:" + j, tm.getText());
               }
   
               sess2.recover();
            }
            
            //At this point all the messages have been delivered exactly MAX_DELIVERIES times 
            
            Message m = cons.receive(1000);
            
            assertNull(m);
            
            //Now should be in override dlq
            
            MessageConsumer cons3 = sess.createConsumer(overrideDLQ);
            
            for (int i = 0; i < NUM_MESSAGES; i++)
            {
               TextMessage tm = (TextMessage)cons3.receive(1000);
   
               assertNotNull(tm);
   
               assertEquals("Message:" + i, tm.getText());
            }
         }
      }
      finally
      {
         ServerManagement.setAttribute(serverPeerObjectName, "DefaultDLQ", "jboss.messaging.destination:service=Queue,name=DLQ");
                  
         ServerManagement.undeployQueue("DefaultDLQ");
         
         ServerManagement.undeployQueue("OverrideDLQ");
         
         ServerManagement.undeployQueue("TestQueue");
         
         if (conn != null)
         {
            conn.close();
         }
      }
   }
            

   public void testWithMessageListenerPersistent() throws Exception
   {
      testWithMessageListener(true);
   }

   public void testWithMessageListenerNonPersistent() throws Exception
   {
      testWithMessageListener(false);
   }

   public void testWithReceiveClientAckPersistent() throws Exception
   {
      this.testWithReceiveClientAck(true);
   }

   public void testWithReceiveClientAckNonPersistent() throws Exception
   {
      testWithReceiveClientAck(false);
   }
   
   public void testWithReceiveTransactionalPersistent() throws Exception
   {
      this.testWithReceiveTransactional(true);
   }

   public void testWithReceiveTransactionalNonPersistent() throws Exception
   {
      testWithReceiveTransactional(false);
   }   
   
   public void testHeadersSet() throws Exception
   {
      Connection conn = null;

      ServerManagement.deployQueue("DLQ");

      Queue dlq = (Queue)ic.lookup("/queue/DLQ");
      
      drainDestination(cf, dlq);
      
      ObjectName serverPeerObjectName = ServerManagement.getServerPeerObjectName();
      
      final int MAX_DELIVERIES = 16;
      
      final int NUM_MESSAGES = 5;      
        
      ServerManagement.setAttribute(serverPeerObjectName, "DefaultMaxDeliveryAttempts", String.valueOf(MAX_DELIVERIES));
      
      int maxRedeliveryAttempts =
         ((Integer)ServerManagement.getAttribute(serverPeerObjectName, "DefaultMaxDeliveryAttempts")).intValue();
      
      assertEquals(MAX_DELIVERIES, maxRedeliveryAttempts);

      try
      {
         conn = cf.createConnection();

         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = sess.createProducer(queue);
         
         Map origIds = new HashMap();         

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess.createTextMessage("Message:" + i);

            prod.send(tm);
            
            origIds.put(tm.getText(), tm.getJMSMessageID());
         }

         Session sess2 = conn.createSession(true, Session.SESSION_TRANSACTED);

         MessageConsumer cons = sess2.createConsumer(queue);

         conn.start();

         for (int i = 0; i < MAX_DELIVERIES; i++) 
         {
            for (int j = 0; j < NUM_MESSAGES; j++)
            {
               TextMessage tm = (TextMessage)cons.receive(1000);

               assertNotNull(tm);

               assertEquals("Message:" + j, tm.getText());
            }

            sess2.rollback();
         }
         
         //At this point all the messages have been delivered exactly MAX_DELIVERIES times - this is ok
         //they haven't exceeded max delivery attempts so shouldn't be in the DLQ - let's check
         
         MessageConsumer cons3 = sess.createConsumer(dlq);
         
         Message m = cons3.receive(1000);
         
         assertNull(m);
         
         //So let's try and consume them - this should cause them to go to the DLQ - since they will then exceed max
         //delivery attempts
         m = cons.receive(1000);
         
         assertNull(m);
         
         //All the messages should now be in the DLQ
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons3.receive(1000);
            
            assertNotNull(tm);

            assertEquals("Message:" + i, tm.getText());
            
            //Check the headers
            String origDest = tm.getStringProperty(ServerSessionEndpoint.JBOSS_MESSAGING_ORIG_DESTINATION);
            
            String origMessageId = tm.getStringProperty(ServerSessionEndpoint.JBOSS_MESSAGING_ORIG_MESSAGEID);
            
            assertEquals(queue.toString(), origDest);
            
            String origId = (String)origIds.get(tm.getText());
            
            assertEquals(origId, origMessageId);
         }

         //No more should be available
         
         m = cons.receive(1000);
         
         assertNull(m);
         
         cons.close();

         MessageConsumer cons2 = sess2.createConsumer(queue);

         m = cons2.receive(1000);

         assertNull(m);
      }
      finally
      {
         ServerManagement.undeployQueue("DLQ");

         if (conn != null) conn.close();
      }
   }

      
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
   protected void testWithMessageListener(boolean persistent) throws Exception
   {
      Connection conn = null;

      ServerManagement.deployQueue("DLQ");

      Queue dlq = (Queue)ic.lookup("/queue/DLQ");
      
      drainDestination(cf, dlq);
      
      ObjectName serverPeerObjectName = ServerManagement.getServerPeerObjectName();
      
      final int MAX_DELIVERIES = 16;
      
      final int NUM_MESSAGES = 5;
      
      ServerManagement.setAttribute(serverPeerObjectName, "DefaultMaxDeliveryAttempts", String.valueOf(MAX_DELIVERIES));
      
      int maxRedeliveryAttempts =
         ((Integer)ServerManagement.getAttribute(serverPeerObjectName, "DefaultMaxDeliveryAttempts")).intValue();
      
      assertEquals(MAX_DELIVERIES, maxRedeliveryAttempts);
      
      try
      {
         conn = cf.createConnection();

         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = sess.createProducer(queue);

         prod.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess.createTextMessage("Message:" + i);

            prod.send(tm);
         }

         MessageConsumer cons = sess.createConsumer(queue);
         
         FailingMessageListener listener  = new FailingMessageListener();

         cons.setMessageListener(listener);

         conn.start();

         Thread.sleep(4000);

         cons.setMessageListener(null);
         
         assertEquals(MAX_DELIVERIES * NUM_MESSAGES, listener.deliveryCount);

         Message m = cons.receive(1000);

         assertNull(m);

         //Message should all be in the dlq - let's check

         MessageConsumer cons2 = sess.createConsumer(dlq);

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons2.receive(1000);

            assertNotNull(tm);

            assertEquals("Message:" + i, tm.getText());
         }

      }
      finally
      {
         ServerManagement.undeployQueue("DLQ");

         if (conn != null) conn.close();
      }
   }
   

   protected void testWithReceiveClientAck(boolean persistent) throws Exception
   {
      Connection conn = null;

      ServerManagement.deployQueue("DLQ");

      Queue dlq = (Queue)ic.lookup("/queue/DLQ");
      
      drainDestination(cf, dlq);
      
      ObjectName serverPeerObjectName = ServerManagement.getServerPeerObjectName();
      
      final int MAX_DELIVERIES = 16;
      
      final int NUM_MESSAGES = 5;      
         
      ServerManagement.setAttribute(serverPeerObjectName, "DefaultMaxDeliveryAttempts", String.valueOf(MAX_DELIVERIES));
      
      int maxRedeliveryAttempts =
         ((Integer)ServerManagement.getAttribute(serverPeerObjectName, "DefaultMaxDeliveryAttempts")).intValue();
      
      assertEquals(MAX_DELIVERIES, maxRedeliveryAttempts);

      try
      {
         conn = cf.createConnection();

         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = sess.createProducer(queue);

         prod.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
    
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess.createTextMessage("Message:" + i);

            prod.send(tm);
         }

         Session sess2 = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);

         MessageConsumer cons = sess2.createConsumer(queue);

         conn.start();

         for (int i = 0; i < MAX_DELIVERIES; i++) 
         {
            for (int j = 0; j < NUM_MESSAGES; j++)
            {
               TextMessage tm = (TextMessage)cons.receive(1000);

               assertNotNull(tm);

               assertEquals("Message:" + j, tm.getText());
            }

            sess2.recover();
         }
         
         //At this point all the messages have been delivered exactly MAX_DELIVERIES times - this is ok
         //they haven't exceeded max delivery attempts so shouldn't be in the DLQ - let's check
         
         MessageConsumer cons3 = sess.createConsumer(dlq);
         
         Message m = cons3.receive(1000);
         
         assertNull(m);
         
         //So let's try and consume them - this should cause them to go to the DLQ - since they will then exceed max
         //delivery attempts
         
         m = cons.receive(1000);
         
         assertNull(m);
         
         //Now, all the messages should now be in the DLQ
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons3.receive(1000);
            
            assertNotNull(tm);

            assertEquals("Message:" + i, tm.getText());
         }

         //No more should be available
         
         m = cons.receive(1000);
         
         assertNull(m);
         
         cons.close();

         MessageConsumer cons2 = sess2.createConsumer(queue);

         m = cons2.receive(1000);

         assertNull(m);
      }
      finally
      {
         ServerManagement.undeployQueue("DLQ");

         if (conn != null) conn.close();
      }
   }
   
   protected void testWithReceiveTransactional(boolean persistent) throws Exception
   {
      Connection conn = null;

      ServerManagement.deployQueue("DLQ");

      Queue dlq = (Queue)ic.lookup("/queue/DLQ");
      
      drainDestination(cf, dlq);
      
      ObjectName serverPeerObjectName = ServerManagement.getServerPeerObjectName();
      
      final int MAX_DELIVERIES = 16;
      
      final int NUM_MESSAGES = 5;      
        
      ServerManagement.setAttribute(serverPeerObjectName, "DefaultMaxDeliveryAttempts", String.valueOf(MAX_DELIVERIES));
      
      int maxRedeliveryAttempts =
         ((Integer)ServerManagement.getAttribute(serverPeerObjectName, "DefaultMaxDeliveryAttempts")).intValue();
      
      assertEquals(MAX_DELIVERIES, maxRedeliveryAttempts);

      try
      {
         conn = cf.createConnection();

         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = sess.createProducer(queue);

         prod.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
    
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess.createTextMessage("Message:" + i);

            prod.send(tm);
         }

         Session sess2 = conn.createSession(true, Session.SESSION_TRANSACTED);

         MessageConsumer cons = sess2.createConsumer(queue);

         conn.start();

         for (int i = 0; i < MAX_DELIVERIES; i++) 
         {
            for (int j = 0; j < NUM_MESSAGES; j++)
            {
               TextMessage tm = (TextMessage)cons.receive(1000);

               assertNotNull(tm);

               assertEquals("Message:" + j, tm.getText());
            }

            sess2.rollback();
         }
         
         //At this point all the messages have been delivered exactly MAX_DELIVERIES times - this is ok
         //they haven't exceeded max delivery attempts so shouldn't be in the DLQ - let's check
         
         MessageConsumer cons3 = sess.createConsumer(dlq);
         
         Message m = cons3.receive(1000);
         
         assertNull(m);
         
         //So let's try and consume them - this should cause them to go to the DLQ - since they will then exceed max
         //delivery attempts
         m = cons.receive(1000);
         
         assertNull(m);
         
         //All the messages should now be in the DLQ
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons3.receive(1000);
            
            assertNotNull(tm);

            assertEquals("Message:" + i, tm.getText());
         }

         //No more should be available
         
         m = cons.receive(1000);
         
         assertNull(m);
         
         cons.close();

         MessageConsumer cons2 = sess2.createConsumer(queue);

         m = cons2.receive(1000);

         assertNull(m);
      }
      finally
      {
         ServerManagement.undeployQueue("DLQ");

         if (conn != null) conn.close();
      }
   }

   protected void setUp() throws Exception
   {
      super.setUp();

      ServerManagement.start("all");

      ic = new InitialContext(ServerManagement.getJNDIEnvironment());

      cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");

      ServerManagement.deployQueue("Queue");

      queue = (Queue)ic.lookup("/queue/Queue");

   }

   protected void tearDown() throws Exception
   {
      super.tearDown();

      ServerManagement.undeployQueue("Queue");

      if (ic != null) ic.close();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
   
   class FailingMessageListener implements MessageListener
   {
      volatile int deliveryCount;

      public void onMessage(Message msg)
      {
         deliveryCount++;
         
         throw new RuntimeException("Your mum!");
      }
      
   }

}
