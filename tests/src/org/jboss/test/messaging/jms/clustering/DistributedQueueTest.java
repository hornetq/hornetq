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
package org.jboss.test.messaging.jms.clustering;

import java.util.HashSet;
import java.util.Set;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.jboss.test.messaging.tools.ServerManagement;

/**
 * 
 * A DistributedQueueTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 2796 $</tt>
 *
 * $Id: DistributedDestinationsTest.java 2796 2007-06-25 22:24:41Z timfox $
 *
 */
public class DistributedQueueTest extends ClusteringTestBase
{

   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public DistributedQueueTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------
   
   public void testMessagePropertiesPreservedOnSuckPersistent() throws Exception
   {
   	this.messagePropertiesPreservedOnSuck(true);
   }
   
   public void testMessagePropertiesPreservedOnSuckNonPersistent() throws Exception
   {
   	this.messagePropertiesPreservedOnSuck(false);
   }

   public void testClusteredQueueNonPersistent() throws Exception
   {
      clusteredQueue(false);
   }

   public void testClusteredQueuePersistent() throws Exception
   {
      clusteredQueue(true);
   }
   
   public void testLocalNonPersistent() throws Exception
   {
      localQueue(false);
   }

   public void testLocalPersistent() throws Exception
   {
      localQueue(true);
   }   
   
   public void testWithConnectionsOnAllNodesClientAck() throws Exception
   {
      Connection conn0 = createConnectionOnServer(cf, 0);
      
      Connection conn1 = createConnectionOnServer(cf, 1);
      
      Connection conn2 = createConnectionOnServer(cf, 2);
      
      try
      {
      	conn0.start();
      	
      	conn1.start();
      	
      	conn2.start();
      	
      	//Send a load of messages on node 0
      	      	         
      	Session sess0_1 = conn0.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      	
      	MessageConsumer cons0_1 = sess0_1.createConsumer(queue[0]);
      	
      	MessageProducer prod0 = sess0_1.createProducer(queue[0]);
      	
      	Set msgIds = new HashSet();
      	
      	final int numMessages = 60;
      	      	      	 
      	for (int i = 0; i < numMessages; i++)
      	{
      		TextMessage tm = sess0_1.createTextMessage("message-" + i);
      		
      		prod0.send(tm);      		
      	}
      		
      	TextMessage tm0_1 = null;
      	
      	for (int i = 0; i < numMessages / 6; i++)
      	{
      		tm0_1 = (TextMessage)cons0_1.receive(5000);
      		
      		assertNotNull(tm0_1);
      		
      		msgIds.add(tm0_1.getText());
      	}
      	
      	tm0_1.acknowledge();
      	
      	cons0_1.close();
      	
      	Session sess0_2 = conn0.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      	
      	MessageConsumer cons0_2 = sess0_2.createConsumer(queue[0]);
      	
      	TextMessage tm0_2 = null;
      	
      	for (int i = 0; i < numMessages / 6; i++)
      	{
      		tm0_2 = (TextMessage)cons0_2.receive(5000);
      		
      		assertNotNull(tm0_2);
      		
      		msgIds.add(tm0_2.getText());
      	}
      	
      	tm0_2.acknowledge();
      	
      	cons0_2.close();
      	
      	
      	//Two on node 1
      	
      	Session sess1_1 = conn1.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      	
      	MessageConsumer cons1_1 = sess1_1.createConsumer(queue[1]);      	
      	
      	TextMessage tm1_1 = null;
      	
      	for (int i = 0; i < numMessages / 6; i++)
      	{
      		tm1_1 = (TextMessage)cons1_1.receive(5000);
      		
      		assertNotNull(tm1_1);
      		
      		msgIds.add(tm1_1.getText());
      	}
      	
      	tm1_1.acknowledge();
      	
      	cons1_1.close();
     
      	Session sess1_2 = conn1.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      	
      	MessageConsumer cons1_2 = sess1_2.createConsumer(queue[1]);
      	
      	TextMessage tm1_2 = null;
      	
      	for (int i = 0; i < numMessages / 6; i++)
      	{
      		tm1_2 = (TextMessage)cons1_2.receive(5000);
      		      		      		
      		assertNotNull(tm1_2);
      		
      		msgIds.add(tm1_2.getText());
      	}
      	
      	tm1_2.acknowledge();
      	
      	cons1_2.close();
      	
      	
      	//Two on node 2
      	
      	Session sess2_1 = conn2.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      	
      	MessageConsumer cons2_1 = sess2_1.createConsumer(queue[2]);
      	
      	TextMessage tm2_1 = null;
      	
      	for (int i = 0; i < numMessages / 6; i++)
      	{
      		tm2_1 = (TextMessage)cons2_1.receive(5000);
      		
      		assertNotNull(tm2_1);
      		
      		msgIds.add(tm2_1.getText());
      	}
      	
      	tm2_1.acknowledge();
      	
      	cons2_1.close();
      	
      	Session sess2_2 = conn2.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      	
      	MessageConsumer cons2_2 = sess2_2.createConsumer(queue[2]);
      	
      	TextMessage tm2_2 = null;
      	
      	for (int i = 0; i < numMessages / 6; i++)
      	{
      		tm2_2 = (TextMessage)cons2_2.receive(5000);
      		
      		assertNotNull(tm2_2);
      		
      		msgIds.add(tm2_2.getText());
      	}
      	
      	tm2_2.acknowledge();
      	
      	cons2_2.close();
      	
      	assertEquals(numMessages, msgIds.size());
      	
      	for (int i = 0; i < numMessages; i++)
      	{
      		assertTrue(msgIds.contains("message-" + i));
      	}      	      
      }
      finally
      {
         if (conn0 != null)
         {
            conn0.close();
         }
         
         if (conn1 != null)
         {
            conn1.close();
         }
         
         if (conn2 != null)
         {
            conn2.close();
         }
      }
   }
   
   public void testMixedSuck() throws Exception
   {
      Connection conn0 = null;
      Connection conn1 = null;
      Connection conn2 = null;

      try
      {

         conn0 = this.createConnectionOnServer(cf, 0);
         conn1 = this.createConnectionOnServer(cf, 1);
         conn2 = this.createConnectionOnServer(cf, 2);
         
         checkConnectionsDifferentServers(new Connection[] {conn0, conn1, conn2});

         Session sess0 = conn0.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons2 = sess2.createConsumer(queue[2]);
         
         conn0.start();
         conn2.start();

         final int NUM_MESSAGES = 300;

         
         // Send at node 0

         MessageProducer prod0 = sess0.createProducer(queue[0]);

         MessageProducer prod2 = sess2.createProducer(queue[2]);

         //Send more messages at node 0 and node 2
         
         boolean persistent = false;
         for (int i = 0; i < NUM_MESSAGES / 2 ; i++)
         {
            TextMessage tm = sess0.createTextMessage("message4-" + i);

            prod0.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
            
            prod0.send(tm);
            
            persistent = !persistent;
         }
         
         for (int i = NUM_MESSAGES / 2; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess2.createTextMessage("message4-" + i);

            prod2.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
                        
            prod2.send(tm);
            
            persistent = !persistent;
         }
         
         //consume them on node 2 - we will get messages from both nodes so the order is undefined
         
         Set msgs = new HashSet();
         
         TextMessage tm = null;
         
         do
         {
            tm = (TextMessage)cons2.receive(5000);
            
            if (tm != null)
            {                     
	            msgs.add(tm.getText());
            }
         }           
         while (tm != null);

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
         	assertTrue(msgs.contains("message4-" + i));
         }
         
         assertEquals(NUM_MESSAGES, msgs.size());
                
         cons2.close();
         
         sess2.close();
         
         sess2 = conn2.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         
         cons2 = sess2.createConsumer(queue[2]);
                  
         Message msg = cons2.receive(5000);
         
         assertNull(msg);                            
      }
      finally
      {
         if (conn0 != null)
         {
            conn0.close();
         }

         if (conn1 != null)
         {
            conn1.close();
         }

         if (conn2 != null)
         {
            conn2.close();
         }
      }
   }

   // Package private ---------------------------------------------
   
   // protected ----------------------------------------------------
   
   protected void setUp() throws Exception
   {
      nodeCount = 3;

      super.setUp();
   }

   // private -----------------------------------------------------


   private void clusteredQueue(boolean persistent) throws Exception
   {
      Connection conn0 = null;
      Connection conn1 = null;
      Connection conn2 = null;

      try
      {
         //This will create 3 different connection on 3 different nodes, since
         //the cf is clustered
         conn0 = this.createConnectionOnServer(cf, 0);
         conn1 = this.createConnectionOnServer(cf, 1);
         conn2 = this.createConnectionOnServer(cf, 2);
         
         checkConnectionsDifferentServers(new Connection[] {conn0, conn1, conn2});

         Session sess0 = conn0.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons0 = sess0.createConsumer(queue[0]);
         MessageConsumer cons1 = sess1.createConsumer(queue[1]);
         MessageConsumer cons2 = sess2.createConsumer(queue[2]);
         
         conn0.start();
         conn1.start();
         conn2.start();

         // Send at node 0

         MessageProducer prod0 = sess0.createProducer(queue[0]);

         prod0.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);

         final int NUM_MESSAGES = 100;

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess0.createTextMessage("message0-" + i);

            prod0.send(tm);
         }
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons0.receive(1000);

            assertNotNull(tm);
            
            assertEquals("message0-" + i, tm.getText());
         }                 

         Message m = cons0.receive(2000);

         assertNull(m);
         
         m = cons1.receive(2000);

         assertNull(m);

         m = cons2.receive(2000);

         assertNull(m);

         // Send at node 1

         MessageProducer prod1 = sess1.createProducer(queue[1]);

         prod1.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess1.createTextMessage("message1-" + i);

            prod1.send(tm);
         }

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons1.receive(1000);

            assertNotNull(tm);

            assertEquals("message1-" + i, tm.getText());
         }

         m = cons0.receive(2000);

         assertNull(m);
         
         m = cons1.receive(2000);

         assertNull(m);

         m = cons2.receive(2000);

         assertNull(m);

         // Send at node 2
         
         MessageProducer prod2 = sess2.createProducer(queue[2]);

         prod2.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess2.createTextMessage("message2-" + i);

            prod2.send(tm);
         }

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons2.receive(1000);

            assertNotNull(tm);

            assertEquals("message2-" + i, tm.getText());
         }

         m = cons0.receive(2000);

         assertNull(m);
         
         m = cons1.receive(2000);

         assertNull(m);

         m = cons2.receive(2000);

         assertNull(m);
         
         
         //Now close the consumers at node 0 and node 1
         
         cons0.close();
         
         cons1.close();
         
         //Send more messages at node 0

         String messageIdCorrelate[] = new String[NUM_MESSAGES];

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess0.createTextMessage("message3-" + i);

            prod0.send(tm);

            messageIdCorrelate[i] = tm.getJMSMessageID();

            log.info("SetID[" + i + "]=" + tm.getJMSMessageID());

         }
              
         // consume them on node2

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons2.receive(1000);

            assertNotNull(tm);
            assertEquals(messageIdCorrelate[i], tm.getJMSMessageID());

            assertEquals("message3-" + i, tm.getText());
         }                 

         m = cons2.receive(2000);

         assertNull(m);
         
         //Send more messages at node 0 and node 1
         
         for (int i = 0; i < NUM_MESSAGES / 2; i++)
         {
            TextMessage tm = sess0.createTextMessage("message4-" + i);

            prod0.send(tm);
         }
         
         for (int i = NUM_MESSAGES / 2; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess2.createTextMessage("message4-" + i);

            prod2.send(tm);
         }
         
         //consume them on node 2 - we will get messages from both nodes so the order is undefined
         
         Set msgs = new HashSet();
         
         TextMessage tm = null;
         
         do
         {
            tm = (TextMessage)cons2.receive(1000);
            
            if (tm != null)
            {                     
	            msgs.add(tm.getText());
            }
         }           
         while (tm != null);

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
         	assertTrue(msgs.contains("message4-" + i));
         }
         
         assertEquals(NUM_MESSAGES, msgs.size());
         
         msgs.clear();
         
         // Now repeat but this time creating the consumer after send
         
         cons2.close();
         
         //	Send more messages at node 0 and node 1
         
         for (int i = 0; i < NUM_MESSAGES / 2; i++)
         {
            tm = sess0.createTextMessage("message5-" + i);

            prod0.send(tm);
         }
         
         for (int i = NUM_MESSAGES / 2; i < NUM_MESSAGES; i++)
         {
            tm = sess1.createTextMessage("message5-" + i);

            prod2.send(tm);
         }
         
         cons2 = sess2.createConsumer(queue[2]);
         
         //consume them on node 2 - we will get messages from both nodes so the order is undefined
         
         msgs = new HashSet();
         
         do
         {
            tm = (TextMessage)cons2.receive(1000);
            
            if (tm != null)
            {            
	            msgs.add(tm.getText());
            }
         }     
         while (tm != null);

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
         	assertTrue(msgs.contains("message5-" + i));
         }
         
         assertEquals(NUM_MESSAGES, msgs.size());
         
         msgs.clear();
         
         
         //Now send messages at node 0 - but consume from node 1 AND node 2
         
         //order is undefined
         
         cons2.close();
         
         cons1 = sess1.createConsumer(queue[1]);
         
         cons2 = sess2.createConsumer(queue[2]);
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            tm = sess0.createTextMessage("message6-" + i);

            prod0.send(tm);
         }
         
         msgs = new HashSet();
         
         int count = 0;
         
         do
         {
            tm = (TextMessage)cons1.receive(1000);
            
            if (tm != null)
            {                
	            msgs.add(tm.getText());
	            
	            count++;
            }
         }
         while (tm != null);
         
         do
         {
            tm = (TextMessage)cons2.receive(1000);
            
            if (tm != null)
            {            
	            msgs.add(tm.getText());
	            
	            count++;
            }
         } 
         while (tm != null);
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
         	assertTrue(msgs.contains("message6-" + i));
         }
         
         assertEquals(NUM_MESSAGES, count);
         
         msgs.clear();
         
         //as above but start consumers AFTER sending
         
         cons1.close();
         
         cons2.close();
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            tm = sess0.createTextMessage("message7-" + i);

            prod0.send(tm);
         }
         
         cons1 = sess1.createConsumer(queue[1]);
         
         cons2 = sess2.createConsumer(queue[2]);
         
         
         msgs = new HashSet();
         
         count = 0;
         
         do
         {
            tm = (TextMessage)cons1.receive(1000);
            
            if (tm != null)
            {            
	            msgs.add(tm.getText());
	            
	            count++;
            }
         }
         while (tm != null);
         
         do
         {
            tm = (TextMessage)cons2.receive(1000);
            
            if (tm != null)
            {
	            msgs.add(tm.getText());
	            
	            count++;
            }
         } 
         while (tm != null);
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
         	assertTrue(msgs.contains("message7-" + i));
         }
         
         assertEquals(NUM_MESSAGES, count);         
         
         msgs.clear();
         
         
         // Now send message on node 0, consume on node2, then cancel, consume on node1, cancel, consume on node 0
         
         cons1.close();
         
         cons2.close();
         
         sess2.close();
         
         sess2 = conn2.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         
         cons2 = sess2.createConsumer(queue[2]);
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            tm = sess0.createTextMessage("message8-" + i);

            prod0.send(tm);
         }
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            tm = (TextMessage)cons2.receive(1000);
            
            assertNotNull(tm);
               
            assertEquals("message8-" + i, tm.getText());
         } 
         
         sess2.close(); // messages should go back on queue
         
         //Now try on node 1
         
         sess1.close();
         
         sess1 = conn1.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         
         cons1 = sess1.createConsumer(queue[1]);
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            tm = (TextMessage)cons1.receive(1000);
            
            assertNotNull(tm);
               
            assertEquals("message8-" + i, tm.getText());
         } 
         
         sess1.close(); // messages should go back on queue
         
         //Now try on node 0
         
         cons0 = sess0.createConsumer(queue[0]);
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            tm = (TextMessage)cons0.receive(1000);
            
            assertNotNull(tm);
               
            assertEquals("message8-" + i, tm.getText());
         }     
         
         Message msg = cons0.receive(5000);
         
         assertNull(msg);                          
      }
      finally
      {
         if (conn0 != null)
         {
            conn0.close();
         }

         if (conn1 != null)
         {
            conn1.close();
         }

         if (conn2 != null)
         {
            conn2.close();
         }
      }
   }
   
   private void messagePropertiesPreservedOnSuck(boolean persistent) throws Exception
   {
      Connection conn0 = null;
      Connection conn1 = null;
      Connection conn2 = null;

      try
      {

         conn0 = this.createConnectionOnServer(cf, 0);
         conn1 = this.createConnectionOnServer(cf, 1);
         conn2 = this.createConnectionOnServer(cf, 2);
         
         checkConnectionsDifferentServers(new Connection[] {conn0, conn1, conn2});

         Session sess0 = conn0.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons2 = sess2.createConsumer(queue[2]);
         
         conn0.start();
         conn2.start();

         // Send at node 0

         MessageProducer prod0 = sess0.createProducer(queue[0]);

         prod0.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
         


         TextMessage tm = sess0.createTextMessage("blahmessage");
            
         prod0.setPriority(7);
         
         prod0.setTimeToLive(1 * 60 * 60 * 1000);

         prod0.send(tm);
         
         long expiration = tm.getJMSExpiration();
         
         assertEquals(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT, tm.getJMSDeliveryMode());
         
                         

         tm = (TextMessage)cons2.receive(1000);
         
         assertNotNull(tm);
         
         assertEquals("blahmessage", tm.getText());

         assertEquals(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT, tm.getJMSDeliveryMode());
         
         assertEquals(7, tm.getJMSPriority());
        
         assertTrue(Math.abs(expiration - tm.getJMSExpiration()) < 100);
                  
         Message m = cons2.receive(5000);
         
         assertNull(m);
         
         
         //Now do one with expiration = 0
         
         
         tm = sess0.createTextMessage("blahmessage2");
         
         prod0.setPriority(7);
         
         prod0.setTimeToLive(0);

         prod0.send(tm);
         
         assertEquals(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT, tm.getJMSDeliveryMode());
         
                         

         tm = (TextMessage)cons2.receive(1000);
         
         assertNotNull(tm);
         
         assertEquals("blahmessage2", tm.getText());

         assertEquals(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT, tm.getJMSDeliveryMode());
         
         assertEquals(7, tm.getJMSPriority());
        
         assertEquals(0, tm.getJMSExpiration());
                  
         m = cons2.receive(5000);
         
         assertNull(m);                          
      }
      finally
      {
         if (conn0 != null)
         {
            conn0.close();
         }

         if (conn1 != null)
         {
            conn1.close();
         }

         if (conn2 != null)
         {
            conn2.close();
         }
      }
   }
   
   
   /* Check that non clustered queues behave properly when deployed on a cluster */
   private void localQueue(boolean persistent) throws Exception
   {
   	Connection conn0 = null;
      Connection conn1 = null;
      Connection conn2 = null;
      
      //Deploy three non clustered queues with same name on different nodes
          
      try
      {
         ServerManagement.deployQueue("nonClusteredQueue", "nonClusteredQueue", 200000, 2000, 2000, 0, false);
         
         ServerManagement.deployQueue("nonClusteredQueue", "nonClusteredQueue", 200000, 2000, 2000, 1, false);
         
         ServerManagement.deployQueue("nonClusteredQueue", "nonClusteredQueue", 200000, 2000, 2000, 2, false);
         
         Queue queue0 = (Queue)ic[0].lookup("/nonClusteredQueue");
         Queue queue1 = (Queue)ic[1].lookup("/nonClusteredQueue");
         Queue queue2 = (Queue)ic[2].lookup("/nonClusteredQueue");
      	
         //This will create 3 different connection on 3 different nodes, since
         //the cf is clustered
         conn0 = this.createConnectionOnServer(cf, 0);
         conn1 = this.createConnectionOnServer(cf, 1);
         conn2 = this.createConnectionOnServer(cf, 2);
         
         checkConnectionsDifferentServers(new Connection[] {conn0, conn1, conn2});

         Session sess0 = conn0.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         conn0.start();
         conn1.start();
         conn2.start();

         // ==============
         // Send at node 0

         MessageProducer prod0 = sess0.createProducer(queue0);

         prod0.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);

         final int NUM_MESSAGES = 100;

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess0.createTextMessage("message" + i);

            prod0.send(tm);
         }
         
         // Try and consume at node 1
         
         MessageConsumer cons1 = sess1.createConsumer(queue1);
         
         Message m = cons1.receive(2000);

         assertNull(m);
         
         cons1.close();
         
         //And at node 2
         
         MessageConsumer cons2 = sess2.createConsumer(queue2);
         
         m = cons2.receive(2000);

         assertNull(m);
         
         cons2.close();
         
         // Now consume at node 0
         
         MessageConsumer cons0 = sess0.createConsumer(queue0);
          
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons0.receive(1000);

            assertNotNull(tm);
            
            assertEquals("message" + i, tm.getText());
         }                 

         m = cons0.receive(2000);

         assertNull(m);
         
         cons0.close();
         
         // ==============
         // Send at node 1

         MessageProducer prod1 = sess1.createProducer(queue1);

         prod1.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess1.createTextMessage("message" + i);

            prod1.send(tm);
         }
         
         // Try and consume at node 0
         
         cons0 = sess0.createConsumer(queue0);
         
         m = cons0.receive(2000);

         assertNull(m);
         
         cons0.close();
         
         //And at node 2
         
         cons2 = sess2.createConsumer(queue2);
         
         m = cons2.receive(2000);

         assertNull(m);
         
         cons2.close();
         
         // Now consume at node 1
         
         cons1 = sess1.createConsumer(queue1);
          
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons1.receive(1000);

            assertNotNull(tm);
            
            assertEquals("message" + i, tm.getText());
         }                 

         m = cons1.receive(2000);

         assertNull(m);
         
         cons1.close();
         
         // ==============
         // Send at node 2

         MessageProducer prod2 = sess2.createProducer(queue2);

         prod2.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess2.createTextMessage("message" + i);

            prod2.send(tm);
         }
         
         // Try and consume at node 0
         
         cons0 = sess0.createConsumer(queue0);
         
         m = cons0.receive(2000);

         assertNull(m);
         
         cons0.close();
         
         //And at node 1
         
         cons1 = sess1.createConsumer(queue1);
         
         m = cons1.receive(2000);

         assertNull(m);
         
         cons1.close();
         
         // Now consume at node 2
         
         cons2 = sess2.createConsumer(queue2);
          
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons2.receive(1000);

            assertNotNull(tm);
            
            assertEquals("message" + i, tm.getText());
         }                 

         m = cons2.receive(2000);

         assertNull(m);
         
         cons2.close();
           
      }
      finally
      {
         if (conn0 != null)
         {
            conn0.close();
         }

         if (conn1 != null)
         {
            conn1.close();
         }

         if (conn2 != null)
         {
            conn2.close();
         }
         
         ServerManagement.undeployQueue("nonClusteredQueue", 0);
         
         ServerManagement.undeployQueue("nonClusteredQueue", 1);
         
         ServerManagement.undeployQueue("nonClusteredQueue", 2);
      }
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
   
}
