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
import javax.jms.Session;
import javax.jms.TextMessage;

import org.jboss.test.messaging.jms.clustering.base.ClusteringTestBase;

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

   public void testClusteredQueueNonPersistent() throws Exception
   {
      clusteredQueue(false);
   }

   public void testClusteredQueuePersistent() throws Exception
   {
      clusteredQueue(true);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      nodeCount = 3;

      super.setUp();

      log.debug("setup done");
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   protected void clusteredQueue(boolean persistent) throws Exception
   {
      Connection conn0 = null;
      Connection conn1 = null;
      Connection conn2 = null;

      try
      {
         //This will create 3 different connection on 3 different nodes, since
         //the cf is clustered
         conn0 = cf.createConnection();
         conn1 = cf.createConnection();
         conn2 = cf.createConnection();
         
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
            TextMessage tm = sess0.createTextMessage("message" + i);

            prod0.send(tm);
         }
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons0.receive(1000);

            assertNotNull(tm);
            
            assertEquals("message" + i, tm.getText());
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
            TextMessage tm = sess1.createTextMessage("message" + i);

            prod1.send(tm);
         }

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons1.receive(1000);

            assertNotNull(tm);

            assertEquals("message" + i, tm.getText());
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
            TextMessage tm = sess2.createTextMessage("message" + i);

            prod2.send(tm);
         }

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons2.receive(1000);

            assertNotNull(tm);

            assertEquals("message" + i, tm.getText());
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
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess0.createTextMessage("message2-" + i);

            prod0.send(tm);
         }
              
         // consume them on node2

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons2.receive(1000);
                  
            assertNotNull(tm);
            
            assertEquals("message2-" + i, tm.getText());
         }                 

         m = cons2.receive(2000);

         assertNull(m);
         
         //Send more messages at node 0 and node 1
         
         for (int i = 0; i < NUM_MESSAGES / 2; i++)
         {
            TextMessage tm = sess0.createTextMessage("message3-" + i);

            prod0.send(tm);
         }
         
         for (int i = NUM_MESSAGES / 2; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess1.createTextMessage("message3-" + i);

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
	            assertNotNull(tm);
	            
	            msgs.add(tm.getText());
            }
         }           
         while (tm != null);

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
         	assertTrue(msgs.contains("message3-" + i));
         }
         
         // Now repeat but this time creating the consumer after send
         
         cons2.close();
         
         //	Send more messages at node 0 and node 1
         
         for (int i = 0; i < NUM_MESSAGES / 2; i++)
         {
            tm = sess0.createTextMessage("message3-" + i);

            prod0.send(tm);
         }
         
         for (int i = NUM_MESSAGES / 2; i < NUM_MESSAGES; i++)
         {
            tm = sess1.createTextMessage("message3-" + i);

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
	            assertNotNull(tm);
	            
	            msgs.add(tm.getText());
            }
         }     
         while (tm != null);

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
         	assertTrue(msgs.contains("message3-" + i));
         }
         
         
         //Now send messages at node 0 - but consume from node 1 AND node 2
         
         //order is undefined
         
         cons2.close();
         
         cons1 = sess1.createConsumer(queue[1]);
         
         cons2 = sess2.createConsumer(queue[2]);
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            tm = sess0.createTextMessage("message4-" + i);

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
         	assertTrue(msgs.contains("message4-" + i));
         }
         
         assertEquals(NUM_MESSAGES, count);
         
         //as above but start consumers AFTER sending
         
         cons1.close();
         
         cons2.close();
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            tm = sess0.createTextMessage("message4-" + i);

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
         	assertTrue(msgs.contains("message4-" + i));
         }
         
         assertEquals(NUM_MESSAGES, count);         
         
         
         // Now send message on node 0, consume on node2, then cancel, consume on node1, cancel, consume on node 0
         
         cons1.close();
         
         cons2.close();
         
         sess2.close();
         
         sess2 = conn2.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         
         cons2 = sess2.createConsumer(queue[2]);
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            tm = sess0.createTextMessage("message5-" + i);

            prod0.send(tm);
         }
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            tm = (TextMessage)cons2.receive(1000);
            
            assertNotNull(tm);
               
            assertEquals("message5-" + i, tm.getText());
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
               
            assertEquals("message5-" + i, tm.getText());
         } 
         
         sess1.close(); // messages should go back on queue
         
         //Now try on node 0
         
         cons0 = sess0.createConsumer(queue[0]);
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            tm = (TextMessage)cons0.receive(1000);
            
            assertNotNull(tm);
               
            assertEquals("message5-" + i, tm.getText());
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

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
   
}
