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

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.jboss.test.messaging.tools.container.ServiceAttributeOverrides;
import org.jboss.test.messaging.tools.container.ServiceContainer;

/**
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: $</tt>2 Jul 2007
 *
 * $Id: $
 *
 */
public class PreserveOrderingTest extends ClusteringTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public PreserveOrderingTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------
   
   public void testPreserveOrderingQueuePersistent() throws Exception
   {
   	preserveOrderingQueue(true);
   }
   
   public void testPreserveOrderingQueueNonPersistent() throws Exception
   {
   	preserveOrderingQueue(false);
   }   
   
   public void testPreserveOrderingTopicPersistent() throws Exception
   {
   	preserveOrderingDurableSub(true);
   }
   
   public void testPreserveOrderingTopicNonPersistent() throws Exception
   {
   	preserveOrderingDurableSub(false);
   }   

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      nodeCount = 3;
      
      this.overrides = new ServiceAttributeOverrides();
      
      overrides.put(ServiceContainer.SERVER_PEER_OBJECT_NAME, "DefaultPreserveOrdering", "true");
   	      
      super.setUp();
   }

   protected void preserveOrderingQueue(boolean persistent) throws Exception
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
         
         log.info("Created connections");
         
         checkConnectionsDifferentServers(new Connection[] {conn0, conn1, conn2});

         Session sess0 = conn0.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Send at node 0

         MessageProducer prod0 = sess0.createProducer(queue[0]);

         prod0.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);

         final int NUM_MESSAGES = 100;

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess0.createTextMessage("message" + i);

            prod0.send(tm);
         }
         
         log.info("Sent messages");
         
         //Consume them on node1, but dont ack
         
         Session sess1 = conn1.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         MessageConsumer cons1 = sess1.createConsumer(queue[1]);
         conn1.start();

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons1.receive(5000);
            
            log.info("Got message: " + tm);

            assertNotNull(tm);

            assertEquals("message" + i, tm.getText());
         }                 
         
         //Now close sess1-  this will cancel the messages back to the queue
         
         sess1.close();
         
         //Now try and consume them back on node 0 - this should fail since we shouldn't be allowed to consume them back on node0

         MessageConsumer cons0 = sess0.createConsumer(queue[0]);
         
         conn0.start();
         
         Message m = cons0.receive(5000);

         assertNull(m);
         
         //Now try and consume them on node 2 - this should be fail too
         
         Session sess2 = conn2.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         MessageConsumer cons2 = sess2.createConsumer(queue[2]);
         conn2.start();
         
         m = cons2.receive(5000);

         assertNull(m);
         
         //Finish them off on node 1
         
         sess1 = conn1.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         cons1 = sess1.createConsumer(queue[1]);

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons1.receive(5000);

            assertNotNull(tm);
            
            assertEquals("message" + i, tm.getText());
            
            if (i == NUM_MESSAGES - 1)
            {
            	tm.acknowledge();
            }
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
   
   protected void preserveOrderingDurableSub(boolean persistent) throws Exception
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
         conn0.setClientID("cl1");
         conn1.setClientID("cl1");
         conn2.setClientID("cl1");
         
         log.info("Created connections");
         
         checkConnectionsDifferentServers(new Connection[] {conn0, conn1, conn2});

         Session sess0 = conn0.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer sub0_1 = sess0.createDurableSubscriber(topic[0], "sub1");
         
         MessageConsumer sub0_2 = sess0.createDurableSubscriber(topic[0], "sub2");
         
         sub0_1.close();
         
         sub0_2.close();
         
         
         Session sess1 = conn1.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         
         MessageConsumer sub1_1 = sess1.createDurableSubscriber(topic[1], "sub1");
         
         MessageConsumer sub1_2 = sess1.createDurableSubscriber(topic[1], "sub2");
         
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer sub2_1 = sess2.createDurableSubscriber(topic[2], "sub1");
         
         MessageConsumer sub2_2 = sess2.createDurableSubscriber(topic[2], "sub2");
         
         sub2_1.close();
         
         sub2_2.close();
         
         sess2.close();
         

         // Send at node 0

         MessageProducer prod0 = sess0.createProducer(topic[0]);

         prod0.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);

         final int NUM_MESSAGES = 100;

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess0.createTextMessage("message" + i);

            prod0.send(tm);
         }
         
         log.info("Sent messages");
         
         //Consume them on node1, but dont ack
         
         conn1.start();

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)sub1_1.receive(5000);

            assertNotNull(tm);
                 
            assertEquals("message" + i, tm.getText());
         }    
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)sub1_2.receive(5000);

            assertNotNull(tm);
            
            assertEquals("message" + i, tm.getText());
         }     
         
         //Now close sess1-  this will cancel the messages back to the queue
         
         sess1.close();
         
         //Now try and consume them back on node 0 - this should fail

         sub0_1 = sess0.createDurableSubscriber(topic[0], "sub1");
         
         sub0_2 = sess0.createDurableSubscriber(topic[0], "sub2");
         
         conn0.start();
         
         Message m = sub0_1.receive(5000);

         assertNull(m);
         
         m = sub0_2.receive(5000);

         assertNull(m);
         
         sess0.close();
         
         //Now try and consume them on node 2 - this should be fail too
         
         sess2 = conn2.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         
         sub2_1 = sess2.createDurableSubscriber(topic[2], "sub1");
         
         sub2_2 = sess2.createDurableSubscriber(topic[2], "sub2");
         
         conn2.start();
         
         m = sub2_1.receive(5000);
         
         assertNull(m);
         
         m = sub2_2.receive(5000);

         assertNull(m);
         
         sess2.close();
         
         //Finish them off on node 1
         
         sess1 = conn1.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         
         sub1_1 = sess1.createDurableSubscriber(topic[1], "sub1");
         
         sub1_2 = sess1.createDurableSubscriber(topic[1], "sub2");
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)sub1_1.receive(5000);

            assertNotNull(tm);
                     
            assertEquals("message" + i, tm.getText());
            
            if (i == NUM_MESSAGES - 1)
            {
            	tm.acknowledge();
            }
         }    
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)sub1_2.receive(5000);

            assertNotNull(tm);
                    
            assertEquals("message" + i, tm.getText());
            
            if (i == NUM_MESSAGES - 1)
            {
            	tm.acknowledge();
            }
         }         
         
         sub1_1.close();
         
         sub1_2.close();
         
         sess1.unsubscribe("sub1");
         
         sess1.unsubscribe("sub2");
         
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
