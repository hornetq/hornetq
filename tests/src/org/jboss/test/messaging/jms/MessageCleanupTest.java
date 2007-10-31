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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.Topic;

import org.jboss.messaging.core.impl.message.SimpleMessageStore;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * 
 * A MessageCleanupTest

 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class MessageCleanupTest extends JMSTestCase
{
   // Constants -----------------------------------------------------
   
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Topic ourTopic;

   // Constructors --------------------------------------------------
   
   public MessageCleanupTest(String name)
   {
      super(name);
   }
   
   // TestCase overrides -------------------------------------------
   
   public void setUp() throws Exception
   {
      super.setUp();                  
      
      ServerManagement.deployTopic("TestTopic", 100, 10, 10);
      
      ourTopic = (Topic)ic.lookup("/topic/TestTopic");
   }
   
   public void tearDown() throws Exception
   {
      super.tearDown();
      
      ServerManagement.undeployTopic("TestTopic");      
   }
   
   /*
    * Test that all messages on a non durable sub are removed on close
    */
   public void testNonDurableClose() throws Exception
   {
      if (ServerManagement.isRemote()) return;
      
      SimpleMessageStore ms = (SimpleMessageStore)ServerManagement.getMessageStore();      
        
      Connection conn = cf.createConnection();
      
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageProducer prod = sess.createProducer(ourTopic);
      
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);      
       
      MessageConsumer cons = sess.createConsumer(ourTopic);
                  
      for (int i = 0; i < 150; i++)
      {
         prod.send(sess.createMessage());
      }
      
      //Give them time to arrive NP messages are sent one way
      Thread.sleep(10000);
              
      //50 Should be paged onto disk
      
      assertEquals(50, getReferenceIds().size());
      
      assertEquals(50, getMessageIds().size());
      
      //Now we close the consumer
      
      cons.close();
      
      assertEquals(0, getReferenceIds().size());
      
      assertEquals(0, getMessageIds().size());
      
      conn.close();
   }
   
   public void testNonDurableClose2() throws Exception
   {
      if (ServerManagement.isRemote()) return;
      
      Connection conn = cf.createConnection();
      
      conn.setClientID("wibble12345");
      
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageProducer prod = sess.createProducer(ourTopic);
      
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      MessageConsumer cons1 = sess.createConsumer(ourTopic);
      
      MessageConsumer cons2 = sess.createDurableSubscriber(ourTopic, "sub1");
                  
      for (int i = 0; i < 150; i++)
      {
         prod.send(sess.createMessage());
      }
      
      //Give them time to arrive NP messages are sent one way
      Thread.sleep(10000);
       
      assertEquals(100, getReferenceIds().size());
      
      assertEquals(50, getMessageIds().size());
      
      //Now we close the consumers
      
      cons1.close();
      cons2.close();
       
      assertEquals(50, getReferenceIds().size());
      
      assertEquals(50, getMessageIds().size());
      
      sess.unsubscribe("sub1");
        
      assertEquals(0, getReferenceIds().size());
      
      assertEquals(0, getMessageIds().size());
            
      conn.close();
   }
   


   /*
    * Test that all messages on a temporary queue are removed on close
    */
   public void testTemporaryQueueClose() throws Exception
   {
      if (ServerManagement.isRemote()) return;
      
      String objectName = "somedomain:service=TempQueueConnectionFactory";
      String[] jndiBindings = new String[] { "/TempQueueConnectionFactory" };

      ServerManagement.deployConnectionFactory(objectName, jndiBindings, 150, 100, 10, 10);

      ConnectionFactory cf2 = (ConnectionFactory)ic.lookup("/TempQueueConnectionFactory");
      
      Connection conn = cf2.createConnection();
      
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      TemporaryQueue queue = sess.createTemporaryQueue();
      
      MessageProducer prod = sess.createProducer(queue);
      
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
           
      conn.start();
      
      for (int i = 0; i < 150; i++)
      {
         prod.send(sess.createMessage());
      }
      
      SimpleMessageStore ms = (SimpleMessageStore)ServerManagement.getMessageStore();
      
      assertEquals(50, getReferenceIds().size());
      
      assertEquals(50, getMessageIds().size());
      
      //Now we close the connection
      
      conn.close();
      
      assertEquals(0, getReferenceIds().size());
      
      assertEquals(0, getMessageIds().size());      
   }
   
   /*
    * Test that all messages on a temporary topic are removed on close
    */
   public void testTemporaryTopicClose() throws Exception
   {
      if (ServerManagement.isRemote()) return;
      
      String objectName = "somedomain:service=TempTopicConnectionFactory";
      String[] jndiBindings = new String[] { "/TempTopicConnectionFactory" };

      ServerManagement.deployConnectionFactory(objectName, jndiBindings, 150, 100, 10, 10);

      ConnectionFactory cf2 = (ConnectionFactory)ic.lookup("/TempTopicConnectionFactory");
      
      Connection conn = cf2.createConnection();
      
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      TemporaryTopic topic = sess.createTemporaryTopic();
      
      MessageProducer prod = sess.createProducer(topic);
      
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
           
      sess.createConsumer(topic);
      
      sess.createConsumer(topic);
      
      //Don't start the connection
      
      for (int i = 0; i < 150; i++)
      {
         prod.send(sess.createMessage());
      }
      
      SimpleMessageStore ms = (SimpleMessageStore)ServerManagement.getMessageStore();

      assertEquals(100, getReferenceIds().size());
      
      assertEquals(50, getMessageIds().size());
      
      //Now we close the connection
      
      conn.close();
        
      assertEquals(0, getReferenceIds().size());
      
      assertEquals(0, getMessageIds().size());      
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
    
   // Inner classes -------------------------------------------------      
}


