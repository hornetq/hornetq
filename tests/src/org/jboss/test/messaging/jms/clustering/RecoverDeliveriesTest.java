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
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.jboss.jms.client.FailoverEvent;
import org.jboss.jms.client.JBossConnection;
import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.test.messaging.tools.ServerManagement;


/**
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: $</tt>7 Jul 2007
 *
 * $Id: $
 *
 */
public class RecoverDeliveriesTest extends ClusteringTestBase
{
   
   // Constants -----------------------------------------------------
   
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
	
	/*
	 * Need to test acknowledging with and without transaction
	 * 
	 * Test timeout
	 * 
	 * Test multiple failover
	 * 
	 * Test nothing left in map or area
	 * 
	 * Multiple queues in same session
	 * 
	 * Queues
	 * 
	 * multiple durable subs on same topic
	 * 
	 * Temporary queues
	 */
	
   public RecoverDeliveriesTest(String name)
   {
      super(name);
   }
   
   // Public --------------------------------------------------------
   
   public void testSimpleTransactional() throws Exception
   {
   	this.simple(true);
   }
   
   public void testSimpleNonTransactional() throws Exception
   {
   	this.simple(false);
   }
      
   public void testWithConnectionOnNewNodeTransactional() throws Exception
   {
   	connectionOnNewNode(true);
   }
   
   public void testWithConnectionOnNewNodeNonTransactional() throws Exception
   {
   	connectionOnNewNode(false);
   }
         
   public void testWithConnectionsOnAllNodesTransactional() throws Exception
   {
   	connectionsOnAllNodes(true);
   }
   
   public void testWithConnectionsOnAllNodesNonTransactional() throws Exception
   {
   	connectionsOnAllNodes(false);
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
   
   // Private -------------------------------------------------------

   private void simple(boolean transactional) throws Exception
   {
   	JBossConnectionFactory factory = (JBossConnectionFactory) ic[0].lookup("/ClusteredConnectionFactory");

      Connection conn1 = createConnectionOnServer(factory,1);
 
      try
      {
      	SimpleFailoverListener failoverListener = new SimpleFailoverListener();
         ((JBossConnection)conn1).registerFailoverListener(failoverListener);
      	
         Session sessSend = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      		
      	MessageProducer prod1 = sessSend.createProducer(queue[1]);
      	
      	final int numMessages = 10;
      	
      	for (int i = 0; i < numMessages; i++)
      	{
      		TextMessage tm = sessSend.createTextMessage("message" + i);
      		
      		prod1.send(tm);      		
      	}
      	
      	Session sess1 = conn1.createSession(transactional, transactional ? Session.SESSION_TRANSACTED : Session.CLIENT_ACKNOWLEDGE);
      	
      	MessageConsumer cons1 = sess1.createConsumer(queue[1]);
      
      	
      	conn1.start();
      	
      	TextMessage tm = null;
      	
      	for (int i = 0; i < numMessages; i++)
      	{
      		tm = (TextMessage)cons1.receive(2000);
      		
      		assertNotNull(tm);
      		
      		assertEquals("message" + i, tm.getText());
      	}
      	
      	//Don't ack
      	
      	//Now kill server
      	
      	int failoverNodeId = this.getFailoverNodeForNode(factory, 1);
      	
      	ServerManagement.kill(1);

         log.info("########");
         log.info("######## KILLED NODE 1");
         log.info("########");

         // wait for the client-side failover to complete

         log.info("Waiting for failover to complete");
         
         while(true)
         {
            FailoverEvent event = failoverListener.getEvent(120000);
            if (event != null && FailoverEvent.FAILOVER_COMPLETED == event.getType())
            {
               break;
            }
            if (event == null)
            {
               fail("Did not get expected FAILOVER_COMPLETED event");
            }
         }
         
         log.info("Failover completed");
         
         assertEquals(failoverNodeId, getServerId(conn1));
                  
         //Now ack
         if (transactional)
         {
         	sess1.commit();
         }
         else
         {
         	tm.acknowledge();
         }
         
         log.info("acked");
         
         sess1.close();
         
         log.info("closed");
         
	      sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      
	      log.info("created new session");
      	
      	cons1 = sess1.createConsumer(queue[1]);
      	
      	log.info("Created consumer");
      	
         //Messages should be gone
      	
         tm = (TextMessage)cons1.receive(5000);
      		
      	assertNull(tm);      		
      }
      finally
      {
         if (conn1 != null)
         {
            conn1.close();
         }
      }
   }
   
   
   private void killFailoverNode(boolean transactional) throws Exception
   {
   	JBossConnectionFactory factory = (JBossConnectionFactory) ic[0].lookup("/ClusteredConnectionFactory");

      Connection conn1 = createConnectionOnServer(factory,1);
 
      try
      {
      	SimpleFailoverListener failoverListener = new SimpleFailoverListener();
         ((JBossConnection)conn1).registerFailoverListener(failoverListener);
      	
         Session sessSend = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      		
      	MessageProducer prod1 = sessSend.createProducer(queue[1]);
      	
      	final int numMessages = 10;
      	
      	for (int i = 0; i < numMessages; i++)
      	{
      		TextMessage tm = sessSend.createTextMessage("message" + i);
      		
      		prod1.send(tm);      		
      	}
      	
      	Session sess1 = conn1.createSession(transactional, transactional ? Session.SESSION_TRANSACTED : Session.CLIENT_ACKNOWLEDGE);
      	
      	MessageConsumer cons1 = sess1.createConsumer(queue[1]);
      
      	
      	conn1.start();
      	
      	TextMessage tm = null;
      	
      	for (int i = 0; i < numMessages; i++)
      	{
      		tm = (TextMessage)cons1.receive(2000);
      		
      		assertNotNull(tm);
      		
      		assertEquals("message" + i, tm.getText());
      	}
      	
      	//Don't ack
      	
      	//We kill the failover node for node 1
      	int failoverNodeId = this.getFailoverNodeForNode(factory, 1);
      	
      	log.info("Killing failover node:" + failoverNodeId);
      	
      	ServerManagement.kill(failoverNodeId);
      	
      	log.info("Killed failover node");
      	
      	Thread.sleep(5000);
      	
      	//Now kill 
      	
      	failoverNodeId = this.getFailoverNodeForNode(factory, 1);
      	
      	log.info("Failover node id is now " + failoverNodeId);
      	
      	ServerManagement.kill(1);

         log.info("########");
         log.info("######## KILLED NODE 1");
         log.info("########");

         // wait for the client-side failover to complete

         log.info("Waiting for failover to complete");
         
         while(true)
         {
            FailoverEvent event = failoverListener.getEvent(120000);
            if (event != null && FailoverEvent.FAILOVER_COMPLETED == event.getType())
            {
               break;
            }
            if (event == null)
            {
               fail("Did not get expected FAILOVER_COMPLETED event");
            }
         }
         
         log.info("Failover completed");
         
         assertEquals(failoverNodeId, getServerId(conn1));
                  
         //Now ack
         if (transactional)
         {
         	sess1.commit();
         }
         else
         {
         	tm.acknowledge();
         }
         
         log.info("acked");
         
         sess1.close();
         
         log.info("closed");
         
	      sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      
	      log.info("created new session");
      	
      	cons1 = sess1.createConsumer(queue[1]);
      	
      	log.info("Created consumer");
      	
         //Messages should be gone
      	
         tm = (TextMessage)cons1.receive(5000);
      		
      	assertNull(tm);      		
      }
      finally
      {
         if (conn1 != null)
         {
            conn1.close();
         }
      }
   }
   
   
   
   private void connectionOnNewNode(boolean transactional) throws Exception
   {
   	JBossConnectionFactory factory = (JBossConnectionFactory) ic[0].lookup("/ClusteredConnectionFactory");

      Connection conn1 = createConnectionOnServer(factory,1);
      
      Connection conn2 = createConnectionOnServer(factory,2);
 
      try
      {
      	SimpleFailoverListener failoverListener = new SimpleFailoverListener();
         ((JBossConnection)conn1).registerFailoverListener(failoverListener);
      	
         Session sessSend = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);

      	MessageProducer prod1 = sessSend.createProducer(queue[1]);
      	
      	final int numMessages = 10;
      	
      	for (int i = 0; i < numMessages; i++)
      	{
      		TextMessage tm = sessSend.createTextMessage("message" + i);
      		
      		prod1.send(tm);      		
      	}
      	
      	Session sess1 = conn1.createSession(transactional, transactional ? Session.SESSION_TRANSACTED : Session.CLIENT_ACKNOWLEDGE);
      	
      	MessageConsumer cons1 = sess1.createConsumer(queue[1]);
      	      	
      	conn1.start();
      	
      	TextMessage tm = null;
      	
      	for (int i = 0; i < numMessages; i++)
      	{
      		tm = (TextMessage)cons1.receive(2000);
      		
      		assertNotNull(tm);
      		
      		assertEquals("message" + i, tm.getText());
      	}
      	
      	//Create another connection on server 2 with a consumer
      	
      	Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      	
      	MessageConsumer cons2 = sess2.createConsumer(queue[2]);
      	
      	conn2.start();
      	
      	//Don't ack
      	
      	//Now kill server
      	
      	int failoverNodeId = this.getFailoverNodeForNode(factory, 1);      	
      	
      	ServerManagement.kill(1);

         log.info("########");
         log.info("######## KILLED NODE 1");
         log.info("########");

         // wait for the client-side failover to complete

         log.info("Waiting for failover to complete");
         
         while(true)
         {
            FailoverEvent event = failoverListener.getEvent(120000);
            if (event != null && FailoverEvent.FAILOVER_COMPLETED == event.getType())
            {
               break;
            }
            if (event == null)
            {
               fail("Did not get expected FAILOVER_COMPLETED event");
            }
         }
         
         log.info("Failover completed");
         
         assertEquals(failoverNodeId, getServerId(conn1));
                  
         if (transactional)
         {
         	sess1.commit();
         }
         else
         {
	         //Now ack
	         tm.acknowledge();
         }
         
         log.info("acked");
         
         sess1.close();
         
         log.info("closed");
         
	      sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      
	      log.info("created new session");
      	
      	cons1 = sess1.createConsumer(queue[1]);
      	
      	log.info("Created consumer");
      	
         //Messages should be gone
      	
         tm = (TextMessage)cons1.receive(5000);
      		
      	assertNull(tm);      		
      }
      finally
      {
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
   
   private void connectionsOnAllNodes(boolean transactional) throws Exception
   {
   	JBossConnectionFactory factory = (JBossConnectionFactory) ic[0].lookup("/ClusteredConnectionFactory");

      Connection conn0 = createConnectionOnServer(factory, 0);
      
      Connection conn1 = createConnectionOnServer(factory, 1);
      
      Connection conn2 = createConnectionOnServer(factory, 2);
 
      try
      {
      	SimpleFailoverListener failoverListener = new SimpleFailoverListener();
         ((JBossConnection)conn1).registerFailoverListener(failoverListener);
         
      	conn0.start();
      	
      	conn1.start();
      	
      	conn2.start();
      	
      	//Send a load of messages on node 0
      	
      	Session sessSend = conn0.createSession(false, Session.AUTO_ACKNOWLEDGE);
      	
      	MessageProducer prod0 = sessSend.createProducer(queue[0]);
      	
      	      	
      	final int numMessages = 60;
      	
      	for (int i = 0; i < numMessages; i++)
      	{
      		TextMessage tm = sessSend.createTextMessage("message-" + i);
      		
      		prod0.send(tm);      		
      	}
      	      	
      	      	         
      	Session sess0_1 = conn0.createSession(transactional, transactional ? Session.SESSION_TRANSACTED : Session.CLIENT_ACKNOWLEDGE);
      	
      	MessageConsumer cons0_1 = sess0_1.createConsumer(queue[0]);      	
      	
      	Set msgIds = new HashSet();
      	      	      	
      	TextMessage tm0_1 = null;
      	
      	for (int i = 0; i < numMessages / 6; i++)
      	{
      		tm0_1 = (TextMessage)cons0_1.receive(5000000);
      		
      		assertNotNull(tm0_1);
      		
      		log.info("got:" + tm0_1.getText());      		
      		
      		msgIds.add(tm0_1.getText());
      	}
      	
      	cons0_1.close();
      	
      	Session sess0_2 = conn0.createSession(transactional, transactional ? Session.SESSION_TRANSACTED : Session.CLIENT_ACKNOWLEDGE);
      	
      	MessageConsumer cons0_2 = sess0_2.createConsumer(queue[0]);
      	
      	TextMessage tm0_2 = null;
      	
      	for (int i = 0; i < numMessages / 6; i++)
      	{
      		tm0_2 = (TextMessage)cons0_2.receive(5000000);
      		
      		assertNotNull(tm0_2);
      		
      		log.info("got:" + tm0_2.getText());      		
      		
      		msgIds.add(tm0_2.getText());
      	}
      	
      	cons0_2.close();
      	
      	
      	//Two on node 1
      	
      	Session sess1_1 = conn1.createSession(transactional, transactional ? Session.SESSION_TRANSACTED : Session.CLIENT_ACKNOWLEDGE);
      	
      	MessageConsumer cons1_1 = sess1_1.createConsumer(queue[1]);      	
      	
      	TextMessage tm1_1 = null;
      	
      	for (int i = 0; i < numMessages / 6; i++)
      	{
      		tm1_1 = (TextMessage)cons1_1.receive(5000000);
      		
      		assertNotNull(tm1_1);
      		
      		log.info("got:" + tm1_1.getText());      		
      		
      		msgIds.add(tm1_1.getText());
      	}
      	
      	cons1_1.close();
      	
      	Session sess1_2 = conn1.createSession(transactional, transactional ? Session.SESSION_TRANSACTED : Session.CLIENT_ACKNOWLEDGE);
      	
      	MessageConsumer cons1_2 = sess1_2.createConsumer(queue[1]);
      	
      	TextMessage tm1_2 = null;
      	
      	for (int i = 0; i < numMessages / 6; i++)
      	{
      		tm1_2 = (TextMessage)cons1_2.receive(5000000);
      		      		      		
      		assertNotNull(tm1_2);
      		
      		log.info("got:" + tm1_2.getText());
      		
      		msgIds.add(tm1_2.getText());
      	}
      	
      	cons1_2.close();
      	
      	
      	//Two on node 2
      	
      	Session sess2_1 = conn2.createSession(transactional, transactional ? Session.SESSION_TRANSACTED : Session.CLIENT_ACKNOWLEDGE);
      	
      	MessageConsumer cons2_1 = sess2_1.createConsumer(queue[2]);
      	
      	TextMessage tm2_1 = null;
      	
      	for (int i = 0; i < numMessages / 6; i++)
      	{
      		tm2_1 = (TextMessage)cons2_1.receive(5000000);
      		
      		assertNotNull(tm2_1);
      		
      		log.info("got:" + tm2_1.getText());      		
      		
      		msgIds.add(tm2_1.getText());
      	}
      	
      	cons2_1.close();
      	
      	Session sess2_2 = conn2.createSession(transactional, transactional ? Session.SESSION_TRANSACTED : Session.CLIENT_ACKNOWLEDGE);
      	
      	MessageConsumer cons2_2 = sess2_2.createConsumer(queue[2]);
      	
      	TextMessage tm2_2 = null;
      	
      	for (int i = 0; i < numMessages / 6; i++)
      	{
      		tm2_2 = (TextMessage)cons2_2.receive(5000000);
      		
      		assertNotNull(tm2_2);
      		
      		log.info("got:" + tm2_2.getText());
      		
      		msgIds.add(tm2_2.getText());
      	}
      	
      	cons2_2.close();
      	
      	assertEquals(numMessages, msgIds.size());
      	
      	for (int i = 0; i < numMessages; i++)
      	{
      		assertTrue(msgIds.contains("message-" + i));
      	}
      	
      	//Don't ack
      	
      	//Now kill server
      	
      	int failoverNodeId = this.getFailoverNodeForNode(factory, 1);    
      	
      	log.info("Failover node is " + failoverNodeId);
      	
      	ServerManagement.kill(1);

         log.info("########");
         log.info("######## KILLED NODE 1");
         log.info("########");

         // wait for the client-side failover to complete

         log.info("Waiting for failover to complete");
         
         while(true)
         {
            FailoverEvent event = failoverListener.getEvent(120000);
            if (event != null && FailoverEvent.FAILOVER_COMPLETED == event.getType())
            {
               break;
            }
            if (event == null)
            {
               fail("Did not get expected FAILOVER_COMPLETED event");
            }
         }
         
         log.info("Failover completed");
                           
         log.info("server id is now " + getServerId(conn1));
         
         assertEquals(failoverNodeId, getServerId(conn1));
         
         log.info("ok, committing");
                  
         //Now ack
         if (transactional)
         {
         	sess0_1.commit();
         	sess0_2.commit();
         	sess1_1.commit();
         	sess1_2.commit();
         	sess2_1.commit();
         	sess2_2.commit();
         }
         else
         {
	         tm0_1.acknowledge();
	         tm0_2.acknowledge();
	         tm1_1.acknowledge();
	         tm1_2.acknowledge();
	         tm2_1.acknowledge();
	         tm2_2.acknowledge();
         }
         
         log.info("acked");
         
         sess0_1.close();
         sess0_2.close();
         sess1_1.close();
         sess1_2.close();
         sess2_1.close();
         sess2_2.close();
         
         log.info("closed");
         
	      Session sess0 = conn0.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      	
      	MessageConsumer cons0 = sess0.createConsumer(queue[0]);
      	
         Message msg = cons0.receive(5000);         
      		
      	assertNull(msg);      		
      	
      	Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      	
      	MessageConsumer cons1 = sess1.createConsumer(queue[1]);
      	
         msg = cons1.receive(5000);
      		
      	assertNull(msg);      		
      	
      	Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      	
      	MessageConsumer cons2 = sess2.createConsumer(queue[2]);
      	
         msg = cons2.receive(5000);
      		
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
   
   // Inner classes -------------------------------------------------
   
}
