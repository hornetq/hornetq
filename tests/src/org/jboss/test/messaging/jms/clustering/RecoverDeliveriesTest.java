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
import java.util.Map;
import java.util.Set;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.jboss.jms.client.FailoverEvent;
import org.jboss.jms.client.JBossConnection;
import org.jboss.jms.client.delegate.ClientClusteredConnectionFactoryDelegate;
import org.jboss.messaging.util.MessageQueueNameHelper;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.container.Server;


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
   
   public void testCancelTransactional() throws Exception
   {
   	cancel(true);
   }
   
   public void testCancelNonTransactional() throws Exception
   {
   	cancel(false);
   }
   
   public void testDurableSubTransactional() throws Exception
   {
   	durableSub(true);
   }
   
   public void testDurableSubNonTransactional() throws Exception
   {
   	durableSub(false);
   }
   
   
   public void testTimeout() throws Exception
   {
   	final long timeout = 20 * 1000;
   	
   	long oldTimeout = 0;
   	
   	((ClientClusteredConnectionFactoryDelegate)cf.getDelegate()).setSupportsFailover(false);
   	
      Connection conn1 = createConnectionOnServer(cf,1);
      
      Connection conn2 = null;
 
      try
      {      	
      	for (int i = 0; i < ServerManagement.MAX_SERVER_COUNT; i++)
      	{
      		Server server = ServerManagement.getServer(i);
      		
      		if (server != null)
      		{
      			oldTimeout = ((Long)server.getAttribute(ServerManagement.getServerPeerObjectName(), "RecoverDeliveriesTimeout")).longValue();
      			server.setAttribute(ServerManagement.getServerPeerObjectName(), "RecoverDeliveriesTimeout", String.valueOf(timeout));      	      	               
      		}
      	}
      	
      	ServerManagement.deployQueue("timeoutQueue", 0);
      	ServerManagement.deployQueue("timeoutQueue", 1);
      	ServerManagement.deployQueue("timeoutQueue", 2);
      	
      	Queue timeoutQueue = (Queue)ic[1].lookup("/queue/timeoutQueue");
      	
         Session sessSend = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      		
      	MessageProducer prod1 = sessSend.createProducer(timeoutQueue);
      	
      	final int numMessages = 10;
      	
      	for (int i = 0; i < numMessages; i++)
      	{
      		TextMessage tm = sessSend.createTextMessage("message" + i);
      		
      		prod1.send(tm);      		
      	}
      	
      	Session sess1 = conn1.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      	
      	MessageConsumer cons1 = sess1.createConsumer(timeoutQueue);
      
      	
      	conn1.start();
      	
      	TextMessage tm = null;
      	
      	for (int i = 0; i < numMessages; i++)
      	{
      		tm = (TextMessage)cons1.receive(2000);
      		
      		assertNotNull(tm);
      		
      		assertEquals("message" + i, tm.getText());
      	}
      		
      	int failoverNodeId = this.getFailoverNodeForNode(cf, 1);
      	
      	int recoveryMapSize = ServerManagement.getServer(failoverNodeId).getRecoveryMapSize(timeoutQueue.getQueueName());
      	assertEquals(0, recoveryMapSize);
      	Map recoveryArea = ServerManagement.getServer(failoverNodeId).getRecoveryArea(timeoutQueue.getQueueName());
      	Map ids = (Map)recoveryArea.get(new Integer(1));
      	assertNotNull(ids);
      	assertEquals(numMessages, ids.size());
      	
      	//First turn OFF failover on the connection factory
      	
      	ServerManagement.kill(1);

         log.info("########");
         log.info("######## KILLED NODE 1");
         log.info("########");
         
         //Failover won't occur on the client
         
         //Let's give it enough time to happen on the server
         
         Thread.sleep(10000);
         
         recoveryMapSize = ServerManagement.getServer(failoverNodeId).getRecoveryMapSize(timeoutQueue.getQueueName());
      	assertEquals(numMessages, recoveryMapSize);
      	recoveryArea = ServerManagement.getServer(failoverNodeId).getRecoveryArea(timeoutQueue.getQueueName());
      	ids = (Map)recoveryArea.get(new Integer(1));
      	assertNull(ids);
 
      	//Now we wait for the timeout period
      	
      	log.info("Waiting for timeout");
      	Thread.sleep(timeout + 1000);
      	log.info("Waited");
      	
      	recoveryMapSize = ServerManagement.getServer(failoverNodeId).getRecoveryMapSize(timeoutQueue.getQueueName());
      	assertEquals(0, recoveryMapSize);
      	recoveryArea = ServerManagement.getServer(failoverNodeId).getRecoveryArea(timeoutQueue.getQueueName());
      	ids = (Map)recoveryArea.get(new Integer(1));
      	assertNull(ids);
      	
      	//Now we should be able to consume the refs again
      	
      	conn1.close();
      	
      	log.info("Creating connection");
      	
      	conn2 = createConnectionOnServer(cf, failoverNodeId);
                     
       	Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
       	
       	MessageConsumer cons2 = sess2.createConsumer(timeoutQueue);
              	
       	conn2.start();
       	
       	Set msgs = new HashSet();
       	for (int i = 0; i < numMessages; i++)
       	{
       		tm = (TextMessage)cons2.receive(2000);
       		
       		assertNotNull(tm);
       		
       		log.info("Got message:" + tm.getText());
       		
       		msgs.add(tm.getText());
       	}
       	
       	for (int i = 0; i < numMessages; i++)
       	{
       		assertTrue(msgs.contains("message" + i));
       	}
       	
       	tm = (TextMessage)cons2.receive(5000);
       	
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
         
         ((ClientClusteredConnectionFactoryDelegate)cf.getDelegate()).setSupportsFailover(true);
         
         for (int i = 0; i < ServerManagement.MAX_SERVER_COUNT; i++)
      	{
      		Server server = ServerManagement.getServer(i);
      		
      		if (server != null)
      		{
      			server.setAttribute(ServerManagement.getServerPeerObjectName(), "RecoverDeliveriesTimeout", String.valueOf(oldTimeout));      	      	               
      		}
      	}
      }
   }
   
   

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   protected void setUp() throws Exception
   {
      nodeCount = 3;

      super.setUp();
   }
   
   // Private -------------------------------------------------------

   private void simple(boolean transactional) throws Exception
   {
      Connection conn1 = createConnectionOnServer(cf,1);
 
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
      	
      	int failoverNodeId = this.getFailoverNodeForNode(cf, 1);
      	
      	int recoveryMapSize = ServerManagement.getServer(failoverNodeId).getRecoveryMapSize(queue[failoverNodeId].getQueueName());
      	assertEquals(0, recoveryMapSize);
      	Map recoveryArea = ServerManagement.getServer(failoverNodeId).getRecoveryArea(queue[failoverNodeId].getQueueName());
      	Map ids = (Map)recoveryArea.get(new Integer(1));
      	assertNotNull(ids);
      	assertEquals(numMessages, ids.size());
      	
      	ServerManagement.kill(1);

         log.info("########");
         log.info("######## KILLED NODE 1");
         log.info("########");

         // wait for the client-side failover to complete

         log.info("Waiting for failover to complete");
         
         while(true)
         {
            FailoverEvent event = failoverListener.getEvent(30000);
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
         
         recoveryMapSize = ServerManagement.getServer(failoverNodeId).getRecoveryMapSize(queue[failoverNodeId].getQueueName());
      	assertEquals(0, recoveryMapSize);
      	recoveryArea = ServerManagement.getServer(failoverNodeId).getRecoveryArea(queue[failoverNodeId].getQueueName());
      	ids = (Map)recoveryArea.get(new Integer(1));
      	assertNull(ids);
         
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
   
  
  
   
   private void durableSub(boolean transactional) throws Exception
   {
      Connection conn1 = createConnectionOnServer(cf,1);
 
      try
      {
      	String clientID = "I am sick of writing these fucking tests!!! AAAAAAAAAARRRRRRRRRRGGGGGGGHHHHHHH";
      	conn1.setClientID(clientID);
      	
      	SimpleFailoverListener failoverListener = new SimpleFailoverListener();
         ((JBossConnection)conn1).registerFailoverListener(failoverListener);
         
         Session sess1 = conn1.createSession(transactional, transactional ? Session.SESSION_TRANSACTED : Session.CLIENT_ACKNOWLEDGE);
      	
         String subName = "ooooooooo matron!!";
               	
         MessageConsumer sub = sess1.createDurableSubscriber(topic[1], subName);
      	
      	final int numMessages = 10;
      	         
         {         
	         Session sessSend = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
	               		
	      	MessageProducer prod1 = sessSend.createProducer(topic[1]);
	      	
	      	for (int i = 0; i < numMessages; i++)
	      	{
	      		TextMessage tm = sessSend.createTextMessage("message" + i);
	      		
	      		prod1.send(tm);      		
	      	}
	      	
	      	sessSend.close();      	
         }      	      	      	      	
         
         String queueName = MessageQueueNameHelper.createSubscriptionName(clientID, subName);
         
         log.info("queuename is:" + queueName);
               	
      	conn1.start();
      	
      	TextMessage tm = null;
      	
      	for (int i = 0; i < numMessages; i++)
      	{
      		tm = (TextMessage)sub.receive(2000);
      		
      		assertNotNull(tm);
      		
      		assertEquals("message" + i, tm.getText());
      	}
      	
      	//Don't ack
      	
      	//Now kill server
      	
      	int failoverNodeId = this.getFailoverNodeForNode(cf, 1);
      	
      	log.info("Failover node is " + failoverNodeId);
      	
      	Thread.sleep(5000);
      	
      	int recoveryMapSize = ServerManagement.getServer(failoverNodeId).getRecoveryMapSize(queueName);
      	assertEquals(0, recoveryMapSize);
      	Map recoveryArea = ServerManagement.getServer(failoverNodeId).getRecoveryArea(queueName);
      	Map ids = (Map)recoveryArea.get(new Integer(1));
      	assertNotNull(ids);
      	assertEquals(numMessages, ids.size());
      	
      	ServerManagement.kill(1);

         log.info("########");
         log.info("######## KILLED NODE 1");
         log.info("########");

         // wait for the client-side failover to complete

         log.info("Waiting for failover to complete");
         
         while(true)
         {
            FailoverEvent event = failoverListener.getEvent(30000);
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
         
         recoveryMapSize = ServerManagement.getServer(failoverNodeId).getRecoveryMapSize(queueName);
      	assertEquals(0, recoveryMapSize);
      	recoveryArea = ServerManagement.getServer(failoverNodeId).getRecoveryArea(queueName);
      	ids = (Map)recoveryArea.get(new Integer(1));
      	assertNull(ids);
         
         log.info("acked");
         
         sess1.close();
         
         log.info("closed");
         
	      sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      
	      log.info("created new session");
      	
      	MessageConsumer cons1 = sess1.createConsumer(topic[1]);
      	
      	log.info("Created consumer");
      	
         //Messages should be gone
      	
         tm = (TextMessage)cons1.receive(5000);
      		
      	assertNull(tm);      		
      	
      	sess1.unsubscribe(subName);
      }
      finally
      {
         if (conn1 != null)
         {
            conn1.close();
         }
      }
   }
   
   private void cancel(boolean transactional) throws Exception
   {
      Connection conn1 = createConnectionOnServer(cf,1);
 
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
      	
      	int failoverNodeId = this.getFailoverNodeForNode(cf, 1);      	      	

      	int recoveryMapSize = ServerManagement.getServer(failoverNodeId).getRecoveryMapSize(queue[failoverNodeId].getQueueName());
      	assertEquals(0, recoveryMapSize);
      	Map recoveryArea = ServerManagement.getServer(failoverNodeId).getRecoveryArea(queue[failoverNodeId].getQueueName());
      	Map ids = (Map)recoveryArea.get(new Integer(1));
      	assertNotNull(ids);
      	assertEquals(numMessages, ids.size());
      	
      	//Now cancel the session
      	
      	sess1.close();
      	
      	Thread.sleep(5000);
      	
      	//Ensure the dels are removed from the backup
      	
      	recoveryMapSize = ServerManagement.getServer(failoverNodeId).getRecoveryMapSize(queue[failoverNodeId].getQueueName());
      	assertEquals(0, recoveryMapSize);
      	recoveryArea = ServerManagement.getServer(failoverNodeId).getRecoveryArea(queue[failoverNodeId].getQueueName());
      	ids = (Map)recoveryArea.get(new Integer(1));
      	assertNull(ids);
 
      
      }
      finally
      {
         if (conn1 != null)
         {
            conn1.close();
         }
         
         this.removeAllMessages(queue[0].getQueueName(), true, 1);
      }
   }
   
   
   
   private void connectionOnNewNode(boolean transactional) throws Exception
   {
      Connection conn1 = createConnectionOnServer(cf,1);
      
      Connection conn2 = createConnectionOnServer(cf,2);
 
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
      	
      	int failoverNodeId = this.getFailoverNodeForNode(cf, 1);      	
      	
      	int recoveryMapSize = ServerManagement.getServer(failoverNodeId).getRecoveryMapSize(queue[failoverNodeId].getQueueName());
      	assertEquals(0, recoveryMapSize);
      	Map recoveryArea = ServerManagement.getServer(failoverNodeId).getRecoveryArea(queue[failoverNodeId].getQueueName());
      	Map ids = (Map)recoveryArea.get(new Integer(1));
      	assertNotNull(ids);
      	assertEquals(numMessages, ids.size());
      	
      	ServerManagement.kill(1);

         log.info("########");
         log.info("######## KILLED NODE 1");
         log.info("########");

         // wait for the client-side failover to complete

         log.info("Waiting for failover to complete");
         
         while(true)
         {
            FailoverEvent event = failoverListener.getEvent(30000);
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
         
         recoveryMapSize = ServerManagement.getServer(failoverNodeId).getRecoveryMapSize(queue[failoverNodeId].getQueueName());
      	assertEquals(0, recoveryMapSize);
      	recoveryArea = ServerManagement.getServer(failoverNodeId).getRecoveryArea(queue[failoverNodeId].getQueueName());
      	ids = (Map)recoveryArea.get(new Integer(1));
      	assertNull(ids);
         
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
      Connection conn0 = createConnectionOnServer(cf, 0);
      
      Connection conn1 = createConnectionOnServer(cf, 1);
      
      Connection conn2 = createConnectionOnServer(cf, 2);
 
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
      	
      	int failoverNodeId = this.getFailoverNodeForNode(cf, 1);    
      	
      	ServerManagement.kill(1);

         log.info("########");
         log.info("######## KILLED NODE 1");
         log.info("########");

         // wait for the client-side failover to complete

         log.info("Waiting for failover to complete");
         
         while(true)
         {
            FailoverEvent event = failoverListener.getEvent(30000);
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
         
         int recoveryMapSize = ServerManagement.getServer(failoverNodeId).getRecoveryMapSize(queue[failoverNodeId].getQueueName());
      	assertEquals(0, recoveryMapSize);
      	Map recoveryArea = ServerManagement.getServer(failoverNodeId).getRecoveryArea(queue[failoverNodeId].getQueueName());
      	Map ids = (Map)recoveryArea.get(new Integer(1));
      	assertNull(ids);
          
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
         
         sess0_1.close();
         sess0_2.close();
         sess1_1.close();
         sess1_2.close();
         sess2_1.close();
         sess2_2.close();
         
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
