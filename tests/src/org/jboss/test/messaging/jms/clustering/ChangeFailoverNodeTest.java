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

import java.util.Iterator;
import java.util.Map;

import javax.jms.Connection;
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
 * @version <tt>$Revision: $</tt>8 Jul 2007
 *
 * $Id: $
 *
 */
public class ChangeFailoverNodeTest extends ClusteringTestBase
{
   
   // Constants -----------------------------------------------------
   
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   	
	
   public ChangeFailoverNodeTest(String name)
   {
      super(name);
   }
   
   // Public --------------------------------------------------------
   
   public void testKillFailoverNodeTransactional() throws Exception
   {
   	this.killFailoverNode(true);
   }
   
   public void testKillFailoverNodeNonTransactional() throws Exception
   {
   	this.killFailoverNode(false);
   }      
   
   public void testStopFailoverNodeTransactional() throws Exception
   {
   	this.stopFailoverNode(true);
   }
   
   public void testStopFailoverNodeNonTransactional() throws Exception
   {
   	this.stopFailoverNode(false);
   }
      
   public void testAddNodeToGetNewFailoverNodeNonTransactional() throws Exception
   {
   	this.addNodeToGetNewFailoverNode(false);
   }

   public void testAddNodeToGetNewFailoverNodeTransactional() throws Exception
   {
   	this.addNodeToGetNewFailoverNode(true);
   }
   
   public void testKillTwoFailoverNodesNonTransactional() throws Exception
   {
   	this.killTwoFailoverNodes(false);
   }
   
   public void testKillTwoFailoverNodesTransactional() throws Exception
   {
   	this.killTwoFailoverNodes(true);
   }
   
   public void testKillAllToOneAndBackAgainNonTransactional() throws Exception
   {
   	this.killAllToOneAndBackAgain(false);
   }
   
   public void testKillAllToOneAndBackAgainTransactional() throws Exception
   {
   	this.killAllToOneAndBackAgain(true);
   }   
     
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   protected void setUp() throws Exception
   {
      nodeCount = 4;

      super.setUp();

      log.debug("setup done");
   }
   
   protected void tearDown() throws Exception
   {   	   	
      super.tearDown();
            
      for (int i = 0; i < nodeCount; i++)
      {
         if (ServerManagement.isStarted(i))
         {
            ServerManagement.log(ServerManagement.INFO, "Undeploying Server " + i, i);
            ServerManagement.stop(i);
         }
      }
   }
   
   // Private -------------------------------------------------------

   private void killAllToOneAndBackAgain(boolean transactional) throws Exception
   {
   	JBossConnectionFactory factory = (JBossConnectionFactory) ic[0].lookup("/ClusteredConnectionFactory");

      Connection conn0 = createConnectionOnServer(factory, 0);
 
      try
      {
      	SimpleFailoverListener failoverListener = new SimpleFailoverListener();
         ((JBossConnection)conn0).registerFailoverListener(failoverListener);
      	
         Session sessSend = conn0.createSession(false, Session.AUTO_ACKNOWLEDGE);
      		
      	MessageProducer prod0 = sessSend.createProducer(queue[0]);
      	
      	final int numMessages = 10;
      	
      	for (int i = 0; i < numMessages; i++)
      	{
      		TextMessage tm = sessSend.createTextMessage("message" + i);
      		
      		prod0.send(tm);      		
      	}
      	
      	sessSend.close();
      	
      	Session sess0 = conn0.createSession(transactional, transactional ? Session.SESSION_TRANSACTED : Session.CLIENT_ACKNOWLEDGE);
      	
      	MessageConsumer cons0 = sess0.createConsumer(queue[0]);
      
      	
      	conn0.start();
      	
      	TextMessage tm = null;
      	
      	for (int i = 0; i < numMessages; i++)
      	{
      		tm = (TextMessage)cons0.receive(2000);
      		
      		assertNotNull(tm);
      		
      		assertEquals("message" + i, tm.getText());
      	}
      	
      	//Don't ack
      	
      	int failoverNodeId = this.getFailoverNodeForNode(factory, 0);
      	
      	log.info("Failover node for node 0 is " + failoverNodeId);
      	
      	int recoveryMapSize = ServerManagement.getServer(failoverNodeId).getRecoveryMapSize(queue[failoverNodeId].getQueueName());
      	assertEquals(0, recoveryMapSize);
      	Map recoveryArea = ServerManagement.getServer(failoverNodeId).getRecoveryArea(queue[failoverNodeId].getQueueName());
      	Map ids = (Map)recoveryArea.get(new Integer(0));
      	assertNotNull(ids);
      	assertEquals(numMessages, ids.size());
      	
      	//Now kill the failover node
      	
      	log.info("killing node " + failoverNodeId);
      	ServerManagement.kill(failoverNodeId);
      	
      	Thread.sleep(5000);
      	
      	int newFailoverNodeId = this.getFailoverNodeForNode(factory, 0);      	    
      	
      	recoveryMapSize = ServerManagement.getServer(newFailoverNodeId).getRecoveryMapSize(queue[newFailoverNodeId].getQueueName());
      	assertEquals(0, recoveryMapSize);
      	recoveryArea = ServerManagement.getServer(newFailoverNodeId).getRecoveryArea(queue[newFailoverNodeId].getQueueName());
      	ids = (Map)recoveryArea.get(new Integer(0));
      	assertNotNull(ids);
      	assertEquals(numMessages, ids.size());
      	
      	//Now kill the second failover node
      	
      	log.info("killing node " + newFailoverNodeId);
      	ServerManagement.kill(newFailoverNodeId);
      	
      	Thread.sleep(5000);
      	
      	int evennewerFailoverNodeId = this.getFailoverNodeForNode(factory, 0);
      	
      	recoveryMapSize = ServerManagement.getServer(evennewerFailoverNodeId).getRecoveryMapSize(queue[evennewerFailoverNodeId].getQueueName());
      	assertEquals(0, recoveryMapSize);
      	recoveryArea = ServerManagement.getServer(evennewerFailoverNodeId).getRecoveryArea(queue[evennewerFailoverNodeId].getQueueName());
      	ids = (Map)recoveryArea.get(new Integer(0));
      	assertNotNull(ids);
      	assertEquals(numMessages, ids.size());
      	
      	//Now kill the third failover node
      	
      	log.info("killing node " + evennewerFailoverNodeId);
      	ServerManagement.kill(evennewerFailoverNodeId);
      	
      	//This just leaves the current node
      	
      	Thread.sleep(5000);
      	
      	int evenevennewerFailoverNodeId = this.getFailoverNodeForNode(factory, 0);
      	
      	assertEquals(0, evenevennewerFailoverNodeId);
      	
      	//Add a node
      	
      	ServerManagement.start(1, "all", false);
      	
      	ServerManagement.deployQueue("testDistributedQueue", 1);
      	
      	Thread.sleep(5000);
      	
      	log.info("started node 1");
      	
      	int evenevenevennewerFailoverNodeId = this.getFailoverNodeForNode(factory, 0);
      	
      	recoveryMapSize = ServerManagement.getServer(evenevenevennewerFailoverNodeId).getRecoveryMapSize(queue[evenevenevennewerFailoverNodeId].getQueueName());
      	assertEquals(0, recoveryMapSize);
      	recoveryArea = ServerManagement.getServer(evenevenevennewerFailoverNodeId).getRecoveryArea(queue[evenevenevennewerFailoverNodeId].getQueueName());
      	ids = (Map)recoveryArea.get(new Integer(0));
      	assertNotNull(ids);
      	assertEquals(numMessages, ids.size());
      	
         //Now kill the node itself
      	
      	ServerManagement.kill(0);

         log.info("########");
         log.info("######## KILLED NODE 0");
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
         
         assertEquals(evenevenevennewerFailoverNodeId, getServerId(conn0));
         
                            
         //Now ack
         if (transactional)
         {
         	sess0.commit();
         }
         else
         {
         	tm.acknowledge();
         }
         
         log.info("acked");
         
         sess0.close();
         
         log.info("closed");
         
	      sess0 = conn0.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      
	      log.info("created new session");
      	
      	cons0 = sess0.createConsumer(queue[0]);
      	
      	log.info("Created consumer");
      	
         //Messages should be gone
      	
      	tm = (TextMessage)cons0.receive(5000);
      	
      	assertNull(tm); 	
      	
         recoveryMapSize = ServerManagement.getServer(evenevenevennewerFailoverNodeId).getRecoveryMapSize(queue[evenevenevennewerFailoverNodeId].getQueueName());
      	assertEquals(0, recoveryMapSize);
      	recoveryArea = ServerManagement.getServer(evenevenevennewerFailoverNodeId).getRecoveryArea(queue[evenevenevennewerFailoverNodeId].getQueueName());
      	ids = (Map)recoveryArea.get(new Integer(0));
      	assertNull(ids);
      }
      finally
      {
         if (conn0 != null)
         {
            conn0.close();
         }
         
         // Since we kill the rmi server in this test, we must kill the other servers too
      	
      	for (int i = nodeCount - 1; i >= 0; i--)
      	{
      		ServerManagement.kill(i);
      	}
      }
   }
   
   
   private void killTwoFailoverNodes(boolean transactional) throws Exception
   {
   	JBossConnectionFactory factory = (JBossConnectionFactory) ic[0].lookup("/ClusteredConnectionFactory");

      Connection conn0 = createConnectionOnServer(factory, 0);
 
      try
      {
      	SimpleFailoverListener failoverListener = new SimpleFailoverListener();
         ((JBossConnection)conn0).registerFailoverListener(failoverListener);
      	
         Session sessSend = conn0.createSession(false, Session.AUTO_ACKNOWLEDGE);
      		
      	MessageProducer prod0 = sessSend.createProducer(queue[0]);
      	
      	final int numMessages = 10;
      	
      	for (int i = 0; i < numMessages; i++)
      	{
      		TextMessage tm = sessSend.createTextMessage("message" + i);
      		
      		prod0.send(tm);      		
      	}
      	
      	Session sess0 = conn0.createSession(transactional, transactional ? Session.SESSION_TRANSACTED : Session.CLIENT_ACKNOWLEDGE);
      	
      	MessageConsumer cons0 = sess0.createConsumer(queue[0]);
            	
      	conn0.start();
      	
      	TextMessage tm = null;
      	
      	for (int i = 0; i < numMessages; i++)
      	{
      		tm = (TextMessage)cons0.receive(2000);
      		
      		assertNotNull(tm);
      		
      		assertEquals("message" + i, tm.getText());
      	}
      	
      	//Don't ack
      	
      	int failoverNodeId = this.getFailoverNodeForNode(factory, 0);
      	
      	log.info("Failover node for node 0 is " + failoverNodeId);
      	
      	dumpFailoverMap(ServerManagement.getServer(0).getFailoverMap());
      	
      	int recoveryMapSize = ServerManagement.getServer(failoverNodeId).getRecoveryMapSize(queue[failoverNodeId].getQueueName());
      	assertEquals(0, recoveryMapSize);
      	Map recoveryArea = ServerManagement.getServer(failoverNodeId).getRecoveryArea(queue[failoverNodeId].getQueueName());
      	Map ids = (Map)recoveryArea.get(new Integer(0));
      	assertNotNull(ids);
      	assertEquals(numMessages, ids.size());
      	
      	//Now kill the failover node
      	
      	log.info("killing node " + failoverNodeId);
      	ServerManagement.kill(failoverNodeId);
      	
      	Thread.sleep(5000);
      	
      	int newFailoverNodeId = this.getFailoverNodeForNode(factory, 0);
      	
      	log.info("New Failover node for node 0 is " + newFailoverNodeId);
      	
      	recoveryMapSize = ServerManagement.getServer(newFailoverNodeId).getRecoveryMapSize(queue[newFailoverNodeId].getQueueName());
      	assertEquals(0, recoveryMapSize);
      	recoveryArea = ServerManagement.getServer(newFailoverNodeId).getRecoveryArea(queue[newFailoverNodeId].getQueueName());
      	ids = (Map)recoveryArea.get(new Integer(0));
      	assertNotNull(ids);
      	assertEquals(numMessages, ids.size());
      	
      	//Now kill the second failover node
      	
      	log.info("killing node " + newFailoverNodeId);
      	ServerManagement.kill(newFailoverNodeId);
      	
      	Thread.sleep(5000);
      	
      	int evennewerFailoverNodeId = this.getFailoverNodeForNode(factory, 0);
      	
      	recoveryMapSize = ServerManagement.getServer(evennewerFailoverNodeId).getRecoveryMapSize(queue[evennewerFailoverNodeId].getQueueName());
      	assertEquals(0, recoveryMapSize);
      	recoveryArea = ServerManagement.getServer(evennewerFailoverNodeId).getRecoveryArea(queue[evennewerFailoverNodeId].getQueueName());
      	ids = (Map)recoveryArea.get(new Integer(0));
      	assertNotNull(ids);
      	assertEquals(numMessages, ids.size());
      	
      	log.info("New Failover node for node 0 is " + evennewerFailoverNodeId);
      	      	         
         //Now kill the node itself
      	
      	ServerManagement.kill(0);

         log.info("########");
         log.info("######## KILLED NODE 0");
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
         
         assertEquals(evennewerFailoverNodeId, getServerId(conn0));
         
         recoveryMapSize = ServerManagement.getServer(evennewerFailoverNodeId).getRecoveryMapSize(queue[evennewerFailoverNodeId].getQueueName());
      	assertEquals(0, recoveryMapSize);
      	recoveryArea = ServerManagement.getServer(evennewerFailoverNodeId).getRecoveryArea(queue[evennewerFailoverNodeId].getQueueName());
      	ids = (Map)recoveryArea.get(new Integer(0));
      	assertNull(ids);
                                                      
         //Now ack
         if (transactional)
         {
         	sess0.commit();
         }
         else
         {
         	tm.acknowledge();
         }
         
         log.info("acked");
         
         sess0.close();
         
         log.info("closed");
         
	      sess0 = conn0.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      
	      log.info("created new session");
      	
      	cons0 = sess0.createConsumer(queue[0]);
      	
      	log.info("Created consumer");
      	
         //Messages should be gone
      	
      	tm = (TextMessage)cons0.receive(5000);
      	
      	assertNull(tm); 	
      }
      finally
      {
         if (conn0 != null)
         {
            conn0.close();
         }
         
         //  Since we kill the rmi server in this test, we must kill the other servers too
      	
      	for (int i = nodeCount - 1; i >= 0; i--)
      	{
      		ServerManagement.kill(i);
      	}
      }
   }
   
   
   private void addNodeToGetNewFailoverNode(boolean transactional) throws Exception
   {
   	JBossConnectionFactory factory = (JBossConnectionFactory) ic[0].lookup("/ClusteredConnectionFactory");

      Connection conn3 = createConnectionOnServer(factory, 3);
 
      try
      {
      	SimpleFailoverListener failoverListener = new SimpleFailoverListener();
         ((JBossConnection)conn3).registerFailoverListener(failoverListener);
      	
         Session sessSend = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);
      		
      	MessageProducer prod2 = sessSend.createProducer(queue[2]);
      	
      	final int numMessages = 10;
      	
      	for (int i = 0; i < numMessages; i++)
      	{
      		TextMessage tm = sessSend.createTextMessage("message" + i);
      		
      		prod2.send(tm);      		
      	}
      	
      	Session sess3 = conn3.createSession(transactional, transactional ? Session.SESSION_TRANSACTED : Session.CLIENT_ACKNOWLEDGE);
      	
      	MessageConsumer cons3 = sess3.createConsumer(queue[3]);
      
      	
      	conn3.start();
      	
      	TextMessage tm = null;
      	
      	for (int i = 0; i < numMessages; i++)
      	{
      		tm = (TextMessage)cons3.receive(2000);
      		
      		assertNotNull(tm);
      		
      		assertEquals("message" + i, tm.getText());
      	}
      	
      	//Don't ack
      	
      	int failoverNodeId = this.getFailoverNodeForNode(factory, 3);
      	
      	log.info("Failover node for node 3 is " + failoverNodeId);
      	
      	dumpFailoverMap(ServerManagement.getServer(3).getFailoverMap());
      	
      	int recoveryMapSize = ServerManagement.getServer(failoverNodeId).getRecoveryMapSize(queue[failoverNodeId].getQueueName());
      	assertEquals(0, recoveryMapSize);
      	Map recoveryArea = ServerManagement.getServer(failoverNodeId).getRecoveryArea(queue[failoverNodeId].getQueueName());
      	Map ids = (Map)recoveryArea.get(new Integer(3));
      	assertNotNull(ids);
      	assertEquals(numMessages, ids.size());      	      	
      	
      	//We now add a new node - this should cause the failover node to change
      	
         ServerManagement.start(4, "all", false);
         
         ServerManagement.deployQueue("testDistributedQueue", 4);
         
         Thread.sleep(5000);         
         
         recoveryMapSize = ServerManagement.getServer(failoverNodeId).getRecoveryMapSize(queue[failoverNodeId].getQueueName());
      	assertEquals(0, recoveryMapSize);
      	recoveryArea = ServerManagement.getServer(failoverNodeId).getRecoveryArea(queue[failoverNodeId].getQueueName());
      	ids = (Map)recoveryArea.get(new Integer(3));
      	assertNull(ids);
                  
         dumpFailoverMap(ServerManagement.getServer(3).getFailoverMap());
      	
         int newFailoverNodeId = this.getFailoverNodeForNode(factory, 3);
         
         assertTrue(failoverNodeId != newFailoverNodeId);
         
         log.info("New failover node is " + newFailoverNodeId);
         
         recoveryMapSize = ServerManagement.getServer(newFailoverNodeId).getRecoveryMapSize(queue[3].getQueueName());
      	assertEquals(0, recoveryMapSize);
      	recoveryArea = ServerManagement.getServer(newFailoverNodeId).getRecoveryArea(queue[3].getQueueName());
      	ids = (Map)recoveryArea.get(new Integer(3));
      	assertNotNull(ids);
      	assertEquals(numMessages, ids.size());                  
         
         //Now kill the node
      	
      	ServerManagement.kill(3);

         log.info("########");
         log.info("######## KILLED NODE 3");
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
         
         assertEquals(newFailoverNodeId, getServerId(conn3));
         
         recoveryMapSize = ServerManagement.getServer(failoverNodeId).getRecoveryMapSize(queue[3].getQueueName());
      	assertEquals(0, recoveryMapSize);
      	recoveryArea = ServerManagement.getServer(newFailoverNodeId).getRecoveryArea(queue[3].getQueueName());
      	ids = (Map)recoveryArea.get(new Integer(3));
      	assertNull(ids);                                    
                  
         //Now ack
         if (transactional)
         {
         	sess3.commit();
         }
         else
         {
         	tm.acknowledge();
         }
         
         log.info("acked");
         
         sess3.close();
         
         log.info("closed");
         
	      sess3 = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      
	      log.info("created new session");
      	
      	cons3 = sess3.createConsumer(queue[3]);
      	
      	log.info("Created consumer");
      	
         //Messages should be gone
      	
      	tm = (TextMessage)cons3.receive(5000);
      	
      	assertNull(tm);      		  	
      }
      finally
      {
         if (conn3 != null)
         {
            conn3.close();
         }
         
         try
         {
         	ServerManagement.stop(4);
         }
         catch (Exception e)
         {}
      }
   }
   
   
   
   private void dumpFailoverMap(Map map)
   {
   	Iterator iter = map.entrySet().iterator();
   	
   	log.info("*** dumping failover map ***");
   	
   	while (iter.hasNext())
   	{
   		Map.Entry entry = (Map.Entry)iter.next();
   		
   		log.info(entry.getKey() + "-->" + entry.getValue());
   	}
   	
   	log.info("*** end dump ***");
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
      	
      	//Now kill node 1
      	
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
   
   
   private void stopFailoverNode(boolean transactional) throws Exception
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
      	
      	//We stop the failover node for node 1
      	int failoverNodeId = this.getFailoverNodeForNode(factory, 1);
      	
      	int recoveryMapSize = ServerManagement.getServer(failoverNodeId).getRecoveryMapSize(queue[failoverNodeId].getQueueName());
      	assertEquals(0, recoveryMapSize);
      	Map recoveryArea = ServerManagement.getServer(failoverNodeId).getRecoveryArea(queue[failoverNodeId].getQueueName());
      	Map ids = (Map)recoveryArea.get(new Integer(1));
      	assertNotNull(ids);
      	assertEquals(numMessages, ids.size());
      	
      	log.info("Killing failover node:" + failoverNodeId);
      	
      	ServerManagement.stop(failoverNodeId);
      	
      	log.info("Killed failover node");
      	
      	int newfailoverNode = this.getFailoverNodeForNode(factory, 1);
      	
      	recoveryMapSize = ServerManagement.getServer(newfailoverNode).getRecoveryMapSize(queue[newfailoverNode].getQueueName());
      	assertEquals(0, recoveryMapSize);
      	recoveryArea = ServerManagement.getServer(newfailoverNode).getRecoveryArea(queue[newfailoverNode].getQueueName());
      	ids = (Map)recoveryArea.get(new Integer(1));
      	assertNotNull(ids);
      	assertEquals(numMessages, ids.size());
      	
      	Thread.sleep(5000);
      	
      	//Now kill node 1
      	
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
         
         assertEquals(newfailoverNode, getServerId(conn1));
         
         recoveryMapSize = ServerManagement.getServer(newfailoverNode).getRecoveryMapSize(queue[newfailoverNode].getQueueName());
      	assertEquals(0, recoveryMapSize);
      	recoveryArea = ServerManagement.getServer(newfailoverNode).getRecoveryArea(queue[newfailoverNode].getQueueName());
      	ids = (Map)recoveryArea.get(new Integer(1));
      	assertNull(ids);

                  
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
   
   
   
   // Inner classes -------------------------------------------------
   
}
