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
   
//   public void testKillFailoverNodeTransactional() throws Exception
//   {
//   	this.killFailoverNode(true);
//   }
//   
//   public void testKillFailoverNodeNonTransactional() throws Exception
//   {
//   	this.killFailoverNode(false);
//   }
   
   
   
   public void testStopFailoverNodeTransactional() throws Exception
   {
   	this.stopFailoverNode(true);
   }
   
   public void testStopFailoverNodeNonTransactional() throws Exception
   {
   	this.stopFailoverNode(false);
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
   }
   
   // Private -------------------------------------------------------

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
      	
      	//We kill the failover node for node 1
      	int failoverNodeId = this.getFailoverNodeForNode(factory, 1);
      	
      	log.info("Killing failover node:" + failoverNodeId);
      	
      	ServerManagement.stop(failoverNodeId);
      	
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
   
   
   
   // Inner classes -------------------------------------------------
   
}
