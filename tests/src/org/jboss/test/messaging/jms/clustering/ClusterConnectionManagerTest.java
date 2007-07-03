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
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.ObjectName;
import javax.naming.InitialContext;

import org.jboss.test.messaging.tools.ServerManagement;

/**
 * 
 * We test every combination of the order of deployment of connection factory, local and remote queue
 * 
 * and verify message sucking still works
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: $</tt>2 Jul 2007
 *
 * $Id: $
 *
 */
public class ClusterConnectionManagerTest extends ClusteringTestBase
{

   // Constants ------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public ClusterConnectionManagerTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------
   
   public void test1() throws Exception
   {
   	deployCF();
   	
   	deployLocal();
   	
   	deployRemote();
   	
   	suck();
   }
   
   public void test2() throws Exception
   {
   	deployCF();
   	
   	deployRemote();
   	
   	deployLocal();
   	   	
   	suck();
   }
   
   public void test3() throws Exception
   {
   	deployRemote();
   	
   	deployCF();
   	   	
   	deployLocal();
   	   	
   	suck();
   }
   
   public void test4() throws Exception
   {
   	deployRemote();
   	
   	deployLocal();
   	
   	deployCF();
   	     	   	
   	suck();
   }
   
   public void test5() throws Exception
   {
   	deployLocal();
   	
   	deployRemote();
   	   	
   	deployCF();
   	     	   	
   	suck();
   }
   
   public void test6() throws Exception
   {
   	deployLocal();
   	
   	deployCF();
   	
   	deployRemote();
   	      	     	   
   	suck();
   }
   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected void setUp() throws Exception
   {
      nodeCount = 2;
      super.setUp();

      log.debug("setup done");
      
      //undeploy CF
      
      String cfName =
   		(String)ServerManagement.getServer(1).getAttribute(ServerManagement.getServerPeerObjectName(), "ClusterPullConnectionFactoryName");

   	//undeploy cf on node 1   	
   	ServerManagement.undeployConnectionFactory(new ObjectName(cfName));            
   }

   protected void tearDown() throws Exception
   {
   	ServerManagement.undeployQueue("suckQueue", 0);

   	ServerManagement.undeployQueue("suckQueue", 1);

   	super.tearDown();
   }

   // Private --------------------------------------------------------------------------------------
   

   
   private void deployCF() throws Exception
   {
   	String cfName =
   		(String)ServerManagement.getServer(1).getAttribute(ServerManagement.getServerPeerObjectName(), "ClusterPullConnectionFactoryName");

   	//Deploy cf on node 1   	
   	ServerManagement.deployConnectionFactory(cfName, null, 150);
   }
   
   private void deployLocal() throws Exception
   {
   	 ServerManagement.deployQueue("suckQueue", 1);
   }
   
   private void deployRemote() throws Exception
   {
   	 ServerManagement.deployQueue("suckQueue", 0);
   }
   
   private void suck() throws Exception
   {
      InitialContext ic0 = new InitialContext(ServerManagement.getJNDIEnvironment(0));
      
      Queue queue0 = (Queue)ic0.lookup("/queue/suckQueue");
      
      InitialContext ic1 = new InitialContext(ServerManagement.getJNDIEnvironment(1));
      
      Queue queue1 = (Queue)ic1.lookup("/queue/suckQueue");
      
      ConnectionFactory cf0 = (ConnectionFactory)ic0.lookup("/ConnectionFactory");
      
      ConnectionFactory cf1 = (ConnectionFactory)ic1.lookup("/ConnectionFactory");
      
      Connection conn0 = null;
      
      Connection conn1 = null;
      
      try
      {
      	conn0 = cf0.createConnection();
      	
      	//Send some messages on node 0
      	
      	final int NUM_MESSAGES = 100;
      	
      	Session sess0 = conn0.createSession(false, Session.AUTO_ACKNOWLEDGE);
      	
      	MessageProducer prod = sess0.createProducer(queue0);
      	
      	for (int i = 0; i < NUM_MESSAGES; i++)
      	{
      		TextMessage tm = sess0.createTextMessage("message" + i);
      		
      		prod.send(tm);
      	}
      	
      	//Consume them on node 1
      	
      	conn1 = cf1.createConnection();
      	
      	Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      	
      	MessageConsumer cons1 = sess1.createConsumer(queue1);
      	
      	conn1.start();
      	
      	for (int i = 0; i < NUM_MESSAGES; i++)
      	{
      		TextMessage tm = (TextMessage)cons1.receive(5000);
      		
      		assertNotNull(tm);
      		
      		assertEquals("message" + i, tm.getText());
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
      }
      
   }



   // Inner classes --------------------------------------------------------------------------------

}
