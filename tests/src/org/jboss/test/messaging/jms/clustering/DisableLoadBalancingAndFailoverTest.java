/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.clustering;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.ObjectName;

import org.jboss.jms.client.FailoverEvent;
import org.jboss.jms.client.JBossConnection;
import org.jboss.test.messaging.tools.ServerManagement;

import EDU.oswego.cs.dl.util.concurrent.Latch;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 2685 $</tt>
 *
 * $Id: FailoverTest.java 2685 2007-05-15 07:56:12Z timfox $
 */
public class DisableLoadBalancingAndFailoverTest extends ClusteringTestBase
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------
	
	protected ConnectionFactory nonClusteredCF1;
	
	protected ConnectionFactory nonClusteredCF2;
	
	protected ConnectionFactory lbCF1;
	
	protected ConnectionFactory lbCF2;
	
	protected ConnectionFactory foCF;

   // Constructors ---------------------------------------------------------------------------------

   public DisableLoadBalancingAndFailoverTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------
   
   public void testNoFailoverNoLoadBalancing1() throws Exception
   {
   	//If no load balancing then always on local node
   	this.noFailover(nonClusteredCF1);
   }
   
   public void testNoFailoverNoLoadBalancing2() throws Exception
   {
   	this.noFailoverWithExceptionListener(nonClusteredCF1, nonClusteredCF2);   	
   }
   	
   public void testNoFailoverNoLoadBalancing3() throws Exception
   {
   	this.noLoadBalancing(nonClusteredCF1);
   }
   
   public void testNoFailoverLoadBalancing1() throws Exception
   {
   	this.noFailover(lbCF1);
   }
   
   public void testNoFailoverLoadBalancing2() throws Exception
   {
   	this.noFailoverWithExceptionListener(lbCF1, lbCF2);   	
   }
   	
   public void testNoFailoverLoadBalancing3() throws Exception
   {
   	this.loadBalancing(lbCF1);
   }
   
   public void testFailoverNoLoadBalancing1() throws Exception
   {
   	this.failover(foCF);
   }
   	
   public void testFailoverNoLoadBalancing2() throws Exception
   {
   	this.noLoadBalancing(foCF);
   }
   
   // Protected -------------------------------------------------------------------------------------

   protected void noLoadBalancing(ConnectionFactory theCF) throws Exception
   {
   	Connection conn = null;      

      try
      {
      	conn = theCF.createConnection();
      	
      	int serverID = getServerId(conn);
      	
         conn.close();
         
         conn = theCF.createConnection();
      	
         assertEquals(serverID, getServerId(conn));
         
         conn.close();
         
         conn = theCF.createConnection();
      	
         assertEquals(serverID, getServerId(conn));
         
         conn.close();
         
         conn = theCF.createConnection();
      	
         assertEquals(serverID, getServerId(conn));
         
         conn.close();
         
         conn = theCF.createConnection();
      	
         assertEquals(serverID, getServerId(conn));                
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }
   
   protected void loadBalancing(ConnectionFactory theCF) throws Exception
   {
   	Connection conn = null;      

      try
      {
      	log.info("In test **************");
      	      	
      	conn = theCF.createConnection();
      	
      	int serverID = getServerId(conn);
      	log.info("server id is " + serverID);
      	
         conn.close();
         
         conn = theCF.createConnection();
      	
         serverID = ++serverID % 3;
         assertEquals(serverID, getServerId(conn));
         
      	log.info("server id is " + serverID);
         
         conn.close();
         
         conn = theCF.createConnection();
      	
         serverID = ++serverID % 3;
         assertEquals(serverID, getServerId(conn));
      	log.info("server id is " + serverID);
         
         conn.close();
         
         conn = theCF.createConnection();
      	
         serverID = ++serverID % 3;
         assertEquals(serverID, getServerId(conn));
      	log.info("server id is " + serverID);
         
         conn.close();
         
         conn = theCF.createConnection();
      	
         serverID = ++serverID % 3;
         assertEquals(serverID, getServerId(conn));
      	log.info("server id is " + serverID);
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }
   
   
   protected void noFailover(ConnectionFactory theCF) throws Exception
   {
      Connection conn = null;      

      try
      {
         conn = createConnectionOnServer(theCF, 1);
      	
         assertEquals(1, getServerId(conn));
         
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod = sess.createProducer(queue[1]);
         
         conn.start();
         
         TextMessage tm = sess.createTextMessage("uhasduihd");
         
         //Now kill server 1
         
         log.info("KILLING SERVER 1");
         ServerManagement.kill(1);
         log.info("KILLED SERVER 1");
         
         Thread.sleep(5000);
         
         long start = System.currentTimeMillis();
         try
         {
         	prod.send(tm);
         		
         	// We shouldn't get here
            fail();
         }
         catch (org.jboss.jms.exception.MessagingNetworkFailureException e)
         {
         	//OK - this is what we should get
         	long end = System.currentTimeMillis();
         	
         	//Make sure it doesn't take too long
         	assertTrue((end - start) <= 100);
         }                 
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }
         
   protected void noFailoverWithExceptionListener(ConnectionFactory theCF1, ConnectionFactory theCF2) throws Exception
   {
      Connection conn = null;      

      try
      {
         conn = createConnectionOnServer(theCF1, 1);
      	
      	MyListener listener = new MyListener();
      	
      	conn.setExceptionListener(listener);
      	
         assertEquals(1, getServerId(conn));
         
         //Now kill server 1
         
         log.info("KILLING SERVER 1");
         ServerManagement.kill(1);
         log.info("KILLED SERVER 1");
         
         JMSException e = listener.waitForException(20000);
         
         assertNotNull(e);
         
         assertTrue(e.getMessage().equals("Failure on underlying remoting connection"));
         
         //Now try and recreate connection on different node
         
         conn.close();
         
         conn = theCF2.createConnection();
         
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = sess.createProducer(queue[2]);
         
         MessageConsumer cons = sess.createConsumer(queue[2]);

         conn.start();

         TextMessage tm = sess.createTextMessage("uhasduihd");
         
         prod.send(tm);
         
         TextMessage rm = (TextMessage)cons.receive(1000);
         
         assertNotNull(rm);
         
         assertEquals(tm.getText(), rm.getText());
           
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }
   
   protected void failover(ConnectionFactory theCF) throws Exception
   {
      Connection conn = null;      

      try
      {
         conn = createConnectionOnServer(theCF, 1);
      	
      	// register a failover listener
         SimpleFailoverListener failoverListener = new SimpleFailoverListener();
         ((JBossConnection)conn).registerFailoverListener(failoverListener);
      	
         assertEquals(1, getServerId(conn));
         
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod = sess.createProducer(queue[1]);
         
         MessageConsumer cons = sess.createConsumer(queue[1]);
         
         conn.start();
         
         TextMessage tm = sess.createTextMessage("uhasduihd");
         
         //Now kill server 1
         
         log.info("KILLING SERVER 1");
         ServerManagement.kill(1);
         log.info("KILLED SERVER 1");
         
         // wait for the client-side failover to complete

         while (true)
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

         // failover complete
         log.info("failover completed");
         
         prod.send(tm);
         
         TextMessage rm = (TextMessage)cons.receive(2000);
         
         assertNotNull(rm);
         
         assertEquals(tm.getText(), rm.getText());         		                
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }
   
   protected void setUp() throws Exception
   {
      nodeCount = 3;

      super.setUp();
      
      nonClusteredCF1 = (ConnectionFactory)ic[1].lookup("/ConnectionFactory");
      
      nonClusteredCF2 = (ConnectionFactory)ic[2].lookup("/ConnectionFactory");
      
      //Deploy a connection factory with load balancing but no failover on node0
      ServerManagement.getServer(0).deployConnectionFactory("org.jboss.messaging:service=LBConnectionFactory",
      		                                   new String[] { "/LBConnectionFactory" }, false, true);
      
      //Deploy a connection factory with load balancing but no failover on node1
      ServerManagement.getServer(1).deployConnectionFactory("org.jboss.messaging:service=LBConnectionFactory",
      		                                   new String[] { "/LBConnectionFactory" }, false, true);
      
      //Deploy a connection factory with load balancing but no failover on node2
      ServerManagement.getServer(2).deployConnectionFactory("org.jboss.messaging:service=LBConnectionFactory",
      		                                   new String[] { "/LBConnectionFactory" }, false, true);
      
      this.lbCF1 = (ConnectionFactory)ic[1].lookup("/LBConnectionFactory");      
      
      this.lbCF2 = (ConnectionFactory)ic[2].lookup("/LBConnectionFactory");
      
      //Deploy a connection factory with failover but no load balancing on node 0
      ServerManagement.getServer(0).deployConnectionFactory("org.jboss.messaging:service=FOConnectionFactory",
            new String[] { "/FOConnectionFactory" }, true, false);
      
      //Deploy a connection factory with failover but no load balancing on node 1
      ServerManagement.getServer(1).deployConnectionFactory("org.jboss.messaging:service=FOConnectionFactory",
            new String[] { "/FOConnectionFactory" }, true, false);
      
      //Deploy a connection factory with failover but no load balancing on node 2
      ServerManagement.getServer(2).deployConnectionFactory("org.jboss.messaging:service=FOConnectionFactory",
            new String[] { "/FOConnectionFactory" }, true, false);

      this.foCF = (ConnectionFactory)ic[1].lookup("/FOConnectionFactory");

      log.debug("setup done");
   }

   protected void tearDown() throws Exception
   {
   	try
   	{
   		ServerManagement.getServer(0).undeployConnectionFactory(new ObjectName("org.jboss.messaging:service=LBConnectionFactory"));
   	}
   	catch (Exception ignore)
   	{   		
   	}
      
   	try
   	{
         ServerManagement.getServer(1).undeployConnectionFactory(new ObjectName("org.jboss.messaging:service=LBConnectionFactory"));
   	}
   	catch (Exception ignore)
   	{   		
   	}
   	
   	try
   	{
         ServerManagement.getServer(2).undeployConnectionFactory(new ObjectName("org.jboss.messaging:service=LBConnectionFactory"));
   	}
   	catch (Exception ignore)
   	{   		
   	}
   	
   	try
   	{
   		ServerManagement.getServer(0).undeployConnectionFactory(new ObjectName("org.jboss.messaging:service=FOConnectionFactory"));
   	}
   	catch (Exception ignore)
   	{   		
   	}
      
   	try
   	{
         ServerManagement.getServer(1).undeployConnectionFactory(new ObjectName("org.jboss.messaging:service=FOConnectionFactory"));
   	}
   	catch (Exception ignore)
   	{   		
   	}
   	
   	try
   	{
         ServerManagement.getServer(2).undeployConnectionFactory(new ObjectName("org.jboss.messaging:service=FOConnectionFactory"));
   	}
   	catch (Exception ignore)
   	{   		
   	}
   	
      super.tearDown();      
   }



   // Inner classes --------------------------------------------------------------------------------
   
	private class MyListener implements ExceptionListener
	{
		private JMSException e;
		
		Latch l = new Latch();

		public void onException(JMSException e)
		{
			this.e = e;
			
			l.release();
		}
		
		JMSException waitForException(long timeout) throws Exception
		{
			l.attempt(timeout);
			
			return e;
		}
		
	}	
   
   
}
