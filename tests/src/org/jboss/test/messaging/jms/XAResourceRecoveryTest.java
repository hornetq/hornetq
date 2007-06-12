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

import java.util.Hashtable;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XASession;
import javax.naming.InitialContext;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import org.jboss.jms.jndi.JMSProviderAdapter;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.TestJMSProviderAdaptor;
import org.jboss.test.messaging.tools.aop.PoisonInterceptor;
import org.jboss.test.messaging.tools.jmx.ServiceContainer;
import org.jboss.test.messaging.tools.jndi.InVMInitialContextFactory;
import org.jboss.tm.TxUtils;

import com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionManagerImple;

/**
 * 
 * A XAResourceRecoveryTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class XAResourceRecoveryTest extends MessagingTestCase
{	
	protected int nodeCount = 2;

	protected ServiceContainer sc;

	protected XAConnectionFactory cf0, cf1;

	protected Destination queue0, queue1;
	
	protected TransactionManager tm;
	
	protected Transaction suspendedTx;

	public XAResourceRecoveryTest(String name)
	{
		super(name);
	}

   protected void setUp() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         throw new IllegalStateException("This test should only be run in remote mode");
      }
      
      super.setUp();
       
      log.info("Starting " + nodeCount + " servers");
                     
      for (int i = 0; i < nodeCount; i++)
      {
         // make sure all servers are created and started; make sure that database is zapped
         // ONLY for the first server, the others rely on values they expect to find in shared
         // tables; don't clear the database for those.
         ServerManagement.start(i, "all", i == 0);
      }
      
      //We need a local transaction and recovery manager
      //We must start this after the remote servers have been created or it won't
      //have deleted the database and the recovery manager may attempt to recover transactions
      sc = new ServiceContainer("jbossjta");   

      sc.start(false);
      
      ServerManagement.undeployQueue("queue0", 0);
      
      ServerManagement.undeployQueue("queue1", 1);      
      
      ServerManagement.deployQueue("queue0", 0);
      
      ServerManagement.deployQueue("queue1", 1);   
      
      Hashtable props0 = ServerManagement.getJNDIEnvironment(0);
      
      Hashtable props1 = ServerManagement.getJNDIEnvironment(1);
          
      InitialContext ic0 = new InitialContext(props0);
      
      InitialContext ic1 = new InitialContext(props1);
      
      cf0 = (XAConnectionFactory)ic0.lookup("/XAConnectionFactory");
      
      cf1 = (XAConnectionFactory)ic1.lookup("/XAConnectionFactory");
      
      queue0 = (Queue)ic0.lookup("/queue/queue0");
      
      queue1 = (Queue)ic1.lookup("/queue/queue1");
      
      InitialContext localIc = new InitialContext(InVMInitialContextFactory.getJNDIEnvironment());
      
      tm = (TransactionManager)localIc.lookup(ServiceContainer.TRANSACTION_MANAGER_JNDI_NAME);

      log.info("tm is " + tm.getClass().getName());
      assertTrue(tm instanceof TransactionManagerImple);
      
      drainDestination((ConnectionFactory)cf0, queue0);
      
      drainDestination((ConnectionFactory)cf1, queue1);

      if (!ServerManagement.isRemote())
      {
         suspendedTx = tm.suspend();
      }
      
      //Now install local JMSProviderAdaptor classes
      
      Properties p1 = new Properties();
      p1.putAll(ServerManagement.getJNDIEnvironment(1));
        
      JMSProviderAdapter targetAdaptor =
         new TestJMSProviderAdaptor(p1, "/XAConnectionFactory", "adaptor1");
      
      sc.installJMSProviderAdaptor("adaptor1", targetAdaptor);
      
      sc.startRecoveryManager();

   }
   
   protected void tearDown() throws Exception
   {       
      try
      {
         ServerManagement.undeployQueue("queue0", 0);
      }
      catch (Exception e)
      {
         log.error("Failed to undeploy", e);
      }
            
      
      try
      {
         ServerManagement.undeployQueue("queue1", 1);
      }
      catch (Exception e)
      {
         log.error("Failed to undeploy", e);
      }
      
      if (TxUtils.isUncommitted(tm))
      {
         //roll it back
         try
         {
            tm.rollback();
         }
         catch (Throwable ignore)
         {
            //The connection will probably be closed so this may well throw an exception
         }
      }
      if (tm.getTransaction() != null)
      {
         Transaction tx = tm.suspend();
         if (tx != null)
            log.warn("Transaction still associated with thread " + tx + " at status " + TxUtils.getStatusAsString(tx.getStatus()));
      }

      if (suspendedTx != null)
      {
         tm.resume(suspendedTx);
      }
            
      for (int i = 0; i < nodeCount; i++)
      {
         try
         {
            if (ServerManagement.isStarted(i))
            {
               ServerManagement.log(ServerManagement.INFO, "Undeploying Server " + i, i);
               
               ServerManagement.stop(i);
            }
         }
         catch (Exception e)
         {
            log.error("Failed to stop server", e);
         }
      }
      
      for (int i = 1; i < nodeCount; i++)
      {
         try
         {
            ServerManagement.kill(i);
         }
         catch (Exception e)
         {
            log.error("Failed to kill server", e);
         }
      }
      
      sc.uninstallJMSProviderAdaptor("adaptor1");
      
      sc.stopRecoveryManager();
      
      sc.stop();
      
      super.tearDown();      
   }
   
   public void testRecoveryOnSend() throws Exception
   {
   	XAConnection conn0 = null;
   	
   	XAConnection conn1 = null;
   	
   	Connection conn2 = null;
   	
   	Connection conn3 = null;
   	
   	try
   	{
   		conn0 = cf0.createXAConnection();
   		
   		XASession sess0 = conn0.createXASession();
   		
   		MessageProducer prod0 = sess0.createProducer(queue0);
   		
   		XAResource res0 = sess0.getXAResource();
   		
   		
   		conn1 = cf1.createXAConnection();
   		
   		XASession sess1 = conn1.createXASession();
   		
   		MessageProducer prod1 = sess1.createProducer(queue1);
   		
   		XAResource res1 = sess1.getXAResource();
   		
   		
   		tm.begin();
   		
   		Transaction tx = tm.getTransaction();
   		
   		tx.enlistResource(res0);
   		
   		tx.enlistResource(res1);
   		
   		
   		TextMessage tm0 = sess0.createTextMessage("message0");
   		
   		prod0.send(tm0);
   		
   		
   		TextMessage tm1 = sess1.createTextMessage("message1");
   		
   		prod1.send(tm1);
   		
   		
   		//	Poison server 1 so it crashes on commit of dest but after prepare
         
         //This means the transaction branch on source will get commmitted
         //but the branch on dest won't be - it will remain prepared
         //This corresponds to a HeuristicMixedException
         
         ServerManagement.poisonTheServer(1, PoisonInterceptor.TYPE_2PC_COMMIT);
         
         log.info("Poisoned server");
         
         tx.delistResource(res0, XAResource.TMSUCCESS);
   		
         tx.delistResource(res1, XAResource.TMSUCCESS);
                  
         tx.commit();
         
         conn0.close();
         
         conn1.close();
         
         //Now restart the server
         
         log.info("Restarting server");
         
         ServerManagement.start(1, "all", false);
         
         log.info("Restarted server");
         
         Thread.sleep(5000);
         
         ServerManagement.deployQueue("queue1", 1);   
         
         Hashtable props1 = ServerManagement.getJNDIEnvironment(1);
             
         InitialContext ic1 = new InitialContext(props1);
         
         cf1 = (XAConnectionFactory)ic1.lookup("/XAConnectionFactory");
         
         queue1 = (Queue)ic1.lookup("/queue/queue1");
         
         
         conn2 = ((ConnectionFactory)cf0).createConnection();
         
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons2 = sess2.createConsumer(queue0);
         
         conn2.start();
         
         TextMessage rm0 = (TextMessage)cons2.receive(2000);
         
         assertNotNull(rm0);
         
         assertEquals(tm0.getText(), rm0.getText());
         
         Message m = cons2.receive(2000);
         
         assertNull(m);
         
         //Now even though the commit on the second server failed since the server was dead, the recovery manager should kick in
         //eventually and recover it.
                           
         conn3 = ((ConnectionFactory)cf1).createConnection();
         
         Session sess3 = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons3 = sess3.createConsumer(queue1);
         
         conn3.start();
         
         TextMessage rm1 = (TextMessage)cons3.receive(60000);
         
         assertNotNull(rm1);
         
         assertEquals(tm1.getText(), rm1.getText());
         
         m = cons3.receive(2000);
         
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
   		if (conn3 != null)
   		{
   			conn3.close();
   		}
   	}
   }
   
   public void testRecoveryOnAck() throws Exception
   {
   	XAConnection conn0 = null;
   	
   	XAConnection conn1 = null;
   	
   	Connection conn2 = null;
   	
   	Connection conn3 = null;
   	
   	try
   	{
   		conn0 = cf0.createXAConnection();
   		
   		XASession sess0 = conn0.createXASession();
   		
   		MessageProducer prod0 = sess0.createProducer(queue0);
   		
   		XAResource res0 = sess0.getXAResource();
   		
   		
   		conn1 = cf1.createXAConnection();
   		
   		XASession sess1 = conn1.createXASession();
   		
   		MessageConsumer cons1 = sess1.createConsumer(queue1);
   		
   		XAResource res1 = sess1.getXAResource();
   		
   		conn1.start();
   		
   		
   		
   		//first send a few messages to server 1
   		
   		conn2 = ((ConnectionFactory)cf1).createConnection();
   		
   		Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
   		
   		MessageProducer prod2 = sess2.createProducer(queue1);
   		
   		TextMessage tm1 = sess1.createTextMessage("message1");
   		
   		prod2.send(tm1);
   		
   		TextMessage tm2 = sess1.createTextMessage("message2");
   		
   		prod2.send(tm2);
   		
   		conn2.close();
   		
   		
   		
   		tm.begin();
   		
   		Transaction tx = tm.getTransaction();
   		
   		tx.enlistResource(res0);
   		
   		tx.enlistResource(res1);
   		
   		
   		TextMessage tm0 = sess0.createTextMessage("message0");
   		
   		prod0.send(tm0);
   		
   		//Consume one of the messages on dest
   		
   		TextMessage rm1 = (TextMessage)cons1.receive(1000);
   		
   		assertNotNull(rm1);
   		
   		assertEquals(tm1.getText(), rm1.getText());
   		
   		
   		
   		//	Poison server 1 so it crashes on commit of dest but after prepare
         
         //This means the transaction branch on source will get commmitted
         //but the branch on dest won't be - it will remain prepared
         //This corresponds to a HeuristicMixedException
         
         ServerManagement.poisonTheServer(1, PoisonInterceptor.TYPE_2PC_COMMIT);
         
         log.info("Poisoned server");
         
         tx.delistResource(res0, XAResource.TMSUCCESS);
   		
         tx.delistResource(res1, XAResource.TMSUCCESS);
                  
         tx.commit();
         
         conn0.close();
         
         conn1.close();
         
         //Now restart the server
         
         log.info("Restarting server");
         
         ServerManagement.start(1, "all", false);
         
         log.info("Restarted server");
         
         Thread.sleep(5000);
         
         ServerManagement.deployQueue("queue1", 1);   
         
         Hashtable props1 = ServerManagement.getJNDIEnvironment(1);
             
         InitialContext ic1 = new InitialContext(props1);
         
         cf1 = (XAConnectionFactory)ic1.lookup("/XAConnectionFactory");
         
         queue1 = (Queue)ic1.lookup("/queue/queue1");
         
         
         conn2 = ((ConnectionFactory)cf0).createConnection();
         
         sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons2 = sess2.createConsumer(queue0);
         
         conn2.start();
         
         TextMessage rm0 = (TextMessage)cons2.receive(2000);
         
         assertNotNull(rm0);
         
         assertEquals(tm0.getText(), rm0.getText());
         
         Message m = cons2.receive(2000);
         
         assertNull(m);
         
         //Now even though the commit on the second server failed since the server was dead, the recovery manager should kick in
         //eventually and recover it.
                           
         conn3 = ((ConnectionFactory)cf1).createConnection();
         
         Session sess3 = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons3 = sess3.createConsumer(queue1);
         
         conn3.start();
         
         TextMessage rm2 = (TextMessage)cons3.receive(60000);
         
         assertNotNull(rm2);
         
         //tm1 should have been acked on recovery
         
         assertEquals(tm2.getText(), rm2.getText());
         
         m = cons3.receive(2000);
         
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
   		if (conn3 != null)
   		{
   			conn3.close();
   		}
   	}
   }



}
