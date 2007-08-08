/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.clustering;

import java.util.HashSet;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XASession;
import javax.naming.InitialContext;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.jms.client.FailoverEvent;
import org.jboss.jms.client.JBossConnection;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.aop.PoisonInterceptor;
import org.jboss.test.messaging.tools.container.InVMInitialContextFactory;
import org.jboss.test.messaging.tools.container.ServiceContainer;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class XAFailoverTest extends ClusteringTestBase
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private ServiceContainer sc;
   
   private TransactionManager tm;
   
   private Transaction suspended;
   
   // Constructors ---------------------------------------------------------------------------------

   public XAFailoverTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------
   
   protected void setUp() throws Exception
   {
      nodeCount = 2;

      super.setUp();

      sc = new ServiceContainer("transaction");
      
      //Don't drop the tables again!
      sc.start(false);
   
      InitialContext localIc = new InitialContext(InVMInitialContextFactory.getJNDIEnvironment());
           
      tm = (TransactionManager)localIc.lookup(ServiceContainer.TRANSACTION_MANAGER_JNDI_NAME);
      
      suspended = tm.suspend();
      
      log.debug("setup done");
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
      
      sc.stop();
      
      if (suspended != null)
      {
         tm.resume(suspended);
      }
   }

   public void testSimpleXAConnectionFailover() throws Exception
   {
      XAConnection conn = null;
      
      XAConnectionFactory xaCF = (XAConnectionFactory)cf;

      try
      {
         // create a connection to node 1
         conn = createXAConnectionOnServer(xaCF, 1);
         conn.start();

         assertEquals(1, getServerId(conn));

         // register a failover listener
         SimpleFailoverListener failoverListener = new SimpleFailoverListener();
         ((JBossConnection)conn).registerFailoverListener(failoverListener);

         log.debug("killing node 1 ....");

         ServerManagement.kill(1);

         log.info("########");
         log.info("######## KILLED NODE 1");
         log.info("########");

         // wait for the client-side failover to complete

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

         // failover complete
         log.info("failover completed");

         assertEquals(0, getServerId(conn));

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }
   

   public void testSendFailBeforePrepare() throws Exception
   {
      XAConnection xaConn = null;
      
      XAConnectionFactory xaCF = (XAConnectionFactory)cf;
      
      Connection conn = null;
      
      try
      {
         // create a connection to node 1
         xaConn = createXAConnectionOnServer(xaCF, 1);
         
         assertEquals(1, getServerId(xaConn));

         conn = createConnectionOnServer(cf, 1);
         
         assertEquals(1, getServerId(conn));
         
         conn.start();


         // register a failover listener
         SimpleFailoverListener failoverListener = new SimpleFailoverListener();
         ((JBossConnection)xaConn).registerFailoverListener(failoverListener);
         
         // Create a normal consumer on the queue
         Session sessRec = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons = sessRec.createConsumer(queue[1]);
         
         // Create an XA session
         
         XASession sess = xaConn.createXASession();
         
         XAResource res = sess.getXAResource();
         
         MessageProducer prod = sess.createProducer(queue[1]);
         
         tm.begin();
         
         Transaction tx = tm.getTransaction();
         
         tx.enlistResource(res);
         
         //Enlist a dummy XAResource to force 2pc
         XAResource dummy = new DummyXAResource();
         
         tx.enlistResource(dummy);
         
         //Send a message
         
         TextMessage msg = sess.createTextMessage("Cupid stunt");
         
         prod.send(msg);
         
         //Make sure message can't be received
         
         Message m = cons.receive(2000);
         
         assertNull(m);
         
         tx.delistResource(res, XAResource.TMSUCCESS);
         
         tx.delistResource(dummy, XAResource.TMSUCCESS);
         
         //Now kill node 1
         
         log.debug("killing node 1 ....");

         ServerManagement.kill(1);

         log.info("########");
         log.info("######## KILLED NODE 1");
         log.info("########");

         // wait for the client-side failover to complete

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

         // failover complete
         log.info("failover completed");
         
         //Now commit the transaction
         
         tm.commit();
         
         // Message should now be receivable
         
         TextMessage mrec = (TextMessage)cons.receive(2000);
         
         assertNotNull(mrec);
         
         assertEquals(msg.getText(), mrec.getText());
         
         m = cons.receive(2000);
         
         assertNull(m);

         assertEquals(0, getServerId(xaConn));

      }
      finally
      {
         if (xaConn != null)
         {
            xaConn.close();
         }
         if (conn != null)
         {
            conn.close();
         }
      }
   }
   
   public void testSendAndReceiveFailBeforePrepare() throws Exception
   {
      XAConnection xaConn = null;
      
      XAConnectionFactory xaCF = (XAConnectionFactory)cf;
      
      Connection conn = null;
      
      try
      {
         // create a connection to node 1
         xaConn = createXAConnectionOnServer(xaCF, 1);
         
         assertEquals(1, ((JBossConnection)xaConn).getServerID());

         conn = this.createConnectionOnServer(cf, 1);      
         
         assertEquals(1, ((JBossConnection)conn).getServerID());
         
         conn.start();
         
         xaConn.start();

         // register a failover listener
         SimpleFailoverListener failoverListener = new SimpleFailoverListener();
         ((JBossConnection)xaConn).registerFailoverListener(failoverListener);
         
         // Create a normal consumer on the queue
         Session sessRec = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         //Send a message to the queue
         MessageProducer prod = sessRec.createProducer(queue[1]);
         
         TextMessage sent = sessRec.createTextMessage("plop");
         
         prod.send(sent);
         
         // Create an XA session
         
         XASession sess = xaConn.createXASession();
         
         XAResource res = sess.getXAResource();
         
         MessageProducer prod2 = sess.createProducer(queue[1]);
         
         MessageConsumer cons2 = sess.createConsumer(queue[1]);
         
         tm.begin();
         
         Transaction tx = tm.getTransaction();
         
         tx.enlistResource(res);
         
         //Enlist a dummy XAResource to force 2pc
         XAResource dummy = new DummyXAResource();        
         
         tx.enlistResource(dummy);
         
         //receive a message
         
         TextMessage received = (TextMessage)cons2.receive(2000);
         
         assertNotNull(received);
         
         assertEquals(sent.getText(), received.getText());
         
         //Send a message
         
         TextMessage msg = sess.createTextMessage("Cupid stunt");
         
         prod2.send(msg);
         
         // Make sure can't be received
         
         MessageConsumer cons = sessRec.createConsumer(queue[1]);
         
         Message m = cons.receive(2000);
         
         assertNull(m);
                  
         tx.delistResource(res, XAResource.TMSUCCESS);
         
         tx.delistResource(dummy, XAResource.TMSUCCESS);
         
         //Now kill node 1
         
         log.debug("killing node 1 ....");

         ServerManagement.kill(1);

         log.info("########");
         log.info("######## KILLED NODE 1");
         log.info("########");

         // wait for the client-side failover to complete

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

         // failover complete
         log.info("failover completed");
         
         //Now commit the transaction
         
         tm.commit();
         
         // Message should now be receivable
         
         cons2.close();
         
         TextMessage mrec = (TextMessage)cons.receive(2000);
         
         assertNotNull(mrec);
         
         assertEquals(msg.getText(), mrec.getText());
         
         m = cons.receive(2000);
         
         //And the other message should be acked
         assertNull(m);                  

         assertEquals(0, ((JBossConnection)xaConn).getServerID());

      }
      finally
      {
         if (xaConn != null)
         {
            xaConn.close();
         }
         if (conn != null)
         {
            conn.close();
         }
      }
   }
     
   public void testSendAndReceiveTwoConnectionsFailBeforePrepare() throws Exception
   {
      XAConnection xaConn0 = null;
      
      XAConnection xaConn1 = null;
      
      XAConnectionFactory xaCF = (XAConnectionFactory)cf;
      
      try
      {
         xaConn0 = createXAConnectionOnServer(xaCF, 0);
         
         assertEquals(0, ((JBossConnection)xaConn0).getServerID());

         xaConn1 = createXAConnectionOnServer(xaCF, 1);
         
         assertEquals(1, ((JBossConnection)xaConn1).getServerID());

         TextMessage sent0 = null;

         TextMessage sent1 = null;

         // Sending two messages.. on each server
         {
            Connection conn0 = null;

            Connection conn1 = null;

            conn0 = this.createConnectionOnServer(cf, 0);

            assertEquals(0, ((JBossConnection)conn0).getServerID());

            conn1 = this.createConnectionOnServer(cf, 1);

            assertEquals(1, ((JBossConnection)conn1).getServerID());

            //Send a message to each queue

            Session sess = conn0.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageProducer prod = sess.createProducer(queue[0]);

            sent0 = sess.createTextMessage("plop0");

            prod.send(sent0);

            sess.close();

            sess = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);

            prod = sess.createProducer(queue[1]);

            sent1 = sess.createTextMessage("plop1");

            prod.send(sent1);

            sess.close();
            
            conn0.close();
            
            conn1.close();
         }

         xaConn0.start();
         
         xaConn1.start();
                  
         // register a failover listener
         SimpleFailoverListener failoverListener = new SimpleFailoverListener();
         ((JBossConnection)xaConn1).registerFailoverListener(failoverListener);
                
         tm.begin();
         
         Transaction tx = tm.getTransaction();
            
         //receive and send a message on each
         
         // node 0
         
         XASession sess0 = xaConn0.createXASession();
         
         XAResource res0 = sess0.getXAResource();
         
         tx.enlistResource(res0);         
         
         MessageProducer prod0 = sess0.createProducer(queue[0]);
         
         MessageConsumer cons0 = sess0.createConsumer(queue[0]);
         
         TextMessage received = (TextMessage)cons0.receive(2000);
         
         log.info("Got message " + received.getText());
         
         assertNotNull(received);         
         
         assertEquals(sent0.getText(), received.getText());                  
         
         TextMessage msg0 = sess0.createTextMessage("Cupid stunt0");
         
         prod0.send(msg0);
         
         //Make sure the consumer is closed otherwise message might be sucked
         cons0.close();
                  
         //node 1
         
         XASession sess1 = xaConn1.createXASession();
         
         XAResource res1 = sess1.getXAResource();
         
         tx.enlistResource(res1);
                  
         MessageProducer prod1 = sess1.createProducer(queue[1]);
         
         MessageConsumer cons1 = sess1.createConsumer(queue[1]);
         
         received = (TextMessage)cons1.receive(2000);
              
         log.info("Got message " + received.getText());
         
         assertNotNull(received);         
         
         assertEquals(sent1.getText(), received.getText());         
                      
         TextMessage msg1 = sess1.createTextMessage("Cupid stunt1");
         
         prod1.send(msg1);
         
         cons1.close();
                 
         tx.delistResource(res0, XAResource.TMSUCCESS);
         
         tx.delistResource(res1, XAResource.TMSUCCESS);
         
         //Now kill node 1
         
         log.debug("killing node 1 ....");

         ServerManagement.kill(1);

         log.info("########");
         log.info("######## KILLED NODE 1");
         log.info("########");

         // wait for the client-side failover to complete

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

         // failover complete
         log.info("failover completed");
         
         //Now commit the transaction
         
         tm.commit();
         
         // Messages should now be receivable

         Connection conn = null;
         try
         {
            conn = this.createConnectionOnServer(cf, 0);

            conn.start();

            Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageConsumer cons = session.createConsumer(queue[0]);

            HashSet receivedMessages = new HashSet();

            int numberOfReceivedMessages = 0;

            while(true)
            {
               TextMessage message = (TextMessage)cons.receive(2000);
               if (message == null)
               {
                  break;
               }
               log.info("Message = (" + message.getText() + ")");
               receivedMessages.add(message.getText());
               numberOfReceivedMessages++;
            }

            //These two should be acked
            
            assertFalse("\"plop0\" message was duplicated",
               receivedMessages.contains("plop0"));

            assertFalse("\"plop1\" message was duplicated",
               receivedMessages.contains("plop1"));

            //And these should be receivable
            
            assertTrue("\"Cupid stunt0\" message wasn't received",
               receivedMessages.contains("Cupid stunt0"));

            assertTrue("\"Cupid stunt1\" message wasn't received",
               receivedMessages.contains("Cupid stunt1"));

            assertEquals(2, numberOfReceivedMessages);

            assertEquals(0, ((JBossConnection)xaConn1).getServerID());
         }
         finally
         {
            if (conn != null)
            {
               conn.close();
            }
         }
      }
      finally
      {
         if (xaConn1 != null)
         {
            xaConn1.close();
         }
         if (xaConn0 != null)
         {
            xaConn0.close();
         }
      }
   }
   
   public void testSendAndReceiveFailAfterPrepareAndRetryCommit() throws Exception
   {
      XAConnection xaConn1 = null;
      
      XAConnectionFactory xaCF = (XAConnectionFactory)cf;
      
      TextMessage sent1 = null;

      // Sending a messages
      {
         Connection conn1 = createConnectionOnServer(cf, 1);

         assertEquals(1, getServerId(conn1));

         //Send a message
        
         Session sess = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = sess.createProducer(queue[1]);

         sent1 = sess.createTextMessage("plop1");

         prod.send(sent1);

         conn1.close();
      }

      try
      {
         xaConn1 = createXAConnectionOnServer(xaCF, 1);
         assertEquals(1, getServerId(xaConn1));

         xaConn1.start();
                  
         // register a failover listener
         SimpleFailoverListener failoverListener = new SimpleFailoverListener();
         ((JBossConnection)xaConn1).registerFailoverListener(failoverListener);         
         
         XASession sess1 = xaConn1.createXASession();
         
         XAResource res1 = sess1.getXAResource();
         
         MessageProducer prod1 = sess1.createProducer(queue[1]);
         
         MessageConsumer cons1 = sess1.createConsumer(queue[1]);
                                    
         tm.begin();
         
         Transaction tx = tm.getTransaction();
         
         tx.enlistResource(res1);
         
         //enlist an extra resource to force 2pc
         
         XAResource dummy = new DummyXAResource();
         tx.enlistResource(dummy);
                  
         //receive a message
         
         TextMessage received = (TextMessage)cons1.receive(2000);
         
         assertNotNull(received);
         
         assertEquals(sent1.getText(), received.getText());
                                             
         //Send a message
               
         TextMessage msg1 = sess1.createTextMessage("Cupid stunt1");
         
         prod1.send(msg1);
                  
         tx.delistResource(res1, XAResource.TMSUCCESS);
         
         tx.delistResource(dummy, XAResource.TMSUCCESS);
         
         // We poison node 1 so that it crashes after prepare but before commit is processed
         
         ServerManagement.poisonTheServer(1, PoisonInterceptor.TYPE_2PC_COMMIT);

         log.info("################################################################## Sending a commit");
         tm.commit();
         
         log.info("########");
         log.info("######## KILLED NODE 1");
         log.info("########");

         // wait for the client-side failover to complete

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
         
         //When the node comes back up, the invocation to commit() will be retried on the new node.
         //The new node will by then already have loaded into memory the prepared transactions from
         //the failed node so this should complete ok

         // failover complete
         log.info("failover completed");
         
         xaConn1.close();
                           

         // Message should now be receivable
         Connection conn = null;
         try
         {
            conn = this.createConnectionOnServer(cf, 0);
            
            assertEquals(0, getServerId(conn));

            conn.start();

            Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageConsumer cons = session.createConsumer(queue[0]);

            HashSet receivedMessages = new HashSet();

            int numberOfReceivedMessages = 0;

            while(true)
            {
               TextMessage message = (TextMessage)cons.receive(2000);
               if (message == null)
               {
                  break;
               }
               log.info("Message = (" + message.getText() + ")");
               receivedMessages.add(message.getText());
               numberOfReceivedMessages++;
            }


            assertFalse("\"plop1\" message was duplicated",
               receivedMessages.contains("plop0"));

            assertTrue("\"Cupid stunt1\" message wasn't received",
               receivedMessages.contains("Cupid stunt1"));

            assertEquals(1, numberOfReceivedMessages);

            assertEquals(0, getServerId(xaConn1));
         }
         finally
         {
            if (conn != null)
            {
               conn.close();
            }
         }
                  
         assertEquals(0, getServerId(xaConn1));
      }
      finally
      {
         if (xaConn1 != null)
         {
            xaConn1.close();
         }
      }
   }
   
   public void testSendAndReceiveTwoConnectionsFailAfterPrepareAndRecover() throws Exception
   {
      XAConnection xaConn0 = null;
      
      XAConnection xaConn1 = null;
      
      XAConnectionFactory xaCF = (XAConnectionFactory)cf;
      
      TextMessage sent0 = null;

      TextMessage sent1 = null;

      // Sending two messages.. on each server
      {
         Connection conn0 = null;

         Connection conn1 = null;

         conn0 = this.createConnectionOnServer(cf, 0);

         assertEquals(0, ((JBossConnection)conn0).getServerID());

         conn1 = this.createConnectionOnServer(cf, 1);

         assertEquals(1, ((JBossConnection)conn1).getServerID());

         //Send a message to each queue

         Session sess = conn0.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = sess.createProducer(queue[0]);

         sent0 = sess.createTextMessage("plop0");

         prod.send(sent0);

         sess.close();

         sess = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);

         prod = sess.createProducer(queue[1]);

         sent1 = sess.createTextMessage("plop1");

         prod.send(sent1);

         sess.close();
         
         conn0.close();
         
         conn1.close();
      }

      try
      {
         xaConn0 = xaCF.createXAConnection();
         
         assertEquals(0, ((JBossConnection)xaConn0).getServerID());

         xaConn1 = xaCF.createXAConnection();
         
         assertEquals(1, ((JBossConnection)xaConn1).getServerID());

         xaConn0.start();
         
         xaConn1.start();
                  
         // register a failover listener
         SimpleFailoverListener failoverListener = new SimpleFailoverListener();
         ((JBossConnection)xaConn1).registerFailoverListener(failoverListener);
         
                                         
         tm.begin();
         
         Transaction tx = tm.getTransaction();
         
         //receive and send a message on each
         
         // node 0
         
         XASession sess0 = xaConn0.createXASession();
         
         XAResource res0 = sess0.getXAResource();
         
         tx.enlistResource(res0);         
         
         MessageProducer prod0 = sess0.createProducer(queue[0]);
         
         MessageConsumer cons0 = sess0.createConsumer(queue[0]);
         
         TextMessage received = (TextMessage)cons0.receive(2000);
         
         log.info("Got message " + received.getText());
         
         assertNotNull(received);         
         
         assertEquals(sent0.getText(), received.getText());                  
         
         TextMessage msg0 = sess0.createTextMessage("Cupid stunt0");
         
         prod0.send(msg0);
         
         //Make sure the consumer is closed otherwise message might be sucked
         cons0.close();
                 
         //node 1
         
         XASession sess1 = xaConn1.createXASession();
         
         XAResource res1 = sess1.getXAResource();
         
         tx.enlistResource(res1);
                  
         MessageProducer prod1 = sess1.createProducer(queue[1]);
         
         MessageConsumer cons1 = sess1.createConsumer(queue[1]);
         
         received = (TextMessage)cons1.receive(2000);
              
         log.info("Got message " + received.getText());
         
         assertNotNull(received);         
         
         assertEquals(sent1.getText(), received.getText());         
                      
         TextMessage msg1 = sess1.createTextMessage("Cupid stunt1");
         
         prod1.send(msg1);
         
         cons1.close();
              
         tx.delistResource(res0, XAResource.TMSUCCESS);
         
         tx.delistResource(res1, XAResource.TMSUCCESS);
         
         // We poison node 1 so that it crashes after prepare but before commit is processed
         
         ServerManagement.poisonTheServer(1, PoisonInterceptor.TYPE_2PC_COMMIT);
         
         tm.commit();
         
         //Now kill node 1
         
         log.debug("killing node 1 ....");

         ServerManagement.kill(1);

         log.info("########");
         log.info("######## KILLED NODE 1");
         log.info("########");

         // wait for the client-side failover to complete

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
         
         //When the node comes back up, the invocation to commit() will be retried on the new node.
         //The new node will by then already have loaded into memory the prepared transactions from
         //the failed node so this should complete ok

         // failover complete
         log.info("failover completed");
         
         // Message should now be receivable
         Connection conn = null;
         try
         {
            conn = this.createConnectionOnServer(cf, 0);

            conn.start();

            Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageConsumer cons = session.createConsumer(queue[0]);

            HashSet receivedMessages = new HashSet();

            int numberOfReceivedMessages = 0;

            while(true)
            {
               TextMessage message = (TextMessage)cons.receive(2000);
               if (message == null)
               {
                  break;
               }
               log.info("Message = (" + message.getText() + ")");
               receivedMessages.add(message.getText());
               numberOfReceivedMessages++;
            }

            assertFalse("\"plop0\" message was duplicated",
               receivedMessages.contains("plop0"));

            assertFalse("\"plop1\" message was duplicated",
               receivedMessages.contains("plop0"));

            assertTrue("\"Cupid stunt0\" message wasn't received",
               receivedMessages.contains("Cupid stunt0"));

            assertTrue("\"Cupid stunt1\" message wasn't received",
               receivedMessages.contains("Cupid stunt1"));

            assertEquals(2, numberOfReceivedMessages);

            assertEquals(0, ((JBossConnection)xaConn1).getServerID());
         }
         finally
         {
            if (conn != null)
            {
               conn.close();
            }
         }
                  
         assertEquals(0, ((JBossConnection)xaConn1).getServerID());
      }
      finally
      {
         if (xaConn1 != null)
         {
            xaConn1.close();
         }
         if (xaConn0 != null)
         {
            xaConn0.close();
         }
      }
   }
   
   // Inner classes --------------------------------------------------------------------------------

   static class DummyXAResource implements XAResource
   {
      boolean failOnPrepare;
      
      DummyXAResource()
      {         
      }
      
      public void commit(Xid arg0, boolean arg1) throws XAException
      {         
      }

      public void end(Xid arg0, int arg1) throws XAException
      {
      }

      public void forget(Xid arg0) throws XAException
      {
      }

      public int getTransactionTimeout() throws XAException
      {
          return 0;
      }

      public boolean isSameRM(XAResource arg0) throws XAException
      {
         return false;
      }

      public int prepare(Xid arg0) throws XAException
      {
         return XAResource.XA_OK;
      }

      public Xid[] recover(int arg0) throws XAException
      {
         return null;
      }

      public void rollback(Xid arg0) throws XAException
      {
      }

      public boolean setTransactionTimeout(int arg0) throws XAException
      {
         return false;
      }

      public void start(Xid arg0, int arg1) throws XAException
      {
      }      
   }  
}
