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
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.XAConnection;
import javax.jms.XASession;
import javax.jms.MessageListener;
import javax.jms.XAConnectionFactory;
import javax.jms.ServerSessionPool;
import javax.jms.ServerSession;
import javax.jms.JMSException;
import javax.management.ObjectName;
import javax.naming.InitialContext;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.UserTransaction;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.jms.client.JBossConnection;
import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.jms.client.JBossSession;
import org.jboss.jms.client.delegate.ClientConnectionDelegate;
import org.jboss.jms.client.delegate.DelegateSupport;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.client.state.SessionState;
import org.jboss.jms.tx.MessagingXAResource;
import org.jboss.jms.tx.ResourceManager;
import org.jboss.jms.tx.LocalTx;
import org.jboss.messaging.core.tx.MessagingXid;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.jmx.ServiceContainer;
import org.jboss.test.messaging.tools.jndi.InVMInitialContextFactory;
import org.jboss.tm.TransactionManagerLocator;
import org.jboss.tm.TxUtils;
import org.jboss.logging.Logger;

import com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionManagerImple;
import java.util.ArrayList;

/**
 *
 * A XATestBase
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public abstract class XATestBase extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected InitialContext initialContext;

   protected JBossConnectionFactory cf;

   protected Destination queue;

   protected TransactionManager tm;

   protected Transaction suspendedTx;

   protected ServiceContainer sc;

   protected boolean useArjuna;


   // Constructors --------------------------------------------------

   public XATestBase(String name)
   {
      super(name);
   }


   // TestCase overrides -------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      if (useArjuna)
      {
         ServerManagement.start("all,-transaction,jbossjta");
      }
      else
      {
         ServerManagement.start("all");
      }

      //Also need a local tx mgr if test is running remote
      if (ServerManagement.isRemote())
      {
         if (useArjuna)
         {
            sc = new ServiceContainer("all,-transaction,jbossjta");
         }
         else
         {
            sc = new ServiceContainer("transaction");
         }

         //Don't drop the tables again!
         sc.start(false);
      }

      initialContext = new InitialContext(ServerManagement.getJNDIEnvironment());

      InitialContext localIc = new InitialContext(InVMInitialContextFactory.getJNDIEnvironment());

      cf = (JBossConnectionFactory)initialContext.lookup("/ConnectionFactory");

      tm = (TransactionManager)localIc.lookup(ServiceContainer.TRANSACTION_MANAGER_JNDI_NAME);

      if (useArjuna)
      {
         log.info("tm is " + tm.getClass().getName());
         assertTrue(tm instanceof TransactionManagerImple);
      }
      else
      {
         assertTrue(tm instanceof org.jboss.tm.TxManager);
      }

      ServerManagement.undeployQueue("Queue");

      ServerManagement.deployQueue("Queue");

      queue = (Destination)initialContext.lookup("/queue/Queue");

      drainDestination(cf, queue);

      if (!ServerManagement.isRemote())
      {
         suspendedTx = tm.suspend();
      }
   }

   public void tearDown() throws Exception
   {
      ServerManagement.undeployQueue("Queue");

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

      if (ServerManagement.isRemote())
      {
         sc.stop();
      }

      ServerManagement.stop();

      super.tearDown();
   }




   // Public --------------------------------------------------------


   /* If there is no global tx present the send must behave as non transacted
    * See http://www.jboss.com/index.html?module=bb&op=viewtopic&t=98577&postdays=0&postorder=asc&start=0
    * http://jira.jboss.com/jira/browse/JBMESSAGING-410
    * http://jira.jboss.com/jira/browse/JBMESSAGING-721
    * http://jira.jboss.org/jira/browse/JBMESSAGING-946
    */
   public void testSendNoGlobalTransaction() throws Exception
   {

      if (ServerManagement.isRemote())
      {
         log.info("************************************************************************************************************************");
         log.info("testSendNoGlobalTransaction is being ignored on remote runs, as this requires access to the JCA layer on java:/");
         log.info("************************************************************************************************************************");
         return;
      }

      Transaction suspended = null;

      try
      {
         ServerManagement.deployQueue("MyQueue");

         // make sure there's no active JTA transaction

         suspended = TransactionManagerLocator.getInstance().locate().suspend();

         // send a message to the queue, using a JCA wrapper

         Queue queue = (Queue)initialContext.lookup("queue/MyQueue");

         ConnectionFactory mcf =
            (ConnectionFactory)initialContext.lookup("java:/JCAConnectionFactory");

         Connection conn = mcf.createConnection();

         Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = s.createProducer(queue);
         p.setDeliveryMode(DeliveryMode.PERSISTENT);
         Message m = s.createTextMessage("one");

         p.send(m);

         log.debug("message sent");

         conn.close();

         // receive the message
         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");
         conn = cf.createConnection();
         conn.start();
         s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer c = s.createConsumer(queue);
         TextMessage rm = (TextMessage)c.receive(1000);

         assertEquals("one", rm.getText());

         conn.close();
      }
      finally
      {
         ServerManagement.undeployQueue("MyQueue");

         if (suspended != null)
         {
            TransactionManagerLocator.getInstance().locate().resume(suspended);
         }
      }
   }

   /* If there is no global tx present the send must behave as non transacted
    * See http://www.jboss.com/index.html?module=bb&op=viewtopic&t=98577&postdays=0&postorder=asc&start=0
    * http://jira.jboss.com/jira/browse/JBMESSAGING-410
    * http://jira.jboss.com/jira/browse/JBMESSAGING-721
    * http://jira.jboss.org/jira/browse/JBMESSAGING-946
    */
   public void testSendNoGlobalTransaction2() throws Exception
   {

      if (ServerManagement.isRemote())
      {
         log.info("************************************************************************************************************************");
         log.info("testSendNoGlobalTransaction2 is being ignored on remote runs, as this requires access to the JCA layer on java:/");
         log.info("************************************************************************************************************************");
         return;
      }

      Transaction suspended = TransactionManagerLocator.getInstance().locate().suspend();

      try
      {

         ConnectionFactory mcf =
            (ConnectionFactory)initialContext.lookup("java:/JCAConnectionFactory");
         Connection conn = mcf.createConnection();
         conn.start();

         UserTransaction ut = ServerManagement.getUserTransaction();

         ut.begin();

         Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = s.createProducer(queue);
         Message m = s.createTextMessage("one");

         p.send(m);

         ut.commit();

         conn.close();

         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("ConnectionFactory");
         conn = cf.createConnection();
         s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         conn.start();

         TextMessage rm = (TextMessage)s.createConsumer(queue).receive(500);

         assertEquals("one", rm.getText());

         conn.close();

         // make sure there's no active JTA transaction

         assertNull(TransactionManagerLocator.getInstance().locate().getTransaction());

         // send a message to the queue, using a JCA wrapper

         mcf = (ConnectionFactory)initialContext.lookup("java:/JCAConnectionFactory");

         conn = mcf.createConnection();

         s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         p = s.createProducer(queue);
         p.setDeliveryMode(DeliveryMode.PERSISTENT);
         m = s.createTextMessage("one");

         p.send(m);

         conn.close();

         // receive the message
         cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");
         conn = cf.createConnection();
         conn.start();
         s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer c = s.createConsumer(queue);
         rm = (TextMessage)c.receive(1000);

         assertEquals("one", rm.getText());

         conn.close();
      }
      finally
      {
         if (suspended != null)
         {
            TransactionManagerLocator.getInstance().locate().resume(suspended);
         }
      }
   }

   /*
    * If there is no global tx present messages consumed must consumed as if they were in a
    * local tx. Note this behaviour differs from messages sent
    * This is so we can support transacted delivery of messags in an MDB as mentioned
    *
    * However to accomodate TCK tests, when Sessions/Consumers are being used without transaction
    * enlist, we will process those cases as NonTransactional, AutoAck.
    *
    * See http://www.jboss.com/index.html?module=bb&op=viewtopic&t=98577&postdays=0&postorder=asc&start=0
    * http://jira.jboss.com/jira/browse/JBMESSAGING-410
    * http://jira.jboss.com/jira/browse/JBMESSAGING-721
    * http://jira.jboss.org/jira/browse/JBMESSAGING-946
    *
    * For transactional delivery the receipt of the message should be in a transaction but by the time
    * the mdb container is invoked the message has already been received it is too late - the message
    * has already been received and passed on (see page 199 (chapter 5 JMS and Transactions, section "Application Server Integration"
    * of Mark Little's book Java Transaction processing
    * for a discussion of how different app serves deal with this)
    * The way jboss messaging (and jboss mq) deals with this is to convert any work done
    * prior to when the xasession is enlisted in the tx, into work done in the xa tx.
    *
    *      NOTE: To accomodate TCK tests where Session/Consumers are being used without transaction enlisting
    *            we are processing those cases as nonTransactional/AutoACK, however if the session is being used
    *            to process MDBs we will consider the LocalTransaction convertion and process those as the comment above
    *            This was done as per: http://jira.jboss.org/jira/browse/JBMESSAGING-946
    *
    */
   public void testConnectionConsumer() throws Exception
   {
      ServerManagement.deployQueue("MyQueue2");


      try
      {
         // send a message to the queue

         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("/ConnectionFactory");
         Queue queue = (Queue) initialContext.lookup("queue/MyQueue2");
         Connection conn = cf.createConnection();
         Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = s.createProducer(queue);
         p.setDeliveryMode(DeliveryMode.PERSISTENT);
         Message m = s.createTextMessage("one");
         p.send(m);
         conn.close();

         // make sure there's no active JTA transaction

         Transaction suspended = tm.suspend();

         XAConnection xaconn = null;
         try
         {

            ObjectName queueMBean = new ObjectName("jboss.messaging.destination:service=Queue,name=MyQueue2");
            Integer count = (Integer) ServerManagement.getAttribute(queueMBean, "MessageCount");
            assertEquals(1, count.intValue());

            // using XA with a ConnectionConsumer (testing the transaction behavior under MDBs)

            XAConnectionFactory xacf = (XAConnectionFactory) cf;
            xaconn = xacf.createXAConnection();
            xaconn.start();
            XASession xasession = xaconn.createXASession();
            DummyListener listener = new DummyListener();
            xasession.setMessageListener(listener);

            ServerSessionPool pool = new MockServerSessionPool(xasession);

            xaconn.createConnectionConsumer(queue, null, pool, 1);

            Thread.sleep(1000);
            assertEquals(1, listener.messages.size());

            // Message should still be on server
            count = (Integer) ServerManagement.getAttribute(queueMBean, "MessageCount");
            assertEquals(1, count.intValue());

            XAResource resource = xasession.getXAResource();

            // Starts a new transaction
            tm.begin();

            Transaction trans = tm.getTransaction();

            JBossSession session = (JBossSession)xasession;
            SessionState state = (SessionState)((DelegateSupport)session.getDelegate()).getState();

            // Validates TX convertion
            assertTrue(state.getCurrentTxId() instanceof LocalTx);

            // Enlist the transaction... as supposed to be happening on JBossAS with the
            // default listener (enlist happening after message is received)
            trans.enlistResource(resource);

            // Validates TX convertion
            assertFalse(state.getCurrentTxId() instanceof LocalTx);

            trans.delistResource(resource, XAResource.TMSUCCESS);

            trans.commit();


            // After commit the message should be consumed
            count = (Integer) ServerManagement.getAttribute(queueMBean, "MessageCount");
            assertEquals(0, count.intValue());
         }
         finally
         {
            if (xaconn != null)
            {
               xaconn.close();
            }
            if (suspended != null)
            {
               TransactionManagerLocator.getInstance().locate().resume(suspended);
            }
         }
      }
      finally
      {
         ServerManagement.undeployQueue("MyQueue2");
      }


   }

   /*
    * If there is no global tx present messages consumed must consumed as if they were in a
    * local tx. Note this behaviour differs from messages sent
    * This is so we can support transacted delivery of messags in an MDB as mentioned
    *
    * However to accomodate TCK tests, when Sessions/Consumers are being used without transaction
    * enlist, we will process those cases as NonTransactional, AutoAck.
    *
    * See http://www.jboss.com/index.html?module=bb&op=viewtopic&t=98577&postdays=0&postorder=asc&start=0
    * http://jira.jboss.com/jira/browse/JBMESSAGING-410
    * http://jira.jboss.com/jira/browse/JBMESSAGING-721
    * http://jira.jboss.org/jira/browse/JBMESSAGING-946
    *
    * For transactional delivery the receipt of the message should be in a transaction but by the time
    * the mdb container is invoked the message has already been received it is too late - the message
    * has already been received and passed on (see page 199 (chapter 5 JMS and Transactions, section "Application Server Integration"
    * of Mark Little's book Java Transaction processing
    * for a discussion of how different app serves deal with this)
    * The way jboss messaging (and jboss mq) deals with this is to convert any work done
    * prior to when the xasession is enlisted in the tx, into work done in the xa tx.
    *
    *      NOTE: To accomodate TCK tests where Session/Consumers are being used without transaction enlisting
    *            we are processing those cases as nonTransactional/AutoACK, however if the session is being used
    *            to process MDBs we will consider the LocalTransaction convertion and process those as the comment above
    *            This was done as per: http://jira.jboss.org/jira/browse/JBMESSAGING-946
    *
    */
   public void testReceiveGlobalTransaction() throws Exception
   {
      XAConnection xaconn = null;

      try
      {
         ServerManagement.deployQueue("MyQueue2");

         // send a message to the queue

         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");
         Queue queue = (Queue)initialContext.lookup("queue/MyQueue2");
         Connection conn = cf.createConnection();
         Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = s.createProducer(queue);
         p.setDeliveryMode(DeliveryMode.PERSISTENT);
         Message m = s.createTextMessage("one");
         p.send(m);
         conn.close();

         ObjectName queueMBean = new ObjectName("jboss.messaging.destination:service=Queue,name=MyQueue2");
         Integer count = (Integer)ServerManagement.getAttribute(queueMBean, "MessageCount");
         assertEquals(1, count.intValue());

         tm.begin();

         Transaction trans = tm.getTransaction();

         XAConnectionFactory xacf = (XAConnectionFactory)cf;

         xaconn = xacf.createXAConnection();

         xaconn.start();

         XASession xasession = xaconn.createXASession();


         XAResource resouce = xasession.getXAResource();
         trans.enlistResource(resouce);

         MessageConsumer consumer = xasession.createConsumer(queue);

         TextMessage messageReceived = (TextMessage)consumer.receive(1000);

         assertNotNull(messageReceived);

         assertEquals("one", messageReceived.getText());

         assertNull(consumer.receive(1000));

         count = (Integer)ServerManagement.getAttribute(queueMBean, "MessageCount");
         assertEquals(1, count.intValue());

         trans.delistResource(resouce, XAResource.TMSUCCESS);

         tm.rollback();

         tm.begin();
         trans = tm.getTransaction();
         trans.enlistResource(resouce);

         messageReceived = (TextMessage)consumer.receive(1000);

         assertNotNull(messageReceived);

         assertEquals("one", messageReceived.getText());

         count = (Integer)ServerManagement.getAttribute(queueMBean, "MessageCount");
         assertEquals(1, count.intValue());

         trans.commit();

         count = (Integer)ServerManagement.getAttribute(queueMBean, "MessageCount");
         assertEquals(0, count.intValue());

      }
      finally
      {
         if (xaconn != null)
         {
            xaconn.close();
         }
         ServerManagement.undeployQueue("MyQueue2");
      }
   }

   /*
    *   This test will:
    *     - Send two messages over a producer
    *     - Receive one message over a consumer created used a XASession
    *     - Call Recover
    *     - Receive the second message
    *     - The queue should be empty after that 
    *   Verifies if messages are sent ok and ack properly when recovery is called
    *      NOTE: To accomodate TCK tests where Session/Consumers are being used without transaction enlisting
    *            we are processing those cases as nonTransactional/AutoACK, however if the session is being used
    *            to process MDBs we will consider the LocalTransaction convertion and process those as the comment above
    *            This was done as per: http://jira.jboss.org/jira/browse/JBMESSAGING-946
    *
    */
   public void testRecoverOnXA() throws Exception
   {
      XAConnection xaconn = null;

      try
      {
         ServerManagement.deployQueue("MyQueue2");

         // send a message to the queue

         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");
         Queue queue = (Queue)initialContext.lookup("queue/MyQueue2");
         Connection conn = cf.createConnection();
         Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = s.createProducer(queue);
         p.setDeliveryMode(DeliveryMode.PERSISTENT);
         Message m = s.createTextMessage("one");
         p.send(m);
         m = s.createTextMessage("two");
         p.send(m);
         conn.close();

         ObjectName queueMBean = new ObjectName("jboss.messaging.destination:service=Queue,name=MyQueue2");
         Integer count = (Integer)ServerManagement.getAttribute(queueMBean, "MessageCount");
         assertEquals(2, count.intValue());

         XAConnectionFactory xacf = (XAConnectionFactory)cf;

         xaconn = xacf.createXAConnection();

         xaconn.start();

         XASession xasession = xaconn.createXASession();

         MessageConsumer consumer = xasession.createConsumer(queue);

         TextMessage messageReceived = (TextMessage)consumer.receive(1000);

         assertNotNull(messageReceived);

         assertEquals("one", messageReceived.getText());

         xasession.recover();

         messageReceived = (TextMessage)consumer.receive(1000);

         assertEquals("two", messageReceived.getText());

         consumer.close();

         // I can't call xasession.close for this test as JCA layer would cache the session
         // So.. keep this close commented!
         //xasession.close();

         count = (Integer)ServerManagement.getAttribute(queueMBean, "MessageCount");
         assertEquals(0, count.intValue());


      }
      finally
      {
         if (xaconn != null)
         {
            xaconn.close();
         }
         ServerManagement.undeployQueue("MyQueue2");
      }
   }

   /*
    * If there is no global tx present messages consumed must consumed as if they were in a
    * local tx. Note this behaviour differs from messages sent
    * This is so we can support transacted delivery of messags in an MDB as mentioned
    *
    * However to accomodate TCK tests, when Sessions/Consumers are being used without transaction
    * enlist, we will process those cases as NonTransactional, AutoAck.
    *
    * See http://www.jboss.com/index.html?module=bb&op=viewtopic&t=98577&postdays=0&postorder=asc&start=0
    * http://jira.jboss.com/jira/browse/JBMESSAGING-410
    * http://jira.jboss.com/jira/browse/JBMESSAGING-721
    * http://jira.jboss.org/jira/browse/JBMESSAGING-946
    *
    * For transactional delivery the receipt of the message should be in a transaction but by the time
    * the mdb container is invoked the message has already been received it is too late - the message
    * has already been received and passed on (see page 199 (chapter 5 JMS and Transactions, section "Application Server Integration"
    * of Mark Little's book Java Transaction processing
    * for a discussion of how different app serves deal with this)
    * The way jboss messaging (and jboss mq) deals with this is to convert any work done
    * prior to when the xasession is enlisted in the tx, into work done in the xa tx.
    *
    *      NOTE: To accomodate TCK tests where Session/Consumers are being used without transaction enlisting
    *            we are processing those cases as nonTransactional/AutoACK, however if the session is being used
    *            to process MDBs we will consider the LocalTransaction convertion and process those as the comment above
    *            This was done as per: http://jira.jboss.org/jira/browse/JBMESSAGING-946
    *
    */
   public void testReceiveNoGlobalTransaction() throws Exception
   {
      if (ServerManagement.isRemote())
      {
         log.info("************************************************************************************************************************");
         log.info("testReceiveNoGlobalTransaction is being ignored on remote runs, as this requires access to the JCA layer on java:/");
         log.info("************************************************************************************************************************");
         return;
      }

      try
      {
         ServerManagement.deployQueue("MyQueue2");

         // send a message to the queue

         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");
         Queue queue = (Queue)initialContext.lookup("queue/MyQueue2");
         Connection conn = cf.createConnection();
         Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = s.createProducer(queue);
         p.setDeliveryMode(DeliveryMode.PERSISTENT);
         Message m = s.createTextMessage("one");
         p.send(m);
         conn.close();

         // make sure there's no active JTA transaction

         Transaction suspended = TransactionManagerLocator.getInstance().locate().suspend();

         try
         {

            ObjectName queueMBean = new ObjectName("jboss.messaging.destination:service=Queue,name=MyQueue2");
            Integer count = (Integer)ServerManagement.getAttribute(queueMBean, "MessageCount");
            assertEquals(1, count.intValue());

            // using a JCA wrapper without a transaction

            ConnectionFactory mcf =
                  (ConnectionFactory)initialContext.lookup("java:/JCAConnectionFactory");
            conn = mcf.createConnection();
            conn.start();

            // no active JTA transaction here

            s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageConsumer c = s.createConsumer(queue);

            // the messge should be store unacked in the local session
            TextMessage rm = (TextMessage)c.receive(1000);

            assertEquals("one", rm.getText());

            conn.close();

            // messages should be consumed
            count = (Integer)ServerManagement.getAttribute(queueMBean, "MessageCount");
            assertEquals(0, count.intValue());
         }
         finally
         {

            if (suspended != null)
            {
               TransactionManagerLocator.getInstance().locate().resume(suspended);
            }
         }
      }
      finally
      {
         ServerManagement.undeployQueue("MyQueue2");
      }
   }


   //See http://jira.jboss.com/jira/browse/JBMESSAGING-638
   public void testResourceManagerMemoryLeakOnCommit() throws Exception
   {

      XAConnection xaConn = null;

      try
      {
         xaConn = cf.createXAConnection();

         JBossConnection jbConn = (JBossConnection)xaConn;

         ClientConnectionDelegate del = (ClientConnectionDelegate)jbConn.getDelegate();

         ConnectionState state = (ConnectionState)del.getState();

         ResourceManager rm = state.getResourceManager();

         XASession xaSession = xaConn.createXASession();

         xaConn.start();

         XAResource res = xaSession.getXAResource();

         XAResource dummy = new DummyXAResource();

         for (int i = 0; i < 100; i++)
         {

            tm.begin();

            Transaction tx = tm.getTransaction();

            tx.enlistResource(res);

            tx.enlistResource(dummy);

            assertEquals(1, rm.size());

            tx.delistResource(res, XAResource.TMSUCCESS);

            tx.delistResource(dummy, XAResource.TMSUCCESS);

            tm.commit();
         }

         assertEquals(1, rm.size());

         xaConn.close();

         xaConn = null;

         assertEquals(0, rm.size());

      }
      finally
      {
         if (xaConn != null)
         {
            xaConn.close();
         }
      }
   }

   //See http://jira.jboss.com/jira/browse/JBMESSAGING-638
   public void testResourceManagerMemoryLeakOnRollback() throws Exception
   {
      XAConnection xaConn = null;

      try
      {
         xaConn = cf.createXAConnection();

         JBossConnection jbConn = (JBossConnection)xaConn;

         ClientConnectionDelegate del = (ClientConnectionDelegate)jbConn.getDelegate();

         ConnectionState state = (ConnectionState)del.getState();

         ResourceManager rm = state.getResourceManager();

         XASession xaSession = xaConn.createXASession();

         xaConn.start();

         XAResource res = xaSession.getXAResource();

         XAResource dummy = new DummyXAResource();

         for (int i = 0; i < 100; i++)
         {
            tm.begin();

            Transaction tx = tm.getTransaction();

            tx.enlistResource(res);

            tx.enlistResource(dummy);

            assertEquals(1, rm.size());

            tx.delistResource(res, XAResource.TMSUCCESS);

            tx.delistResource(dummy, XAResource.TMSUCCESS);

            tm.rollback();
         }

         assertEquals(1, rm.size());

         xaConn.close();

         xaConn = null;

         assertEquals(0, rm.size());

      }
      finally
      {
         if (xaConn != null)
         {
            xaConn.close();
         }
      }
   }


   /**
   * //http://jira.jboss.com/jira/browse/JBMESSAGING-721
   *      NOTE: To accomodate TCK tests where Session/Consumers are being used without transaction enlisting
   *            we are processing those cases as nonTransactional/AutoACK, however if the session is being used
   *            to process MDBs we will consider the LocalTransaction convertion and process those as the comment above
   *            This was done as per: http://jira.jboss.org/jira/browse/JBMESSAGING-946
   *
   */

   public void testConvertFromLocalTx() throws Exception
   {
      Connection conn = null;

      XAConnection xaConn = null;


      try
      {

         //First send some messages to a queue

         ObjectName queueMBean = new ObjectName("jboss.messaging.destination:service=Queue,name=Queue");
         Integer count = (Integer) ServerManagement.getAttribute(queueMBean, "MessageCount");
         assertEquals(0, count.intValue());

         conn = cf.createConnection();

         Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = sessSend.createProducer(queue);

         TextMessage tm1 = sessSend.createTextMessage("message1");

         TextMessage tm2 = sessSend.createTextMessage("message2");

         prod.send(tm1);

         prod.send(tm2);

         count = (Integer) ServerManagement.getAttribute(queueMBean, "MessageCount");
         assertEquals(2, count.intValue());

         xaConn = cf.createXAConnection();

         XASession xaSession = xaConn.createXASession();

         xaConn.start();

         DummyListener listener = new DummyListener();

         xaSession.setMessageListener(listener);

         ServerSessionPool pool = new MockServerSessionPool(xaSession);

         xaConn.createConnectionConsumer(queue, null, pool, 1);

         Thread.sleep(1000);

         assertEquals(2, listener.messages.size());

         assertEquals("message1", ((TextMessage)(listener.messages.get(0))).getText());
         assertEquals("message2", ((TextMessage)(listener.messages.get(1))).getText());

         count = (Integer) ServerManagement.getAttribute(queueMBean, "MessageCount");
         assertEquals(2, count.intValue());

         listener.messages.clear();

         //Now we enlist the session in an xa transaction

         XAResource res = xaSession.getXAResource();

         tm.begin();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);

         //This should cause the work done previously to be converted into work done in the xa transaction
         //this is what an MDB does
         //There is a difficulty in transactional delivery with an MDB.
         //The message is received from the destination and then sent to the mdb container so
         //it can call onMessage.
         //For transactional delivery the receipt of the message should be in a transaction but by the time
         //the mdb container is invoked the message has already been received it is too late - the message
         //has already been received and passed on (see page 199 (chapter 5 JMS and Transactions, section "Application Server Integration"
         //of Mark Little's book Java Transaction processing
         //for a discussion of how different app serves deal with this)
         //The way jboss messaging (and jboss mq) deals with this is to convert any work done
         //prior to when the xasession is enlisted in the tx, into work done in the xa tx

         tx.delistResource(res, XAResource.TMSUCCESS);

         //Now rollback the tx - this should cause redelivery of the two messages
         tm.rollback();

         count = (Integer) ServerManagement.getAttribute(queueMBean, "MessageCount");
         assertEquals(2, count.intValue());

         Thread.sleep(1000);

         assertEquals(2, listener.messages.size());

         listener.messages.clear();

         tm.begin();

         tx = tm.getTransaction();
         tx.enlistResource(res);

         tm.commit();

         Thread.sleep(1000);

         assertEquals(0, listener.messages.size());

         count = (Integer) ServerManagement.getAttribute(queueMBean, "MessageCount");
         assertEquals(0, count.intValue());

         assertNull(tm.getTransaction());


      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }

         if (xaConn != null)
         {
            xaConn.close();
         }

        /* if (suspended != null)
         {
            tm.resume(suspended);
         }*/
      }
   }

   //http://jira.jboss.com/jira/browse/JBMESSAGING-721
   // Note: The behavior of this test was changed after http://jira.jboss.com/jira/browse/JBMESSAGING-946
   // When you have a XASession without a transaction enlisted we will behave the same way as non transactedSession, AutoAck
   public void testTransactionIdSetAfterCommit() throws Exception
   {
      Connection conn = null;

      XAConnection xaConn = null;

      try
      {

         //First send some messages to a queue

         conn = cf.createConnection();

         Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = sessSend.createProducer(queue);

         TextMessage tm1 = sessSend.createTextMessage("message1");

         TextMessage tm2 = sessSend.createTextMessage("message2");

         prod.send(tm1);

         prod.send(tm2);


         xaConn = cf.createXAConnection();

         XASession xaSession = xaConn.createXASession();

         xaConn.start();

         MessageConsumer cons = xaSession.createConsumer(queue);

         //Now we enlist the session in an xa transaction

         XAResource res = xaSession.getXAResource();

         tm.begin();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);

         tx.delistResource(res, XAResource.TMSUCCESS);

         //Then we do a commit
         tm.commit();

         // I have changed where this begin was originally set
         // as when you don't have a resource enlisted, XASessions will act as
         // non transacted + AutoAck

         //And enlist again - this should convert the work done in the local tx
         //into the global branch

         tx = tm.getTransaction();

         tm.begin();

         tx = tm.getTransaction();
         tx.enlistResource(res);

         //Then we receive the messages outside the tx

         TextMessage rm1 = (TextMessage)cons.receive(1000);

         assertNotNull(rm1);

         assertEquals("message1", rm1.getText());

         TextMessage rm2 = (TextMessage)cons.receive(1000);

         assertNotNull(rm2);

         assertEquals("message2", rm2.getText());

         Message rm3 = cons.receive(1000);

         assertNull(rm3);

         tx.delistResource(res, XAResource.TMSUCCESS);

         //Now rollback the tx - this should cause redelivery of the two messages
         tx.rollback();

         rm1 = (TextMessage)cons.receive(1000);

         assertNotNull(rm1);

         assertEquals("message1", rm1.getText());

         rm2 = (TextMessage)cons.receive(1000);

         assertNotNull(rm2);

         assertEquals("message2", rm2.getText());

         rm3 = cons.receive(1000);

         assertNull(rm3);
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }

         if (xaConn != null)
         {
            xaConn.close();
         }
      }

   }

   //http://jira.jboss.com/jira/browse/JBMESSAGING-721
   public void testTransactionIdSetAfterRollback() throws Exception
   {
      Connection conn = null;

      XAConnection xaConn = null;

      try
      {

         //First send some messages to a queue

         conn = cf.createConnection();

         Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = sessSend.createProducer(queue);

         TextMessage tm1 = sessSend.createTextMessage("message1");

         TextMessage tm2 = sessSend.createTextMessage("message2");

         prod.send(tm1);

         prod.send(tm2);


         xaConn = cf.createXAConnection();

         XASession xaSession = xaConn.createXASession();

         xaConn.start();

         MessageConsumer cons = xaSession.createConsumer(queue);

         //Now we enlist the session in an xa transaction

         XAResource res = xaSession.getXAResource();

         tm.begin();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.delistResource(res, XAResource.TMSUCCESS);

         //Then we do a rollback
         tm.rollback();


         tm.begin();

         //And enlist again - the work should then be converted into the global tx branch


         // I have changed where this begin was originally set
         // as when you don't have a resource enlisted, XASessions will act as
         // non transacted + AutoAck

         tx = tm.getTransaction();

         tx.enlistResource(res);

         //Then we receive the messages outside the global tx

         TextMessage rm1 = (TextMessage)cons.receive(1000);

         assertNotNull(rm1);

         assertEquals("message1", rm1.getText());

         TextMessage rm2 = (TextMessage)cons.receive(1000);

         assertNotNull(rm2);

         assertEquals("message2", rm2.getText());

         Message rm3 = cons.receive(1000);

         assertNull(rm3);
         tx.delistResource(res, XAResource.TMSUCCESS);

         //Now rollback the tx - this should cause redelivery of the two messages
         tx.rollback();

         rm1 = (TextMessage)cons.receive(1000);

         assertNotNull(rm1);

         assertEquals("message1", rm1.getText());

         rm2 = (TextMessage)cons.receive(1000);

         assertNotNull(rm2);

         assertEquals("message2", rm2.getText());

         rm3 = cons.receive(1000);

         assertNull(rm3);
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }

         if (xaConn != null)
         {
            xaConn.close();
         }
      }

   }

   // See http://jira.jboss.org/jira/browse/JBMESSAGING-825
   // Need to test that ids with trailing zeros are dealt with properly - sybase has the habit
   // of truncating trailing zeros in varbinary columns
   public void testXidsWithTrailingZeros() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         return;
      }

      XAConnection conn1 = null;

      try
      {
         conn1 = cf.createXAConnection();

         XASession sess1 = conn1.createXASession();

         XAResource res1 = sess1.getXAResource();

         byte[] branchQualifier = new byte[] { 1, 2, 3, 4, 5, 6, 0, 0, 0, 0 };

         byte[] globalTxId = new byte[] { 6, 5, 4, 3, 2, 1, 0, 0, 0, 0 };

         Xid trailing = new MessagingXid(branchQualifier, 12435, globalTxId);

         res1.start(trailing, XAResource.TMNOFLAGS);

         MessageProducer prod1 = sess1.createProducer(queue);

         TextMessage tm1 = sess1.createTextMessage("testing1");

         prod1.send(tm1);

         res1.end(trailing, XAResource.TMSUCCESS);


         res1.prepare(trailing);


         //Now "crash" the server

         ServerManagement.stopServerPeer();

         ServerManagement.startServerPeer();

         ServerManagement.deployQueue("Queue");


         XAResource res = cf.createXAConnection().createXASession().getXAResource();

         Xid[] xids = res.recover(XAResource.TMSTARTRSCAN);
         assertEquals(1, xids.length);

         Xid[] xids2 = res.recover(XAResource.TMENDRSCAN);
         assertEquals(0, xids2.length);

         Xid trailing2 = xids[0];

         assertTrue(trailing.getFormatId() == trailing2.getFormatId());

         assertEqualByteArrays(trailing.getGlobalTransactionId(), trailing2.getGlobalTransactionId());

         assertEqualByteArrays(trailing.getBranchQualifier(), trailing2.getBranchQualifier());

         res.commit(trailing, false);

         if (checkNoMessageData())
         {
            fail("Data remains in database");
         }
      }
      finally
      {
         if (conn1 != null)
         {
            try
            {
               conn1.close();
            }
            catch (Exception e)
            {
               //Ignore
            }
         }
      }
   }

   public void test2PCSendCommit1PCOptimization() throws Exception
   {
      //Since both resources have some RM, TM will probably use 1PC optimization

      XAConnection conn = null;
      Connection conn2 = null;

      try
      {
         conn = cf.createXAConnection();

         tm.begin();

         XASession sess = conn.createXASession();
         XAResource res = sess.getXAResource();

         XAResource res2 = new DummyXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.enlistResource(res2);

         MessageProducer prod = sess.createProducer(queue);
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         Message m = sess.createTextMessage("XATest1");
         prod.send(queue, m);
         m = sess.createTextMessage("XATest2");
         prod.send(queue, m);

         tx.delistResource(res, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         tm.commit();

         conn2 = cf.createConnection();
         conn2.start();
         Session sessReceiver = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sessReceiver.createConsumer(queue);
         TextMessage m2 = (TextMessage)cons.receive(1000);
         assertNotNull(m2);
         assertEquals("XATest1", m2.getText());
         m2 = (TextMessage)cons.receive(1000);
         assertNotNull(m2);
         assertEquals("XATest2", m2.getText());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }

   }



   public void test2PCSendCommit() throws Exception
   {
      XAConnection conn = null;
      Connection conn2 = null;

      try
      {

         conn = cf.createXAConnection();

         tm.begin();

         XASession sess = conn.createXASession();

         MessagingXAResource res = (MessagingXAResource)sess.getXAResource();
         XAResource res2 = new DummyXAResource();

         //To prevent 1PC optimization being used
         res.setPreventJoining(true);

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.enlistResource(res2);

         MessageProducer prod = sess.createProducer(queue);
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         Message m = sess.createTextMessage("XATest1");
         prod.send(queue, m);
         m = sess.createTextMessage("XATest2");
         prod.send(queue, m);

         tx.delistResource(res, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         tm.commit();

         conn2 = cf.createConnection();
         conn2.start();
         Session sessReceiver = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sessReceiver.createConsumer(queue);
         TextMessage m2 = (TextMessage)cons.receive(MAX_TIMEOUT);
         assertNotNull(m2);
         assertEquals("XATest1", m2.getText());
         m2 = (TextMessage)cons.receive(MAX_TIMEOUT);
         assertNotNull(m2);
         assertEquals("XATest2", m2.getText());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }

   }


   public void test2PCSendRollback1PCOptimization() throws Exception
   {
      //Since both resources have some RM, TM will probably use 1PC optimization

      XAConnection conn = null;
      Connection conn2 = null;
      try
      {
         conn = cf.createXAConnection();

         tm.begin();

         XASession sess = conn.createXASession();
         XAResource res = sess.getXAResource();

         XAResource res2 = new DummyXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.enlistResource(res2);

         MessageProducer prod = sess.createProducer(queue);
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         Message m = sess.createTextMessage("XATest1");
         prod.send(queue, m);
         m = sess.createTextMessage("XATest2");
         prod.send(queue, m);

         tx.delistResource(res, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         tm.rollback();

         conn2 = cf.createConnection();
         conn2.start();
         Session sessReceiver = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sessReceiver.createConsumer(queue);
         Message m2 = cons.receive(MIN_TIMEOUT);
         assertNull(m2);

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }
   }


   public void test2PCSendFailOnPrepare() throws Exception
   {
      XAConnection conn = null;
      Connection conn2 = null;
      try
      {
         conn = cf.createXAConnection();

         tm.begin();

         XASession sess = conn.createXASession();
         MessagingXAResource res = (MessagingXAResource)sess.getXAResource();

         //prevent 1Pc optimisation
         res.setPreventJoining(true);

         XAResource res2 = new DummyXAResource(true);
         XAResource res3 = new DummyXAResource();
         XAResource res4 = new DummyXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.enlistResource(res2);
         tx.enlistResource(res3);
         tx.enlistResource(res4);

         MessageProducer prod = sess.createProducer(queue);
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         Message m = sess.createTextMessage("XATest1");
         prod.send(queue, m);
         m = sess.createTextMessage("XATest2");
         prod.send(queue, m);

         tx.delistResource(res, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);
         tx.delistResource(res3, XAResource.TMSUCCESS);
         tx.delistResource(res4, XAResource.TMSUCCESS);

         try
         {
            tm.commit();

            fail("should not get here");
         }
         catch (Exception e)
         {
            //We should expect this
         }

         conn2 = cf.createConnection();
         conn2.start();
         Session sessReceiver = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sessReceiver.createConsumer(queue);
         Message m2 = cons.receive(MIN_TIMEOUT);
         assertNull(m2);

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }
   }

   public void test2PCSendRollback() throws Exception
   {
      XAConnection conn = null;
      Connection conn2 = null;
      try
      {
         conn = cf.createXAConnection();

         tm.begin();

         XASession sess = conn.createXASession();
         MessagingXAResource res = (MessagingXAResource)sess.getXAResource();

         //prevent 1Pc optimisation
         res.setPreventJoining(true);

         XAResource res2 = new DummyXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.enlistResource(res2);

         MessageProducer prod = sess.createProducer(queue);
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         Message m = sess.createTextMessage("XATest1");
         prod.send(queue, m);
         m = sess.createTextMessage("XATest2");
         prod.send(queue, m);

         tx.delistResource(res, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         tm.rollback();

         conn2 = cf.createConnection();
         conn2.start();
         Session sessReceiver = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sessReceiver.createConsumer(queue);
         Message m2 = cons.receive(MIN_TIMEOUT);
         assertNull(m2);

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }
   }

   public void test2PCReceiveCommit1PCOptimization() throws Exception
   {
      //Since both resources have some RM, TM will probably use 1PC optimization

      XAConnection conn = null;
      Connection conn2 = null;

      try
      {
         conn2 = cf.createConnection();
         conn2.start();
         Session sessProducer = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod  = sessProducer.createProducer(queue);
         Message m = sessProducer.createTextMessage("XATest1");
         prod.send(m);
         m = sessProducer.createTextMessage("XATest2");
         prod.send(m);

         conn = cf.createXAConnection();
         conn.start();

         tm.begin();

         XASession sess = conn.createXASession();
         XAResource res = sess.getXAResource();

         XAResource res2 = new DummyXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.enlistResource(res2);

         MessageConsumer cons = sess.createConsumer(queue);


         TextMessage m2 = (TextMessage)cons.receive(MAX_TIMEOUT);

         assertNotNull(m2);
         assertEquals("XATest1", m2.getText());

         m2 = (TextMessage)cons.receive(MAX_TIMEOUT);

         assertNotNull(m2);
         assertEquals("XATest2", m2.getText());

         tx.delistResource(res, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         tm.commit();

         //New tx
         tm.begin();
         tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.enlistResource(res2);

         Message m3 = cons.receive(MIN_TIMEOUT);

         assertNull(m3);

         tx.delistResource(res, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         tm.commit();


      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }

   }

   public void test2PCReceiveCommit() throws Exception
   {
      XAConnection conn = null;
      Connection conn2 = null;

      try
      {
         conn2 = cf.createConnection();
         conn2.start();
         Session sessProducer = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod  = sessProducer.createProducer(queue);
         Message m = sessProducer.createTextMessage("XATest1");
         prod.send(m);
         m = sessProducer.createTextMessage("XATest2");
         prod.send(m);

         conn = cf.createXAConnection();
         conn.start();

         tm.begin();

         XASession sess = conn.createXASession();
         MessagingXAResource res = (MessagingXAResource)sess.getXAResource();
         res.setPreventJoining(true);

         XAResource res2 = new DummyXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.enlistResource(res2);

         MessageConsumer cons = sess.createConsumer(queue);


         TextMessage m2 = (TextMessage)cons.receive(MAX_TIMEOUT);

         assertNotNull(m2);
         assertEquals("XATest1", m2.getText());

         m2 = (TextMessage)cons.receive(MAX_TIMEOUT);

         assertNotNull(m2);
         assertEquals("XATest2", m2.getText());

         tx.delistResource(res, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         tm.commit();

         //New tx
         tm.begin();
         tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.enlistResource(res2);

         Message m3 = cons.receive(MIN_TIMEOUT);

         assertNull(m3);

         tx.delistResource(res, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         tm.commit();


      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }

   }

   public void test2PCReceiveRollback1PCOptimization() throws Exception
   {
      //Since both resources have some RM, TM will probably use 1PC optimization

      XAConnection conn = null;
      Connection conn2 = null;

      try
      {
         conn2 = cf.createConnection();
         Session sessProducer = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod  = sessProducer.createProducer(queue);
         Message m = sessProducer.createTextMessage("XATest1");
         prod.send(m);

         m = sessProducer.createTextMessage("XATest2");
         prod.send(m);


         conn = cf.createXAConnection();
         conn.start();

         tm.begin();

         XASession sess = conn.createXASession();
         XAResource res = sess.getXAResource();

         XAResource res2 = new DummyXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.enlistResource(res2);

         MessageConsumer cons = sess.createConsumer(queue);


         TextMessage m2 = (TextMessage)cons.receive(MAX_TIMEOUT);
         assertNotNull(m2);
         assertEquals("XATest1", m2.getText());
         m2 = (TextMessage)cons.receive(MAX_TIMEOUT);
         assertNotNull(m2);
         assertEquals("XATest2", m2.getText());

         tx.delistResource(res, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         tm.rollback();

         //Message should be redelivered

         //New tx
         tm.begin();
         tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.enlistResource(res2);

         TextMessage m3 = (TextMessage)cons.receive(MAX_TIMEOUT);
         assertNotNull(m3);
         assertEquals("XATest1", m3.getText());
         m3 = (TextMessage)cons.receive(MAX_TIMEOUT);
         assertNotNull(m3);
         assertEquals("XATest2", m3.getText());

         assertTrue(m3.getJMSRedelivered());

         tx.delistResource(res, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         tm.commit();

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }

   }

   public void test2PCReceiveRollback() throws Exception
   {
      XAConnection conn = null;
      Connection conn2 = null;

      try
      {
         conn2 = cf.createConnection();
         Session sessProducer = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod  = sessProducer.createProducer(queue);
         Message m = sessProducer.createTextMessage("XATest1");
         prod.send(m);

         m = sessProducer.createTextMessage("XATest2");
         prod.send(m);


         conn = cf.createXAConnection();
         conn.start();

         tm.begin();

         XASession sess = conn.createXASession();
         MessagingXAResource res = (MessagingXAResource)sess.getXAResource();
         res.setPreventJoining(true);

         XAResource res2 = new DummyXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.enlistResource(res2);

         MessageConsumer cons = sess.createConsumer(queue);


         TextMessage m2 = (TextMessage)cons.receive(MAX_TIMEOUT);
         assertNotNull(m2);
         assertEquals("XATest1", m2.getText());
         m2 = (TextMessage)cons.receive(MAX_TIMEOUT);
         assertNotNull(m2);
         assertEquals("XATest2", m2.getText());

         tx.delistResource(res, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         tm.rollback();

         //Message should be redelivered

         //New tx
         tm.begin();
         tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.enlistResource(res2);

         TextMessage m3 = (TextMessage)cons.receive(MAX_TIMEOUT);
         assertNotNull(m3);
         assertEquals("XATest1", m3.getText());
         m3 = (TextMessage)cons.receive(MAX_TIMEOUT);
         assertNotNull(m3);
         assertEquals("XATest2", m3.getText());

         assertTrue(m3.getJMSRedelivered());

         tx.delistResource(res, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         tm.commit();

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }

   }


   public void test1PCSendCommit() throws Exception
   {
      XAConnection conn = null;
      Connection conn2 = null;

      try
      {

         conn = cf.createXAConnection();

         tm.begin();

         XASession sess = conn.createXASession();
         XAResource res = sess.getXAResource();


         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);


         MessageProducer prod = sess.createProducer(queue);
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         Message m = sess.createTextMessage("XATest1");
         prod.send(queue, m);
         m = sess.createTextMessage("XATest2");
         prod.send(queue, m);

         tx.delistResource(res, XAResource.TMSUCCESS);

         tm.commit();

         conn2 = cf.createConnection();
         conn2.start();
         Session sessReceiver = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sessReceiver.createConsumer(queue);
         TextMessage m2 = (TextMessage)cons.receive(MAX_TIMEOUT);
         assertNotNull(m2);
         assertEquals("XATest1", m2.getText());
         m2 = (TextMessage)cons.receive(MAX_TIMEOUT);
         assertNotNull(m2);
         assertEquals("XATest2", m2.getText());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }

   }


   public void test1PCSendRollback() throws Exception
   {
      XAConnection conn = null;
      Connection conn2 = null;
      try
      {
         conn = cf.createXAConnection();

         tm.begin();

         XASession sess = conn.createXASession();
         XAResource res = sess.getXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);

         MessageProducer prod = sess.createProducer(queue);
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         Message m = sess.createTextMessage("XATest1");
         prod.send(queue, m);
         m = sess.createTextMessage("XATest2");
         prod.send(queue, m);

         tx.delistResource(res, XAResource.TMSUCCESS);

         tm.rollback();

         conn2 = cf.createConnection();
         conn2.start();
         Session sessReceiver = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sessReceiver.createConsumer(queue);
         Message m2 = cons.receive(MIN_TIMEOUT);
         assertNull(m2);

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }
   }

   public void test1PCReceiveCommit() throws Exception
   {
      XAConnection conn = null;
      Connection conn2 = null;

      try
      {
         conn2 = cf.createConnection();
         conn2.start();
         Session sessProducer = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod  = sessProducer.createProducer(queue);
         Message m = sessProducer.createTextMessage("XATest1");
         prod.send(m);
         m = sessProducer.createTextMessage("XATest2");
         prod.send(m);

         conn = cf.createXAConnection();
         conn.start();

         tm.begin();

         XASession sess = conn.createXASession();
         XAResource res = sess.getXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);

         MessageConsumer cons = sess.createConsumer(queue);


         TextMessage m2 = (TextMessage)cons.receive(MAX_TIMEOUT);

         assertNotNull(m2);
         assertEquals("XATest1", m2.getText());
         m2 = (TextMessage)cons.receive(MAX_TIMEOUT);

         assertNotNull(m2);
         assertEquals("XATest2", m2.getText());

         tx.delistResource(res, XAResource.TMSUCCESS);

         tm.commit();

         //New tx
         tm.begin();
         tx = tm.getTransaction();
         tx.enlistResource(res);

         Message m3 = cons.receive(MIN_TIMEOUT);

         assertNull(m3);

         tx.delistResource(res, XAResource.TMSUCCESS);

         tm.commit();


      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }

   }

   public void test1PCReceiveRollback() throws Exception
   {
      XAConnection conn = null;
      Connection conn2 = null;

      try
      {
         conn2 = cf.createConnection();
         Session sessProducer = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod  = sessProducer.createProducer(queue);
         Message m = sessProducer.createTextMessage("XATest1");
         prod.send(m);
         m = sessProducer.createTextMessage("XATest2");
         prod.send(m);


         conn = cf.createXAConnection();
         conn.start();

         tm.begin();

         XASession sess = conn.createXASession();
         XAResource res = sess.getXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);

         MessageConsumer cons = sess.createConsumer(queue);


         TextMessage m2 = (TextMessage)cons.receive(MAX_TIMEOUT);

         assertNotNull(m2);
         assertEquals("XATest1", m2.getText());

         m2 = (TextMessage)cons.receive(MAX_TIMEOUT);

         assertNotNull(m2);
         assertEquals("XATest2", m2.getText());

         tx.delistResource(res, XAResource.TMSUCCESS);

         tm.rollback();

         //Message should be redelivered

         //New tx
         tm.begin();
         tx = tm.getTransaction();
         tx.enlistResource(res);

         TextMessage m3 = (TextMessage)cons.receive(MAX_TIMEOUT);

         assertNotNull(m3);
         assertEquals("XATest1", m3.getText());

         m3 = (TextMessage)cons.receive(MAX_TIMEOUT);

         assertNotNull(m3);
         assertEquals("XATest2", m3.getText());

         assertTrue(m3.getJMSRedelivered());

         tx.delistResource(res, XAResource.TMSUCCESS);

         tm.commit();

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }

   }

   public void testMultipleSessionsOneTxCommitAcknowledge1PCOptimization() throws Exception
   {
      XAConnection conn = null;
      Connection conn2 = null;

      //Since both resources have some RM, TM will probably use 1PC optimization

      try
      {
         //First send 2 messages
         conn2 = cf.createConnection();
         Session sessProducer = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod  = sessProducer.createProducer(queue);
         Message m = sessProducer.createTextMessage("jellyfish1");
         prod.send(m);
         m = sessProducer.createTextMessage("jellyfish2");
         prod.send(m);


         conn = cf.createXAConnection();
         conn.start();

         tm.begin();

         //Create 2 sessions and enlist them
         XASession sess1 = conn.createXASession();
         XAResource res1 = sess1.getXAResource();
         XASession sess2 = conn.createXASession();
         XAResource res2 = sess2.getXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res1);
         tx.enlistResource(res2);

         //Receive the messages, one on each consumer
         MessageConsumer cons1 = sess1.createConsumer(queue);
         TextMessage r1 = (TextMessage)cons1.receive(MAX_TIMEOUT);

         assertNotNull(r1);
         assertEquals("jellyfish1", r1.getText());

         cons1.close();

         MessageConsumer cons2 = sess2.createConsumer(queue);
         TextMessage r2 = (TextMessage)cons2.receive(MAX_TIMEOUT);

         assertNotNull(r2);
         assertEquals("jellyfish2", r2.getText());

         tx.delistResource(res1, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         //commit
         tm.commit();

         Session sess = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sess.createConsumer(queue);
         conn2.start();

         TextMessage r3 = (TextMessage)cons.receive(MIN_TIMEOUT);
         assertNull(r3);

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }

   }

   public void testMultipleSessionsOneTxCommitAcknowledge() throws Exception
   {
      XAConnection conn = null;
      Connection conn2 = null;

      try
      {
         //First send 2 messages
         conn2 = cf.createConnection();
         Session sessProducer = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod  = sessProducer.createProducer(queue);
         Message m = sessProducer.createTextMessage("jellyfish1");
         prod.send(m);
         m = sessProducer.createTextMessage("jellyfish2");
         prod.send(m);


         conn = cf.createXAConnection();
         conn.start();

         tm.begin();

         //Create 2 sessions and enlist them
         XASession sess1 = conn.createXASession();
         MessagingXAResource res1 = (MessagingXAResource)sess1.getXAResource();
         XASession sess2 = conn.createXASession();
         MessagingXAResource res2 = (MessagingXAResource)sess2.getXAResource();
         res1.setPreventJoining(true);
         res2.setPreventJoining(true);

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res1);
         tx.enlistResource(res2);

         //Receive the messages, one on each consumer
         MessageConsumer cons1 = sess1.createConsumer(queue);
         TextMessage r1 = (TextMessage)cons1.receive(MAX_TIMEOUT);

         assertNotNull(r1);
         assertEquals("jellyfish1", r1.getText());

         cons1.close();

         MessageConsumer cons2 = sess2.createConsumer(queue);
         TextMessage r2 = (TextMessage)cons2.receive(MAX_TIMEOUT);

         assertNotNull(r2);
         assertEquals("jellyfish2", r2.getText());

         tx.delistResource(res1, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         //commit
         tm.commit();

         Session sess = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sess.createConsumer(queue);
         conn2.start();

         TextMessage r3 = (TextMessage)cons.receive(MIN_TIMEOUT);
         assertNull(r3);

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }

   }


   public void testMultipleSessionsOneTxRollbackAcknowledge1PCOptimization() throws Exception
   {
      XAConnection conn = null;
      Connection conn2 = null;

      //Since both resources have some RM, TM will probably use 1PC optimization

      try
      {
         //First send 2 messages
         conn2 = cf.createConnection();
         Session sessProducer = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod  = sessProducer.createProducer(queue);
         Message m = sessProducer.createTextMessage("jellyfish1");
         prod.send(m);
         m = sessProducer.createTextMessage("jellyfish2");
         prod.send(m);
         m = sessProducer.createTextMessage("jellyfish3");
         prod.send(m);
         m = sessProducer.createTextMessage("jellyfish4");
         prod.send(m);


         conn = cf.createXAConnection();
         conn.start();

         tm.begin();

         //Create 2 sessions and enlist them
         XASession sess1 = conn.createXASession();
         MessagingXAResource res1 = (MessagingXAResource)sess1.getXAResource();
         XASession sess2 = conn.createXASession();
         MessagingXAResource res2 = (MessagingXAResource)sess2.getXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res1);
         tx.enlistResource(res2);

         //Receive the messages, two on each consumer
         MessageConsumer cons1 = sess1.createConsumer(queue);
         TextMessage r1 = (TextMessage)cons1.receive(MAX_TIMEOUT);

         assertNotNull(r1);
         assertEquals("jellyfish1", r1.getText());

         r1 = (TextMessage)cons1.receive(MAX_TIMEOUT);

         assertNotNull(r1);
         assertEquals("jellyfish2", r1.getText());

         cons1.close();

         MessageConsumer cons2 = sess2.createConsumer(queue);
         TextMessage r2 = (TextMessage)cons2.receive(MAX_TIMEOUT);

         assertNotNull(r2);
         assertEquals("jellyfish3", r2.getText());

         r2 = (TextMessage)cons2.receive(MAX_TIMEOUT);

         assertNotNull(r2);
         assertEquals("jellyfish4", r2.getText());

         cons2.close();

         //rollback

         tx.delistResource(res1, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         tm.rollback();

         //Rollback causes cancel which is asynch
         Thread.sleep(1000);

         //We cannot assume anything about the order in which the transaction manager rollsback
         //the sessions - this is implementation dependent

         Session sess = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sess.createConsumer(queue);
         conn2.start();

         TextMessage r = (TextMessage)cons.receive(MAX_TIMEOUT);
         assertNotNull(r);

         boolean session1First = false;

         if (r.getText().equals("jellyfish1"))
         {
            session1First = true;
         }
         else if (r.getText().equals("jellyfish3"))
         {
            session1First = false;
         }
         else
         {
            fail("Unexpected message");
         }

         if (session1First)
         {
            r = (TextMessage)cons.receive(MAX_TIMEOUT);

            assertNotNull(r);

            assertEquals("jellyfish2", r.getText());

            r = (TextMessage)cons.receive(MAX_TIMEOUT);

            assertNotNull(r);

            assertEquals("jellyfish3", r.getText());

            r = (TextMessage)cons.receive(MAX_TIMEOUT);

            assertNotNull(r);

            assertEquals("jellyfish4", r.getText());


         }
         else
         {
            r = (TextMessage)cons.receive(MAX_TIMEOUT);

            assertNotNull(r);

            assertEquals("jellyfish4", r.getText());

            r = (TextMessage)cons.receive(MAX_TIMEOUT);

            assertNotNull(r);

            assertEquals("jellyfish1", r.getText());

            r = (TextMessage)cons.receive(MAX_TIMEOUT);

            assertNotNull(r);

            assertEquals("jellyfish2", r.getText());
         }

         r = (TextMessage)cons.receive(MIN_TIMEOUT);

         assertNull(r);

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }

   }

   public void testMultipleSessionsOneTxRollbackAcknowledge() throws Exception
   {
      XAConnection conn = null;
      Connection conn2 = null;

      try
      {
         //First send 2 messages
         conn2 = cf.createConnection();
         Session sessProducer = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod  = sessProducer.createProducer(queue);
         Message m = sessProducer.createTextMessage("jellyfish1");
         prod.send(m);
         m = sessProducer.createTextMessage("jellyfish2");
         prod.send(m);
         m = sessProducer.createTextMessage("jellyfish3");
         prod.send(m);
         m = sessProducer.createTextMessage("jellyfish4");
         prod.send(m);


         conn = cf.createXAConnection();
         conn.start();

         tm.begin();

         //Create 2 sessions and enlist them
         XASession sess1 = conn.createXASession();
         MessagingXAResource res1 = (MessagingXAResource)sess1.getXAResource();
         XASession sess2 = conn.createXASession();
         MessagingXAResource res2 = (MessagingXAResource)sess2.getXAResource();
         res1.setPreventJoining(true);
         res2.setPreventJoining(true);

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res1);
         tx.enlistResource(res2);

         //Receive the messages, two on each consumer
         MessageConsumer cons1 = sess1.createConsumer(queue);
         TextMessage r1 = (TextMessage)cons1.receive(MAX_TIMEOUT);

         assertNotNull(r1);
         assertEquals("jellyfish1", r1.getText());

         r1 = (TextMessage)cons1.receive(MAX_TIMEOUT);

         assertNotNull(r1);
         assertEquals("jellyfish2", r1.getText());

         cons1.close();

         //Cancel is asynch
         Thread.sleep(500);

         MessageConsumer cons2 = sess2.createConsumer(queue);
         TextMessage r2 = (TextMessage)cons2.receive(MAX_TIMEOUT);

         assertNotNull(r2);
         assertEquals("jellyfish3", r2.getText());

         r2 = (TextMessage)cons2.receive(MAX_TIMEOUT);

         assertNotNull(r2);
         assertEquals("jellyfish4", r2.getText());

         //rollback

         cons2.close();

         tx.delistResource(res1, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         tm.rollback();

         // Rollback causes cancel which is asynch
         Thread.sleep(1000);

         //We cannot assume anything about the order in which the transaction manager rollsback
         //the sessions - this is implementation dependent


         Session sess = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sess.createConsumer(queue);
         conn2.start();

         TextMessage r = (TextMessage)cons.receive(MAX_TIMEOUT);
         assertNotNull(r);

         boolean session1First = false;

         if (r.getText().equals("jellyfish1"))
         {
            session1First = true;
         }
         else if (r.getText().equals("jellyfish3"))
         {
            session1First = false;
         }
         else
         {
            fail("Unexpected message");
         }

         if (session1First)
         {
            r = (TextMessage)cons.receive(MAX_TIMEOUT);

            assertNotNull(r);

            assertEquals("jellyfish2", r.getText());

            r = (TextMessage)cons.receive(MAX_TIMEOUT);

            assertNotNull(r);

            assertEquals("jellyfish3", r.getText());

            r = (TextMessage)cons.receive(MAX_TIMEOUT);

            assertNotNull(r);

            assertEquals("jellyfish4", r.getText());


         }
         else
         {
            r = (TextMessage)cons.receive(MAX_TIMEOUT);

            assertNotNull(r);

            assertEquals("jellyfish4", r.getText());

            r = (TextMessage)cons.receive(MAX_TIMEOUT);

            assertNotNull(r);

            assertEquals("jellyfish1", r.getText());

            r = (TextMessage)cons.receive(MAX_TIMEOUT);

            assertNotNull(r);

            assertEquals("jellyfish2", r.getText());
         }

         r = (TextMessage)cons.receive(MIN_TIMEOUT);

         assertNull(r);


      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }

   }

   public void testMultipleSessionsOneTxRollbackAcknowledgeForceFailureInCommit() throws Exception
   {
      XAConnection conn = null;
      Connection conn2 = null;

      try
      {
         //First send 4 messages
         conn2 = cf.createConnection();
         Session sessProducer = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod  = sessProducer.createProducer(queue);

         Message m = sessProducer.createTextMessage("jellyfish1");
         prod.send(m);
         m = sessProducer.createTextMessage("jellyfish2");
         prod.send(m);
         m = sessProducer.createTextMessage("jellyfish3");
         prod.send(m);
         m = sessProducer.createTextMessage("jellyfish4");
         prod.send(m);

         conn = cf.createXAConnection();
         conn.start();

         tm.begin();

         XASession sess1 = conn.createXASession();
         MessagingXAResource res1 = (MessagingXAResource)sess1.getXAResource();
         DummyXAResource res2 = new DummyXAResource(true);
         res1.setPreventJoining(true);

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res1);
         tx.enlistResource(res2);

         MessageConsumer cons1 = sess1.createConsumer(queue);
         TextMessage r1 = (TextMessage)cons1.receive(MAX_TIMEOUT);

         assertNotNull(r1);
         assertEquals("jellyfish1", r1.getText());

         r1 = (TextMessage)cons1.receive(MAX_TIMEOUT);

         assertNotNull(r1);
         assertEquals("jellyfish2", r1.getText());

         r1 = (TextMessage)cons1.receive(MAX_TIMEOUT);

         assertNotNull(r1);
         assertEquals("jellyfish3", r1.getText());

         r1 = (TextMessage)cons1.receive(MAX_TIMEOUT);

         assertNotNull(r1);
         assertEquals("jellyfish4", r1.getText());

         r1 = (TextMessage)cons1.receive(1000);

         assertNull(r1);

         cons1.close();


         //try and commit - and we're going to make the dummyxaresource throw an exception on commit,
         //which should cause rollback to be called on the other resource

         tx.delistResource(res1, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         //rollback will cause an attemp to deliver messages locally to the original consumers.
         //the original consumer has closed, so it will cancelled to the server
         //the server cancel is asynch, so we need to sleep for a bit to make sure it completes
         log.trace("Forcing failure");
         try
         {
            tm.commit();
            fail("should not get here");
         }
         catch (Exception e)
         {
            //We should expect this
         }

         Thread.sleep(1000);


         Session sess = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sess.createConsumer(queue);
         conn2.start();

         TextMessage r = (TextMessage)cons.receive(MAX_TIMEOUT);

         assertNotNull(r);

         assertEquals("jellyfish1", r.getText());

         r = (TextMessage)cons.receive(MAX_TIMEOUT);

         assertNotNull(r);

         assertEquals("jellyfish2", r.getText());

         r = (TextMessage)cons.receive(MAX_TIMEOUT);

         assertNotNull(r);

         assertEquals("jellyfish3", r.getText());

         r = (TextMessage)cons.receive(MAX_TIMEOUT);

         assertNotNull(r);

         assertEquals("jellyfish4", r.getText());

         r = (TextMessage)cons.receive(MIN_TIMEOUT);

         assertNull(r);
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }

   }

   public void testMultipleSessionsOneTxCommitSend1PCOptimization() throws Exception
   {
      //Since both resources have some RM, TM will probably use 1PC optimization

      XAConnection conn = null;

      Connection conn2 = null;

      try
      {
         conn = cf.createXAConnection();
         conn.start();

         tm.begin();

         //Create 2 sessions and enlist them
         XASession sess1 = conn.createXASession();
         XAResource res1 = sess1.getXAResource();
         XASession sess2 = conn.createXASession();
         XAResource res2 = sess2.getXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res1);
         tx.enlistResource(res2);

         // Send 2 messages - one from each session

         MessageProducer prod1 = sess1.createProducer(queue);
         MessageProducer prod2 = sess2.createProducer(queue);

         prod1.send(sess1.createTextMessage("echidna1"));
         prod2.send(sess2.createTextMessage("echidna2"));

         tx.delistResource(res1, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         //commit
         tm.commit();

         //Messages should be in queue

         conn2 = cf.createConnection();
         Session sess = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sess.createConsumer(queue);
         conn2.start();

         TextMessage r1 = (TextMessage)cons.receive(MAX_TIMEOUT);
         assertNotNull(r1);
         assertEquals("echidna1", r1.getText());

         TextMessage r2 = (TextMessage)cons.receive(MAX_TIMEOUT);
         assertNotNull(r2);
         assertEquals("echidna2", r2.getText());

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }

      }

   }

   public void testMultipleSessionsOneTxCommitSend() throws Exception
   {
      //Since both resources have some RM, TM will probably use 1PC optimization

      XAConnection conn = null;

      Connection conn2 = null;

      try
      {

         conn = cf.createXAConnection();
         conn.start();

         tm.begin();

         //Create 2 sessions and enlist them
         XASession sess1 = conn.createXASession();
         MessagingXAResource res1 = (MessagingXAResource)sess1.getXAResource();
         XASession sess2 = conn.createXASession();
         MessagingXAResource res2 = (MessagingXAResource)sess2.getXAResource();
         res1.setPreventJoining(true);
         res2.setPreventJoining(true);

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res1);
         tx.enlistResource(res2);

         // Send 2 messages - one from each session

         MessageProducer prod1 = sess1.createProducer(queue);
         MessageProducer prod2 = sess2.createProducer(queue);

         prod1.send(sess1.createTextMessage("echidna1"));
         prod2.send(sess2.createTextMessage("echidna2"));

         tx.delistResource(res1, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         //commit
         tm.commit();

         //Messages should be in queue

         conn2 = cf.createConnection();
         Session sess = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sess.createConsumer(queue);
         conn2.start();

         TextMessage r1 = (TextMessage)cons.receive(MAX_TIMEOUT);
         assertNotNull(r1);
         assertEquals("echidna1", r1.getText());

         TextMessage r2 = (TextMessage)cons.receive(MAX_TIMEOUT);
         assertNotNull(r2);
         assertEquals("echidna2", r2.getText());

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }

      }

   }


   public void testMultipleSessionsOneTxRollbackSend1PCOptimization() throws Exception
   {
      //Since both resources have some RM, TM will probably use 1PC optimization

      XAConnection conn = null;

      Connection conn2 = null;

      try
      {

         conn = cf.createXAConnection();
         conn.start();

         tm.begin();

         //Create 2 sessions and enlist them
         XASession sess1 = conn.createXASession();
         XAResource res1 = sess1.getXAResource();
         XASession sess2 = conn.createXASession();
         XAResource res2 = sess2.getXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res1);
         tx.enlistResource(res2);

         // Send 2 messages - one from each session

         MessageProducer prod1 = sess1.createProducer(queue);
         MessageProducer prod2 = sess2.createProducer(queue);

         prod1.send(sess1.createTextMessage("echidna1"));
         prod2.send(sess2.createTextMessage("echidna2"));

         tx.delistResource(res1, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         //rollback
         tm.rollback();

         //Messages should not be in queue

         conn2 = cf.createConnection();
         Session sess = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sess.createConsumer(queue);
         conn2.start();

         TextMessage r1 = (TextMessage)cons.receive(MIN_TIMEOUT);
         assertNull(r1);


      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }

      }

   }

   public void testMultipleSessionsOneTxRollbackSend() throws Exception
   {
      XAConnection conn = null;

      Connection conn2 = null;

      try
      {

         conn = cf.createXAConnection();
         conn.start();

         tm.begin();

         //Create 2 sessions and enlist them
         XASession sess1 = conn.createXASession();
         MessagingXAResource res1 = (MessagingXAResource)sess1.getXAResource();
         XASession sess2 = conn.createXASession();
         MessagingXAResource res2 = (MessagingXAResource)sess2.getXAResource();
         res1.setPreventJoining(true);
         res2.setPreventJoining(true);

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res1);
         tx.enlistResource(res2);

         // Send 2 messages - one from each session

         MessageProducer prod1 = sess1.createProducer(queue);
         MessageProducer prod2 = sess2.createProducer(queue);

         prod1.send(sess1.createTextMessage("echidna1"));
         prod2.send(sess2.createTextMessage("echidna2"));

         tx.delistResource(res1, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         //rollback
         tm.rollback();

         //Messages should not be in queue

         conn2 = cf.createConnection();
         Session sess = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sess.createConsumer(queue);
         conn2.start();

         TextMessage r1 = (TextMessage)cons.receive(MIN_TIMEOUT);
         assertNull(r1);


      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }

      }

   }


   public void testOneSessionTwoTransactionsCommitAcknowledge() throws Exception
   {
      XAConnection conn = null;

      Connection conn2 = null;

      try
      {
         //First send 2 messages
         conn2 = cf.createConnection();
         Session sessProducer = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod  = sessProducer.createProducer(queue);
         Message m = sessProducer.createTextMessage("jellyfish1");
         prod.send(m);
         m = sessProducer.createTextMessage("jellyfish2");
         prod.send(m);

         conn = cf.createXAConnection();

         //Create a session
         XASession sess1 = conn.createXASession();
         XAResource res1 = sess1.getXAResource();

         conn.start();
         MessageConsumer cons1 = sess1.createConsumer(queue);

         tm.begin();

         Transaction tx1 = tm.getTransaction();
         tx1.enlistResource(res1);

         //Receive one message in one tx

         TextMessage r1 = (TextMessage)cons1.receive(MAX_TIMEOUT);
         assertNotNull(r1);
         assertEquals("jellyfish1", r1.getText());

         //suspend the tx
         Transaction suspended = tm.suspend();

         tm.begin();

         Transaction tx2 = tm.getTransaction();
         tx2.enlistResource(res1);

         //Receive 2nd message in a different tx
         TextMessage r2 = (TextMessage)cons1.receive(MAX_TIMEOUT);
         assertNotNull(r2);
         assertEquals("jellyfish2", r2.getText());

         tx2.delistResource(res1, XAResource.TMSUCCESS);

         //commit this transaction
         tm.commit();

         //verify that no messages are available
         conn2 = cf.createConnection();
         Session sess = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         conn2.start();
         MessageConsumer cons = sess.createConsumer(queue);
         TextMessage r3 = (TextMessage)cons.receive(MIN_TIMEOUT);
         assertNull(r3);

         //now resume the first tx and then commit it
         tm.resume(suspended);

         tx1.delistResource(res1, XAResource.TMSUCCESS);

         tm.commit();


      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }

      }

   }


   public void testOneSessionTwoTransactionsRollbackAcknowledge() throws Exception
   {
      XAConnection conn = null;

      Connection conn2 = null;

      try
      {
         //First send 2 messages
         conn2 = cf.createConnection();
         Session sessProducer = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod  = sessProducer.createProducer(queue);
         Message m = sessProducer.createTextMessage("jellyfish1");
         prod.send(m);
         m = sessProducer.createTextMessage("jellyfish2");
         prod.send(m);

         conn = cf.createXAConnection();

         //Create a session
         XASession sess1 = conn.createXASession();
         XAResource res1 = sess1.getXAResource();

         conn.start();
         MessageConsumer cons1 = sess1.createConsumer(queue);

         tm.begin();

         Transaction tx1 = tm.getTransaction();
         tx1.enlistResource(res1);

         //Receive one message in one tx

         TextMessage r1 = (TextMessage)cons1.receive(MAX_TIMEOUT);
         assertNotNull(r1);
         assertEquals("jellyfish1", r1.getText());

         //suspend the tx
         Transaction suspended = tm.suspend();

         tm.begin();

         Transaction tx2 = tm.getTransaction();
         tx2.enlistResource(res1);

         //Receive 2nd message in a different tx
         TextMessage r2 = (TextMessage)cons1.receive(MAX_TIMEOUT);
         assertNotNull(r2);
         assertEquals("jellyfish2", r2.getText());

         cons1.close();

         tx1.delistResource(res1, XAResource.TMSUCCESS);

         //rollback this transaction
         tm.rollback();

         //verify that second message is available
         conn2 = cf.createConnection();
         Session sess = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         conn2.start();
         MessageConsumer cons = sess.createConsumer(queue);

         TextMessage r3 = (TextMessage)cons.receive(MAX_TIMEOUT);

         assertNotNull(r3);
         assertEquals("jellyfish2", r3.getText());
         r3 = (TextMessage)cons.receive(MIN_TIMEOUT);
         assertNull(r3);


         //rollback the other tx
         tm.resume(suspended);
         tm.rollback();

         //Verify the first message is now available
         r3 = (TextMessage)cons.receive(MAX_TIMEOUT);
         assertNotNull(r3);
         assertEquals("jellyfish1", r3.getText());
         r3 = (TextMessage)cons.receive(MIN_TIMEOUT);
         assertNull(r3);

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }

      }

   }


   public void testOneSessionTwoTransactionsCommitSend() throws Exception
   {
      XAConnection conn = null;

      Connection conn2 = null;

      try
      {

         conn = cf.createXAConnection();

         //Create a session
         XASession sess1 = conn.createXASession();
         XAResource res1 = sess1.getXAResource();

         MessageProducer prod1 = sess1.createProducer(queue);

         tm.begin();

         Transaction tx1 = tm.getTransaction();
         tx1.enlistResource(res1);

         //Send a message
         prod1.send(sess1.createTextMessage("kangaroo1"));

         //suspend the tx
         Transaction suspended = tm.suspend();

         tm.begin();

         //Send another message in another tx using the same session
         Transaction tx2 = tm.getTransaction();
         tx2.enlistResource(res1);

         //Send a message
         prod1.send(sess1.createTextMessage("kangaroo2"));

         tx2.delistResource(res1, XAResource.TMSUCCESS);

         //commit this transaction
         tm.commit();

         //verify only kangaroo2 message is sent
         conn2 = cf.createConnection();
         Session sess = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         conn2.start();
         MessageConsumer cons = sess.createConsumer(queue);
         TextMessage r1 = (TextMessage)cons.receive(MAX_TIMEOUT);
         assertNotNull(r1);
         assertEquals("kangaroo2", r1.getText());
         TextMessage r2 = (TextMessage)cons.receive(MIN_TIMEOUT);
         assertNull(r2);

         //now resume the first tx and then commit it
         tm.resume(suspended);

         tx1.delistResource(res1, XAResource.TMSUCCESS);

         tm.commit();

         //verify that the first text message is received
         TextMessage r3 = (TextMessage)cons.receive(MAX_TIMEOUT);
         assertNotNull(r3);
         assertEquals("kangaroo1", r3.getText());

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }

      }

   }


   public void testOneSessionTwoTransactionsRollbackSend() throws Exception
   {
      XAConnection conn = null;

      Connection conn2 = null;

      try
      {

         conn = cf.createXAConnection();

         //Create a session
         XASession sess1 = conn.createXASession();
         XAResource res1 = sess1.getXAResource();

         MessageProducer prod1 = sess1.createProducer(queue);

         tm.begin();

         Transaction tx1 = tm.getTransaction();
         tx1.enlistResource(res1);

         //Send a message
         prod1.send(sess1.createTextMessage("kangaroo1"));

         //suspend the tx
         Transaction suspended = tm.suspend();

         tm.begin();

         //Send another message in another tx using the same session
         Transaction tx2 = tm.getTransaction();
         tx2.enlistResource(res1);

         //Send a message
         prod1.send(sess1.createTextMessage("kangaroo2"));

         tx2.delistResource(res1, XAResource.TMSUCCESS);

         //rollback this transaction
         tm.rollback();

         //verify no messages are sent
         conn2 = cf.createConnection();
         Session sess = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         conn2.start();
         MessageConsumer cons = sess.createConsumer(queue);
         TextMessage r1 = (TextMessage)cons.receive(MIN_TIMEOUT);

         assertNull(r1);


         //now resume the first tx and then commit it
         tm.resume(suspended);

         tx1.delistResource(res1, XAResource.TMSUCCESS);

         tm.commit();

         //verify that the first text message is received
         TextMessage r3 = (TextMessage)cons.receive(MAX_TIMEOUT);
         assertNotNull(r3);
         assertEquals("kangaroo1", r3.getText());

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }

      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void assertEqualByteArrays(byte[] b1, byte[] b2)
   {
      log.info("b1 length: " + b1.length + " b2 length " + b2.length);

      if (b1.length != b2.length)
      {
         fail("Lengths not the same");
      }

      for (int i = 0; i < b1.length; i++)
      {
         if (b1[i] != b2[i])
         {
            fail("Not same at index " + i);
         }
      }
   }

   // Inner classes -------------------------------------------------


   static class DummyListener implements MessageListener
   {

      protected Logger log = Logger.getLogger(getClass());

      public ArrayList messages = new ArrayList();

      public void onMessage(Message message)
      {
         log.info("Message received on DummyListener " + message);
         messages.add(message);
      }
   }

   static class MockServerSessionPool implements ServerSessionPool
   {
      private ServerSession serverSession;

      MockServerSessionPool(Session sess)
      {
         serverSession = new MockServerSession(sess);
      }

      public ServerSession getServerSession() throws JMSException
      {
         return serverSession;
      }
   }

   static class MockServerSession implements ServerSession
   {
      Session session;

      MockServerSession(Session sess)
      {
         this.session = sess;
      }


      public Session getSession() throws JMSException
      {
         return session;
      }

      public void start() throws JMSException
      {
         session.run();
      }

   }



   static class DummyXAResource implements XAResource
   {
      boolean failOnPrepare;

      DummyXAResource()
      {
      }

      DummyXAResource(boolean failOnPrepare)
      {
         this.failOnPrepare = failOnPrepare;
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
         if (failOnPrepare)
         {
            throw new XAException(XAException.XAER_RMFAIL);
         }
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
