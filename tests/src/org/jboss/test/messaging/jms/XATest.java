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
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.XAConnection;
import javax.jms.XASession;
import javax.naming.InitialContext;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.tm.TxManager;

/**
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 *
 */
public class XATest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected InitialContext initialContext;
   
   protected JBossConnectionFactory cf;
   protected Destination queue;
   protected TransactionManager tm;

   // Constructors --------------------------------------------------

   public XATest(String name)
   {
      super(name);
   }

   // TestCase overrides -------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      ServerManagement.init("all");
      initialContext = new InitialContext(ServerManagement.getJNDIEnvironment());
      cf = (JBossConnectionFactory)initialContext.lookup("/ConnectionFactory");
      
      
      if (!ServerManagement.isRemote()) tm = TxManager.getInstance();
      
      ServerManagement.undeployQueue("Queue");
      ServerManagement.deployQueue("Queue");
      queue = (Destination)initialContext.lookup("/queue/Queue"); 
      drainDestination(cf, queue);
   }

   public void tearDown() throws Exception
   {
      ServerManagement.undeployQueue("Queue");
      //ServerManagement.deInit();
      super.tearDown();
   }


   // Public --------------------------------------------------------

   public void test2PCSendCommit() throws Exception
   {
      if (ServerManagement.isRemote()) return;
      
      XAConnection conn = null;
      Connection conn2 = null;
      
      try
      {
      
         conn = cf.createXAConnection();
         
         tm.begin();
         
         XASession sess = conn.createXASession();
         XAResource res = sess.getXAResource();
         
         //We also create and enroll another xa resource so the tx mgr uses 
         //2pc protocol
         XAResource res2 = new DummyXAResource();
         
         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.enlistResource(res2);
         
         MessageProducer prod = sess.createProducer(queue);
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         Message m = sess.createTextMessage("XATest");
         prod.send(queue, m);
         
         tx.commit();
         
         conn2 = cf.createConnection();
         conn2.start();
         Session sessReceiver = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sessReceiver.createConsumer(queue);
         TextMessage m2 = (TextMessage)cons.receive(3000);
         assertNotNull(m2);
         assertEquals("XATest", m2.getText());
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
      if (ServerManagement.isRemote()) return;
      
      XAConnection conn = null;
      Connection conn2 = null;
      try
      {
         conn = cf.createXAConnection();
         
         tm.begin();
         
         XASession sess = conn.createXASession();
         XAResource res = sess.getXAResource();
         
         //We also create and enroll another xa resource so the tx mgr uses 
         //2pc protocol
         XAResource res2 = new DummyXAResource();
         
         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.enlistResource(res2);
         
         MessageProducer prod = sess.createProducer(queue);
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         Message m = sess.createTextMessage("XATest");
         prod.send(queue, m);    
         
         tx.rollback();
         
         conn2 = cf.createConnection();
         conn2.start();
         Session sessReceiver = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sessReceiver.createConsumer(queue);
         Message m2 = cons.receive(3000);
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

   public void test2PCReceiveCommit() throws Exception
   {
      if (ServerManagement.isRemote()) return;
      
      XAConnection conn = null;
      Connection conn2 = null;
      
      try
      {
         conn2 = cf.createConnection();
         conn2.start();
         Session sessProducer = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod  = sessProducer.createProducer(queue);
         Message m = sessProducer.createTextMessage("XATest2");
         prod.send(m);
         
         conn = cf.createXAConnection();
         conn.start();
         
         tm.begin();
         
         XASession sess = conn.createXASession();
         XAResource res = sess.getXAResource();
         
         //We also create and enroll another xa resource so the tx mgr uses 
         //2pc protocol
         XAResource res2 = new DummyXAResource();
         
         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.enlistResource(res2);
         
         MessageConsumer cons = sess.createConsumer(queue);
         

         TextMessage m2 = (TextMessage)cons.receive(3000);
         
         assertNotNull(m2);
         assertEquals("XATest2", m2.getText());
         
         tx.commit();
         
         //New tx
         tm.begin();
         tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.enlistResource(res2);
         
         Message m3 = cons.receive(3000);
         
         assertNull(m3);
         
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
      if (ServerManagement.isRemote()) return;
      
      XAConnection conn = null;
      Connection conn2 = null;
      
      try
      {
         conn2 = cf.createConnection();
         Session sessProducer = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod  = sessProducer.createProducer(queue);
         Message m = sessProducer.createTextMessage("XATest2");
         prod.send(m);
         
         
         conn = cf.createXAConnection();
         conn.start();   
         
         tm.begin();
         
         XASession sess = conn.createXASession();
         XAResource res = sess.getXAResource();
         
         //We also create and enroll another xa resource so the tx mgr uses 
         //2pc protocol
         XAResource res2 = new DummyXAResource();
         
         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.enlistResource(res2);
         
         MessageConsumer cons = sess.createConsumer(queue);
         

         TextMessage m2 = (TextMessage)cons.receive(3000);
         
         assertNotNull(m2);
         assertEquals("XATest2", m2.getText());
         
         tx.rollback();
         
         //Message should be redelivered
         
         //New tx
         tm.begin();
         tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.enlistResource(res2);
         
         TextMessage m3 = (TextMessage)cons.receive(3000);
         
         assertNotNull(m3);
         assertEquals("XATest2", m3.getText());
         
         assertTrue(m3.getJMSRedelivered());
         
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
      if (ServerManagement.isRemote()) return;
      
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
         Message m = sess.createTextMessage("XATest");
         prod.send(queue, m);
         
         tx.commit();
         
         conn2 = cf.createConnection();
         conn2.start();
         Session sessReceiver = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sessReceiver.createConsumer(queue);
         TextMessage m2 = (TextMessage)cons.receive(3000);
         assertNotNull(m2);
         assertEquals("XATest", m2.getText());
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
      if (ServerManagement.isRemote()) return;
      
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
         Message m = sess.createTextMessage("XATest");
         prod.send(queue, m);    
         
         tx.rollback();
         
         conn2 = cf.createConnection();
         conn2.start();
         Session sessReceiver = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sessReceiver.createConsumer(queue);
         Message m2 = cons.receive(3000);
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
      if (ServerManagement.isRemote()) return;
      
      XAConnection conn = null;
      Connection conn2 = null;
      
      try
      {
         conn2 = cf.createConnection();
         conn2.start();
         Session sessProducer = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod  = sessProducer.createProducer(queue);
         Message m = sessProducer.createTextMessage("XATest2");
         prod.send(m);
         
         conn = cf.createXAConnection();
         conn.start();
         
         tm.begin();
         
         XASession sess = conn.createXASession();
         XAResource res = sess.getXAResource();
         
         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);
         
         MessageConsumer cons = sess.createConsumer(queue);
         

         TextMessage m2 = (TextMessage)cons.receive(3000);
         
         assertNotNull(m2);
         assertEquals("XATest2", m2.getText());
         
         tx.commit();
         
         //New tx
         tm.begin();
         tx = tm.getTransaction();
         tx.enlistResource(res);
         
         Message m3 = cons.receive(3000);
         
         assertNull(m3);
         
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
      if (ServerManagement.isRemote()) return;
      
      XAConnection conn = null;
      Connection conn2 = null;
      
      try
      {
         conn2 = cf.createConnection();
         Session sessProducer = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod  = sessProducer.createProducer(queue);
         Message m = sessProducer.createTextMessage("XATest2");
         prod.send(m);
         
         
         conn = cf.createXAConnection();
         conn.start();   
         
         tm.begin();
         
         XASession sess = conn.createXASession();
         XAResource res = sess.getXAResource();
         
         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);

         MessageConsumer cons = sess.createConsumer(queue);
         

         TextMessage m2 = (TextMessage)cons.receive(3000);
         
         assertNotNull(m2);
         assertEquals("XATest2", m2.getText());
         
         tx.rollback();
         
         //Message should be redelivered
         
         //New tx
         tm.begin();
         tx = tm.getTransaction();
         tx.enlistResource(res);
         
         TextMessage m3 = (TextMessage)cons.receive(3000);
         
         assertNotNull(m3);
         assertEquals("XATest2", m3.getText());
         
         assertTrue(m3.getJMSRedelivered());
         
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
   
   
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
   
   static class DummyXAResource implements XAResource
   {

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
