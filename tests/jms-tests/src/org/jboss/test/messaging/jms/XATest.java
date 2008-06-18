/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

import java.util.ArrayList;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ServerSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.XAConnection;
import javax.jms.XASession;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.messaging.core.client.impl.ClientSessionImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.transaction.impl.XidImpl;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.test.messaging.JBMServerTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.tm.TxUtils;
import org.jboss.tm.TransactionManagerLocator;

import com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionManagerImple;

/**
 *
 * A XATestBase
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class XATest extends JBMServerTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------
  
   protected TransactionManager tm;

   protected Transaction suspendedTx;

   protected JBossConnectionFactory cf;

   // Constructors --------------------------------------------------

   public XATest(String name)
   {
      super(name);
   }


   // TestCase overrides -------------------------------------------

   public void setUp() throws Exception
   { 
      super.setUp();
      
      cf = getConnectionFactory();

      tm = TransactionManagerLocator.getInstance().getTransactionManager();//this.getTransactionManager();
      

      assertTrue(tm instanceof TransactionManagerImple);
     
      if (!ServerManagement.isRemote())
      {
         suspendedTx = tm.suspend();
      }
   }

   public void tearDown() throws Exception
   {      
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
      
      super.tearDown();
   }

   // Public --------------------------------------------------------


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

         Xid trailing = new XidImpl(branchQualifier, 12435, globalTxId);

         res1.start(trailing, XAResource.TMNOFLAGS);

         MessageProducer prod1 = sess1.createProducer(queue1);

         TextMessage tm1 = sess1.createTextMessage("testing1");

         prod1.send(tm1);

         res1.end(trailing, XAResource.TMSUCCESS);

         res1.prepare(trailing);

         //Now "crash" the server

         stopServerPeer();

         startServerPeer();

         deployAndLookupAdministeredObjects();
         
         conn1.close();
         
         conn1 = cf.createXAConnection();

         XAResource res = conn1.createXASession().getXAResource();

         Xid[] xids = res.recover(XAResource.TMSTARTRSCAN);
         assertEquals(1, xids.length);

         Xid[] xids2 = res.recover(XAResource.TMENDRSCAN);
         assertEquals(0, xids2.length);

         Xid trailing2 = xids[0];

         assertTrue(trailing.getFormatId() == trailing2.getFormatId());

         assertEqualByteArrays(trailing.getGlobalTransactionId(), trailing2.getGlobalTransactionId());

         assertEqualByteArrays(trailing.getBranchQualifier(), trailing2.getBranchQualifier());

         res.commit(trailing, false);
      }
      finally
      {
         removeAllMessages(queue1.getQueueName(), true, 0);
       
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
      //Since both resources have same RM, TM will probably use 1PC optimization

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

         MessageProducer prod = sess.createProducer(queue1);
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         Message m = sess.createTextMessage("XATest1");
         prod.send(queue1, m);
         m = sess.createTextMessage("XATest2");
         prod.send(queue1, m);

         tx.delistResource(res, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         tm.commit();

         conn2 = cf.createConnection();
         conn2.start();
         Session sessReceiver = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sessReceiver.createConsumer(queue1);
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

         XAResource res = sess.getXAResource();
         XAResource res2 = new DummyXAResource();

         //To prevent 1PC optimization being used
         //res.setForceNotSameRM(true);

         Transaction tx = tm.getTransaction();
         
         log.info("res is " + res);
         log.info("res2 is " + res2);
         
         log.info("Enlisting reousrce1");
         tx.enlistResource(res);
                           
         log.info("enlisting resource2");
         tx.enlistResource(res2);

         MessageProducer prod = sess.createProducer(queue1);
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         Message m = sess.createTextMessage("XATest1");
         prod.send(queue1, m);
         m = sess.createTextMessage("XATest2");
         prod.send(queue1, m);

         tx.delistResource(res, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         log.info("Committing***");
         tm.commit();
         log.info("Committed****");

         conn2 = cf.createConnection();
         conn2.start();
         Session sessReceiver = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sessReceiver.createConsumer(queue1);
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

         MessageProducer prod = sess.createProducer(queue1);
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         Message m = sess.createTextMessage("XATest1");
         prod.send(queue1, m);
         m = sess.createTextMessage("XATest2");
         prod.send(queue1, m);

         tx.delistResource(res, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         tm.rollback();

         conn2 = cf.createConnection();
         conn2.start();
         Session sessReceiver = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sessReceiver.createConsumer(queue1);
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
         XAResource res = sess.getXAResource();

         //prevent 1Pc optimisation
         //res.setForceNotSameRM(true);

         XAResource res2 = new DummyXAResource(true);
         XAResource res3 = new DummyXAResource();
         XAResource res4 = new DummyXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.enlistResource(res2);
         tx.enlistResource(res3);
         tx.enlistResource(res4);

         MessageProducer prod = sess.createProducer(queue1);
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         Message m = sess.createTextMessage("XATest1");
         prod.send(queue1, m);
         m = sess.createTextMessage("XATest2");
         prod.send(queue1, m);

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
         MessageConsumer cons = sessReceiver.createConsumer(queue1);
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
         XAResource res = sess.getXAResource();

         //prevent 1Pc optimisation
         //res.setForceNotSameRM(true);

         XAResource res2 = new DummyXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.enlistResource(res2);

         MessageProducer prod = sess.createProducer(queue1);
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         Message m = sess.createTextMessage("XATest1");
         prod.send(queue1, m);
         m = sess.createTextMessage("XATest2");
         prod.send(queue1, m);

         tx.delistResource(res, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         tm.rollback();

         conn2 = cf.createConnection();
         conn2.start();
         Session sessReceiver = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sessReceiver.createConsumer(queue1);
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
         MessageProducer prod  = sessProducer.createProducer(queue1);
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

         MessageConsumer cons = sess.createConsumer(queue1);


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
         MessageProducer prod  = sessProducer.createProducer(queue1);
         Message m = sessProducer.createTextMessage("XATest1");
         prod.send(m);
         m = sessProducer.createTextMessage("XATest2");
         prod.send(m);

         conn = cf.createXAConnection();
         conn.start();

         tm.begin();

         XASession sess = conn.createXASession();
         XAResource res = (XAResource)sess.getXAResource();
         //res.setForceNotSameRM(true);

         XAResource res2 = new DummyXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.enlistResource(res2);

         MessageConsumer cons = sess.createConsumer(queue1);


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
         MessageProducer prod  = sessProducer.createProducer(queue1);
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

         MessageConsumer cons = sess.createConsumer(queue1);


         TextMessage m2 = (TextMessage)cons.receive(MAX_TIMEOUT);
         assertNotNull(m2);
         assertEquals("XATest1", m2.getText());
         m2 = (TextMessage)cons.receive(MAX_TIMEOUT);
         assertNotNull(m2);
         assertEquals("XATest2", m2.getText());

         tx.delistResource(res, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         log.info("rolling back");
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
         MessageProducer prod  = sessProducer.createProducer(queue1);
         Message m = sessProducer.createTextMessage("XATest1");
         prod.send(m);

         m = sessProducer.createTextMessage("XATest2");
         prod.send(m);


         conn = cf.createXAConnection();
         conn.start();

         tm.begin();

         XASession sess = conn.createXASession();
         XAResource res = sess.getXAResource();
         //res.setForceNotSameRM(true);

         XAResource res2 = new DummyXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.enlistResource(res2);

         MessageConsumer cons = sess.createConsumer(queue1);


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


         MessageProducer prod = sess.createProducer(queue1);
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         Message m = sess.createTextMessage("XATest1");
         prod.send(queue1, m);
         m = sess.createTextMessage("XATest2");
         prod.send(queue1, m);

         tx.delistResource(res, XAResource.TMSUCCESS);

         tm.commit();

         conn2 = cf.createConnection();
         conn2.start();
         Session sessReceiver = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sessReceiver.createConsumer(queue1);
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

         MessageProducer prod = sess.createProducer(queue1);
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         Message m = sess.createTextMessage("XATest1");
         prod.send(queue1, m);
         m = sess.createTextMessage("XATest2");
         prod.send(queue1, m);

         tx.delistResource(res, XAResource.TMSUCCESS);

         tm.rollback();

         conn2 = cf.createConnection();
         conn2.start();
         Session sessReceiver = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sessReceiver.createConsumer(queue1);
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
         MessageProducer prod  = sessProducer.createProducer(queue1);
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

         MessageConsumer cons = sess.createConsumer(queue1);


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
         MessageProducer prod  = sessProducer.createProducer(queue1);
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

         MessageConsumer cons = sess.createConsumer(queue1);


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
         MessageProducer prod  = sessProducer.createProducer(queue1);
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
         MessageConsumer cons1 = sess1.createConsumer(queue1);
         TextMessage r1 = (TextMessage)cons1.receive(MAX_TIMEOUT);

         assertNotNull(r1);
         assertEquals("jellyfish1", r1.getText());

         cons1.close();

         MessageConsumer cons2 = sess2.createConsumer(queue1);
         TextMessage r2 = (TextMessage)cons2.receive(MAX_TIMEOUT);

         assertNotNull(r2);
         assertEquals("jellyfish2", r2.getText());

         tx.delistResource(res1, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         //commit
         tm.commit();

         Session sess = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sess.createConsumer(queue1);
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
         MessageProducer prod  = sessProducer.createProducer(queue1);
         Message m = sessProducer.createTextMessage("jellyfish1");
         prod.send(m);
         m = sessProducer.createTextMessage("jellyfish2");
         prod.send(m);


         conn = cf.createXAConnection();
         conn.start();

         tm.begin();

         //Create 2 sessions and enlist them
         XASession sess1 = conn.createXASession();
         ClientSessionImpl res1 = (ClientSessionImpl)sess1.getXAResource();
         XASession sess2 = conn.createXASession();
         ClientSessionImpl res2 = (ClientSessionImpl)sess2.getXAResource();
         res1.setForceNotSameRM(true);
         res2.setForceNotSameRM(true);

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res1);
         tx.enlistResource(res2);

         //Receive the messages, one on each consumer
         MessageConsumer cons1 = sess1.createConsumer(queue1);
         TextMessage r1 = (TextMessage)cons1.receive(MAX_TIMEOUT);

         assertNotNull(r1);
         assertEquals("jellyfish1", r1.getText());

         cons1.close();

         MessageConsumer cons2 = sess2.createConsumer(queue1);
         TextMessage r2 = (TextMessage)cons2.receive(MAX_TIMEOUT);

         assertNotNull(r2);
         assertEquals("jellyfish2", r2.getText());

         tx.delistResource(res1, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         //commit
         tm.commit();

         Session sess = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sess.createConsumer(queue1);
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
         MessageProducer prod  = sessProducer.createProducer(queue1);
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
         ClientSessionImpl res1 = (ClientSessionImpl)sess1.getXAResource();
         XASession sess2 = conn.createXASession();
         ClientSessionImpl res2 = (ClientSessionImpl)sess2.getXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res1);
         tx.enlistResource(res2);

         //Receive the messages, two on each consumer
         MessageConsumer cons1 = sess1.createConsumer(queue1);
         TextMessage r1 = (TextMessage)cons1.receive(MAX_TIMEOUT);

         assertNotNull(r1);
         assertEquals("jellyfish1", r1.getText());

         r1 = (TextMessage)cons1.receive(MAX_TIMEOUT);

         assertNotNull(r1);
         assertEquals("jellyfish2", r1.getText());

         cons1.close();

         MessageConsumer cons2 = sess2.createConsumer(queue1);
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
         MessageConsumer cons = sess.createConsumer(queue1);
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
         MessageProducer prod  = sessProducer.createProducer(queue1);
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
         ClientSessionImpl res1 = (ClientSessionImpl)sess1.getXAResource();
         XASession sess2 = conn.createXASession();
         ClientSessionImpl res2 = (ClientSessionImpl)sess2.getXAResource();
         res1.setForceNotSameRM(true);
         res2.setForceNotSameRM(true);

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res1);
         tx.enlistResource(res2);

         //Receive the messages, two on each consumer
         MessageConsumer cons1 = sess1.createConsumer(queue1);
         TextMessage r1 = (TextMessage)cons1.receive(MAX_TIMEOUT);

         assertNotNull(r1);
         assertEquals("jellyfish1", r1.getText());

         r1 = (TextMessage)cons1.receive(MAX_TIMEOUT);

         assertNotNull(r1);
         assertEquals("jellyfish2", r1.getText());

         cons1.close();

         //Cancel is asynch
         Thread.sleep(500);

         MessageConsumer cons2 = sess2.createConsumer(queue1);
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
         MessageConsumer cons = sess.createConsumer(queue1);
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
         MessageProducer prod  = sessProducer.createProducer(queue1);

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
         XAResource res1 = sess1.getXAResource();
         DummyXAResource res2 = new DummyXAResource(true);

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res1);
         tx.enlistResource(res2);

         MessageConsumer cons1 = sess1.createConsumer(queue1);
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
         MessageConsumer cons = sess.createConsumer(queue1);
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

         MessageProducer prod1 = sess1.createProducer(queue1);
         MessageProducer prod2 = sess2.createProducer(queue1);

         prod1.send(sess1.createTextMessage("echidna1"));
         prod2.send(sess2.createTextMessage("echidna2"));

         tx.delistResource(res1, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         //commit
         tm.commit();

         //Messages should be in queue

         conn2 = cf.createConnection();
         Session sess = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sess.createConsumer(queue1);
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
         ClientSessionImpl res1 = (ClientSessionImpl)sess1.getXAResource();
         XASession sess2 = conn.createXASession();
         ClientSessionImpl res2 = (ClientSessionImpl)sess2.getXAResource();
         res1.setForceNotSameRM(true);
         res2.setForceNotSameRM(true);

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res1);
         tx.enlistResource(res2);

         // Send 2 messages - one from each session

         MessageProducer prod1 = sess1.createProducer(queue1);
         MessageProducer prod2 = sess2.createProducer(queue1);

         prod1.send(sess1.createTextMessage("echidna1"));
         prod2.send(sess2.createTextMessage("echidna2"));

         tx.delistResource(res1, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         //commit
         tm.commit();

         //Messages should be in queue

         conn2 = cf.createConnection();
         Session sess = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sess.createConsumer(queue1);
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

         MessageProducer prod1 = sess1.createProducer(queue1);
         MessageProducer prod2 = sess2.createProducer(queue1);

         prod1.send(sess1.createTextMessage("echidna1"));
         prod2.send(sess2.createTextMessage("echidna2"));

         tx.delistResource(res1, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         //rollback
         tm.rollback();

         //Messages should not be in queue

         conn2 = cf.createConnection();
         Session sess = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sess.createConsumer(queue1);
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
         ClientSessionImpl res1 = (ClientSessionImpl)sess1.getXAResource();
         XASession sess2 = conn.createXASession();
         ClientSessionImpl res2 = (ClientSessionImpl)sess2.getXAResource();
         res1.setForceNotSameRM(true);
         res2.setForceNotSameRM(true);

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res1);
         tx.enlistResource(res2);

         // Send 2 messages - one from each session

         MessageProducer prod1 = sess1.createProducer(queue1);
         MessageProducer prod2 = sess2.createProducer(queue1);

         prod1.send(sess1.createTextMessage("echidna1"));
         prod2.send(sess2.createTextMessage("echidna2"));

         tx.delistResource(res1, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         //rollback
         tm.rollback();

         //Messages should not be in queue

         conn2 = cf.createConnection();
         Session sess = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sess.createConsumer(queue1);
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
         MessageProducer prod  = sessProducer.createProducer(queue1);
         Message m = sessProducer.createTextMessage("jellyfish1");
         prod.send(m);
         m = sessProducer.createTextMessage("jellyfish2");
         prod.send(m);

         conn = cf.createXAConnection();

         //Create a session
         XASession sess1 = conn.createXASession();
         XAResource res1 = sess1.getXAResource();

         conn.start();
         MessageConsumer cons1 = sess1.createConsumer(queue1);

         tm.begin();

         Transaction tx1 = tm.getTransaction();
         tx1.enlistResource(res1);

         //Receive one message in one tx

         TextMessage r1 = (TextMessage)cons1.receive(MAX_TIMEOUT);
         assertNotNull(r1);
         assertEquals("jellyfish1", r1.getText());
         
         tx1.delistResource(res1, XAResource.TMSUCCESS);

         //suspend the tx
         log.info("Calling suspend");
         Transaction suspended = tm.suspend();
         log.info("Called suspend");

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
         conn2.close();
         conn2 = cf.createConnection();
         Session sess = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         conn2.start();
         MessageConsumer cons = sess.createConsumer(queue1);
         TextMessage r3 = (TextMessage)cons.receive(MIN_TIMEOUT);
         assertNull(r3);

         //now resume the first tx and then commit it
         tm.resume(suspended);

         tm.commit();
         
         log.info("Got to end");
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
         MessageProducer prod  = sessProducer.createProducer(queue1);
         Message m = sessProducer.createTextMessage("jellyfish1");
         prod.send(m);
         m = sessProducer.createTextMessage("jellyfish2");
         prod.send(m);

         conn = cf.createXAConnection();

         //Create a session
         XASession sess1 = conn.createXASession();
         XAResource res1 = sess1.getXAResource();

         conn.start();
         MessageConsumer cons1 = sess1.createConsumer(queue1);

         tm.begin();

         Transaction tx1 = tm.getTransaction();
         tx1.enlistResource(res1);

         //Receive one message in one tx

         TextMessage r1 = (TextMessage)cons1.receive(MAX_TIMEOUT);
         assertNotNull(r1);
         assertEquals("jellyfish1", r1.getText());
         
         tx1.delistResource(res1, XAResource.TMSUCCESS);

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

         tx2.delistResource(res1, XAResource.TMSUCCESS);

         //rollback this transaction
         tm.rollback();

         //verify that second message is available
         conn2.close();
         conn2 = cf.createConnection();
         Session sess = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         conn2.start();
         MessageConsumer cons = sess.createConsumer(queue1);

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

         MessageProducer prod1 = sess1.createProducer(queue1);

         tm.begin();

         Transaction tx1 = tm.getTransaction();
         tx1.enlistResource(res1);

         //Send a message
         prod1.send(sess1.createTextMessage("kangaroo1"));
         
         tx1.delistResource(res1, XAResource.TMSUCCESS);

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
         MessageConsumer cons = sess.createConsumer(queue1);
         TextMessage r1 = (TextMessage)cons.receive(MAX_TIMEOUT);
         assertNotNull(r1);
         assertEquals("kangaroo2", r1.getText());
         TextMessage r2 = (TextMessage)cons.receive(MIN_TIMEOUT);
         assertNull(r2);

         //now resume the first tx and then commit it
         tm.resume(suspended);

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

         MessageProducer prod1 = sess1.createProducer(queue1);

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
         MessageConsumer cons = sess.createConsumer(queue1);
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
