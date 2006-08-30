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

import javax.jms.Destination;
import javax.naming.InitialContext;

import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * 
 * A XARecoveryTest

 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class XARecoveryTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected InitialContext initialContext;
   
   protected JBossConnectionFactory cf;
   protected Destination queue;   

   // Constructors --------------------------------------------------

   public XARecoveryTest(String name)
   {
      super(name);
   }

   // TestCase overrides -------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      ServerManagement.start("all");
      
      
      initialContext = new InitialContext(ServerManagement.getJNDIEnvironment());
      cf = (JBossConnectionFactory)initialContext.lookup("/ConnectionFactory");
      
      
      //if (!ServerManagement.isRemote()) tm = TransactionManagerLocator.getInstance().locate();
      
      ServerManagement.undeployQueue("Queue");
      ServerManagement.deployQueue("Queue");
      queue = (Destination)initialContext.lookup("/queue/Queue"); 

   }

   public void tearDown() throws Exception
   {
      ServerManagement.undeployQueue("Queue");
      
      super.tearDown();
   }


   // Public --------------------------------------------------------
   
   //TODO Re-enable when we complete XA Recovery
   
   public void testNoop()
   {
      
   }
   
//
//   public void test1() throws Exception
//   {
//      
//      XAConnection conn1 = cf.createXAConnection();
//      
//      XAConnection conn2 = cf.createXAConnection();
//      
//      XASession sess1 = conn1.createXASession();
//      
//      XASession sess2 = conn2.createXASession();
//      
//      XAResource res1 = sess1.getXAResource();
//      
//      XAResource res2 = sess2.getXAResource();
//      
//      //Pretend to be a transaction manager by interacting through the XAResources
//      Xid xid1 = new XidImpl("bq1".getBytes(), 123, "gbtxid1".getBytes());
//      Xid xid2 = new XidImpl("bq2".getBytes(), 124, "gbtxid2".getBytes());
//      
//      
////    Send a message in each tx
//      
//      
//      res1.start(xid1, XAResource.TMNOFLAGS);
//      
//      MessageProducer prod1 = sess1.createProducer(queue);
//      
//      TextMessage tm1 = sess1.createTextMessage("testing1");
//      
//      prod1.send(tm1);
//      
//      res1.end(xid1, XAResource.TMSUCCESS);
//      
//      
//      
//      
//      res2.start(xid2, XAResource.TMNOFLAGS);
//      
//      MessageProducer prod2 = sess2.createProducer(queue);
//      
//      TextMessage tm2 = sess2.createTextMessage("testing2");
//      
//      prod2.send(tm2);
//      
//      res2.end(xid2, XAResource.TMSUCCESS);
//      
//      //prepare both txs
//      
//      
//      res1.prepare(xid1);      
//      res2.prepare(xid2);
//      
//      //Now "crash" the server
//      
//      ServerManagement.stopServerPeer();
//
//      ServerManagement.startServerPeer();
//
//      //Now lookup the recoverable in JNDI
//      InitialContext ic = new InitialContext();
//      JMSRecoverable recoverable = (JMSRecoverable)ic.lookup("/" +
//            ServerPeer.RECOVERABLE_CTX_NAME + "/"+ ServerManagement.getServerPeer().getServerPeerID());
//      
//      XAResource res = recoverable.getResource();
//      
//      Xid[] xids = res.recover(XAResource.TMSTARTRSCAN);
//      assertEquals(2, xids.length);
//      
//      Xid[] xids2 = res.recover(XAResource.TMENDRSCAN);
//      assertEquals(0, xids2.length);
//      
//      assertTrue(xids[0].equals(xid1) || xids[1].equals(xid1));
//      assertTrue(xids[0].equals(xid2) || xids[1].equals(xid2));
//      
//      res.commit(xid1, false);
//      
//      res.commit(xid2, false);
//      
//      recoverable.cleanUp();
//      
//      ServerManagement.deployQueue("Queue");
//            
//      Connection conn3 = cf.createConnection();
//      
//      Session sessRec = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);
//      MessageConsumer cons = sessRec.createConsumer(queue);
//      conn3.start();
//      
//      TextMessage m2 = (TextMessage)cons.receiveNoWait();
//      assertNotNull(m2);
//      assertEquals("testing1", m2.getText());
//      
//      
//      TextMessage m3 = (TextMessage)cons.receiveNoWait();
//      assertNotNull(m3);
//      assertEquals("testing2", m3.getText());
//      
//      conn3.close();
//
//   }
 
}

