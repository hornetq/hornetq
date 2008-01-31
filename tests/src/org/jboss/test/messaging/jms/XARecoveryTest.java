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
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.XAConnection;
import javax.jms.XASession;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.messaging.core.impl.XidImpl;

import com.arjuna.ats.arjuna.common.Uid;
import com.arjuna.ats.jta.xa.XidImple;

/**
 * 
 * A XARecoveryTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:juha@jboss.org">Juha Lindfors</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class XARecoveryTest extends JMSTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public XARecoveryTest(String name)
   {
      super(name);
   }

   // TestCase overrides -------------------------------------------

   // Public --------------------------------------------------------
   
   /*
    * In this test, we have two queues, each with four messages already in them.
    * 
    * We send 4 more messages to each queue, and ack the original 4 in a tx
    * 
    * Then recover it without restarting the server
    * 
    */
   public void testComplexTransactionalRecoveryWithoutRestart() throws Exception
   {
      Connection conn1 = null;
      
      XAConnection conn2 = null;
      
      XAConnection conn3 = null;
      
      try
      {
         conn1 = cf.createConnection();
         
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod1 = sess1.createProducer(queue2);
         
         MessageProducer prod2 = sess1.createProducer(queue3);
         
         TextMessage tm1 = sess1.createTextMessage("tm1");
         TextMessage tm2 = sess1.createTextMessage("tm2");
         TextMessage tm3 = sess1.createTextMessage("tm3");
         TextMessage tm4 = sess1.createTextMessage("tm4");
         TextMessage tm5 = sess1.createTextMessage("tm5");
         TextMessage tm6 = sess1.createTextMessage("tm6");
         TextMessage tm7 = sess1.createTextMessage("tm7");
         TextMessage tm8 = sess1.createTextMessage("tm8");
         
         prod1.send(tm1);
         prod1.send(tm2);
         prod1.send(tm3);         
         prod1.send(tm4);
         
         prod2.send(tm5);
         prod2.send(tm6);
         prod2.send(tm7);
         prod2.send(tm8);
         
         conn1.close();
         
         conn2 = cf.createXAConnection();
         
         conn2.start();
         
         XASession sess2 = conn2.createXASession();
         
         XAResource res = sess2.getXAResource();
         
         Xid xid1 = new XidImpl("bq1".getBytes(), 42, "eemeli".getBytes());
         
         res.start(xid1, XAResource.TMNOFLAGS);
         
         MessageProducer prod3 = sess2.createProducer(queue2);
         
         TextMessage tm9 = sess2.createTextMessage("tm9");
         TextMessage tm10 = sess2.createTextMessage("tm10");
         TextMessage tm11 = sess2.createTextMessage("tm11");
         TextMessage tm12 = sess2.createTextMessage("tm12");
         
         prod3.send(tm9);
         prod3.send(tm10);
         prod3.send(tm11);
         prod3.send(tm12);
         
         MessageProducer prod4 = sess2.createProducer(queue3);
         
         TextMessage tm13 = sess2.createTextMessage("tm13");
         TextMessage tm14 = sess2.createTextMessage("tm14");
         TextMessage tm15 = sess2.createTextMessage("tm15");
         TextMessage tm16 = sess2.createTextMessage("tm16");
         
         prod4.send(tm13);
         prod4.send(tm14);
         prod4.send(tm15);
         prod4.send(tm16);
         
         MessageConsumer cons1 = sess2.createConsumer(queue2);
         
         TextMessage rm1 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm1);
         assertEquals(tm1.getText(), rm1.getText());
         
         TextMessage rm2 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm2);
         assertEquals(tm2.getText(), rm2.getText());
         
         TextMessage rm3 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm3);
         assertEquals(tm3.getText(), rm3.getText());
         
         TextMessage rm4 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm4);
         assertEquals(tm4.getText(), rm4.getText());
         
         Message m = cons1.receive(1000);
         
         assertNull(m);
                  
         MessageConsumer cons2 = sess2.createConsumer(queue3);
         
         TextMessage rm5 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm5);
         assertEquals(tm5.getText(), rm5.getText());
         
         TextMessage rm6 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm6);
         assertEquals(tm6.getText(), rm6.getText());
         
         TextMessage rm7 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm7);
         assertEquals(tm7.getText(), rm7.getText());
         
         TextMessage rm8 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm8);
         assertEquals(tm8.getText(), rm8.getText());
         
         m = cons2.receive(1000);
         
         assertNull(m);
         
         res.end(xid1, XAResource.TMSUCCESS);
         
         //prepare it
         
         log.trace("Preparing xid " + xid1);
         res.prepare(xid1);
         log.trace("Prepared xid " + xid1);
            
         conn3 = cf.createXAConnection();
         
         XASession sess3 = conn3.createXASession();
         
         XAResource res3 = sess3.getXAResource();
         
         Xid[] xids = res3.recover(XAResource.TMSTARTRSCAN);
         assertEquals(1, xids.length);
         
         Xid[] xids2 = res3.recover(XAResource.TMENDRSCAN);
         assertEquals(0, xids2.length);

         assertEquals(xid1, xids[0]);
         
         log.trace("Committing the tx");
         
         //Commit
         res3.commit(xids[0], false);
         
         log.trace("committed the tx");
         
         conn1.close();
         
         conn2.close();
         
         conn1 = cf.createConnection();
         
         conn1.start();
         
         sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         log.trace("creating a consumer");
         
         cons1 = sess1.createConsumer(queue2);
         
         log.trace("created a consumer");
         
         TextMessage rm9 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm9);
         assertEquals(tm9.getText(), rm9.getText());
         
         TextMessage rm10 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm10);
         assertEquals(tm10.getText(), rm10.getText());
         
         TextMessage rm11 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm11);
         assertEquals(tm11.getText(), rm11.getText());
         
         TextMessage rm12 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm12);
         assertEquals(tm12.getText(), rm12.getText());
         
         m = cons1.receive(1000);
         
         assertNull(m);
                  
         cons2 = sess1.createConsumer(queue3);
         
         TextMessage rm13 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm13);
         assertEquals(tm13.getText(), rm13.getText());
         
         TextMessage rm14 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm14);
         assertEquals(tm14.getText(), rm14.getText());
         
         TextMessage rm15 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm15);
         assertEquals(tm15.getText(), rm15.getText());
         
         TextMessage rm16 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm16);
         assertEquals(tm16.getText(), rm16.getText());
         
         m = cons2.receive(1000);
         
         assertNull(m);
           
      }
      finally
      {
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
   
   /*
    * In this test, we have two queues, each with four messages already in them.
    * 
    * We send 4 more messages to each queue, and ack the original 4 in a tx
    * 
    * Then recover it without restarting the server, then rollback
    * 
    */
   public void testComplexTransactionalRecoveryWithoutRestartRollback() throws Exception
   {
      Connection conn1 = null;
      
      XAConnection conn2 = null;
      
      XAConnection conn3 = null;
      
      try
      {
         conn1 = cf.createConnection();
         
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod1 = sess1.createProducer(queue2);
         
         MessageProducer prod2 = sess1.createProducer(queue3);
         
         TextMessage tm1 = sess1.createTextMessage("tm1");
         TextMessage tm2 = sess1.createTextMessage("tm2");
         TextMessage tm3 = sess1.createTextMessage("tm3");
         TextMessage tm4 = sess1.createTextMessage("tm4");
         TextMessage tm5 = sess1.createTextMessage("tm5");
         TextMessage tm6 = sess1.createTextMessage("tm6");
         TextMessage tm7 = sess1.createTextMessage("tm7");
         TextMessage tm8 = sess1.createTextMessage("tm8");
         
         prod1.send(tm1);
         prod1.send(tm2);
         prod1.send(tm3);         
         prod1.send(tm4);
         
         prod2.send(tm5);
         prod2.send(tm6);
         prod2.send(tm7);
         prod2.send(tm8);
         
         conn2 = cf.createXAConnection();
         
         conn2.start();
         
         XASession sess2 = conn2.createXASession();
         
         XAResource res = sess2.getXAResource();
         
         Xid xid1 = new XidImpl("bq1".getBytes(), 42, "eemeli".getBytes());
         
         res.start(xid1, XAResource.TMNOFLAGS);
         
         MessageProducer prod3 = sess2.createProducer(queue2);
         
         TextMessage tm9 = sess2.createTextMessage("tm9");
         TextMessage tm10 = sess2.createTextMessage("tm10");
         TextMessage tm11 = sess2.createTextMessage("tm11");
         TextMessage tm12 = sess2.createTextMessage("tm12");
         
         prod3.send(tm9);
         prod3.send(tm10);
         prod3.send(tm11);
         prod3.send(tm12);
         
         MessageProducer prod4 = sess2.createProducer(queue3);
         
         TextMessage tm13 = sess2.createTextMessage("tm13");
         TextMessage tm14 = sess2.createTextMessage("tm14");
         TextMessage tm15 = sess2.createTextMessage("tm15");
         TextMessage tm16 = sess2.createTextMessage("tm16");
         
         prod4.send(tm13);
         prod4.send(tm14);
         prod4.send(tm15);
         prod4.send(tm16);
         
         MessageConsumer cons1 = sess2.createConsumer(queue2);
         
         TextMessage rm1 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm1);
         assertEquals(tm1.getText(), rm1.getText());
         
         TextMessage rm2 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm2);
         assertEquals(tm2.getText(), rm2.getText());
         
         TextMessage rm3 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm3);
         assertEquals(tm3.getText(), rm3.getText());
         
         TextMessage rm4 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm4);
         assertEquals(tm4.getText(), rm4.getText());
         
         Message m = cons1.receive(1000);
         
         assertNull(m);
                  
         MessageConsumer cons2 = sess2.createConsumer(queue3);
         
         TextMessage rm5 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm5);
         assertEquals(tm5.getText(), rm5.getText());
         
         TextMessage rm6 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm6);
         assertEquals(tm6.getText(), rm6.getText());
         
         TextMessage rm7 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm7);
         assertEquals(tm7.getText(), rm7.getText());
         
         TextMessage rm8 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm8);
         assertEquals(tm8.getText(), rm8.getText());
         
         m = cons2.receive(1000);
         
         assertNull(m);
         
         res.end(xid1, XAResource.TMSUCCESS);
         
         //prepare it
         
         res.prepare(xid1);
                  
         conn3 = cf.createXAConnection();
         
         XASession sess3 = conn3.createXASession();
         
         XAResource res3 = sess3.getXAResource();
         
         Xid[] xids = res3.recover(XAResource.TMSTARTRSCAN);
         assertEquals(1, xids.length);
         
         Xid[] xids2 = res3.recover(XAResource.TMENDRSCAN);
         assertEquals(0, xids2.length);

         assertEquals(xid1, xids[0]);
         
         log.trace("rolling back the tx");
         
         //rollback
         res3.rollback(xids[0]);
         
         log.trace("rolledb back the tx");
         
         conn1.close();
         
         conn2.close();
         
         
         conn1 = cf.createConnection();
         
         conn1.start();
         
         sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         log.trace("creating a consumer");
         
         cons1 = sess1.createConsumer(queue2);
         
         log.trace("created a consumer");
         
         TextMessage rm9 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm9);
         assertEquals(tm1.getText(), rm9.getText());
         
         TextMessage rm10 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm10);
         assertEquals(tm2.getText(), rm10.getText());
         
         TextMessage rm11 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm11);
         assertEquals(tm3.getText(), rm11.getText());
         
         TextMessage rm12 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm12);
         assertEquals(tm4.getText(), rm12.getText());
         
         m = cons1.receive(1000);
         
         assertNull(m);
                  
         cons2 = sess1.createConsumer(queue3);
         
         TextMessage rm13 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm13);
         assertEquals(tm5.getText(), rm13.getText());
         
         TextMessage rm14 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm14);
         assertEquals(tm6.getText(), rm14.getText());
         
         TextMessage rm15 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm15);
         assertEquals(tm7.getText(), rm15.getText());
         
         TextMessage rm16 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm16);
         assertEquals(tm8.getText(), rm16.getText());
         
         m = cons2.receive(1000);
         
         assertNull(m);         
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
         
         if (conn2 != null)
         {
            try
            {
               conn2.close();
            }
            catch (Exception e)
            {
               //Ignore
            }
         }
         
         if (conn3 != null)
         {
            try
            {
               conn3.close();
            }
            catch (Exception e)
            {
               //Ignore
            }
         }               
      }
   }
   
   /*
    * In this test, we have two queues, each with four messages already in them.
    * 
    * We send 4 more messages to each queue, and ack the original 4 in a tx
    * 
    * Then recover it after restarting the server
    * 
    */
   public void testComplexTransactionalRecoveryWithRestart() throws Exception
   {
      Connection conn1 = null;
      
      XAConnection conn2 = null;
      
      XAConnection conn3 = null;
      
      try
      {
         conn1 = cf.createConnection();
         
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod1 = sess1.createProducer(queue2);
         
         MessageProducer prod2 = sess1.createProducer(queue3);
         
         TextMessage tm1 = sess1.createTextMessage("tm1");
         TextMessage tm2 = sess1.createTextMessage("tm2");
         TextMessage tm3 = sess1.createTextMessage("tm3");
         TextMessage tm4 = sess1.createTextMessage("tm4");
         TextMessage tm5 = sess1.createTextMessage("tm5");
         TextMessage tm6 = sess1.createTextMessage("tm6");
         TextMessage tm7 = sess1.createTextMessage("tm7");
         TextMessage tm8 = sess1.createTextMessage("tm8");
         
         prod1.send(tm1);
         prod1.send(tm2);
         prod1.send(tm3);         
         prod1.send(tm4);
         
         prod2.send(tm5);
         prod2.send(tm6);
         prod2.send(tm7);
         prod2.send(tm8);
         
         conn2 = cf.createXAConnection();
         
         conn2.start();
         
         XASession sess2 = conn2.createXASession();
         
         XAResource res = sess2.getXAResource();
         
         Xid xid1 = new XidImpl("bq1".getBytes(), 42, "eemeli".getBytes());
         
         res.start(xid1, XAResource.TMNOFLAGS);
         
         MessageProducer prod3 = sess2.createProducer(queue2);
         
         TextMessage tm9 = sess2.createTextMessage("tm9");
         TextMessage tm10 = sess2.createTextMessage("tm10");
         TextMessage tm11 = sess2.createTextMessage("tm11");
         TextMessage tm12 = sess2.createTextMessage("tm12");
         
         prod3.send(tm9);
         prod3.send(tm10);
         prod3.send(tm11);
         prod3.send(tm12);
         
         MessageProducer prod4 = sess2.createProducer(queue3);
         
         TextMessage tm13 = sess2.createTextMessage("tm13");
         TextMessage tm14 = sess2.createTextMessage("tm14");
         TextMessage tm15 = sess2.createTextMessage("tm15");
         TextMessage tm16 = sess2.createTextMessage("tm16");
         
         prod4.send(tm13);
         prod4.send(tm14);
         prod4.send(tm15);
         prod4.send(tm16);
         
         MessageConsumer cons1 = sess2.createConsumer(queue2);
         
         TextMessage rm1 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm1);
         assertEquals(tm1.getText(), rm1.getText());
         
         TextMessage rm2 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm2);
         assertEquals(tm2.getText(), rm2.getText());
         
         TextMessage rm3 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm3);
         assertEquals(tm3.getText(), rm3.getText());
         
         TextMessage rm4 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm4);
         assertEquals(tm4.getText(), rm4.getText());
         
         Message m = cons1.receive(1000);
         
         assertNull(m);
                  
         MessageConsumer cons2 = sess2.createConsumer(queue3);
         
         TextMessage rm5 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm5);
         assertEquals(tm5.getText(), rm5.getText());
         
         TextMessage rm6 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm6);
         assertEquals(tm6.getText(), rm6.getText());
         
         TextMessage rm7 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm7);
         assertEquals(tm7.getText(), rm7.getText());
         
         TextMessage rm8 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm8);
         assertEquals(tm8.getText(), rm8.getText());
         
         m = cons2.receive(1000);
         
         assertNull(m);
         
         res.end(xid1, XAResource.TMSUCCESS);
         
         //prepare it
         
         res.prepare(xid1);
         
         conn1.close();
         conn2.close();
         
         conn1 = null;
         
         conn2 = null;
         
         // Now "crash" the server

         stopServerPeer();

         startServerPeer();
         
         log.info("Restarted the server");
         
         deployAndLookupAdministeredObjects();
                  
         conn3 = cf.createXAConnection();
         
         XASession sess3 = conn3.createXASession();
         
         XAResource res3 = sess3.getXAResource();
         
         Xid[] xids = res3.recover(XAResource.TMSTARTRSCAN);
         assertEquals(1, xids.length);
         
         Xid[] xids2 = res3.recover(XAResource.TMENDRSCAN);
         assertEquals(0, xids2.length);

         assertEquals(xid1, xids[0]);
         
         log.trace("Committing the tx");
         
         //Commit
         res3.commit(xids[0], false);
         
         log.trace("committed the tx");

         conn1 = cf.createConnection();
         
         conn1.start();
         
         sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         log.trace("creating a consumer");
         
         cons1 = sess1.createConsumer(queue2);
         
         log.trace("created a consumer");
         
         TextMessage rm9 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm9);
         assertEquals(tm9.getText(), rm9.getText());
         
         TextMessage rm10 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm10);
         assertEquals(tm10.getText(), rm10.getText());
         
         TextMessage rm11 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm11);
         assertEquals(tm11.getText(), rm11.getText());
         
         TextMessage rm12 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm12);
         assertEquals(tm12.getText(), rm12.getText());
         
         m = cons1.receive(1000);
         
         assertNull(m);
                  
         cons2 = sess1.createConsumer(queue3);
         
         TextMessage rm13 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm13);
         assertEquals(tm13.getText(), rm13.getText());
         
         TextMessage rm14 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm14);
         assertEquals(tm14.getText(), rm14.getText());
         
         TextMessage rm15 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm15);
         assertEquals(tm15.getText(), rm15.getText());
         
         TextMessage rm16 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm16);
         assertEquals(tm16.getText(), rm16.getText());
         
         m = cons2.receive(1000);
         
         assertNull(m);
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
         
         if (conn2 != null)
         {
            try
            {
               conn2.close();
            }
            catch (Exception e)
            {
               //Ignore
            }
         }
         
         if (conn3 != null)
         {
            try
            {
               conn3.close();
            }
            catch (Exception e)
            {
               //Ignore
            }
         } 
         Thread.sleep(1000);
         removeAllMessages(queue2.getQueueName(), true, 0);
         removeAllMessages(queue3.getQueueName(), true, 0);
      }
   }
   
            
   
   /*
    * In this test, we have two queues, each with four messages already in them.
    * 
    * We send 4 more messages to each queue, and ack the original 4 in a tx
    * 
    * Then recover it after restarting the server, then rollback
    * 
    */
   public void testComplexTransactionalRecoveryWithRestartRollback() throws Exception
   {
      Connection conn1 = null;
      
      XAConnection conn2 = null;
      
      XAConnection conn3 = null;
      
      try
      {
         conn1 = cf.createConnection();
         
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod1 = sess1.createProducer(queue2);
         
         MessageProducer prod2 = sess1.createProducer(queue3);
         
         TextMessage tm1 = sess1.createTextMessage("tm1");
         TextMessage tm2 = sess1.createTextMessage("tm2");
         TextMessage tm3 = sess1.createTextMessage("tm3");
         TextMessage tm4 = sess1.createTextMessage("tm4");
         TextMessage tm5 = sess1.createTextMessage("tm5");
         TextMessage tm6 = sess1.createTextMessage("tm6");
         TextMessage tm7 = sess1.createTextMessage("tm7");
         TextMessage tm8 = sess1.createTextMessage("tm8");
         
         prod1.send(tm1);
         prod1.send(tm2);
         prod1.send(tm3);         
         prod1.send(tm4);
         
         prod2.send(tm5);
         prod2.send(tm6);
         prod2.send(tm7);
         prod2.send(tm8);
         
         conn2 = cf.createXAConnection();
         
         conn2.start();
         
         XASession sess2 = conn2.createXASession();
         
         XAResource res = sess2.getXAResource();
         
         Xid xid1 = new XidImpl("bq1".getBytes(), 42, "eemeli".getBytes());
         
         res.start(xid1, XAResource.TMNOFLAGS);
         
         MessageProducer prod3 = sess2.createProducer(queue2);
         
         TextMessage tm9 = sess2.createTextMessage("tm9");
         TextMessage tm10 = sess2.createTextMessage("tm10");
         TextMessage tm11 = sess2.createTextMessage("tm11");
         TextMessage tm12 = sess2.createTextMessage("tm12");
         
         prod3.send(tm9);
         prod3.send(tm10);
         prod3.send(tm11);
         prod3.send(tm12);
         
         MessageProducer prod4 = sess2.createProducer(queue3);
         
         TextMessage tm13 = sess2.createTextMessage("tm13");
         TextMessage tm14 = sess2.createTextMessage("tm14");
         TextMessage tm15 = sess2.createTextMessage("tm15");
         TextMessage tm16 = sess2.createTextMessage("tm16");
         
         prod4.send(tm13);
         prod4.send(tm14);
         prod4.send(tm15);
         prod4.send(tm16);
         
         MessageConsumer cons1 = sess2.createConsumer(queue2);
         
         TextMessage rm1 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm1);
         assertEquals(tm1.getText(), rm1.getText());
         
         TextMessage rm2 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm2);
         assertEquals(tm2.getText(), rm2.getText());
         
         TextMessage rm3 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm3);
         assertEquals(tm3.getText(), rm3.getText());
         
         TextMessage rm4 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm4);
         assertEquals(tm4.getText(), rm4.getText());
         
         Message m = cons1.receive(1000);
         
         assertNull(m);
                  
         MessageConsumer cons2 = sess2.createConsumer(queue3);
         
         TextMessage rm5 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm5);
         assertEquals(tm5.getText(), rm5.getText());
         
         TextMessage rm6 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm6);
         assertEquals(tm6.getText(), rm6.getText());
         
         TextMessage rm7 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm7);
         assertEquals(tm7.getText(), rm7.getText());
         
         TextMessage rm8 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm8);
         assertEquals(tm8.getText(), rm8.getText());
         
         m = cons2.receive(1000);
         
         assertNull(m);
         
         res.end(xid1, XAResource.TMSUCCESS);
         
         //prepare it
         
         res.prepare(xid1);
         
         conn1.close();
         conn2.close();
         conn1 = null;
         
         conn2 = null;
         
         // Now "crash" the server

         stopServerPeer();

         startServerPeer();
         
         deployAndLookupAdministeredObjects();
         
         conn3 = cf.createXAConnection();
         
         XASession sess3 = conn3.createXASession();
         
         XAResource res3 = sess3.getXAResource();
         
         Xid[] xids = res3.recover(XAResource.TMSTARTRSCAN);
         assertEquals(1, xids.length);
         
         Xid[] xids2 = res3.recover(XAResource.TMENDRSCAN);
         assertEquals(0, xids2.length);

         assertEquals(xid1, xids[0]);
         
         log.trace("rolling back the tx");
         
         //rollback
         res3.rollback(xids[0]);
         
         log.trace("rolled back the tx");
         
         Thread.sleep(1000);

         conn1 = cf.createConnection();
         
         conn1.start();
         
         sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         log.trace("creating a consumer");
         
         cons1 = sess1.createConsumer(queue2);
         
         log.trace("created a consumer");
         
         TextMessage rm9 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm9);
         assertEquals(tm1.getText(), rm9.getText());
         
         TextMessage rm10 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm10);
         assertEquals(tm2.getText(), rm10.getText());
         
         TextMessage rm11 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm11);
         assertEquals(tm3.getText(), rm11.getText());
         
         TextMessage rm12 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm12);
         assertEquals(tm4.getText(), rm12.getText());
         
         m = cons1.receive(1000);
         
         assertNull(m);
                  
         cons2 = sess1.createConsumer(queue3);
         
         TextMessage rm13 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm13);
         assertEquals(tm5.getText(), rm13.getText());
         
         TextMessage rm14 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm14);
         assertEquals(tm6.getText(), rm14.getText());
         
         TextMessage rm15 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm15);
         assertEquals(tm7.getText(), rm15.getText());
         
         TextMessage rm16 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm16);
         assertEquals(tm8.getText(), rm16.getText());
         
         m = cons2.receive(1000);
         
         assertNull(m);
         
         cons1.close();
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
         
         if (conn2 != null)
         {
            try
            {
               conn2.close();
            }
            catch (Exception e)
            {
               //Ignore
            }
         }
         
         if (conn3 != null)
         {
            try
            {
               conn3.close();
            }
            catch (Exception e)
            {
               //Ignore
            }
         }         
      }
   }
   
   /* Not really necessary - but it does no harm */
   public void testComplexTransactional() throws Exception
   {
      Connection conn1 = null;
      
      XAConnection conn2 = null;
      
      XAConnection conn3 = null;
      
      try
      {
         conn1 = cf.createConnection();
         
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod1 = sess1.createProducer(queue2);
         
         MessageProducer prod2 = sess1.createProducer(queue3);
         
         TextMessage tm1 = sess1.createTextMessage("tm1");
         TextMessage tm2 = sess1.createTextMessage("tm2");
         TextMessage tm3 = sess1.createTextMessage("tm3");
         TextMessage tm4 = sess1.createTextMessage("tm4");
         TextMessage tm5 = sess1.createTextMessage("tm5");
         TextMessage tm6 = sess1.createTextMessage("tm6");
         TextMessage tm7 = sess1.createTextMessage("tm7");
         TextMessage tm8 = sess1.createTextMessage("tm8");
         
         prod1.send(tm1);
         prod1.send(tm2);
         prod1.send(tm3);         
         prod1.send(tm4);
         
         prod2.send(tm5);
         prod2.send(tm6);
         prod2.send(tm7);
         prod2.send(tm8);
         
         conn2 = cf.createXAConnection();
         
         conn2.start();
         
         XASession sess2 = conn2.createXASession();
         
         XAResource res = sess2.getXAResource();
         
         Xid xid1 = new XidImpl("bq1".getBytes(), 42, "eemeli".getBytes());
         
         res.start(xid1, XAResource.TMNOFLAGS);
         
         MessageProducer prod3 = sess2.createProducer(queue2);
         
         TextMessage tm9 = sess2.createTextMessage("tm9");
         TextMessage tm10 = sess2.createTextMessage("tm10");
         TextMessage tm11 = sess2.createTextMessage("tm11");
         TextMessage tm12 = sess2.createTextMessage("tm12");
         
         prod3.send(tm9);
         prod3.send(tm10);
         prod3.send(tm11);
         prod3.send(tm12);
         
         MessageProducer prod4 = sess2.createProducer(queue3);
         
         TextMessage tm13 = sess2.createTextMessage("tm13");
         TextMessage tm14 = sess2.createTextMessage("tm14");
         TextMessage tm15 = sess2.createTextMessage("tm15");
         TextMessage tm16 = sess2.createTextMessage("tm16");
         
         prod4.send(tm13);
         prod4.send(tm14);
         prod4.send(tm15);
         prod4.send(tm16);
         
         MessageConsumer cons1 = sess2.createConsumer(queue2);
         
         TextMessage rm1 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm1);
         assertEquals(tm1.getText(), rm1.getText());
         
         TextMessage rm2 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm2);
         assertEquals(tm2.getText(), rm2.getText());
         
         TextMessage rm3 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm3);
         assertEquals(tm3.getText(), rm3.getText());
         
         TextMessage rm4 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm4);
         assertEquals(tm4.getText(), rm4.getText());
         
         Message m = cons1.receive(1000);
         
         assertNull(m);
                  
         MessageConsumer cons2 = sess2.createConsumer(queue3);
         
         TextMessage rm5 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm5);
         assertEquals(tm5.getText(), rm5.getText());
         
         TextMessage rm6 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm6);
         assertEquals(tm6.getText(), rm6.getText());
         
         TextMessage rm7 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm7);
         assertEquals(tm7.getText(), rm7.getText());
         
         TextMessage rm8 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm8);
         assertEquals(tm8.getText(), rm8.getText());
         
         m = cons2.receive(1000);
         
         assertNull(m);
         
         res.end(xid1, XAResource.TMSUCCESS);
         
         //prepare it
         
         res.prepare(xid1);
         
         res.commit(xid1, false);
         
         conn1.close();
         
         conn2.close();
                                    
         conn1 = cf.createConnection();
         
         conn1.start();
         
         sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         cons1 = sess1.createConsumer(queue2);
         
         TextMessage rm9 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm9);
         assertEquals(tm9.getText(), rm9.getText());
         
         TextMessage rm10 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm10);
         assertEquals(tm10.getText(), rm10.getText());
         
         TextMessage rm11 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm11);
         assertEquals(tm11.getText(), rm11.getText());
         
         TextMessage rm12 = (TextMessage)cons1.receive(1000);
         assertNotNull(rm12);
         assertEquals(tm12.getText(), rm12.getText());
         
         m = cons1.receive(1000);
         
         assertNull(m);
                  
         cons2 = sess1.createConsumer(queue3);
         
         TextMessage rm13 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm13);
         assertEquals(tm13.getText(), rm13.getText());
         
         TextMessage rm14 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm14);
         assertEquals(tm14.getText(), rm14.getText());
         
         TextMessage rm15 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm15);
         assertEquals(tm15.getText(), rm15.getText());
         
         TextMessage rm16 = (TextMessage)cons2.receive(1000);
         assertNotNull(rm16);
         assertEquals(tm16.getText(), rm16.getText());
         
         m = cons2.receive(1000);
         
         assertNull(m);
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
         
         if (conn2 != null)
         {
            try
            {
               conn2.close();
            }
            catch (Exception e)
            {
               //Ignore
            }
         }
         
         if (conn3 != null)
         {
            try
            {
               conn3.close();
            }
            catch (Exception e)
            {
               //Ignore
            }
         }               
      }
   }
   
   
   /* A simple send in a transaction - recovered without restarting the server */
   public void testSimpleTransactionalSendRecoveryWithoutRestart() throws Exception
   {
      log.trace("starting testSimpleTransactionalDeliveryRecoveryWithoutRestart");
      
      XAConnection conn1 = null;
      
      Connection conn2 = null;
      
      XAConnection conn3 = null;
      
      try
      {
         conn1 = cf.createXAConnection();
   
         XASession sess1 = conn1.createXASession();
   
         XAResource res1 = sess1.getXAResource();
   
         //Pretend to be a transaction manager by interacting through the XAResources
         Xid xid1 = new XidImpl("bq1".getBytes(), 42, "eemeli".getBytes());
   
         log.trace("Sending message");
         
         //Send message in tx
         
         res1.start(xid1, XAResource.TMNOFLAGS);
   
         MessageProducer prod1 = sess1.createProducer(queue4);
   
         TextMessage tm1 = sess1.createTextMessage("tm1");
   
         prod1.send(tm1);
         
         res1.end(xid1, XAResource.TMSUCCESS);
         
         log.trace("Sent message");
   
         //prepare tx
   
         res1.prepare(xid1);
   
         log.trace("prepared tx");
         
         conn2 = cf.createConnection();
         
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons2 = sess2.createConsumer(queue4);
         
         conn2.start();
         
         //Verify message can't be received
         
         Message m = cons2.receive(1000);
         
         assertNull(m);
         
         
         //Now recover
         
         conn3 = cf.createXAConnection();
         
         XASession sess3 = conn3.createXASession();
         
         XAResource res3 = sess3.getXAResource();
         
         Xid[] xids = res3.recover(XAResource.TMSTARTRSCAN);
         assertEquals(1, xids.length);
         
         Xid[] xids2 = res3.recover(XAResource.TMENDRSCAN);
         assertEquals(0, xids2.length);

         assertEquals(xid1, xids[0]);
                  
         // Verify message still can't be received
         
         m = cons2.receive(1000);
         
         assertNull(m);
         
         //Commit the tx
         
         res1.commit(xids[0], false);
         
         //The message should now be available
         
         TextMessage rm1 = (TextMessage)cons2.receive(1000);
         
         assertNotNull(rm1);
         
         assertEquals(tm1.getText(), rm1.getText());
         
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
         
         if (conn2 != null)
         {
            try
            {
               conn2.close();
            }
            catch (Exception e)
            {
               //Ignore
            }
         }
         
         if (conn3 != null)
         {
            try
            {
               conn3.close();
            }
            catch (Exception e)
            {
               //Ignore
            }
         }               
      }       
   }
   
   /* A simple send in a transaction - recovered after restarting the server */
   public void testSimpleTransactionalSendRecoveryWithRestart() throws Exception
   {
      log.trace("starting testSimpleTransactionalDeliveryRecoveryWithRestart");
      
      XAConnection conn1 = null;
      
      Connection conn2 = null;
      
      XAConnection conn3 = null;
      
      try
      {
         conn1 = cf.createXAConnection();
   
         XASession sess1 = conn1.createXASession();
   
         XAResource res1 = sess1.getXAResource();
   
         //Pretend to be a transaction manager by interacting through the XAResources
         Xid xid1 = new XidImpl("bq1".getBytes(), 42, "eemeli".getBytes());
   
         log.trace("Sending message");
         
         //Send message in tx
         
         res1.start(xid1, XAResource.TMNOFLAGS);
   
         MessageProducer prod1 = sess1.createProducer(queue4);
   
         TextMessage tm1 = sess1.createTextMessage("tm1");
   
         prod1.send(tm1);
         
         res1.end(xid1, XAResource.TMSUCCESS);
         
         log.trace("Sent message");
   
         //prepare tx
   
         res1.prepare(xid1);
   
         log.trace("prepared tx");
         
         conn2 = cf.createConnection();
         
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons2 = sess2.createConsumer(queue4);
         
         conn2.start();
         
         //Verify message can't be received
         
         Message m = cons2.receive(1000);
         
         assertNull(m);
         
         conn1.close();
         
         conn2.close();
         
         conn1 = null;
         
         conn2 = null;
         
         // Now "crash" the server

         stopServerPeer();

         startServerPeer();
         
         deployAndLookupAdministeredObjects();
         
         conn3 = cf.createXAConnection();
         
         XASession sess3 = conn3.createXASession();
         
         XAResource res3 = sess3.getXAResource();
         
         log.trace("created connection");
         
         Xid[] xids = res3.recover(XAResource.TMSTARTRSCAN);
         assertEquals(1, xids.length);
         
         Xid[] xids2 = res3.recover(XAResource.TMENDRSCAN);
         assertEquals(0, xids2.length);

         assertEquals(xid1, xids[0]);
         
         log.trace("recovered");
         
         conn2 = cf.createConnection();
         
         sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         cons2 = sess2.createConsumer(queue4);
         
         conn2.start();
                  
         // Verify message still can't be received
         
         m = cons2.receive(1000);
         
         assertNull(m);
         
         log.trace("still can't see message");
         
         //Commit the tx
         
         res3.commit(xids[0], false);
         
         log.trace("committed");
         
         //The message should now be available
         
         TextMessage rm1 = (TextMessage)cons2.receive(1000);
         
         assertNotNull(rm1);
         
         assertEquals(tm1.getText(), rm1.getText());
   
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
         
         if (conn2 != null)
         {
            try
            {
               conn2.close();
            }
            catch (Exception e)
            {
               //Ignore
            }
         }
         
         if (conn3 != null)
         {
            try
            {
               conn3.close();
            }
            catch (Exception e)
            {
               //Ignore
            }
         }               
      }        
   }
   
   
   
   
   /* A simple acknowledgement in a transaction, recovered without restart */
   public void testSimpleTransactionalAcknowledgementRecoveryWithoutRestart() throws Exception
   {
      log.trace("starting testSimpleTransactionalAcknowledgementRecovery");
      
      Connection conn1 = null;
      
      XAConnection conn2 = null;
      
      XAConnection conn3 = null;
      
      try
      {
         //First send a message to the queue
         conn1 = cf.createConnection();
         
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod = sess1.createProducer(queue4);
         
         TextMessage tm1 = sess1.createTextMessage("tm1");
         
         prod.send(tm1);
                           
         
         conn2 = cf.createXAConnection();
   
         XASession sess2 = conn2.createXASession();
   
         XAResource res1 = sess2.getXAResource();
   
         //Pretend to be a transaction manager by interacting through the XAResources
         Xid xid1 = new XidImpl("bq1".getBytes(), 42, "eemeli".getBytes());
         
         res1.start(xid1, XAResource.TMNOFLAGS);
         
         MessageConsumer cons = sess2.createConsumer(queue4);
         
         conn2.start();
         
         //Consume the message
         
         TextMessage rm1 = (TextMessage)cons.receive(1000);
         
         assertNotNull(rm1);
         
         assertEquals(tm1.getText(), rm1.getText());
         
         res1.end(xid1, XAResource.TMSUCCESS);
         
         //prepare the tx
         
         res1.prepare(xid1);
         
         
         //Now recover
         
         conn3 = cf.createXAConnection();
         
         XASession sess3 = conn3.createXASession();
         
         XAResource res3 = sess3.getXAResource();
         
         Xid[] xids = res3.recover(XAResource.TMSTARTRSCAN);
         assertEquals(1, xids.length);
         
         Xid[] xids2 = res3.recover(XAResource.TMENDRSCAN);
         assertEquals(0, xids2.length);

         assertEquals(xid1, xids[0]);
                  
         //Commit the tx
         
         res1.commit(xids[0], false);
         
         //The message should be acknowldged
         
         conn1.close();
         
         conn2.close();
         
         conn3.close();
         
         conn1 = cf.createConnection();
         
         sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons1 = sess1.createConsumer(queue4);
         
         conn1.start();
         
         Message m = cons1.receive(1000);
         
         assertNull(m);
         
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
         
         if (conn2 != null)
         {
            try
            {
               conn2.close();
            }
            catch (Exception e)
            {
               //Ignore
            }
         }
         
         if (conn3 != null)
         {
            try
            {
               conn3.close();
            }
            catch (Exception e)
            {
               //Ignore
            }
         }               
      }            
   }
   
   
   
   /* A simple acknowledgement in a transaction, recovered with restart */
   public void testSimpleTransactionalAcknowledgementRecoveryWithRestart() throws Exception
   {
      log.trace("starting testSimpleTransactionalAcknowledgementRecoveryWithRestart");
      
      Connection conn1 = null;
      
      XAConnection conn2 = null;
      
      XAConnection conn3 = null;
      
      try
      {
         //First send a message to the queue
         conn1 = cf.createConnection();
         
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod = sess1.createProducer(queue4);
         
         TextMessage tm1 = sess1.createTextMessage("tm1");
         
         prod.send(tm1);
                                    
         conn2 = cf.createXAConnection();
   
         XASession sess2 = conn2.createXASession();
   
         XAResource res1 = sess2.getXAResource();
   
         //Pretend to be a transaction manager by interacting through the XAResources
         Xid xid1 = new XidImpl("bq1".getBytes(), 42, "eemeli".getBytes());
         
         res1.start(xid1, XAResource.TMNOFLAGS);
         
         MessageConsumer cons = sess2.createConsumer(queue4);
         
         conn2.start();
         
         //Consume the message
         
         TextMessage rm1 = (TextMessage)cons.receive(1000);
         
         assertNotNull(rm1);
         
         assertEquals(tm1.getText(), rm1.getText());
         
         res1.end(xid1, XAResource.TMSUCCESS);
         
         //prepare the tx
         
         res1.prepare(xid1);
         
         conn1.close();
         
         conn2.close();
         
         conn1 = null;
         
         conn2 = null;
         
         // Now "crash" the server

         stopServerPeer();

         startServerPeer();
         
         deployAndLookupAdministeredObjects();  
         
         //Now recover
         
         conn3 = cf.createXAConnection();
         
         XASession sess3 = conn3.createXASession();
         
         XAResource res3 = sess3.getXAResource();
         
         Xid[] xids = res3.recover(XAResource.TMSTARTRSCAN);
         assertEquals(1, xids.length);
         
         Xid[] xids2 = res3.recover(XAResource.TMENDRSCAN);
         assertEquals(0, xids2.length);

         assertEquals(xid1, xids[0]);
                  
         //Commit the tx
         
         res3.commit(xids[0], false);
         
         //The message should be acknowldged
         
         conn3.close();
         
         conn1 = cf.createConnection();
         
         sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons1 = sess1.createConsumer(queue4);
         
         conn1.start();
         
         Message m = cons1.receive(1000);
         
         assertNull(m);
                       
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
         
         if (conn2 != null)
         {
            try
            {
               conn2.close();
            }
            catch (Exception e)
            {
               //Ignore
            }
         }
         
         if (conn3 != null)
         {
            try
            {
               conn3.close();
            }
            catch (Exception e)
            {
               //Ignore
            }
         }               
      }           
   }
   
           
   /*
    * In this test, we have 4 messages in a 2 durable subs on the same topic.
    * 
    * We ack them, then add four more messages
    * 
    * This test tests for the recovery when the same message is in multiple channels
    * 
    * We don't restart in this test
    * 
    */
   public void testRecoveryWithTwoDurableSubsWithoutRestart() throws Exception
   {
      Connection conn1 = null;
      
      XAConnection conn2 = null;
      
      XAConnection conn3 = null;
      
      try
      {
         conn1 = cf.createConnection();
         
         conn1.setClientID("wib1");
         
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod1 = sess1.createProducer(topic2);
         
         MessageConsumer sub1 = sess1.createDurableSubscriber((Topic)topic2, "sub1");
         
         MessageConsumer sub2 = sess1.createDurableSubscriber((Topic)topic2, "sub2");
         
         //send four messages
          
         TextMessage tm1 = sess1.createTextMessage("tm1");
         TextMessage tm2 = sess1.createTextMessage("tm2");
         TextMessage tm3 = sess1.createTextMessage("tm3");
         TextMessage tm4 = sess1.createTextMessage("tm4");
         
         prod1.send(tm1);
         prod1.send(tm2);
         prod1.send(tm3);         
         prod1.send(tm4);
         
         conn1.close();
         
         //The messages should now be in both durable subs
         
         conn2 = cf.createXAConnection();
         
         conn2.setClientID("wib1");
         
         conn2.start();
         
         XASession sess2 = conn2.createXASession();
         
         XAResource res = sess2.getXAResource();
         
         Xid xid1 = new XidImpl("bq1".getBytes(), 42, "eemeli".getBytes());
         
         res.start(xid1, XAResource.TMNOFLAGS);
         
         //Now send four more messages in a global tx
         
         MessageProducer prod2 = sess2.createProducer(topic2);
         
         TextMessage tm5 = sess2.createTextMessage("tm5");
         TextMessage tm6 = sess2.createTextMessage("tm6");
         TextMessage tm7 = sess2.createTextMessage("tm7");
         TextMessage tm8 = sess2.createTextMessage("tm8");
         
         prod2.send(tm5);
         prod2.send(tm6);
         prod2.send(tm7);
         prod2.send(tm8);

         //And consume the first four from each in the tx
         
         sub1 = sess2.createDurableSubscriber((Topic)topic2, "sub1");
         
         sub2 = sess2.createDurableSubscriber((Topic)topic2, "sub2");
         
         TextMessage rm1 = (TextMessage)sub1.receive(1000);
         assertNotNull(rm1);
         assertEquals(tm1.getText(), rm1.getText());
         
         TextMessage rm2 = (TextMessage)sub1.receive(1000);
         assertNotNull(rm2);
         assertEquals(tm2.getText(), rm2.getText());
         
         TextMessage rm3 = (TextMessage)sub1.receive(1000);
         assertNotNull(rm3);
         assertEquals(tm3.getText(), rm3.getText());
         
         TextMessage rm4 = (TextMessage)sub1.receive(1000);
         assertNotNull(rm4);
         assertEquals(tm4.getText(), rm4.getText());
         
         Message m = sub1.receive(1000);
         
         assertNull(m);
                       
         rm1 = (TextMessage)sub2.receive(1000);
         assertNotNull(rm1);
         assertEquals(tm1.getText(), rm1.getText());
         
         rm2 = (TextMessage)sub2.receive(1000);
         assertNotNull(rm2);
         assertEquals(tm2.getText(), rm2.getText());
         
         rm3 = (TextMessage)sub2.receive(1000);
         assertNotNull(rm3);
         assertEquals(tm3.getText(), rm3.getText());
         
         rm4 = (TextMessage)sub2.receive(1000);
         assertNotNull(rm4);
         assertEquals(tm4.getText(), rm4.getText());
         
         m = sub2.receive(1000);
         
         assertNull(m);
         
         res.end(xid1, XAResource.TMSUCCESS);
         
         //prepare it
         
         res.prepare(xid1);
                  
         conn3 = cf.createXAConnection();
         
         XASession sess3 = conn3.createXASession();
         
         XAResource res3 = sess3.getXAResource();
         
         //recover
         
         Xid[] xids = res3.recover(XAResource.TMSTARTRSCAN);
         assertEquals(1, xids.length);
         
         Xid[] xids2 = res3.recover(XAResource.TMENDRSCAN);
         assertEquals(0, xids2.length);

         assertEquals(xid1, xids[0]);
         
         log.trace("Committing the tx");
         
         //Commit
         res3.commit(xids[0], false);
         
         log.trace("committed the tx");
         
         conn2.close();
         
         
         conn1 = cf.createConnection();
         
         conn1.setClientID("wib1");
         
         conn1.start();
         
         sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         //Should now see the last 4 messages
         
         sub1 = sess1.createDurableSubscriber((Topic)topic2, "sub1");
         
         sub2 = sess1.createDurableSubscriber((Topic)topic2, "sub2");
         
         TextMessage rm5 = (TextMessage)sub1.receive(1000);
         assertNotNull(rm5);
         assertEquals(tm5.getText(), rm5.getText());
         
         TextMessage rm6 = (TextMessage)sub1.receive(1000);
         assertNotNull(rm6);
         assertEquals(tm6.getText(), rm6.getText());
         
         TextMessage rm7 = (TextMessage)sub1.receive(1000);
         assertNotNull(rm7);
         assertEquals(tm7.getText(), rm7.getText());
         
         TextMessage rm8 = (TextMessage)sub1.receive(1000);
         assertNotNull(rm8);
         assertEquals(tm8.getText(), rm8.getText());
         
         m = sub1.receive(1000);
         
         assertNull(m);
                       
         rm5 = (TextMessage)sub2.receive(1000);
         assertNotNull(rm5);
         assertEquals(tm5.getText(), rm5.getText());
         
         rm6 = (TextMessage)sub2.receive(1000);
         assertNotNull(rm6);
         assertEquals(tm6.getText(), rm6.getText());
         
         rm7 = (TextMessage)sub2.receive(1000);
         assertNotNull(rm7);
         assertEquals(tm7.getText(), rm7.getText());
         
         rm8 = (TextMessage)sub2.receive(1000);
         assertNotNull(rm8);
         assertEquals(tm8.getText(), rm8.getText());
         
         m = sub2.receive(1000);
         
         assertNull(m);
         
         sub1.close();
         
         sub2.close();
         
         sess1.unsubscribe("sub1");
         
         sess1.unsubscribe("sub2");
         
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
         
         if (conn2 != null)
         {
            try
            {
               conn2.close();
            }
            catch (Exception e)
            {
               //Ignore
            }
         }
         
         if (conn3 != null)
         {
            try
            {
               conn3.close();
            }
            catch (Exception e)
            {
               //Ignore
            }
         }               
      }
   }
   
   
   
   /*
    * In this test, we have 4 messages in a 2 durable subs on the same topic.
    * 
    * We ack them, then add four more messages
    * 
    * This test tests for the recovery when the same message is in multiple channels
    * 
    * We do restart in this test
    * 
    */
   public void testRecoveryWithTwoDurableSubsWithRestart() throws Exception
   {
      Connection conn1 = null;
      
      XAConnection conn2 = null;
      
      XAConnection conn3 = null;
      
      try
      {
         conn1 = cf.createConnection();
         
         conn1.setClientID("wib1");
         
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod1 = sess1.createProducer(topic2);
         
         MessageConsumer sub1 = sess1.createDurableSubscriber((Topic)topic2, "sub1");
         
         MessageConsumer sub2 = sess1.createDurableSubscriber((Topic)topic2, "sub2");
         
         //send four messages
          
         TextMessage tm1 = sess1.createTextMessage("tm1");
         TextMessage tm2 = sess1.createTextMessage("tm2");
         TextMessage tm3 = sess1.createTextMessage("tm3");
         TextMessage tm4 = sess1.createTextMessage("tm4");
         
         prod1.send(tm1);
         prod1.send(tm2);
         prod1.send(tm3);         
         prod1.send(tm4);
         
         conn1.close();
         
         //The messages should now be in both durable subs
         
         conn2 = cf.createXAConnection();
         
         conn2.setClientID("wib1");
         
         conn2.start();
         
         XASession sess2 = conn2.createXASession();
         
         XAResource res = sess2.getXAResource();
         
         Xid xid1 = new XidImpl("bq1".getBytes(), 42, "eemeli".getBytes());
         
         res.start(xid1, XAResource.TMNOFLAGS);
         
         //Now send four more messages in a global tx
         
         MessageProducer prod2 = sess2.createProducer(topic2);
         
         TextMessage tm5 = sess2.createTextMessage("tm5");
         TextMessage tm6 = sess2.createTextMessage("tm6");
         TextMessage tm7 = sess2.createTextMessage("tm7");
         TextMessage tm8 = sess2.createTextMessage("tm8");
         
         prod2.send(tm5);
         prod2.send(tm6);
         prod2.send(tm7);
         prod2.send(tm8);

         //And consume the first four from each in the tx
         
         sub1 = sess2.createDurableSubscriber((Topic)topic2, "sub1");
         
         sub2 = sess2.createDurableSubscriber((Topic)topic2, "sub2");
         
         TextMessage rm1 = (TextMessage)sub1.receive(1000);
         assertNotNull(rm1);
         assertEquals(tm1.getText(), rm1.getText());
         
         TextMessage rm2 = (TextMessage)sub1.receive(1000);
         assertNotNull(rm2);
         assertEquals(tm2.getText(), rm2.getText());
         
         TextMessage rm3 = (TextMessage)sub1.receive(1000);
         assertNotNull(rm3);
         assertEquals(tm3.getText(), rm3.getText());
         
         TextMessage rm4 = (TextMessage)sub1.receive(1000);
         assertNotNull(rm4);
         assertEquals(tm4.getText(), rm4.getText());
         
         Message m = sub1.receive(1000);
         
         assertNull(m);
                       
         rm1 = (TextMessage)sub2.receive(1000);
         assertNotNull(rm1);
         assertEquals(tm1.getText(), rm1.getText());
         
         rm2 = (TextMessage)sub2.receive(1000);
         assertNotNull(rm2);
         assertEquals(tm2.getText(), rm2.getText());
         
         rm3 = (TextMessage)sub2.receive(1000);
         assertNotNull(rm3);
         assertEquals(tm3.getText(), rm3.getText());
         
         rm4 = (TextMessage)sub2.receive(1000);
         assertNotNull(rm4);
         assertEquals(tm4.getText(), rm4.getText());
         
         m = sub2.receive(1000);
         
         assertNull(m);
         
         res.end(xid1, XAResource.TMSUCCESS);
         
         //prepare it
         
         res.prepare(xid1);
         
         conn1.close();
         conn2.close();
         conn1 = null;
         
         conn2 = null;
         
         // Now "crash" the server

         stopServerPeer();

         startServerPeer();
         
         deployAndLookupAdministeredObjects();
                           
         conn3 = cf.createXAConnection();
         
         XASession sess3 = conn3.createXASession();
         
         XAResource res3 = sess3.getXAResource();
         
         //recover
         
         Xid[] xids = res3.recover(XAResource.TMSTARTRSCAN);
         assertEquals(1, xids.length);
         
         Xid[] xids2 = res3.recover(XAResource.TMENDRSCAN);
         assertEquals(0, xids2.length);

         assertEquals(xid1, xids[0]);
         
         log.trace("Committing the tx");
         
         //Commit
         res3.commit(xids[0], false);
         
         log.trace("committed the tx");
                           
         conn1 = cf.createConnection();
         
         conn1.setClientID("wib1");
         
         conn1.start();
         
         sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         //Should now see the last 4 messages
         
         sub1 = sess1.createDurableSubscriber((Topic)topic2, "sub1");
         
         sub2 = sess1.createDurableSubscriber((Topic)topic2, "sub2");
         
         TextMessage rm5 = (TextMessage)sub1.receive(1000);
         assertNotNull(rm5);
         assertEquals(tm5.getText(), rm5.getText());
         
         TextMessage rm6 = (TextMessage)sub1.receive(1000);
         assertNotNull(rm6);
         assertEquals(tm6.getText(), rm6.getText());
         
         TextMessage rm7 = (TextMessage)sub1.receive(1000);
         assertNotNull(rm7);
         assertEquals(tm7.getText(), rm7.getText());
         
         TextMessage rm8 = (TextMessage)sub1.receive(1000);
         assertNotNull(rm8);
         assertEquals(tm8.getText(), rm8.getText());
         
         m = sub1.receive(1000);
         
         assertNull(m);
                       
         rm5 = (TextMessage)sub2.receive(1000);
         assertNotNull(rm5);
         assertEquals(tm5.getText(), rm5.getText());
         
         rm6 = (TextMessage)sub2.receive(1000);
         assertNotNull(rm6);
         assertEquals(tm6.getText(), rm6.getText());
         
         rm7 = (TextMessage)sub2.receive(1000);
         assertNotNull(rm7);
         assertEquals(tm7.getText(), rm7.getText());
         
         rm8 = (TextMessage)sub2.receive(1000);
         assertNotNull(rm8);
         assertEquals(tm8.getText(), rm8.getText());
         
         m = sub2.receive(1000);
         
         assertNull(m);
         
         sub1.close();
         
         sub2.close();
         
         sess1.unsubscribe("sub1");
         
         sess1.unsubscribe("sub2");         

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
         
         if (conn2 != null)
         {
            try
            {
               conn2.close();
            }
            catch (Exception e)
            {
               //Ignore
            }
         }
         
         if (conn3 != null)
         {
            try
            {
               conn3.close();
            }
            catch (Exception e)
            {
               //Ignore
            }
         }               
      }
   }
         
   

   /*
    * Send two messages in two transactions.
    * Prepare tx
    * Crash the server
    * Restart the server
    * Make sure the messages can be received
    * NOTE this test only tests transactional sends, not transactional acknowledgments 
    * 
    */
   public void testTransactionalDeliveryRecovery() throws Exception
   {
      log.trace("starting testTransactionalDeliveryRecovery");
      
      XAConnection conn1 = null;

      XAConnection conn2 = null;
      
      Connection conn3 = null;
      
      try
      {      
         conn1 = cf.createXAConnection();
   
         conn2 = cf.createXAConnection();         
            
         XASession sess1 = conn1.createXASession();
   
         XASession sess2 = conn2.createXASession();
   
         XAResource res1 = sess1.getXAResource();
   
         XAResource res2 = sess2.getXAResource();
   
         //Pretend to be a transaction manager by interacting through the XAResources
         Xid xid1 = new XidImpl("bq1".getBytes(), 42, "eemeli".getBytes());
         Xid xid2 = new XidImpl("bq2".getBytes(), 42, "frigtard".getBytes());
   
         log.trace("Sending messages");
         
         //Send two messages in transaction 1
         
         res1.start(xid1, XAResource.TMNOFLAGS);
   
         MessageProducer prod1 = sess1.createProducer(queue4);
   
         TextMessage tm1 = sess1.createTextMessage("tm1");
   
         prod1.send(tm1);
   
         TextMessage tm2 = sess1.createTextMessage("tm2");
   
         prod1.send(tm2);
   
         res1.end(xid1, XAResource.TMSUCCESS);
   
         //Send two messages in transaction 2
         
         res2.start(xid2, XAResource.TMNOFLAGS);
   
         MessageProducer prod2 = sess2.createProducer(queue4);
   
         TextMessage tm3 = sess2.createTextMessage("tm3");
   
         prod2.send(tm3);
   
         TextMessage tm4 = sess2.createTextMessage("tm4");
   
         prod2.send(tm4);
   
         res2.end(xid2, XAResource.TMSUCCESS);
         
         log.trace("Sent messages");
   
         //prepare both txs
   
         res1.prepare(xid1);
         res2.prepare(xid2);
         
         log.trace("prepared messages");
   
         //Now "crash" the server
         
         conn1.close();
         
         conn2.close();
         
         conn1 = null;
         
         conn2 = null;
   
         stopServerPeer();
   
         startServerPeer();
   
         deployAndLookupAdministeredObjects();       
         
         conn1 = cf.createXAConnection();
   
         XAResource res = conn1.createXASession().getXAResource();
   
         log.trace("Recovering");
         
         Xid[] xids = res.recover(XAResource.TMSTARTRSCAN);
         assertEquals(2, xids.length);
         
         Xid[] xids2 = res.recover(XAResource.TMENDRSCAN);
         assertEquals(0, xids2.length);
   
         //They may be in a different order
         assertTrue(xids[0].equals(xid1) || xids[1].equals(xid1));
         assertTrue(xids[0].equals(xid2) || xids[1].equals(xid2));
         
         //Make sure can't receive the messages
         
         log.trace("Creating conn");
         
         conn3 = cf.createConnection();
   
         Session sessRec = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sessRec.createConsumer(queue4);
         conn3.start();
         
         log.trace("Created conn3");
   
         TextMessage m1 = (TextMessage)cons.receive(1000);
         assertNull(m1);
         
         log.trace("comitting");
   
         //Commit tx1
         
         res.commit(xid1, false);
         
         log.trace("comitted");
         
         
         //Should now be able to receive tm1 and tm2
         
         m1 = (TextMessage)cons.receive(MAX_TIMEOUT);
         assertNotNull(m1);
         
         assertEquals(tm1.getText(), m1.getText());
         
         TextMessage m2 = (TextMessage)cons.receive(MAX_TIMEOUT);
         assertNotNull(m2);
         
         assertEquals(tm2.getText(), m2.getText());
         
         TextMessage m3 = (TextMessage)cons.receive(1000);
         assertNull(m3);
         
         //Now commit tx2
         
         res.commit(xid2, false);
         
         //Should now be able to receive tm3 and tm4
         
         m3 = (TextMessage)cons.receive(MAX_TIMEOUT);
         assertNotNull(m3);
         
         assertEquals(tm3.getText(), m3.getText());
         
         TextMessage m4 = (TextMessage)cons.receive(MAX_TIMEOUT);
         assertNotNull(m4);
         
         assertEquals(tm4.getText(), m4.getText());
         
         TextMessage m5 = (TextMessage)cons.receive(1000);
         assertNull(m5);
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
         
         if (conn2 != null)
         {
            try
            {
               conn2.close();
            }
            catch (Exception e)
            {
               //Ignore
            }
         }
         
         if (conn3 != null)
         {
            try
            {
               conn3.close();
            }
            catch (Exception e)
            {
               //Ignore
            }
         }               
      }
   }


   /*
    * This test sends some messages in a couple of txs, crashes then recovers
    * NOTE it does not test transactional acknowledgements
    */
   public void testMockCoordinatorRecovery() throws Exception
   {
      XAConnection conn1 = null;
      
      XAConnection conn2 = null;
      
      Connection conn3 = null;
      
      try
      {
      
         conn1 = cf.createXAConnection();
   
         conn2 = cf.createXAConnection();
   
         XASession sess1 = conn1.createXASession();
   
         XASession sess2 = conn2.createXASession();
   
         XAResource res1 = sess1.getXAResource();
   
         XAResource res2 = sess2.getXAResource();
   
         //Pretend to be a transaction manager by interacting through the XAResources
         Xid xid1 = new XidImpl("bq1".getBytes(), 42, "aapeli".getBytes());
         Xid xid2 = new XidImpl("bq2".getBytes(), 42, "belsebub".getBytes());
   
         //    Send a message in each tx
   
         res1.start(xid1, XAResource.TMNOFLAGS);
   
         MessageProducer prod1 = sess1.createProducer(queue2);
   
         TextMessage tm1 = sess1.createTextMessage("alpha");
   
         prod1.send(tm1);
   
         res1.end(xid1, XAResource.TMSUCCESS);
   
   
         res2.start(xid2, XAResource.TMNOFLAGS);
   
         MessageProducer prod2 = sess2.createProducer(queue2);
   
         TextMessage tm2 = sess2.createTextMessage("beta");
   
         prod2.send(tm2);
   
         res2.end(xid2, XAResource.TMSUCCESS);
   
         //prepare both txs
   
   
         res1.prepare(xid1);
         res2.prepare(xid2);
                     
         //Now "crash" the server
   
         stopServerPeer();
   
         startServerPeer();
   
         deployAndLookupAdministeredObjects();         
         
         conn1.close();
         
         conn2.close();
         
         conn1 = cf.createXAConnection();
   
         XAResource res = conn1.createXASession().getXAResource();
   
         Xid[] xids = res.recover(XAResource.TMSTARTRSCAN);
         assertEquals(2, xids.length);
   
         Xid[] xids2 = res.recover(XAResource.TMENDRSCAN);
         assertEquals(0, xids2.length);
   
         assertTrue(xids[0].equals(xid1) || xids[1].equals(xid1));
         assertTrue(xids[0].equals(xid2) || xids[1].equals(xid2));
   
         res.commit(xid1, false);
   
         res.commit(xid2, false);
   
   
         conn3 = cf.createConnection();
   
         Session sessRec = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sessRec.createConsumer(queue2);
         conn3.start();
   
         Message msg = cons.receive(MAX_TIMEOUT);
   
         TextMessage m1 = (TextMessage)msg;
         assertNotNull(m1);
   
         assertTrue("alpha".equals(m1.getText()) ||
                    "beta".equals(m1.getText()));
   
         TextMessage m2 = (TextMessage)cons.receive(MAX_TIMEOUT);
         assertNotNull(m2);
   
         assertTrue("alpha".equals(m2.getText()) ||
                    "beta".equals(m2.getText()));
   
         assertTrue(!tm1.getText().equals(tm2.getText()));
   
         Message nullMessage = cons.receive(MIN_TIMEOUT);
         assertTrue(nullMessage == null);

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
         
         if (conn2 != null)
         {
            try
            {
               conn2.close();
            }
            catch (Exception e)
            {
               //Ignore
            }
         }
         
         if (conn3 != null)
         {
            try
            {
               conn3.close();
            }
            catch (Exception e)
            {
               //Ignore
            }
         }    
      }

   }

   
   /*
    * This test sends some messages in a couple of txs, crashes then recovers
    * It uses the JBoss TS XId implementation - so we can show compatibility
    * NOTE it does not test transactional acknowledgements
    */
   public void testMockCoordinatorRecoveryWithJBossTSXids() throws Exception
   {
      XAConnection conn1 = null;
      
      XAConnection conn2 = null;
      
      Connection conn3 = null;
      
      try
      {
      
         conn1 = cf.createXAConnection();
   
         conn2 = cf.createXAConnection();

         XASession sess1 = conn1.createXASession();
   
         XASession sess2 = conn2.createXASession();
   
         XAResource res1 = sess1.getXAResource();
   
         XAResource res2 = sess2.getXAResource();
   
         //Pretend to be a transaction manager by interacting through the XAResources
         Xid xid1 = new XidImple(new Uid("cadaver"), new Uid("bq1"), 666);
         Xid xid2 = new XidImple(new Uid("dalidom"), new Uid("bq2"), 661);   // TODO
   
   
         //    Send a message in each tx
   
         res1.start(xid1, XAResource.TMNOFLAGS);
   
         MessageProducer prod1 = sess1.createProducer(queue1);
   
         TextMessage tm1 = sess1.createTextMessage("testing1");
   
         prod1.send(tm1);
   
         res1.end(xid1, XAResource.TMSUCCESS);
   
   
         res2.start(xid2, XAResource.TMNOFLAGS);
   
         MessageProducer prod2 = sess2.createProducer(queue1);
   
         TextMessage tm2 = sess2.createTextMessage("testing2");
   
         prod2.send(tm2);
   
         res2.end(xid2, XAResource.TMSUCCESS);
   
         //prepare both txs
   
   
         res1.prepare(xid1);
         res2.prepare(xid2);
   
         //Now "crash" the server
   
         stopServerPeer();
   
         startServerPeer();
   
         deployAndLookupAdministeredObjects();  
         
         conn1.close();
         
         conn2.close();
         
         conn1 = cf.createXAConnection();
   
         XAResource res = conn1.createXASession().getXAResource();
   
         Xid[] xids = res.recover(XAResource.TMSTARTRSCAN);
         assertEquals(2, xids.length);
   
         Xid[] xids2 = res.recover(XAResource.TMENDRSCAN);
         assertEquals(0, xids2.length);
   
         assertTrue(xids[0].equals(xid1) || xids[1].equals(xid1));
         assertTrue(xids[0].equals(xid2) || xids[1].equals(xid2));
     
         res.commit(xid1, false);
   
         res.commit(xid2, false);
      
         conn3 = cf.createConnection();
   
         Session sessRec = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sessRec.createConsumer(queue1);
         conn3.start();
   
         TextMessage m1 = (TextMessage)cons.receive(MAX_TIMEOUT);
         assertNotNull(m1);
         assertEquals("testing1", m1.getText());
   
         TextMessage m2 = (TextMessage)cons.receive(MAX_TIMEOUT);
         assertNotNull(m2);
   
         assertEquals("testing2", m2.getText());

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
         
         if (conn2 != null)
         {
            try
            {
               conn2.close();
            }
            catch (Exception e)
            {
               //Ignore
            }
         }
         
         if (conn3 != null)
         {
            try
            {
               conn3.close();
            }
            catch (Exception e)
            {
               //Ignore
            }
         }    
      }
   }
     

   public void testMockCoordinatorRecovery3() throws Exception
   {
      XAConnection conn1 = null;
      
      XAConnection conn2 = null;
      
      Connection conn3 = null;
      
      try
      {
      
         conn1 = cf.createXAConnection();
   
         conn2 = cf.createXAConnection();

         XASession sess1 = conn1.createXASession();
   
         XASession sess2 = conn2.createXASession();
   
         XAResource res1 = sess1.getXAResource();
   
         XAResource res2 = sess2.getXAResource();
   
         //Pretend to be a transaction manager by interacting through the XAResources
         Xid xid1 = new XidImpl("bq1".getBytes(), 123, "gbtxid1".getBytes());
         Xid xid2 = new XidImpl("bq2".getBytes(), 124, "gbtxid2".getBytes());
   
         //    Send a message in each tx
   
         res1.start(xid1, XAResource.TMNOFLAGS);
   
         MessageProducer prod1 = sess1.createProducer(queue1);
   
         TextMessage tm1 = sess1.createTextMessage("testing1");
   
         prod1.send(tm1);
   
         res1.end(xid1, XAResource.TMSUCCESS);
   
   
         res2.start(xid2, XAResource.TMNOFLAGS);
   
         MessageProducer prod2 = sess2.createProducer(queue1);
   
         TextMessage tm2 = sess2.createTextMessage("testing2");
   
         prod2.send(tm2);
   
         res2.end(xid2, XAResource.TMSUCCESS);
   
         //prepare both txs
   
   
         res1.prepare(xid1);
         res2.prepare(xid2);
   
         //Now "crash" the server
   
         stopServerPeer();
   
         startServerPeer();
   
         deployAndLookupAdministeredObjects();      
         
         conn1.close();
         
         conn2.close();
         
         conn1 = cf.createXAConnection();
   
         XAResource res = conn1.createXASession().getXAResource();
   
         Xid[] xids = res.recover(XAResource.TMSTARTRSCAN);
         assertEquals(2, xids.length);
   
         Xid[] xids2 = res.recover(XAResource.TMENDRSCAN);
         assertEquals(0, xids2.length);
   
         assertTrue(xids[0].equals(xid1) || xids[1].equals(xid1));
         assertTrue(xids[0].equals(xid2) || xids[1].equals(xid2));
   
         res.commit(xids[0], false);
   
         res.commit(xids[1], false);
      
         conn3 = cf.createConnection();
   
         Session sessRec = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sessRec.createConsumer(queue1);
         conn3.start();
   
         TextMessage m1 = (TextMessage)cons.receive(MAX_TIMEOUT);
         assertNotNull(m1);
         assertEquals("testing1", m1.getText());
   
         TextMessage m2 = (TextMessage)cons.receive(MAX_TIMEOUT);
         assertNotNull(m2);
   
         assertEquals("testing2", m2.getText());
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
         
         if (conn2 != null)
         {
            try
            {
               conn2.close();
            }
            catch (Exception e)
            {
               //Ignore
            }
         }
         
         if (conn3 != null)
         {
            try
            {
               conn3.close();
            }
            catch (Exception e)
            {
               //Ignore
            }
         }    
      }
   }


   public void testMultiChannelRecovery() throws Exception
   {
      XAConnection conn1 = null;
      
      XAConnection conn2 = null;
      
      Connection conn3 = null;
      
      try
      {
      
         conn1 = cf.createXAConnection();
   
         conn2 = cf.createXAConnection();

         XASession sess1 = conn1.createXASession();
   
         XASession sess2 = conn2.createXASession();
   
         XAResource res1 = sess1.getXAResource();
   
         XAResource res2 = sess2.getXAResource();
   
         //Pretend to be a transaction manager by interacting through the XAResources
         Xid xid1 = new XidImpl("bq1".getBytes(), 123, "gbtxid1".getBytes());
         Xid xid2 = new XidImpl("bq2".getBytes(), 124, "gbtxid2".getBytes());
   
         //    Send a message in each tx
   
         res1.start(xid1, XAResource.TMNOFLAGS);
   
         MessageProducer prod1 = sess1.createProducer(queue2);
         MessageProducer prod2 = sess1.createProducer(queue3);
   
         TextMessage tm1 = sess1.createTextMessage("testing1");
         TextMessage tm2 = sess1.createTextMessage("testing2");
   
         prod1.send(tm1);
         prod2.send(tm2);
   
         res1.end(xid1, XAResource.TMSUCCESS);
   
   
         res2.start(xid2, XAResource.TMNOFLAGS);
   
         MessageProducer prod3 = sess2.createProducer(queue1);
   
         TextMessage tm3 = sess2.createTextMessage("testing3");
   
         prod3.send(tm3);
   
         res2.end(xid2, XAResource.TMSUCCESS);
   
         //prepare both txs
   
   
         res1.prepare(xid1);
         res2.prepare(xid2);
   
         //Now "crash" the server
   
         stopServerPeer();
   
         startServerPeer();
   
         deployAndLookupAdministeredObjects();
         
         conn1.close();
         
         conn2.close();
          
         conn1 = cf.createXAConnection();
         
         XAResource res = conn1.createXASession().getXAResource();
   
         Xid[] xids = res.recover(XAResource.TMSTARTRSCAN);
         assertEquals(2, xids.length);
   
         Xid[] xids2 = res.recover(XAResource.TMENDRSCAN);
         assertEquals(0, xids2.length);
   
         assertTrue(xids[0].equals(xid1) || xids[1].equals(xid1));
         assertTrue(xids[0].equals(xid2) || xids[1].equals(xid2));
   
         res.commit(xids[0], false);
   
         res.commit(xids[1], false);
   
         conn3 = cf.createConnection();
   
         Session sessRec = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);
   
         MessageConsumer cons1 = sessRec.createConsumer(queue2);
         MessageConsumer cons2 = sessRec.createConsumer(queue3);
         MessageConsumer cons3 = sessRec.createConsumer(queue1);
   
         conn3.start();
   
         TextMessage m1 = (TextMessage)cons1.receive(MAX_TIMEOUT);
         assertNotNull(m1);
         assertEquals("testing1", m1.getText());
   
         TextMessage m2 = (TextMessage)cons2.receive(MAX_TIMEOUT);
         assertNotNull(m2);
         assertEquals("testing2", m2.getText());
   
         TextMessage m3 = (TextMessage)cons3.receive(MAX_TIMEOUT);
         assertNotNull(m3);
         assertEquals("testing3", m3.getText());
         
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
         
         if (conn2 != null)
         {
            try
            {
               conn2.close();
            }
            catch (Exception e)
            {
               //Ignore
            }
         }
         
         if (conn3 != null)
         {
            try
            {
               conn3.close();
            }
            catch (Exception e)
            {
               //Ignore
            }
         }    
      }
   }
}

