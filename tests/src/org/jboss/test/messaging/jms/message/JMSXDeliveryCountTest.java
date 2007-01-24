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
package org.jboss.test.messaging.jms.message;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
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
import org.jboss.test.messaging.tools.jmx.ServiceContainer;
import org.jboss.tm.TransactionManagerLocator;

/**
 * 
 * A JMSXDeliveryCountTest

 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class JMSXDeliveryCountTest extends MessagingTestCase
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------
   
   // Attributes -----------------------------------------------------------------------------------
   
   protected JBossConnectionFactory cf;
   protected Queue queue;
   protected Topic topic;
   
   protected ServiceContainer sc;

   // Constructors ---------------------------------------------------------------------------------

   public JMSXDeliveryCountTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      ServerManagement.start("all");
      
      
      InitialContext initialContext = new InitialContext(ServerManagement.getJNDIEnvironment());
      cf = (JBossConnectionFactory)initialContext.lookup("/ConnectionFactory");
      
      ServerManagement.undeployQueue("Queue");
      ServerManagement.deployQueue("Queue");
      
      ServerManagement.undeployTopic("Topic");
      ServerManagement.deployTopic("Topic");
      
      queue = (Queue)initialContext.lookup("/queue/Queue");
      
      this.drainDestination(cf, queue);
      
      topic = (Topic)initialContext.lookup("/topic/Topic");
      
      if (ServerManagement.isRemote())
      {
         // We need to start a service container otherwise transaction manager jndi lookup
         // will fail
         sc = new ServiceContainer("transaction");
         
         sc.start(false);
      }
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
      
      if (ServerManagement.isRemote())
      {
         sc.stop();
      }
   }

   public void testSimpleJMSXDeliveryCount() throws Exception
   {
      Connection conn = cf.createConnection();
      Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer p = s.createProducer(queue);
      p.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

      p.send(s.createTextMessage("xoxo"));

      s.close();

      s = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      MessageConsumer c = s.createConsumer(queue);

      conn.start();

      TextMessage tm = (TextMessage)c.receive(1000);

      assertEquals("xoxo", tm.getText());
      assertEquals(1, tm.getIntProperty("JMSXDeliveryCount"));

      s.recover();

      tm = (TextMessage)c.receive(1000);

      assertEquals("xoxo", tm.getText());
      assertEquals(2, tm.getIntProperty("JMSXDeliveryCount"));

      conn.close();
   }

   public void testRedeliveryOnQueue() throws Exception
   {
      Connection conn = cf.createConnection();
      
      Session sess1 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageProducer prod = sess1.createProducer(queue);
      
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      final int NUM_MESSAGES = 1000;
      
      final int NUM_RECOVERIES = 8;
      
      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         TextMessage tm = sess1.createTextMessage();
         tm.setText("testing" + i);
         prod.send(tm);
      }
      
      Session sess2 = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      
      MessageConsumer cons = sess2.createConsumer(queue);
      
      conn.start();
     
      for (int j = 0; j < NUM_RECOVERIES; j++)
      {      
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons.receive(3000);
            assertNotNull(tm);
            assertEquals("testing" + i, tm.getText());
            assertEquals(j + 1, tm.getIntProperty("JMSXDeliveryCount"));
         }
         sess2.recover();
      }
      
      conn.close();
      
      
   }
   

   public void testRedeliveryOnTopic() throws Exception
   {
      Connection conn = cf.createConnection();
      
      conn.setClientID("myclientid");
      
      Session sess1 = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      Session sess2 = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      Session sess3 = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);

      MessageConsumer cons1 = sess1.createConsumer(topic);
      MessageConsumer cons2 = sess2.createConsumer(topic);
      MessageConsumer cons3 = sess3.createDurableSubscriber(topic, "subxyz");

      conn.start();
      
      final int NUM_MESSAGES = 300;
      final int NUM_RECOVERIES = 9;
      
      Receiver r1 = new Receiver("R1", sess1, cons1, NUM_MESSAGES, NUM_RECOVERIES);
      Receiver r2 = new Receiver("R2", sess2, cons2, NUM_MESSAGES, NUM_RECOVERIES);
      Receiver r3 = new Receiver("R3", sess3, cons3, NUM_MESSAGES, NUM_RECOVERIES);
      
      Thread t1 = new Thread(r1);
      Thread t2 = new Thread(r2);
      Thread t3 = new Thread(r3);
      
      t1.start();
      t2.start();
      t3.start();
      
      Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = sessSend.createProducer(topic);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      
      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         TextMessage tm1 = sessSend.createTextMessage("testing" + i);
         prod.send(tm1);
      }
      
      t1.join();
      t2.join();
      t3.join();
      
      assertFalse(r1.failed);
      assertFalse(r2.failed);
      assertFalse(r3.failed);
      
      conn.close();
   }
   
   public void testDeliveryCountUpdatedOnCloseTransacted() throws Exception
   {
      Connection conn = null;
      
      try
      {         
         conn = cf.createConnection();
   
         Session producerSess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = producerSess.createProducer(queue);
   
         Session consumerSess = conn.createSession(true, Session.SESSION_TRANSACTED);
         MessageConsumer consumer = consumerSess.createConsumer(queue);
         conn.start();
   
         TextMessage tm = producerSess.createTextMessage("message1");
         
         producer.send(tm);
         
         TextMessage rm = (TextMessage)consumer.receive(1000);
         
         assertNotNull(rm);
         
         assertEquals(tm.getText(), rm.getText());
         
         assertEquals(1, rm.getIntProperty("JMSXDeliveryCount"));
         
         assertFalse(rm.getJMSRedelivered());
         
         consumerSess.rollback();
         
         rm = (TextMessage)consumer.receive(1000);
         
         assertNotNull(rm);
         
         assertEquals(tm.getText(), rm.getText());
         
         assertEquals(2, rm.getIntProperty("JMSXDeliveryCount"));
         
         assertTrue(rm.getJMSRedelivered());
         
         consumerSess.rollback();
         
         rm = (TextMessage)consumer.receive(1000);
         
         assertNotNull(rm);
         
         assertEquals(tm.getText(), rm.getText());
         
         assertEquals(3, rm.getIntProperty("JMSXDeliveryCount"));
         
         assertTrue(rm.getJMSRedelivered());
         
         //Now close the session without committing
         
         log.info("Closing session");
         
         consumerSess.close();
         
         consumerSess = conn.createSession(true, Session.SESSION_TRANSACTED);
         
         consumer = consumerSess.createConsumer(queue);
         
         rm = (TextMessage)consumer.receive(1000);
         
         assertNotNull(rm);
         
         assertEquals(tm.getText(), rm.getText());
         
         assertEquals(4, rm.getIntProperty("JMSXDeliveryCount"));
         
         assertTrue(rm.getJMSRedelivered());
         
      }
      finally
      {      
         if (conn != null)
         {
            conn.close();
         }
      }
   }
   
   public void testDeliveryCountUpdatedOnCloseClientAck() throws Exception
   {
      Connection conn = null;
      
      try
      {         
         conn = cf.createConnection();
   
         Session producerSess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = producerSess.createProducer(queue);
   
         Session consumerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         MessageConsumer consumer = consumerSess.createConsumer(queue);
         conn.start();
   
         TextMessage tm = producerSess.createTextMessage("message1");
         
         producer.send(tm);
         
         TextMessage rm = (TextMessage)consumer.receive(1000);
         
         assertNotNull(rm);
         
         assertEquals(tm.getText(), rm.getText());
         
         assertEquals(1, rm.getIntProperty("JMSXDeliveryCount"));
         
         assertFalse(rm.getJMSRedelivered());
         
         consumerSess.recover();
         
         rm = (TextMessage)consumer.receive(1000);
         
         assertNotNull(rm);
         
         assertEquals(tm.getText(), rm.getText());
         
         assertEquals(2, rm.getIntProperty("JMSXDeliveryCount"));
         
         assertTrue(rm.getJMSRedelivered());
         
         consumerSess.recover();
         
         rm = (TextMessage)consumer.receive(1000);
         
         assertNotNull(rm);
         
         assertEquals(tm.getText(), rm.getText());
         
         assertEquals(3, rm.getIntProperty("JMSXDeliveryCount"));
         
         assertTrue(rm.getJMSRedelivered());
         
         //Now close the session without committing
         
         consumerSess.close();
         
         consumerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         
         consumer = consumerSess.createConsumer(queue);
         
         rm = (TextMessage)consumer.receive(1000);
         
         assertNotNull(rm);
         
         assertEquals(tm.getText(), rm.getText());
         
         assertEquals(4, rm.getIntProperty("JMSXDeliveryCount"));
         
         assertTrue(rm.getJMSRedelivered());
         
      }
      finally
      {      
         if (conn != null)
         {
            conn.close();
         }
      }
   }
   
   public void testDeliveryCountUpdatedOnCloseXA() throws Exception
   {
      XAConnection xaConn = null;
      
      Connection conn = null;
      
      TransactionManager mgr = TransactionManagerLocator.getInstance().locate();
      
      Transaction toResume = null;
      
      Transaction tx = null;
      
      try
      {         
         toResume = mgr.suspend();
         
         conn = cf.createConnection();
         
         //Send a message
         
         Session producerSess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = producerSess.createProducer(queue);
         
         TextMessage tm = producerSess.createTextMessage("message1");
         
         producer.send(tm);
         
         
         
         xaConn = ((XAConnectionFactory)cf).createXAConnection();
         
         XASession consumerSess = xaConn.createXASession();
         MessageConsumer consumer = consumerSess.createConsumer(queue);
         xaConn.start();
         
         DummyXAResource res = new DummyXAResource();
         
         mgr.begin();
         
         tx = mgr.getTransaction();
         
         tx.enlistResource(res);
         
         tx.enlistResource(consumerSess.getXAResource());
         
         TextMessage rm = (TextMessage)consumer.receive(1000);
         
         assertNotNull(rm);
         
         assertEquals(tm.getText(), rm.getText());
         
         assertEquals(1, rm.getIntProperty("JMSXDeliveryCount"));
         
         assertFalse(rm.getJMSRedelivered());
         
         tx.delistResource(res, XAResource.TMSUCCESS);
         
         tx.delistResource(consumerSess.getXAResource(), XAResource.TMSUCCESS);
         
         tx.rollback();
         
         mgr.begin();
         
         tx = mgr.getTransaction();
         
         tx.enlistResource(res);
         
         tx.enlistResource(consumerSess.getXAResource());
         
         rm = (TextMessage)consumer.receive(1000);
         
         assertNotNull(rm);
         
         assertEquals(tm.getText(), rm.getText());
         
         assertEquals(2, rm.getIntProperty("JMSXDeliveryCount"));
         
         assertTrue(rm.getJMSRedelivered());
         
         tx.delistResource(res, XAResource.TMSUCCESS);
         
         tx.delistResource(consumerSess.getXAResource(), XAResource.TMSUCCESS);
         
         tx.rollback();
         
         mgr.begin();
         
         tx = mgr.getTransaction();
         
         tx.enlistResource(res);
         
         tx.enlistResource(consumerSess.getXAResource());
            
         rm = (TextMessage)consumer.receive(1000);
         
         assertNotNull(rm);
         
         assertEquals(tm.getText(), rm.getText());
         
         assertEquals(3, rm.getIntProperty("JMSXDeliveryCount"));
         
         assertTrue(rm.getJMSRedelivered());
                  
         tx.delistResource(res, XAResource.TMSUCCESS);
         
         tx.delistResource(consumerSess.getXAResource(), XAResource.TMSUCCESS);
         
         tx.rollback();
         
         log.info("Closing the consumer");
         
         //Must close consumer first
         
         consumer.close();
         
         consumerSess.close();
         
         consumerSess = xaConn.createXASession();
         
         consumer = consumerSess.createConsumer(queue);
         
         mgr.begin();
         
         tx = mgr.getTransaction();
         
         tx.enlistResource(res);
         
         tx.enlistResource(consumerSess.getXAResource());
                           
         rm = (TextMessage)consumer.receive(1000);
         
         assertNotNull(rm);
         
         assertEquals(tm.getText(), rm.getText());
         
         assertEquals(4, rm.getIntProperty("JMSXDeliveryCount"));
         
         assertTrue(rm.getJMSRedelivered());
         
         tx.delistResource(res, XAResource.TMSUCCESS);
         
         tx.delistResource(consumerSess.getXAResource(), XAResource.TMSUCCESS);
         
      }
      finally
      {      
         if (conn != null)
         {
            try
            {
               conn.close();
            }
            catch (Exception ignore)
            {              
            }             
         }
         
         if (tx != null)
         {
            try
            {
               tx.commit();
            }
            catch (Exception ignore)
            {              
            }
         }
         
         if (toResume != null)
         {
            try
            {
               mgr.resume(toResume);
            }
            catch (Exception ignore)
            {              
            }
         }
      }
   }
   
   
   
   
   
   class Receiver implements Runnable
   {
      MessageConsumer cons;
      
      int numMessages;
      
      int numRecoveries;
      
      boolean failed;
      
      Session sess;
      
      String name;
      
      Receiver(String name, Session sess, MessageConsumer cons, int numMessages, int numRecoveries)
      {
         this.sess = sess;
         this.cons = cons;
         this.numMessages = numMessages;
         this.numRecoveries = numRecoveries;
         this.name = name;
      }
      
      public void run()
      {
         try
         {
            Message lastMessage = null;
            for (int j = 0; j < numRecoveries; j++)
            {
               
               for (int i = 0; i < numMessages; i++)
               {                  
                  TextMessage tm = (TextMessage)cons.receive();
                  lastMessage = tm;
                  
                  if (tm == null)
                  {                     
                     failed = true;
                  }
                  
                  if (!tm.getText().equals("testing" + i))
                  {
                     log.error("Out of order!!");
                     failed = true;
                  }

                  if (tm.getIntProperty("JMSXDeliveryCount") != (j + 1))
                  {
                     log.error("Delivery count not expected value:" + (j + 1) +
                               " actual:" + tm.getIntProperty("JMSXDeliveryCount"));;
                     failed = true;
                  }
               }
               if (j != numRecoveries -1)
               {
                  sess.recover();
               }
               
            }
            lastMessage.acknowledge();
         }
         catch (Exception e)
         {
            failed = true;
         }
      }
   }
   
   //TODO Check that delivery count is persisted properly, local to the channel
   //Currently we are not persisting delivery count on the channel, only on the message
   //So this won't work


   // Package protected ----------------------------------------------------------------------------
   
   // Protected ------------------------------------------------------------------------------------
   
   // Private --------------------------------------------------------------------------------------
   
   // Inner classes --------------------------------------------------------------------------------
   
   static class DummyXAResource implements XAResource
   {
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

