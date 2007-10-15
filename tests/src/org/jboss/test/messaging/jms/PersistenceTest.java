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

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.jboss.test.messaging.tools.ServerManagement;

/**
 * 
 * A PersistenceTest

 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class PersistenceTest extends JMSTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public PersistenceTest(String name)
   {
      super(name);
   }

   // TestCase overrides -------------------------------------------

   // Public --------------------------------------------------------

   /**
    * Test that the messages in a persistent queue survive starting and stopping and server,
    * 
    */
   public void testQueuePersistence() throws Exception
   {
      Connection conn = null;
      
      try
      {      
	      conn = cf.createConnection();
	      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      MessageProducer prod = sess.createProducer(queue1);
	      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
	      
	      for (int i = 0; i < 10; i++)
	      {
	         TextMessage tm = sess.createTextMessage("message" + i);
	         prod.send(tm);
	      }
	      
	      conn.close();
	      
	      ServerManagement.stopServerPeer();
	      
	      ServerManagement.startServerPeer();
	      
	      // Messaging server restart implies new ConnectionFactory lookup
	      deployAndLookupAdministeredObjects();
	      
	      conn = cf.createConnection();
	      sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      conn.start();
	      MessageConsumer cons = sess.createConsumer(queue1);
	      for (int i = 0; i < 10; i++)
	      {
	         TextMessage tm = (TextMessage)cons.receive(3000);
	         assertNotNull(tm);
	         if (tm == null)
	         {
	            break;
	         }
	         assertEquals("message" + i, tm.getText());
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
   
   /**
    * Test that the JMSRedelivered and delivery count survives a restart
    * 
    */
   public void testJMSRedeliveredRestart() throws Exception
   {
      Connection conn = null;
      
      try
      {      
	      conn = cf.createConnection();
	      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      MessageProducer prod = sess.createProducer(queue1);
	      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
	      
	      for (int i = 0; i < 10; i++)
	      {
	         TextMessage tm = sess.createTextMessage("message" + i);
	         prod.send(tm);
	      }
	      
	      Session sess2 = conn.createSession(true, Session.SESSION_TRANSACTED);
	      
	      MessageConsumer cons = sess2.createConsumer(queue1);
	      
	      conn.start();
	      
	      for (int i = 0; i < 10; i++)
	      {
	         TextMessage tm = (TextMessage)cons.receive(1000);
	         
	         assertNotNull(tm);
	         
	         assertEquals("message" + i, tm.getText());
	         
	         assertFalse(tm.getJMSRedelivered());
	         
	         assertEquals(1, tm.getIntProperty("JMSXDeliveryCount"));
	      }
	      
	      //rollback
	      sess2.rollback();
	      
	      for (int i = 0; i < 10; i++)
	      {
	         TextMessage tm = (TextMessage)cons.receive(1000);
	         
	         assertNotNull(tm);
	         
	         assertEquals("message" + i, tm.getText());
	         
	         assertTrue(tm.getJMSRedelivered());
	         
	         assertEquals(2, tm.getIntProperty("JMSXDeliveryCount"));
	      }
	      
	      conn.close();
	      	            	      
	      ServerManagement.stopServerPeer();
	      
	      ServerManagement.startServerPeer();
	      
	      // Messaging server restart implies new ConnectionFactory lookup
	      deployAndLookupAdministeredObjects();
	      
	      conn = cf.createConnection();
	      sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      conn.start();
	      cons = sess.createConsumer(queue1);
	      for (int i = 0; i < 10; i++)
	      {
	         TextMessage tm = (TextMessage)cons.receive(3000);
	         
	         assertNotNull(tm);
	         
	         assertEquals("message" + i, tm.getText());
	         
	         assertTrue(tm.getJMSRedelivered());
	         
	         assertEquals(3, tm.getIntProperty("JMSXDeliveryCount"));
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
   
   
   /**
    * First test that message order survives a restart 
    */
   public void testMessageOrderPersistence_1() throws Exception
   {
      Connection conn = null;
      
      try
      {      
	      conn = cf.createConnection();
	      Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      MessageProducer prod = sessSend.createProducer(queue1);
	      
	      TextMessage m0 = sessSend.createTextMessage("a");
	      TextMessage m1 = sessSend.createTextMessage("b");
	      TextMessage m2 = sessSend.createTextMessage("c");
	      TextMessage m3 = sessSend.createTextMessage("d");
	      TextMessage m4 = sessSend.createTextMessage("e");
	      TextMessage m5 = sessSend.createTextMessage("f");
	      TextMessage m6 = sessSend.createTextMessage("g");
	      TextMessage m7 = sessSend.createTextMessage("h");
	      TextMessage m8 = sessSend.createTextMessage("i");
	      TextMessage m9 = sessSend.createTextMessage("j"); 
	      
	      prod.send(m0, DeliveryMode.PERSISTENT, 0, 0);
	      prod.send(m1, DeliveryMode.PERSISTENT, 1, 0);
	      prod.send(m2, DeliveryMode.PERSISTENT, 2, 0);
	      prod.send(m3, DeliveryMode.PERSISTENT, 3, 0);
	      prod.send(m4, DeliveryMode.PERSISTENT, 4, 0);
	      prod.send(m5, DeliveryMode.PERSISTENT, 5, 0);
	      prod.send(m6, DeliveryMode.PERSISTENT, 6, 0);
	      prod.send(m7, DeliveryMode.PERSISTENT, 7, 0);
	      prod.send(m8, DeliveryMode.PERSISTENT, 8, 0);
	      prod.send(m9, DeliveryMode.PERSISTENT, 9, 0);
	      
	      conn.close();
	      
	      ServerManagement.stopServerPeer();
	
	      ServerManagement.startServerPeer();
	
	      // Messaging server restart implies new ConnectionFactory lookup
	      deployAndLookupAdministeredObjects();
	      
	      conn = cf.createConnection();
	      Session sessReceive = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      conn.start();
	      MessageConsumer cons = sessReceive.createConsumer(queue1);
	     
	      {
	         TextMessage t = (TextMessage)cons.receive(1000);
	         assertNotNull(t);
	         assertEquals("j", t.getText());
	      }
	      {
	         TextMessage t = (TextMessage)cons.receive(1000);
	         assertNotNull(t);
	         assertEquals("i", t.getText());
	      }
	      {
	         TextMessage t = (TextMessage)cons.receive(1000);
	         assertNotNull(t);
	         assertEquals("h", t.getText());
	      }
	      {
	         TextMessage t = (TextMessage)cons.receive(1000);
	         assertNotNull(t);
	         assertEquals("g", t.getText());
	      }
	      {
	         TextMessage t = (TextMessage)cons.receive(1000);
	         assertNotNull(t);
	         assertEquals("f", t.getText());
	      }
	      {
	         TextMessage t = (TextMessage)cons.receive(1000);
	         assertNotNull(t);
	         assertEquals("e", t.getText());
	      }
	      {
	         TextMessage t = (TextMessage)cons.receive(1000);
	         assertNotNull(t);
	         assertEquals("d", t.getText());
	      }
	      {
	         TextMessage t = (TextMessage)cons.receive(1000);
	         assertNotNull(t);
	         assertEquals("c", t.getText());
	      }
	      {
	         TextMessage t = (TextMessage)cons.receive(1000);
	         assertNotNull(t);
	         assertEquals("b", t.getText());
	      }
	      {
	         TextMessage t = (TextMessage)cons.receive(1000);
	         assertNotNull(t);
	         assertEquals("a", t.getText());
	      }
	      {
	         TextMessage t = (TextMessage)cons.receive(500);
	         assertNull(t);
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

   
   /**
    * Second test that message order survives a restart 
    */
   public void testMessageOrderPersistence_2() throws Exception
   {
      Connection conn = null;
      
      try
      {      
	      conn = cf.createConnection();
	      Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      MessageProducer prod = sessSend.createProducer(queue1);
	      
	      TextMessage m0 = sessSend.createTextMessage("a");
	      TextMessage m1 = sessSend.createTextMessage("b");
	      TextMessage m2 = sessSend.createTextMessage("c");
	      TextMessage m3 = sessSend.createTextMessage("d");
	      TextMessage m4 = sessSend.createTextMessage("e");
	      TextMessage m5 = sessSend.createTextMessage("f");
	      TextMessage m6 = sessSend.createTextMessage("g");
	      TextMessage m7 = sessSend.createTextMessage("h");
	      TextMessage m8 = sessSend.createTextMessage("i");
	      TextMessage m9 = sessSend.createTextMessage("j");
	
	      
	      prod.send(m0, DeliveryMode.PERSISTENT, 0, 0);
	      prod.send(m1, DeliveryMode.PERSISTENT, 0, 0);
	      prod.send(m2, DeliveryMode.PERSISTENT, 0, 0);
	      prod.send(m3, DeliveryMode.PERSISTENT, 3, 0);
	      prod.send(m4, DeliveryMode.PERSISTENT, 3, 0);
	      prod.send(m5, DeliveryMode.PERSISTENT, 4, 0);
	      prod.send(m6, DeliveryMode.PERSISTENT, 4, 0);
	      prod.send(m7, DeliveryMode.PERSISTENT, 5, 0);
	      prod.send(m8, DeliveryMode.PERSISTENT, 5, 0);
	      prod.send(m9, DeliveryMode.PERSISTENT, 6, 0);
	      
	      conn.close();
	      
	      ServerManagement.stopServerPeer();
	      
	      ServerManagement.startServerPeer();
	      
	      deployAndLookupAdministeredObjects();
	
	      conn = cf.createConnection();
	      Session sessReceive = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      conn.start();
	      MessageConsumer cons = sessReceive.createConsumer(queue1);
	     
	      {
	         TextMessage t = (TextMessage)cons.receive(1000);
	         assertNotNull(t);
	         assertEquals("j", t.getText());
	      }
	      {
	         TextMessage t = (TextMessage)cons.receive(1000);
	         assertNotNull(t);
	         assertEquals("h", t.getText());
	      }
	      {
	         TextMessage t = (TextMessage)cons.receive(1000);
	         assertNotNull(t);
	         assertEquals("i", t.getText());
	      }
	      {
	         TextMessage t = (TextMessage)cons.receive(1000);
	         assertNotNull(t);
	         assertEquals("f", t.getText());
	      }
	      {
	         TextMessage t = (TextMessage)cons.receive(1000);
	         assertNotNull(t);
	         assertEquals("g", t.getText());
	      }
	      {
	         TextMessage t = (TextMessage)cons.receive(1000);
	         assertNotNull(t);
	         assertEquals("d", t.getText());
	      }
	      {
	         TextMessage t = (TextMessage)cons.receive(1000);
	         assertNotNull(t);
	         assertEquals("e", t.getText());
	      }
	      {
	         TextMessage t = (TextMessage)cons.receive(1000);
	         assertNotNull(t);
	         assertEquals("a", t.getText());
	      }
	      {
	         TextMessage t = (TextMessage)cons.receive(1000);
	         assertNotNull(t);
	         assertEquals("b", t.getText());
	      }
	      {
	         TextMessage t = (TextMessage)cons.receive(1000);
	         assertNotNull(t);
	         assertEquals("c", t.getText());
	      }
	      {
	         TextMessage t = (TextMessage)cons.receiveNoWait();
	         assertNull(t);
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

   /*
    * Test durable subscription state survives a server crash
    */
   public void testDurableSubscriptionPersistence_1() throws Exception
   {
      Connection conn = null;
      
      try
      {      
	      conn = cf.createConnection();
	      conn.setClientID("five");
	
	      Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	
	      MessageConsumer ds = s.createDurableSubscriber(topic1, "sub", null, false);
	
	      MessageProducer p = s.createProducer(topic1);
	      p.setDeliveryMode(DeliveryMode.PERSISTENT);
	      TextMessage tm = s.createTextMessage("thebody");
	      
	      p.send(tm);
	      log.debug("message sent");
	
	      conn.close();
	
	      ServerManagement.stopServerPeer();
	
	      ServerManagement.startServerPeer();

	      deployAndLookupAdministeredObjects();
	
	      conn = cf.createConnection();
	      conn.setClientID("five");
	
	      s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      conn.start();
	
	      ds = s.createDurableSubscriber(topic1, "sub", null, false);
	
	      TextMessage rm = (TextMessage)ds.receive(3000);
	      assertNotNull(rm);
	      assertEquals("thebody", rm.getText());
	      
	      ds.close();
	      
	      s.unsubscribe("sub");
      }
      finally
      {
      	if (conn != null)
      	{
      		conn.close();
      	}
      }
   }

   /*
    * Test durable subscription state survives a restart
    */
   public void testDurableSubscriptionPersistence_2() throws Exception
   {
      Connection conn = null;
      
      try
      {      
	      conn = cf.createConnection();
	      conn.setClientID("Sausages");
	      
	      Session sessConsume = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      
	      MessageConsumer sub1 = sessConsume.createDurableSubscriber(topic1, "sub1", null, false);
	      MessageConsumer sub2 = sessConsume.createDurableSubscriber(topic1, "sub2", null, false);
	      MessageConsumer sub3 = sessConsume.createDurableSubscriber(topic1, "sub3", null, false);
	      
	      
	      Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      MessageProducer prod = sessSend.createProducer(topic1);
	      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
	      
	      for (int i = 0; i < 10; i++)
	      {
	         TextMessage tm = sessSend.createTextMessage("message" + i);
	         prod.send(tm);
	      }
	      
	      conn.close();
	      
	      ServerManagement.stopServerPeer();
	
	      ServerManagement.startServerPeer();
	
	      // Messaging server restart implies new ConnectionFactory lookup
	      deployAndLookupAdministeredObjects();
	      
	      conn = cf.createConnection();
	      conn.setClientID("Sausages");
	      
	      sessConsume = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      conn.start();
	      
	      sub1 = sessConsume.createDurableSubscriber(topic1, "sub1", null, false);
	      sub2 = sessConsume.createDurableSubscriber(topic1, "sub2", null, false);
	      sub3 = sessConsume.createDurableSubscriber(topic1, "sub3", null, false);
	                  
	      for (int i = 0; i < 10; i++)
	      {
	         TextMessage tm1 = (TextMessage)sub1.receive(3000);
	         assertNotNull(tm1);
	         if (tm1 == null)
	         {
	            break;
	         }
	         assertEquals("message" + i, tm1.getText());
	         
	         TextMessage tm2 = (TextMessage)sub2.receive(3000);
	         assertNotNull(tm2);
	         if (tm2 == null)
	         {
	            break;
	         }
	         assertEquals("message" + i, tm2.getText());
	         
	         TextMessage tm3 = (TextMessage)sub3.receive(3000);
	         assertNotNull(tm3);
	         if (tm3 == null)
	         {
	            break;
	         }
	         assertEquals("message" + i, tm3.getText());
	      }
	      
	      sub1.close();
	      sub2.close();
	      sub3.close();
	      
	      sessConsume.unsubscribe("sub1");
	      sessConsume.unsubscribe("sub2");
	      sessConsume.unsubscribe("sub3");
      }
      finally
      {
      	if (conn != null)
      	{
      		conn.close();
      	}
      }
   }
   

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

}
