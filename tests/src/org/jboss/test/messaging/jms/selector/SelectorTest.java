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
package org.jboss.test.messaging.jms.selector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.jboss.test.messaging.jms.JMSTestCase;

import EDU.oswego.cs.dl.util.concurrent.Latch;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class SelectorTest extends JMSTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public SelectorTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   /**
    * Test case for http://jira.jboss.org/jira/browse/JBMESSAGING-105
    *
    * Two Messages are sent to a queue. There is one receiver on the queue. The receiver only
    * receives one of the messages due to a message selector only matching one of them. The receiver
    * is then closed. A new receiver is now attached to the queue. Redelivery of the remaining
    * message is now attempted. The message should be redelivered.
    */
   public void testSelectiveClosingConsumer() throws Exception
   {
      Connection conn = null;
      
      try
      {      
	      conn = cf.createConnection();
	      conn.start();
	
	      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      MessageProducer prod = session.createProducer(queue1);
	      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
	
	      String selector = "color = 'red'";
	      MessageConsumer redConsumer = session.createConsumer(queue1, selector);
	      conn.start();
	
	      Message redMessage = session.createMessage();
	      redMessage.setStringProperty("color", "red");
	
	      Message blueMessage = session.createMessage();
	      blueMessage.setStringProperty("color", "blue");
	
	      prod.send(redMessage);
	      prod.send(blueMessage);
	
	      Message rec = redConsumer.receive();
	      assertEquals(redMessage.getJMSMessageID(), rec.getJMSMessageID());
	      assertEquals("red", rec.getStringProperty("color"));
	
	      assertNull(redConsumer.receive(3000));
	
	      redConsumer.close();
	
	      MessageConsumer universalConsumer = session.createConsumer(queue1);
	
	      rec = universalConsumer.receive();
	
	      assertEquals(rec.getJMSMessageID(), blueMessage.getJMSMessageID());
	      assertEquals("blue", rec.getStringProperty("color"));
      }
      finally
      {
      	if (conn != null)
      	{
      		conn.close();
      	}
      }
   }

   public void testManyTopic() throws Exception
   {
      String selector1 = "beatle = 'john'";

      Connection conn = null;
      
      try
      {      
	      conn = cf.createConnection();
	      conn.start();
	
	      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	
	      MessageConsumer cons1 = sess.createConsumer(topic1, selector1);
	
	      MessageProducer prod = sess.createProducer(topic1);
	
	      for (int j = 0; j < 100; j++)
	      {
	         Message m = sess.createMessage();
	
	         m.setStringProperty("beatle", "john");
	
	         prod.send(m);
	
	         m = sess.createMessage();
	
	         m.setStringProperty("beatle", "kermit the frog");
	
	         prod.send(m);
	      }
	
	      for (int j = 0; j < 100; j++)
	      {
	         Message m = cons1.receive(1000);
	
	         assertNotNull(m);
	      }
	
	      Thread.sleep(500);
	
	      Message m = cons1.receiveNoWait();
	
	      assertNull(m);
      }
      finally
      {
      	if (conn != null)
      	{
      		conn.close();
      	}
      }
   }

   public void testManyQueue() throws Exception
   {
      String selector1 = "beatle = 'john'";

      Connection conn = null;
      
      try
      {      
	      conn = cf.createConnection();
	      conn.start();
	
	      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	
	      MessageConsumer cons1 = sess.createConsumer(queue1, selector1);
	
	      MessageProducer prod = sess.createProducer(queue1);
	
	      for (int j = 0; j < 100; j++)
	      {
	         Message m = sess.createMessage();
	
	         m.setStringProperty("beatle", "john");
	
	         prod.send(m);
	         
	         m = sess.createMessage();
	
	         m.setStringProperty("beatle", "kermit the frog");
	
	         prod.send(m);
	      }
	
	      for (int j = 0; j < 100; j++)
	      {
	         Message m = cons1.receive(1000);
	         
	         assertNotNull(m);
	         
	         assertEquals("john", m.getStringProperty("beatle"));
	      }
	
	      Message m = cons1.receiveNoWait();
	
	      assertNull(m);
	      
	      String selector2 = "beatle = 'kermit the frog'";
	      
	      MessageConsumer cons2 = sess.createConsumer(queue1, selector2);
	      
	      for (int j = 0; j < 100; j++)
	      {
	         m = cons2.receive(1000);
	         
	         assertNotNull(m);
	         
	         assertEquals("kermit the frog", m.getStringProperty("beatle"));
	      }
	      
	      m = cons2.receiveNoWait();
	
	      assertNull(m);
      }
      finally
      {
      	if (conn != null)
      	{
      		conn.close();
      	}
      }

   }

   // http://jira.jboss.org/jira/browse/JBMESSAGING-775

   public void testManyQueueWithExpired() throws Exception
   {
      String selector1 = "beatle = 'john'";

      Connection conn = null;
      
      try
      {      
	      conn = cf.createConnection();
	      conn.start();
	
	      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	
	      int NUM_MESSAGES = 2;
	
	      MessageProducer prod = sess.createProducer(queue1);
	
	      for (int j = 0; j < NUM_MESSAGES; j++)
	      {
	         Message m = sess.createMessage();
	
	         m.setStringProperty("beatle", "john");
	
	         prod.setTimeToLive(0);
	
	         prod.send(m);
	
	         m = sess.createMessage();
	
	         m.setStringProperty("beatle", "john");
	
	         prod.setTimeToLive(1);
	
	         prod.send(m);
	
	         m = sess.createMessage();
	
	         m.setStringProperty("beatle", "kermit the frog");
	
	         prod.setTimeToLive(0);
	
	         prod.send(m);
	
	         m = sess.createMessage();
	
	         m.setStringProperty("beatle", "kermit the frog");
	
	         m.setJMSExpiration(System.currentTimeMillis());
	
	         prod.setTimeToLive(1);
	
	         prod.send(m);
	      }
	
	      MessageConsumer cons1 = sess.createConsumer(queue1, selector1);
	
	      for (int j = 0; j < NUM_MESSAGES; j++)
	      {
	         Message m = cons1.receive(1000);
	
	         assertNotNull(m);
	
	         assertEquals("john", m.getStringProperty("beatle"));
	      }
	
	      Message m = cons1.receiveNoWait();
	      
	      assertNull(m);
	
	      String selector2 = "beatle = 'kermit the frog'";
	
	      MessageConsumer cons2 = sess.createConsumer(queue1, selector2);
	
	      for (int j = 0; j < NUM_MESSAGES; j++)
	      {
	         m = cons2.receive(1000);
	
	         assertNotNull(m);
	
	         assertEquals("kermit the frog", m.getStringProperty("beatle"));
	      }
	
	      m = cons1.receiveNoWait();
	
	      assertNull(m);
      }
      finally
      {
      	if (conn != null)
      	{
      		conn.close();
      	}
      }
   }

   public void testManyRedeliveriesTopic() throws Exception
   {
      String selector1 = "beatle = 'john'";

      Connection conn = null;
      
      try
      {      
	      conn = cf.createConnection();
	      conn.start();
	
	      for (int i = 0; i < 5; i++)
	      {
	         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	
	         MessageConsumer cons1 = sess.createConsumer(topic1, selector1);
	
	         MessageProducer prod = sess.createProducer(topic1);
	
	         for (int j = 0; j < 10; j++)
	         {
	            Message m = sess.createMessage();
	
	            m.setStringProperty("beatle", "john");
	
	            prod.send(m);
	
	            m = sess.createMessage();
	
	            m.setStringProperty("beatle", "kermit the frog");
	
	            prod.send(m);
	         }
	
	         for (int j = 0; j < 10; j++)
	         {
	            Message m = cons1.receive(1000);
	
	            assertNotNull(m);
	         }
	
	         Message m = cons1.receiveNoWait();
	
	         assertNull(m);
	         
	         sess.close();
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

   public void testManyRedeliveriesQueue() throws Exception
   {
      String selector1 = "beatle = 'john'";

      Connection conn = null;
       
      try
      {
         conn = cf.createConnection();
               	
	      conn.start();
	
	      for (int i = 0; i < 5; i++)
	      {
	         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	
	         MessageConsumer cons1 = sess.createConsumer(queue1, selector1);
	
	         MessageProducer prod = sess.createProducer(queue1);
	
	         for (int j = 0; j < 10; j++)
	         {
	            Message m = sess.createMessage();
	
	            m.setStringProperty("beatle", "john");
	
	            prod.send(m);
	
	            m = sess.createMessage();
	
	            m.setStringProperty("beatle", "kermit the frog");
	
	            prod.send(m);
	         }
	
	         for (int j = 0; j < 10; j++)
	         {
	            Message m = cons1.receive(1000);
	            
	            assertNotNull(m);
	         }
	
	         Message m = cons1.receiveNoWait();
	
	         assertNull(m);
	         
	         sess.close();
	      }
      }
      finally
      {
      	if (conn != null)
      	{
      		conn.close();
      	}
      	
      	removeAllMessages(queue1.getQueueName(), true, 0);
      }
   }

   public void testWithSelector() throws Exception
   {
      String selector1 = "beatle = 'john'";
      String selector2 = "beatle = 'paul'";
      String selector3 = "beatle = 'george'";
      String selector4 = "beatle = 'ringo'";
      String selector5 = "beatle = 'jesus'";

      Connection conn = null;
      
      try
      {	      
	      conn = cf.createConnection();
	      conn.start();
	      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      MessageConsumer cons1 = sess.createConsumer(topic1, selector1);
	      MessageConsumer cons2 = sess.createConsumer(topic1, selector2);
	      MessageConsumer cons3 = sess.createConsumer(topic1, selector3);
	      MessageConsumer cons4 = sess.createConsumer(topic1, selector4);
	      MessageConsumer cons5 = sess.createConsumer(topic1, selector5);
	
	      Message m1 = sess.createMessage();
	      m1.setStringProperty("beatle", "john");
	
	      Message m2 = sess.createMessage();
	      m2.setStringProperty("beatle", "paul");
	
	      Message m3 = sess.createMessage();
	      m3.setStringProperty("beatle", "george");
	
	      Message m4 = sess.createMessage();
	      m4.setStringProperty("beatle", "ringo");
	
	      Message m5 = sess.createMessage();
	      m5.setStringProperty("beatle", "jesus");
	
	      MessageProducer prod = sess.createProducer(topic1);
	
	      prod.send(m1);
	      prod.send(m2);
	      prod.send(m3);
	      prod.send(m4);
	      prod.send(m5);
	
	      Message r1 = cons1.receive(500);
	      assertNotNull(r1);
	      Message n = cons1.receive(500);
	      assertNull(n);
	
	      Message r2 = cons2.receive(500);
	      assertNotNull(r2);
	      n = cons2.receive(500);
	      assertNull(n);
	
	      Message r3 = cons3.receive(500);
	      assertNotNull(r3);
	      n = cons3.receive(500);
	      assertNull(n);
	
	      Message r4 = cons4.receive(500);
	      assertNotNull(r4);
	      n = cons4.receive(500);
	      assertNull(n);
	
	      Message r5 = cons5.receive(500);
	      assertNotNull(r5);
	      n = cons5.receive(500);
	      assertNull(n);
	
	      assertEquals("john", r1.getStringProperty("beatle"));
	      assertEquals("paul", r2.getStringProperty("beatle"));
	      assertEquals("george", r3.getStringProperty("beatle"));
	      assertEquals("ringo", r4.getStringProperty("beatle"));
	      assertEquals("jesus", r5.getStringProperty("beatle"));
      }
      finally
      {
      	if (conn != null)
      	{
      		conn.close();
      	}
      }
   }

   public void testManyConsumersWithDifferentSelectors() throws Exception
   {
      Connection conn = null;
      
      try
      {      
	      conn = cf.createConnection();
	      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      MessageProducer p = sess.createProducer(queue1);
	
	      Session cs = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      final MessageConsumer c = cs.createConsumer(queue1, "weight = 1");
	
	      Session cs2 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      final MessageConsumer c2 = cs2.createConsumer(queue1, "weight = 2");
	
	      for(int i = 0; i < 10; i++)
	      {
	         Message m = sess.createTextMessage("message" + i);
	         m.setIntProperty("weight", i % 2 + 1);
	         p.send(m);
	      }
	
	      conn.start();
	
	      final List received = new ArrayList();
	      final List received2 = new ArrayList();
	      final Latch latch = new Latch();
	      final Latch latch2 = new Latch();
	
	      new Thread(new Runnable()
	      {
	         public void run()
	         {
	            try
	            {
	               while(true)
	               {
	                  Message m = c.receive(1000);
	                  if (m != null)
	                  {
	                     received.add(m);
	                  }
	                  else
	                  {
	                     latch.release();
	                     return;
	                  }
	               }
	            }
	            catch(Exception e)
	            {
	               log.error("receive failed", e);
	            }
	         }
	      }, "consumer thread 1").start();
	
	      new Thread(new Runnable()
	      {
	         public void run()
	         {
	            try
	            {
	               while(true)
	               {
	                  Message m = c2.receive(1000);
	                  if (m != null)
	                  {
	                     received2.add(m);
	                  }
	                  else
	                  {
	                     latch2.release();
	                     return;
	                  }
	               }
	            }
	            catch(Exception e)
	            {
	               log.error("receive failed", e);
	            }
	         }
	      }, "consumer thread 2").start();
	
	      latch.acquire();
	      latch2.acquire();
	
	      assertEquals(5, received.size());
	      for(Iterator i = received.iterator(); i.hasNext(); )
	      {
	         Message m = (Message)i.next();
	         int value = m.getIntProperty("weight");
	         assertEquals(value, 1);
	      }
	
	      assertEquals(5, received2.size());
	      for(Iterator i = received2.iterator(); i.hasNext(); )
	      {
	         Message m = (Message)i.next();
	         int value = m.getIntProperty("weight");
	         assertEquals(value, 2);
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

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------   
}
