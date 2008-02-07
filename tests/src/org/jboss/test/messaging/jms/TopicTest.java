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

import java.io.Serializable;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class TopicTest extends JMSTestCase
{
   // Constants -----------------------------------------------------
   
   // Static --------------------------------------------------------
	
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------
   
   public TopicTest(String name)
   {
      super(name);
   }
   
   protected void setUp() throws Exception
   {
      
      log.info("______________________Test starting");
      super.setUp();
   }

   protected void tearDown() throws Exception
   {      
      super.tearDown();
      log.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~ Test endded");
   }
   
   // Public --------------------------------------------------------

   /**
    * The simplest possible topic test.
    */
   public void testTopic() throws Exception
   {
      Connection conn = null;
      
      try
      {
         conn = cf.createConnection();

         Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = s.createProducer(topic1);
         MessageConsumer c = s.createConsumer(topic1);
         conn.start();

         p.send(s.createTextMessage("payload"));
         TextMessage m = (TextMessage)c.receive();

         assertEquals("payload", m.getText());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }
   
   public void testTopic2() throws Exception
   {
      Connection conn = null;
      
      try
      {
         conn = cf.createConnection();

         Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = s.createProducer(topic1);
         MessageConsumer c = s.createConsumer(topic1);
         conn.start();

         p.send(s.createTextMessage("payload"));
         TextMessage m = (TextMessage)c.receive();

         assertEquals("payload", m.getText());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

//   public void testTopicName() throws Exception
//   {
//      Topic topic = (Topic)ic.lookup("/topic/Topic1");
//      assertEquals("Topic1", topic.getTopicName());
//   }
//   
//   /*
//    * See http://jira.jboss.com/jira/browse/JBMESSAGING-399
//    */
//   public void testRace() throws Exception
//   {
//      Connection conn = null;
//      
//      try
//      {	      
//	      conn = cf.createConnection();
//	      
//	      Session sSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
//	      
//	      MessageProducer prod = sSend.createProducer(topic1);
//	      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
//	      
//	      Session s1 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
//	      Session s2 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
//	      Session s3 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
//	      
//	      MessageConsumer c1 = s1.createConsumer(topic1);
//	      MessageConsumer c2 = s2.createConsumer(topic1);
//	      MessageConsumer c3 = s3.createConsumer(topic1);            
//	      
//	      final int numMessages = 500;
//         	    
//	      TestListener l1 = new TestListener(numMessages);
//	      TestListener l2 = new TestListener(numMessages);
//	      TestListener l3 = new TestListener(numMessages);
//	      
//	      c1.setMessageListener(l1);
//	      c2.setMessageListener(l2);
//	      c3.setMessageListener(l3);
//	            
//	      conn.start();
//	       	      
//	      for (int i = 0; i < numMessages; i++)
//	      {
//	         byte[] blah = new byte[10000];
//	         String str = new String(blah);
//	           
//	         Wibble2 w = new Wibble2();
//	         w.s = str;
//	         ObjectMessage om = sSend.createObjectMessage(w);
//	         
//	         prod.send(om);
//	      }          
//	      
//	      l1.waitForMessages();
//	      l2.waitForMessages();
//	      l3.waitForMessages();
//	      
//	      assertFalse(l1.failed);
//	      assertFalse(l2.failed);
//	      assertFalse(l3.failed);
//      }
//      finally
//      {
//      	if (conn != null)
//      	{
//      		conn.close();
//      	}
//      }
//   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
   
   static class Wibble2 implements Serializable
   {
      private static final long serialVersionUID = -5146179676719808756L;
      String s;
   }
   
   static class TestListener implements MessageListener
   {
      boolean failed;
      
      int count;
      
      int num;
      
      TestListener(int num)
      {
      	this.num = num;
      }
      
      public synchronized void onMessage(Message m)
      {
         ObjectMessage om = (ObjectMessage)m;
         
         try
         {         
            Wibble2 w = (Wibble2)om.getObject();
         }
         catch (Exception e)
         {
            failed = true;
         }
         
         count++;
         
         if (count == num)
         {         
         	this.notify();
         }
      }
      
      synchronized void waitForMessages() throws Exception
      {
      	while (count < num)
      	{
      		this.wait();
      	}
      }
   }
   
}

