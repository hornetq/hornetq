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
import javax.jms.InvalidSelectorException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.QueueConnection;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import javax.naming.InitialContext;

import org.jboss.test.messaging.jms.message.SimpleJMSBytesMessage;
import org.jboss.test.messaging.jms.message.SimpleJMSMessage;

/**
 * Safeguards for previously detected TCK failures.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.org">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class CTSMiscellaneousTest extends JMSTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected InitialContext ic;

   // Constructors --------------------------------------------------

   public CTSMiscellaneousTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testForeignByteMessage() throws Exception
   {
      Connection c = null;
      
      try
      {	      
	      c = cf.createConnection();
	      Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
	
	      MessageProducer p = s.createProducer(queue1);
	
	      // create a Bytes foreign message
	      SimpleJMSBytesMessage bfm = new SimpleJMSBytesMessage();
	
	      p.send(bfm);
	
	      MessageConsumer cons = s.createConsumer(queue1);
	      c.start();
	
	      BytesMessage bm = (BytesMessage)cons.receive();
	      assertNotNull(bm);
      }
      finally
      {
      	if (c != null)
      	{
      		c.close();
      	}
      }
	     
   }

   public void testJMSMessageIDChanged() throws Exception
   {
      Connection c = null;
      
      try
      {
	      
	      c= cf.createConnection();
	      Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
	
	      MessageProducer p = s.createProducer(queue1);
	
	      Message m = new SimpleJMSMessage();
	      m.setJMSMessageID("something");
	
	      p.send(m);
	
	      assertFalse("something".equals(m.getJMSMessageID()));
	
	      c.close();
      }
      finally
      {
      	if (c != null)
      	{
      		c.close();
      	}
      	
      	removeAllMessages(queue1.getQueueName(), true, 0);
      }
   }

   /**
    * com.sun.ts.tests.jms.ee.all.queueconn.QueueConnTest line 171
    */
   public void test_1() throws Exception
   {
      QueueConnection qc = null;
      
      try
      {	      
	      qc = cf.createQueueConnection();
	      QueueSession qs = qc.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
	
	      QueueReceiver qreceiver = qs.createReceiver(queue1, "targetMessage = TRUE");
	
	      qc.start();
	
	      TextMessage m = qs.createTextMessage();
	      m.setText("one");
	      m.setBooleanProperty("targetMessage", false);
	
	      QueueSender qsender = qs.createSender(queue1);
	
	      qsender.send(m);
	
	      m.setText("two");
	      m.setBooleanProperty("targetMessage", true);
	
	      qsender.send(m);
	
	      TextMessage rm = (TextMessage)qreceiver.receive(1000);
	
	      assertEquals("two", rm.getText());
      }
      finally
      {
      	if (qc != null)
      	{
      		qc.close();      		
      	}
      	Thread.sleep(2000);
      	log.info("****** removing merssages");
      	removeAllMessages(queue1.getQueueName(), true, 0);
      	checkEmpty(queue1);
      }
   }

   public void testInvalidSelectorOnDurableSubscription() throws Exception
   {
      Connection c = null;
      
      try
      {      
         c = cf.createConnection();
         c.setClientID("something");

         Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);

         try
         {
            s.createDurableSubscriber(topic1, "somename", "=TEST 'test'", false);
            fail("this should fail");
         }
         catch(InvalidSelectorException e)
         {
            // OK
         }
      }
      finally
      {
         c.close();
      }
   }

   public void testInvalidSelectorOnSubscription() throws Exception
   {
      TopicConnection c = null;      
      try
      {
         c = cf.createTopicConnection();
         c.setClientID("something");
      	
         TopicSession s = c.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

         try
         {
            s.createSubscriber(topic1, "=TEST 'test'", false);
            fail("this should fail");
         }
         catch(InvalidSelectorException e)
         {
            // OK
         }
      }
      finally
      {
         c.close();
      }
   }


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------   
}
