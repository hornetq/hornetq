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

import java.util.HashSet;
import java.util.Set;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class QueueTest extends JMSTestCase
{
   // Constants ------------------------------------------------------------------------------------
   
   // Static ---------------------------------------------------------------------------------------
   
   // Attributes -----------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------
   
   // Public ---------------------------------------------------------------------------------------

   /**
    * The simplest possible queue test.
    */
   public void testQueue() throws Exception
   {
      Connection conn = null;
      
      try
      {
         conn = cf.createConnection();

         Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = s.createProducer(queue1);
         MessageConsumer c = s.createConsumer(queue1);
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
   

   //http://jira.jboss.com/jira/browse/JBMESSAGING-1101
   public void testBytesMessagePersistence() throws Exception
   {
      Connection conn = null;
      
      byte[] bytes = new byte[] { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 123, 55, 0, 12, -100, -11};		
      
      try
      {      
	      conn = cf.createConnection();
	      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = sess.createProducer(queue1);
	      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
	      
	      for (int i = 0; i < 1; i++)
	      {
	      	BytesMessage bm = sess.createBytesMessage();
				
				bm.writeBytes(bytes);
				
	         prod.send(bm);
	      }
	      
	      conn.close();
	      
	      stop();
	      
	      startNoDelete();
	      
	      // Messaging server restart implies new ConnectionFactory lookup
	      deployAndLookupAdministeredObjects();
	      
	      conn = cf.createConnection();
	      sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      conn.start();
	      MessageConsumer cons = sess.createConsumer(queue1);
	      for (int i = 0; i < 1; i++)
	      {
	      	BytesMessage bm = (BytesMessage)cons.receive(3000);
				
				assertNotNull(bm);
						
				byte[] bytes2 = new byte[bytes.length];
				
				bm.readBytes(bytes2);
				
				for (int j = 0; j < bytes.length; j++)
				{
					assertEquals(bytes[j], bytes2[j]);
				}
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
      
   // added for http://jira.jboss.org/jira/browse/JBMESSAGING-899
   public void testClosedConsumerAfterStart() throws Exception
   {
      // This loop is to increase chances of a failure.
      for (int counter = 0; counter < 20; counter++)
      {
         Connection conn1 = null;
         
         Connection conn2 = null;
         
         try
         {
         	conn1 = cf.createConnection();

         	conn2 = cf.createConnection();

         	Session s = conn1.createSession(true, Session.SESSION_TRANSACTED);

         	MessageProducer p = s.createProducer(queue1);

         	for (int i = 0; i < 20; i++)
         	{
         		p.send(s.createTextMessage("message " + i));
            }

            s.commit();

            Session s2 = conn2.createSession(true, Session.SESSION_TRANSACTED);

            // Create a consumer, start the session, close the consumer..
            // This shouldn't cause any message to be lost
            MessageConsumer c2 = s2.createConsumer(queue1);
            conn2.start();
            c2.close();

            c2 = s2.createConsumer(queue1);

            //There is a possibility the messages arrive out of order if they hit the closed
            //consumer and are cancelled back before delivery to the other consumer has finished.
            //There is nothing much we can do about this
            Set texts = new HashSet();

            for (int i = 0; i < 20; i++)
            {
               TextMessage txt = (TextMessage) c2.receive(5000);
               assertNotNull(txt);
               texts.add(txt.getText());
            }

            for (int i = 0; i < 20; i++)
            {
               assertTrue(texts.contains("message " + i));
            }

            s2.commit();

            checkEmpty(queue1);      
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
         }
      }
   }

   public void testRedeployQueue() throws Exception
   {
      Connection conn = null;
            
      try
      {
         conn = cf.createConnection();

         Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = s.createProducer(queue1);
         MessageConsumer c = s.createConsumer(queue1);
         conn.start();

         for (int i = 0; i < 500; i++)
         {
            p.send(s.createTextMessage("payload " + i));
         }

         conn.close();

         stop();

         startNoDelete();

         deployAndLookupAdministeredObjects();

         conn = cf.createConnection();
         s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         p = s.createProducer(queue1);
         c = s.createConsumer(queue1);
         conn.start();

         for (int i = 0; i < 500; i++)
         {
            TextMessage message = (TextMessage)c.receive(3000);
            assertNotNull(message);
            assertNotNull(message.getJMSDestination());
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


   public void testQueueName() throws Exception
   {
      assertEquals("Queue1", queue1.getQueueName());
   }

   // Package protected ----------------------------------------------------------------------------
   
   // Protected ------------------------------------------------------------------------------------
   
   // Private --------------------------------------------------------------------------------------
   
   // Inner classes --------------------------------------------------------------------------------
   
}

