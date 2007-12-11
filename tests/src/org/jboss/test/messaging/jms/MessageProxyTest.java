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
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.message.MessageProxy;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * 
 * A MessageProxyTest

 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class MessageProxyTest extends JMSTestCase
{
   // Constants -----------------------------------------------------
   
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   public MessageProxyTest(String name)
   {
      super(name);
   }
   
   // TestCase overrides -------------------------------------------
   
   // Public --------------------------------------------------------
         
   public void testMessageIDs1() throws Exception
   {
      if (ServerManagement.isRemote())
      {
         return;
      }
      
      Connection conn = null;
      
      try
      {
         conn = cf.createConnection();
         
         conn.start();
         
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod = sess.createProducer(queue1);
     
         MessageConsumer cons = sess.createConsumer(queue1);
         
         Message msent = sess.createMessage();

         prod.send(msent);
                  
         Message mrec = cons.receive();
         
         //The two ids should be the same
         
         long id1 = ((MessageProxy)msent).getMessage().getMessageID();
         long id2 = ((MessageProxy)mrec).getMessage().getMessageID();
         
         assertEquals(id1, id2);
         
         //Now send the message again
         prod.send(msent);
         
         //The sent id should be different
         long id3 = ((MessageProxy)msent).getMessage().getMessageID();
         long id4 = ((MessageProxy)mrec).getMessage().getMessageID();
         
         assertFalse(id1 == id3);
         
         //But this shouldn't affect the received id
         assertEquals(id2, id4);            
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
     
   public void testMessageIDs2() throws Exception
   {
      if (ServerManagement.isRemote())
      {
         return;
      }
      
      Connection conn = null;
      
      try
      {
         conn = cf.createConnection();
         
         conn.start();
         
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod = sess.createProducer(queue1);
    
         MessageConsumer cons = sess.createConsumer(queue1);
         
         Message msent = sess.createMessage();

         prod.send(msent);
                  
         Message mrec = cons.receive();
         
         //The two ids should be the same
         
         long id1 = ((MessageProxy)msent).getMessage().getMessageID();
         long id2 = ((MessageProxy)mrec).getMessage().getMessageID();
         
         assertEquals(id1, id2);
         
         //Now send the received again
         prod.send(mrec);
         
         //The sent id should be different
         long id3 = ((MessageProxy)msent).getMessage().getMessageID();
         
         //But this shouldn't affect the sent id
         assertEquals(id1, id3);            
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
   
  
   public void testNewMessage() throws Exception
   {
      if (ServerManagement.isRemote())
      {
         return;
      }
      
      Connection conn = null;
      
      try
      {
         conn = cf.createConnection();
         
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod = sess.createProducer(queue1);
         
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
               
         MapMessage m = sess.createMapMessage();
         
         JBossMessage check1 = ((MessageProxy)m).getMessage();
         
         m.setString("map_entry", "map_value");         
         m.setStringProperty("property_entry", "property_value");   
         
         m.setJMSReplyTo(new JBossQueue("wibble"));
         
         JBossMessage check2 = ((MessageProxy)m).getMessage();
         
         checkSameUnderlyingMessage(check1, check2, true);
         checkSameBody(check1, check2, true);
         
         prod.send(m);
         
         JBossMessage check3 = ((MessageProxy)m).getMessage();
         
         //Underlying message should be the same before and after
         checkSameUnderlyingMessage(check2, check3, true);
         checkSameBody(check2, check3, true);
         
         prod.send(m);
         
         JBossMessage check4 = ((MessageProxy)m).getMessage();
         
         assertFalse(check3.getMessageID() == check4.getMessageID());
         
         //The underlying message should now be different
         checkSameUnderlyingMessage(check3, check4, false);
         
         //But the body should be the same
         checkSameBody(check3, check4, true);
         
         prod.send(m);
         
         JBossMessage check5 = ((MessageProxy)m).getMessage();
         
         // The message should be different
         assertFalse(check4.getMessageID() == check5.getMessageID());
         
         checkSameUnderlyingMessage(check4, check5, false);
         
         //But the body should be the same
         checkSameBody(check4, check5, true);
         
         //Now set another header
         
         m.setJMSType("type123");
         
         JBossMessage check6 = ((MessageProxy)m).getMessage();
         
         
         //The message should be different
         checkSameUnderlyingMessage(check5, check6, false);
         
         //But the body should be the same
         checkSameBody(check5, check6, true);
         
         prod.send(m);
         
         JBossMessage check7 = ((MessageProxy)m).getMessage();
                  
         //The message should be the same
         
         checkSameUnderlyingMessage(check6, check7, true);
         
         // But the body should be the same
         checkSameBody(check6, check7, true);
         
         // Set the body
         m.setString("key1", "blah");
         
         JBossMessage check8 = ((MessageProxy)m).getMessage();
         
         //The message should be the same
         
         checkSameUnderlyingMessage(check7, check8, true);
         
         // But the body should not be the same
         checkSameBody(check7, check8, false);
         
         //And the body not the same
         
         checkSameUnderlyingMessage(check7, check8, false);
         
         prod.send(m);
         
         JBossMessage check9 = ((MessageProxy)m).getMessage();
         
         //The message should be the same
         
         checkSameUnderlyingMessage(check8, check9, true);         
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
         
   public void testReceivedMessage(boolean persistent) throws Exception
   {
      if (ServerManagement.isRemote())
      {
         return;
      }
      
      Connection conn = null;
      
      try
      {
         conn = cf.createConnection();
         
         conn.start();
         
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod = sess.createProducer(queue1);
         
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
              
         MapMessage m = sess.createMapMessage();
         
         MessageConsumer cons = sess.createConsumer(queue1);
         
         prod.send(m);
         
         m = (MapMessage)cons.receive(2000);
         
         assertNotNull(m);
                           
         JBossMessage check1 = ((MessageProxy)m).getMessage();         
         
         prod.send(m);
         
         JBossMessage check3 = ((MessageProxy)m).getMessage();
         
         checkSameUnderlyingMessage(check1, check3, true);
         
         checkSameBody(check1, check3, true);
         
         prod.send(m);
         
         JBossMessage check4 = ((MessageProxy)m).getMessage();
         
         assertFalse(check3.getMessageID() == check4.getMessageID());
         
         //The underlying message should now be different
         checkSameUnderlyingMessage(check3, check4, false);
         
         //But the body should be the same
         checkSameBody(check3, check4, true);
         
         prod.send(m);
         
         JBossMessage check5 = ((MessageProxy)m).getMessage();
         
         // The message should be different
         assertFalse(check4.getMessageID() == check5.getMessageID());
         
         checkSameUnderlyingMessage(check4, check5, false);
         
         //But the body should be the same
         checkSameBody(check4, check5, true);
         
         //Now set another header
         
         m.setJMSType("type123");
         
         JBossMessage check6 = ((MessageProxy)m).getMessage();
         
         
         //The message should be different
         checkSameUnderlyingMessage(check5, check6, false);
         
         //But the body should be the same
         checkSameBody(check5, check6, true);
         
         prod.send(m);
         
         JBossMessage check7 = ((MessageProxy)m).getMessage();
                  
         //The message should be the same
         
         checkSameUnderlyingMessage(check6, check7, true);
         
         // But the body should be the same
         checkSameBody(check6, check7, true);
           
         // Set the body
         m.setString("key1", "blah");
         
         JBossMessage check8 = ((MessageProxy)m).getMessage();
         
         //The message should be the same
         
         checkSameUnderlyingMessage(check7, check8, true);
         
         // But the body should not be the same
         checkSameBody(check7, check8, false);
         
         //And the body not the same
         
         checkSameUnderlyingMessage(check7, check8, false);
         
         prod.send(m);
         
         JBossMessage check9 = ((MessageProxy)m).getMessage();
         
         //The message should be the same
         
         checkSameUnderlyingMessage(check8, check9, true);        
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
         
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   private void checkSameUnderlyingMessage(JBossMessage m1, JBossMessage m2, boolean same)
   {
      if ((m1 == m2) && (m1.getHeaders() == m2.getHeaders()) && !same)
      {         
         fail("Underlying message not same");
      }
   }
   
   private void checkSameBody(JBossMessage m1, JBossMessage m2, boolean same)
   {
      // JIRA http://jira.jboss.org/jira/browse/JBMESSAGING-1195
      // comparison is done based on equality instead of identity until in-vm 
      // optimization task is done.
      
      // if (same && (m1.getPayload() != m2.getPayload()))
      // {
      //    fail("Body not same");
      // }
      // else if (!same && (m1.getPayload() == m2.getPayload()))
      // {
      //    fail("Body same");
      //  }
      if (same && (!m1.getPayload().equals(m2.getPayload())))
      {
         fail("Body not same");
      }
      else if (!same && (m1.getPayload().equals(m2.getPayload())))
      {
         fail("Body same");
      }
   }
      
   
   
   // Inner classes -------------------------------------------------
   
}


