/*
 * JORAM: Java(TM) Open Reliable Asynchronous Messaging
 * Copyright (C) 2002 INRIA
 * Contact: joram-team@objectweb.org
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
 * USA
 * 
 * Initial developer(s): Jeff Mesnil (jmesnil@gmail.com)
 * Contributor(s): ______________________________________.
 */

package org.objectweb.jtests.jms.conform.message.properties;

import java.util.Enumeration;

import javax.jms.ConnectionMetaData;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.objectweb.jtests.jms.framework.PTPTestCase;
import org.objectweb.jtests.jms.framework.TestConfig;

/**
 * Test the JMSX defined properties.
 * <br />
 *  See JMS Specification, sec. 3.5.9 JMS Defined Properties
 *
 * @author Jeff Mesnil (jmesnil@gmail.com)
 * @version $Id: JMSXPropertyTest.java,v 1.2 2007/06/19 23:32:34 csuconic Exp $
 */
public class JMSXPropertyTest extends PTPTestCase
{

   /**
    * Test that the JMSX property <code>JMSXGroupID</code> is supported.
    */
   public void testSupportsJMSXGroupID()
   {
      try
      {
         boolean found = false;
         ConnectionMetaData metaData = senderConnection.getMetaData();
         Enumeration enumeration = metaData.getJMSXPropertyNames();
         while (enumeration.hasMoreElements())
         {
            String jmsxPropertyName = (String) enumeration.nextElement();
            if (jmsxPropertyName.equals("JMSXGroupID"))
            {
               found = true;
            }
         }
         assertTrue("JMSXGroupID property is not supported", found);
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /**
    * Test that the JMSX property <code>JMSXGroupID</code> works
    */
   public void testJMSXGroupID_1()
   {
      try
      {
         String groupID = "testSupportsJMSXGroupID_1:group";
         TextMessage message = senderSession.createTextMessage();
         message.setStringProperty("JMSXGroupID", groupID);
         message.setText("testSupportsJMSXGroupID_1");
         sender.send(message);

         Message m = receiver.receive(TestConfig.TIMEOUT);
         assertTrue(m instanceof TextMessage);
         TextMessage msg = (TextMessage) m;
         assertEquals(groupID, msg.getStringProperty("JMSXGroupID"));
         assertEquals("testSupportsJMSXGroupID_1", msg.getText());
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }


   /**
    * Test that the JMSX property <code>JMSXDeliveryCount</code> works.
    */
   public void testJMSXDeliveryCount() throws Exception
   {
      if (!supportsJMSXDeliveryCount())
      {
         return;
      }
      
      try
      {
         senderConnection.stop();
         // senderSession has been created as non transacted
         // we create it again but as a transacted session
         senderSession = senderConnection.createQueueSession(true, 0);
         assertEquals(true, senderSession.getTransacted());
         // we create again the sender
         sender = senderSession.createSender(senderQueue);
         senderConnection.start();

         receiverConnection.stop();
         // receiverSession has been created as non transacted
         // we create it again but as a transacted session
         receiverSession = receiverConnection.createQueueSession(true, 0);
         assertEquals(true, receiverSession.getTransacted());
         // we create again the receiver
         if (receiver!=null)
         {
        	 receiver.close();
         }
         receiver = receiverSession.createReceiver(receiverQueue);
         receiverConnection.start();

         // we send a message...
         TextMessage message = senderSession.createTextMessage();
         message.setText("testJMSXDeliveryCount");
         sender.send(message);
         // ... and commit the *producer* transaction
         senderSession.commit();

         // we receive a message...
         Message m = receiver.receive(TestConfig.TIMEOUT);
         assertTrue(m != null);
         assertTrue(m instanceof TextMessage);
         TextMessage msg = (TextMessage) m;
         // ... which is the one which was sent...
         assertEquals("testJMSXDeliveryCount", msg.getText());
         // ...and has not been redelivered
         assertEquals(false, msg.getJMSRedelivered());
         // ... so it has been delivered once
         int jmsxDeliveryCount = msg.getIntProperty("JMSXDeliveryCount");
         assertEquals(1, jmsxDeliveryCount);
         // we rollback the *consumer* transaction
         receiverSession.rollback();

         // we receive again a message
         m = receiver.receive(TestConfig.TIMEOUT);
         assertTrue(m != null);
         assertTrue(m instanceof TextMessage);
         msg = (TextMessage) m;
         // ... which is still the one which was sent...
         assertEquals("testJMSXDeliveryCount", msg.getText());
         // .. but this time, it has been redelivered
         assertEquals(true, msg.getJMSRedelivered());
         // ... so it has been delivered a second time
         jmsxDeliveryCount = msg.getIntProperty("JMSXDeliveryCount");
         assertEquals(2, jmsxDeliveryCount);
      }
      catch (JMSException e)
      {
         fail(e);
      }
      catch (Exception e)
      {
         fail(e);
      }
   }


   /**
    * checks if the JMSX property <code>JMSXDeliveryCount</code> is supported.
    */
   private boolean supportsJMSXDeliveryCount() throws Exception
   {
      ConnectionMetaData metaData = senderConnection.getMetaData();
      Enumeration enumeration = metaData.getJMSXPropertyNames();
      while (enumeration.hasMoreElements())
      {
         String jmsxPropertyName = (String) enumeration.nextElement();
         if (jmsxPropertyName.equals("JMSXDeliveryCount"))
         {
            return true;
         }
      }
      return false;
   }
   
   /** 
    * Method to use this class in a Test suite
    */
   public static Test suite()
   {
      return new TestSuite(JMSXPropertyTest.class);
   }

   public JMSXPropertyTest(String name)
   {
      super(name);
   }
}
