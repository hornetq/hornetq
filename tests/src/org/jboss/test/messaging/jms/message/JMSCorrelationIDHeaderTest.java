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

import javax.jms.Message;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JMSCorrelationIDHeaderTest extends MessageHeaderTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public JMSCorrelationIDHeaderTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testJMSDestination() throws Exception
   {
      Message m1 = queueProducerSession.createMessage();
      
      //Test with correlation id containing a message id
      final String messageID = "ID:812739812378"; 
      m1.setJMSCorrelationID(messageID);
      
      queueProducer.send(m1);
      Message m2 = queueConsumer.receive();
      assertEquals(messageID, m2.getJMSCorrelationID());
      
      //Test with correlation id containing an application defined string
      Message m3 = queueProducerSession.createMessage();
      final String appDefinedID = "oiwedjiwjdoiwejdoiwjd"; 
      m3.setJMSCorrelationID(appDefinedID);
      
      queueProducer.send(m3);
      Message m4 = queueConsumer.receive();
      assertEquals(appDefinedID, m4.getJMSCorrelationID());
      
      // Test with correlation id containing a byte[]
      Message m5 = queueProducerSession.createMessage();
      final byte[] bytes = new byte[] { -111, 45, 106, 3, -44 };
      m5.setJMSCorrelationIDAsBytes(bytes);
      
      queueProducer.send(m5);
      Message m6 = queueConsumer.receive();      
      assertByteArraysEqual(bytes, m6.getJMSCorrelationIDAsBytes());
          
   }



   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   private void assertByteArraysEqual(byte[] bytes1, byte[] bytes2)
   {
      if (bytes1 == null | bytes2 == null)
      {
         fail();
      }

      if (bytes1.length != bytes2.length)
      {
         fail();
      }

      for (int i = 0; i < bytes1.length; i++)
      {
         assertEquals(bytes1[i], bytes2[i]);
      }

   }
   
   // Inner classes -------------------------------------------------

}
