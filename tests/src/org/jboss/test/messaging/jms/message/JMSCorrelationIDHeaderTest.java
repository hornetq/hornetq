/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.message;

import javax.jms.JMSException;
import javax.jms.Message;

/**
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public class JMSCorrelationIDHeaderTest extends MessageTest
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

   public void setUp() throws Exception
   {
      super.setUp();
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
   }


   public void testJMSDestination() throws Exception
   {
      Message m1 = queueProducerSession.createMessage();
      
      //Test with correlation id containing a message id
      final String messageID = "ID:812739812378"; 
      m1.setJMSCorrelationID(messageID);
      
      queueProducer.send(m1);
      Message m2 = queueConsumer.receive();
      assertEquals(messageID, m2.getJMSCorrelationID());
      
      try
      {
         m2.getJMSCorrelationIDAsBytes();
         fail();
      }
      catch(JMSException e) {}
      
      //Test with correlation id containing an application defined string
      Message m3 = queueProducerSession.createMessage();
      final String appDefinedID = "oiwedjiwjdoiwejdoiwjd"; 
      m3.setJMSCorrelationID(appDefinedID);
      
      queueProducer.send(m3);
      Message m4 = queueConsumer.receive();
      assertEquals(appDefinedID, m4.getJMSCorrelationID());
      
      try
      {
         m4.getJMSCorrelationIDAsBytes();
         fail();
      }
      catch(JMSException e) {}
      
      
      // Test with correlation id containing a byte[]
      Message m5 = queueProducerSession.createMessage();
      final byte[] bytes = new byte[] { -111, 45, 106, 3, -44 };
      m5.setJMSCorrelationIDAsBytes(bytes);
      
      queueProducer.send(m5);
      Message m6 = queueConsumer.receive();
      log.trace("Correlation id bytes:" + m6.getJMSCorrelationIDAsBytes());
      assertByteArraysEqual(bytes, m6.getJMSCorrelationIDAsBytes());
      
      try
      {
         m6.getJMSCorrelationID();
         fail();
      }
      catch(JMSException e) {}
      
      
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
