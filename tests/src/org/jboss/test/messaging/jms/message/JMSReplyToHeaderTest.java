/*
 /**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.message;

import javax.jms.Message;
import javax.jms.TemporaryQueue;

/**
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public class JMSReplyToHeaderTest extends MessageTest
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public JMSReplyToHeaderTest(String name)
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


   public void testJMSDestinationSimple() throws Exception
   {
      Message m = queueProducerSession.createMessage();
      TemporaryQueue tempQ = queueProducerSession.createTemporaryQueue();
      m.setJMSReplyTo(tempQ);
      
      queueProducer.send(m);
      Message m2 = queueConsumer.receive();
      assertEquals(tempQ, m.getJMSReplyTo());
   }
   
   


   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

}
