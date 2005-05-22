/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.message;

import javax.jms.Message;

/**
 * @author <a href="mailto:dua_rajdeep@yahoo.com">Rajdeep Dua</a>
 * @version <tt>$Revision$</tt>
 */
public class JMSTypeHeaderTest extends MessageTest
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public JMSTypeHeaderTest(String name)
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

   public void testJMSType() throws Exception
   {
      Message m = queueProducerSession.createMessage();
      String originalType =  "TYPE1";
      m.setJMSType(originalType);
      queueProducer.send(m);
      String gotType = queueConsumer.receive().getJMSType();
      assertEquals(originalType, gotType);
   }

   public void testNULLJMSType() throws Exception
   {
      Message m = queueProducerSession.createMessage();
      queueProducer.send(m);
      assertEquals(null, queueConsumer.receive().getJMSType());
   }


   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

}
