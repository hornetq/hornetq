/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.message;

import javax.jms.Message;
import javax.jms.DeliveryMode;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class JMSExpirationHeaderTest extends MessageTest
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public JMSExpirationHeaderTest(String name)
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

   public void testZeroExpiration() throws Exception
   {
      Message m = producerSession.createMessage();
      producer.send(m);
      assertEquals(Long.MAX_VALUE, consumer.receive().getJMSExpiration());
   }

   public void testFiniteExpiration() throws Exception
   {
      Message m = producerSession.createMessage();
      producer.send(m, DeliveryMode.NON_PERSISTENT, 4, 2000);
      Thread.sleep(3000);
      log.info("Trying to read ...");
      assertNull(consumer.receive(1000));
   }


   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

}
