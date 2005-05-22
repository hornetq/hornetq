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
public class JMSDeliveryModeHeaderTest extends MessageTest
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public JMSDeliveryModeHeaderTest(String name)
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

   public void testDefaultDeliveryMode() throws Exception
   {
      assertEquals(DeliveryMode.PERSISTENT, queueProducer.getDeliveryMode());
   }

   public void testNonPersistentDeliveryMode() throws Exception
   {
      queueProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      assertEquals(DeliveryMode.NON_PERSISTENT, queueProducer.getDeliveryMode());

      Message m = queueProducerSession.createMessage();
      queueProducer.send(m);

      assertEquals(DeliveryMode.NON_PERSISTENT, queueConsumer.receive().getJMSDeliveryMode());
   }

   public void testPersistentDeliveryMode() throws Exception
   {
      queueProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
      assertEquals(DeliveryMode.PERSISTENT, queueProducer.getDeliveryMode());

      Message m = queueProducerSession.createMessage();
      queueProducer.send(m);

      assertEquals(DeliveryMode.PERSISTENT, queueConsumer.receive().getJMSDeliveryMode());
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

}
