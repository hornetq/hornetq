/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.test.messaging.jms.message.base;

import org.jboss.test.messaging.jms.base.JMSTestBase;
import org.jboss.messaging.core.message.SoftMessageReference;

import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.JMSException;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class MessageTestBase extends JMSTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Message message;

   // Constructors --------------------------------------------------

   public MessageTestBase(String name)
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

   public void testNonPersistentSend() throws Exception
   {
      prepareMessage(message);

      queueProd.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      queueProd.send(message);

      Message r = queueCons.receive();

      assertEquals(DeliveryMode.NON_PERSISTENT, r.getJMSDeliveryMode());

      assertEquivalent(r, DeliveryMode.NON_PERSISTENT);
   }

   public void testPersistentSend() throws Exception
   {
      prepareMessage(message);

      queueProd.setDeliveryMode(DeliveryMode.PERSISTENT);

      // make sure the message is serialized to and from the database
      SoftMessageReference.keepSoftReference = false;

      queueProd.send(message);

      Message r = queueCons.receive();

      assertEquals(DeliveryMode.PERSISTENT, r.getJMSDeliveryMode());

      assertEquivalent(r, DeliveryMode.PERSISTENT);
   }


   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   protected void prepareMessage(Message m) throws JMSException
   {
      m.setBooleanProperty("booleanProperty", true);
      m.setByteProperty("byteProperty", (byte)3);
      m.setDoubleProperty("doubleProperty", 4.0);
      m.setFloatProperty("floatProperty", 5.0f);
      m.setIntProperty("intProperty", 6);
      m.setLongProperty("longProperty", 7);
      m.setShortProperty("shortProperty", (short)8);
      m.setStringProperty("stringProperty", "this is a String property");

      m.setJMSCorrelationID("this is the correlation ID");
      m.setJMSReplyTo(topic);
      m.setJMSType("someArbitraryType");
   }

   protected void assertEquivalent(Message m, int mode) throws JMSException
   {
      assertEquals(true, m.getBooleanProperty("booleanProperty"));
      assertEquals((byte)3, m.getByteProperty("byteProperty"));
      assertEquals(new Double(4.0), new Double(m.getDoubleProperty("doubleProperty")));
      assertEquals(new Float(5.0f), new Float(m.getFloatProperty("floatProperty")));
      assertEquals(6, m.getIntProperty("intProperty"));
      assertEquals(7, m.getLongProperty("longProperty"));
      assertEquals((short)8, m.getShortProperty("shortProperty"));
      assertEquals("this is a String property", m.getStringProperty("stringProperty"));

      assertEquals("this is the correlation ID", m.getJMSCorrelationID());
      assertEquals(topic, m.getJMSReplyTo());
      assertEquals("someArbitraryType", m.getJMSType());
      assertEquals(queue, m.getJMSDestination());
      assertFalse(m.getJMSRedelivered());
      assertEquals(mode, m.getJMSDeliveryMode());
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
