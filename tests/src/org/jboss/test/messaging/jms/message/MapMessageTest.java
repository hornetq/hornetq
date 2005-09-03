/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.message;

import org.jboss.test.messaging.jms.message.base.MessageTestBase;

import javax.jms.MapMessage;
import javax.jms.JMSException;
import javax.jms.Message;

/**
 * A test that sends/receives map messages to the JMS provider and verifies their integrity.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class MapMessageTest extends MessageTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public MapMessageTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      message = session.createMapMessage();
   }

   public void tearDown() throws Exception
   {
      message = null;
      super.tearDown();
   }

   // Protected -----------------------------------------------------

   protected void prepareMessage(Message m) throws JMSException
   {
      super.prepareMessage(m);

      MapMessage mm = (MapMessage)m;

      mm.setBoolean("boolean", true);
      mm.setByte("byte", (byte)3);
      mm.setBytes("bytes", new byte[] { (byte)3, (byte)4, (byte)5 });
      mm.setChar("char", (char)6);
      mm.setDouble("double", 7.0);
      mm.setFloat("float", 8.0f);
      mm.setInt("int", 9);
      mm.setLong("long", 10l);
      mm.setObject("object", new String("this is an object"));
      mm.setShort("short", (short)11);
      mm.setString("string", "this is a string");
   }

   protected void assertEquivalent(Message m, int mode) throws JMSException
   {
      super.assertEquivalent(m, mode);

      MapMessage mm = (MapMessage)m;

      assertEquals(true, mm.getBoolean("boolean"));
      assertEquals((byte)3, mm.getByte("byte"));
      byte[] bytes = mm.getBytes("bytes");
      assertEquals((byte)3, bytes[0]);
      assertEquals((byte)4, bytes[1]);
      assertEquals((byte)5, bytes[2]);
      assertEquals((char)6, mm.getChar("char"));
      assertEquals(new Double(7.0), new Double(mm.getDouble("double")));
      assertEquals(new Float(8.0f), new Float(mm.getFloat("float")));
      assertEquals(9, mm.getInt("int"));
      assertEquals(10l, mm.getLong("long"));
      assertEquals("this is an object", mm.getObject("object"));
      assertEquals((short)11, mm.getShort("short"));
      assertEquals("this is a string", mm.getString("string"));
   }
}
