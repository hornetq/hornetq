/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.message;

import org.jboss.test.messaging.jms.message.base.MessageTestBase;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.StreamMessage;

/**
 * A test that sends/receives stream messages to the JMS provider and verifies their integrity.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class StreamMessageTest extends MessageTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public StreamMessageTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      message = session.createStreamMessage();
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

      StreamMessage sm = (StreamMessage)m;

      sm.writeBoolean(true);
      sm.writeByte((byte)3);
      sm.writeBytes(new byte[] {(byte)4, (byte)5, (byte)6});
      sm.writeChar((char)7);
      sm.writeDouble(8.0);
      sm.writeFloat(9.0f);
      sm.writeInt(10);
      sm.writeLong(11l);
      sm.writeObject("this is an object");
      sm.writeShort((short)12);
      sm.writeString("this is a String");
   }

   protected void assertEquivalent(Message m, int mode) throws JMSException
   {
      super.assertEquivalent(m, mode);

      StreamMessage sm = (StreamMessage)m;

      sm.reset();

      assertEquals(true, sm.readBoolean());
      assertEquals((byte)3, sm.readByte());
      byte[] bytes = new byte[3];
      sm.readBytes(bytes);
      assertEquals((byte)4, bytes[0]);
      assertEquals((byte)5, bytes[1]);
      assertEquals((byte)6, bytes[2]);
      assertEquals(-1, sm.readBytes(bytes));
      assertEquals((char)7, sm.readChar());
      assertEquals(new Double(8.0), new Double(sm.readDouble()));
      assertEquals(new Float(9.0), new Float(sm.readFloat()));
      assertEquals(10, sm.readInt());
      assertEquals(11l, sm.readLong());
      assertEquals("this is an object", sm.readObject());
      assertEquals((short)12, sm.readShort());
      assertEquals("this is a String", sm.readString());
   }


}
