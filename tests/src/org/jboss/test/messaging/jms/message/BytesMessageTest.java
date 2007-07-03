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


import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;

/**
 * A test that sends/receives bytes messages to the JMS provider and verifies their integrity.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class BytesMessageTest extends MessageTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public BytesMessageTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      message = session.createBytesMessage();
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

      BytesMessage bm = (BytesMessage)m;

      bm.writeBoolean(true);
      bm.writeByte((byte)3);
      bm.writeBytes(new byte[] {(byte)4, (byte)5, (byte)6});
      bm.writeChar((char)7);
      bm.writeDouble(8.0);
      bm.writeFloat(9.0f);
      bm.writeInt(10);
      bm.writeLong(11l);
      bm.writeShort((short)12);
      bm.writeUTF("this is an UTF String");
      bm.reset();
   }

   protected void assertEquivalent(Message m, int mode) throws JMSException
   {
      super.assertEquivalent(m, mode);

      BytesMessage bm = (BytesMessage)m;

      assertEquals(true, bm.readBoolean());
      assertEquals((byte)3, bm.readByte());
      byte[] bytes = new byte[3];
      bm.readBytes(bytes);
      assertEquals((byte)4, bytes[0]);
      assertEquals((byte)5, bytes[1]);
      assertEquals((byte)6, bytes[2]);
      assertEquals((char)7, bm.readChar());
      assertEquals(new Double(8.0), new Double(bm.readDouble()));
      assertEquals(new Float(9.0), new Float(bm.readFloat()));
      assertEquals(10, bm.readInt());
      assertEquals(11l, bm.readLong());
      assertEquals((short)12, bm.readShort());
      assertEquals("this is an UTF String", bm.readUTF());
   }


}
