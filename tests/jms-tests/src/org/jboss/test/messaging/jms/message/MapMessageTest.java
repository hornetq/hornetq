/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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


import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;

/**
 * A test that sends/receives map messages to the JMS provider and verifies their integrity.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
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

   public void testNullValue() throws Exception
   {
      MapMessage m = session.createMapMessage();

      m.setString("nullValue", null);

      queueProd.send(m);

      MapMessage rm = (MapMessage)queueCons.receive(2000);
      
      assertNotNull(rm);
      
      assertNull(rm.getString("nullValue"));
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

   protected void assertEquivalent(Message m, int mode, boolean redelivery) throws JMSException
   {
      super.assertEquivalent(m, mode, redelivery);

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
