/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.jms.tests.message;


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
