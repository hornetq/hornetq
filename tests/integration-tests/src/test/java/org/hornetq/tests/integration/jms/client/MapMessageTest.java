/*
 * Copyright 2005-2014 Red Hat, Inc.
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

package org.hornetq.tests.integration.jms.client;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.hornetq.tests.util.JMSTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Clebert Suconic
 */

public class MapMessageTest extends JMSTestBase
{

   private static final String QUEUE_NAME = "someQueue";

   private Session session;
   private MessageProducer queueProd;
   private MessageConsumer queueCons;

   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      createQueue(false, QUEUE_NAME);
      conn = cf.createConnection();
      conn.start();
      session = conn.createSession();
      queueProd = session.createProducer(session.createQueue(QUEUE_NAME));
      queueCons = session.createConsumer(session.createQueue(QUEUE_NAME));
   }

   @After
   public void tearDown() throws Exception
   {
      super.tearDown();
   }


   @Test
   public void testMapValues() throws Exception
   {
      try
         (
            Connection conn = cf.createConnection();
         )
      {
         Session session = conn.createSession();
         MapMessage m = session.createMapMessage();

         m.setString("nullValue", null);
         m.setBoolean("boolean", true);
         m.setByte("byte", (byte) 3);
         m.setBytes("bytes", new byte[]{(byte) 3, (byte) 4, (byte) 5});
         m.setChar("char", (char) 6);
         m.setDouble("double", 7.0);
         m.setFloat("float", 8.0f);
         m.setInt("int", 9);
         m.setLong("long", 10L);
         m.setObject("object", new String("this is an object"));
         m.setShort("short", (short) 11);
         m.setString("string", "this is a string");

         queueProd.send(m);

         MapMessage rm = (MapMessage) queueCons.receive(2000);

         assertNotNull(rm);

         assertNull(rm.getString("nullValue"));

         assertEquals(true, rm.getBoolean("boolean"));
         assertEquals((byte) 3, rm.getByte("byte"));
         byte[] bytes = rm.getBytes("bytes");
         assertEquals((byte) 3, bytes[0]);
         assertEquals((byte) 4, bytes[1]);
         assertEquals((byte) 5, bytes[2]);
         assertEquals((char) 6, rm.getChar("char"));
         assertEquals(new Double(7.0), new Double(rm.getDouble("double")));
         assertEquals(new Float(8.0f), new Float(rm.getFloat("float")));
         assertEquals(9, rm.getInt("int"));
         assertEquals(10L, rm.getLong("long"));
         assertEquals("this is an object", rm.getObject("object"));
         assertEquals((short) 11, rm.getShort("short"));
         assertEquals("this is a string", rm.getString("string"));

      }
   }

   // Protected -----------------------------------------------------

   protected void prepareMessage(final Message m) throws JMSException
   {
      MapMessage mm = (MapMessage) m;

   }

   protected void assertEquivalent(final Message m, final int mode, final boolean redelivery) throws JMSException
   {
      MapMessage mm = (MapMessage) m;

   }

}
