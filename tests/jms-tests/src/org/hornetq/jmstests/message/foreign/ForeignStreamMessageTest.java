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

package org.hornetq.jmstests.message.foreign;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.StreamMessage;

import org.hornetq.jmstests.message.SimpleJMSStreamMessage;

/**
 * Tests the delivery/receipt of a foreign stream message
 *
 *
 * @author <a href="mailto:a.walker@base2group.com>Aaron Walker</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class ForeignStreamMessageTest extends ForeignMessageTest
{

   protected Message createForeignMessage() throws Exception
   {
      SimpleJMSStreamMessage m = new SimpleJMSStreamMessage();

      log.debug("creating JMS Message type " + m.getClass().getName());

      m.writeBoolean(true);
      m.writeBytes("jboss".getBytes());
      m.writeChar('c');
      m.writeDouble(1.0D);
      m.writeFloat(2.0F);
      m.writeInt(3);
      m.writeLong(4L);
      m.writeObject("object");
      m.writeShort((short)5);
      m.writeString("stringvalue");

      return m;
   }

   protected void assertEquivalent(Message m, int mode, boolean redelivery) throws JMSException
   {
      super.assertEquivalent(m, mode, redelivery);

      StreamMessage sm = (StreamMessage)m;

      assertTrue(sm.readBoolean());

      byte bytes[] = new byte[5];
      sm.readBytes(bytes);
      String s = new String(bytes);
      assertEquals("jboss", s);
      assertEquals(-1, sm.readBytes(bytes));

      assertEquals(sm.readChar(), 'c');
      assertEquals(sm.readDouble(), 1.0D, 0.0D);
      assertEquals(sm.readFloat(), 2.0F, 0.0F);
      assertEquals(sm.readInt(), 3);
      assertEquals(sm.readLong(), 4L);
      assertEquals(sm.readObject(), "object");
      assertEquals(sm.readShort(), (short)5);
      assertEquals(sm.readString(), "stringvalue");
   }

}
