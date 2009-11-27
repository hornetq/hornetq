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

package org.hornetq.tests.unit.core.remoting;

import static org.hornetq.tests.util.RandomUtil.randomByte;
import static org.hornetq.tests.util.RandomUtil.randomBytes;
import static org.hornetq.tests.util.RandomUtil.randomDouble;
import static org.hornetq.tests.util.RandomUtil.randomFloat;
import static org.hornetq.tests.util.RandomUtil.randomInt;
import static org.hornetq.tests.util.RandomUtil.randomLong;
import static org.hornetq.tests.util.RandomUtil.randomString;

import org.hornetq.core.buffers.HornetQBuffer;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.SimpleString;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 *
 * @version <tt>$Revision$</tt>
 */
public abstract class HornetQBufferTestBase extends UnitTestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   private HornetQBuffer wrapper;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      
      wrapper = createBuffer();
   }

   @Override
   protected void tearDown() throws Exception
   {
      wrapper = null;
      
      super.tearDown();
   }

   protected abstract HornetQBuffer createBuffer();

   public void testNullString() throws Exception
   {
      assertNull(putAndGetNullableString(null));
   }

   public void testEmptyString() throws Exception
   {
      String result = putAndGetNullableString("");

      assertNotNull(result);
      assertEquals("", result);
   }

   public void testNonEmptyString() throws Exception
   {
      String junk = randomString();

      String result = putAndGetNullableString(junk);

      assertNotNull(result);
      assertEquals(junk, result);
   }

   public void testNullSimpleString() throws Exception
   {
      assertNull(putAndGetNullableSimpleString(null));
   }

   public void testEmptySimpleString() throws Exception
   {
      SimpleString emptySimpleString = new SimpleString("");
      SimpleString result = putAndGetNullableSimpleString(emptySimpleString);

      assertNotNull(result);
      assertEqualsByteArrays(emptySimpleString.getData(), result.getData());
   }

   public void testNonEmptySimpleString() throws Exception
   {
      SimpleString junk = RandomUtil.randomSimpleString();
      SimpleString result = putAndGetNullableSimpleString(junk);

      assertNotNull(result);
      assertEqualsByteArrays(junk.getData(), result.getData());
   }

   public void testByte() throws Exception
   {
      byte b = randomByte();
      wrapper.writeByte(b);

      assertEquals(b, wrapper.readByte());
   }

   public void testUnsignedByte() throws Exception
   {
      byte b = (byte) 0xff;
      wrapper.writeByte(b);

      assertEquals(255, wrapper.readUnsignedByte());

      b = (byte) 0xf;
      wrapper.writeByte(b);

      assertEquals(b, wrapper.readUnsignedByte());
   }



   public void testBytes() throws Exception
   {
      byte[] bytes = randomBytes();
      wrapper.writeBytes(bytes);

      byte[] b = new byte[bytes.length];
      wrapper.readBytes(b);
      assertEqualsByteArrays(bytes, b);
   }

   public void testBytesWithLength() throws Exception
   {
      byte[] bytes = randomBytes();
      // put only half of the bytes
      wrapper.writeBytes(bytes, 0, bytes.length / 2);

      byte[] b = new byte[bytes.length / 2];
      wrapper.readBytes(b, 0, b.length);
      assertEqualsByteArrays(b.length, bytes, b);
   }

   public void testPutTrueBoolean() throws Exception
   {
      wrapper.writeBoolean(true);

      assertTrue(wrapper.readBoolean());
   }

   public void testPutFalseBoolean() throws Exception
   {
      wrapper.writeBoolean(false);

      assertFalse(wrapper.readBoolean());
   }

   public void testChar() throws Exception
   {
      wrapper.writeChar('a');

      assertEquals('a', wrapper.readChar());
   }

   public void testInt() throws Exception
   {
      int i = randomInt();
      wrapper.writeInt(i);

      assertEquals(i, wrapper.readInt());
   }

   public void testIntAtPosition() throws Exception
   {
      int firstInt = randomInt();
      int secondInt = randomInt();

      wrapper.writeInt(secondInt);
      wrapper.writeInt(secondInt);
      // rewrite firstInt at the beginning
      wrapper.setInt(0, firstInt);

      assertEquals(firstInt, wrapper.readInt());
      assertEquals(secondInt, wrapper.readInt());
   }

   public void testLong() throws Exception
   {
      long l = randomLong();
      wrapper.writeLong(l);

      assertEquals(l, wrapper.readLong());
   }

   public void testUnsignedShort() throws Exception
   {
      short s1 = Short.MAX_VALUE;

      wrapper.writeShort(s1);

      int s2 = wrapper.readUnsignedShort();

      assertEquals(s1, s2);

      s1 = Short.MIN_VALUE;

      wrapper.writeShort(s1);

      s2 = wrapper.readUnsignedShort();

      assertEquals(s1 * -1, s2);

      s1 = -1;

      wrapper.writeShort(s1);

      s2 = wrapper.readUnsignedShort();

      // / The max of an unsigned short
      // (http://en.wikipedia.org/wiki/Unsigned_short)
      assertEquals(s2, 65535);
   }

   public void testShort() throws Exception
   {
      wrapper.writeShort((short) 1);

      assertEquals((short)1, wrapper.readShort());
   }

   public void testDouble() throws Exception
   {
      double d = randomDouble();
      wrapper.writeDouble(d);

      assertEquals(d, wrapper.readDouble());
   }

   public void testFloat() throws Exception
   {
      float f = randomFloat();
      wrapper.writeFloat(f);

      assertEquals(f, wrapper.readFloat());
   }

   public void testUTF() throws Exception
   {
      String str = randomString();
      wrapper.writeUTF(str);

      assertEquals(str, wrapper.readUTF());
   }

   public void testArray() throws Exception
   {
      byte[] bytes = randomBytes(128);
      wrapper.writeBytes(bytes);

      byte[] array = wrapper.toByteBuffer().array();
      assertEquals(wrapper.capacity(), array.length);
      assertEqualsByteArrays(128, bytes, wrapper.toByteBuffer().array());
   }

   public void testRewind() throws Exception
   {
      int i = randomInt();
      wrapper.writeInt(i);

      assertEquals(i, wrapper.readInt());

      wrapper.resetReaderIndex();
      
      assertEquals(i, wrapper.readInt());
   }

   public void testRemaining() throws Exception
   {
      int capacity = wrapper.capacity();

      // fill 1/3 of the buffer
      int fill = capacity / 3;
      byte[] bytes = randomBytes(fill);
      wrapper.writeBytes(bytes);

      // check the remaining is 2/3
      assertEquals(capacity - fill, wrapper.writableBytes());
   }

   public void testPosition() throws Exception
   {
      assertEquals(0, wrapper.writerIndex());

      byte[] bytes = randomBytes(128);
      wrapper.writeBytes(bytes);

      assertEquals(bytes.length, wrapper.writerIndex());

      wrapper.writerIndex(0);
      assertEquals(0, wrapper.writerIndex());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private String putAndGetNullableString(String nullableString) throws Exception
   {
      wrapper.writeNullableString(nullableString);

      return wrapper.readNullableString();
   }

   private SimpleString putAndGetNullableSimpleString(SimpleString nullableSimpleString) throws Exception
   {
      wrapper.writeNullableSimpleString(nullableSimpleString);

      return wrapper.readNullableSimpleString();
   }

   // Inner classes -------------------------------------------------
}
