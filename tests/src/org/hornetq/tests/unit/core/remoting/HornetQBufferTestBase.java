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

import junit.framework.Assert;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.SimpleString;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.UnitTestCase;

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
      Assert.assertNull(putAndGetNullableString(null));
   }

   public void testEmptyString() throws Exception
   {
      String result = putAndGetNullableString("");

      Assert.assertNotNull(result);
      Assert.assertEquals("", result);
   }

   public void testNonEmptyString() throws Exception
   {
      String junk = RandomUtil.randomString();

      String result = putAndGetNullableString(junk);

      Assert.assertNotNull(result);
      Assert.assertEquals(junk, result);
   }

   public void testNullSimpleString() throws Exception
   {
      Assert.assertNull(putAndGetNullableSimpleString(null));
   }

   public void testEmptySimpleString() throws Exception
   {
      SimpleString emptySimpleString = new SimpleString("");
      SimpleString result = putAndGetNullableSimpleString(emptySimpleString);

      Assert.assertNotNull(result);
      UnitTestCase.assertEqualsByteArrays(emptySimpleString.getData(), result.getData());
   }

   public void testNonEmptySimpleString() throws Exception
   {
      SimpleString junk = RandomUtil.randomSimpleString();
      SimpleString result = putAndGetNullableSimpleString(junk);

      Assert.assertNotNull(result);
      UnitTestCase.assertEqualsByteArrays(junk.getData(), result.getData());
   }

   public void testByte() throws Exception
   {
      byte b = RandomUtil.randomByte();
      wrapper.writeByte(b);

      Assert.assertEquals(b, wrapper.readByte());
   }

   public void testUnsignedByte() throws Exception
   {
      byte b = (byte)0xff;
      wrapper.writeByte(b);

      Assert.assertEquals(255, wrapper.readUnsignedByte());

      b = (byte)0xf;
      wrapper.writeByte(b);

      Assert.assertEquals(b, wrapper.readUnsignedByte());
   }

   public void testBytes() throws Exception
   {
      byte[] bytes = RandomUtil.randomBytes();
      wrapper.writeBytes(bytes);

      byte[] b = new byte[bytes.length];
      wrapper.readBytes(b);
      UnitTestCase.assertEqualsByteArrays(bytes, b);
   }

   public void testBytesWithLength() throws Exception
   {
      byte[] bytes = RandomUtil.randomBytes();
      // put only half of the bytes
      wrapper.writeBytes(bytes, 0, bytes.length / 2);

      byte[] b = new byte[bytes.length / 2];
      wrapper.readBytes(b, 0, b.length);
      UnitTestCase.assertEqualsByteArrays(b.length, bytes, b);
   }

   public void testPutTrueBoolean() throws Exception
   {
      wrapper.writeBoolean(true);

      Assert.assertTrue(wrapper.readBoolean());
   }

   public void testPutFalseBoolean() throws Exception
   {
      wrapper.writeBoolean(false);

      Assert.assertFalse(wrapper.readBoolean());
   }

   public void testChar() throws Exception
   {
      wrapper.writeChar('a');

      Assert.assertEquals('a', wrapper.readChar());
   }

   public void testInt() throws Exception
   {
      int i = RandomUtil.randomInt();
      wrapper.writeInt(i);

      Assert.assertEquals(i, wrapper.readInt());
   }

   public void testIntAtPosition() throws Exception
   {
      int firstInt = RandomUtil.randomInt();
      int secondInt = RandomUtil.randomInt();

      wrapper.writeInt(secondInt);
      wrapper.writeInt(secondInt);
      // rewrite firstInt at the beginning
      wrapper.setInt(0, firstInt);

      Assert.assertEquals(firstInt, wrapper.readInt());
      Assert.assertEquals(secondInt, wrapper.readInt());
   }

   public void testLong() throws Exception
   {
      long l = RandomUtil.randomLong();
      wrapper.writeLong(l);

      Assert.assertEquals(l, wrapper.readLong());
   }

   public void testUnsignedShort() throws Exception
   {
      short s1 = Short.MAX_VALUE;

      wrapper.writeShort(s1);

      int s2 = wrapper.readUnsignedShort();

      Assert.assertEquals(s1, s2);

      s1 = Short.MIN_VALUE;

      wrapper.writeShort(s1);

      s2 = wrapper.readUnsignedShort();

      Assert.assertEquals(s1 * -1, s2);

      s1 = -1;

      wrapper.writeShort(s1);

      s2 = wrapper.readUnsignedShort();

      // / The max of an unsigned short
      // (http://en.wikipedia.org/wiki/Unsigned_short)
      Assert.assertEquals(s2, 65535);
   }

   public void testShort() throws Exception
   {
      wrapper.writeShort((short)1);

      Assert.assertEquals((short)1, wrapper.readShort());
   }

   public void testDouble() throws Exception
   {
      double d = RandomUtil.randomDouble();
      wrapper.writeDouble(d);

      Assert.assertEquals(d, wrapper.readDouble());
   }

   public void testFloat() throws Exception
   {
      float f = RandomUtil.randomFloat();
      wrapper.writeFloat(f);

      Assert.assertEquals(f, wrapper.readFloat());
   }

   public void testUTF() throws Exception
   {
      String str = RandomUtil.randomString();
      wrapper.writeUTF(str);

      Assert.assertEquals(str, wrapper.readUTF());
   }

   public void testArray() throws Exception
   {
      byte[] bytes = RandomUtil.randomBytes(128);
      wrapper.writeBytes(bytes);

      byte[] array = wrapper.toByteBuffer().array();
      Assert.assertEquals(wrapper.capacity(), array.length);
      UnitTestCase.assertEqualsByteArrays(128, bytes, wrapper.toByteBuffer().array());
   }

   public void testRewind() throws Exception
   {
      int i = RandomUtil.randomInt();
      wrapper.writeInt(i);

      Assert.assertEquals(i, wrapper.readInt());

      wrapper.resetReaderIndex();

      Assert.assertEquals(i, wrapper.readInt());
   }

   public void testRemaining() throws Exception
   {
      int capacity = wrapper.capacity();

      // fill 1/3 of the buffer
      int fill = capacity / 3;
      byte[] bytes = RandomUtil.randomBytes(fill);
      wrapper.writeBytes(bytes);

      // check the remaining is 2/3
      Assert.assertEquals(capacity - fill, wrapper.writableBytes());
   }

   public void testPosition() throws Exception
   {
      Assert.assertEquals(0, wrapper.writerIndex());

      byte[] bytes = RandomUtil.randomBytes(128);
      wrapper.writeBytes(bytes);

      Assert.assertEquals(bytes.length, wrapper.writerIndex());

      wrapper.writerIndex(0);
      Assert.assertEquals(0, wrapper.writerIndex());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private String putAndGetNullableString(final String nullableString) throws Exception
   {
      wrapper.writeNullableString(nullableString);

      return wrapper.readNullableString();
   }

   private SimpleString putAndGetNullableSimpleString(final SimpleString nullableSimpleString) throws Exception
   {
      wrapper.writeNullableSimpleString(nullableSimpleString);

      return wrapper.readNullableSimpleString();
   }

   // Inner classes -------------------------------------------------
}
