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

package org.jboss.messaging.tests.unit.core.remoting;

import static org.jboss.messaging.tests.util.RandomUtil.*;
import static org.jboss.messaging.tests.util.UnitTestCase.*;
import junit.framework.TestCase;

import org.jboss.messaging.core.remoting.MessagingBuffer;
import org.jboss.messaging.tests.util.RandomUtil;
import org.jboss.messaging.util.SimpleString;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 *
 * @version <tt>$Revision$</tt>
 */
public abstract class MessagingBufferTestBase extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   private MessagingBuffer wrapper;

   @Override
   protected void setUp() throws Exception
   {
      wrapper = createBuffer();
   }

   @Override
   protected void tearDown() throws Exception
   {
      wrapper = null;
   }

   protected abstract MessagingBuffer createBuffer();

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
      wrapper.putByte(b);

      wrapper.flip();

      assertEquals(b, wrapper.getByte());
   }

   public void testUnsignedByte() throws Exception
   {
      byte b = (byte) 0xff;
      wrapper.putByte(b);

      wrapper.flip();

      assertEquals(255, wrapper.getUnsignedByte());

      wrapper.rewind();

      b = (byte) 0xf;
      wrapper.putByte(b);

      wrapper.flip();

      assertEquals(b, wrapper.getUnsignedByte());
   }



   public void testBytes() throws Exception
   {
      byte[] bytes = randomBytes();
      wrapper.putBytes(bytes);

      wrapper.flip();

      byte[] b = new byte[bytes.length];
      wrapper.getBytes(b);
      assertEqualsByteArrays(bytes, b);
   }

   public void testBytesWithLength() throws Exception
   {
      byte[] bytes = randomBytes();
      // put only half of the bytes
      wrapper.putBytes(bytes, 0, bytes.length / 2);

      wrapper.flip();

      byte[] b = new byte[bytes.length / 2];
      wrapper.getBytes(b, 0, b.length);
      assertEqualsByteArrays(b.length, bytes, b);
   }

   public void testPutTrueBoolean() throws Exception
   {
      wrapper.putBoolean(true);

      wrapper.flip();

      assertTrue(wrapper.getBoolean());
   }

   public void testPutFalseBoolean() throws Exception
   {
      wrapper.putBoolean(false);

      wrapper.flip();

      assertFalse(wrapper.getBoolean());
   }

   public void testChar() throws Exception
   {
      wrapper.putChar('a');

      wrapper.flip();

      assertEquals('a', wrapper.getChar());
   }

   public void testInt() throws Exception
   {
      int i = randomInt();
      wrapper.putInt(i);

      wrapper.flip();

      assertEquals(i, wrapper.getInt());
   }

   public void testIntAtPosition() throws Exception
   {
      int firstInt = randomInt();
      int secondInt = randomInt();

      wrapper.putInt(secondInt);
      wrapper.putInt(secondInt);
      // rewrite firstInt at the beginning
      wrapper.putInt(0, firstInt);

      wrapper.flip();

      assertEquals(firstInt, wrapper.getInt());
      assertEquals(secondInt, wrapper.getInt());
   }

   public void testLong() throws Exception
   {
      long l = randomLong();
      wrapper.putLong(l);

      wrapper.flip();

      assertEquals(l, wrapper.getLong());
   }

   public void testUnsignedShort() throws Exception
   {
      short s1 = Short.MAX_VALUE;

      wrapper.putShort(s1);

      wrapper.flip();

      int s2 = wrapper.getUnsignedShort();

      assertEquals(s1, s2);

      wrapper.rewind();

      s1 = Short.MIN_VALUE;

      wrapper.putShort(s1);

      wrapper.flip();

      s2 = wrapper.getUnsignedShort();

      assertEquals(s1 * -1, s2);

      wrapper.rewind();

      s1 = -1;

      wrapper.putShort(s1);

      wrapper.flip();

      s2 = wrapper.getUnsignedShort();

      // / The max of an unsigned short
      // (http://en.wikipedia.org/wiki/Unsigned_short)
      assertEquals(s2, 65535);
   }

   public void testShort() throws Exception
   {
      wrapper.putShort((short) 1);

      wrapper.flip();

      assertEquals((short)1, wrapper.getShort());
   }

   public void testDouble() throws Exception
   {
      double d = randomDouble();
      wrapper.putDouble(d);

      wrapper.flip();

      assertEquals(d, wrapper.getDouble());
   }

   public void testFloat() throws Exception
   {
      float f = randomFloat();
      wrapper.putFloat(f);

      wrapper.flip();

      assertEquals(f, wrapper.getFloat());
   }

   public void testUTF() throws Exception
   {
      String str = randomString();
      wrapper.putUTF(str);

      wrapper.flip();

      assertEquals(str, wrapper.getUTF());
   }

   public void testArray() throws Exception
   {
      byte[] bytes = randomBytes(128);
      wrapper.putBytes(bytes);

      wrapper.flip();

      byte[] array = wrapper.array();
      assertEquals(wrapper.capacity(), array.length);
      assertEqualsByteArrays(128, bytes, wrapper.array());
   }

   public void testRewind() throws Exception
   {
      int i = randomInt();
      wrapper.putInt(i);

      wrapper.flip();

      assertEquals(i, wrapper.getInt());

      wrapper.rewind();

      assertEquals(i, wrapper.getInt());
   }

   public void testRemaining() throws Exception
   {
      int capacity = wrapper.capacity();
      assertEquals(capacity, wrapper.remaining());

      // fill 1/3 of the buffer
      int fill = capacity / 3;
      byte[] bytes = randomBytes(fill);
      wrapper.putBytes(bytes);

      // check the remaining is 2/3
      assertEquals(capacity - fill, wrapper.remaining());
   }

   public void testPosition() throws Exception
   {
      assertEquals(0, wrapper.position());

      byte[] bytes = randomBytes(128);
      wrapper.putBytes(bytes);

      assertEquals(bytes.length, wrapper.position());

      wrapper.position(0);
      assertEquals(0, wrapper.position());
   }

   public void testLimit() throws Exception
   {
      assertEquals(wrapper.capacity(), wrapper.limit());

      byte[] bytes = randomBytes(128);
      wrapper.putBytes(bytes);

      assertTrue(wrapper.limit() >= bytes.length);

      wrapper.limit(128);
      assertEquals(128, wrapper.limit());
   }

   public void testSlice() throws Exception
   {
      byte[] bytes = randomBytes(128);
      wrapper.putBytes(bytes);

      wrapper.position(0);
      wrapper.limit(128);

      MessagingBuffer slicedBuffer = wrapper.slice();
      assertEquals(128, slicedBuffer.capacity());

      byte[] slicedBytes = new byte[128];
      slicedBuffer.getBytes(slicedBytes);

      assertEqualsByteArrays(bytes, slicedBytes);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private String putAndGetNullableString(String nullableString) throws Exception
   {
      wrapper.putNullableString(nullableString);

      wrapper.flip();

      return wrapper.getNullableString();
   }

   private SimpleString putAndGetNullableSimpleString(SimpleString nullableSimpleString) throws Exception
   {
      wrapper.putNullableSimpleString(nullableSimpleString);

      wrapper.flip();

      return wrapper.getNullableSimpleString();
   }

   // Inner classes -------------------------------------------------
}
