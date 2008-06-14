/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.integration.core.remoting.mina;

import junit.framework.TestCase;
import static org.jboss.messaging.tests.unit.core.remoting.impl.CodecAssert.assertEqualsByteArrays;
import org.jboss.messaging.tests.util.RandomUtil;
import static org.jboss.messaging.tests.util.RandomUtil.*;
import org.jboss.messaging.util.MessagingBuffer;
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
   
   public void testBytes() throws Exception
   {
      byte[] bytes = randomBytes();
      wrapper.putBytes(bytes);
      
      wrapper.flip();
      
      byte[] b = new byte[bytes.length];
      wrapper.getBytes(b);
      assertEqualsByteArrays(bytes, b);
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
