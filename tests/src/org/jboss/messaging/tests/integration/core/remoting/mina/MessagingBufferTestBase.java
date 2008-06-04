/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.integration.core.remoting.mina;

import static java.util.UUID.randomUUID;
import static org.jboss.messaging.tests.unit.core.remoting.impl.wireformat.CodecAssert.assertEqualsByteArrays;
import static org.jboss.messaging.tests.util.RandomUtil.randomByte;
import static org.jboss.messaging.tests.util.RandomUtil.randomBytes;
import static org.jboss.messaging.tests.util.RandomUtil.randomDouble;
import static org.jboss.messaging.tests.util.RandomUtil.randomFloat;
import static org.jboss.messaging.tests.util.RandomUtil.randomInt;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;
import junit.framework.TestCase;

import org.apache.mina.common.IoBuffer;
import org.jboss.messaging.core.remoting.impl.mina.IoBufferWrapper;
import org.jboss.messaging.tests.unit.core.remoting.impl.wireformat.CodecAssert;
import org.jboss.messaging.tests.util.RandomUtil;
import org.jboss.messaging.util.MessagingBuffer;

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
   protected abstract void flipBuffer();

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
      String junk = randomUUID().toString();

      String result = putAndGetNullableString(junk);

      assertNotNull(result);
      assertEquals(junk, result);
   }

   public void testByte() throws Exception
   {
      byte b = randomByte();
      wrapper.putByte(b);
      
      flipBuffer();
      
      assertEquals(b, wrapper.getByte());
   }
   
   public void testBytes() throws Exception
   {
      byte[] bytes = randomBytes();
      wrapper.putBytes(bytes);
      
      flipBuffer();
      
      byte[] b = new byte[bytes.length];
      wrapper.getBytes(b);
      assertEqualsByteArrays(bytes, b);
   }
   
   public void testPutTrueBoolean() throws Exception
   {
      wrapper.putBoolean(true);
      
      flipBuffer();
      
      assertTrue(wrapper.getBoolean());
   }

   public void testPutFalseBoolean() throws Exception
   {
      wrapper.putBoolean(false);
      
      flipBuffer();
      
      assertFalse(wrapper.getBoolean());
   }
      
   public void testChar() throws Exception
   {
      wrapper.putChar('a');
      
      flipBuffer();
      
      assertEquals('a', wrapper.getChar());
   }
   
   public void testInt() throws Exception
   {
      int i = randomInt();
      wrapper.putInt(i);
      
      flipBuffer();
      
      assertEquals(i, wrapper.getInt());
   }
   
   public void testShort() throws Exception
   {
      wrapper.putShort((short) 1);
      
      flipBuffer();
      
      assertEquals((short)1, wrapper.getShort());
   }
      
   public void testDouble() throws Exception
   {
      double d = randomDouble();
      wrapper.putDouble(d);
      
      flipBuffer();
      
      assertEquals(d, wrapper.getDouble());
   }
   
   public void testFloat() throws Exception
   {
      float f = randomFloat();
      wrapper.putFloat(f);
      
      flipBuffer();
      
      assertEquals(f, wrapper.getFloat());
   }
   
   public void testUTF() throws Exception
   {
      String str = randomString();
      wrapper.putUTF(str);
      
      flipBuffer();
      
      assertEquals(str, wrapper.getUTF());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private String putAndGetNullableString(String nullableString) throws Exception
   {
      wrapper.putNullableString(nullableString);

      flipBuffer();
      
      return wrapper.getNullableString();
   }
   // Inner classes -------------------------------------------------
}
