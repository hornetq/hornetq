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
import org.jboss.messaging.core.remoting.impl.mina.BufferWrapper;
import org.jboss.messaging.tests.unit.core.remoting.impl.wireformat.CodecAssert;
import org.jboss.messaging.tests.util.RandomUtil;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class BufferWrapperTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   private BufferWrapper wrapper;
   private IoBuffer buffer;

   @Override
   protected void setUp() throws Exception
   {
      buffer = IoBuffer.allocate(256);
      buffer.setAutoExpand(true);
      wrapper = new BufferWrapper(buffer);
   }

   @Override
   protected void tearDown() throws Exception
   {
      wrapper = null;
      buffer = null;

   }

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
      
      buffer.flip();
      
      assertEquals(b, wrapper.getByte());
   }
   
   public void testBytes() throws Exception
   {
      byte[] bytes = randomBytes();
      wrapper.putBytes(bytes);
      
      buffer.flip();
      
      byte[] b = new byte[bytes.length];
      wrapper.getBytes(b);
      assertEqualsByteArrays(bytes, b);
   }
   
   public void testPutTrueBoolean() throws Exception
   {
      wrapper.putBoolean(true);
      
      buffer.flip();
      
      assertTrue(wrapper.getBoolean());
   }

   public void testPutFalseBoolean() throws Exception
   {
      wrapper.putBoolean(false);
      
      buffer.flip();
      
      assertFalse(wrapper.getBoolean());
   }
      
   public void testChar() throws Exception
   {
      wrapper.putChar('a');
      
      buffer.flip();
      
      assertEquals('a', wrapper.getChar());
   }
   
   public void testInt() throws Exception
   {
      int i = randomInt();
      wrapper.putInt(i);
      
      buffer.flip();
      
      assertEquals(i, wrapper.getInt());
   }
   
   public void testShort() throws Exception
   {
      wrapper.putShort((short) 1);
      
      buffer.flip();
      
      assertEquals((short)1, wrapper.getShort());
   }
      
   public void testDouble() throws Exception
   {
      double d = randomDouble();
      wrapper.putDouble(d);
      
      buffer.flip();
      
      assertEquals(d, wrapper.getDouble());
   }
   
   public void testFloat() throws Exception
   {
      float f = randomFloat();
      wrapper.putFloat(f);
      
      buffer.flip();
      
      assertEquals(f, wrapper.getFloat());
   }
   
   public void testUTF() throws Exception
   {
      String str = randomString();
      wrapper.putUTF(str);
      
      buffer.flip();
      
      assertEquals(str, wrapper.getUTF());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private String putAndGetNullableString(String nullableString) throws Exception
   {
      wrapper.putNullableString(nullableString);

      buffer.flip();
      
      return wrapper.getNullableString();
   }
   // Inner classes -------------------------------------------------
}
