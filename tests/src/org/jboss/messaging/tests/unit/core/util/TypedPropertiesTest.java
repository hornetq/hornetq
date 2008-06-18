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

package org.jboss.messaging.tests.unit.core.util;

import static org.jboss.messaging.tests.util.RandomUtil.randomBoolean;
import static org.jboss.messaging.tests.util.RandomUtil.randomByte;
import static org.jboss.messaging.tests.util.RandomUtil.randomBytes;
import static org.jboss.messaging.tests.util.RandomUtil.randomChar;
import static org.jboss.messaging.tests.util.RandomUtil.randomDouble;
import static org.jboss.messaging.tests.util.RandomUtil.randomFloat;
import static org.jboss.messaging.tests.util.RandomUtil.randomInt;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;
import static org.jboss.messaging.tests.util.RandomUtil.randomShort;
import static org.jboss.messaging.tests.util.RandomUtil.randomSimpleString;
import static org.jboss.messaging.tests.util.UnitTestCase.assertEqualsByteArrays;

import java.util.Iterator;

import junit.framework.TestCase;

import org.jboss.messaging.core.remoting.impl.mina.IoBufferWrapper;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.MessagingBuffer;
import org.jboss.messaging.util.SimpleString;
import org.jboss.messaging.util.TypedProperties;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class TypedPropertiesTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   private static void assertEqualsTypeProperties(TypedProperties expected,
         TypedProperties actual)
   {
      assertNotNull(expected);
      assertNotNull(actual);
      assertEquals(expected.getEncodeSize(), actual.getEncodeSize());
      assertEquals(expected.getPropertyNames(), actual.getPropertyNames());
      Iterator<SimpleString> iterator = actual.getPropertyNames().iterator();
      while (iterator.hasNext())
      {
         SimpleString key = (SimpleString) iterator.next();
         Object expectedValue = expected.getProperty(key);
         Object actualValue = actual.getProperty(key);
         if ((expectedValue instanceof byte[])
               && (actualValue instanceof byte[]))
         {
            byte[] expectedBytes = (byte[]) expectedValue;
            byte[] actualBytes = (byte[]) actualValue;
            UnitTestCase.assertEqualsByteArrays(expectedBytes, actualBytes);
         } else
         {
            assertEquals(expectedValue, actualValue);
         }
      }
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   private TypedProperties props;
   private SimpleString key;

   public void testCopyContructor() throws Exception
   {
      props.putStringProperty(key, randomSimpleString());

      TypedProperties copy = new TypedProperties(props);

      assertEquals(props.getEncodeSize(), copy.getEncodeSize());
      assertEquals(props.getPropertyNames(), copy.getPropertyNames());

      assertTrue(copy.containsProperty(key));
      assertEquals(props.getProperty(key), copy.getProperty(key));
   }

   public void testRemove() throws Exception
   {
      props.putStringProperty(key, randomSimpleString());

      assertTrue(props.containsProperty(key));
      assertNotNull(props.getProperty(key));

      props.removeProperty(key);

      assertFalse(props.containsProperty(key));
      assertNull(props.getProperty(key));
   }

   public void testClear() throws Exception
   {
      props.putStringProperty(key, randomSimpleString());

      assertTrue(props.containsProperty(key));
      assertNotNull(props.getProperty(key));

      props.clear();

      assertFalse(props.containsProperty(key));
      assertNull(props.getProperty(key));
   }

   public void testKey() throws Exception
   {
      props.putBooleanProperty(key, true);
      boolean bool = (Boolean) props.getProperty(key);
      assertEquals(true, bool);

      props.putCharProperty(key, 'a');
      char c = (Character) props.getProperty(key);
      assertEquals('a', c);
   }

   public void testGetPropertyOnEmptyProperties() throws Exception
   {
      assertFalse(props.containsProperty(key));
      assertNull(props.getProperty(key));
   }
   
   public void testRemovePropertyOnEmptyProperties() throws Exception
   {
      assertFalse(props.containsProperty(key));
      assertNull(props.removeProperty(key));
   }
   
   public void testNullProperty() throws Exception
   {
      props.putStringProperty(key, null);
      assertTrue(props.containsProperty(key));
      assertNull(props.getProperty(key));
   }

   public void testBooleanProperty() throws Exception
   {
      props.putBooleanProperty(key, true);
      boolean bool = (Boolean) props.getProperty(key);
      assertEquals(true, bool);

      props.putBooleanProperty(key, false);
      bool = (Boolean) props.getProperty(key);
      assertEquals(false, bool);
   }

   public void testByteProperty() throws Exception
   {
      byte b = randomByte();
      props.putByteProperty(key, b);
      byte bb = (Byte) props.getProperty(key);
      assertEquals(b, bb);
   }

   public void testBytesProperty() throws Exception
   {
      byte[] b = randomBytes();
      props.putBytesProperty(key, b);
      byte[] bb = (byte[]) props.getProperty(key);
      assertEqualsByteArrays(b, bb);
   }

   public void testBytesPropertyWithNull() throws Exception
   {
      props.putBytesProperty(key, null);

      assertTrue(props.containsProperty(key));
      byte[] bb = (byte[]) props.getProperty(key);
      assertNull(bb);
   }

   public void testFloatProperty() throws Exception
   {
      float f = randomFloat();
      props.putFloatProperty(key, f);
      float ff = (Float) props.getProperty(key);
      assertEquals(f, ff);
   }

   public void testDoubleProperty() throws Exception
   {
      double d = randomDouble();
      props.putDoubleProperty(key, d);
      double dd = (Double) props.getProperty(key);
      assertEquals(d, dd);
   }

   public void testShortProperty() throws Exception
   {
      short s = randomShort();
      props.putShortProperty(key, s);
      short ss = (Short) props.getProperty(key);
      assertEquals(s, ss);
   }

   public void testIntProperty() throws Exception
   {
      int i = randomInt();
      props.putIntProperty(key, i);
      int ii = (Integer) props.getProperty(key);
      assertEquals(i, ii);
   }

   public void testLongProperty() throws Exception
   {
      long l = randomLong();
      props.putLongProperty(key, l);
      long ll = (Long) props.getProperty(key);
      assertEquals(l, ll);
   }

   public void testCharProperty() throws Exception
   {
      char c = randomChar();
      props.putCharProperty(key, c);
      char cc = (Character) props.getProperty(key);
      assertEquals(c, cc);
   }

   public void testEncodeDecode() throws Exception
   {
      props.putByteProperty(randomSimpleString(), randomByte());
      props.putBytesProperty(randomSimpleString(), randomBytes());
      props.putBytesProperty(randomSimpleString(), null);
      props.putBooleanProperty(randomSimpleString(), randomBoolean());
      props.putShortProperty(randomSimpleString(), randomShort());
      props.putIntProperty(randomSimpleString(), randomInt());
      props.putLongProperty(randomSimpleString(), randomLong());
      props.putFloatProperty(randomSimpleString(), randomFloat());
      props.putDoubleProperty(randomSimpleString(), randomDouble());
      props.putCharProperty(randomSimpleString(), randomChar());
      props.putStringProperty(randomSimpleString(), randomSimpleString());
      props.putStringProperty(randomSimpleString(), null);
      SimpleString keyToRemove = randomSimpleString();
      props.putStringProperty(keyToRemove, randomSimpleString());

      MessagingBuffer buffer = new IoBufferWrapper(1024);
      props.encode(buffer);
      
      assertEquals(props.getEncodeSize(), buffer.position());

      buffer.flip();

      TypedProperties decodedProps = new TypedProperties();
      decodedProps.decode(buffer);

      assertEqualsTypeProperties(props, decodedProps);
      
      buffer.flip();
      
      // After removing a property, you should still be able to encode the Property
      props.removeProperty(keyToRemove);
      props.encode(buffer);
      
      assertEquals(props.getEncodeSize(), buffer.position());
   }
   
   public void testEncodeDecodeEmpty() throws Exception
   {
      TypedProperties emptyProps = new TypedProperties();

      MessagingBuffer buffer = new IoBufferWrapper(1024);
      emptyProps.encode(buffer);
      
      assertEquals(props.getEncodeSize(), buffer.position());

      buffer.flip();

      TypedProperties decodedProps = new TypedProperties();
      decodedProps.decode(buffer);

      assertEqualsTypeProperties(emptyProps, decodedProps);
   }

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      props = new TypedProperties();
      key = randomSimpleString();
   }

   @Override
   protected void tearDown() throws Exception
   {
      key = null;
      props = null;

      super.tearDown();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
