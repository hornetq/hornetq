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

package org.hornetq.tests.unit.util;

import static org.hornetq.tests.util.RandomUtil.randomBoolean;
import static org.hornetq.tests.util.RandomUtil.randomByte;
import static org.hornetq.tests.util.RandomUtil.randomBytes;
import static org.hornetq.tests.util.RandomUtil.randomChar;
import static org.hornetq.tests.util.RandomUtil.randomDouble;
import static org.hornetq.tests.util.RandomUtil.randomFloat;
import static org.hornetq.tests.util.RandomUtil.randomInt;
import static org.hornetq.tests.util.RandomUtil.randomLong;
import static org.hornetq.tests.util.RandomUtil.randomShort;
import static org.hornetq.tests.util.RandomUtil.randomSimpleString;

import java.util.Iterator;

import org.hornetq.core.buffers.HornetQBuffer;
import org.hornetq.core.buffers.HornetQBuffers;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.SimpleString;
import org.hornetq.utils.TypedProperties;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class TypedPropertiesTest extends UnitTestCase
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
      props.putSimpleStringProperty(key, randomSimpleString());

      TypedProperties copy = new TypedProperties(props);

      assertEquals(props.getEncodeSize(), copy.getEncodeSize());
      assertEquals(props.getPropertyNames(), copy.getPropertyNames());

      assertTrue(copy.containsProperty(key));
      assertEquals(props.getProperty(key), copy.getProperty(key));
   }

   public void testRemove() throws Exception
   {
      props.putSimpleStringProperty(key, randomSimpleString());

      assertTrue(props.containsProperty(key));
      assertNotNull(props.getProperty(key));

      props.removeProperty(key);

      assertFalse(props.containsProperty(key));
      assertNull(props.getProperty(key));
   }

   public void testClear() throws Exception
   {
      props.putSimpleStringProperty(key, randomSimpleString());

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
      props.putSimpleStringProperty(key, null);
      assertTrue(props.containsProperty(key));
      assertNull(props.getProperty(key));
   }

   public void testBytesPropertyWithNull() throws Exception
   {
      props.putBytesProperty(key, null);

      assertTrue(props.containsProperty(key));
      byte[] bb = (byte[]) props.getProperty(key);
      assertNull(bb);
   }

   public void testTypedProperties() throws Exception
   {
      SimpleString longKey = randomSimpleString();
      long longValue = randomLong();
      SimpleString simpleStringKey = randomSimpleString();
      SimpleString simpleStringValue = randomSimpleString();
      TypedProperties otherProps = new TypedProperties();
      otherProps.putLongProperty(longKey, longValue);
      otherProps.putSimpleStringProperty(simpleStringKey, simpleStringValue);
      
      props.putTypedProperties(otherProps);
      
      long ll = props.getLongProperty(longKey);
      assertEquals(longValue, ll);
      SimpleString ss = props.getSimpleStringProperty(simpleStringKey);
      assertEquals(simpleStringValue, ss);
   }
   
   public void testEmptyTypedProperties() throws Exception
   {     
      assertEquals(0, props.getPropertyNames().size());
      
      props.putTypedProperties(new TypedProperties());
      
      assertEquals(0, props.getPropertyNames().size());
   }
   
   public void testNullTypedProperties() throws Exception
   {     
      assertEquals(0, props.getPropertyNames().size());
      
      props.putTypedProperties(null);
      
      assertEquals(0, props.getPropertyNames().size());
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
      props.putSimpleStringProperty(randomSimpleString(), randomSimpleString());
      props.putSimpleStringProperty(randomSimpleString(), null);
      SimpleString keyToRemove = randomSimpleString();
      props.putSimpleStringProperty(keyToRemove, randomSimpleString());

      HornetQBuffer buffer = HornetQBuffers.dynamicBuffer(1024); 
      props.encode(buffer);
      
      assertEquals(props.getEncodeSize(), buffer.writerIndex());

      TypedProperties decodedProps = new TypedProperties();
      decodedProps.decode(buffer);

      assertEqualsTypeProperties(props, decodedProps);

      buffer.clear();
      
      
      // After removing a property, you should still be able to encode the Property
      props.removeProperty(keyToRemove);
      props.encode(buffer);
      
      assertEquals(props.getEncodeSize(), buffer.writerIndex());
   }
   
   public void testEncodeDecodeEmpty() throws Exception
   {
      TypedProperties emptyProps = new TypedProperties();

      HornetQBuffer buffer = HornetQBuffers.dynamicBuffer(1024); 
      emptyProps.encode(buffer);
      
      assertEquals(props.getEncodeSize(), buffer.writerIndex());

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
