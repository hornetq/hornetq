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
import static org.hornetq.tests.util.RandomUtil.randomDouble;
import static org.hornetq.tests.util.RandomUtil.randomFloat;
import static org.hornetq.tests.util.RandomUtil.randomInt;
import static org.hornetq.tests.util.RandomUtil.randomShort;
import static org.hornetq.tests.util.RandomUtil.randomSimpleString;
import static org.hornetq.tests.util.UnitTestCase.assertEqualsByteArrays;
import junit.framework.TestCase;

import org.hornetq.core.message.PropertyConversionException;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.utils.SimpleString;
import org.hornetq.utils.TypedProperties;

/**
 * A TypedPropertiesConversionTest
 *
 * @author jmesnil
 *
 *
 */
public class TypedPropertiesConversionTest extends TestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private TypedProperties props;

   private SimpleString key;

   private SimpleString unknownKey = new SimpleString("this.key.is.never.used");

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      key = randomSimpleString();
      props = new TypedProperties();
   }

   @Override
   protected void tearDown() throws Exception
   {
      key = null;
      props = null;

      super.tearDown();
   }

   public void testBooleanProperty() throws Exception
   {
      Boolean val = randomBoolean();
      props.putBooleanProperty(key, val);

      assertEquals(val, props.getBooleanProperty(key));
      assertEquals(new SimpleString(Boolean.toString(val)), props.getSimpleStringProperty(key));

      props.putSimpleStringProperty(key, new SimpleString(Boolean.toString(val)));
      assertEquals(val, props.getBooleanProperty(key));
      
      try
      {
         props.putByteProperty(key, randomByte());
         props.getBooleanProperty(key);
         fail();
      }
      catch (PropertyConversionException e)
      {
      }
      
      assertFalse(props.getBooleanProperty(unknownKey));
   }
   
   public void testCharProperty() throws Exception
   {
      Character val = RandomUtil.randomChar();
      props.putCharProperty(key, val);

      assertEquals(val, props.getCharProperty(key));
      assertEquals(new SimpleString(Character.toString(val)), props.getSimpleStringProperty(key));

      try
      {
         props.putByteProperty(key, randomByte());
         props.getCharProperty(key);
         fail();
      }
      catch (PropertyConversionException e)
      {
      }
      
      try
      {
         props.getCharProperty(unknownKey);
         fail();
      }
      catch (NullPointerException e)
      {
      }
   }

   public void testByteProperty() throws Exception
   {
      Byte val = randomByte();
      props.putByteProperty(key, val);

      assertEquals(val, props.getByteProperty(key));
      assertEquals(new SimpleString(Byte.toString(val)), props.getSimpleStringProperty(key));

      props.putSimpleStringProperty(key, new SimpleString(Byte.toString(val)));
      assertEquals(val, props.getByteProperty(key));
      
      try
      {
         props.putBooleanProperty(key, randomBoolean());
         props.getByteProperty(key);
         fail();
      }
      catch (PropertyConversionException e)
      {
      }
      
      try
      {
         props.getByteProperty(unknownKey);
         fail();
      }
      catch (NumberFormatException e)
      {
      }
   }
   
   public void testIntProperty() throws Exception
   {
      Integer val = randomInt();
      props.putIntProperty(key, val);

      assertEquals(val, props.getIntProperty(key));
      assertEquals(new SimpleString(Integer.toString(val)), props.getSimpleStringProperty(key));

      props.putSimpleStringProperty(key, new SimpleString(Integer.toString(val)));
      assertEquals(val, props.getIntProperty(key));
      
      Byte byteVal = randomByte();
      props.putByteProperty(key, byteVal);
      assertEquals(Integer.valueOf(byteVal), props.getIntProperty(key));
      
      try
      {
         props.putBooleanProperty(key, randomBoolean());
         props.getIntProperty(key);
         fail();
      }
      catch (PropertyConversionException e)
      {
      }
      
      try
      {
         props.getIntProperty(unknownKey);
         fail();
      }
      catch (NumberFormatException e)
      {
      }
   }

   public void testLongProperty() throws Exception
   {
      Long val = RandomUtil.randomLong();
      props.putLongProperty(key, val);

      assertEquals(val, props.getLongProperty(key));
      assertEquals(new SimpleString(Long.toString(val)), props.getSimpleStringProperty(key));

      props.putSimpleStringProperty(key, new SimpleString(Long.toString(val)));
      assertEquals(val, props.getLongProperty(key));
      
      Byte byteVal = randomByte();
      props.putByteProperty(key, byteVal);
      assertEquals(Long.valueOf(byteVal), props.getLongProperty(key));

      Short shortVal = randomShort();
      props.putShortProperty(key, shortVal);
      assertEquals(Long.valueOf(shortVal), props.getLongProperty(key));

      Integer intVal = randomInt();
      props.putIntProperty(key, intVal);
      assertEquals(Long.valueOf(intVal), props.getLongProperty(key));

      try
      {
         props.putBooleanProperty(key, randomBoolean());
         props.getLongProperty(key);
         fail();
      }
      catch (PropertyConversionException e)
      {
      }
      
      try
      {
         props.getLongProperty(unknownKey);
         fail();
      }
      catch (NumberFormatException e)
      {
      }
   }
   
   public void testDoubleProperty() throws Exception
   {
      Double val = randomDouble();
      props.putDoubleProperty(key, val);

      assertEquals(val, props.getDoubleProperty(key));
      assertEquals(new SimpleString(Double.toString(val)), props.getSimpleStringProperty(key));

      props.putSimpleStringProperty(key, new SimpleString(Double.toString(val)));
      assertEquals(val, props.getDoubleProperty(key));
      
      try
      {
         props.putBooleanProperty(key, randomBoolean());
         props.getDoubleProperty(key);
         fail();
      }
      catch (PropertyConversionException e)
      {
      }

      try
      {
         props.getDoubleProperty(unknownKey);
         fail();
      }
      catch (Exception e)
      {
      }
   }

   public void testFloatProperty() throws Exception
   {
      Float val = randomFloat();
      props.putFloatProperty(key, val);

      assertEquals(val, props.getFloatProperty(key));
      assertEquals(Double.valueOf(val), props.getDoubleProperty(key));
      assertEquals(new SimpleString(Float.toString(val)), props.getSimpleStringProperty(key));

      props.putSimpleStringProperty(key, new SimpleString(Float.toString(val)));
      assertEquals(val, props.getFloatProperty(key));

      try
      {
         props.putBooleanProperty(key, randomBoolean());
         props.getFloatProperty(key);
         fail();
      }
      catch (PropertyConversionException e)
      {
      }
      
      try
      {
         props.getFloatProperty(unknownKey);
         fail();
      }
      catch (Exception e)
      {
      }
   }

   public void testShortProperty() throws Exception
   {
      Short val = randomShort();
      props.putShortProperty(key, val);

      assertEquals(val, props.getShortProperty(key));
      assertEquals(Integer.valueOf(val), props.getIntProperty(key));
      assertEquals(new SimpleString(Short.toString(val)), props.getSimpleStringProperty(key));

      props.putSimpleStringProperty(key, new SimpleString(Short.toString(val)));
      assertEquals(val, props.getShortProperty(key));
      
      Byte byteVal = randomByte();
      props.putByteProperty(key, byteVal);
      assertEquals(Short.valueOf(byteVal), props.getShortProperty(key));
      
      try
      {
         props.putBooleanProperty(key, randomBoolean());
         props.getShortProperty(key);
         fail();
      }
      catch (PropertyConversionException e)
      {
      }

      try
      {
         props.getShortProperty(unknownKey);
         fail();
      }
      catch (NumberFormatException e)
      {
      }
   }

   public void testSimpleStringProperty() throws Exception
   {
      SimpleString strVal = randomSimpleString();
      props.putSimpleStringProperty(key, strVal);
      assertEquals(strVal, props.getSimpleStringProperty(key));
   }
   
   public void testBytesProperty() throws Exception
   {
      byte[] val = RandomUtil.randomBytes();
      props.putBytesProperty(key, val);

      assertEqualsByteArrays(val, props.getBytesProperty(key));
     
      try
      {
         props.putBooleanProperty(key, randomBoolean());
         props.getBytesProperty(key);
         fail();
      }
      catch (PropertyConversionException e)
      {
      }

      assertNull(props.getBytesProperty(unknownKey));
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
