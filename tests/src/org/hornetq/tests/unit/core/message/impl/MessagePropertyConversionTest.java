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

package org.hornetq.tests.unit.core.message.impl;

import static org.hornetq.tests.util.RandomUtil.randomBoolean;
import static org.hornetq.tests.util.RandomUtil.randomByte;
import static org.hornetq.tests.util.RandomUtil.randomDouble;
import static org.hornetq.tests.util.RandomUtil.randomFloat;
import static org.hornetq.tests.util.RandomUtil.randomInt;
import static org.hornetq.tests.util.RandomUtil.randomShort;
import static org.hornetq.tests.util.RandomUtil.randomSimpleString;
import static org.hornetq.tests.util.UnitTestCase.assertEqualsByteArrays;
import junit.framework.TestCase;

import org.hornetq.core.client.impl.ClientMessageImpl;
import org.hornetq.core.message.Message;
import org.hornetq.core.message.PropertyConversionException;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.utils.SimpleString;

/**
 * A MessagePropertyConversionTest
 *
 * @author jmesnil
 *
 *
 */
public class MessagePropertyConversionTest extends TestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private Message msg;

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
      msg = new ClientMessageImpl(false);
   }

   @Override
   protected void tearDown() throws Exception
   {
      key = null;
      msg = null;

      super.tearDown();
   }

   public void testBooleanProperty() throws Exception
   {
      Boolean val = randomBoolean();
      msg.putBooleanProperty(key, val);

      assertEquals(val, msg.getBooleanProperty(key));
      assertEquals(Boolean.toString(val), msg.getStringProperty(key));

      msg.putStringProperty(key, new SimpleString(Boolean.toString(val)));
      assertEquals(val, msg.getBooleanProperty(key));
      
      try
      {
         msg.putByteProperty(key, randomByte());
         msg.getBooleanProperty(key);
         fail();
      }
      catch (PropertyConversionException e)
      {
      }
      
      assertFalse(msg.getBooleanProperty(unknownKey));
   }

   public void testByteProperty() throws Exception
   {
      Byte val = randomByte();
      msg.putByteProperty(key, val);

      assertEquals(val, msg.getByteProperty(key));
      assertEquals(Byte.toString(val), msg.getStringProperty(key));

      msg.putStringProperty(key, new SimpleString(Byte.toString(val)));
      assertEquals(val, msg.getByteProperty(key));
      
      try
      {
         msg.putBooleanProperty(key, randomBoolean());
         msg.getByteProperty(key);
         fail();
      }
      catch (PropertyConversionException e)
      {
      }
      
      try
      {
         msg.getByteProperty(unknownKey);
         fail();
      }
      catch (NumberFormatException e)
      {
      }
   }
   
   public void testIntProperty() throws Exception
   {
      Integer val = randomInt();
      msg.putIntProperty(key, val);

      assertEquals(val, msg.getIntProperty(key));
      assertEquals(Integer.toString(val), msg.getStringProperty(key));

      msg.putStringProperty(key, new SimpleString(Integer.toString(val)));
      assertEquals(val, msg.getIntProperty(key));
      
      Byte byteVal = randomByte();
      msg.putByteProperty(key, byteVal);
      assertEquals(Integer.valueOf(byteVal), msg.getIntProperty(key));
      
      try
      {
         msg.putBooleanProperty(key, randomBoolean());
         msg.getIntProperty(key);
         fail();
      }
      catch (PropertyConversionException e)
      {
      }
      
      try
      {
         msg.getIntProperty(unknownKey);
         fail();
      }
      catch (NumberFormatException e)
      {
      }
   }

   public void testLongProperty() throws Exception
   {
      Long val = RandomUtil.randomLong();
      msg.putLongProperty(key, val);

      assertEquals(val, msg.getLongProperty(key));
      assertEquals(Long.toString(val), msg.getStringProperty(key));

      msg.putStringProperty(key, new SimpleString(Long.toString(val)));
      assertEquals(val, msg.getLongProperty(key));
      
      Byte byteVal = randomByte();
      msg.putByteProperty(key, byteVal);
      assertEquals(Long.valueOf(byteVal), msg.getLongProperty(key));

      Short shortVal = randomShort();
      msg.putShortProperty(key, shortVal);
      assertEquals(Long.valueOf(shortVal), msg.getLongProperty(key));

      Integer intVal = randomInt();
      msg.putIntProperty(key, intVal);
      assertEquals(Long.valueOf(intVal), msg.getLongProperty(key));

      try
      {
         msg.putBooleanProperty(key, randomBoolean());
         msg.getLongProperty(key);
         fail();
      }
      catch (PropertyConversionException e)
      {
      }
      
      try
      {
         msg.getLongProperty(unknownKey);
         fail();
      }
      catch (NumberFormatException e)
      {
      }
   }
   
   public void testDoubleProperty() throws Exception
   {
      Double val = randomDouble();
      msg.putDoubleProperty(key, val);

      assertEquals(val, msg.getDoubleProperty(key));
      assertEquals(Double.toString(val), msg.getStringProperty(key));

      msg.putStringProperty(key, new SimpleString(Double.toString(val)));
      assertEquals(val, msg.getDoubleProperty(key));
      
      try
      {
         msg.putBooleanProperty(key, randomBoolean());
         msg.getDoubleProperty(key);
         fail();
      }
      catch (PropertyConversionException e)
      {
      }

      try
      {
         msg.getDoubleProperty(unknownKey);
         fail();
      }
      catch (Exception e)
      {
      }
   }

   public void testFloatProperty() throws Exception
   {
      Float val = randomFloat();
      msg.putFloatProperty(key, val);

      assertEquals(val, msg.getFloatProperty(key));
      assertEquals(Double.valueOf(val), msg.getDoubleProperty(key));
      assertEquals(Float.toString(val), msg.getStringProperty(key));

      msg.putStringProperty(key, new SimpleString(Float.toString(val)));
      assertEquals(val, msg.getFloatProperty(key));

      try
      {
         msg.putBooleanProperty(key, randomBoolean());
         msg.getFloatProperty(key);
         fail();
      }
      catch (PropertyConversionException e)
      {
      }
      
      try
      {
         msg.getFloatProperty(unknownKey);
         fail();
      }
      catch (Exception e)
      {
      }
   }

   public void testShortProperty() throws Exception
   {
      Short val = randomShort();
      msg.putShortProperty(key, val);

      assertEquals(val, msg.getShortProperty(key));
      assertEquals(Integer.valueOf(val), msg.getIntProperty(key));
      assertEquals(Short.toString(val), msg.getStringProperty(key));

      msg.putStringProperty(key, new SimpleString(Short.toString(val)));
      assertEquals(val, msg.getShortProperty(key));
      
      Byte byteVal = randomByte();
      msg.putByteProperty(key, byteVal);
      assertEquals(Short.valueOf(byteVal), msg.getShortProperty(key));
      
      try
      {
         msg.putBooleanProperty(key, randomBoolean());
         msg.getShortProperty(key);
         fail();
      }
      catch (PropertyConversionException e)
      {
      }

      try
      {
         msg.getShortProperty(unknownKey);
         fail();
      }
      catch (NumberFormatException e)
      {
      }
   }

   public void testStringProperty() throws Exception
   {
      SimpleString strVal = randomSimpleString();
      msg.putStringProperty(key, strVal);
      assertEquals(strVal.toString(), msg.getStringProperty(key));
   }
   
   public void testBytesProperty() throws Exception
   {
      byte[] val = RandomUtil.randomBytes();
      msg.putBytesProperty(key, val);

      assertEqualsByteArrays(val, msg.getBytesProperty(key));
     
      try
      {
         msg.putBooleanProperty(key, randomBoolean());
         msg.getBytesProperty(key);
         fail();
      }
      catch (PropertyConversionException e)
      {
      }

      assertNull(msg.getBytesProperty(unknownKey));
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
