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


package org.hornetq.tests.unit.jms.client;

import static org.hornetq.tests.util.RandomUtil.randomBoolean;
import static org.hornetq.tests.util.RandomUtil.randomByte;
import static org.hornetq.tests.util.RandomUtil.randomBytes;
import static org.hornetq.tests.util.RandomUtil.randomChar;
import static org.hornetq.tests.util.RandomUtil.randomDouble;
import static org.hornetq.tests.util.RandomUtil.randomFloat;
import static org.hornetq.tests.util.RandomUtil.randomInt;
import static org.hornetq.tests.util.RandomUtil.randomLong;
import static org.hornetq.tests.util.RandomUtil.randomShort;
import static org.hornetq.tests.util.RandomUtil.randomString;

import javax.jms.MessageFormatException;

import org.hornetq.jms.client.HornetQMapMessage;
import org.hornetq.tests.util.UnitTestCase;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class HornetQMapMessageTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private String itemName;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      itemName = randomString();
   }

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testClearBody() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();
      message.setBoolean(itemName, true);

      assertTrue(message.itemExists(itemName));

      message.clearBody();

      assertFalse(message.itemExists(itemName));
   }

   public void testGetType() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();
      assertEquals(HornetQMapMessage.TYPE, message.getType());
   }
   
   public void testCheckItemNameIsNull() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();
      try
      {
         message.setBoolean(null, true);
         fail("item name can not be null");         
      } catch (IllegalArgumentException e)
      {
      }
      
   }

   public void testCheckItemNameIsEmpty() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();
      try
      {
         message.setBoolean("", true);
         fail("item name can not be empty");         
      } catch (IllegalArgumentException e)
      {
      }
      
   }
   
   public void testGetBooleanFromBoolean() throws Exception
   {
      boolean value = true;

      HornetQMapMessage message = new HornetQMapMessage();
      message.setBoolean(itemName, value);

      assertEquals(value, message.getBoolean(itemName));
   }

   public void testGetBooleanFromNull() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();
      assertEquals(false, message.getBoolean(itemName));
   }

   public void testGetBooleanFromString() throws Exception
   {
      boolean value = true;

      HornetQMapMessage message = new HornetQMapMessage();
      message.setString(itemName, Boolean.toString(value));

      assertEquals(value, message.getBoolean(itemName));
   }

   public void testGetBooleanFromInvalidType() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();
      message.setFloat(itemName, randomFloat());

      try
      {
         message.getBoolean(itemName);
         fail("MessageFormatException");
      } catch (MessageFormatException e)
      {
      }
   }

   public void testGetByteFromByte() throws Exception
   {
      byte value = randomByte();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setByte(itemName, value);

      assertEquals(value, message.getByte(itemName));
   }

   public void testGetByteFromNull() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();

      try
      {
         message.getByte(itemName);
         fail("NumberFormatException");
      } catch (NumberFormatException e)
      {
      }
   }

   public void testGetByteFromString() throws Exception
   {
      byte value = randomByte();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setString(itemName, Byte.toString(value));

      assertEquals(value, message.getByte(itemName));
   }

   public void testGetByteFromInvalidType() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();
      message.setFloat(itemName, randomFloat());

      try
      {
         message.getByte(itemName);
         fail("MessageFormatException");
      } catch (MessageFormatException e)
      {
      }
   }

   public void testGetShortFromByte() throws Exception
   {
      byte value = randomByte();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setByte(itemName, value);

      assertEquals(value, message.getShort(itemName));
   }

   public void testGetShortFromShort() throws Exception
   {
      short value = randomShort();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setShort(itemName, value);

      assertEquals(value, message.getShort(itemName));
   }

   public void testGetShortFromNull() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();

      try
      {
         message.getShort(itemName);
         fail("NumberFormatException");
      } catch (NumberFormatException e)
      {
      }
   }

   public void testGetShortFromString() throws Exception
   {
      short value = randomShort();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setString(itemName, Short.toString(value));

      assertEquals(value, message.getShort(itemName));
   }

   public void testGetShortFromInvalidType() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();
      message.setFloat(itemName, randomFloat());

      try
      {
         message.getShort(itemName);
         fail("MessageFormatException");
      } catch (MessageFormatException e)
      {
      }
   }

   public void testGetIntFromByte() throws Exception
   {
      byte value = randomByte();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setByte(itemName, value);

      assertEquals(value, message.getInt(itemName));
   }

   public void testGetIntFromShort() throws Exception
   {
      short value = randomShort();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setShort(itemName, value);

      assertEquals(value, message.getInt(itemName));
   }

   public void testGetIntFromInt() throws Exception
   {
      int value = randomInt();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setInt(itemName, value);

      assertEquals(value, message.getInt(itemName));
   }

   public void testGetIntFromNull() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();

      try
      {
         message.getInt(itemName);
         fail("NumberFormatException");
      } catch (NumberFormatException e)
      {
      }
   }

   public void testGetIntFromString() throws Exception
   {
      int value = randomInt();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setString(itemName, Integer.toString(value));

      assertEquals(value, message.getInt(itemName));
   }

   public void testGetIntFromInvalidType() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();
      message.setFloat(itemName, randomFloat());

      try
      {
         message.getInt(itemName);
         fail("MessageFormatException");
      } catch (MessageFormatException e)
      {
      }
   }

   public void testGetCharFromChar() throws Exception
   {
      char value = randomChar();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setChar(itemName, value);

      assertEquals(value, message.getChar(itemName));
   }

   public void testGetCharFromNull() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();

      try
      {
         message.getChar(itemName);
         fail("NullPointerException");
      } catch (NullPointerException e)
      {
      }
   }

   public void testGetCharFromInvalidType() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();
      message.setFloat(itemName, randomFloat());

      try
      {
         message.getChar(itemName);
         fail("MessageFormatException");
      } catch (MessageFormatException e)
      {
      }
   }

   public void testGetLongFromByte() throws Exception
   {
      byte value = randomByte();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setByte(itemName, value);

      assertEquals(value, message.getLong(itemName));
   }

   public void testGetLongFromShort() throws Exception
   {
      short value = randomShort();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setShort(itemName, value);

      assertEquals(value, message.getLong(itemName));
   }

   public void testGetLongFromInt() throws Exception
   {
      int value = randomInt();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setInt(itemName, value);

      assertEquals(value, message.getLong(itemName));
   }

   public void testGetLongFromLong() throws Exception
   {
      long value = randomLong();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setLong(itemName, value);

      assertEquals(value, message.getLong(itemName));
   }

   public void testGetLongFromNull() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();

      try
      {
         message.getLong(itemName);
         fail("NumberFormatException");
      } catch (NumberFormatException e)
      {
      }
   }

   public void testGetLongFromString() throws Exception
   {
      long value = randomLong();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setString(itemName, Long.toString(value));

      assertEquals(value, message.getLong(itemName));
   }

   public void testGetLongFromInvalidType() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();
      message.setFloat(itemName, randomFloat());

      try
      {
         message.getLong(itemName);
         fail("MessageFormatException");
      } catch (MessageFormatException e)
      {
      }
   }
   
   public void testGetFloatFromFloat() throws Exception
   {
      float value = randomFloat();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setFloat(itemName, value);

      assertEquals(value, message.getFloat(itemName));
   }

   public void testGetFloatFromNull() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();

      try
      {
         message.getFloat(itemName);
         fail("NullPointerException");
      } catch (NullPointerException e)
      {
      }
   }

   public void testGetFloatFromString() throws Exception
   {
      float value = randomFloat();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setString(itemName, Float.toString(value));

      assertEquals(value, message.getFloat(itemName));
   }

   public void testGetFloatFromInvalidType() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();
      message.setChar(itemName, randomChar());

      try
      {
         message.getFloat(itemName);
         fail("MessageFormatException");
      } catch (MessageFormatException e)
      {
      }
   }

   public void testGetDoubleFromFloat() throws Exception
   {
      float value = randomFloat();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setFloat(itemName, value);

      assertEquals(Float.valueOf(value).doubleValue(), message.getDouble(itemName));
   }
   
   public void testGetDoubleFromDouble() throws Exception
   {
      double value = randomDouble();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setDouble(itemName, value);

      assertEquals(value, message.getDouble(itemName));
   }

   public void testGetDoubleFromNull() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();

      try
      {
         message.getDouble(itemName);
         fail("NullPointerException");
      } catch (NullPointerException e)
      {
      }
   }

   public void testGetDoubleFromString() throws Exception
   {
      double value = randomDouble();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setString(itemName, Double.toString(value));

      assertEquals(value, message.getDouble(itemName));
   }

   public void testGetDoubleFromInvalidType() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();
      message.setChar(itemName, randomChar());

      try
      {
         message.getDouble(itemName);
         fail("MessageFormatException");
      } catch (MessageFormatException e)
      {
      }
   }
   
   public void testGetStringFromBoolean() throws Exception
   {
      boolean value = randomBoolean();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setBoolean(itemName, value);

      assertEquals(Boolean.toString(value), message.getString(itemName));
   }
   
   public void testGetStringFromByte() throws Exception
   {
      byte value = randomByte();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setByte(itemName, value);

      assertEquals(Byte.toString(value), message.getString(itemName));
   }
   
   public void testGetStringFromChar() throws Exception
   {
      char value = randomChar();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setChar(itemName, value);

      assertEquals(Character.toString(value), message.getString(itemName));
   }

   public void testGetStringFromShort() throws Exception
   {
      short value = randomShort();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setShort(itemName, value);

      assertEquals(Short.toString(value), message.getString(itemName));
   }

   public void testGetStringFromInt() throws Exception
   {
      int value = randomInt();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setInt(itemName, value);

      assertEquals(Integer.toString(value), message.getString(itemName));
   }

   public void testGetStringFromLong() throws Exception
   {
      long value = randomLong();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setLong(itemName, value);

      assertEquals(Long.toString(value), message.getString(itemName));
   }

   public void testGetStringFromFloat() throws Exception
   {
      float value = randomFloat();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setFloat(itemName, value);

      assertEquals(Float.toString(value), message.getString(itemName));
   }
   
   public void testGetStringFromDouble() throws Exception
   {
      double value = randomByte();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setDouble(itemName, value);

      assertEquals(Double.toString(value), message.getString(itemName));
   }
   
   public void testGetStringFromNull() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();

      assertNull(message.getString(itemName));
   }

   public void testGetStringFromString() throws Exception
   {
      String value = randomString();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setString(itemName, value);

      assertEquals(value, message.getString(itemName));
   }
   
   public void testGetBytesFromBytes() throws Exception
   {
      byte[] value = randomBytes();
      HornetQMapMessage message = new HornetQMapMessage();
      message.setBytes(itemName, value);

      assertEqualsByteArrays(value, message.getBytes(itemName));
   }

   public void testGetBytesFromNull() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();
      
      assertNull(message.getBytes(itemName));
   }
   
   public void testGetBytesFromInvalidType() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();
      message.setChar(itemName, randomChar());

      try
      {
         message.getBytes(itemName);
         fail("MessageFormatException");
      } catch (MessageFormatException e)
      {
      }
   }
   
   public void testSetObjectFromBoolean() throws Exception
   {
      boolean value = randomBoolean();
      HornetQMapMessage message = new HornetQMapMessage();
      message.setObject(itemName, value);

      assertEquals(value, message.getObject(itemName));
   }
   
   public void testSetObjectFromByte() throws Exception
   {
      doTestSetObject(randomByte());
   }
   
   public void testSetObjectFromShort() throws Exception
   {
      doTestSetObject(randomShort());
   }
   
   public void testSetObjectFromChar() throws Exception
   {
      doTestSetObject(randomChar());
   }
   
   public void testSetObjectFromInt() throws Exception
   {
      doTestSetObject(randomInt());
   }
   
   public void testSetObjectFromLong() throws Exception
   {
      doTestSetObject(randomLong());
   }
   
   public void testSetObjectFromFloat() throws Exception
   {
      doTestSetObject(randomFloat());
   }
   
   public void testSetObjectFromDouble() throws Exception
   {
      doTestSetObject(randomDouble());
   }
   
   public void testSetObjectFromString() throws Exception
   {
      doTestSetObject(randomString());
   }
   
   public void testSetObjectFromBytes() throws Exception
   {
      doTestSetObject(randomBytes());
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void doTestSetObject(Object value) throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();
      message.setObject(itemName, value);

      assertEquals(value, message.getObject(itemName));
   }
   
   // Inner classes -------------------------------------------------
}
