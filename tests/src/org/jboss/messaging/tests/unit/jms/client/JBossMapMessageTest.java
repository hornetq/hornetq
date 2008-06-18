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

package org.jboss.messaging.tests.unit.jms.client;

import static org.jboss.messaging.tests.unit.core.remoting.impl.CodecAssert.assertEqualsByteArrays;
import static org.jboss.messaging.tests.util.RandomUtil.randomBoolean;
import static org.jboss.messaging.tests.util.RandomUtil.randomByte;
import static org.jboss.messaging.tests.util.RandomUtil.randomBytes;
import static org.jboss.messaging.tests.util.RandomUtil.randomChar;
import static org.jboss.messaging.tests.util.RandomUtil.randomDouble;
import static org.jboss.messaging.tests.util.RandomUtil.randomFloat;
import static org.jboss.messaging.tests.util.RandomUtil.randomInt;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;
import static org.jboss.messaging.tests.util.RandomUtil.randomShort;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import javax.jms.MessageFormatException;

import junit.framework.TestCase;

import org.jboss.messaging.jms.client.JBossMapMessage;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class JBossMapMessageTest extends TestCase
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
      JBossMapMessage message = new JBossMapMessage();
      message.setBoolean(itemName, true);

      assertTrue(message.itemExists(itemName));

      message.clearBody();

      assertFalse(message.itemExists(itemName));
   }

   public void testGetType() throws Exception
   {
      JBossMapMessage message = new JBossMapMessage();
      assertEquals(JBossMapMessage.TYPE, message.getType());
   }
   
   public void testCheckItemNameIsNull() throws Exception
   {
      JBossMapMessage message = new JBossMapMessage();
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
      JBossMapMessage message = new JBossMapMessage();
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

      JBossMapMessage message = new JBossMapMessage();
      message.setBoolean(itemName, value);

      assertEquals(value, message.getBoolean(itemName));
   }

   public void testGetBooleanFromNull() throws Exception
   {
      JBossMapMessage message = new JBossMapMessage();
      assertEquals(false, message.getBoolean(itemName));
   }

   public void testGetBooleanFromString() throws Exception
   {
      boolean value = true;

      JBossMapMessage message = new JBossMapMessage();
      message.setString(itemName, Boolean.toString(value));

      assertEquals(value, message.getBoolean(itemName));
   }

   public void testGetBooleanFromInvalidType() throws Exception
   {
      JBossMapMessage message = new JBossMapMessage();
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

      JBossMapMessage message = new JBossMapMessage();
      message.setByte(itemName, value);

      assertEquals(value, message.getByte(itemName));
   }

   public void testGetByteFromNull() throws Exception
   {
      JBossMapMessage message = new JBossMapMessage();

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

      JBossMapMessage message = new JBossMapMessage();
      message.setString(itemName, Byte.toString(value));

      assertEquals(value, message.getByte(itemName));
   }

   public void testGetByteFromInvalidType() throws Exception
   {
      JBossMapMessage message = new JBossMapMessage();
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

      JBossMapMessage message = new JBossMapMessage();
      message.setByte(itemName, value);

      assertEquals(value, message.getShort(itemName));
   }

   public void testGetShortFromShort() throws Exception
   {
      short value = randomShort();

      JBossMapMessage message = new JBossMapMessage();
      message.setShort(itemName, value);

      assertEquals(value, message.getShort(itemName));
   }

   public void testGetShortFromNull() throws Exception
   {
      JBossMapMessage message = new JBossMapMessage();

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

      JBossMapMessage message = new JBossMapMessage();
      message.setString(itemName, Short.toString(value));

      assertEquals(value, message.getShort(itemName));
   }

   public void testGetShortFromInvalidType() throws Exception
   {
      JBossMapMessage message = new JBossMapMessage();
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

      JBossMapMessage message = new JBossMapMessage();
      message.setByte(itemName, value);

      assertEquals(value, message.getInt(itemName));
   }

   public void testGetIntFromShort() throws Exception
   {
      short value = randomShort();

      JBossMapMessage message = new JBossMapMessage();
      message.setShort(itemName, value);

      assertEquals(value, message.getInt(itemName));
   }

   public void testGetIntFromInt() throws Exception
   {
      int value = randomInt();

      JBossMapMessage message = new JBossMapMessage();
      message.setInt(itemName, value);

      assertEquals(value, message.getInt(itemName));
   }

   public void testGetIntFromNull() throws Exception
   {
      JBossMapMessage message = new JBossMapMessage();

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

      JBossMapMessage message = new JBossMapMessage();
      message.setString(itemName, Integer.toString(value));

      assertEquals(value, message.getInt(itemName));
   }

   public void testGetIntFromInvalidType() throws Exception
   {
      JBossMapMessage message = new JBossMapMessage();
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

      JBossMapMessage message = new JBossMapMessage();
      message.setChar(itemName, value);

      assertEquals(value, message.getChar(itemName));
   }

   public void testGetCharFromNull() throws Exception
   {
      JBossMapMessage message = new JBossMapMessage();

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
      JBossMapMessage message = new JBossMapMessage();
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

      JBossMapMessage message = new JBossMapMessage();
      message.setByte(itemName, value);

      assertEquals(value, message.getLong(itemName));
   }

   public void testGetLongFromShort() throws Exception
   {
      short value = randomShort();

      JBossMapMessage message = new JBossMapMessage();
      message.setShort(itemName, value);

      assertEquals(value, message.getLong(itemName));
   }

   public void testGetLongFromInt() throws Exception
   {
      int value = randomInt();

      JBossMapMessage message = new JBossMapMessage();
      message.setInt(itemName, value);

      assertEquals(value, message.getLong(itemName));
   }

   public void testGetLongFromLong() throws Exception
   {
      long value = randomLong();

      JBossMapMessage message = new JBossMapMessage();
      message.setLong(itemName, value);

      assertEquals(value, message.getLong(itemName));
   }

   public void testGetLongFromNull() throws Exception
   {
      JBossMapMessage message = new JBossMapMessage();

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

      JBossMapMessage message = new JBossMapMessage();
      message.setString(itemName, Long.toString(value));

      assertEquals(value, message.getLong(itemName));
   }

   public void testGetLongFromInvalidType() throws Exception
   {
      JBossMapMessage message = new JBossMapMessage();
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

      JBossMapMessage message = new JBossMapMessage();
      message.setFloat(itemName, value);

      assertEquals(value, message.getFloat(itemName));
   }

   public void testGetFloatFromNull() throws Exception
   {
      JBossMapMessage message = new JBossMapMessage();

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

      JBossMapMessage message = new JBossMapMessage();
      message.setString(itemName, Float.toString(value));

      assertEquals(value, message.getFloat(itemName));
   }

   public void testGetFloatFromInvalidType() throws Exception
   {
      JBossMapMessage message = new JBossMapMessage();
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

      JBossMapMessage message = new JBossMapMessage();
      message.setFloat(itemName, value);

      assertEquals(Float.valueOf(value).doubleValue(), message.getDouble(itemName));
   }
   
   public void testGetDoubleFromDouble() throws Exception
   {
      double value = randomDouble();

      JBossMapMessage message = new JBossMapMessage();
      message.setDouble(itemName, value);

      assertEquals(value, message.getDouble(itemName));
   }

   public void testGetDoubleFromNull() throws Exception
   {
      JBossMapMessage message = new JBossMapMessage();

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

      JBossMapMessage message = new JBossMapMessage();
      message.setString(itemName, Double.toString(value));

      assertEquals(value, message.getDouble(itemName));
   }

   public void testGetDoubleFromInvalidType() throws Exception
   {
      JBossMapMessage message = new JBossMapMessage();
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

      JBossMapMessage message = new JBossMapMessage();
      message.setBoolean(itemName, value);

      assertEquals(Boolean.toString(value), message.getString(itemName));
   }
   
   public void testGetStringFromByte() throws Exception
   {
      byte value = randomByte();

      JBossMapMessage message = new JBossMapMessage();
      message.setByte(itemName, value);

      assertEquals(Byte.toString(value), message.getString(itemName));
   }
   
   public void testGetStringFromChar() throws Exception
   {
      char value = randomChar();

      JBossMapMessage message = new JBossMapMessage();
      message.setChar(itemName, value);

      assertEquals(Character.toString(value), message.getString(itemName));
   }

   public void testGetStringFromShort() throws Exception
   {
      short value = randomShort();

      JBossMapMessage message = new JBossMapMessage();
      message.setShort(itemName, value);

      assertEquals(Short.toString(value), message.getString(itemName));
   }

   public void testGetStringFromInt() throws Exception
   {
      int value = randomInt();

      JBossMapMessage message = new JBossMapMessage();
      message.setInt(itemName, value);

      assertEquals(Integer.toString(value), message.getString(itemName));
   }

   public void testGetStringFromLong() throws Exception
   {
      long value = randomLong();

      JBossMapMessage message = new JBossMapMessage();
      message.setLong(itemName, value);

      assertEquals(Long.toString(value), message.getString(itemName));
   }

   public void testGetStringFromFloat() throws Exception
   {
      float value = randomFloat();

      JBossMapMessage message = new JBossMapMessage();
      message.setFloat(itemName, value);

      assertEquals(Float.toString(value), message.getString(itemName));
   }
   
   public void testGetStringFromDouble() throws Exception
   {
      double value = randomByte();

      JBossMapMessage message = new JBossMapMessage();
      message.setDouble(itemName, value);

      assertEquals(Double.toString(value), message.getString(itemName));
   }
   
   public void testGetStringFromNull() throws Exception
   {
      JBossMapMessage message = new JBossMapMessage();

      assertNull(message.getString(itemName));
   }

   public void testGetStringFromString() throws Exception
   {
      String value = randomString();

      JBossMapMessage message = new JBossMapMessage();
      message.setString(itemName, value);

      assertEquals(value, message.getString(itemName));
   }
   
   public void testGetBytesFromBytes() throws Exception
   {
      byte[] value = randomBytes();
      JBossMapMessage message = new JBossMapMessage();
      message.setBytes(itemName, value);

      assertEqualsByteArrays(value, message.getBytes(itemName));
   }

   public void testGetBytesFromNull() throws Exception
   {
      JBossMapMessage message = new JBossMapMessage();
      
      assertNull(message.getBytes(itemName));
   }
   
   public void testGetBytesFromInvalidType() throws Exception
   {
      JBossMapMessage message = new JBossMapMessage();
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
      JBossMapMessage message = new JBossMapMessage();
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
      JBossMapMessage message = new JBossMapMessage();
      message.setObject(itemName, value);

      assertEquals(value, message.getObject(itemName));
   }
   
   // Inner classes -------------------------------------------------
}
