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

import java.util.ArrayList;

import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;

import junit.framework.Assert;

import org.hornetq.jms.client.HornetQStreamMessage;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.UnitTestCase;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class HornetQStreamMessageTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testGetType() throws Exception
   {
      HornetQStreamMessage message = new HornetQStreamMessage();
      Assert.assertEquals(HornetQStreamMessage.TYPE, message.getType());
   }

   public void testReadBooleanFromBoolean() throws Exception
   {
      boolean value = RandomUtil.randomBoolean();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeBoolean(value);
      message.reset();

      Assert.assertEquals(value, message.readBoolean());
   }

   public void testReadBooleanFromString() throws Exception
   {
      boolean value = RandomUtil.randomBoolean();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeString(Boolean.toString(value));
      message.reset();

      Assert.assertEquals(value, message.readBoolean());
   }

   public void testReadBooleanFromInvalidType() throws Exception
   {
      doReadTypeFromInvalidType(RandomUtil.randomFloat(), new TypeReader()
      {
         public Object readType(final HornetQStreamMessage message) throws Exception
         {
            return message.readBoolean();
         }
      });
   }

   public void testReadBooleanFromEmptyMessage() throws Exception
   {
      doReadTypeFromEmptyMessage(new TypeReader()
      {
         public Object readType(final HornetQStreamMessage message) throws Exception
         {
            return message.readBoolean();
         }
      });
   }

   public void testReadCharFromInvalidType() throws Exception
   {
      doReadTypeFromInvalidType(RandomUtil.randomFloat(), new TypeReader()
      {
         public Object readType(final HornetQStreamMessage message) throws Exception
         {
            return message.readChar();
         }
      });
   }

   public void testReadCharFromEmptyMessage() throws Exception
   {
      doReadTypeFromEmptyMessage(new TypeReader()
      {
         public Object readType(final HornetQStreamMessage message) throws Exception
         {
            return message.readChar();
         }
      });
   }

   public void testReadByteFromByte() throws Exception
   {
      byte value = RandomUtil.randomByte();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeByte(value);
      message.reset();

      Assert.assertEquals(value, message.readByte());
   }

   public void testReadByteFromString() throws Exception
   {
      byte value = RandomUtil.randomByte();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeString(Byte.toString(value));
      message.reset();

      Assert.assertEquals(value, message.readByte());
   }

   public void testReadByteFromInvalidType() throws Exception
   {
      doReadTypeFromInvalidType(RandomUtil.randomFloat(), new TypeReader()
      {
         public Object readType(final HornetQStreamMessage message) throws Exception
         {
            return message.readByte();
         }
      });
   }

   public void testReadByteFromEmptyMessage() throws Exception
   {
      doReadTypeFromEmptyMessage(new TypeReader()
      {
         public Object readType(final HornetQStreamMessage message) throws Exception
         {
            return message.readByte();
         }
      });
   }

   public void testReadBytesFromBytes() throws Exception
   {
      byte[] value = RandomUtil.randomBytes();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeBytes(value);
      message.reset();

      byte[] v = new byte[value.length];
      message.readBytes(v);

      UnitTestCase.assertEqualsByteArrays(value, v);
   }

   public void testReadBytesFromBytes_2() throws Exception
   {
      byte[] value = RandomUtil.randomBytes(512);
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeBytes(value, 0, 256);
      message.reset();

      byte[] v = new byte[256];
      message.readBytes(v);

      UnitTestCase.assertEqualsByteArrays(256, value, v);
   }

   public void testReadBytesFromInvalidType() throws Exception
   {
      doReadTypeFromInvalidType(RandomUtil.randomBoolean(), new TypeReader()
      {
         public Object readType(final HornetQStreamMessage message) throws Exception
         {
            return message.readByte();
         }
      });
   }

   public void testReadBytesFromEmptyMessage() throws Exception
   {
      doReadTypeFromEmptyMessage(new TypeReader()
      {
         public Object readType(final HornetQStreamMessage message) throws Exception
         {
            byte[] bytes = new byte[1];
            return message.readBytes(bytes);
         }
      });
   }

   public void testReadShortFromByte() throws Exception
   {
      byte value = RandomUtil.randomByte();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeByte(value);
      message.reset();

      Assert.assertEquals(value, message.readShort());
   }

   public void testReadShortFromShort() throws Exception
   {
      short value = RandomUtil.randomShort();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeShort(value);
      message.reset();

      Assert.assertEquals(value, message.readShort());
   }

   public void testReadShortFromString() throws Exception
   {
      short value = RandomUtil.randomShort();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeString(Short.toString(value));
      message.reset();

      Assert.assertEquals(value, message.readShort());
   }

   public void testReadShortFromInvalidType() throws Exception
   {
      doReadTypeFromInvalidType(RandomUtil.randomFloat(), new TypeReader()
      {
         public Object readType(final HornetQStreamMessage message) throws Exception
         {
            return message.readShort();
         }
      });
   }

   public void testReadShortFromEmptyMessage() throws Exception
   {
      doReadTypeFromEmptyMessage(new TypeReader()
      {
         public Object readType(final HornetQStreamMessage message) throws Exception
         {
            return message.readShort();
         }
      });
   }

   public void testReadIntFromByte() throws Exception
   {
      byte value = RandomUtil.randomByte();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeByte(value);
      message.reset();

      Assert.assertEquals(value, message.readInt());
   }

   public void testReadIntFromShort() throws Exception
   {
      short value = RandomUtil.randomShort();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeShort(value);
      message.reset();

      Assert.assertEquals(value, message.readInt());
   }

   public void testReadIntFromInt() throws Exception
   {
      int value = RandomUtil.randomInt();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeInt(value);
      message.reset();

      Assert.assertEquals(value, message.readInt());
   }

   public void testReadIntFromString() throws Exception
   {
      int value = RandomUtil.randomInt();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeString(Integer.toString(value));
      message.reset();

      Assert.assertEquals(value, message.readInt());
   }

   public void testReadIntFromInvalidType() throws Exception
   {
      doReadTypeFromInvalidType(RandomUtil.randomFloat(), new TypeReader()
      {
         public Object readType(final HornetQStreamMessage message) throws Exception
         {
            return message.readInt();
         }
      });
   }

   public void testReadIntFromEmptyMessage() throws Exception
   {
      doReadTypeFromEmptyMessage(new TypeReader()
      {
         public Object readType(final HornetQStreamMessage message) throws Exception
         {
            return message.readInt();
         }
      });
   }

   public void testReadCharFromChar() throws Exception
   {
      char value = RandomUtil.randomChar();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeChar(value);
      message.reset();

      Assert.assertEquals(value, message.readChar());
   }
   
   public void testReadCharFromNull() throws Exception
   {
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeString(null);
      message.reset();

      try
      {
         message.readChar();
         fail();
      }
      catch (NullPointerException e)
      {
      }
   }

   public void testReadLongFromByte() throws Exception
   {
      byte value = RandomUtil.randomByte();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeByte(value);
      message.reset();

      Assert.assertEquals(value, message.readLong());
   }

   public void testReadLongFromShort() throws Exception
   {
      short value = RandomUtil.randomShort();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeShort(value);
      message.reset();

      Assert.assertEquals(value, message.readLong());
   }

   public void testReadLongFromInt() throws Exception
   {
      int value = RandomUtil.randomInt();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeInt(value);
      message.reset();

      Assert.assertEquals(value, message.readLong());
   }

   public void testReadLongFromLong() throws Exception
   {
      long value = RandomUtil.randomLong();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeLong(value);
      message.reset();

      Assert.assertEquals(value, message.readLong());
   }

   public void testReadLongFromString() throws Exception
   {
      long value = RandomUtil.randomLong();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeString(Long.toString(value));
      message.reset();

      Assert.assertEquals(value, message.readLong());
   }

   public void testReadLongFromInvalidType() throws Exception
   {
      doReadTypeFromInvalidType(RandomUtil.randomFloat(), new TypeReader()
      {
         public Object readType(final HornetQStreamMessage message) throws Exception
         {
            return message.readLong();
         }
      });
   }

   public void testReadLongFromEmptyMessage() throws Exception
   {
      doReadTypeFromEmptyMessage(new TypeReader()
      {
         public Object readType(final HornetQStreamMessage message) throws Exception
         {
            return message.readLong();
         }
      });
   }

   public void testReadFloatFromFloat() throws Exception
   {
      float value = RandomUtil.randomFloat();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeFloat(value);
      message.reset();

      Assert.assertEquals(value, message.readFloat());
   }

   public void testReadFloatFromString() throws Exception
   {
      float value = RandomUtil.randomFloat();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeString(Float.toString(value));
      message.reset();

      Assert.assertEquals(value, message.readFloat());
   }

   public void testReadFloatFromInvalidType() throws Exception
   {
      doReadTypeFromInvalidType(RandomUtil.randomBoolean(), new TypeReader()
      {
         public Object readType(final HornetQStreamMessage message) throws Exception
         {
            return message.readFloat();
         }
      });
   }

   public void testReadFloatFromEmptyMessage() throws Exception
   {
      doReadTypeFromEmptyMessage(new TypeReader()
      {
         public Object readType(final HornetQStreamMessage message) throws Exception
         {
            return message.readFloat();
         }
      });
   }

   public void testReadDoubleFromFloat() throws Exception
   {
      float value = RandomUtil.randomFloat();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeFloat(value);
      message.reset();

      Assert.assertEquals(Float.valueOf(value).doubleValue(), message.readDouble());
   }

   public void testReadDoubleFromDouble() throws Exception
   {
      double value = RandomUtil.randomDouble();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeDouble(value);
      message.reset();

      Assert.assertEquals(value, message.readDouble());
   }

   public void testReadDoubleFromString() throws Exception
   {
      double value = RandomUtil.randomDouble();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeString(Double.toString(value));
      message.reset();

      Assert.assertEquals(value, message.readDouble());
   }

   public void testReadDoubleFromInvalidType() throws Exception
   {
      doReadTypeFromInvalidType(RandomUtil.randomBoolean(), new TypeReader()
      {
         public Object readType(final HornetQStreamMessage message) throws Exception
         {
            return message.readDouble();
         }
      });
   }

   public void testReadDoubleFromEmptyMessage() throws Exception
   {
      doReadTypeFromEmptyMessage(new TypeReader()
      {
         public Object readType(final HornetQStreamMessage message) throws Exception
         {
            return message.readDouble();
         }
      });
   }

   public void testReadStringFromBoolean() throws Exception
   {
      boolean value = RandomUtil.randomBoolean();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeBoolean(value);
      message.reset();

      Assert.assertEquals(Boolean.toString(value), message.readString());
   }

   public void testReadStringFromChar() throws Exception
   {
      char value = RandomUtil.randomChar();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeChar(value);
      message.reset();

      Assert.assertEquals(Character.toString(value), message.readString());
   }

   public void testReadStringFromByte() throws Exception
   {
      byte value = RandomUtil.randomByte();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeByte(value);
      message.reset();

      Assert.assertEquals(Byte.toString(value), message.readString());
   }

   public void testString() throws Exception
   {
      String value = RandomUtil.randomString();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeString(value);
      message.reset();

      try
      {
         message.readByte();
         fail("must throw a NumberFormatException");
      }
      catch (NumberFormatException e)
      {
      }

      // we can read the String without resetting the message
      Assert.assertEquals(value, message.readString());
   }
   
   public void testReadStringFromShort() throws Exception
   {
      short value = RandomUtil.randomShort();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeShort(value);
      message.reset();

      Assert.assertEquals(Short.toString(value), message.readString());
   }

   public void testReadStringFromInt() throws Exception
   {
      int value = RandomUtil.randomInt();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeInt(value);
      message.reset();

      Assert.assertEquals(Integer.toString(value), message.readString());
   }

   public void testReadStringFromLong() throws Exception
   {
      long value = RandomUtil.randomLong();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeLong(value);
      message.reset();

      Assert.assertEquals(Long.toString(value), message.readString());
   }

   public void testReadStringFromFloat() throws Exception
   {
      float value = RandomUtil.randomFloat();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeFloat(value);
      message.reset();

      Assert.assertEquals(Float.toString(value), message.readString());
   }

   public void testReadStringFromDouble() throws Exception
   {
      double value = RandomUtil.randomDouble();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeDouble(value);
      message.reset();

      Assert.assertEquals(Double.toString(value), message.readString());
   }

   public void testReadStringFromString() throws Exception
   {
      String value = RandomUtil.randomString();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeString(value);
      message.reset();

      Assert.assertEquals(value, message.readString());
   }

   public void testReadStringFromNullString() throws Exception
   {
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeString(null);
      message.reset();

      Assert.assertNull(message.readString());
   }

   public void testReadStringFromEmptyMessage() throws Exception
   {
      doReadTypeFromEmptyMessage(new TypeReader()
      {
         public Object readType(final HornetQStreamMessage message) throws Exception
         {
            return message.readString();
         }
      });
   }

   public void testWriteObjectWithBoolean() throws Exception
   {
      doWriteObjectWithType(RandomUtil.randomBoolean(), new TypeReader()
      {
         public Object readType(final HornetQStreamMessage message) throws Exception
         {
            return message.readBoolean();
         }
      });
   }

   public void testWriteObjectWithChar() throws Exception
   {
      doWriteObjectWithType(RandomUtil.randomChar(), new TypeReader()
      {
         public Object readType(final HornetQStreamMessage message) throws Exception
         {
            return message.readChar();
         }
      });
   }

   public void testWriteObjectWithByte() throws Exception
   {
      doWriteObjectWithType(RandomUtil.randomByte(), new TypeReader()
      {
         public Object readType(final HornetQStreamMessage message) throws Exception
         {
            return message.readByte();
         }
      });
   }

   public void testWriteObjectWithBytes() throws Exception
   {
      final byte[] value = RandomUtil.randomBytes();
      doWriteObjectWithType(value, new TypeReader()
      {
         public Object readType(final HornetQStreamMessage message) throws Exception
         {
            byte[] bytes = new byte[value.length];
            message.readBytes(bytes);
            return bytes;
         }
      });
   }

   public void testWriteObjectWithShort() throws Exception
   {
      doWriteObjectWithType(RandomUtil.randomShort(), new TypeReader()
      {
         public Object readType(final HornetQStreamMessage message) throws Exception
         {
            return message.readShort();
         }
      });
   }

   public void testWriteObjectWithInt() throws Exception
   {
      doWriteObjectWithType(RandomUtil.randomInt(), new TypeReader()
      {
         public Object readType(final HornetQStreamMessage message) throws Exception
         {
            return message.readInt();
         }
      });
   }

   public void testWriteObjectWithLong() throws Exception
   {
      doWriteObjectWithType(RandomUtil.randomLong(), new TypeReader()
      {
         public Object readType(final HornetQStreamMessage message) throws Exception
         {
            return message.readLong();
         }
      });
   }

   public void testWriteObjectWithFloat() throws Exception
   {
      doWriteObjectWithType(RandomUtil.randomFloat(), new TypeReader()
      {
         public Object readType(final HornetQStreamMessage message) throws Exception
         {
            return message.readFloat();
         }
      });
   }

   public void testWriteObjectWithDouble() throws Exception
   {
      doWriteObjectWithType(RandomUtil.randomDouble(), new TypeReader()
      {
         public Object readType(final HornetQStreamMessage message) throws Exception
         {
            return message.readDouble();
         }
      });
   }

   public void testWriteObjectWithString() throws Exception
   {
      doWriteObjectWithType(RandomUtil.randomString(), new TypeReader()
      {
         public Object readType(final HornetQStreamMessage message) throws Exception
         {
            return message.readString();
         }
      });
   }

   public void testWriteObjectWithNull() throws Exception
   {
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeObject(null);
   }

   public void testWriteObjectWithInvalidType() throws Exception
   {
      HornetQStreamMessage message = new HornetQStreamMessage();

      try
      {
         message.writeObject(new ArrayList());
         Assert.fail("MessageFormatException");
      }
      catch (MessageFormatException e)
      {
      }
   }

   public void testReadObjectFromBoolean() throws Exception
   {
      boolean value = RandomUtil.randomBoolean();
      HornetQStreamMessage message = new HornetQStreamMessage();
      message.writeBoolean(value);

      message.reset();

      Assert.assertEquals(value, message.readObject());
   }

   public void testReadObjectFromChar() throws Exception
   {
      char value = RandomUtil.randomChar();
      HornetQStreamMessage message = new HornetQStreamMessage();
      message.writeChar(value);

      message.reset();

      Assert.assertEquals(value, message.readObject());
   }

   public void testReadObjectFromByte() throws Exception
   {
      byte value = RandomUtil.randomByte();
      HornetQStreamMessage message = new HornetQStreamMessage();
      message.writeByte(value);

      message.reset();

      Assert.assertEquals(value, message.readObject());
   }

   public void testReadObjectFromBytes() throws Exception
   {
      byte[] value = RandomUtil.randomBytes();
      HornetQStreamMessage message = new HornetQStreamMessage();
      message.writeBytes(value);

      message.reset();

      byte[] v = (byte[])message.readObject();
      UnitTestCase.assertEqualsByteArrays(value, v);
   }

   public void testReadObjectFromShort() throws Exception
   {
      short value = RandomUtil.randomShort();
      HornetQStreamMessage message = new HornetQStreamMessage();
      message.writeShort(value);

      message.reset();

      Assert.assertEquals(value, message.readObject());
   }

   public void testReadObjectFromInt() throws Exception
   {
      int value = RandomUtil.randomInt();
      HornetQStreamMessage message = new HornetQStreamMessage();
      message.writeInt(value);

      message.reset();

      Assert.assertEquals(value, message.readObject());
   }

   public void testReadObjectFromLong() throws Exception
   {
      long value = RandomUtil.randomLong();
      HornetQStreamMessage message = new HornetQStreamMessage();
      message.writeLong(value);

      message.reset();

      Assert.assertEquals(value, message.readObject());
   }

   public void testReadObjectFromFloat() throws Exception
   {
      float value = RandomUtil.randomFloat();
      HornetQStreamMessage message = new HornetQStreamMessage();
      message.writeFloat(value);

      message.reset();

      Assert.assertEquals(value, message.readObject());
   }

   public void testReadObjectFromDouble() throws Exception
   {
      double value = RandomUtil.randomDouble();
      HornetQStreamMessage message = new HornetQStreamMessage();
      message.writeDouble(value);

      message.reset();

      Assert.assertEquals(value, message.readObject());
   }

   public void testReadObjectFromString() throws Exception
   {
      String value = RandomUtil.randomString();
      HornetQStreamMessage message = new HornetQStreamMessage();
      message.writeString(value);

      message.reset();

      Assert.assertEquals(value, message.readObject());
   }

   // Private -------------------------------------------------------

   private void doReadTypeFromEmptyMessage(final TypeReader reader) throws Exception
   {
      HornetQStreamMessage message = new HornetQStreamMessage();
      message.reset();

      try
      {
         reader.readType(message);
         Assert.fail("MessageEOFException");
      }
      catch (MessageEOFException e)
      {
      }
   }

   private void doReadTypeFromInvalidType(final Object invalidValue, final TypeReader reader) throws Exception
   {
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeObject(invalidValue);
      message.reset();

      try
      {
         reader.readType(message);
         Assert.fail("MessageFormatException");
      }
      catch (MessageFormatException e)
      {
      }
   }

   private void doWriteObjectWithType(final Object value, final TypeReader reader) throws Exception
   {
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeObject(value);
      message.reset();

      Object v = reader.readType(message);
      if (value instanceof byte[])
      {
         UnitTestCase.assertEqualsByteArrays((byte[])value, (byte[])v);
      }
      else
      {
         Assert.assertEquals(value, v);
      }
   }

   // Inner classes -------------------------------------------------

   private interface TypeReader
   {
      Object readType(HornetQStreamMessage message) throws Exception;
   }
}
