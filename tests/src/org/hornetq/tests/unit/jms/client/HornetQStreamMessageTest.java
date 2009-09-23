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

import java.util.ArrayList;

import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;

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
      assertEquals(HornetQStreamMessage.TYPE, message.getType());
   }

   public void testReadBooleanFromBoolean() throws Exception
   {
      boolean value = randomBoolean();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeBoolean(value);
      message.reset();

      assertEquals(value, message.readBoolean());
   }

   public void testReadBooleanFromString() throws Exception
   {
      boolean value = randomBoolean();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeString(Boolean.toString(value));
      message.reset();

      assertEquals(value, message.readBoolean());
   }

   public void testReadBooleanFromInvalidType() throws Exception
   {
      doReadTypeFromInvalidType(randomFloat(), new TypeReader()
      {
         public Object readType(HornetQStreamMessage message) throws Exception
         {
            return message.readBoolean();
         }
      });
   }

   public void testReadBooleanFromEmptyMessage() throws Exception
   {
      doReadTypeFromEmptyMessage(new TypeReader()
      {
         public Object readType(HornetQStreamMessage message) throws Exception
         {
            return message.readBoolean();
         }
      });
   }

   public void testReadCharFromInvalidType() throws Exception
   {
      doReadTypeFromInvalidType(randomFloat(), new TypeReader()
      {
         public Object readType(HornetQStreamMessage message) throws Exception
         {
            return message.readChar();
         }
      });
   }

   public void testReadCharFromEmptyMessage() throws Exception
   {
      doReadTypeFromEmptyMessage(new TypeReader()
      {
         public Object readType(HornetQStreamMessage message) throws Exception
         {
            return message.readChar();
         }
      });
   }

   public void testReadByteFromByte() throws Exception
   {
      byte value = randomByte();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeByte(value);
      message.reset();

      assertEquals(value, message.readByte());
   }

   public void testReadByteFromString() throws Exception
   {
      byte value = randomByte();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeString(Byte.toString(value));
      message.reset();

      assertEquals(value, message.readByte());
   }

   public void testReadByteFromInvalidType() throws Exception
   {
      doReadTypeFromInvalidType(randomFloat(), new TypeReader()
      {
         public Object readType(HornetQStreamMessage message) throws Exception
         {
            return message.readByte();
         }
      });
   }

   public void testReadByteFromEmptyMessage() throws Exception
   {
      doReadTypeFromEmptyMessage(new TypeReader()
      {
         public Object readType(HornetQStreamMessage message) throws Exception
         {
            return message.readByte();
         }
      });
   }

   public void testReadBytesFromBytes() throws Exception
   {
      byte[] value = randomBytes();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeBytes(value);
      message.reset();

      byte[] v = new byte[value.length];
      message.readBytes(v);

      assertEqualsByteArrays(value, v);
   }

   public void testReadBytesFromBytes_2() throws Exception
   {
      byte[] value = randomBytes(512);
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeBytes(value, 0, 256);
      message.reset();

      byte[] v = new byte[256];
      message.readBytes(v);

      assertEqualsByteArrays(256, value, v);
   }

   public void testReadBytesFromInvalidType() throws Exception
   {
      doReadTypeFromInvalidType(randomBoolean(), new TypeReader()
      {
         public Object readType(HornetQStreamMessage message) throws Exception
         {
            return message.readByte();
         }
      });
   }

   public void testReadBytesFromEmptyMessage() throws Exception
   {
      doReadTypeFromEmptyMessage(new TypeReader()
      {
         public Object readType(HornetQStreamMessage message) throws Exception
         {
            byte[] bytes = new byte[1];
            return message.readBytes(bytes);
         }
      });
   }

   public void testReadShortFromByte() throws Exception
   {
      byte value = randomByte();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeByte(value);
      message.reset();

      assertEquals(value, message.readShort());
   }

   public void testReadShortFromShort() throws Exception
   {
      short value = randomShort();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeShort(value);
      message.reset();

      assertEquals(value, message.readShort());
   }

   public void testReadShortFromString() throws Exception
   {
      short value = randomShort();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeString(Short.toString(value));
      message.reset();

      assertEquals(value, message.readShort());
   }

   public void testReadShortFromInvalidType() throws Exception
   {
      doReadTypeFromInvalidType(randomFloat(), new TypeReader()
      {
         public Object readType(HornetQStreamMessage message) throws Exception
         {
            return message.readShort();
         }
      });
   }

   public void testReadShortFromEmptyMessage() throws Exception
   {
      doReadTypeFromEmptyMessage(new TypeReader()
      {
         public Object readType(HornetQStreamMessage message) throws Exception
         {
            return message.readShort();
         }
      });
   }

   public void testReadIntFromByte() throws Exception
   {
      byte value = randomByte();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeByte(value);
      message.reset();

      assertEquals(value, message.readInt());
   }

   public void testReadIntFromShort() throws Exception
   {
      short value = randomShort();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeShort(value);
      message.reset();

      assertEquals(value, message.readInt());
   }

   public void testReadIntFromInt() throws Exception
   {
      int value = randomInt();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeInt(value);
      message.reset();

      assertEquals(value, message.readInt());
   }

   public void testReadIntFromString() throws Exception
   {
      int value = randomInt();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeString(Integer.toString(value));
      message.reset();

      assertEquals(value, message.readInt());
   }

   public void testReadIntFromInvalidType() throws Exception
   {
      doReadTypeFromInvalidType(randomFloat(), new TypeReader()
      {
         public Object readType(HornetQStreamMessage message) throws Exception
         {
            return message.readInt();
         }
      });
   }

   public void testReadIntFromEmptyMessage() throws Exception
   {
      doReadTypeFromEmptyMessage(new TypeReader()
      {
         public Object readType(HornetQStreamMessage message) throws Exception
         {
            return message.readInt();
         }
      });
   }

   public void testReadCharFromChar() throws Exception
   {
      char value = randomChar();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeChar(value);
      message.reset();

      assertEquals(value, message.readChar());
   }

   public void testReadLongFromByte() throws Exception
   {
      byte value = randomByte();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeByte(value);
      message.reset();

      assertEquals(value, message.readLong());
   }

   public void testReadLongFromShort() throws Exception
   {
      short value = randomShort();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeShort(value);
      message.reset();

      assertEquals(value, message.readLong());
   }

   public void testReadLongFromInt() throws Exception
   {
      int value = randomInt();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeInt(value);
      message.reset();

      assertEquals(value, message.readLong());
   }

   public void testReadLongFromLong() throws Exception
   {
      long value = RandomUtil.randomLong();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeLong(value);
      message.reset();

      assertEquals(value, message.readLong());
   }

   public void testReadLongFromString() throws Exception
   {
      long value = RandomUtil.randomLong();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeString(Long.toString(value));
      message.reset();

      assertEquals(value, message.readLong());
   }

   public void testReadLongFromInvalidType() throws Exception
   {
      doReadTypeFromInvalidType(randomFloat(), new TypeReader()
      {
         public Object readType(HornetQStreamMessage message) throws Exception
         {
            return message.readLong();
         }
      });
   }

   public void testReadLongFromEmptyMessage() throws Exception
   {
      doReadTypeFromEmptyMessage(new TypeReader()
      {
         public Object readType(HornetQStreamMessage message) throws Exception
         {
            return message.readLong();
         }
      });
   }

   public void testReadFloatFromFloat() throws Exception
   {
      float value = randomFloat();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeFloat(value);
      message.reset();

      assertEquals(value, message.readFloat());
   }

   public void testReadFloatFromString() throws Exception
   {
      float value = randomFloat();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeString(Float.toString(value));
      message.reset();

      assertEquals(value, message.readFloat());
   }

   public void testReadFloatFromInvalidType() throws Exception
   {
      doReadTypeFromInvalidType(randomBoolean(), new TypeReader()
      {
         public Object readType(HornetQStreamMessage message) throws Exception
         {
            return message.readFloat();
         }
      });
   }

   public void testReadFloatFromEmptyMessage() throws Exception
   {
      doReadTypeFromEmptyMessage(new TypeReader()
      {
         public Object readType(HornetQStreamMessage message) throws Exception
         {
            return message.readFloat();
         }
      });
   }

   public void testReadDoubleFromFloat() throws Exception
   {
      float value = randomFloat();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeFloat(value);
      message.reset();

      assertEquals(Float.valueOf(value).doubleValue(), message.readDouble());
   }

   public void testReadDoubleFromDouble() throws Exception
   {
      double value = randomDouble();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeDouble(value);
      message.reset();

      assertEquals(value, message.readDouble());
   }

   public void testReadDoubleFromString() throws Exception
   {
      double value = randomDouble();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeString(Double.toString(value));
      message.reset();

      assertEquals(value, message.readDouble());
   }

   public void testReadDoubleFromInvalidType() throws Exception
   {
      doReadTypeFromInvalidType(randomBoolean(), new TypeReader()
      {
         public Object readType(HornetQStreamMessage message) throws Exception
         {
            return message.readDouble();
         }
      });
   }

   public void testReadDoubleFromEmptyMessage() throws Exception
   {
      doReadTypeFromEmptyMessage(new TypeReader()
      {
         public Object readType(HornetQStreamMessage message) throws Exception
         {
            return message.readDouble();
         }
      });
   }

   public void testReadStringFromBoolean() throws Exception
   {
      boolean value = randomBoolean();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeBoolean(value);
      message.reset();

      assertEquals(Boolean.toString(value), message.readString());
   }

   public void testReadStringFromChar() throws Exception
   {
      char value = randomChar();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeChar(value);
      message.reset();

      assertEquals(Character.toString(value), message.readString());
   }

   public void testReadStringFromByte() throws Exception
   {
      byte value = randomByte();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeByte(value);
      message.reset();

      assertEquals(Byte.toString(value), message.readString());
   }

   public void testReadStringFromShort() throws Exception
   {
      short value = randomShort();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeShort(value);
      message.reset();

      assertEquals(Short.toString(value), message.readString());
   }

   public void testReadStringFromInt() throws Exception
   {
      int value = randomInt();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeInt(value);
      message.reset();

      assertEquals(Integer.toString(value), message.readString());
   }

   public void testReadStringFromLong() throws Exception
   {
      long value = randomLong();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeLong(value);
      message.reset();

      assertEquals(Long.toString(value), message.readString());
   }

   public void testReadStringFromFloat() throws Exception
   {
      float value = randomFloat();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeFloat(value);
      message.reset();

      assertEquals(Float.toString(value), message.readString());
   }

   public void testReadStringFromDouble() throws Exception
   {
      double value = randomDouble();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeDouble(value);
      message.reset();

      assertEquals(Double.toString(value), message.readString());
   }

   public void testReadStringFromString() throws Exception
   {
      String value = randomString();
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeString(value);
      message.reset();

      assertEquals(value, message.readString());
   }

   public void testReadStringFromNullString() throws Exception
   {
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeString(null);
      message.reset();

      assertNull(message.readString());
   }

   public void testReadStringFromEmptyMessage() throws Exception
   {
      doReadTypeFromEmptyMessage(new TypeReader()
      {
         public Object readType(HornetQStreamMessage message) throws Exception
         {
            return message.readString();
         }
      });
   }

   public void testWriteObjectWithBoolean() throws Exception
   {
      doWriteObjectWithType(randomBoolean(), new TypeReader()
      {
         public Object readType(HornetQStreamMessage message) throws Exception
         {
            return message.readBoolean();
         }
      });
   }

   public void testWriteObjectWithChar() throws Exception
   {
      doWriteObjectWithType(randomChar(), new TypeReader()
      {
         public Object readType(HornetQStreamMessage message) throws Exception
         {
            return message.readChar();
         }
      });
   }

   public void testWriteObjectWithByte() throws Exception
   {
      doWriteObjectWithType(randomByte(), new TypeReader()
      {
         public Object readType(HornetQStreamMessage message) throws Exception
         {
            return message.readByte();
         }
      });
   }

   public void testWriteObjectWithBytes() throws Exception
   {
      final byte[] value = randomBytes();
      doWriteObjectWithType(value, new TypeReader()
      {
         public Object readType(HornetQStreamMessage message) throws Exception
         {
            byte[] bytes = new byte[value.length];
            message.readBytes(bytes);
            return bytes;
         }
      });
   }

   public void testWriteObjectWithShort() throws Exception
   {
      doWriteObjectWithType(randomShort(), new TypeReader()
      {
         public Object readType(HornetQStreamMessage message) throws Exception
         {
            return message.readShort();
         }
      });
   }

   public void testWriteObjectWithInt() throws Exception
   {
      doWriteObjectWithType(randomInt(), new TypeReader()
      {
         public Object readType(HornetQStreamMessage message) throws Exception
         {
            return message.readInt();
         }
      });
   }

   public void testWriteObjectWithLong() throws Exception
   {
      doWriteObjectWithType(randomLong(), new TypeReader()
      {
         public Object readType(HornetQStreamMessage message) throws Exception
         {
            return message.readLong();
         }
      });
   }

   public void testWriteObjectWithFloat() throws Exception
   {
      doWriteObjectWithType(randomFloat(), new TypeReader()
      {
         public Object readType(HornetQStreamMessage message) throws Exception
         {
            return message.readFloat();
         }
      });
   }

   public void testWriteObjectWithDouble() throws Exception
   {
      doWriteObjectWithType(randomDouble(), new TypeReader()
      {
         public Object readType(HornetQStreamMessage message) throws Exception
         {
            return message.readDouble();
         }
      });
   }

   public void testWriteObjectWithString() throws Exception
   {
      doWriteObjectWithType(randomString(), new TypeReader()
      {
         public Object readType(HornetQStreamMessage message) throws Exception
         {
            return message.readString();
         }
      });
   }

   public void testWriteObjectWithNull() throws Exception
   {
      HornetQStreamMessage message = new HornetQStreamMessage();

      try
      {
         message.writeObject(null);
         fail("NullPointerException");
      } catch (NullPointerException e)
      {
      }
   }

   public void testWriteObjectWithInvalidType() throws Exception
   {
      HornetQStreamMessage message = new HornetQStreamMessage();

      try
      {
         message.writeObject(new ArrayList());
         fail("MessageFormatException");
      } catch (MessageFormatException e)
      {
      }
   }

   public void testReadObjectFromBoolean() throws Exception
   {
      boolean value = randomBoolean();
      HornetQStreamMessage message = new HornetQStreamMessage();
      message.writeBoolean(value);

      message.reset();

      assertEquals(value, message.readObject());
   }

   public void testReadObjectFromChar() throws Exception
   {
      char value = randomChar();
      HornetQStreamMessage message = new HornetQStreamMessage();
      message.writeChar(value);

      message.reset();

      assertEquals(value, message.readObject());
   }

   public void testReadObjectFromByte() throws Exception
   {
      byte value = randomByte();
      HornetQStreamMessage message = new HornetQStreamMessage();
      message.writeByte(value);

      message.reset();

      assertEquals(value, message.readObject());
   }

   public void testReadObjectFromBytes() throws Exception
   {
      byte[] value = randomBytes();
      HornetQStreamMessage message = new HornetQStreamMessage();
      message.writeBytes(value);

      message.reset();

      byte[] v = (byte[]) message.readObject();
      assertEqualsByteArrays(value, v);
   }

   public void testReadObjectFromShort() throws Exception
   {
      short value = randomShort();
      HornetQStreamMessage message = new HornetQStreamMessage();
      message.writeShort(value);

      message.reset();

      assertEquals(value, message.readObject());
   }

   public void testReadObjectFromInt() throws Exception
   {
      int value = randomInt();
      HornetQStreamMessage message = new HornetQStreamMessage();
      message.writeInt(value);

      message.reset();

      assertEquals(value, message.readObject());
   }

   public void testReadObjectFromLong() throws Exception
   {
      long value = randomLong();
      HornetQStreamMessage message = new HornetQStreamMessage();
      message.writeLong(value);

      message.reset();

      assertEquals(value, message.readObject());
   }

   public void testReadObjectFromFloat() throws Exception
   {
      float value = randomFloat();
      HornetQStreamMessage message = new HornetQStreamMessage();
      message.writeFloat(value);

      message.reset();

      assertEquals(value, message.readObject());
   }

   public void testReadObjectFromDouble() throws Exception
   {
      double value = randomDouble();
      HornetQStreamMessage message = new HornetQStreamMessage();
      message.writeDouble(value);

      message.reset();

      assertEquals(value, message.readObject());
   }

   public void testReadObjectFromString() throws Exception
   {
      String value = randomString();
      HornetQStreamMessage message = new HornetQStreamMessage();
      message.writeString(value);

      message.reset();

      assertEquals(value, message.readObject());
   }

   // Private -------------------------------------------------------

   private void doReadTypeFromEmptyMessage(TypeReader reader) throws Exception
   {
      HornetQStreamMessage message = new HornetQStreamMessage();
      message.reset();

      try
      {
         reader.readType(message);
         fail("MessageEOFException");
      } catch (MessageEOFException e)
      {
      }
   }

   private void doReadTypeFromInvalidType(Object invalidValue, TypeReader reader)
         throws Exception
   {
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeObject(invalidValue);
      message.reset();

      try
      {
         reader.readType(message);
         fail("MessageFormatException");
      } catch (MessageFormatException e)
      {
      }
   }

   private void doWriteObjectWithType(Object value, TypeReader reader)
         throws Exception
   {
      HornetQStreamMessage message = new HornetQStreamMessage();

      message.writeObject(value);
      message.reset();

      Object v = reader.readType(message);
      if (value instanceof byte[])
      {
         assertEqualsByteArrays((byte[]) value, (byte[]) v);
      } else
      {
         assertEquals(value, v);
      }
   }

   // Inner classes -------------------------------------------------

   private interface TypeReader
   {
      Object readType(HornetQStreamMessage message) throws Exception;
   }
}
