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

package org.hornetq.utils;

import static org.hornetq.utils.DataConstants.BOOLEAN;
import static org.hornetq.utils.DataConstants.BYTE;
import static org.hornetq.utils.DataConstants.BYTES;
import static org.hornetq.utils.DataConstants.CHAR;
import static org.hornetq.utils.DataConstants.DOUBLE;
import static org.hornetq.utils.DataConstants.FLOAT;
import static org.hornetq.utils.DataConstants.INT;
import static org.hornetq.utils.DataConstants.LONG;
import static org.hornetq.utils.DataConstants.NOT_NULL;
import static org.hornetq.utils.DataConstants.NULL;
import static org.hornetq.utils.DataConstants.SHORT;
import static org.hornetq.utils.DataConstants.SIZE_BOOLEAN;
import static org.hornetq.utils.DataConstants.SIZE_BYTE;
import static org.hornetq.utils.DataConstants.SIZE_CHAR;
import static org.hornetq.utils.DataConstants.SIZE_DOUBLE;
import static org.hornetq.utils.DataConstants.SIZE_FLOAT;
import static org.hornetq.utils.DataConstants.SIZE_INT;
import static org.hornetq.utils.DataConstants.SIZE_LONG;
import static org.hornetq.utils.DataConstants.SIZE_SHORT;
import static org.hornetq.utils.DataConstants.STRING;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.hornetq.core.buffers.HornetQBuffer;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.PropertyConversionException;

/**
 * 
 * A TypedProperties
 * 
 * TODO - should have typed property getters and do conversions herein
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class TypedProperties
{
   private static final Logger log = Logger.getLogger(TypedProperties.class);

   private Map<SimpleString, PropertyValue> properties;

   private volatile int size;

   public TypedProperties()
   {
   }

   public TypedProperties(final TypedProperties other)
   {
      properties = other.properties == null ? null : new HashMap<SimpleString, PropertyValue>(other.properties);
      size = other.size;
   }

   public void putBooleanProperty(final SimpleString key, final boolean value)
   {
      checkCreateProperties();
      doPutValue(key, new BooleanValue(value));
   }

   public void putByteProperty(final SimpleString key, final byte value)
   {
      checkCreateProperties();
      doPutValue(key, new ByteValue(value));
   }

   public void putBytesProperty(final SimpleString key, final byte[] value)
   {
      checkCreateProperties();
      doPutValue(key, value == null ? new NullValue() : new BytesValue(value));
   }

   public void putShortProperty(final SimpleString key, final short value)
   {
      checkCreateProperties();
      doPutValue(key, new ShortValue(value));
   }

   public void putIntProperty(final SimpleString key, final int value)
   {
      checkCreateProperties();
      doPutValue(key, new IntValue(value));
   }

   public void putLongProperty(final SimpleString key, final long value)
   {
      checkCreateProperties();
      doPutValue(key, new LongValue(value));
   }

   public void putFloatProperty(final SimpleString key, final float value)
   {
      checkCreateProperties();
      doPutValue(key, new FloatValue(value));
   }

   public void putDoubleProperty(final SimpleString key, final double value)
   {
      checkCreateProperties();
      doPutValue(key, new DoubleValue(value));
   }

   public void putSimpleStringProperty(final SimpleString key, final SimpleString value)
   {
      checkCreateProperties();
      doPutValue(key, value == null ? new NullValue() : new StringValue(value));
   }

   public void putCharProperty(final SimpleString key, final char value)
   {
      checkCreateProperties();
      doPutValue(key, new CharValue(value));
   }

   public void putTypedProperties(final TypedProperties otherProps)
   {
      if (otherProps == null || otherProps.properties == null)
      {
         return;
      }
      
      checkCreateProperties();
      Set<Entry<SimpleString,PropertyValue>> otherEntries = otherProps.properties.entrySet();
      for (Entry<SimpleString, PropertyValue> otherEntry : otherEntries)
      {
         doPutValue(otherEntry.getKey(), otherEntry.getValue());
      }
   }
   
   public Object getProperty(final SimpleString key)
   {
      return doGetProperty(key);
   }

   public Boolean getBooleanProperty(final SimpleString key) throws PropertyConversionException
   {
      Object value = doGetProperty(key);
      if (value == null)
      {
         return Boolean.valueOf(null);
      }
      else if (value instanceof Boolean)
      {
         return (Boolean)value;
      }
      else if (value instanceof SimpleString)
      {
         return Boolean.valueOf(((SimpleString)value).toString()).booleanValue();
      }
      else
      {
         throw new PropertyConversionException("Invalid conversion");
      }
   }
   
   public Byte getByteProperty(SimpleString key) throws PropertyConversionException
   {
      Object value = doGetProperty(key);
      if (value == null)
      {
         return Byte.valueOf(null);
      }
      else if (value instanceof Byte)
      {
         return (Byte)value;
      }
      else if (value instanceof SimpleString)
      {
         return Byte.parseByte(((SimpleString)value).toString());
      }
      else
      {
         throw new PropertyConversionException("Invalid conversion");
      }
   }
   
   public Character getCharProperty(SimpleString key) throws PropertyConversionException
   {
      Object value = doGetProperty(key);
      if (value == null)
         throw new NullPointerException("Invalid conversion");

      if (value instanceof Character)
         return ((Character) value).charValue();
      else
         throw new PropertyConversionException("Invalid conversion");
   }
   
   public byte[] getBytesProperty(SimpleString key) throws PropertyConversionException
   {
      Object value = doGetProperty(key);
      if (value == null)
      {
         return null;
      }
      else if (value instanceof byte[])
      {
         return (byte[])value;
      }
      else
      {
         throw new PropertyConversionException("Invalid conversion");
      }
   }
   
   public Double getDoubleProperty(SimpleString key) throws PropertyConversionException
   {
      Object value = doGetProperty(key);
      if (value == null)
      {
         return Double.valueOf(null);
      }
      else if (value instanceof Float)
      {
         return ((Float)value).doubleValue();
      }
      else if (value instanceof Double)
      {
         return (Double)value;
      }
      else if (value instanceof SimpleString)
      {
         return Double.parseDouble(((SimpleString)value).toString());
      }
      else
      {
         throw new PropertyConversionException("Invalid conversion");
      }
   }
   
   public Integer getIntProperty(SimpleString key) throws PropertyConversionException
   {
      Object value = doGetProperty(key);
      if (value == null)
      {
         return Integer.valueOf(null);
      }
      else if (value instanceof Integer)
      {
         return (Integer)value;
      }
      else if (value instanceof Byte)
      {
         return ((Byte)value).intValue();
      }
      else if (value instanceof Short)
      {
         return ((Short)value).intValue();
      }
      else if (value instanceof SimpleString)
      {
         return Integer.parseInt(((SimpleString)value).toString());
      }
      else
      {
         throw new PropertyConversionException("Invalid conversion");
      }
   }
   
   public Long getLongProperty(SimpleString key) throws PropertyConversionException
   {
      Object value = doGetProperty(key);
      if (value == null)
      {
         return Long.valueOf(null);
      }
      else if (value instanceof Long)
      {
         return (Long)value;
      }
      else if (value instanceof Byte)
      {
         return ((Byte)value).longValue();
      }
      else if (value instanceof Short)
      {
         return ((Short)value).longValue();
      }
      else if (value instanceof Integer)
      {
         return ((Integer)value).longValue();
      }
      else if (value instanceof SimpleString)
      {
         return Long.parseLong(((SimpleString)value).toString());
      }
      else
      {
         throw new PropertyConversionException("Invalid conversion");
      }
   }
   
   public Short getShortProperty(SimpleString key) throws PropertyConversionException
   {
      Object value = doGetProperty(key);
      if (value == null)
      {
         return Short.valueOf(null);
      }
      else if (value instanceof Byte)
      {
         return ((Byte)value).shortValue();
      }
      else if (value instanceof Short)
      {
         return (Short)value;
      }
      else if (value instanceof SimpleString)
      {
         return Short.parseShort(((SimpleString)value).toString());
      }
      else
      {
         throw new PropertyConversionException("Invalid Conversion.");
      }
   }
   
   public Float getFloatProperty(SimpleString key) throws PropertyConversionException
   {
      Object value = doGetProperty(key);
      if (value == null)
      {
         return Float.valueOf(null);
      }

      if (value instanceof Float)
      {
         return ((Float)value).floatValue();
      }
      else if (value instanceof SimpleString)
      {
         return Float.parseFloat(((SimpleString)value).toString());
      }
      else
      {
         throw new PropertyConversionException("Invalid conversion");
      }
   }
   
   public SimpleString getSimpleStringProperty(SimpleString key) throws PropertyConversionException
   {
      Object value = doGetProperty(key);

      if (value == null)
      {
         return null;
      }

      if (value instanceof SimpleString)
      {
         return (SimpleString)value;
      }
      else if (value instanceof Boolean)
      {
         return new SimpleString(value.toString());
      }
      else if (value instanceof Character)
      {
         return new SimpleString(value.toString());
      }
      else if (value instanceof Byte)
      {
         return new SimpleString(value.toString());
      }
      else if (value instanceof Short)
      {
         return new SimpleString(value.toString());
      }
      else if (value instanceof Integer)
      {
         return new SimpleString(value.toString());
      }
      else if (value instanceof Long)
      {
         return new SimpleString(value.toString());
      }
      else if (value instanceof Float)
      {
         return new SimpleString(value.toString());
      }
      else if (value instanceof Double)
      {
         return new SimpleString(value.toString());
      }
      else
      {
         throw new PropertyConversionException("Invalid conversion");
      }
   }
   
   public Object removeProperty(final SimpleString key)
   {
      return doRemoveProperty(key);
   }

   public boolean containsProperty(final SimpleString key)
   {
      if (properties != null)
      {
         return properties.containsKey(key);
      }
      else
      {
         return false;
      }
   }

   public Set<SimpleString> getPropertyNames()
   {
      if (properties != null)
      {
         return properties.keySet();
      }
      else
      {
         return Collections.EMPTY_SET;
      }
   }

   public synchronized void decode(final HornetQBuffer buffer)
   {
      byte b = buffer.readByte();

      if (b == NULL)
      {
         properties = null;
      }
      else
      {
         int numHeaders = buffer.readInt();

         properties = new HashMap<SimpleString, PropertyValue>(numHeaders);
         size = 0;

         for (int i = 0; i < numHeaders; i++)
         {
            int len = buffer.readInt();        
            byte[] data = new byte[len];
            buffer.readBytes(data);
            SimpleString key = new SimpleString(data);

            byte type = buffer.readByte();

            PropertyValue val;

            switch (type)
            {
               case NULL:
               {
                  val = new NullValue();
                  doPutValue(key, val);
                  break;
               }
               case CHAR:
               {
                  val = new CharValue(buffer);
                  doPutValue(key, val);
                  break;
               }
               case BOOLEAN:
               {
                  val = new BooleanValue(buffer);
                  doPutValue(key, val);
                  break;
               }
               case BYTE:
               {
                  val = new ByteValue(buffer);
                  doPutValue(key, val);
                  break;
               }
               case BYTES:
               {
                  val = new BytesValue(buffer);
                  doPutValue(key, val);
                  break;
               }
               case SHORT:
               {
                  val = new ShortValue(buffer);
                  doPutValue(key, val);
                  break;
               }
               case INT:
               {
                  val = new IntValue(buffer);
                  doPutValue(key, val);
                  break;
               }
               case LONG:
               {
                  val = new LongValue(buffer);
                  doPutValue(key, val);
                  break;
               }
               case FLOAT:
               {
                  val = new FloatValue(buffer);
                  doPutValue(key, val);
                  break;
               }
               case DOUBLE:
               {
                  val = new DoubleValue(buffer);
                  doPutValue(key, val);
                  break;
               }
               case STRING:
               {
                  val = new StringValue(buffer);
                  doPutValue(key, val);
                  break;
               }
               default:
               {
                  throw new IllegalArgumentException("Invalid type: " + type);
               }
            }
         }
      }
   }

   public synchronized void encode(final HornetQBuffer buffer)
   {
      if (properties == null)
      {
         buffer.writeByte(NULL);
      }
      else
      {
         buffer.writeByte(NOT_NULL);

         buffer.writeInt(properties.size());

         for (Map.Entry<SimpleString, PropertyValue> entry : properties.entrySet())
         {
            SimpleString s = entry.getKey();
            byte[] data = s.getData();
            buffer.writeInt(data.length);
            buffer.writeBytes(data);

            entry.getValue().write(buffer);
         }
      }
   }

   public int getEncodeSize()
   {
      if (properties == null)
      {
         return SIZE_BYTE;
      }
      else
      {
         return SIZE_BYTE + SIZE_INT + size;

      }
   }

   public void clear()
   {
      if (properties != null)
      {
         properties.clear();
      }
   }
   
   @Override
   public String toString()
   {
      return "TypedProperties[" + properties  + "]";
   }
   
   // Private ------------------------------------------------------------------------------------

   private void checkCreateProperties()
   {
      if (properties == null)
      {
         properties = new HashMap<SimpleString, PropertyValue>();
      }
   }

   private synchronized void doPutValue(final SimpleString key, final PropertyValue value)
   {
      PropertyValue oldValue = properties.put(key, value);
      if (oldValue != null)
      {
         size += value.encodeSize() - oldValue.encodeSize();
      }
      else
      {
         size += SimpleString.sizeofString(key) + value.encodeSize();
      }
   }

   private synchronized Object doRemoveProperty(final SimpleString key)
   {
      if (properties == null)
      {
         return null;
      }

      PropertyValue val = properties.remove(key);

      if (val == null)
      {
         return null;
      }
      else
      {
         size -= SimpleString.sizeofString(key) + val.encodeSize();

         return val.getValue();
      }
   }

   private synchronized Object doGetProperty(final Object key)
   {
      if (properties == null)
      {
         return null;
      }

      PropertyValue val = properties.get(key);

      if (val == null)
      {
         return null;
      }
      else
      {
         return val.getValue();
      }
   }

   // Inner classes ------------------------------------------------------------------------------

   private static abstract class PropertyValue
   {
      abstract Object getValue();

      abstract void write(HornetQBuffer buffer);

      abstract int encodeSize();
      
      @Override
      public String toString()
      {
         return "" + getValue();
      }
   }

   private static final class NullValue extends PropertyValue
   {
      public NullValue()
      {
      }

      public Object getValue()
      {
         return null;
      }

      public void write(final HornetQBuffer buffer)
      {
         buffer.writeByte(NULL);
      }

      public int encodeSize()
      {
         return SIZE_BYTE;
      }

   }

   private static final class BooleanValue extends PropertyValue
   {
      final boolean val;

      public BooleanValue(final boolean val)
      {
         this.val = val;
      }

      public BooleanValue(final HornetQBuffer buffer)
      {
         val = buffer.readBoolean();
      }

      public Object getValue()
      {
         return val;
      }

      public void write(final HornetQBuffer buffer)
      {
         buffer.writeByte(BOOLEAN);
         buffer.writeBoolean(val);
      }

      public int encodeSize()
      {
         return SIZE_BYTE + SIZE_BOOLEAN;
      }

   }

   private static final class ByteValue extends PropertyValue
   {
      final byte val;

      public ByteValue(final byte val)
      {
         this.val = val;
      }

      public ByteValue(final HornetQBuffer buffer)
      {
         val = buffer.readByte();
      }

      public Object getValue()
      {
         return val;
      }

      public void write(final HornetQBuffer buffer)
      {
         buffer.writeByte(BYTE);
         buffer.writeByte(val);
      }

      public int encodeSize()
      {
         return SIZE_BYTE + SIZE_BYTE;
      }
   }

   private static final class BytesValue extends PropertyValue
   {
      final byte[] val;

      public BytesValue(final byte[] val)
      {
         this.val = val;
      }

      public BytesValue(final HornetQBuffer buffer)
      {
         int len = buffer.readInt();
         val = new byte[len];
         buffer.readBytes(val);
      }

      public Object getValue()
      {
         return val;
      }

      public void write(final HornetQBuffer buffer)
      {
         buffer.writeByte(BYTES);
         buffer.writeInt(val.length);
         buffer.writeBytes(val);
      }

      public int encodeSize()
      {
         return SIZE_BYTE + SIZE_INT + val.length;
      }

   }

   private static final class ShortValue extends PropertyValue
   {
      final short val;

      public ShortValue(final short val)
      {
         this.val = val;
      }

      public ShortValue(final HornetQBuffer buffer)
      {
         val = buffer.readShort();
      }

      public Object getValue()
      {
         return val;
      }

      public void write(final HornetQBuffer buffer)
      {
         buffer.writeByte(SHORT);
         buffer.writeShort(val);
      }

      public int encodeSize()
      {
         return SIZE_BYTE + SIZE_SHORT;
      }
   }

   private static final class IntValue extends PropertyValue
   {
      final int val;

      public IntValue(final int val)
      {
         this.val = val;
      }

      public IntValue(final HornetQBuffer buffer)
      {
         val = buffer.readInt();
      }

      public Object getValue()
      {
         return val;
      }

      public void write(final HornetQBuffer buffer)
      {
         buffer.writeByte(INT);
         buffer.writeInt(val);
      }

      public int encodeSize()
      {
         return SIZE_BYTE + SIZE_INT;
      }
   }

   private static final class LongValue extends PropertyValue
   {
      final long val;

      public LongValue(final long val)
      {
         this.val = val;
      }

      public LongValue(final HornetQBuffer buffer)
      {
         val = buffer.readLong();
      }

      public Object getValue()
      {
         return val;
      }

      public void write(final HornetQBuffer buffer)
      {
         buffer.writeByte(LONG);
         buffer.writeLong(val);
      }

      public int encodeSize()
      {
         return SIZE_BYTE + SIZE_LONG;
      }
   }

   private static final class FloatValue extends PropertyValue
   {
      final float val;

      public FloatValue(final float val)
      {
         this.val = val;
      }

      public FloatValue(final HornetQBuffer buffer)
      {
         val = Float.intBitsToFloat(buffer.readInt());
      }

      public Object getValue()
      {
         return val;
      }

      public void write(final HornetQBuffer buffer)
      {
         buffer.writeByte(FLOAT);
         buffer.writeInt(Float.floatToIntBits(val));
      }

      public int encodeSize()
      {
         return SIZE_BYTE + SIZE_FLOAT;
      }

   }

   private static final class DoubleValue extends PropertyValue
   {
      final double val;

      public DoubleValue(final double val)
      {
         this.val = val;
      }

      public DoubleValue(final HornetQBuffer buffer)
      {
         val = Double.longBitsToDouble(buffer.readLong());
      }

      public Object getValue()
      {
         return val;
      }

      public void write(final HornetQBuffer buffer)
      {
         buffer.writeByte(DOUBLE);
         buffer.writeLong(Double.doubleToLongBits(val));
      }

      public int encodeSize()
      {
         return SIZE_BYTE + SIZE_DOUBLE;
      }
   }

   private static final class CharValue extends PropertyValue
   {
      final char val;

      public CharValue(final char val)
      {
         this.val = val;
      }

      public CharValue(final HornetQBuffer buffer)
      {
         val = (char)buffer.readShort();
      }

      public Object getValue()
      {
         return val;
      }

      public void write(final HornetQBuffer buffer)
      {
         buffer.writeByte(CHAR);
         buffer.writeShort((short)val);
      }

      public int encodeSize()
      {
         return SIZE_BYTE + SIZE_CHAR;
      }
   }

   private static final class StringValue extends PropertyValue
   {
      final SimpleString val;

      public StringValue(final SimpleString val)
      {
         this.val = val;
      }

      public StringValue(final HornetQBuffer buffer)
      {
         val = buffer.readSimpleString();
      }

      public Object getValue()
      {
         return val;
      }

      public void write(final HornetQBuffer buffer)
      {
         buffer.writeByte(STRING);
         buffer.writeSimpleString(val);
      }

      public int encodeSize()
      {
         return SIZE_BYTE + SimpleString.sizeofString(val);
      }
   }
}
