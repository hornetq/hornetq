/*
 * JBoss, Home of Professional Open Source
 *
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
package org.jboss.messaging.core.remoting.impl;

import static org.jboss.messaging.util.DataConstants.*;

import java.nio.ByteBuffer;

import org.jboss.messaging.core.remoting.MessagingBuffer;
import org.jboss.messaging.util.SimpleString;

/**
 * A {@link MessagingBuffer} which increases its capacity and length by itself
 * when there's not enough space for a {@code put} operation.
 *
 * @author <a href="mailto:tlee@redhat.com">Trustin Lee</a>
 * @version $Rev$, $Date$
 */
public class ExpandingMessagingBuffer implements MessagingBuffer
{
   private ByteBuffer buf;

   public ExpandingMessagingBuffer(ByteBuffer buf) {
      this.buf = buf;
   }

   public ExpandingMessagingBuffer(int size) {
      this(ByteBuffer.allocate(size));
   }

   public byte[] array()
   {
      if(buf.hasArray() && buf.arrayOffset() == 0 && buf.capacity() == buf.array().length)
      {
         return buf.array();
      }
      else
      {
         byte[] b = new byte[remaining()];
         getBytes(b);
         return b;
      }
   }

   public int capacity()
   {
      return buf.capacity();
   }

   public MessagingBuffer createNewBuffer(int len)
   {
      return new ExpandingMessagingBuffer(len);
   }

   public void flip()
   {
      buf.flip();
   }

   public boolean getBoolean()
   {
      return getByte() != 0;
   }

   public byte getByte()
   {
      return buf.get();
   }

   public void getBytes(byte[] bytes)
   {
      buf.get(bytes);
   }

   public void getBytes(byte[] bytes, int offset, int length)
   {
      buf.get(bytes, offset, length);
   }

   public char getChar()
   {
      return buf.getChar();
   }

   public double getDouble()
   {
      return buf.getDouble();
   }

   public float getFloat()
   {
      return buf.getFloat();
   }

   public int getInt()
   {
      return buf.getInt();
   }

   public long getLong()
   {
      return buf.getLong();
   }

   public SimpleString getNullableSimpleString()
   {
      if (getByte() == NULL)
      {
         return null;
      }
      else
      {
         return getSimpleString();
      }
   }

   public String getNullableString()
   {
      if (getByte() == NULL)
      {
         return null;
      }
      else
      {
         return getString();
      }
   }

   public short getShort()
   {
      return buf.getShort();
   }

   public SimpleString getSimpleString()
   {
      int len = getInt();
      byte[] data = new byte[len];
      getBytes(data);
      return new SimpleString(data);
   }

   public String getString()
   {
      int len = getInt();
      char[] chars = new char[len];
      for (int i = 0; i < len; i++)
      {
         chars[i] = getChar();
      }
      return new String(chars);
   }

   public String getUTF() throws Exception
   {
      int length = getUnsignedShort();
      byte[] data = new byte[length];
      getBytes(data);
      return new String(data, "UTF-8");
   }

   public short getUnsignedByte()
   {
      return (short) (getByte() & 0xFF);
   }

   public int getUnsignedShort()
   {
      return getShort() & 0xFFFF;
   }

   public int limit()
   {
      return buf.limit();
   }

   public void limit(int limit)
   {
      buf.limit(limit);
   }

   public void position(int position)
   {
      buf.position(position);
   }

   public int position()
   {
      return buf.position();
   }

   public void putBoolean(boolean val)
   {
      putByte((byte) (val ? -1 : 0));
   }

   public void putByte(byte val)
   {
      ensureRemaining(1).put(val);
   }

   public void putBytes(byte[] bytes)
   {
      ensureRemaining(bytes.length).put(bytes);
   }

   public void putBytes(byte[] bytes, int offset, int length)
   {
      ensureRemaining(length).put(bytes, offset, length);
   }

   public void putChar(char val)
   {
      ensureRemaining(2).putChar(val);
   }

   public void putDouble(double val)
   {
      ensureRemaining(8).putDouble(val);
   }

   public void putFloat(float val)
   {
      ensureRemaining(4).putFloat(val);
   }

   public void putInt(int val)
   {
      ensureRemaining(4).putInt(val);
   }

   public void putInt(int pos, int val)
   {
      buf.putInt(pos, val);
   }

   public void putLong(long val)
   {
      ensureRemaining(8).putLong(val);
   }

   public void putNullableSimpleString(SimpleString val)
   {
      if (val == null)
      {
         ensureRemaining(1).put(NULL);
      }
      else
      {
         ensureRemaining(5 + (val.length() << 1));
         buf.put(NOT_NULL);
         byte[] data = val.getData();
         ensureRemaining(data.length + 4);
         buf.putInt(data.length);
         buf.put(data);
      }
   }

   public void putNullableString(String val)
   {
      if (val == null)
      {
         ensureRemaining(1).put(NULL);
      }
      else
      {
         ensureRemaining(5 + (val.length() << 1));
         buf.put(NOT_NULL);
         buf.putInt(val.length());
         for (int i = 0; i < val.length(); i++)
         {
            buf.putChar(val.charAt(i));
         }
      }
   }

   public void putShort(short val)
   {
      ensureRemaining(2).putShort(val);
   }

   public void putSimpleString(SimpleString val)
   {
      byte[] data = val.getData();
      ensureRemaining(4 + data.length);
      buf.putInt(data.length);
      buf.put(data);
   }

   public void putString(String val)
   {
      ensureRemaining(4 + (val.length() << 1));
      buf.putInt(val.length());
      for (int i = 0; i < val.length(); i++)
      {
         buf.putChar(val.charAt(i));
      }
   }

   public void putUTF(String utf) throws Exception
   {
      byte[] data = utf.getBytes("UTF-8");
      if (data.length > 65535) {
         throw new IllegalArgumentException("String is too long: " + data.length);
      }
      ensureRemaining(2 + data.length);
      buf.putShort((short) data.length);
      buf.put(data);
   }

   public int remaining()
   {
      return buf.remaining();
   }

   public void rewind()
   {
      buf.rewind();
   }

   public MessagingBuffer slice()
   {
      return new ExpandingMessagingBuffer(buf.slice());
   }
   
   public Object getUnderlyingBuffer()
   {
      return buf;
   }

   private ByteBuffer ensureRemaining(int minRemaining)
   {
      int remaining = remaining();
      if (remaining >= minRemaining)
      {
         return buf;
      }

      int capacity = capacity();
      int limit = limit();
      if (capacity - limit >= minRemaining - remaining) {
         buf.limit(limit + minRemaining - remaining);
         return buf;
      }

      int position = position();
      int minCapacityDifference = minRemaining - remaining;
      int oldCapacity = capacity();
      int newCapacity = oldCapacity;
      for (;;)
      {
         newCapacity <<= 1;
         if (newCapacity - oldCapacity >= minCapacityDifference)
         {
            break;
         }
      }

      ByteBuffer newBuf = ByteBuffer.allocate(newCapacity);
      buf.clear();
      newBuf.put(buf);
      newBuf.limit(position + minRemaining);
      newBuf.position(position);
      buf = newBuf;

      return newBuf;
   }
}
