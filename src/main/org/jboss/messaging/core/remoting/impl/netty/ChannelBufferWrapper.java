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

package org.jboss.messaging.core.remoting.impl.netty;

import static org.jboss.messaging.util.DataConstants.*;
import static org.jboss.netty.buffer.ChannelBuffers.*;

import java.nio.ByteBuffer;

import org.jboss.messaging.core.remoting.MessagingBuffer;
import org.jboss.messaging.util.SimpleString;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * Wraps Netty {@link ChannelBuffer} with {@link MessagingBuffer}.
 * Because there's neither {@code position()} nor {@code limit()} in a Netty
 * buffer.  {@link ChannelBuffer#readerIndex()} and {@link ChannelBuffer#writerIndex()}
 * are used as {@code position} and {@code limit} of the buffer respectively
 * instead.
 *
 * @author <a href="mailto:tlee@redhat.com">Trustin Lee</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * @version $Rev$, $Date$
 */
public class ChannelBufferWrapper implements MessagingBuffer
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final ChannelBuffer buffer;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ChannelBufferWrapper(final int size)
   {
      if (size == 0) {
         buffer = ChannelBuffer.EMPTY_BUFFER;
      } else {
         try {
            buffer = dynamicBuffer(size);
         } catch (IllegalArgumentException e) {
            // FIXME: This block should go away once Netty 3.0.0.CR2 is
            //        released.
            throw new IllegalArgumentException("size: " + size);
         }
      }
      buffer.writerIndex(buffer.capacity());
   }

   public ChannelBufferWrapper(final ChannelBuffer buffer)
   {
      this.buffer = buffer;
   }

   // Public --------------------------------------------------------

   // MessagingBuffer implementation ----------------------------------------------

   public byte[] array()
   {
      ByteBuffer bb = buffer.toByteBuffer();
      if (bb.hasArray() && !bb.isReadOnly()) {
         return bb.array();
      } else {
         byte[] ba = new byte[bb.remaining()];
         bb.get(ba);
         return ba;
      }
   }

   public int position()
   {
      return buffer.readerIndex();
   }

   public void position(final int position)
   {
      buffer.readerIndex(position);
   }

   public int limit()
   {
      return buffer.writerIndex();
   }

   public void limit(final int limit)
   {
      buffer.writerIndex(limit);
   }

   public int capacity()
   {
      return buffer.capacity();
   }

   public void flip()
   {
      int oldPosition = position();
      position(0);
      limit(oldPosition);
   }

   public MessagingBuffer slice()
   {
      return new ChannelBufferWrapper(buffer.slice());
   }

   public MessagingBuffer createNewBuffer(int len)
   {
      return new ChannelBufferWrapper(len);
   }

   public int remaining()
   {
      return buffer.readableBytes();
   }

   public void rewind()
   {
      position(0);
      buffer.markReaderIndex();
   }

   public void putByte(byte byteValue)
   {
      int limit = buffer.writerIndex();
      buffer.writerIndex(buffer.readerIndex());
      try {
         buffer.writeByte(byteValue);
      } finally {
         buffer.readerIndex(buffer.writerIndex());
         if (limit < buffer.readerIndex()) {
            limit = buffer.readerIndex();
         }
         buffer.writerIndex(limit);
      }
   }

   public void putBytes(final byte[] byteArray)
   {
      int limit = buffer.writerIndex();
      buffer.writerIndex(buffer.readerIndex());
      try {
         buffer.writeBytes(byteArray);
      } finally {
         buffer.readerIndex(buffer.writerIndex());
         if (limit < buffer.readerIndex()) {
            limit = buffer.readerIndex();
         }
         buffer.writerIndex(limit);
      }
   }

   public void putBytes(final byte[] bytes, int offset, int length)
   {
      int limit = buffer.writerIndex();
      buffer.writerIndex(buffer.readerIndex());
      try {
         buffer.writeBytes(bytes, offset, length);
      } finally {
         buffer.readerIndex(buffer.writerIndex());
         if (limit < buffer.readerIndex()) {
            limit = buffer.readerIndex();
         }
         buffer.writerIndex(limit);
      }
   }

   public void putInt(final int intValue)
   {
      int limit = buffer.writerIndex();
      buffer.writerIndex(buffer.readerIndex());
      try {
         buffer.writeInt(intValue);
      } finally {
         buffer.readerIndex(buffer.writerIndex());
         if (limit < buffer.readerIndex()) {
            limit = buffer.readerIndex();
         }
         buffer.writerIndex(limit);
      }
   }

   public void putInt(final int pos, final int intValue)
   {
      buffer.setInt(pos, intValue);
   }

   public void putLong(final long longValue)
   {
      int limit = buffer.writerIndex();
      buffer.writerIndex(buffer.readerIndex());
      try {
         buffer.writeLong(longValue);
      } finally {
         buffer.readerIndex(buffer.writerIndex());
         if (limit < buffer.readerIndex()) {
            limit = buffer.readerIndex();
         }
         buffer.writerIndex(limit);
      }
   }

   public void putFloat(final float floatValue)
   {
      putInt(Float.floatToIntBits(floatValue));
   }

   public void putDouble(final double d)
   {
      putLong(Double.doubleToLongBits(d));
   }

   public void putShort(final short s)
   {
      int limit = buffer.writerIndex();
      buffer.writerIndex(buffer.readerIndex());
      try {
         buffer.writeShort(s);
      } finally {
         buffer.readerIndex(buffer.writerIndex());
         if (limit < buffer.readerIndex()) {
            limit = buffer.readerIndex();
         }
         buffer.writerIndex(limit);
      }
   }

   public void putChar(final char chr)
   {
      putShort((short) chr);
   }

   public byte getByte()
   {
      return buffer.readByte();
   }

   public short getUnsignedByte()
   {
      return buffer.readUnsignedByte();
   }

   public void getBytes(final byte[] b)
   {
      buffer.readBytes(b);
   }

   public void getBytes(final byte[] b, final int offset, final int length)
   {
      buffer.readBytes(b, offset, length);
   }

   public int getInt()
   {
      return buffer.readInt();
   }

   public long getLong()
   {
      return buffer.readLong();
   }

   public float getFloat()
   {
      return Float.intBitsToFloat(getInt());
   }

   public short getShort()
   {
      return buffer.readShort();
   }

   public int getUnsignedShort()
   {
      return buffer.readUnsignedShort();
   }

   public double getDouble()
   {
      return Double.longBitsToDouble(getLong());
   }

   public char getChar()
   {
      return (char) getShort();
   }

   public void putBoolean(final boolean b)
   {
      if (b)
      {
         putByte(TRUE);
      } else
      {
         putByte(FALSE);
      }
   }

   public boolean getBoolean()
   {
      byte b = getByte();
      return b == TRUE;
   }

   public void putString(final String nullableString)
   {
      putInt(nullableString.length());

      for (int i = 0; i < nullableString.length(); i++)
      {
         putChar(nullableString.charAt(i));
      }
   }

   public void putNullableString(final String nullableString)
   {
      if (nullableString == null)
      {
         putByte(NULL);
      }
      else
      {
         putByte(NOT_NULL);
         putString(nullableString);
      }
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

   public String getNullableString()
   {
      byte check = getByte();

      if (check == NULL)
      {
         return null;
      }
      else
      {
         return getString();
      }
   }

   public void putUTF(final String str) throws Exception
   {
      ChannelBuffer encoded = copiedBuffer(str, "UTF-8");
      int length = encoded.readableBytes();
      if (length >= 65536) {
         throw new IllegalArgumentException(
               "the specified string is too long (" + length + ")");
      }

      int limit = buffer.writerIndex();
      buffer.writerIndex(buffer.readerIndex());
      try {
         buffer.writeShort((short) length);
         buffer.writeBytes(encoded);
      } finally {
         buffer.readerIndex(buffer.writerIndex());
         if (limit < buffer.readerIndex()) {
            limit = buffer.readerIndex();
         }
         buffer.writerIndex(limit);
      }
   }

   public void putNullableSimpleString(final SimpleString string)
   {
      if (string == null)
      {
         putByte(NULL);
      }
      else
      {
         putByte(NOT_NULL);
         putSimpleString(string);
      }
   }

   public void putSimpleString(final SimpleString string)
   {
      byte[] data = string.getData();

      putInt(data.length);
      putBytes(data);
   }

   public SimpleString getSimpleString()
   {
      int len = getInt();

      byte[] data = new byte[len];
      getBytes(data);

      return new SimpleString(data);
   }

   public SimpleString getNullableSimpleString()
   {
      int b = getByte();
      if (b == NULL)
      {
         return null;
      }
      else
      {
         return getSimpleString();
      }
   }

   public String getUTF() throws Exception
   {
      int length = buffer.readUnsignedShort();
      ChannelBuffer utf8value = buffer.readSlice(length);
      return utf8value.toString("UTF-8");
   }

   public Object getUnderlyingBuffer()
   {
      return buffer;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}