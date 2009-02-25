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

package org.jboss.messaging.integration.transports.netty;

import static org.jboss.messaging.utils.DataConstants.FALSE;
import static org.jboss.messaging.utils.DataConstants.NOT_NULL;
import static org.jboss.messaging.utils.DataConstants.NULL;
import static org.jboss.messaging.utils.DataConstants.TRUE;
import org.jboss.messaging.utils.SimpleString;
import static org.jboss.netty.buffer.ChannelBuffers.copiedBuffer;
import static org.jboss.netty.buffer.ChannelBuffers.dynamicBuffer;

import java.nio.BufferUnderflowException;

import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.utils.UTF8Util;
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
      buffer = dynamicBuffer(size);
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
      return buffer.toByteBuffer().array();
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
      flip();
      buffer.writeByte(byteValue);
      buffer.readerIndex(buffer.writerIndex());
   }

   public void putBytes(final byte[] byteArray)
   {
      flip();
      buffer.writeBytes(byteArray);
      buffer.readerIndex(buffer.writerIndex());
   }

   public void putBytes(final byte[] bytes, int offset, int length)
   {
      flip();
      buffer.writeBytes(bytes, offset, length);
      buffer.readerIndex(buffer.writerIndex());
   }

   public void putInt(final int intValue)
   {
      flip();
      buffer.writeInt(intValue);
      buffer.readerIndex(buffer.writerIndex());
   }

   public void putInt(final int pos, final int intValue)
   {
      buffer.setInt(pos, intValue);
   }

   public void putLong(final long longValue)
   {
      flip();
      buffer.writeLong(longValue);
      buffer.readerIndex(buffer.writerIndex());
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
      flip();
      buffer.writeShort(s);
      buffer.readerIndex(buffer.writerIndex());
   }

   public void putChar(final char chr)
   {
      putShort((short)chr);
   }

   public byte getByte()
   {
      try
      {
         return buffer.readByte();
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new BufferUnderflowException();
      }
   }

   public short getUnsignedByte()
   {
      try
      {
         return buffer.readUnsignedByte();
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new BufferUnderflowException();
      }
   }

   public void getBytes(final byte[] b)
   {
      try
      {
         buffer.readBytes(b);
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new BufferUnderflowException();
      }
   }

   public void getBytes(final byte[] b, final int offset, final int length)
   {
      try
      {
         buffer.readBytes(b, offset, length);
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new BufferUnderflowException();
      }
   }

   public int getInt()
   {
      try
      {
         return buffer.readInt();
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new BufferUnderflowException();
      }
   }

   public long getLong()
   {
      try
      {
         return buffer.readLong();
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new BufferUnderflowException();
      }
   }

   public float getFloat()
   {
      return Float.intBitsToFloat(getInt());
   }

   public short getShort()
   {
      try
      {
         return buffer.readShort();
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new BufferUnderflowException();
      }
   }

   public int getUnsignedShort()
   {
      try
      {
         return buffer.readUnsignedShort();
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new BufferUnderflowException();
      }
   }

   public double getDouble()
   {
      return Double.longBitsToDouble(getLong());
   }

   public char getChar()
   {
      return (char)getShort();
   }

   public void putBoolean(final boolean b)
   {
      if (b)
      {
         putByte(TRUE);
      }
      else
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
      flip();
      buffer.writeInt(nullableString.length());
      for (int i = 0; i < nullableString.length(); i++)
      {
         buffer.writeShort((short)nullableString.charAt(i));
      }
      buffer.readerIndex(buffer.writerIndex());
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
      UTF8Util.saveUTF(this, str);
      buffer.readerIndex(buffer.writerIndex());
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

      flip();
      buffer.writeInt(data.length);
      buffer.writeBytes(data);
      buffer.readerIndex(buffer.writerIndex());
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
      return UTF8Util.readUTF(this);
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