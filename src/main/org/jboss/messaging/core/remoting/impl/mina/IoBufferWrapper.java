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

package org.jboss.messaging.core.remoting.impl.mina;

import static org.jboss.messaging.util.DataConstants.*;

import java.nio.charset.Charset;

import org.apache.mina.core.buffer.IoBuffer;
import org.jboss.messaging.core.remoting.MessagingBuffer;
import org.jboss.messaging.util.SimpleString;

/**
 *
 * A BufferWrapper
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class IoBufferWrapper implements MessagingBuffer
{
   // Constants -----------------------------------------------------

   private static final Charset utf8 = Charset.forName("UTF-8");

   // Attributes ----------------------------------------------------

   private final IoBuffer buffer;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public IoBufferWrapper(final int size)
   {
      buffer = IoBuffer.allocate(size);

      buffer.setAutoExpand(true);
   }

   public IoBufferWrapper(final IoBuffer buffer)
   {
      this.buffer = buffer;
   }

   // Public --------------------------------------------------------

   // MessagingBuffer implementation ----------------------------------------------

   public byte[] array()
   {
      return buffer.array();
   }

   public int position()
   {
      return buffer.position();
   }

   public void position(final int position)
   {
      buffer.position(position);
   }

   public int limit()
   {
      return buffer.limit();
   }

   public void limit(final int limit)
   {
      buffer.limit(limit);
   }

   public int capacity()
   {
      return buffer.capacity();
   }

   public void flip()
   {
      buffer.flip();
   }

   public MessagingBuffer slice()
   {
      return new IoBufferWrapper(buffer.slice());
   }

   public MessagingBuffer createNewBuffer(int len)
   {
      return new IoBufferWrapper(len);
   }

   public int remaining()
   {
      return buffer.remaining();
   }

   public void rewind()
   {
      buffer.rewind();
   }

   public void putByte(byte byteValue)
   {
      buffer.put(byteValue);
   }

   public void putBytes(final byte[] byteArray)
   {
      buffer.put(byteArray);
   }

   public void putBytes(final byte[] bytes, int offset, int length)
   {
      buffer.put(bytes, offset, length);
   }

   public void putInt(final int intValue)
   {
      buffer.putInt(intValue);
   }

   public void putInt(final int pos, final int intValue)
   {
      buffer.putInt(pos, intValue);
   }

   public void putLong(final long longValue)
   {
      buffer.putLong(longValue);
   }

   public void putFloat(final float floatValue)
   {
      buffer.putFloat(floatValue);
   }

   public void putDouble(final double d)
   {
      buffer.putDouble(d);
   }

   public void putShort(final short s)
   {
      buffer.putShort(s);
   }

   public void putChar(final char chr)
   {
      buffer.putChar(chr);
   }

   public byte getByte()
   {
      return buffer.get();
   }

   public short getUnsignedByte()
   {
      return buffer.getUnsigned();
   }

   public void getBytes(final byte[] b)
   {
      buffer.get(b);
   }

   public void getBytes(final byte[] b, final int offset, final int length)
   {
      buffer.get(b, offset, length);
   }

   public int getInt()
   {
      return buffer.getInt();
   }

   public long getLong()
   {
      return buffer.getLong();
   }

   public float getFloat()
   {
      return buffer.getFloat();
   }

   public short getShort()
   {
      return buffer.getShort();
   }

   public int getUnsignedShort()
   {
      return buffer.getUnsignedShort();
   }

   public double getDouble()
   {
      return buffer.getDouble();
   }

   public char getChar()
   {
      return buffer.getChar();
   }

   public void putBoolean(final boolean b)
   {
      if (b)
      {
         buffer.put(TRUE);
      } else
      {
         buffer.put(FALSE);
      }
   }

   public boolean getBoolean()
   {
      byte b = buffer.get();
      return b == TRUE;
   }

   public void putString(final String nullableString)
   {
      buffer.putInt(nullableString.length());

      for (int i = 0; i < nullableString.length(); i++)
      {
         buffer.putChar(nullableString.charAt(i));
      }
   }

   public void putNullableString(final String nullableString)
   {
      if (nullableString == null)
      {
         buffer.put(NULL);
      }
      else
      {
         buffer.put(NOT_NULL);

         putString(nullableString);
      }
   }

   public String getString()
   {
      int len = buffer.getInt();

      char[] chars = new char[len];

      for (int i = 0; i < len; i++)
      {
         chars[i] = buffer.getChar();
      }

      return new String(chars);
   }

   public String getNullableString()
   {
      byte check = buffer.get();

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
      buffer.putPrefixedString(str, utf8.newEncoder());
   }

   public void putNullableSimpleString(final SimpleString string)
   {
      if (string == null)
      {
         buffer.put(NULL);
      }
      else
      {
         buffer.put(NOT_NULL);
         putSimpleString(string);
      }
   }

   public void putSimpleString(final SimpleString string)
   {
      byte[] data = string.getData();

      buffer.putInt(data.length);
      buffer.put(data);
   }

   public SimpleString getSimpleString()
   {
      int len = buffer.getInt();

      byte[] data = new byte[len];
      buffer.get(data);

      return new SimpleString(data);
   }

   public SimpleString getNullableSimpleString()
   {
      int b = buffer.get();
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
      return buffer.getPrefixedString(utf8.newDecoder());
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