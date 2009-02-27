/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.buffers;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.UnsupportedCharsetException;

/**
 * A skeletal implementation for Java heap buffers.
 *
 * @author The Netty Project (netty-dev@lists.jboss.org)
 * @author Trustin Lee (tlee@redhat.com)
 *
 * @version $Rev: 486 $, $Date: 2008-11-16 22:52:47 +0900 (Sun, 16 Nov 2008) $
 */
public class HeapChannelBuffer extends AbstractChannelBuffer
{

   /**
    * The underlying heap byte array that this buffer is wrapping.
    */
   protected final byte[] array;

   /**
    * Creates a new heap buffer with a newly allocated byte array.
    *
    * @param length the length of the new byte array
    */
   HeapChannelBuffer(final int length)
   {
      this(new byte[length], 0, 0);
   }

   /**
    * Creates a new heap buffer with an existing byte array.
    *
    * @param array the byte array to wrap
    */
   HeapChannelBuffer(final byte[] array)
   {
      this(array, 0, array.length);
   }

   /**
    * Creates a new heap buffer with an existing byte array.
    *
    * @param array        the byte array to wrap
    * @param readerIndex  the initial reader index of this buffer
    * @param writerIndex  the initial writer index of this buffer
    */
   protected HeapChannelBuffer(final byte[] array, final int readerIndex, final int writerIndex)
   {
      if (array == null)
      {
         throw new NullPointerException("array");
      }
      this.array = array;
      setIndex(readerIndex, writerIndex);
   }

   public int capacity()
   {
      return array.length;
   }

   public byte getByte(final int index)
   {
      return array[index];
   }

   public void getBytes(final int index, final ChannelBuffer dst, final int dstIndex, final int length)
   {
      if (dst instanceof HeapChannelBuffer)
      {
         getBytes(index, ((HeapChannelBuffer)dst).array, dstIndex, length);
      }
      else
      {
         dst.setBytes(dstIndex, array, index, length);
      }
   }

   public void getBytes(final int index, final byte[] dst, final int dstIndex, final int length)
   {
      System.arraycopy(array, index, dst, dstIndex, length);
   }

   public void getBytes(final int index, final ByteBuffer dst)
   {
      dst.put(array, index, Math.min(capacity() - index, dst.remaining()));
   }

   public void getBytes(final int index, final OutputStream out, final int length) throws IOException
   {
      out.write(array, index, length);
   }

   public int getBytes(final int index, final GatheringByteChannel out, final int length) throws IOException
   {
      return out.write(ByteBuffer.wrap(array, index, length));
   }

   public void setByte(final int index, final byte value)
   {
      array[index] = value;
   }

   public void setBytes(final int index, final ChannelBuffer src, final int srcIndex, final int length)
   {
      if (src instanceof HeapChannelBuffer)
      {
         setBytes(index, ((HeapChannelBuffer)src).array, srcIndex, length);
      }
      else
      {
         src.getBytes(srcIndex, array, index, length);
      }
   }

   public void setBytes(final int index, final byte[] src, final int srcIndex, final int length)
   {
      System.arraycopy(src, srcIndex, array, index, length);
   }

   public void setBytes(final int index, final ByteBuffer src)
   {
      src.get(array, index, src.remaining());
   }

   public int setBytes(int index, final InputStream in, int length) throws IOException
   {
      int readBytes = 0;
      do
      {
         int localReadBytes = in.read(array, index, length);
         if (localReadBytes < 0)
         {
            if (readBytes == 0)
            {
               return -1;
            }
            else
            {
               break;
            }
         }
         readBytes += localReadBytes;
         index += localReadBytes;
         length -= localReadBytes;
      }
      while (length > 0);

      return readBytes;
   }

   public int setBytes(final int index, final ScatteringByteChannel in, final int length) throws IOException
   {
      ByteBuffer buf = ByteBuffer.wrap(array, index, length);
      int readBytes = 0;

      do
      {
         int localReadBytes;
         try
         {
            localReadBytes = in.read(buf);
         }
         catch (ClosedChannelException e)
         {
            localReadBytes = -1;
         }
         if (localReadBytes < 0)
         {
            if (readBytes == 0)
            {
               return -1;
            }
            else
            {
               break;
            }
         }
         else if (localReadBytes == 0)
         {
            break;
         }
         readBytes += localReadBytes;
      }
      while (readBytes < length);

      return readBytes;
   }

   public short getShort(final int index)
   {
      return (short)(array[index] << 8 | array[index + 1] & 0xFF);
   }

   public int getUnsignedMedium(final int index)
   {
      return (array[index] & 0xff) << 16 | (array[index + 1] & 0xff) << 8 | (array[index + 2] & 0xff) << 0;
   }

   public int getInt(final int index)
   {
      return (array[index] & 0xff) << 24 | (array[index + 1] & 0xff) << 16 |
             (array[index + 2] & 0xff) << 8 |
             (array[index + 3] & 0xff) << 0;
   }

   public long getLong(final int index)
   {
      return ((long)array[index] & 0xff) << 56 | ((long)array[index + 1] & 0xff) << 48 |
             ((long)array[index + 2] & 0xff) << 40 |
             ((long)array[index + 3] & 0xff) << 32 |
             ((long)array[index + 4] & 0xff) << 24 |
             ((long)array[index + 5] & 0xff) << 16 |
             ((long)array[index + 6] & 0xff) << 8 |
             ((long)array[index + 7] & 0xff) << 0;
   }

   public void setShort(final int index, final short value)
   {
      array[index] = (byte)(value >>> 8);
      array[index + 1] = (byte)(value >>> 0);
   }

   public void setMedium(final int index, final int value)
   {
      array[index] = (byte)(value >>> 16);
      array[index + 1] = (byte)(value >>> 8);
      array[index + 2] = (byte)(value >>> 0);
   }

   public void setInt(final int index, final int value)
   {
      array[index] = (byte)(value >>> 24);
      array[index + 1] = (byte)(value >>> 16);
      array[index + 2] = (byte)(value >>> 8);
      array[index + 3] = (byte)(value >>> 0);
   }

   public void setLong(final int index, final long value)
   {
      array[index] = (byte)(value >>> 56);
      array[index + 1] = (byte)(value >>> 48);
      array[index + 2] = (byte)(value >>> 40);
      array[index + 3] = (byte)(value >>> 32);
      array[index + 4] = (byte)(value >>> 24);
      array[index + 5] = (byte)(value >>> 16);
      array[index + 6] = (byte)(value >>> 8);
      array[index + 7] = (byte)(value >>> 0);
   }

   public ChannelBuffer copy(final int index, final int length)
   {
      if (index < 0 || length < 0 || index + length > array.length)
      {
         throw new IndexOutOfBoundsException();
      }

      byte[] copiedArray = new byte[length];
      System.arraycopy(array, index, copiedArray, 0, length);
      return new HeapChannelBuffer(copiedArray);
   }

   public ChannelBuffer duplicate()
   {
      return new HeapChannelBuffer(array, readerIndex(), writerIndex());
   }

   public ByteBuffer toByteBuffer(final int index, final int length)
   {
      return ByteBuffer.wrap(array, index, length);
   }

   public String toString(final int index, final int length, final String charsetName)
   {
      try
      {
         return new String(array, index, length, charsetName);
      }
      catch (UnsupportedEncodingException e)
      {
         throw new UnsupportedCharsetException(charsetName);
      }
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.spi.MessagingBuffer#array()
    */
   public byte[] array()
   {
      return array;
   }

}
