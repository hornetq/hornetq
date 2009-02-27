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
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

/**
 * A dynamic capacity buffer which increases its capacity as needed.  It is
 * recommended to use {@link ChannelBuffers#dynamicBuffer(int)} instead of
 * calling the constructor explicitly.
 *
 * @author The Netty Project (netty-dev@lists.jboss.org)
 * @author Trustin Lee (tlee@redhat.com)
 *
 * @version $Rev: 237 $, $Date: 2008-09-04 20:53:44 +0900 (Thu, 04 Sep 2008) $
 *
 */
public class DynamicChannelBuffer extends AbstractChannelBuffer
{

   private final int initialCapacity;

   private ChannelBuffer buffer = ChannelBuffers.EMPTY_BUFFER;

   DynamicChannelBuffer(final int estimatedLength)
   {
      if (estimatedLength < 0)
      {
         throw new IllegalArgumentException("estimatedLength: " + estimatedLength);
      }
      initialCapacity = estimatedLength;
   }

   DynamicChannelBuffer(final byte[] initialBuffer)
   {
      initialCapacity = initialBuffer.length;

      buffer = new HeapChannelBuffer(initialBuffer);

      writerIndex(initialBuffer.length);
   }

   public int capacity()
   {
      return buffer.capacity();
   }

   public byte getByte(final int index)
   {
      return buffer.getByte(index);
   }

   public short getShort(final int index)
   {
      return buffer.getShort(index);
   }

   public int getUnsignedMedium(final int index)
   {
      return buffer.getUnsignedMedium(index);
   }

   public int getInt(final int index)
   {
      return buffer.getInt(index);
   }

   public long getLong(final int index)
   {
      return buffer.getLong(index);
   }

   public void getBytes(final int index, final byte[] dst, final int dstIndex, final int length)
   {
      buffer.getBytes(index, dst, dstIndex, length);
   }

   public void getBytes(final int index, final ChannelBuffer dst, final int dstIndex, final int length)
   {
      buffer.getBytes(index, dst, dstIndex, length);
   }

   public void getBytes(final int index, final ByteBuffer dst)
   {
      buffer.getBytes(index, dst);
   }

   public int getBytes(final int index, final GatheringByteChannel out, final int length) throws IOException
   {
      return buffer.getBytes(index, out, length);
   }

   public void getBytes(final int index, final OutputStream out, final int length) throws IOException
   {
      buffer.getBytes(index, out, length);
   }

   public void setByte(final int index, final byte value)
   {
      buffer.setByte(index, value);
   }

   public void setShort(final int index, final short value)
   {
      buffer.setShort(index, value);
   }

   public void setMedium(final int index, final int value)
   {
      buffer.setMedium(index, value);
   }

   public void setInt(final int index, final int value)
   {
      buffer.setInt(index, value);
   }

   public void setLong(final int index, final long value)
   {
      buffer.setLong(index, value);
   }

   public void setBytes(final int index, final byte[] src, final int srcIndex, final int length)
   {
      buffer.setBytes(index, src, srcIndex, length);
   }

   public void setBytes(final int index, final ChannelBuffer src, final int srcIndex, final int length)
   {
      buffer.setBytes(index, src, srcIndex, length);
   }

   public void setBytes(final int index, final ByteBuffer src)
   {
      buffer.setBytes(index, src);
   }

   public int setBytes(final int index, final InputStream in, final int length) throws IOException
   {
      return buffer.setBytes(index, in, length);
   }

   public int setBytes(final int index, final ScatteringByteChannel in, final int length) throws IOException
   {
      return buffer.setBytes(index, in, length);
   }

   @Override
   public void writeByte(final byte value)
   {
      ensureWritableBytes(1);
      super.writeByte(value);
   }

   @Override
   public void writeShort(final short value)
   {
      ensureWritableBytes(2);
      super.writeShort(value);
   }

   @Override
   public void writeMedium(final int value)
   {
      ensureWritableBytes(3);
      super.writeMedium(value);
   }

   @Override
   public void writeInt(final int value)
   {
      ensureWritableBytes(4);
      super.writeInt(value);
   }

   @Override
   public void writeLong(final long value)
   {
      ensureWritableBytes(8);
      super.writeLong(value);
   }

   @Override
   public void writeBytes(final byte[] src, final int srcIndex, final int length)
   {
      ensureWritableBytes(length);
      super.writeBytes(src, srcIndex, length);
   }

   @Override
   public void writeBytes(final ChannelBuffer src, final int srcIndex, final int length)
   {
      ensureWritableBytes(length);
      super.writeBytes(src, srcIndex, length);
   }

   @Override
   public void writeBytes(final ByteBuffer src)
   {
      ensureWritableBytes(src.remaining());
      super.writeBytes(src);
   }

   @Override
   public void writeZero(final int length)
   {
      ensureWritableBytes(length);
      super.writeZero(length);
   }

   public ByteBuffer toByteBuffer(final int index, final int length)
   {
      return buffer.toByteBuffer(index, length);
   }

   public String toString(final int index, final int length, final String charsetName)
   {
      return buffer.toString(index, length, charsetName);
   }

   private void ensureWritableBytes(final int requestedBytes)
   {
      if (requestedBytes <= writableBytes())
      {
         return;
      }

      int newCapacity;
      if (capacity() == 0)
      {
         newCapacity = initialCapacity;
         if (newCapacity == 0)
         {
            newCapacity = 1;
         }
      }
      else
      {
         newCapacity = capacity();
      }
      int minNewCapacity = writerIndex() + requestedBytes;
      while (newCapacity < minNewCapacity)
      {
         newCapacity <<= 1;
      }

      ChannelBuffer newBuffer = ChannelBuffers.buffer(newCapacity);
      newBuffer.writeBytes(buffer, 0, writerIndex());
      buffer = newBuffer;
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.buffers.AbstractChannelBuffer#array()
    */
   public byte[] array()
   {
      return buffer.array();
   }
}
