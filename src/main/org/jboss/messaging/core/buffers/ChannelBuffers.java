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

import java.nio.ByteBuffer;

/**
 * Creates a new {@link ChannelBuffer} by allocating new space or by wrapping
 * or copying existing byte arrays, byte buffers and a string.
 *
 * <h3>Use static import</h3>
 * This classes is intended to be used with Java 5 static import statement:
 *
 * <pre>
 * import static org.jboss.netty.buffer.ChannelBuffers.*;
 *
 * ChannelBuffer heapBuffer = buffer(128);
 * ChannelBuffer directBuffer = directBuffer(256);
 * ChannelBuffer dynamicBuffer = dynamicBuffer(512);
 * ChannelBuffer wrappedBuffer = wrappedBuffer(new byte[128], new byte[256]);
 * ChannelBuffer copiedBuffer = copiedBuffer(ByteBuffer.allocate(128));
 * </pre>
 *
 * <h3>Allocating a new buffer</h3>
 *
 * Three buffer types are provided out of the box.
 *
 * <ul>
 * <li>{@link #buffer(int)} allocates a new fixed-capacity heap buffer.</li>
 * <li>{@link #directBuffer(int)} allocates a new fixed-capacity direct buffer.</li>
 * <li>{@link #dynamicBuffer(int)} allocates a new dynamic-capacity heap
 *     buffer, whose capacity increases automatically as needed by a write
 *     operation.</li>
 * </ul>
 *
 * <h3>Creating a wrapped buffer</h3>
 *
 * Wrapped buffer is a buffer which is a view of one or more existing
 * byte arrays and byte buffers.  Any changes in the content of the original
 * array or buffer will be reflected in the wrapped buffer.  Various wrapper
 * methods are provided and their name is all {@code wrappedBuffer()}.
 * You might want to take a look at this method closely if you want to create
 * a buffer which is composed of more than one array to reduce the number of
 * memory copy.
 *
 * <h3>Creating a copied buffer</h3>
 *
 * Copied buffer is a deep copy of one or more existing byte arrays, byte
 * buffers or a string.  Unlike a wrapped buffer, there's no shared data
 * between the original data and the copied buffer.  Various copy methods are
 * provided and their name is all {@code copiedBuffer()}.  It is also convenient
 * to use this operation to merge multiple buffers into one buffer.
 *
 * <h3>Miscellaneous utility methods</h3>
 *
 * This class also provides various utility methods to help implementation
 * of a new buffer type, generation of hex dump and swapping an integer's
 * byte order.
 *
 * @author The Netty Project (netty-dev@lists.jboss.org)
 * @author Trustin Lee (tlee@redhat.com)
 *
 * @version $Rev: 472 $, $Date: 2008-11-14 16:45:53 +0900 (Fri, 14 Nov 2008) $
 *
 * @apiviz.landmark
 * @apiviz.has org.jboss.netty.buffer.ChannelBuffer oneway - - creates
 */
public class ChannelBuffers
{

   /**
    * A buffer whose capacity is {@code 0}.
    */
   public static final HeapChannelBuffer EMPTY_BUFFER = new HeapChannelBuffer(0);

   private static final char[] HEXDUMP_TABLE = new char[65536 * 4];

   static
   {
      final char[] DIGITS = "0123456789abcdef".toCharArray();
      for (int i = 0; i < 65536; i++)
      {
         HEXDUMP_TABLE[(i << 2) + 0] = DIGITS[i >>> 12 & 0x0F];
         HEXDUMP_TABLE[(i << 2) + 1] = DIGITS[i >>> 8 & 0x0F];
         HEXDUMP_TABLE[(i << 2) + 2] = DIGITS[i >>> 4 & 0x0F];
         HEXDUMP_TABLE[(i << 2) + 3] = DIGITS[i >>> 0 & 0x0F];
      }
   }

   /**
    * Creates a new Java heap buffer with the specified {@code endianness}
    * and {@code capacity}.  The new buffer's {@code readerIndex} and
    * {@code writerIndex} are {@code 0}.
    */
   public static ChannelBuffer buffer(final int capacity)
   {
      if (capacity == 0)
      {
         return EMPTY_BUFFER;
      }
      else
      {
         return new HeapChannelBuffer(capacity);
      }
   }
   
   /**
    * Reuses the initialBuffer on the creation of the DynamicBuffer.
    * This avoids a copy, but you should only call this method if the buffer is not being modified after the call of this method.
    * 
    * @author Clebert
    */
   public static ChannelBuffer dynamicBuffer(final byte[] initialBuffer)
   {
      return new DynamicChannelBuffer(initialBuffer);
   }

   /**
    * Creates a new dynamic buffer with the specified endianness and
    * the specified estimated data length.  More accurate estimation yields
    * less unexpected reallocation overhead.  The new buffer's
    * {@code readerIndex} and {@code writerIndex} are {@code 0}.
    */
   public static ChannelBuffer dynamicBuffer(final int estimatedLength)
   {
      return new DynamicChannelBuffer(estimatedLength);
   }

   /**
    * Creates a new buffer which wraps the specified {@code array} with the
    * specified {@code endianness}.  A modification on the specified array's
    * content will be visible to the returned buffer.
    */
   public static ChannelBuffer wrappedBuffer(final byte[] array)
   {
      return new HeapChannelBuffer(array);
   }
   
   /**
    * Creates a new buffer which wraps the specified NIO buffer's current
    * slice.  A modification on the specified buffer's content and endianness
    * will be visible to the returned buffer.
    * The new buffer's {@code readerIndex}
    * and {@code writerIndex} are {@code 0} and {@code buffer.remaining}
    * respectively.
    * 
    * Note: This method differs from the Original Netty version.
    * 
    * @author Clebert
    */
   public static ChannelBuffer wrappedBuffer(final ByteBuffer buffer)
   {
      
      ChannelBuffer newbuffer = new ByteBufferBackedChannelBuffer(buffer);
      newbuffer.clear();
      return newbuffer;
   }

   /**
    * Creates a new buffer with the specified {@code endianness} whose
    * content is a copy of the specified {@code array}.  The new buffer's
    * {@code readerIndex} and {@code writerIndex} are {@code 0} and
    * {@code array.length} respectively.
    */
   public static ChannelBuffer copiedBuffer(final byte[] array)
   {
      if (array.length == 0)
      {
         return EMPTY_BUFFER;
      }
      else
      {
         return new HeapChannelBuffer(array.clone());
      }
   }

   /**
    * Creates a new buffer whose content is a copy of the specified
    * {@code buffer}'s current slice.  The new buffer's {@code readerIndex}
    * and {@code writerIndex} are {@code 0} and {@code buffer.remaining}
    * respectively.
    */
   public static ChannelBuffer copiedBuffer(final ByteBuffer buffer)
   {
      int length = buffer.remaining();
      if (length == 0)
      {
         return EMPTY_BUFFER;
      }
      byte[] copy = new byte[length];
      int position = buffer.position();
      try
      {
         buffer.get(copy);
      }
      finally
      {
         buffer.position(position);
      }
      return wrappedBuffer(copy);
   }

   /**
    * Returns a <a href="http://en.wikipedia.org/wiki/Hex_dump">hex dump</a>
    * of the specified buffer's readable bytes.
    */
   public static String hexDump(final ChannelBuffer buffer)
   {
      return hexDump(buffer, buffer.readerIndex(), buffer.readableBytes());
   }

   /**
    * Returns a <a href="http://en.wikipedia.org/wiki/Hex_dump">hex dump</a>
    * of the specified buffer's sub-region.
    */
   public static String hexDump(final ChannelBuffer buffer, final int fromIndex, final int length)
   {
      if (length < 0)
      {
         throw new IllegalArgumentException("length: " + length);
      }
      if (length == 0)
      {
         return "";
      }

      int endIndex = fromIndex + (length >>> 1 << 1);
      boolean oddLength = length % 2 != 0;
      char[] buf = new char[length << 1];

      int srcIdx = fromIndex;
      int dstIdx = 0;
      for (; srcIdx < endIndex; srcIdx += 2, dstIdx += 4)
      {
         System.arraycopy(HEXDUMP_TABLE, buffer.getUnsignedShort(srcIdx) << 2, buf, dstIdx, 4);
      }

      if (oddLength)
      {
         System.arraycopy(HEXDUMP_TABLE, (buffer.getUnsignedByte(srcIdx) << 2) + 2, buf, dstIdx, 2);
      }

      return new String(buf);
   }

   /**
    * Calculates the hash code of the specified buffer.  This method is
    * useful when implementing a new buffer type.
    */
   public static int hashCode(final ChannelBuffer buffer)
   {
      final int aLen = buffer.readableBytes();
      final int intCount = aLen >>> 2;
      final int byteCount = aLen & 3;

      int hashCode = 1;
      int arrayIndex = buffer.readerIndex();
      for (int i = intCount; i > 0; i--)
      {
         hashCode = 31 * hashCode + buffer.getInt(arrayIndex);
         arrayIndex += 4;
      }

      for (int i = byteCount; i > 0; i--)
      {
         hashCode = 31 * hashCode + buffer.getByte(arrayIndex++);
      }

      if (hashCode == 0)
      {
         hashCode = 1;
      }

      return hashCode;
   }

   /**
    * Returns {@code true} if and only if the two specified buffers are
    * identical to each other as described in {@code ChannelBuffer#equals(Object)}.
    * This method is useful when implementing a new buffer type.
    */
   public static boolean equals(final ChannelBuffer bufferA, final ChannelBuffer bufferB)
   {
      final int aLen = bufferA.readableBytes();
      if (aLen != bufferB.readableBytes())
      {
         return false;
      }

      final int longCount = aLen >>> 3;
      final int byteCount = aLen & 7;

      int aIndex = bufferA.readerIndex();
      int bIndex = bufferB.readerIndex();

      for (int i = longCount; i > 0; i--)
      {
         if (bufferA.getLong(aIndex) != bufferB.getLong(bIndex))
         {
            return false;
         }
         aIndex += 8;
         bIndex += 8;
      }

      for (int i = byteCount; i > 0; i--)
      {
         if (bufferA.getByte(aIndex) != bufferB.getByte(bIndex))
         {
            return false;
         }
         aIndex++;
         bIndex++;
      }

      return true;
   }

   /**
    * Compares the two specified buffers as described in {@link ChannelBuffer#compareTo(ChannelBuffer)}.
    * This method is useful when implementing a new buffer type.
    */
   public static int compare(final ChannelBuffer bufferA, final ChannelBuffer bufferB)
   {
      final int aLen = bufferA.readableBytes();
      final int bLen = bufferB.readableBytes();
      final int minLength = Math.min(aLen, bLen);
      final int uintCount = minLength >>> 2;
      final int byteCount = minLength & 3;

      int aIndex = bufferA.readerIndex();
      int bIndex = bufferB.readerIndex();

      for (int i = uintCount; i > 0; i--)
      {
         long va = bufferA.getUnsignedInt(aIndex);
         long vb = bufferB.getUnsignedInt(bIndex);
         if (va > vb)
         {
            return 1;
         }
         else if (va < vb)
         {
            return -1;
         }
         aIndex += 4;
         bIndex += 4;
      }

      for (int i = byteCount; i > 0; i--)
      {
         byte va = bufferA.getByte(aIndex);
         byte vb = bufferB.getByte(bIndex);
         if (va > vb)
         {
            return 1;
         }
         else if (va < vb)
         {
            return -1;
         }
         aIndex++;
         bIndex++;
      }

      return aLen - bLen;
   }

   /**
    * The default implementation of {@link ChannelBuffer#indexOf(int, int, byte)}.
    * This method is useful when implementing a new buffer type.
    */
   public static int indexOf(final ChannelBuffer buffer, final int fromIndex, final int toIndex, final byte value)
   {
      if (fromIndex <= toIndex)
      {
         return firstIndexOf(buffer, fromIndex, toIndex, value);
      }
      else
      {
         return lastIndexOf(buffer, fromIndex, toIndex, value);
      }
   }

   /**
    * Toggles the endianness of the specified 16-bit short integer.
    */
   public static short swapShort(final short value)
   {
      return (short)(value << 8 | value >>> 8 & 0xff);
   }

   /**
    * Toggles the endianness of the specified 24-bit medium integer.
    */
   public static int swapMedium(final int value)
   {
      return value << 16 & 0xff0000 | value & 0xff00 | value >>> 16 & 0xff;
   }

   /**
    * Toggles the endianness of the specified 32-bit integer.
    */
   public static int swapInt(final int value)
   {
      return swapShort((short)value) << 16 | swapShort((short)(value >>> 16)) & 0xffff;
   }

   /**
    * Toggles the endianness of the specified 64-bit long integer.
    */
   public static long swapLong(final long value)
   {
      return (long)swapInt((int)value) << 32 | swapInt((int)(value >>> 32)) & 0xffffffffL;
   }

   private static int firstIndexOf(final ChannelBuffer buffer, int fromIndex, final int toIndex, final byte value)
   {
      fromIndex = Math.max(fromIndex, 0);
      if (fromIndex >= toIndex || buffer.capacity() == 0)
      {
         return -1;
      }

      for (int i = fromIndex; i < toIndex; i++)
      {
         if (buffer.getByte(i) == value)
         {
            return i;
         }
      }

      return -1;
   }

   private static int lastIndexOf(final ChannelBuffer buffer, int fromIndex, final int toIndex, final byte value)
   {
      fromIndex = Math.min(fromIndex, buffer.capacity());
      if (fromIndex < 0 || buffer.capacity() == 0)
      {
         return -1;
      }

      for (int i = fromIndex - 1; i >= toIndex; i--)
      {
         if (buffer.getByte(i) == value)
         {
            return i;
         }
      }

      return -1;
   }

   private ChannelBuffers()
   {
      // Unused
   }
}
