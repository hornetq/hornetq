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

import org.jboss.messaging.utils.DataConstants;
import org.jboss.messaging.utils.SimpleString;
import org.jboss.messaging.utils.UTF8Util;

/**
 * A skeletal implementation of a buffer.
 *
 * @author The Netty Project (netty-dev@lists.jboss.org)
 * @author Trustin Lee (tlee@redhat.com)
 *
 * @version $Rev: 303 $, $Date: 2008-09-24 18:48:32 +0900 (Wed, 24 Sep 2008) $
 */
public abstract class AbstractChannelBuffer implements ChannelBuffer
{

   private int readerIndex;

   private int writerIndex;

   private int markedReaderIndex;

   private int markedWriterIndex;

   public int readerIndex()
   {
      return readerIndex;
   }

   public void readerIndex(final int readerIndex)
   {
      if (readerIndex < 0 || readerIndex > writerIndex)
      {
         throw new IndexOutOfBoundsException();
      }
      this.readerIndex = readerIndex;
   }

   public int writerIndex()
   {
      return writerIndex;
   }

   public void writerIndex(final int writerIndex)
   {
      if (writerIndex < readerIndex || writerIndex > capacity())
      {
         throw new IndexOutOfBoundsException();
      }
      this.writerIndex = writerIndex;
   }

   public void setIndex(final int readerIndex, final int writerIndex)
   {
      if (readerIndex < 0 || readerIndex > writerIndex || writerIndex > capacity())
      {
         throw new IndexOutOfBoundsException();
      }
      this.readerIndex = readerIndex;
      this.writerIndex = writerIndex;
   }

   public void clear()
   {
      readerIndex = writerIndex = 0;
   }

   public boolean readable()
   {
      return readableBytes() > 0;
   }

   public boolean writable()
   {
      return writableBytes() > 0;
   }

   public int readableBytes()
   {
      return writerIndex - readerIndex;
   }

   public int writableBytes()
   {
      return capacity() - writerIndex;
   }

   public void markReaderIndex()
   {
      markedReaderIndex = readerIndex;
   }

   public void resetReaderIndex()
   {
      readerIndex(markedReaderIndex);
   }

   public void markWriterIndex()
   {
      markedWriterIndex = writerIndex;
   }

   public void resetWriterIndex()
   {
      writerIndex = markedWriterIndex;
   }

   public void discardReadBytes()
   {
      if (readerIndex == 0)
      {
         return;
      }
      setBytes(0, this, readerIndex, writerIndex - readerIndex);
      writerIndex -= readerIndex;
      markedReaderIndex = Math.max(markedReaderIndex - readerIndex, 0);
      markedWriterIndex = Math.max(markedWriterIndex - readerIndex, 0);
      readerIndex = 0;
   }

   public short getUnsignedByte(final int index)
   {
      return (short)(getByte(index) & 0xFF);
   }

   public int getUnsignedShort(final int index)
   {
      return getShort(index) & 0xFFFF;
   }

   public int getMedium(final int index)
   {
      int value = getUnsignedMedium(index);
      if ((value & 0x800000) != 0)
      {
         value |= 0xff000000;
      }
      return value;
   }

   public long getUnsignedInt(final int index)
   {
      return getInt(index) & 0xFFFFFFFFL;
   }

   public void getBytes(final int index, final byte[] dst)
   {
      getBytes(index, dst, 0, dst.length);
   }

   public void getBytes(final int index, final ChannelBuffer dst)
   {
      getBytes(index, dst, dst.writableBytes());
   }

   public void getBytes(final int index, final ChannelBuffer dst, final int length)
   {
      if (length > dst.writableBytes())
      {
         throw new IndexOutOfBoundsException();
      }
      getBytes(index, dst, dst.writerIndex(), length);
      dst.writerIndex(dst.writerIndex() + length);
   }

   public void setBytes(final int index, final byte[] src)
   {
      setBytes(index, src, 0, src.length);
   }

   public void setBytes(final int index, final ChannelBuffer src)
   {
      setBytes(index, src, src.readableBytes());
   }

   public void setBytes(final int index, final ChannelBuffer src, final int length)
   {
      if (length > src.readableBytes())
      {
         throw new IndexOutOfBoundsException();
      }
      setBytes(index, src, src.readerIndex(), length);
      src.readerIndex(src.readerIndex() + length);
   }

   public void setZero(int index, final int length)
   {
      if (length == 0)
      {
         return;
      }
      if (length < 0)
      {
         throw new IllegalArgumentException("length must be 0 or greater than 0.");
      }

      int nLong = length >>> 3;
      int nBytes = length & 7;
      for (int i = nLong; i > 0; i--)
      {
         setLong(index, 0);
         index += 8;
      }
      if (nBytes == 4)
      {
         setInt(index, 0);
      }
      else if (nBytes < 4)
      {
         for (int i = nBytes; i > 0; i--)
         {
            setByte(index, (byte)0);
            index++;
         }
      }
      else
      {
         setInt(index, 0);
         index += 4;
         for (int i = nBytes - 4; i > 0; i--)
         {
            setByte(index, (byte)0);
            index++;
         }
      }
   }

   public byte readByte()
   {
      if (readerIndex == writerIndex)
      {
         throw new IndexOutOfBoundsException();
      }
      return getByte(readerIndex++);
   }

   public short readUnsignedByte()
   {
      return (short)(readByte() & 0xFF);
   }

   public short readShort()
   {
      checkReadableBytes(2);
      short v = getShort(readerIndex);
      readerIndex += 2;
      return v;
   }

   public int readUnsignedShort()
   {
      return readShort() & 0xFFFF;
   }

   public int readMedium()
   {
      int value = readUnsignedMedium();
      if ((value & 0x800000) != 0)
      {
         value |= 0xff000000;
      }
      return value;
   }

   public int readUnsignedMedium()
   {
      checkReadableBytes(3);
      int v = getUnsignedMedium(readerIndex);
      readerIndex += 3;
      return v;
   }

   public int readInt()
   {
      checkReadableBytes(4);
      int v = getInt(readerIndex);
      readerIndex += 4;
      return v;
   }

   public long readUnsignedInt()
   {
      return readInt() & 0xFFFFFFFFL;
   }

   public long readLong()
   {
      checkReadableBytes(8);
      long v = getLong(readerIndex);
      readerIndex += 8;
      return v;
   }

   public void readBytes(final byte[] dst, final int dstIndex, final int length)
   {
      checkReadableBytes(length);
      getBytes(readerIndex, dst, dstIndex, length);
      readerIndex += length;
   }

   public void readBytes(final byte[] dst)
   {
      readBytes(dst, 0, dst.length);
   }

   public void readBytes(final ChannelBuffer dst)
   {
      readBytes(dst, dst.writableBytes());
   }

   public void readBytes(final ChannelBuffer dst, final int length)
   {
      if (length > dst.writableBytes())
      {
         throw new IndexOutOfBoundsException();
      }
      readBytes(dst, dst.writerIndex(), length);
      dst.writerIndex(dst.writerIndex() + length);
   }

   public void readBytes(final ChannelBuffer dst, final int dstIndex, final int length)
   {
      checkReadableBytes(length);
      getBytes(readerIndex, dst, dstIndex, length);
      readerIndex += length;
   }

   public void readBytes(final ByteBuffer dst)
   {
      int length = dst.remaining();
      checkReadableBytes(length);
      getBytes(readerIndex, dst);
      readerIndex += length;
   }

   public int readBytes(final GatheringByteChannel out, final int length) throws IOException
   {
      checkReadableBytes(length);
      int readBytes = getBytes(readerIndex, out, length);
      readerIndex += readBytes;
      return readBytes;
   }

   public void readBytes(final OutputStream out, final int length) throws IOException
   {
      checkReadableBytes(length);
      getBytes(readerIndex, out, length);
      readerIndex += length;
   }

   public void skipBytes(final int length)
   {
      int newReaderIndex = readerIndex + length;
      if (newReaderIndex > writerIndex)
      {
         throw new IndexOutOfBoundsException();
      }
      readerIndex = newReaderIndex;
   }

   public void writeByte(final byte value)
   {
      setByte(writerIndex++, value);
   }

   public void writeShort(final short value)
   {
      setShort(writerIndex, value);
      writerIndex += 2;
   }

   public void writeMedium(final int value)
   {
      setMedium(writerIndex, value);
      writerIndex += 3;
   }

   public void writeInt(final int value)
   {
      setInt(writerIndex, value);
      writerIndex += 4;
   }

   public void writeLong(final long value)
   {
      setLong(writerIndex, value);
      writerIndex += 8;
   }

   public void writeBytes(final byte[] src, final int srcIndex, final int length)
   {
      setBytes(writerIndex, src, srcIndex, length);
      writerIndex += length;
   }

   public void writeBytes(final byte[] src)
   {
      writeBytes(src, 0, src.length);
   }

   public void writeBytes(final ChannelBuffer src)
   {
      writeBytes(src, src.readableBytes());
   }

   public void writeBytes(final ChannelBuffer src, final int length)
   {
      if (length > src.readableBytes())
      {
         throw new IndexOutOfBoundsException();
      }
      writeBytes(src, src.readerIndex(), length);
      src.readerIndex(src.readerIndex() + length);
   }

   public void writeBytes(final ChannelBuffer src, final int srcIndex, final int length)
   {
      setBytes(writerIndex, src, srcIndex, length);
      writerIndex += length;
   }

   public void writeBytes(final ByteBuffer src)
   {
      int length = src.remaining();
      setBytes(writerIndex, src);
      writerIndex += length;
   }

   public void writeBytes(final InputStream in, final int length) throws IOException
   {
      setBytes(writerIndex, in, length);
      writerIndex += length;
   }

   public int writeBytes(final ScatteringByteChannel in, final int length) throws IOException
   {
      int writtenBytes = setBytes(writerIndex, in, length);
      if (writtenBytes > 0)
      {
         writerIndex += writtenBytes;
      }
      return writtenBytes;
   }

   public void writeZero(final int length)
   {
      if (length == 0)
      {
         return;
      }
      if (length < 0)
      {
         throw new IllegalArgumentException("length must be 0 or greater than 0.");
      }
      int nLong = length >>> 3;
      int nBytes = length & 7;
      for (int i = nLong; i > 0; i--)
      {
         writeLong(0);
      }
      if (nBytes == 4)
      {
         writeInt(0);
      }
      else if (nBytes < 4)
      {
         for (int i = nBytes; i > 0; i--)
         {
            writeByte((byte)0);
         }
      }
      else
      {
         writeInt(0);
         for (int i = nBytes - 4; i > 0; i--)
         {
            writeByte((byte)0);
         }
      }
   }

   public ByteBuffer toByteBuffer()
   {
      return toByteBuffer(readerIndex, readableBytes());
   }

   public ByteBuffer[] toByteBuffers()
   {
      return toByteBuffers(readerIndex, readableBytes());
   }

   public ByteBuffer[] toByteBuffers(final int index, final int length)
   {
      return new ByteBuffer[] { toByteBuffer(index, length) };
   }

   public String toString(final String charsetName)
   {
      return toString(readerIndex, readableBytes(), charsetName);
   }

   @Override
   public int hashCode()
   {
      return ChannelBuffers.hashCode(this);
   }

   @Override
   public boolean equals(final Object o)
   {
      if (!(o instanceof ChannelBuffer))
      {
         return false;
      }
      return ChannelBuffers.equals(this, (ChannelBuffer)o);
   }

   public int compareTo(final ChannelBuffer that)
   {
      return ChannelBuffers.compare(this, that);
   }

   @Override
   public String toString()
   {
      return getClass().getSimpleName() + '(' +
             "ridx=" +
             readerIndex +
             ", " +
             "widx=" +
             writerIndex +
             ", " +
             "cap=" +
             capacity() +
             ')';
   }

   /**
    * Throws an {@link IndexOutOfBoundsException} if the current
    * {@linkplain #readableBytes() readable bytes} of this buffer is less
    * than the specified value.
    */
   protected void checkReadableBytes(final int minimumReadableBytes)
   {
      if (readableBytes() < minimumReadableBytes)
      {
         throw new IndexOutOfBoundsException();
      }
   }

   public Object getUnderlyingBuffer()
   {
      return this;
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.spi.MessagingBuffer#readBoolean()
    */
   public boolean readBoolean()
   {
      return readByte() != 0;
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.spi.MessagingBuffer#readChar()
    */
   public char readChar()
   {
      return (char)readShort();
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.spi.MessagingBuffer#readDouble()
    */
   public double readDouble()
   {
      return Double.longBitsToDouble(readLong());
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.spi.MessagingBuffer#readFloat()
    */
   public float readFloat()
   {
      return Float.intBitsToFloat(readInt());
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.spi.MessagingBuffer#readNullableSimpleString()
    */
   public SimpleString readNullableSimpleString()
   {
      int b = readByte();
      if (b == DataConstants.NULL)
      {
         return null;
      }
      else
      {
         return readSimpleString();
      }
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.spi.MessagingBuffer#readNullableString()
    */
   public String readNullableString()
   {
      int b = readByte();
      if (b == DataConstants.NULL)
      {
         return null;
      }
      else
      {
         return readString();
      }
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.spi.MessagingBuffer#readSimpleString()
    */
   public SimpleString readSimpleString()
   {
      int len = readInt();
      byte[] data = new byte[len];
      readBytes(data);
      return new SimpleString(data);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.spi.MessagingBuffer#readString()
    */
   public String readString()
   {
      int len = readInt();
      char[] chars = new char[len];
      for (int i = 0; i < len; i++)
      {
         chars[i] = readChar();
      }
      return new String(chars);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.spi.MessagingBuffer#readUTF()
    */
   public String readUTF() throws Exception
   {
      return UTF8Util.readUTF(this);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.spi.MessagingBuffer#writeBoolean(boolean)
    */
   public void writeBoolean(final boolean val)
   {
      writeByte((byte)(val ? -1 : 0));
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.spi.MessagingBuffer#writeChar(char)
    */
   public void writeChar(final char val)
   {
      writeShort((short)val);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.spi.MessagingBuffer#writeDouble(double)
    */
   public void writeDouble(final double val)
   {
      writeLong(Double.doubleToLongBits(val));

   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.spi.MessagingBuffer#writeFloat(float)
    */
   public void writeFloat(final float val)
   {
      writeInt(Float.floatToIntBits(val));

   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.spi.MessagingBuffer#writeNullableSimpleString(org.jboss.messaging.util.SimpleString)
    */
   public void writeNullableSimpleString(final SimpleString val)
   {
      if (val == null)
      {
         writeByte(DataConstants.NULL);
      }
      else
      {
         writeByte(DataConstants.NOT_NULL);
         writeSimpleString(val);
      }
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.spi.MessagingBuffer#writeNullableString(java.lang.String)
    */
   public void writeNullableString(final String val)
   {
      if (val == null)
      {
         writeByte(DataConstants.NULL);
      }
      else
      {
         writeByte(DataConstants.NOT_NULL);
         writeString(val);
      }
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.spi.MessagingBuffer#writeSimpleString(org.jboss.messaging.util.SimpleString)
    */
   public void writeSimpleString(final SimpleString val)
   {
      byte[] data = val.getData();
      writeInt(data.length);
      writeBytes(data);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.spi.MessagingBuffer#writeString(java.lang.String)
    */
   public void writeString(final String val)
   {
      writeInt(val.length());
      for (int i = 0; i < val.length(); i++)
      {
         writeShort((short)val.charAt(i));
      }
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.spi.MessagingBuffer#writeUTF(java.lang.String)
    */
   public void writeUTF(final String utf) throws Exception
   {
      UTF8Util.saveUTF(this, utf);
   }

}
