/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.core.client.impl;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.protocol.core.impl.wireformat.SessionReceiveContinuationMessage;
import org.hornetq.utils.DataConstants;
import org.hornetq.utils.HornetQBufferInputStream;
import org.hornetq.utils.InflaterReader;
import org.hornetq.utils.InflaterWriter;
import org.hornetq.utils.UTF8Util;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * A DecompressedHornetQBuffer
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class CompressedLargeMessageControllerImpl implements LargeMessageController
{

   // Constants -----------------------------------------------------

   private static final String OPERATION_NOT_SUPPORTED = "Operation not supported";

   private static final String READ_ONLY_ERROR_MESSAGE = "This is a read-only buffer, setOperations are not supported";

   // Attributes ----------------------------------------------------

   final LargeMessageController bufferDelegate;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public CompressedLargeMessageControllerImpl(final LargeMessageController bufferDelegate)
   {
      this.bufferDelegate = bufferDelegate;
   }


   // Public --------------------------------------------------------

   /**
    * 
    */
   public void discardUnusedPackets()
   {
      bufferDelegate.discardUnusedPackets();
   }

   /**
    * Add a buff to the List, or save it to the OutputStream if set
    * @param packet
    */
   public void addPacket(final SessionReceiveContinuationMessage packet)
   {
      bufferDelegate.addPacket(packet);
   }

   public synchronized void cancel()
   {
      bufferDelegate.cancel();
   }

   public synchronized void close()
   {
      bufferDelegate.cancel();
   }

   public void setOutputStream(final OutputStream output) throws HornetQException
   {
      bufferDelegate.setOutputStream(new InflaterWriter(output));
   }

   public synchronized void saveBuffer(final OutputStream output) throws HornetQException
   {
      setOutputStream(output);
      waitCompletion(0);
   }

   /**
    * 
    * @param timeWait Milliseconds to Wait. 0 means forever
    * @throws Exception
    */
   public synchronized boolean waitCompletion(final long timeWait) throws HornetQException
   {
      return bufferDelegate.waitCompletion(timeWait);
   }

   // Channel Buffer Implementation ---------------------------------

   /* (non-Javadoc)
    * @see org.hornetq.api.core.buffers.ChannelBuffer#array()
    */
   public byte[] array()
   {
      throw new IllegalAccessError("array not supported on LargeMessageBufferImpl");
   }

   /* (non-Javadoc)
    * @see org.hornetq.api.core.buffers.ChannelBuffer#capacity()
    */
   public int capacity()
   {
      return -1;
   }
   
   DataInputStream dataInput = null;
   
   private DataInputStream getStream()
   {
      if (dataInput == null)
      {
         try
         {
            InputStream input = new HornetQBufferInputStream(bufferDelegate);
            
            dataInput = new DataInputStream(new InflaterReader(input));
         }
         catch (Exception e)
         {
            throw new RuntimeException (e.getMessage(), e);
         }
         
      }
      return dataInput;
   }
   
   private void positioningNotSupported()
   {
      throw new IllegalStateException("Position not supported over compressed large messages");
   }

   public byte readByte()
   {
      try
      {
         return getStream().readByte();
      }
      catch (Exception e)
      {
         throw new RuntimeException (e.getMessage(), e);
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.api.core.buffers.ChannelBuffer#getByte(int)
    */
   public byte getByte(final int index)
   {
      positioningNotSupported();
      return 0;
   }

   private byte getByte(final long index)
   {
      positioningNotSupported();
      return 0;
   }

   /* (non-Javadoc)
    * @see org.hornetq.api.core.buffers.ChannelBuffer#getBytes(int, org.hornetq.api.core.buffers.ChannelBuffer, int, int)
    */
   public void getBytes(final int index, final HornetQBuffer dst, final int dstIndex, final int length)
   {
      positioningNotSupported();
   }

   /* (non-Javadoc)
    * @see org.hornetq.api.core.buffers.ChannelBuffer#getBytes(int, org.hornetq.api.core.buffers.ChannelBuffer, int, int)
    */
   public void getBytes(final long index, final HornetQBuffer dst, final int dstIndex, final int length)
   {
      positioningNotSupported();
   }

   /* (non-Javadoc)
    * @see org.hornetq.api.core.buffers.ChannelBuffer#getBytes(int, byte[], int, int)
    */
   public void getBytes(final int index, final byte[] dst, final int dstIndex, final int length)
   {
      positioningNotSupported();
   }

   public void getBytes(final long index, final byte[] dst, final int dstIndex, final int length)
   {
      positioningNotSupported();
   }

   /* (non-Javadoc)
    * @see org.hornetq.api.core.buffers.ChannelBuffer#getBytes(int, java.nio.ByteBuffer)
    */
   public void getBytes(final int index, final ByteBuffer dst)
   {
      positioningNotSupported();
   }

   public void getBytes(final long index, final ByteBuffer dst)
   {
      positioningNotSupported();
   }

   /* (non-Javadoc)
    * @see org.hornetq.api.core.buffers.ChannelBuffer#getBytes(int, java.io.OutputStream, int)
    */
   public void getBytes(final int index, final OutputStream out, final int length) throws IOException
   {
      positioningNotSupported();
   }

   public void getBytes(final long index, final OutputStream out, final int length) throws IOException
   {
      positioningNotSupported();
   }

   /* (non-Javadoc)
    * @see org.hornetq.api.core.buffers.ChannelBuffer#getBytes(int, java.nio.channels.GatheringByteChannel, int)
    */
   public int getBytes(final int index, final GatheringByteChannel out, final int length) throws IOException
   {
      positioningNotSupported();
      return 0;
   }

   /* (non-Javadoc)
    * @see org.hornetq.api.core.buffers.ChannelBuffer#getInt(int)
    */
   public int getInt(final int index)
   {
      positioningNotSupported();
      return 0;
   }

   public int getInt(final long index)
   {
      positioningNotSupported();
      return 0;
   }

   /* (non-Javadoc)
    * @see org.hornetq.api.core.buffers.ChannelBuffer#getLong(int)
    */
   public long getLong(final int index)
   {
      positioningNotSupported();
      return 0;
   }

   public long getLong(final long index)
   {
      positioningNotSupported();
      return 0;
   }

   /* (non-Javadoc)
    * @see org.hornetq.api.core.buffers.ChannelBuffer#getShort(int)
    */
   public short getShort(final int index)
   {
      positioningNotSupported();
      return 0;
   }

   public short getShort(final long index)
   {
      return (short)(getByte(index) << 8 | getByte(index + 1) & 0xFF);
   }

   /* (non-Javadoc)
    * @see org.hornetq.api.core.buffers.ChannelBuffer#getUnsignedMedium(int)
    */
   public int getUnsignedMedium(final int index)
   {
      positioningNotSupported();
      return 0;
   }
   
   

   public int getUnsignedMedium(final long index)
   {
      positioningNotSupported();
      return 0;
   }

   /* (non-Javadoc)
    * @see org.hornetq.api.core.buffers.ChannelBuffer#setByte(int, byte)
    */
   public void setByte(final int index, final byte value)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   /* (non-Javadoc)
    * @see org.hornetq.api.core.buffers.ChannelBuffer#setBytes(int, org.hornetq.api.core.buffers.ChannelBuffer, int, int)
    */
   public void setBytes(final int index, final HornetQBuffer src, final int srcIndex, final int length)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   /* (non-Javadoc)
    * @see org.hornetq.api.core.buffers.ChannelBuffer#setBytes(int, byte[], int, int)
    */
   public void setBytes(final int index, final byte[] src, final int srcIndex, final int length)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   /* (non-Javadoc)
    * @see org.hornetq.api.core.buffers.ChannelBuffer#setBytes(int, java.nio.ByteBuffer)
    */
   public void setBytes(final int index, final ByteBuffer src)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   /* (non-Javadoc)
    * @see org.hornetq.api.core.buffers.ChannelBuffer#setBytes(int, java.io.InputStream, int)
    */
   public int setBytes(final int index, final InputStream in, final int length) throws IOException
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   /* (non-Javadoc)
    * @see org.hornetq.api.core.buffers.ChannelBuffer#setBytes(int, java.nio.channels.ScatteringByteChannel, int)
    */
   public int setBytes(final int index, final ScatteringByteChannel in, final int length) throws IOException
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   /* (non-Javadoc)
    * @see org.hornetq.api.core.buffers.ChannelBuffer#setInt(int, int)
    */
   public void setInt(final int index, final int value)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   /* (non-Javadoc)
    * @see org.hornetq.api.core.buffers.ChannelBuffer#setLong(int, long)
    */
   public void setLong(final int index, final long value)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   /* (non-Javadoc)
    * @see org.hornetq.api.core.buffers.ChannelBuffer#setMedium(int, int)
    */
   public void setMedium(final int index, final int value)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   /* (non-Javadoc)
    * @see org.hornetq.api.core.buffers.ChannelBuffer#setShort(int, short)
    */
   public void setShort(final int index, final short value)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   /* (non-Javadoc)
    * @see org.hornetq.api.core.buffers.ChannelBuffer#toByteBuffer(int, int)
    */
   public ByteBuffer toByteBuffer(final int index, final int length)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   /* (non-Javadoc)
    * @see org.hornetq.api.core.buffers.ChannelBuffer#toString(int, int, java.lang.String)
    */
   public String toString(final int index, final int length, final String charsetName)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public int readerIndex()
   {
      return 0;
   }

   public void readerIndex(final int readerIndex)
   {
      // TODO
   }

   public int writerIndex()
   {
      // TODO
      return 0;
   }

   public long getSize()
   {
      return this.bufferDelegate.getSize();
   }

   public void writerIndex(final int writerIndex)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void setIndex(final int readerIndex, final int writerIndex)
   {
      positioningNotSupported();
   }

   public void clear()
   {
   }

   public boolean readable()
   {
      return true;
   }

   public boolean writable()
   {
      return false;
   }

   public int readableBytes()
   {
      return 1;
   }

   public int writableBytes()
   {
      return 0;
   }

   public void markReaderIndex()
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void resetReaderIndex()
   {
      // TODO: reset positioning if possible
   }

   public void markWriterIndex()
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void resetWriterIndex()
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void discardReadBytes()
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
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

   public void getBytes(int index, final byte[] dst)
   {
      // TODO: optimize this by using System.arraycopy
      for (int i = 0; i < dst.length; i++)
      {
         dst[i] = getByte(index++);
      }
   }

   public void getBytes(long index, final byte[] dst)
   {
      // TODO: optimize this by using System.arraycopy
      for (int i = 0; i < dst.length; i++)
      {
         dst[i] = getByte(index++);
      }
   }

   public void getBytes(final int index, final HornetQBuffer dst)
   {
      getBytes(index, dst, dst.writableBytes());
   }

   public void getBytes(final int index, final HornetQBuffer dst, final int length)
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
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void setBytes(final int index, final HornetQBuffer src)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void setBytes(final int index, final HornetQBuffer src, final int length)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void setZero(final int index, final int length)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public short readUnsignedByte()
   {
      try
      {
         return (short)getStream().readUnsignedByte();
      }
      catch (Exception e)
      {
         throw new IllegalStateException (e.getMessage(), e);
      }
   }

   public short readShort()
   {
      try
      {
         return (short)getStream().readShort();
      }
      catch (Exception e)
      {
         throw new IllegalStateException (e.getMessage(), e);
      }
   }

   public int readUnsignedShort()
   {
      try
      {
         return (int)getStream().readUnsignedShort();
      }
      catch (Exception e)
      {
         throw new IllegalStateException (e.getMessage(), e);
      }
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
      return (readByte() & 0xff) << 16 | (readByte() & 0xff) << 8 | (readByte() & 0xff) << 0;
   }

   public int readInt()
   {
      try
      {
         return getStream().readInt();
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e.getMessage(), e);
      }
   }

   public int readInt(final int pos)
   {
      positioningNotSupported();
      return 0;
   }

   public long readUnsignedInt()
   {
      return readInt() & 0xFFFFFFFFL;
   }

   public long readLong()
   {
      try
      {
         return getStream().readLong();
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e.getMessage(), e);
      }
   }

   public void readBytes(final byte[] dst, final int dstIndex, final int length)
   {
      try
      {
         getStream().read(dst, dstIndex, length);
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e.getMessage(), e);
      }
   }

   public void readBytes(final byte[] dst)
   {
      readBytes(dst, 0, dst.length);
   }

   public void readBytes(final HornetQBuffer dst)
   {
      readBytes(dst, dst.writableBytes());
   }

   public void readBytes(final HornetQBuffer dst, final int length)
   {
      if (length > dst.writableBytes())
      {
         throw new IndexOutOfBoundsException();
      }
      readBytes(dst, dst.writerIndex(), length);
      dst.writerIndex(dst.writerIndex() + length);
   }

   public void readBytes(final HornetQBuffer dst, final int dstIndex, final int length)
   {
      byte[] destBytes = new byte[length];
      readBytes(destBytes);
      dst.setBytes(dstIndex, destBytes);
   }

   public void readBytes(final ByteBuffer dst)
   {
      byte bytesToGet[] = new byte[dst.remaining()];
      readBytes(bytesToGet);
      dst.put(bytesToGet);
   }

   public int readBytes(final GatheringByteChannel out, final int length) throws IOException
   {
      throw new IllegalStateException("Not implemented!");
   }

   public void readBytes(final OutputStream out, final int length) throws IOException
   {
      throw new IllegalStateException("Not implemented!");
   }

   public void skipBytes(final int length)
   {
    
      try
      {
         for (int i = 0 ; i < length; i++)
         {
            getStream().read();
         }
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e.getMessage(), e);
      }
   }

   public void writeByte(final byte value)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void writeShort(final short value)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void writeMedium(final int value)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void writeInt(final int value)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void writeLong(final long value)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void writeBytes(final byte[] src, final int srcIndex, final int length)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void writeBytes(final byte[] src)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void writeBytes(final HornetQBuffer src)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void writeBytes(final HornetQBuffer src, final int length)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void writeBytes(final ByteBuffer src)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public int writeBytes(final InputStream in, final int length) throws IOException
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public int writeBytes(final ScatteringByteChannel in, final int length) throws IOException
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void writeZero(final int length)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public ByteBuffer toByteBuffer()
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public ByteBuffer[] toByteBuffers()
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public ByteBuffer[] toByteBuffers(final int index, final int length)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public String toString(final String charsetName)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public Object getUnderlyingBuffer()
   {
      return this;
   }

   /* (non-Javadoc)
    * @see org.hornetq.spi.core.remoting.HornetQBuffer#readBoolean()
    */
   public boolean readBoolean()
   {
      return readByte() != 0;
   }

   /* (non-Javadoc)
    * @see org.hornetq.spi.core.remoting.HornetQBuffer#readChar()
    */
   public char readChar()
   {
      return (char)readShort();
   }
   
   public char getChar(final int index)
   {
      return (char)getShort(index);
   }

   public double getDouble(final int index)
   {
      return Double.longBitsToDouble(getLong(index));
   }

   public float getFloat(final int index)
   {
      return Float.intBitsToFloat(getInt(index));
   }

   public HornetQBuffer readBytes(final int length)
   {
      byte bytesToGet[] = new byte[length];
      readBytes(bytesToGet);
      return HornetQBuffers.wrappedBuffer(bytesToGet);
   }

   /* (non-Javadoc)
    * @see org.hornetq.spi.core.remoting.HornetQBuffer#readDouble()
    */
   public double readDouble()
   {
      return Double.longBitsToDouble(readLong());
   }

   /* (non-Javadoc)
    * @see org.hornetq.spi.core.remoting.HornetQBuffer#readFloat()
    */
   public float readFloat()
   {
      return Float.intBitsToFloat(readInt());
   }

   /* (non-Javadoc)
    * @see org.hornetq.spi.core.remoting.HornetQBuffer#readNullableSimpleString()
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
    * @see org.hornetq.spi.core.remoting.HornetQBuffer#readNullableString()
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
    * @see org.hornetq.spi.core.remoting.HornetQBuffer#readSimpleString()
    */
   public SimpleString readSimpleString()
   {
      int len = readInt();
      byte[] data = new byte[len];
      readBytes(data);
      return new SimpleString(data);
   }

   /* (non-Javadoc)
    * @see org.hornetq.spi.core.remoting.HornetQBuffer#readString()
    */
   public String readString()
   {
      int len = readInt();

      if (len < 9)
      {
         char[] chars = new char[len];
         for (int i = 0; i < len; i++)
         {
            chars[i] = (char)readShort();
         }
         return new String(chars);
      }
      else if (len < 0xfff)
      {
         return readUTF();
      }
      else
      {
         return readSimpleString().toString();
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.spi.core.remoting.HornetQBuffer#readUTF()
    */
   public String readUTF()
   {
      return UTF8Util.readUTF(this);
   }

   /* (non-Javadoc)
    * @see org.hornetq.spi.core.remoting.HornetQBuffer#writeBoolean(boolean)
    */
   public void writeBoolean(final boolean val)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   /* (non-Javadoc)
    * @see org.hornetq.spi.core.remoting.HornetQBuffer#writeChar(char)
    */
   public void writeChar(final char val)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   /* (non-Javadoc)
    * @see org.hornetq.spi.core.remoting.HornetQBuffer#writeDouble(double)
    */
   public void writeDouble(final double val)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);

   }

   /* (non-Javadoc)
    * @see org.hornetq.spi.core.remoting.HornetQBuffer#writeFloat(float)
    */
   public void writeFloat(final float val)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);

   }

   /* (non-Javadoc)
    * @see org.hornetq.spi.core.remoting.HornetQBuffer#writeNullableSimpleString(org.hornetq.util.SimpleString)
    */
   public void writeNullableSimpleString(final SimpleString val)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   /* (non-Javadoc)
    * @see org.hornetq.spi.core.remoting.HornetQBuffer#writeNullableString(java.lang.String)
    */
   public void writeNullableString(final String val)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   /* (non-Javadoc)
    * @see org.hornetq.spi.core.remoting.HornetQBuffer#writeSimpleString(org.hornetq.util.SimpleString)
    */
   public void writeSimpleString(final SimpleString val)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   /* (non-Javadoc)
    * @see org.hornetq.spi.core.remoting.HornetQBuffer#writeString(java.lang.String)
    */
   public void writeString(final String val)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   /* (non-Javadoc)
    * @see org.hornetq.spi.core.remoting.HornetQBuffer#writeUTF(java.lang.String)
    */
   public void writeUTF(final String utf)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   /* (non-Javadoc)
    * @see org.hornetq.api.core.buffers.ChannelBuffer#compareTo(org.hornetq.api.core.buffers.ChannelBuffer)
    */
   public int compareTo(final HornetQBuffer buffer)
   {
      return -1;
   }

   public HornetQBuffer copy()
   {
      throw new UnsupportedOperationException();
   }

   public HornetQBuffer slice(final int index, final int length)
   {
      throw new UnsupportedOperationException();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   /**
    * @param body
    */
   // Inner classes -------------------------------------------------

   public ChannelBuffer channelBuffer()
   {
      return null;
   }

   public HornetQBuffer copy(final int index, final int length)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public HornetQBuffer duplicate()
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public HornetQBuffer readSlice(final int length)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void setChar(final int index, final char value)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void setDouble(final int index, final double value)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void setFloat(final int index, final float value)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public HornetQBuffer slice()
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void writeBytes(final HornetQBuffer src, final int srcIndex, final int length)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }
}
