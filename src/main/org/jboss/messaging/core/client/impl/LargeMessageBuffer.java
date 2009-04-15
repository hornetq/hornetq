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

package org.jboss.messaging.core.client.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.jboss.messaging.core.buffers.ChannelBuffer;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionReceiveContinuationMessage;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.utils.DataConstants;
import org.jboss.messaging.utils.SimpleString;
import org.jboss.messaging.utils.UTF8Util;

/**
 * This class aggregates several SessionReceiveContinuationMessages as it was being handled by a single buffer.
 * This buffer can be consumed as messages are arriving, and it will hold the packets until they are read using the ChannelBuffer interface, or the setOutputStream or saveStream are called.
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class LargeMessageBuffer implements ChannelBuffer
{
   // Constants -----------------------------------------------------

   private final String READ_ONLY_ERROR_MESSAGE = "This is a read-only buffer, setOperations are not supported";

   // Attributes ----------------------------------------------------

   private final ClientConsumerInternal consumerInternal;

   private final LinkedBlockingQueue<SessionReceiveContinuationMessage> packets = new LinkedBlockingQueue<SessionReceiveContinuationMessage>();

   private SessionReceiveContinuationMessage currentPacket = null;

   private final int totalSize;

   private boolean streamEnded = false;

   private final int readTimeout;

   private int readerIndex = 0;

   private int packetPosition = -1;

   private int lastIndex = 0;

   private int packetLastPosition = -1;

   private OutputStream outStream;

   private Exception handledException;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public LargeMessageBuffer(final ClientConsumerInternal consumerInternal, final int totalSize, final int readTimeout)
   {
      this.consumerInternal = consumerInternal;
      this.readTimeout = readTimeout;
      this.totalSize = totalSize;
   }

   // Public --------------------------------------------------------

   public synchronized Exception getHandledException()
   {
      return handledException;
   }

   /**
    * 
    */
   public void discardUnusedPackets()
   {
      if (outStream == null)
      {
         try
         {
            checkForPacket(this.totalSize - 1);
         }
         catch (Exception ignored)
         {
         }
      }
   }

   /**
    * Add a buff to the List, or save it to the OutputStream if set
    * @param packet
    */
   public synchronized void addPacket(final SessionReceiveContinuationMessage packet)
   {
      if (outStream != null)
      {
         try
         {
            if (!packet.isContinues())
            {
               streamEnded = true;
            }

            outStream.write(packet.getBody());

            consumerInternal.flowControl(packet.getPacketSize(), true);

            if (streamEnded)
            {
               outStream.close();
            }

            notifyAll();
         }
         catch (Exception e)
         {
            handledException = e;

         }
      }
      else
      {
         packets.offer(packet);
      }
   }

   public synchronized void close()
   {
      this.packets.offer(new SessionReceiveContinuationMessage());
      this.streamEnded = true;
      notifyAll();
   }

   public synchronized void setOutputStream(final OutputStream output) throws MessagingException
   {
      while (true)
      {
         SessionReceiveContinuationMessage packet = this.packets.poll();
         if (packet == null)
         {
            break;
         }
         try
         {
            output.write(packet.getBody());
         }
         catch (IOException e)
         {
            throw new MessagingException(MessagingException.LARGE_MESSAGE_ERROR_BODY,
                                         "Error writing body of message",
                                         e);
         }
      }

      this.outStream = output;
   }

   public synchronized void saveBuffer(final OutputStream output) throws MessagingException
   {
      setOutputStream(output);
      waitCompletion(0);
   }

   /**
    * 
    * @param timeWait Milliseconds to Wait. 0 means forever
    * @throws Exception
    */
   public synchronized boolean waitCompletion(long timeWait) throws MessagingException
   {

      if (outStream == null)
      {
         // There is no stream.. it will never achieve the end of streaming
         return false;
      }

      long timeOut = System.currentTimeMillis() + timeWait;
      while (!streamEnded && handledException == null)
      {
         try
         {
            this.wait(readTimeout == 0 ? 1 : (readTimeout * 1000));
         }
         catch (InterruptedException e)
         {
            throw new MessagingException(MessagingException.INTERNAL_ERROR, e.getMessage(), e);
         }

         if (timeWait > 0 && System.currentTimeMillis() > timeOut)
         {
            throw new MessagingException(MessagingException.LARGE_MESSAGE_ERROR_BODY,
                                         "Timeout waiting for LargeMessage Body");
         }
      }

      if (this.handledException != null)
      {
         throw new MessagingException(MessagingException.LARGE_MESSAGE_ERROR_BODY,
                                      "Error on saving LargeMessageBuffer",
                                      this.handledException);
      }

      return this.streamEnded;

   }

   // Channel Buffer Implementation ---------------------------------

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.buffers.ChannelBuffer#array()
    */
   public byte[] array()
   {
      throw new IllegalAccessError("array not supported on LargeMessageBuffer");
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.buffers.ChannelBuffer#capacity()
    */
   public int capacity()
   {
      return -1;
   }

   public byte readByte()
   {
      return getByte(readerIndex++);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.buffers.ChannelBuffer#getByte(int)
    */
   public byte getByte(final int index)
   {
      checkForPacket(index);
      return currentPacket.getBody()[index - packetPosition];
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.buffers.ChannelBuffer#getBytes(int, org.jboss.messaging.core.buffers.ChannelBuffer, int, int)
    */
   public void getBytes(final int index, final ChannelBuffer dst, final int dstIndex, final int length)
   {
      byte[] destBytes = new byte[length];
      getBytes(index, destBytes);
      dst.setBytes(dstIndex, destBytes);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.buffers.ChannelBuffer#getBytes(int, byte[], int, int)
    */
   public void getBytes(final int index, final byte[] dst, final int dstIndex, final int length)
   {
      byte bytesToGet[] = new byte[length];

      getBytes(index, bytesToGet);

      System.arraycopy(bytesToGet, 0, dst, dstIndex, length);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.buffers.ChannelBuffer#getBytes(int, java.nio.ByteBuffer)
    */
   public void getBytes(final int index, final ByteBuffer dst)
   {
      byte bytesToGet[] = new byte[dst.remaining()];
      getBytes(index, bytesToGet);
      dst.put(bytesToGet);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.buffers.ChannelBuffer#getBytes(int, java.io.OutputStream, int)
    */
   public void getBytes(final int index, final OutputStream out, final int length) throws IOException
   {
      byte bytesToGet[] = new byte[length];
      getBytes(index, bytesToGet);
      out.write(bytesToGet);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.buffers.ChannelBuffer#getBytes(int, java.nio.channels.GatheringByteChannel, int)
    */
   public int getBytes(final int index, final GatheringByteChannel out, final int length) throws IOException
   {
      byte bytesToGet[] = new byte[length];
      getBytes(index, bytesToGet);
      return out.write(ByteBuffer.wrap(bytesToGet));
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.buffers.ChannelBuffer#getInt(int)
    */
   public int getInt(final int index)
   {
      return (getByte(index) & 0xff) << 24 | (getByte(index + 1) & 0xff) << 16 |
             (getByte(index + 2) & 0xff) << 8 |
             (getByte(index + 3) & 0xff) << 0;
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.buffers.ChannelBuffer#getLong(int)
    */
   public long getLong(final int index)
   {
      return ((long)getByte(index) & 0xff) << 56 | ((long)getByte(index + 1) & 0xff) << 48 |
             ((long)getByte(index + 2) & 0xff) << 40 |
             ((long)getByte(index + 3) & 0xff) << 32 |
             ((long)getByte(index + 4) & 0xff) << 24 |
             ((long)getByte(index + 5) & 0xff) << 16 |
             ((long)getByte(index + 6) & 0xff) << 8 |
             ((long)getByte(index + 7) & 0xff) << 0;
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.buffers.ChannelBuffer#getShort(int)
    */
   public short getShort(final int index)
   {
      return (short)(getByte(index) << 8 | getByte(index + 1) & 0xFF);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.buffers.ChannelBuffer#getUnsignedMedium(int)
    */
   public int getUnsignedMedium(final int index)
   {
      return (getByte(index) & 0xff) << 16 | (getByte(index + 1) & 0xff) << 8 | (getByte(index + 2) & 0xff) << 0;
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.buffers.ChannelBuffer#setByte(int, byte)
    */
   public void setByte(final int index, final byte value)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.buffers.ChannelBuffer#setBytes(int, org.jboss.messaging.core.buffers.ChannelBuffer, int, int)
    */
   public void setBytes(final int index, final ChannelBuffer src, final int srcIndex, final int length)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.buffers.ChannelBuffer#setBytes(int, byte[], int, int)
    */
   public void setBytes(final int index, final byte[] src, final int srcIndex, final int length)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.buffers.ChannelBuffer#setBytes(int, java.nio.ByteBuffer)
    */
   public void setBytes(final int index, final ByteBuffer src)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.buffers.ChannelBuffer#setBytes(int, java.io.InputStream, int)
    */
   public int setBytes(final int index, final InputStream in, final int length) throws IOException
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.buffers.ChannelBuffer#setBytes(int, java.nio.channels.ScatteringByteChannel, int)
    */
   public int setBytes(final int index, final ScatteringByteChannel in, final int length) throws IOException
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.buffers.ChannelBuffer#setInt(int, int)
    */
   public void setInt(final int index, final int value)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.buffers.ChannelBuffer#setLong(int, long)
    */
   public void setLong(final int index, final long value)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.buffers.ChannelBuffer#setMedium(int, int)
    */
   public void setMedium(final int index, final int value)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.buffers.ChannelBuffer#setShort(int, short)
    */
   public void setShort(final int index, final short value)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.buffers.ChannelBuffer#toByteBuffer(int, int)
    */
   public ByteBuffer toByteBuffer(final int index, final int length)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.buffers.ChannelBuffer#toString(int, int, java.lang.String)
    */
   public String toString(final int index, final int length, final String charsetName)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   public int readerIndex()
   {
      return readerIndex;
   }

   public void readerIndex(final int readerIndex)
   {
      checkForPacket(readerIndex);
      this.readerIndex = readerIndex;
   }

   public int writerIndex()
   {
      return totalSize;
   }

   public void writerIndex(final int writerIndex)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   public void setIndex(final int readerIndex, final int writerIndex)
   {
      checkForPacket(readerIndex);
      this.readerIndex = readerIndex;
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
      return this.totalSize - this.readerIndex;
   }

   public int writableBytes()
   {
      return 0;
   }

   public void markReaderIndex()
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   public void resetReaderIndex()
   {
      checkForPacket(0);
   }

   public void markWriterIndex()
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   public void resetWriterIndex()
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   public void discardReadBytes()
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
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
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   public void setBytes(final int index, final ChannelBuffer src)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   public void setBytes(final int index, final ChannelBuffer src, final int length)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   public void setZero(final int index, final int length)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   public short readUnsignedByte()
   {
      return (short)(readByte() & 0xFF);
   }

   public short readShort()
   {
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
      int v = getUnsignedMedium(readerIndex);
      readerIndex += 3;
      return v;
   }

   public int readInt()
   {
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
      long v = getLong(readerIndex);
      readerIndex += 8;
      return v;
   }

   public void readBytes(final byte[] dst, final int dstIndex, final int length)
   {
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
      getBytes(readerIndex, dst, dstIndex, length);
      readerIndex += length;
   }

   public void readBytes(final ByteBuffer dst)
   {
      int length = dst.remaining();
      getBytes(readerIndex, dst);
      readerIndex += length;
   }

   public int readBytes(final GatheringByteChannel out, final int length) throws IOException
   {
      int readBytes = getBytes(readerIndex, out, length);
      readerIndex += readBytes;
      return readBytes;
   }

   public void readBytes(final OutputStream out, final int length) throws IOException
   {
      getBytes(readerIndex, out, length);
      readerIndex += length;
   }

   public void skipBytes(final int length)
   {

      int newReaderIndex = readerIndex + length;
      checkForPacket(newReaderIndex);
      readerIndex = newReaderIndex;
   }

   public void writeByte(final byte value)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   public void writeShort(final short value)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   public void writeMedium(final int value)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   public void writeInt(final int value)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   public void writeLong(final long value)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   public void writeBytes(final byte[] src, final int srcIndex, final int length)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   public void writeBytes(final byte[] src)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   public void writeBytes(final ChannelBuffer src)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   public void writeBytes(final ChannelBuffer src, final int length)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   public void writeBytes(MessagingBuffer src, int srcIndex, int length)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   public void writeBytes(final ChannelBuffer src, final int srcIndex, final int length)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   public void writeBytes(final ByteBuffer src)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   public void writeBytes(final InputStream in, final int length) throws IOException
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   public int writeBytes(final ScatteringByteChannel in, final int length) throws IOException
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   public void writeZero(final int length)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   public ByteBuffer toByteBuffer()
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   public ByteBuffer[] toByteBuffers()
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   public ByteBuffer[] toByteBuffers(final int index, final int length)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   public String toString(final String charsetName)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
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
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.spi.MessagingBuffer#writeChar(char)
    */
   public void writeChar(final char val)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.spi.MessagingBuffer#writeDouble(double)
    */
   public void writeDouble(final double val)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);

   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.spi.MessagingBuffer#writeFloat(float)
    */
   public void writeFloat(final float val)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);

   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.spi.MessagingBuffer#writeNullableSimpleString(org.jboss.messaging.util.SimpleString)
    */
   public void writeNullableSimpleString(final SimpleString val)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.spi.MessagingBuffer#writeNullableString(java.lang.String)
    */
   public void writeNullableString(final String val)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.spi.MessagingBuffer#writeSimpleString(org.jboss.messaging.util.SimpleString)
    */
   public void writeSimpleString(final SimpleString val)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.spi.MessagingBuffer#writeString(java.lang.String)
    */
   public void writeString(final String val)
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.spi.MessagingBuffer#writeUTF(java.lang.String)
    */
   public void writeUTF(final String utf) throws Exception
   {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.buffers.ChannelBuffer#compareTo(org.jboss.messaging.core.buffers.ChannelBuffer)
    */
   public int compareTo(final ChannelBuffer buffer)
   {
      return -1;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void popPacket()
   {
      try
      {

         if (streamEnded)
         {
            // no more packets, we are over the last one already
            throw new IndexOutOfBoundsException();
         }

         int sizeToAdd = currentPacket != null ? currentPacket.getBody().length : 1;
         currentPacket = packets.poll(readTimeout, TimeUnit.SECONDS);
         if (currentPacket == null)
         {
            throw new IndexOutOfBoundsException();
         }

         if (currentPacket.getBody() == null) // Empty packet as a signal to interruption
         {
            currentPacket = null;
            streamEnded = true;
            throw new IndexOutOfBoundsException();
         }

         consumerInternal.flowControl(currentPacket.getPacketSize(), true);

         packetPosition += sizeToAdd;

         packetLastPosition = packetPosition + currentPacket.getBody().length;
      }
      catch (IndexOutOfBoundsException e)
      {
         throw e;
      }
      catch (Exception e)
      {
         throw new RuntimeException(e);
      }
   }

   private void checkForPacket(final int index)
   {
      if (this.outStream != null)
      {
         throw new IllegalAccessError("Can't read the messageBody after setting outputStream");
      }
      if (index >= totalSize)
      {
         throw new IndexOutOfBoundsException();
      }
      if (index < lastIndex)
      {
         throw new IllegalAccessError("LargeMessage have read-only and one-way buffers");
      }
      lastIndex = index;

      while (index >= packetLastPosition && !streamEnded)
      {
         popPacket();
      }
   }

   // Inner classes -------------------------------------------------

}
