/*
 * Copyright 2009 Red Hat, Inc.
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

package org.hornetq.core.buffers.impl;

import java.nio.ByteBuffer;

import org.hornetq.core.buffers.HornetQBuffer;
import org.hornetq.core.logging.Logger;
import org.hornetq.utils.DataConstants;
import org.hornetq.utils.SimpleString;
import org.hornetq.utils.UTF8Util;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * 
 * A ChannelBufferWrapper
 *
 * @author Tim Fox
 *
 *
 */
public class ChannelBufferWrapper implements HornetQBuffer
{
   private static final Logger log = Logger.getLogger(ChannelBufferWrapper.class);

   protected ChannelBuffer buffer;

   public ChannelBufferWrapper(final ChannelBuffer buffer)
   { 
      this.buffer = buffer;
   }
   
   public boolean readBoolean()
   {
      return readByte() != 0;
   }

   public SimpleString readNullableSimpleString()
   {
      int b = buffer.readByte();
      if (b == DataConstants.NULL)
      {
         return null;
      }
      else
      {
         return readSimpleStringInternal();
      }
   }

   public String readNullableString()
   {
      int b = buffer.readByte();
      if (b == DataConstants.NULL)
      {
         return null;
      }
      else
      {
         return readStringInternal();
      }
   }

   public SimpleString readSimpleString()
   {
      return readSimpleStringInternal();
   }
   
   private SimpleString readSimpleStringInternal()
   {
      int len = buffer.readInt();
      byte[] data = new byte[len];
      buffer.readBytes(data);
      return new SimpleString(data);
   }

   public String readString()
   {
      return readStringInternal();
   }
   
   private String readStringInternal()
   {
      int len = buffer.readInt();
      
      if (len < 9)
      {
         char[] chars = new char[len];
         for (int i = 0; i < len; i++)
         {
            chars[i] = (char)buffer.readShort();
         }
         return new String(chars);
      }
      else if (len < 0xfff)
      {
         return readUTF();
      }
      else
      {
         return readSimpleStringInternal().toString();
      }
   }

   public String readUTF()
   {
      return UTF8Util.readUTF(this);
   }

   public void writeBoolean(final boolean val)
   {
      buffer.writeByte((byte)(val ? -1 : 0));
   }

   public void writeNullableSimpleString(final SimpleString val)
   {
      if (val == null)
      {
         buffer.writeByte(DataConstants.NULL);
      }
      else
      {
         buffer.writeByte(DataConstants.NOT_NULL);
         writeSimpleStringInternal(val);
      }
   }

   public void writeNullableString(final String val)
   {
      if (val == null)
      {
         buffer.writeByte(DataConstants.NULL);
      }
      else
      {
         buffer.writeByte(DataConstants.NOT_NULL);
         writeStringInternal(val);
      }
   }

   public void writeSimpleString(final SimpleString val)
   {
      writeSimpleStringInternal(val);
   }
   
   private void writeSimpleStringInternal(final SimpleString val)
   {
      byte[] data = val.getData();
      buffer.writeInt(data.length);
      buffer.writeBytes(data);
   }

   public void writeString(final String val)
   {
      writeStringInternal(val);
   }
   
   private void writeStringInternal(final String val)
   {
      int length = val.length();
      
      buffer.writeInt(length);
       
      if (length < 9)
      {
         //If very small it's more performant to store char by char
         for (int i = 0; i < val.length(); i++)
         {
            buffer.writeShort((short)val.charAt(i));
         }
      }
      else if (length < 0xfff)
      {
         //Store as UTF - this is quicker than char by char for most strings
         writeUTF(val);
      }
      else
      {
         //Store as SimpleString, since can't store utf > 0xffff in length
         writeSimpleStringInternal(new SimpleString(val));
      }
   }

   public void writeUTF(final String utf)
   {
      UTF8Util.saveUTF(this, utf);
   }


   public int capacity()
   {
      return buffer.capacity();
   }

   public ChannelBuffer channelBuffer()
   {
      return buffer;
   }

   public void clear()
   {
      buffer.clear();
   }

   public HornetQBuffer copy()
   {
      return new ChannelBufferWrapper(buffer.copy());
   }

   public HornetQBuffer copy(int index, int length)
   {
      return new ChannelBufferWrapper(buffer.copy(index, length));
   }

   public void discardReadBytes()
   {
      buffer.discardReadBytes();
   }

   public HornetQBuffer duplicate()
   {
      return new ChannelBufferWrapper(buffer.duplicate());
   }

   public byte getByte(int index)
   {
      return buffer.getByte(index);
   }

   public void getBytes(int index, byte[] dst, int dstIndex, int length)
   {
      buffer.getBytes(index, dst, dstIndex, length);
   }

   public void getBytes(int index, byte[] dst)
   {
      buffer.getBytes(index, dst);
   }

   public void getBytes(int index, ByteBuffer dst)
   {
      buffer.getBytes(index, dst);
   }

   public void getBytes(int index, HornetQBuffer dst, int dstIndex, int length)
   {
      buffer.getBytes(index, dst.channelBuffer(), dstIndex, length);
   }

   public void getBytes(int index, HornetQBuffer dst, int length)
   {
      buffer.getBytes(index, dst.channelBuffer(), length);
   }

   public void getBytes(int index, HornetQBuffer dst)
   {
      buffer.getBytes(index, dst.channelBuffer());
   }

   public char getChar(int index)
   {
      return (char)buffer.getShort(index);
   }

   public double getDouble(int index)
   {
      return Double.longBitsToDouble(buffer.getLong(index));
   }

   public float getFloat(int index)
   {
      return Float.intBitsToFloat(buffer.getInt(index));
   }

   public int getInt(int index)
   {
      return buffer.getInt(index);
   }

   public long getLong(int index)
   {
      return buffer.getLong(index);
   }

   public short getShort(int index)
   {
      return buffer.getShort(index);
   }

   public short getUnsignedByte(int index)
   {
      return buffer.getUnsignedByte(index);
   }

   public long getUnsignedInt(int index)
   {
      return buffer.getUnsignedInt(index);
   }

   public int getUnsignedShort(int index)
   {
      return buffer.getUnsignedShort(index);
   }

   public void markReaderIndex()
   {
      buffer.markReaderIndex();
   }

   public void markWriterIndex()
   {
      buffer.markWriterIndex();
   }

   public boolean readable()
   {
      return buffer.readable();
   }

   public int readableBytes()
   {
      return buffer.readableBytes();
   }

   public byte readByte()
   {
      return buffer.readByte();
   }

   public void readBytes(byte[] dst, int dstIndex, int length)
   {
      buffer.readBytes(dst, dstIndex, length);
   }

   public void readBytes(byte[] dst)
   {
      buffer.readBytes(dst);
   }

   public void readBytes(ByteBuffer dst)
   {
      buffer.readBytes(dst);
   }

   public void readBytes(HornetQBuffer dst, int dstIndex, int length)
   {
      buffer.readBytes(dst.channelBuffer(), dstIndex, length);
   }

   public void readBytes(HornetQBuffer dst, int length)
   {
      buffer.readBytes(dst.channelBuffer(), length);
   }

   public void readBytes(HornetQBuffer dst)
   {
      buffer.readBytes(dst.channelBuffer());
   }

   public HornetQBuffer readBytes(int length)
   {
      return new ChannelBufferWrapper(buffer.readBytes(length));
   }

   public char readChar()
   {
      return (char)buffer.readShort();
   }

   public double readDouble()
   {
      return Double.longBitsToDouble(buffer.readLong());
   }

   public int readerIndex()
   {
      return buffer.readerIndex();
   }

   public void readerIndex(int readerIndex)
   {
      buffer.readerIndex(readerIndex);
   }

   public float readFloat()
   {
      return Float.intBitsToFloat(buffer.readInt());
   }

   public int readInt()
   {
      return buffer.readInt();
   }

   public long readLong()
   {
      return buffer.readLong();
   }

   public short readShort()
   {
      return buffer.readShort();
   }

   public HornetQBuffer readSlice(int length)
   {
      return new ChannelBufferWrapper(buffer.readSlice(length));
   }

   public short readUnsignedByte()
   {
      return buffer.readUnsignedByte();
   }

   public long readUnsignedInt()
   {
      return buffer.readUnsignedInt();
   }

   public int readUnsignedShort()
   {
      return buffer.readUnsignedShort();
   }

   public void resetReaderIndex()
   {
      buffer.resetReaderIndex();
   }

   public void resetWriterIndex()
   {
      buffer.resetWriterIndex();
   }

   public void setByte(int index, byte value)
   {
      buffer.setByte(index, value);
   }

   public void setBytes(int index, byte[] src, int srcIndex, int length)
   {
      buffer.setBytes(index, src, srcIndex, length);
   }

   public void setBytes(int index, byte[] src)
   {
      buffer.setBytes(index, src);
   }

   public void setBytes(int index, ByteBuffer src)
   {
      buffer.setBytes(index, src);
   }

   public void setBytes(int index, HornetQBuffer src, int srcIndex, int length)
   {
      buffer.setBytes(index, src.channelBuffer(), srcIndex, length);
   }

   public void setBytes(int index, HornetQBuffer src, int length)
   {
      buffer.setBytes(index, src.channelBuffer(), length);
   }

   public void setBytes(int index, HornetQBuffer src)
   {
      buffer.setBytes(index, src.channelBuffer());
   }

   public void setChar(int index, char value)
   {
      buffer.setShort(index, (short)value);
   }

   public void setDouble(int index, double value)
   {
      buffer.setLong(index, Double.doubleToLongBits(value));
   }

   public void setFloat(int index, float value)
   {
      buffer.setInt(index, Float.floatToIntBits(value));
   }

   public void setIndex(int readerIndex, int writerIndex)
   {
      buffer.setIndex(readerIndex, writerIndex);
   }

   public void setInt(int index, int value)
   {
      buffer.setInt(index, value);
   }

   public void setLong(int index, long value)
   {
      buffer.setLong(index, value);
   }

   public void setShort(int index, short value)
   {
      buffer.setShort(index, value);
   }

   public void skipBytes(int length)
   {
      buffer.skipBytes(length);
   }

   public HornetQBuffer slice()
   {
      return new ChannelBufferWrapper(buffer.slice());
   }

   public HornetQBuffer slice(int index, int length)
   {
      return new ChannelBufferWrapper(buffer.slice(index, length));
   }

   public ByteBuffer toByteBuffer()
   {
      return buffer.toByteBuffer();
   }

   public ByteBuffer toByteBuffer(int index, int length)
   {
      return buffer.toByteBuffer(index, length);
   }

   public boolean writable()
   {
      return buffer.writable();
   }

   public int writableBytes()
   {
      return buffer.writableBytes();
   }

   public void writeByte(byte value)
   {
      buffer.writeByte(value);
   }

   public void writeBytes(byte[] src, int srcIndex, int length)
   {
      buffer.writeBytes(src, srcIndex, length);
   }

   public void writeBytes(byte[] src)
   {
      buffer.writeBytes(src);
   }

   public void writeBytes(ByteBuffer src)
   {
      buffer.writeBytes(src);
   }

   public void writeBytes(HornetQBuffer src, int srcIndex, int length)
   {
      buffer.writeBytes(src.channelBuffer(), srcIndex, length);
   }

   public void writeBytes(HornetQBuffer src, int length)
   {
      buffer.writeBytes(src.channelBuffer(), length);
   }

   public void writeChar(char chr)
   {
      buffer.writeShort((short)chr);
   }

   public void writeDouble(double value)
   {
      buffer.writeLong(Double.doubleToLongBits(value));
   }

   public void writeFloat(float value)
   {
      buffer.writeInt(Float.floatToIntBits(value));
   }

   public void writeInt(int value)
   {
      buffer.writeInt(value);
   }

   public void writeLong(long value)
   {
      buffer.writeLong(value);
   }

   public int writerIndex()
   {
      return buffer.writerIndex();
   }

   public void writerIndex(int writerIndex)
   {
      buffer.writerIndex(writerIndex);
   }

   public void writeShort(short value)
   {
      buffer.writeShort(value);
   }

}
