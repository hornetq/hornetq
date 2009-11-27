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

package org.hornetq.core.buffers;

import java.nio.ByteBuffer;

import org.hornetq.utils.SimpleString;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 *
 * A HornetQBuffer
 * 
 * Much of it derived from Netty ChannelBuffer by Trustin Lee
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface HornetQBuffer
{
   ChannelBuffer channelBuffer();

   int capacity();

   int readerIndex();

   void readerIndex(int readerIndex);

   int writerIndex();

   void writerIndex(int writerIndex);

   void setIndex(int readerIndex, int writerIndex);

   int readableBytes();

   int writableBytes();

   boolean readable();

   boolean writable();

   void clear();

   void markReaderIndex();

   void resetReaderIndex();

   void markWriterIndex();

   void resetWriterIndex();

   void discardReadBytes();

   byte getByte(int index);

   short getUnsignedByte(int index);

   short getShort(int index);

   int getUnsignedShort(int index);

   int getInt(int index);

   long getUnsignedInt(int index);

   long getLong(int index);

   void getBytes(int index, HornetQBuffer dst);

   void getBytes(int index, HornetQBuffer dst, int length);

   void getBytes(int index, HornetQBuffer dst, int dstIndex, int length);

   void getBytes(int index, byte[] dst);

   void getBytes(int index, byte[] dst, int dstIndex, int length);

   void getBytes(int index, ByteBuffer dst);
   
   char getChar(int index);
   
   float getFloat(int index);
   
   double getDouble(int index);

   void setByte(int index, byte  value);

   void setShort(int index, short value);

   void setInt(int index, int   value);

   void setLong(int index, long  value);

   void setBytes(int index, HornetQBuffer src);

   void setBytes(int index, HornetQBuffer src, int length);

   void setBytes(int index, HornetQBuffer src, int srcIndex, int length);

   void setBytes(int index, byte[] src);

   void setBytes(int index, byte[] src, int srcIndex, int length);

   void setBytes(int index, ByteBuffer src);
   
   void setChar(int index, char value);
   
   void setFloat(int index, float value);
   
   void setDouble(int index, double value);

   byte readByte();

   short readUnsignedByte();

   short readShort();

   int readUnsignedShort();

   int readInt();

   long readUnsignedInt();

   long readLong();
   
   char readChar();
   
   float readFloat();
   
   double readDouble();

   HornetQBuffer readBytes(int length);

   HornetQBuffer readSlice(int length);

   void readBytes(HornetQBuffer dst);

   void readBytes(HornetQBuffer dst, int length);

   void readBytes(HornetQBuffer dst, int dstIndex, int length);

   void readBytes(byte[] dst);

   void readBytes(byte[] dst, int dstIndex, int length);

   void readBytes(ByteBuffer dst);

   void skipBytes(int length);

   void writeByte(byte  value);

   void writeShort(short value);

   void writeInt(int value);

   void writeLong(long value);
   
   void writeChar(char chr);
   
   void writeFloat(float value);
   
   void writeDouble(double value);
   
   void writeBytes(HornetQBuffer src, int length);

   void writeBytes(HornetQBuffer src, int srcIndex, int length);

   void writeBytes(byte[] src);

   void writeBytes(byte[] src, int srcIndex, int length);

   void writeBytes(ByteBuffer src);

   HornetQBuffer copy();

   HornetQBuffer copy(int index, int length);

   HornetQBuffer slice();

   HornetQBuffer slice(int index, int length);

   HornetQBuffer duplicate();

   ByteBuffer toByteBuffer();

   ByteBuffer toByteBuffer(int index, int length);

   boolean readBoolean();

   SimpleString readNullableSimpleString();

   String readNullableString();

   SimpleString readSimpleString();

   String readString();

   String readUTF();

   void writeBoolean(boolean val);

   void writeNullableSimpleString(SimpleString val);

   void writeNullableString(String val);

   void writeSimpleString(SimpleString val);

   void writeString(String val);

   void writeUTF(String utf);
}
