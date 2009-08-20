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

package org.hornetq.core.remoting.spi;

import org.hornetq.core.buffers.ChannelBuffer;
import org.hornetq.utils.SimpleString;

/**
 *
 * A MessagingBuffer
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface MessagingBuffer
{
   void writeByte(byte val);

   void writeBytes(byte[] bytes);

   void writeBytes(byte[] bytes, int offset, int length);
   
   void writeBytes(MessagingBuffer src, int srcIndex, int length);

   void writeInt(int val);

   void setInt(int pos, int val);

   void writeLong(long val);

   void writeShort(short val);

   void writeDouble(double val);

   void writeFloat(float val);

   void writeBoolean(boolean val);

   void writeChar(char val);

   void writeNullableString(String val);

   void writeString(String val);

   void writeSimpleString(SimpleString val);

   void writeNullableSimpleString(SimpleString val);

   void writeUTF(String utf) throws Exception;

   byte readByte();

   short readUnsignedByte();

   void readBytes(byte[] bytes);

   void readBytes(byte[] bytes, int offset, int length);

   int readInt();

   long readLong();

   short readShort();

   int readUnsignedShort();

   double readDouble();

   float readFloat();

   boolean readBoolean();

   char readChar();

   String readString();

   String readNullableString();

   SimpleString readSimpleString();

   SimpleString readNullableSimpleString();

   String readUTF() throws Exception;

   byte[] array();

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

   void resetReaderIndex();

   void resetWriterIndex();

   Object getUnderlyingBuffer();
}
