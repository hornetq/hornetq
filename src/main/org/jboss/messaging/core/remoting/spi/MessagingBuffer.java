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

package org.jboss.messaging.core.remoting.spi;

import org.jboss.messaging.utils.SimpleString;

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
