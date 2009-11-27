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
import org.hornetq.core.message.Message;
import org.hornetq.utils.SimpleString;

/**
 * A ResetLimitWrappedHornetQBuffer
 *
 * @author Tim Fox
 *
 */
public class ResetLimitWrappedHornetQBuffer extends ChannelBufferWrapper
{      
   private static final Logger log = Logger.getLogger(ResetLimitWrappedHornetQBuffer.class);

   private final int limit;
   
   private Message message;
   
   public ResetLimitWrappedHornetQBuffer(final int limit, final HornetQBuffer buffer,
                                         final Message message)
   {
      super(buffer.channelBuffer());
      
      this.limit = limit;
      
      if (writerIndex() < limit)
      {
         writerIndex(limit);
      }
      
      readerIndex(limit);
      
      this.message = message;
   }
      
   private void changed()
   {
      message.bodyChanged();
   }
   
   public void setBuffer(HornetQBuffer buffer)
   {      
      this.buffer = buffer.channelBuffer();
   }
   
   public void clear()
   {
      changed();
      
      buffer.clear();
      
      buffer.setIndex(limit, limit);

   }

   public void readerIndex(int readerIndex)
   {
      if (readerIndex < limit)
      {
         readerIndex = limit;
      }
      
      buffer.readerIndex(readerIndex);
   }

   public void resetReaderIndex()
   {
      buffer.readerIndex(limit);
   }

   public void resetWriterIndex()
   {
      changed();
      
      buffer.writerIndex(limit);
   }

   public void setIndex(int readerIndex, int writerIndex)
   {
      changed();
      
      if (readerIndex < limit)
      {
         readerIndex = limit;
      }
      if (writerIndex < limit)
      {
         writerIndex = limit;
      }
      buffer.setIndex(readerIndex, writerIndex);
   }

   public void writerIndex(int writerIndex)
   {
      changed();
      
      if (writerIndex < limit)
      {
         writerIndex = limit;
      }
      
      buffer.writerIndex(writerIndex);
   }

   @Override
   public void setByte(int index, byte value)
   {
      changed();
      
      super.setByte(index, value);
   }

   @Override
   public void setBytes(int index, byte[] src, int srcIndex, int length)
   {      
      changed();
      
      super.setBytes(index, src, srcIndex, length);
   }

   @Override
   public void setBytes(int index, byte[] src)
   {      
      changed();
      
      super.setBytes(index, src);
   }

   @Override
   public void setBytes(int index, ByteBuffer src)
   {
      changed();
      
      super.setBytes(index, src);
   }

   @Override
   public void setBytes(int index, HornetQBuffer src, int srcIndex, int length)
   {
      changed();
      
      super.setBytes(index, src, srcIndex, length);
   }

   @Override
   public void setBytes(int index, HornetQBuffer src, int length)
   {
      changed();
      
      super.setBytes(index, src, length);
   }

   @Override
   public void setBytes(int index, HornetQBuffer src)
   {
      changed();
      
      super.setBytes(index, src);
   }

   @Override
   public void setChar(int index, char value)
   {
      changed();
      
      super.setChar(index, value);
   }

   @Override
   public void setDouble(int index, double value)
   {
      changed();
      
      super.setDouble(index, value);
   }

   @Override
   public void setFloat(int index, float value)
   {
      changed();
      
      super.setFloat(index, value);
   }

   @Override
   public void setInt(int index, int value)
   {
      changed();
      
      super.setInt(index, value);
   }

   @Override
   public void setLong(int index, long value)
   {
      changed();
      
      super.setLong(index, value);
   }

   @Override
   public void setShort(int index, short value)
   {
      changed();
      
      super.setShort(index, value);
   }

   @Override
   public void writeBoolean(boolean val)
   {
      changed();
      
      super.writeBoolean(val);
   }

   @Override
   public void writeByte(byte value)
   {
      changed();
      
      super.writeByte(value);
   }

   @Override
   public void writeBytes(byte[] src, int srcIndex, int length)
   {
      changed();
      
      super.writeBytes(src, srcIndex, length);
   }

   @Override
   public void writeBytes(byte[] src)
   {
      changed();
      
      super.writeBytes(src);
   }

   @Override
   public void writeBytes(ByteBuffer src)
   {
      changed();
      
      super.writeBytes(src);
   }

   @Override
   public void writeBytes(HornetQBuffer src, int srcIndex, int length)
   {
      changed();
      
      super.writeBytes(src, srcIndex, length);
   }

   @Override
   public void writeBytes(HornetQBuffer src, int length)
   {
      changed();
      
      super.writeBytes(src, length);
   }

   @Override
   public void writeChar(char chr)
   {
      changed();
      
      super.writeChar(chr);
   }

   @Override
   public void writeDouble(double value)
   {
      changed();
      
      super.writeDouble(value);
   }

   @Override
   public void writeFloat(float value)
   {
      changed();
      
      super.writeFloat(value);
   }

   @Override
   public void writeInt(int value)
   {
      changed();
      
      super.writeInt(value);
   }

   @Override
   public void writeLong(long value)
   {
      changed();
      
      super.writeLong(value);
   }

   @Override
   public void writeNullableSimpleString(SimpleString val)
   {
      changed();
      
      super.writeNullableSimpleString(val);
   }

   @Override
   public void writeNullableString(String val)
   {
      changed();
      
      super.writeNullableString(val);
    }

   @Override
   public void writeShort(short value)
   {
      changed();
      
      super.writeShort(value);
   }

   @Override
   public void writeSimpleString(SimpleString val)
   {
      changed();
      
      super.writeSimpleString(val);
   }

   @Override
   public void writeString(String val)
   {
      changed();
      
      super.writeString(val);
   }

   @Override
   public void writeUTF(String utf)
   {
      changed();
      
      super.writeUTF(utf);
   }
}
