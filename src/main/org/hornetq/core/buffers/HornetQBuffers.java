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

import org.hornetq.core.buffers.impl.ChannelBufferWrapper;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;


/**
 * 
 * A HornetQChannelBuffers
 *
 * @author tim
 *
 *
 */
public class HornetQBuffers
{
   public static HornetQBuffer dynamicBuffer(int estimatedLength)
   {
      return new ChannelBufferWrapper(ChannelBuffers.dynamicBuffer(estimatedLength));
   }
   
   public static HornetQBuffer dynamicBuffer(byte[] bytes)
   {
      ChannelBuffer buff = ChannelBuffers.dynamicBuffer(bytes.length);
      
      buff.writeBytes(bytes);
      
      return new ChannelBufferWrapper(buff);
   }
   
   public static HornetQBuffer wrappedBuffer(ByteBuffer underlying)
   {
      HornetQBuffer buff = new ChannelBufferWrapper(ChannelBuffers.wrappedBuffer(underlying));
      
      buff.clear();
      
      return buff;
   }
   
   public static HornetQBuffer wrappedBuffer(byte[] underlying)
   {
      return new ChannelBufferWrapper(ChannelBuffers.wrappedBuffer(underlying));
   }
   
   public static HornetQBuffer fixedBuffer(int size)
   {
      return new ChannelBufferWrapper(ChannelBuffers.buffer(size));
   }
}
