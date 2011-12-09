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

package org.hornetq.core.remoting.impl.netty;

import org.hornetq.core.buffers.impl.ChannelBufferWrapper;
import org.hornetq.core.logging.Logger;
import org.hornetq.spi.core.remoting.BufferDecoder;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

/**
 * A Netty FrameDecoder used to decode messages.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tlee@redhat.com">Trustin Lee</a>
 *
 * @version $Revision$, $Date$
 */
public class HornetQFrameDecoder extends FrameDecoder
{
   private static final Logger log = Logger.getLogger(HornetQFrameDecoder.class);

   private final BufferDecoder decoder;

   public HornetQFrameDecoder(final BufferDecoder decoder)
   {
      this.decoder = decoder;
   }

   // FrameDecoder overrides
   // -------------------------------------------------------------------------------------

   @Override
   protected Object decode(final ChannelHandlerContext ctx, final Channel channel, final ChannelBuffer in) throws Exception
   {    
      int start = in.readerIndex();

      int length = decoder.isReadyToHandle(new ChannelBufferWrapper(in));
      
      in.readerIndex(start);
      
      if (length == -1)
      {
         return null;
      }
      
      ChannelBuffer buffer = in.readBytes(length);

      ChannelBuffer newBuffer = ChannelBuffers.dynamicBuffer(buffer.writerIndex());

      newBuffer.writeBytes(buffer);
      
      return newBuffer;
   }
}
