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

package org.hornetq.integration.transports.netty;

import static org.hornetq.utils.DataConstants.SIZE_INT;

import org.hornetq.core.buffers.impl.ChannelBufferWrapper;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.spi.BufferHandler;
import org.hornetq.utils.DataConstants;
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

   private final BufferHandler handler;

   public HornetQFrameDecoder(final BufferHandler handler)
   {
      this.handler = handler;
   }

   // FrameDecoder overrides
   // -------------------------------------------------------------------------------------

   @Override
   protected Object decode(final ChannelHandlerContext ctx, final Channel channel, final ChannelBuffer in) throws Exception
   {
      // TODO - we can avoid this entirely if we maintain fragmented packets in the handler
      int start = in.readerIndex();

      int length = handler.isReadyToHandle(new ChannelBufferWrapper(in));
      
      in.readerIndex(start);
      
      if (length == -1)
      {                 
         return null;
      }

      //in.readerIndex(start + SIZE_INT);
      
      ChannelBuffer buffer = in.readBytes(length + DataConstants.SIZE_INT);
      
      // FIXME - we should get Netty to give us a DynamicBuffer - seems to currently give us a non resizable buffer

      ChannelBuffer newBuffer = ChannelBuffers.dynamicBuffer(buffer.writerIndex());
      
      newBuffer.writeBytes(buffer);
      
      newBuffer.readInt();
      
      return newBuffer;
   }
}
