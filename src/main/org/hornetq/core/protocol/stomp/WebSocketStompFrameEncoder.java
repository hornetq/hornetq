/*
 * Copyright 2010 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.hornetq.core.protocol.stomp;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.buffers.impl.ChannelBufferWrapper;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.websocket.DefaultWebSocketFrame;
import org.jboss.netty.handler.codec.http.websocket.WebSocketFrame;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

/**
 * Encodes a {@link WebSocketFrame} into a {@link ChannelBuffer}.
 * <p>
 * For the detailed instruction on adding add Web Socket support to your HTTP
* server, take a look into the <tt>WebSocketServer</tt> example located in the
 * {@code org.jboss.netty.example.http.websocket} package.
 *
 * @author The Netty Project (netty-dev@lists.jboss.org)
 * @author Mike Heath (mheath@apache.org)
 * @author Trustin Lee (trustin@gmail.com)
 * @version $Rev: 2019 $, $Date: 2010-01-09 21:00:24 +0900 (Sat, 09 Jan 2010) $
 */
public class WebSocketStompFrameEncoder extends OneToOneEncoder
{
   @Override
   protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception
   {
      
      if (msg instanceof ChannelBuffer)
      {
         // FIXME - this is a silly way to do this - a better way to do this would be to create a new protocol, with protocol manager etc
         // and re-use some of the STOMP codec stuff - Tim
         
         
         // this is ugly and slow!
         // we have to go ChannelBuffer -> HornetQBuffer -> StompFrame -> String -> WebSocketFrame
         // since HornetQ protocol SPI requires to return HornetQBuffer to the transport
         HornetQBuffer buffer = new ChannelBufferWrapper((ChannelBuffer)msg);
         
         StompDecoder decoder = new StompDecoder();
         
         StompFrame frame = decoder.decode(buffer);
         
         if (frame != null)
         {
            WebSocketFrame wsFrame = new DefaultWebSocketFrame(frame.asString());

            // Text frame
            ChannelBuffer data = wsFrame.getBinaryData();
            ChannelBuffer encoded = channel.getConfig().getBufferFactory().getBuffer(data.order(),
                                                                                     data.readableBytes() + 2);
            encoded.writeByte((byte)wsFrame.getType());
            encoded.writeBytes(data, data.readableBytes());
            encoded.writeByte((byte)0xFF);
            return encoded;

         }
      }
      return msg;
   }
}