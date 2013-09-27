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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.hornetq.utils.DataConstants;

/**
 * A Netty decoder specially optimised to to decode messages on the core protocol only
 *
 * @author <a href="tlee@redhat.com">Trustin Lee</a>
 * @author <a href="nmaurer@redhat.com">Norman Maurer</a>
 *
 * @version $Revision: 7839 $, $Date: 2009-08-21 02:26:39 +0900 (2009-08-21, 금) $
 */
public class HornetQFrameDecoder2 extends ChannelInboundHandlerAdapter
{
   private ByteBuf previousData = Unpooled.EMPTY_BUFFER;

   // SimpleChannelUpstreamHandler overrides
   // -------------------------------------------------------------------------------------

   @Override
   public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception
   {
      ByteBuf in = (ByteBuf) msg;
      if (previousData.isReadable())
      {
         if (previousData.readableBytes() + in.readableBytes() < DataConstants.SIZE_INT)
         {
            // XXX Length is unknown. Bet at 200. Tune this value.
            append(in, 200);
            return;
         }

         // Decode the first message. The first message requires a special
         // treatment because it is the only message that spans over the two
         // buffers.
         final int length;
         switch (previousData.readableBytes())
         {
            case 1:
               length = previousData.getUnsignedByte(previousData.readerIndex()) << 24 | in.getMedium(in.readerIndex());
               if (in.readableBytes() - 3 < length)
               {
                  append(in, length);
                  return;
               }
               break;
            case 2:
               length = previousData.getUnsignedShort(previousData.readerIndex()) << 16 | in.getUnsignedShort(in.readerIndex());
               if (in.readableBytes() - 2 < length)
               {
                  append(in, length);
                  return;
               }
               break;
            case 3:
               length = previousData.getUnsignedMedium(previousData.readerIndex()) << 8 | in.getUnsignedByte(in.readerIndex());
               if (in.readableBytes() - 1 < length)
               {
                  append(in, length);
                  return;
               }
               break;
            case 4:
               length = previousData.getInt(previousData.readerIndex());
               if (in.readableBytes() < length)
               {
                  append(in, length);
                  return;
               }
               break;
            default:
               length = previousData.getInt(previousData.readerIndex());
               if (in.readableBytes() + previousData.readableBytes() - 4 < length)
               {
                  append(in, length);
                  return;
               }
         }

         final ByteBuf frame;
         /*if (previousData instanceof DynamicChannelBuffer)
         {
            // It's safe to reuse the current dynamic buffer
            // because previousData will be reassigned to
            // EMPTY_BUFFER or 'in' later.
            previousData.writeBytes(in, length + 4 - previousData.readableBytes());
            frame = previousData;
         }
         else
         {
            // XXX Tune this value: Increasing the initial capacity of the
            // dynamic buffer might reduce the chance of additional memory
            // copy.
            frame = ctx.alloc().buffer(length + 4);
            frame.writeBytes(previousData, previousData.readerIndex(), previousData.readableBytes());
            frame.writeBytes(in, length + 4 - frame.writerIndex());
         }*/
         // XXX Tune this value: Increasing the initial capacity of the
         // dynamic buffer might reduce the chance of additional memory
         // copy.
         frame = ctx.alloc().buffer(length + 4);
         frame.writeBytes(previousData, previousData.readerIndex(), previousData.readableBytes());
         frame.writeBytes(in, length + 4 - frame.writerIndex());
         frame.skipBytes(4);
         if (!in.isReadable())
         {
            in.release();
            previousData = Unpooled.EMPTY_BUFFER;
            ctx.fireChannelRead(frame);
            return;
         }
         else
         {
             ctx.fireChannelRead(frame);
         }
      }

      // And then handle the rest - we don't need to deal with the
      // composite buffer anymore because the second or later messages
      // always belong to the second buffer.
      decode(ctx, in);

      // Handle the leftover.
      if (in.isReadable())
      {
         previousData = in;
      }
      else
      {
         previousData = Unpooled.EMPTY_BUFFER;
      }
   }

   private void decode(final ChannelHandlerContext ctx, final ByteBuf in)
   {
      for (;;)
      {
         final int readableBytes = in.readableBytes();
         if (readableBytes < DataConstants.SIZE_INT)
         {
            break;
         }

         final int length = in.getInt(in.readerIndex());
         if (readableBytes < length + DataConstants.SIZE_INT)
         {
            break;
         }

         // Convert to dynamic buffer (this requires copy)
         // XXX Tune this value: Increasing the initial capacity of the dynamic
         // buffer might reduce the chance of additional memory copy.
         ByteBuf frame = ctx.alloc().buffer(length + DataConstants.SIZE_INT);
         frame.writeBytes(in, length + DataConstants.SIZE_INT);
         frame.skipBytes(DataConstants.SIZE_INT);
         ctx.fireChannelRead(frame);
      }
   }

   private void append(final ByteBuf in, final int length)
   {
       previousData.discardReadBytes();
       previousData.writeBytes(in);
   }
}
