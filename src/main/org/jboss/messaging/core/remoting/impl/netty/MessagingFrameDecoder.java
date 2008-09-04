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

package org.jboss.messaging.core.remoting.impl.netty;

import static org.jboss.messaging.util.DataConstants.SIZE_INT;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.spi.BufferHandler;
import org.jboss.netty.buffer.ChannelBuffer;
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
public class MessagingFrameDecoder extends FrameDecoder
{
   private static final Logger log = Logger.getLogger(MessagingFrameDecoder.class);

   
   private final BufferHandler handler;

   public MessagingFrameDecoder(final BufferHandler handler)
   {
      this.handler = handler;
   }

   // FrameDecoder overrides
   // -------------------------------------------------------------------------------------

   @Override
   protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer in) throws Exception
   {
      //TODO - we can avoid this entirely if we maintain fragmented packets in the handler
      int start = in.readerIndex();

      int length = handler.isReadyToHandle(new ChannelBufferWrapper(in));
      if (length == -1)
      {
         in.readerIndex(start);
         return null;
      }

      in.readerIndex(start + SIZE_INT);
      return in.readBytes(length);
   }
}
