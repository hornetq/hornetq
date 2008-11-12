/*
 * JBoss, Home of Professional Open Source
 *
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
package org.jboss.messaging.integration.transports.netty;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.spi.BufferHandler;
import org.jboss.messaging.core.remoting.spi.ConnectionLifeCycleListener;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

/**
 * Common handler implementation for client and server side handler.
 *
 * @author <a href="mailto:tlee@redhat.com">Trustin Lee</a>
 * @version $Rev$, $Date$
 */
class MessagingChannelHandler extends SimpleChannelHandler
{
   private static final Logger log = Logger.getLogger(MessagingChannelHandler.class);

   private final BufferHandler handler;
   private final ConnectionLifeCycleListener listener;
   volatile boolean active;

   MessagingChannelHandler(BufferHandler handler, ConnectionLifeCycleListener listener)
   {
      this.handler = handler;
      this.listener = listener;
   }

   @Override
   public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
   {
      ChannelBuffer buffer = (ChannelBuffer) e.getMessage();
      handler.bufferReceived(e.getChannel().getId(), new ChannelBufferWrapper(buffer));
   }

   @Override
   public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
   {
      synchronized (this)
      {
         if (active)
         {
            listener.connectionDestroyed(e.getChannel().getId());
            active = false;
         }
      }
   }

   @Override
   public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
   {
      active = false;
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception
   {
      log.error(
            "caught exception " + e.getCause() + " for channel " +
            e.getChannel(), e.getCause());

      synchronized (this)
      {
         if (!active)
         {
            return;
         }

         MessagingException me = new MessagingException(MessagingException.INTERNAL_ERROR, "Netty exception");
         me.initCause(e.getCause());
         try {
            listener.connectionException(e.getChannel().getId(), me);
            active = false;
         } catch (Exception ex) {
            log.error("failed to notify the listener:", ex);
         }
      }
   }
}
