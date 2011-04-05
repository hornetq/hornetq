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

import org.hornetq.api.core.HornetQException;
import org.hornetq.core.buffers.impl.ChannelBufferWrapper;
import org.hornetq.core.logging.Logger;
import org.hornetq.spi.core.remoting.BufferHandler;
import org.hornetq.spi.core.remoting.ConnectionLifeCycleListener;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;

/**
 * Common handler implementation for client and server side handler.
 *
 * @author <a href="mailto:tlee@redhat.com">Trustin Lee</a>
 * @version $Rev$, $Date$
 */
class HornetQChannelHandler extends SimpleChannelHandler
{
   private static final Logger log = Logger.getLogger(HornetQChannelHandler.class);

   private final ChannelGroup group;

   private final BufferHandler handler;

   private final ConnectionLifeCycleListener listener;

   volatile boolean active;

   HornetQChannelHandler(final ChannelGroup group,
                         final BufferHandler handler,
                         final ConnectionLifeCycleListener listener)
   {
      this.group = group;
      this.handler = handler;
      this.listener = listener;
   }

   @Override
   public void channelOpen(final ChannelHandlerContext ctx, final ChannelStateEvent e) throws Exception
   {
      group.add(e.getChannel());
      ctx.sendUpstream(e);
   }
         
   @Override
   public void channelInterestChanged(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
   {
      listener.connectionReadyForWrites(e.getChannel().getId(), e.getChannel().isWritable());      
   }

   @Override
   public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) throws Exception
   {
      ChannelBuffer buffer = (ChannelBuffer)e.getMessage();
      
      handler.bufferReceived(e.getChannel().getId(), new ChannelBufferWrapper(buffer));      
   }

   @Override
   public void channelDisconnected(final ChannelHandlerContext ctx, final ChannelStateEvent e) throws Exception
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
   public void channelClosed(final ChannelHandlerContext ctx, final ChannelStateEvent e) throws Exception
   {
      active = false;
   }

   @Override
   public void exceptionCaught(final ChannelHandlerContext ctx, final ExceptionEvent e) throws Exception
   {
      synchronized (this)
      {
         if (!active)
         {
            return;
         }

         // We don't want to log this - since it is normal for this to happen during failover/reconnect
         // and we don't want to spew out stack traces in that event
         // The user has access to this exeception anyway via the HornetQException initial cause

         HornetQException me = new HornetQException(HornetQException.INTERNAL_ERROR, "Netty exception");
         me.initCause(e.getCause());
         try
         {
            listener.connectionException(e.getChannel().getId(), me);
            active = false;
         }
         catch (Exception ex)
         {
            HornetQChannelHandler.log.error("failed to notify the listener:", ex);
         }
      }
   }   
   
}
