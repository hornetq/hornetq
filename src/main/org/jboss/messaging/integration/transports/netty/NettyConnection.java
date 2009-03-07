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

package org.jboss.messaging.integration.transports.netty;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.spi.Connection;
import org.jboss.messaging.core.remoting.spi.ConnectionLifeCycleListener;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.handler.ssl.SslHandler;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * buhnaflagilibrn
 * @version <tt>$Revision$</tt>
 */
public class NettyConnection implements Connection
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(NettyConnection.class);


   // Attributes ----------------------------------------------------

   private final Channel channel;

   private boolean closed;
   
   private final ConnectionLifeCycleListener listener;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public NettyConnection(final Channel channel, final ConnectionLifeCycleListener listener)
   {      
      this.channel = channel;
      
      this.listener = listener;
      
      listener.connectionCreated(this);
   }

   // Public --------------------------------------------------------

   // Connection implementation ----------------------------

   public synchronized void close()
   {
      if (closed)
      {
         return;
      }
      
      SslHandler sslHandler = (SslHandler)channel.getPipeline().get("ssl");
      if (sslHandler != null)
      {
         try
         {
            sslHandler.close(channel).addListener(ChannelFutureListener.CLOSE);
         }
         catch (Throwable t)
         {
            // ignore
         }
      }
      else
      {
         channel.close();
      }

      // This block has been disabled because this method can be called from
      // the Netty I/O thread.
      // TODO Netty should be improved to provide a way to determine
      // if the current code is running in the I/O thread.
      //
      // if (channel.getParent() == null) {
      // // A client channel - wait until everything is cleaned up.
      // // TODO Do not spin - use signal.
      // MessagingChannelHandler handler = (MessagingChannelHandler) channel.getPipeline().get("handler");
      // while (handler.active) {
      // Thread.yield();
      // }
      // }

      closed = true;
      
      listener.connectionDestroyed(getID());
   }

   public MessagingBuffer createBuffer(final int size)
   {
      return new ChannelBufferWrapper(org.jboss.netty.buffer.ChannelBuffers.buffer(size));
   }

   public Object getID()
   {
      return channel.getId();
   }

   public void write(final MessagingBuffer buffer)
   {
      channel.write(buffer.getUnderlyingBuffer());
   }

   public String getRemoteAddress()
   {
      log.info("channel is " + channel);
      log.info("channel remote address " + channel.getRemoteAddress());
      return channel.getRemoteAddress().toString();
   }
   
   public void fail(final MessagingException me)
   {
      listener.connectionException(channel.getId(), me);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
