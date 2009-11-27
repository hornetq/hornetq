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

import org.hornetq.core.buffers.HornetQBuffer;
import org.hornetq.core.buffers.impl.ChannelBufferWrapper;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.spi.Connection;
import org.hornetq.core.remoting.spi.ConnectionLifeCycleListener;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
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
            ChannelFuture sslCloseFuture = sslHandler.close(channel);

            if (!sslCloseFuture.awaitUninterruptibly(10000))
            {
               log.warn("Timed out waiting for ssl close future to complete");
            }
         }
         catch (Throwable t)
         {
            // ignore
         }
      }

      ChannelFuture closeFuture = channel.close();

      if (!closeFuture.awaitUninterruptibly(10000))
      {
         log.warn("Timed out waiting for channel to close");
      }

      closed = true;

      listener.connectionDestroyed(getID());
   }

   public HornetQBuffer createBuffer(final int size)
   {
      return new ChannelBufferWrapper(ChannelBuffers.dynamicBuffer(size));
   }

   public Object getID()
   {
      return channel.getId();
   }

   public void write(final HornetQBuffer buffer)
   {
      write(buffer, false);
   }

   public void write(HornetQBuffer buffer, final boolean flush)
   {
      ChannelFuture future = channel.write(buffer.channelBuffer());

      if (flush)
      {
         while (true)
         {
            try
            {
               boolean ok = future.await(10000);

               if (!ok)
               {
                  log.warn("Timed out waiting for packet to be flushed");
               }

               break;
            }
            catch (InterruptedException ignore)
            {
            }
         }
      }
   }

   public String getRemoteAddress()
   {
      return channel.getRemoteAddress().toString();
   }

   public void fail(final HornetQException me)
   {
      listener.connectionException(channel.getId(), me);
   }

   // Public --------------------------------------------------------

   @Override
   public String toString()
   {
      return super.toString() + "[local= " + channel.getLocalAddress() + ", remote=" + channel.getRemoteAddress() + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
