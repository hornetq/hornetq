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

import static org.jboss.netty.channel.Channels.*;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jboss.messaging.core.client.ConnectionParams;
import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.ConnectionLifeCycleListener;
import org.jboss.messaging.core.remoting.RemotingHandler;
import org.jboss.messaging.core.remoting.spi.Connection;
import org.jboss.messaging.core.remoting.spi.Connector;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.ssl.SslHandler;

/**
 *
 * A NettyConnector
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:tlee@redhat.com">Trustin Lee</a>
 */
public class NettyConnector implements Connector
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(NettyConnection.class);

   // Attributes ----------------------------------------------------

   private ExecutorService bossExecutor;
   private ExecutorService workerExecutor;
   private ChannelFactory  channelFactory;
   private ClientBootstrap bootstrap;

   private final RemotingHandler handler;

   private final Location location;

   private final ConnectionLifeCycleListener listener;

   private final ConnectionParams params;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public NettyConnector(final Location location, final ConnectionParams params,
                        final RemotingHandler handler,
                        final ConnectionLifeCycleListener listener)
   {
      if (location == null)
      {
         throw new IllegalArgumentException("Invalid argument null location");
      }

      if (params == null)
      {
         throw new IllegalArgumentException("Invalid argument null connection params");
      }

      if (handler == null)
      {
         throw new IllegalArgumentException("Invalid argument null handler");
      }

      if (listener == null)
      {
         throw new IllegalArgumentException("Invalid argument null listener");
      }

      this.handler = handler;
      this.location = location;
      this.listener = listener;
      this.params = params;
   }

   public synchronized void start()
   {
      if (channelFactory != null)
      {
         return;
      }

      bossExecutor = Executors.newCachedThreadPool();
      workerExecutor = Executors.newCachedThreadPool();
      channelFactory = new NioClientSocketChannelFactory(bossExecutor, workerExecutor);
      bootstrap = new ClientBootstrap(channelFactory);

      bootstrap.setOption("tcpNoDelay", params.isTcpNoDelay());
      if (params.getTcpReceiveBufferSize() != -1)
      {
         bootstrap.setOption("receiveBufferSize", params.getTcpReceiveBufferSize());
      }
      if (params.getTcpSendBufferSize() != -1)
      {
         bootstrap.setOption("sendBufferSize", params.getTcpSendBufferSize());
      }
      bootstrap.setOption("keepAlive", true);
      bootstrap.setOption("reuseAddress", true);

      bootstrap.setPipelineFactory(new ChannelPipelineFactory() {

         public ChannelPipeline getPipeline() throws Exception
         {
            ChannelPipeline pipeline = pipeline();
            if (params.isSSLEnabled())
            {
               try
               {
                  ChannelPipelineSupport.addSSLFilter(pipeline, true, params.getKeyStorePath(), params.getKeyStorePassword(), null, null);
               }
               catch (Exception e)
               {
                  IllegalStateException ise = new IllegalStateException(
                        "Unable to create Netty connection for " + location);
                  ise.initCause(e);
                  throw ise;
               }
            }
            ChannelPipelineSupport.addCodecFilter(pipeline, handler);
            pipeline.addLast("handler", new NettyHandler());
            return pipeline;
         }
      });
   }

   public synchronized void close()
   {
      if (channelFactory == null)
      {
         return;
      }

      bootstrap = null;
      channelFactory = null;
      bossExecutor.shutdown();
      workerExecutor.shutdown();
   }

   public Connection createConnection()
   {
      InetSocketAddress address = new InetSocketAddress(location.getHost(), location.getPort());
      ChannelFuture future = bootstrap.connect(address);
      future.awaitUninterruptibly();

      if (future.isSuccess())
      {
         return new NettyConnection(future.getChannel());
      }
      else
      {
         return null;
      }
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   @ChannelPipelineCoverage("one")
   private final class NettyHandler extends SimpleChannelHandler
   {
      @Override
      public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
      {
         ChannelBuffer buffer = (ChannelBuffer) e.getMessage();
         handler.bufferReceived(e.getChannel().getId(), new ChannelBufferWrapper(buffer));
      }

      @Override
      public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
      {
         SslHandler sslHandler = ctx.getPipeline().get(SslHandler.class);
         if (sslHandler != null) {
            log.info("Starting SSL handshake.");
            sslHandler.handshake(e.getChannel());
         }
      }

      @Override
      public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
      {
         listener.connectionDestroyed(e.getChannel().getId());
      }

      @Override
      public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception
      {
         log.error(
               "caught exception " + e.getCause() + " for channel " +
               e.getChannel(), e.getCause());

         MessagingException me = new MessagingException(MessagingException.INTERNAL_ERROR, "Netty exception");
         me.initCause(e.getCause());
         try {
            listener.connectionException(e.getChannel().getId(), me);
         } catch (Exception ex) {
            log.error("failed to notify the listener:", ex);
         }
      }
   }
}
