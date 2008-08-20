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
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.remoting.ConnectionLifeCycleListener;
import org.jboss.messaging.core.remoting.RemotingHandler;
import org.jboss.messaging.core.remoting.impl.ssl.SSLSupport;
import org.jboss.messaging.core.remoting.spi.Acceptor;
import org.jboss.messaging.core.remoting.spi.Connection;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.ssl.SslHandler;

/**
 * A Netty TCP Acceptor that supports SSL
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="tlee@redhat.com">Trustin Lee</a>
 *
 * @version $Rev$, $Date$
 */
public class NettyAcceptor implements Acceptor
{
   private ExecutorService bossExecutor;
   private ExecutorService workerExecutor;
   private ChannelFactory channelFactory;
   private Channel serverChannel;
   private ServerBootstrap bootstrap;

   private final Configuration configuration;

   private final RemotingHandler handler;

   private final ConnectionLifeCycleListener listener;

   public NettyAcceptor(final Configuration configuration, final RemotingHandler handler,
                       final ConnectionLifeCycleListener listener)
   {
      this.configuration = configuration;

      this.handler = handler;

      this.listener = listener;
   }

   public synchronized void start() throws Exception
   {
      if (channelFactory != null)
      {
         //Already started
         return;
      }

      bossExecutor = Executors.newCachedThreadPool();
      workerExecutor = Executors.newCachedThreadPool();
      channelFactory = new NioServerSocketChannelFactory(bossExecutor, workerExecutor);
      bootstrap = new ServerBootstrap(channelFactory);

      final SSLContext context;
      if (configuration.isSSLEnabled()) {
         try {
            context = SSLSupport.createServerContext(
                  configuration.getKeyStorePath(),
                  configuration.getKeyStorePassword(),
                  configuration.getTrustStorePath(),
                  configuration.getTrustStorePassword());
         } catch (Exception e) {
            IllegalStateException ise = new IllegalStateException(
                  "Unable to create NettyAcceptor for " +
                  configuration.getHost() + ":" + configuration.getPort());
            ise.initCause(e);
            throw ise;
         }
      } else {
         context = null; // Unused
      }

      bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
         public ChannelPipeline getPipeline() throws Exception
         {
            ChannelPipeline pipeline = pipeline();
            if (configuration.isSSLEnabled())
            {
               ChannelPipelineSupport.addSSLFilter(pipeline, context, false);
            }
            ChannelPipelineSupport.addCodecFilter(pipeline, handler);
            pipeline.addLast("handler", new MessagingServerChannelHandler(handler, listener));
            return pipeline;
         }
      });

      // Bind
      bootstrap.setOption("localAddress", new InetSocketAddress(configuration.getHost(), configuration.getPort()));
      bootstrap.setOption("child.tcpNoDelay", configuration.getConnectionParams().isTcpNoDelay());
      int receiveBufferSize = configuration.getConnectionParams().getTcpReceiveBufferSize();
      if (receiveBufferSize != -1)
      {
         bootstrap.setOption("child.receiveBufferSize", receiveBufferSize);
      }
      int sendBufferSize = configuration.getConnectionParams().getTcpSendBufferSize();
      if (sendBufferSize != -1)
      {
         bootstrap.setOption("child.sendBufferSize", sendBufferSize);
      }
      bootstrap.setOption("reuseAddress", true);
      bootstrap.setOption("child.reuseAddress", true);
      bootstrap.setOption("child.keepAlive", true);

      serverChannel = bootstrap.bind();
   }

   public synchronized void stop()
   {
      if (channelFactory == null)
      {
         return;
      }

      serverChannel.close().awaitUninterruptibly();
      bossExecutor.shutdown();
      workerExecutor.shutdown();
      for (;;)
      {
         try
         {
            if (bossExecutor.awaitTermination(1, TimeUnit.SECONDS))
            {
               break;
            }
         }
         catch (InterruptedException e)
         {
            // Ignore
         }
      }
      channelFactory = null;
   }

   // Inner classes -----------------------------------------------------------------------------

   @ChannelPipelineCoverage("one")
   private final class MessagingServerChannelHandler extends MessagingChannelHandler
   {
      MessagingServerChannelHandler(RemotingHandler handler, ConnectionLifeCycleListener listener)
      {
         super(handler, listener);
      }

      @Override
      public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
      {
         final Connection tc = new NettyConnection(e.getChannel());

         SslHandler sslHandler = ctx.getPipeline().get(SslHandler.class);
         if (sslHandler != null) {
            sslHandler.handshake(e.getChannel()).addListener(new ChannelFutureListener()
            {
               public void operationComplete(ChannelFuture future) throws Exception
               {
                  if (future.isSuccess()) {
                     listener.connectionCreated(tc);
                     active = true;
                  } else {
                     future.getChannel().close();
                  }
               }
            });
         } else {
            listener.connectionCreated(tc);
            active = true;
         }
      }
   }
}
