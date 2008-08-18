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

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.ConnectionLifeCycleListener;
import org.jboss.messaging.core.remoting.RemotingHandler;
import org.jboss.messaging.core.remoting.spi.Acceptor;
import org.jboss.messaging.core.remoting.spi.Connection;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChildChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

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
   private static final Logger log = Logger.getLogger(NettyAcceptor.class);

   private ExecutorService bossExecutor;
   private ExecutorService workerExecutor;
   private ChannelFactory channelFactory;
   private Channel serverChannel;
   private ServerBootstrap bootstrap;
   private NettyChildChannelHandler childChannelHandler;

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
      childChannelHandler = new NettyChildChannelHandler();
      bootstrap.setParentHandler(childChannelHandler);

      bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
         public ChannelPipeline getPipeline() throws Exception
         {
            ChannelPipeline pipeline = pipeline();
            if (configuration.isSSLEnabled())
            {
               ChannelPipelineSupport.addSSLFilter(pipeline, false, configuration.getKeyStorePath(),
                       configuration.getKeyStorePassword(),
                       configuration.getTrustStorePath(),
                       configuration.getTrustStorePassword());
            }
            ChannelPipelineSupport.addCodecFilter(pipeline, handler);
            pipeline.addLast("handler", new NettyHandler());
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

      // remove the listener before disposing the acceptor
      // so that we're not notified when the sessions are destroyed
      serverChannel.getPipeline().remove(childChannelHandler);
      serverChannel.close().awaitUninterruptibly();
      bossExecutor.shutdown();
      workerExecutor.shutdown();
      channelFactory = null;
   }

   // Inner classes -----------------------------------------------------------------------------

   @ChannelPipelineCoverage("one")
   private final class NettyHandler extends SimpleChannelHandler
   {
      @Override
      public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception
      {
         log.error(
               "caught exception " + e.getCause() + " for channel " +
               e.getChannel(), e.getCause());
         MessagingException me = new MessagingException(MessagingException.INTERNAL_ERROR, "Netty exception");
         me.initCause(e.getCause());
         listener.connectionException(e.getChannel().getId(), me);
      }

      @Override
      public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
      {
         ChannelBuffer buffer = (ChannelBuffer) e.getMessage();
         handler.bufferReceived(e.getChannel().getId(), new ChannelBufferWrapper(buffer));
      }
   }

   @ChannelPipelineCoverage("one")
   private final class NettyChildChannelHandler extends SimpleChannelHandler
   {

      @Override
      public void childChannelOpen(ChannelHandlerContext ctx, ChildChannelStateEvent e) throws Exception
      {
         Connection tc = new NettyConnection(e.getChildChannel());
         listener.connectionCreated(tc);
         ctx.sendUpstream(e);
      }

      @Override
      public void childChannelClosed(ChannelHandlerContext ctx, ChildChannelStateEvent e) throws Exception
      {
         listener.connectionDestroyed(e.getChildChannel().getId());
         ctx.sendUpstream(e);
      }
   }
}
