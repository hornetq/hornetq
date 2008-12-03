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

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.impl.ssl.SSLSupport;
import org.jboss.messaging.core.remoting.spi.Acceptor;
import org.jboss.messaging.core.remoting.spi.BufferHandler;
import org.jboss.messaging.core.remoting.spi.Connection;
import org.jboss.messaging.core.remoting.spi.ConnectionLifeCycleListener;
import org.jboss.messaging.util.ConfigurationHelper;
import org.jboss.messaging.util.JBMThreadFactory;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import static org.jboss.netty.channel.Channels.pipeline;
import static org.jboss.netty.channel.Channels.write;
import org.jboss.netty.channel.DefaultMessageEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.channel.socket.oio.OioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.ssl.SslHandler;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A Netty TCP Acceptor that supports SSL
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="tlee@redhat.com">Trustin Lee</a>
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

   private final BufferHandler handler;

   private final ConnectionLifeCycleListener listener;

   private final boolean sslEnabled;

   private final boolean httpEnabled;

   private final boolean useNio;

   private final String host;

   private final int port;

   private final String keyStorePath;

   private final String keyStorePassword;

   private final String trustStorePath;

   private final String trustStorePassword;

   private final boolean tcpNoDelay;

   private final int tcpSendBufferSize;

   private final int tcpReceiveBufferSize;

   public NettyAcceptor(final Map<String, Object> configuration, final BufferHandler handler,
                        final ConnectionLifeCycleListener listener)
   {
      this.handler = handler;

      this.listener = listener;

      this.sslEnabled =
            ConfigurationHelper.getBooleanProperty(TransportConstants.SSL_ENABLED_PROP_NAME, TransportConstants.DEFAULT_SSL_ENABLED, configuration);
      this.httpEnabled =
            ConfigurationHelper.getBooleanProperty(TransportConstants.HTTP_ENABLED_PROP_NAME, TransportConstants.DEFAULT_HTTP_ENABLED, configuration);
      this.useNio =
            ConfigurationHelper.getBooleanProperty(TransportConstants.USE_NIO_PROP_NAME, TransportConstants.DEFAULT_USE_NIO, configuration);
      this.host =
            ConfigurationHelper.getStringProperty(TransportConstants.HOST_PROP_NAME, TransportConstants.DEFAULT_HOST, configuration);
      this.port =
            ConfigurationHelper.getIntProperty(TransportConstants.PORT_PROP_NAME, TransportConstants.DEFAULT_PORT, configuration);
      if (sslEnabled)
      {
         this.keyStorePath =
               ConfigurationHelper.getStringProperty(TransportConstants.KEYSTORE_PATH_PROP_NAME, TransportConstants.DEFAULT_KEYSTORE_PATH, configuration);
         this.keyStorePassword =
               ConfigurationHelper.getStringProperty(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, TransportConstants.DEFAULT_KEYSTORE_PASSWORD, configuration);
         this.trustStorePath =
               ConfigurationHelper.getStringProperty(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, TransportConstants.DEFAULT_TRUSTSTORE_PATH, configuration);
         this.trustStorePassword =
               ConfigurationHelper.getStringProperty(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, TransportConstants.DEFAULT_TRUSTSTORE_PASSWORD, configuration);
      }
      else
      {
         this.keyStorePath = null;
         this.keyStorePassword = null;
         this.trustStorePath = null;
         this.trustStorePassword = null;
      }

      this.tcpNoDelay =
            ConfigurationHelper.getBooleanProperty(TransportConstants.TCP_NODELAY_PROPNAME, TransportConstants.DEFAULT_TCP_NODELAY, configuration);
      this.tcpSendBufferSize =
            ConfigurationHelper.getIntProperty(TransportConstants.TCP_SENDBUFFER_SIZE_PROPNAME, TransportConstants.DEFAULT_TCP_SENDBUFFER_SIZE, configuration);
      this.tcpReceiveBufferSize =
            ConfigurationHelper.getIntProperty(TransportConstants.TCP_RECEIVEBUFFER_SIZE_PROPNAME, TransportConstants.DEFAULT_TCP_RECEIVEBUFFER_SIZE, configuration);

   }

   public synchronized void start() throws Exception
   {
      if (channelFactory != null)
      {
         //Already started
         return;
      }

      bossExecutor = Executors.newCachedThreadPool(new JBMThreadFactory("jbm-netty-acceptor-boss-threads"));
      workerExecutor = Executors.newCachedThreadPool(new JBMThreadFactory("jbm-netty-acceptor-worker-threads"));
      if (useNio)
      {
         channelFactory = new NioServerSocketChannelFactory(bossExecutor, workerExecutor);
      }
      else
      {
         channelFactory = new OioServerSocketChannelFactory(bossExecutor, workerExecutor);
      }
      bootstrap = new ServerBootstrap(channelFactory);

      final SSLContext context;
      if (sslEnabled)
      {
         try
         {
            context = SSLSupport.createServerContext(
                  keyStorePath,
                  keyStorePassword,
                  trustStorePath,
                  trustStorePassword);
         }
         catch (Exception e)
         {
            IllegalStateException ise = new IllegalStateException(
                  "Unable to create NettyAcceptor for " +
                  host + ":" + port);
            ise.initCause(e);
            throw ise;
         }
      }
      else
      {
         context = null; // Unused
      }

      bootstrap.setPipelineFactory(new ChannelPipelineFactory()
      {
         public ChannelPipeline getPipeline() throws Exception
         {
            ChannelPipeline pipeline = pipeline();
            if (sslEnabled)
            {
               ChannelPipelineSupport.addSSLFilter(pipeline, context, false);
            }
            if (httpEnabled)
            {
               pipeline.addLast("httpRequestDecoder", new HttpRequestDecoder());
               pipeline.addLast("httpResponseEncoder", new HttpResponseEncoder());
               pipeline.addLast("httphandler", new HttpHandler());
            }

            ChannelPipelineSupport.addCodecFilter(pipeline, handler);
            pipeline.addLast("handler", new MessagingServerChannelHandler(handler, listener));
            return pipeline;
         }
      });

      // Bind
      bootstrap.setOption("localAddress", new InetSocketAddress(host, port));
      bootstrap.setOption("child.tcpNoDelay", tcpNoDelay);
      if (tcpReceiveBufferSize != -1)
      {
         bootstrap.setOption("child.receiveBufferSize", tcpReceiveBufferSize);
      }
      if (tcpSendBufferSize != -1)
      {
         bootstrap.setOption("child.sendBufferSize", tcpSendBufferSize);
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
      for (; ;)
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
      MessagingServerChannelHandler(BufferHandler handler, ConnectionLifeCycleListener listener)
      {
         super(handler, listener);
      }

      @Override
      public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
      {
         final Connection tc = new NettyConnection(e.getChannel());

         SslHandler sslHandler = ctx.getPipeline().get(SslHandler.class);
         if (sslHandler != null)
         {
            sslHandler.handshake(e.getChannel()).addListener(new ChannelFutureListener()
            {
               public void operationComplete(ChannelFuture future) throws Exception
               {
                  if (future.isSuccess())
                  {
                     listener.connectionCreated(tc);
                     active = true;
                  }
                  else
                  {
                     future.getChannel().close();
                  }
               }
            });
         }
         else
         {
            listener.connectionCreated(tc);
            active = true;
         }
      }
   }

   @ChannelPipelineCoverage("all")
   class HttpHandler extends SimpleChannelHandler
   {
      @Override
      public void messageReceived(final ChannelHandlerContext ctx,final MessageEvent e) throws Exception
      {
         HttpRequest request = (HttpRequest) e.getMessage();
         MessageEvent event = new DefaultMessageEvent(e.getChannel(), e.getFuture(), request.getContent(), e.getRemoteAddress());
         ctx.sendUpstream(event);
      }

      @Override
      public void writeRequested(final ChannelHandlerContext ctx,final MessageEvent e) throws Exception
      {
         HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
         ChannelBuffer buf = (ChannelBuffer) e.getMessage();
         response.setContent(buf);
         response.addHeader(HttpHeaders.CONTENT_LENGTH, String.valueOf(buf.writerIndex()));
         write(ctx, e.getChannel(), e.getFuture(), response, e.getRemoteAddress());
      }
   }
}
