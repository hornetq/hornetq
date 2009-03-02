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

import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.impl.ssl.SSLSupport;
import org.jboss.messaging.core.remoting.spi.Acceptor;
import org.jboss.messaging.core.remoting.spi.BufferHandler;
import org.jboss.messaging.core.remoting.spi.Connection;
import org.jboss.messaging.core.remoting.spi.ConnectionLifeCycleListener;
import org.jboss.messaging.utils.ConfigurationHelper;
import org.jboss.messaging.utils.JBMThreadFactory;
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
import static org.jboss.netty.channel.Channels.pipeline;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.local.DefaultLocalServerChannelFactory;
import org.jboss.netty.channel.local.LocalAddress;
import org.jboss.netty.channel.local.LocalServerChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.channel.socket.oio.OioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.ssl.SslHandler;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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

   private DefaultChannelGroup serverChannelGroup;

   private ServerBootstrap bootstrap;

   private final BufferHandler handler;

   private final ConnectionLifeCycleListener listener;

   private final boolean sslEnabled;

   private final boolean httpEnabled;

   private final long httpServerScanPeriod;

   private final long httpResponseTime;

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

   private final Timer httpKeepAliveTimer;

   private final HttpKeepAliveTask httpKeepAliveTask;

   private ConcurrentMap<Object, Connection> connections = new ConcurrentHashMap<Object, Connection>();

   public NettyAcceptor(final Map<String, Object> configuration,
                        final BufferHandler handler,
                        final ConnectionLifeCycleListener listener)
   {
      this.handler = handler;

      this.listener = listener;

      this.sslEnabled = ConfigurationHelper.getBooleanProperty(TransportConstants.SSL_ENABLED_PROP_NAME,
                                                               TransportConstants.DEFAULT_SSL_ENABLED,
                                                               configuration);
      this.httpEnabled = ConfigurationHelper.getBooleanProperty(TransportConstants.HTTP_ENABLED_PROP_NAME,
                                                                TransportConstants.DEFAULT_HTTP_ENABLED,
                                                                configuration);

      if (httpEnabled)
      {
         httpServerScanPeriod = ConfigurationHelper.getLongProperty(TransportConstants.HTTP_SERVER_SCAN_PERIOD_PROP_NAME,
                                                                    TransportConstants.DEFAULT_HTTP_SERVER_SCAN_PERIOD,
                                                                    configuration);
         httpResponseTime = ConfigurationHelper.getLongProperty(TransportConstants.HTTP_RESPONSE_TIME_PROP_NAME,
                                                                TransportConstants.DEFAULT_HTTP_RESPONSE_TIME,
                                                                configuration);
         httpKeepAliveTimer = new Timer();
         httpKeepAliveTask = new HttpKeepAliveTask();
         httpKeepAliveTimer.schedule(httpKeepAliveTask, httpServerScanPeriod, httpServerScanPeriod);
      }
      else
      {
         httpServerScanPeriod = 0;
         httpResponseTime = 0;
         httpKeepAliveTimer = null;
         httpKeepAliveTask = null;
      }
      this.useNio = ConfigurationHelper.getBooleanProperty(TransportConstants.USE_NIO_PROP_NAME,
                                                           TransportConstants.DEFAULT_USE_NIO,
                                                           configuration);
      this.host = ConfigurationHelper.getStringProperty(TransportConstants.HOST_PROP_NAME,
                                                        TransportConstants.DEFAULT_HOST,
                                                        configuration);
      this.port = ConfigurationHelper.getIntProperty(TransportConstants.PORT_PROP_NAME,
                                                     TransportConstants.DEFAULT_PORT,
                                                     configuration);
      if (sslEnabled)
      {
         this.keyStorePath = ConfigurationHelper.getStringProperty(TransportConstants.KEYSTORE_PATH_PROP_NAME,
                                                                   TransportConstants.DEFAULT_KEYSTORE_PATH,
                                                                   configuration);
         this.keyStorePassword = ConfigurationHelper.getStringProperty(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME,
                                                                       TransportConstants.DEFAULT_KEYSTORE_PASSWORD,
                                                                       configuration);
         this.trustStorePath = ConfigurationHelper.getStringProperty(TransportConstants.TRUSTSTORE_PATH_PROP_NAME,
                                                                     TransportConstants.DEFAULT_TRUSTSTORE_PATH,
                                                                     configuration);
         this.trustStorePassword = ConfigurationHelper.getStringProperty(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME,
                                                                         TransportConstants.DEFAULT_TRUSTSTORE_PASSWORD,
                                                                         configuration);
      }
      else
      {
         this.keyStorePath = null;
         this.keyStorePassword = null;
         this.trustStorePath = null;
         this.trustStorePassword = null;
      }

      this.tcpNoDelay = ConfigurationHelper.getBooleanProperty(TransportConstants.TCP_NODELAY_PROPNAME,
                                                               TransportConstants.DEFAULT_TCP_NODELAY,
                                                               configuration);
      this.tcpSendBufferSize = ConfigurationHelper.getIntProperty(TransportConstants.TCP_SENDBUFFER_SIZE_PROPNAME,
                                                                  TransportConstants.DEFAULT_TCP_SENDBUFFER_SIZE,
                                                                  configuration);
      this.tcpReceiveBufferSize = ConfigurationHelper.getIntProperty(TransportConstants.TCP_RECEIVEBUFFER_SIZE_PROPNAME,
                                                                     TransportConstants.DEFAULT_TCP_RECEIVEBUFFER_SIZE,
                                                                     configuration);
   }

   public synchronized void start() throws Exception
   {
      if (channelFactory != null)
      {
         // Already started
         return;
      }
      bossExecutor = Executors.newCachedThreadPool(new org.jboss.messaging.utils.JBMThreadFactory("jbm-netty-acceptor-boss-threads"));
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
            context = SSLSupport.createServerContext(keyStorePath, keyStorePassword, trustStorePath, trustStorePassword);
         }
         catch (Exception e)
         {
            IllegalStateException ise = new IllegalStateException("Unable to create NettyAcceptor for " + host +
                                                                  ":" +
                                                                  port);
            ise.initCause(e);
            throw ise;
         }
      }
      else
      {
         context = null; // Unused
      }

      ChannelPipelineFactory factory = new ChannelPipelineFactory()
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
               pipeline.addLast("httphandler", new HttpAcceptorHandler(httpKeepAliveTask, httpResponseTime));
            }

            ChannelPipelineSupport.addCodecFilter(pipeline, handler);
            pipeline.addLast("handler", new MessagingServerChannelHandler(handler, listener));
            return pipeline;
         }
      };
      bootstrap.setPipelineFactory(factory);

      // Bind
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

      serverChannelGroup = new DefaultChannelGroup("jbm");

      String[] hosts = TransportConfiguration.splitHosts(host);
      for (String h : hosts)
      {
         Channel serverChannel = bootstrap.bind(new InetSocketAddress(h, port));
         serverChannelGroup.add(serverChannel);
      }
   }

   public synchronized void stop()
   {
      
      if (channelFactory == null)
      {
         return;
      }

      if (httpKeepAliveTimer != null)
      {
         httpKeepAliveTask.cancel();

         httpKeepAliveTimer.cancel();
      }
      serverChannelGroup.close().awaitUninterruptibly();
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

      for (Connection connection : connections.values())
      {
         listener.connectionDestroyed(connection.getID());
      }

      connections.clear();
   }

   public boolean isStarted()
   {
      return (channelFactory != null);
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
         final Connection tc = new NettyConnection(e.getChannel(), new Listener());

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

   private class Listener implements ConnectionLifeCycleListener
   {
      public void connectionCreated(final Connection connection)
      {
         if (connections.putIfAbsent(connection.getID(), connection) != null)
         {
            throw new IllegalArgumentException("Connection already exists with id " + connection.getID());
         }
      }

      public void connectionDestroyed(final Object connectionID)
      {
         if (connections.remove(connectionID) != null)
         {
            listener.connectionDestroyed(connectionID);
         }
      }

      public void connectionException(final Object connectionID, final MessagingException me)
      {
         listener.connectionException(connectionID, me);
      }
   }
}
