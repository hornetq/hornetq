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

import static org.jboss.netty.channel.Channels.pipeline;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.impl.ssl.SSLSupport;
import org.jboss.messaging.core.remoting.spi.BufferHandler;
import org.jboss.messaging.core.remoting.spi.Connection;
import org.jboss.messaging.core.remoting.spi.ConnectionLifeCycleListener;
import org.jboss.messaging.core.remoting.spi.Connector;
import org.jboss.messaging.util.ConfigurationHelper;
import org.jboss.messaging.util.JBMThreadFactory;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.oio.OioClientSocketChannelFactory;
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

   private ChannelFactory channelFactory;

   private ClientBootstrap bootstrap;

   private final BufferHandler handler;

   private final ConnectionLifeCycleListener listener;

   private final boolean sslEnabled;

   private final boolean useNio;

   private final String host;

   private final int port;

   private final String keyStorePath;

   private final String keyStorePassword;

   private final boolean tcpNoDelay;

   private final int tcpSendBufferSize;

   private final int tcpReceiveBufferSize;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public NettyConnector(final Map<String, Object> configuration,
                         final BufferHandler handler,
                         final ConnectionLifeCycleListener listener)
   {
      if (listener == null)
      {
         throw new IllegalArgumentException("Invalid argument null listener");
      }

      if (handler == null)
      {
         throw new IllegalArgumentException("Invalid argument null handler");
      }

      this.listener = listener;

      this.handler = handler;

      this.sslEnabled = ConfigurationHelper.getBooleanProperty(TransportConstants.SSL_ENABLED_PROP_NAME,
                                                               TransportConstants.DEFAULT_SSL_ENABLED,
                                                               configuration);
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
      }
      else
      {
         this.keyStorePath = null;
         this.keyStorePassword = null;
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

   public synchronized void start()
   {
      if (channelFactory != null)
      {
         return;
      }

      workerExecutor = Executors.newCachedThreadPool(new JBMThreadFactory("jbm-netty-connector-worker-threads"));
      if (useNio)
      {
         bossExecutor = Executors.newCachedThreadPool(new JBMThreadFactory("jbm-netty-connector-boss-threads"));
         channelFactory = new NioClientSocketChannelFactory(bossExecutor, workerExecutor);
      }
      else
      {
         channelFactory = new OioClientSocketChannelFactory(workerExecutor);
      }
      bootstrap = new ClientBootstrap(channelFactory);

      bootstrap.setOption("tcpNoDelay", tcpNoDelay);
      if (tcpReceiveBufferSize != -1)
      {
         bootstrap.setOption("receiveBufferSize", tcpReceiveBufferSize);
      }
      if (tcpSendBufferSize != -1)
      {
         bootstrap.setOption("sendBufferSize", tcpSendBufferSize);
      }
      bootstrap.setOption("keepAlive", true);
      bootstrap.setOption("reuseAddress", true);

      final SSLContext context;
      if (sslEnabled)
      {
         try
         {
            context = SSLSupport.getInstance(true, keyStorePath, keyStorePassword, null, null);
         }
         catch (Exception e)
         {
            close();
            IllegalStateException ise = new IllegalStateException("Unable to create NettyConnector for " + host);
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
               ChannelPipelineSupport.addSSLFilter(pipeline, context, true);
            }
            ChannelPipelineSupport.addCodecFilter(pipeline, handler);
            pipeline.addLast("handler", new MessagingClientChannelHandler(handler, listener));
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
      if (bossExecutor != null)
      {
         bossExecutor.shutdown();
      }
      workerExecutor.shutdown();
      if (bossExecutor != null)
      {
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
      }
   }

   public Connection createConnection()
   {
      if (channelFactory == null)
      {
         return null;
      }

      InetSocketAddress address = new InetSocketAddress(host, port);
      ChannelFuture future = bootstrap.connect(address);
      future.awaitUninterruptibly();

      if (future.isSuccess())
      {
         final Channel ch = future.getChannel();
         SslHandler sslHandler = ch.getPipeline().get(SslHandler.class);
         if (sslHandler != null)
         {
            try
            {
               ChannelFuture handshakeFuture = sslHandler.handshake(ch);
               handshakeFuture.awaitUninterruptibly();
               if (handshakeFuture.isSuccess())
               {
                  ch.getPipeline().get(MessagingChannelHandler.class).active = true;
               }
               else
               {
                  ch.close().awaitUninterruptibly();
                  return null;
               }
            }
            catch (SSLException e)
            {
               ch.close();
               return null;
            }
         }
         else
         {
            ch.getPipeline().get(MessagingChannelHandler.class).active = true;
         }

         return new NettyConnection(ch);
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
   private final class MessagingClientChannelHandler extends MessagingChannelHandler
   {
      MessagingClientChannelHandler(BufferHandler handler, ConnectionLifeCycleListener listener)
      {
         super(handler, listener);
      }
   }
}
