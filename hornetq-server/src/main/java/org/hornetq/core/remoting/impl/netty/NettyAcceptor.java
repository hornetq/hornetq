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

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalEventLoopGroup;
import io.netty.channel.local.LocalServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.hornetq.api.config.HornetQDefaultConfiguration;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.management.NotificationType;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.protocol.ProtocolHandler;
import org.hornetq.core.remoting.impl.ssl.SSLSupport;
import org.hornetq.core.security.HornetQPrincipal;
import org.hornetq.core.server.HornetQComponent;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.HornetQMessageBundle;
import org.hornetq.core.server.cluster.ClusterConnection;
import org.hornetq.core.server.management.Notification;
import org.hornetq.core.server.management.NotificationService;
import org.hornetq.spi.core.protocol.ProtocolManager;
import org.hornetq.spi.core.remoting.Acceptor;
import org.hornetq.spi.core.remoting.BufferDecoder;
import org.hornetq.spi.core.remoting.BufferHandler;
import org.hornetq.spi.core.remoting.Connection;
import org.hornetq.spi.core.remoting.ConnectionLifeCycleListener;
import org.hornetq.utils.ConfigurationHelper;
import org.hornetq.utils.HornetQThreadFactory;
import org.hornetq.utils.TypedProperties;

/**
 * A Netty TCP Acceptor that supports SSL
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="tlee@redhat.com">Trustin Lee</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="nmaurer@redhat.com">Norman Maurer</a>
 * @version $Rev$, $Date$
 */
public class NettyAcceptor implements Acceptor
{

   static
   {
      // Disable resource leak detection for performance reasons by default
      ResourceLeakDetector.setEnabled(false);
   }

   private final ClusterConnection clusterConnection;

   private Class<? extends ServerChannel> channelClazz;
   private EventLoopGroup eventLoopGroup;

   private volatile ChannelGroup serverChannelGroup;

   private volatile ChannelGroup channelGroup;

   private ServerBootstrap bootstrap;

   private final BufferHandler handler;

   private final BufferDecoder decoder;

   private final ConnectionLifeCycleListener listener;

   private final boolean sslEnabled;

   private final boolean useInvm;

   private final ProtocolHandler protocolHandler;

   private final String host;

   private final int port;

   private final String keyStorePath;

   private final String keyStorePassword;

   private final String trustStorePath;

   private final String trustStorePassword;

   private final boolean needClientAuth;

   private final boolean tcpNoDelay;

   private final int backlog;

   private final int tcpSendBufferSize;

   private final int tcpReceiveBufferSize;

   private final int nioRemotingThreads;

   private final ConcurrentMap<Object, NettyServerConnection> connections = new ConcurrentHashMap<Object, NettyServerConnection>();

   private final Map<String, Object> configuration;

   private final ScheduledExecutorService scheduledThreadPool;

   private NotificationService notificationService;

   private boolean paused;

   private BatchFlusher flusher;

   private ScheduledFuture<?> batchFlusherFuture;

   private final long batchDelay;

   private final boolean directDeliver;


   public NettyAcceptor(final Map<String, Object> configuration,
                        final BufferHandler handler,
                        final BufferDecoder decoder,
                        final ConnectionLifeCycleListener listener,
                        final Executor threadPool,
                        final ScheduledExecutorService scheduledThreadPool,
                        final Map<String, ProtocolManager> protocolMap)
   {
      this(null, configuration, handler, decoder, listener, threadPool, scheduledThreadPool, protocolMap);
   }


   public NettyAcceptor(final ClusterConnection clusterConnection,
                        final Map<String, Object> configuration,
                        final BufferHandler handler,
                        final BufferDecoder decoder,
                        final ConnectionLifeCycleListener listener,
                        final Executor threadPool,
                        final ScheduledExecutorService scheduledThreadPool,
                        final Map<String, ProtocolManager> protocolMap)
   {

      this.clusterConnection = clusterConnection;

      this.configuration = configuration;

      this.handler = handler;

      this.decoder = decoder;

      this.listener = listener;

      sslEnabled = ConfigurationHelper.getBooleanProperty(TransportConstants.SSL_ENABLED_PROP_NAME,
                                                          TransportConstants.DEFAULT_SSL_ENABLED,
                                                          configuration);

      nioRemotingThreads = ConfigurationHelper.getIntProperty(TransportConstants.NIO_REMOTING_THREADS_PROPNAME,
                                                              -1,
                                                              configuration);
      backlog = ConfigurationHelper.getIntProperty(TransportConstants.BACKLOG_PROP_NAME,
            -1,
            configuration);
      useInvm = ConfigurationHelper.getBooleanProperty(TransportConstants.USE_INVM_PROP_NAME,
            TransportConstants.DEFAULT_USE_INVM,
            configuration);

      this.protocolHandler = new ProtocolHandler(protocolMap, this, configuration, scheduledThreadPool);

      host = ConfigurationHelper.getStringProperty(TransportConstants.HOST_PROP_NAME,
                                                   TransportConstants.DEFAULT_HOST,
                                                   configuration);
      port = ConfigurationHelper.getIntProperty(TransportConstants.PORT_PROP_NAME,
                                                TransportConstants.DEFAULT_PORT,
                                                configuration);
      if (sslEnabled)
      {
         keyStorePath = ConfigurationHelper.getStringProperty(TransportConstants.KEYSTORE_PATH_PROP_NAME,
                                                              TransportConstants.DEFAULT_KEYSTORE_PATH,
                                                              configuration);
         keyStorePassword = ConfigurationHelper.getPasswordProperty(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME,
                                                                  TransportConstants.DEFAULT_KEYSTORE_PASSWORD,
                                                                  configuration,
                                                                  HornetQDefaultConfiguration.getPropMaskPassword(),
                                                                  HornetQDefaultConfiguration.getPropMaskPassword());
         trustStorePath = ConfigurationHelper.getStringProperty(TransportConstants.TRUSTSTORE_PATH_PROP_NAME,
                                                                TransportConstants.DEFAULT_TRUSTSTORE_PATH,
                                                                configuration);
         trustStorePassword = ConfigurationHelper.getPasswordProperty(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME,
                                                                    TransportConstants.DEFAULT_TRUSTSTORE_PASSWORD,
                                                                    configuration,
                                                                    HornetQDefaultConfiguration.getPropMaskPassword(),
                                                                    HornetQDefaultConfiguration.getPropMaskPassword());

         needClientAuth = ConfigurationHelper.getBooleanProperty(TransportConstants.NEED_CLIENT_AUTH_PROP_NAME,
                                                                 TransportConstants.DEFAULT_NEED_CLIENT_AUTH,
                                                                 configuration);
      }
      else
      {
         keyStorePath = TransportConstants.DEFAULT_KEYSTORE_PATH;
         keyStorePassword = TransportConstants.DEFAULT_KEYSTORE_PASSWORD;
         trustStorePath = TransportConstants.DEFAULT_TRUSTSTORE_PATH;
         trustStorePassword = TransportConstants.DEFAULT_TRUSTSTORE_PASSWORD;
         needClientAuth = TransportConstants.DEFAULT_NEED_CLIENT_AUTH;
      }

      tcpNoDelay = ConfigurationHelper.getBooleanProperty(TransportConstants.TCP_NODELAY_PROPNAME,
                                                          TransportConstants.DEFAULT_TCP_NODELAY,
                                                          configuration);
      tcpSendBufferSize = ConfigurationHelper.getIntProperty(TransportConstants.TCP_SENDBUFFER_SIZE_PROPNAME,
                                                             TransportConstants.DEFAULT_TCP_SENDBUFFER_SIZE,
                                                             configuration);
      tcpReceiveBufferSize = ConfigurationHelper.getIntProperty(TransportConstants.TCP_RECEIVEBUFFER_SIZE_PROPNAME,
                                                                TransportConstants.DEFAULT_TCP_RECEIVEBUFFER_SIZE,
                                                                configuration);

      this.scheduledThreadPool = scheduledThreadPool;

      batchDelay = ConfigurationHelper.getLongProperty(TransportConstants.BATCH_DELAY,
                                                       TransportConstants.DEFAULT_BATCH_DELAY,
                                                       configuration);

      directDeliver = ConfigurationHelper.getBooleanProperty(TransportConstants.DIRECT_DELIVER,
                                                             TransportConstants.DEFAULT_DIRECT_DELIVER,
                                                             configuration);
   }

   public synchronized void start() throws Exception
   {
      if (channelClazz != null)
      {
         // Already started
         return;
      }

      if (useInvm)
      {
         channelClazz = LocalServerChannel.class;
         eventLoopGroup = new LocalEventLoopGroup();
      }
      else
      {
         int threadsToUse;

         if (nioRemotingThreads == -1)
         {
            // Default to number of cores * 3

            threadsToUse = Runtime.getRuntime().availableProcessors() * 3;
         }
         else
         {
            threadsToUse = this.nioRemotingThreads;
         }
         channelClazz = NioServerSocketChannel.class;
         eventLoopGroup = new NioEventLoopGroup(threadsToUse, new HornetQThreadFactory("hornetq-netty-threads", true, getThisClassLoader()));
      }

      bootstrap = new ServerBootstrap();
      bootstrap.group(eventLoopGroup);
      bootstrap.channel(channelClazz);
      final SSLContext context;
      if (sslEnabled)
      {
         try
         {
            if (keyStorePath == null)
               throw new IllegalArgumentException("If \"" + TransportConstants.SSL_ENABLED_PROP_NAME +
                       "\" is true then \"" + TransportConstants.KEYSTORE_PATH_PROP_NAME + "\" must be non-null");
            context = SSLSupport.createContext(keyStorePath, keyStorePassword, trustStorePath, trustStorePassword);
         }
         catch (Exception e)
         {
            IllegalStateException ise = new IllegalStateException("Unable to create NettyAcceptor for " + host +
                                                                  ":" + port);
            ise.initCause(e);
            throw ise;
         }
      }
      else
      {
         context = null; // Unused
      }

      ChannelInitializer<Channel> factory = new ChannelInitializer<Channel>()
      {
         @Override
         public void initChannel(Channel channel) throws Exception
         {
            ChannelPipeline pipeline = channel.pipeline();
            if (sslEnabled)
            {
               SSLEngine engine = context.createSSLEngine();

               engine.setUseClientMode(false);

               if (needClientAuth)
                  engine.setNeedClientAuth(true);

               SslHandler handler = new SslHandler(engine);

               pipeline.addLast("ssl", handler);
            }
            pipeline.addLast(protocolHandler.getProtocolDecoder());
         }
      };
      bootstrap.childHandler(factory);

      // Bind
      bootstrap.childOption(ChannelOption.TCP_NODELAY, tcpNoDelay);
      if (tcpReceiveBufferSize != -1)
      {
         bootstrap.childOption(ChannelOption.SO_RCVBUF, tcpReceiveBufferSize);
      }
      if (tcpSendBufferSize != -1)
      {
         bootstrap.childOption(ChannelOption.SO_SNDBUF, tcpSendBufferSize);
      }
      if (backlog != -1)
      {
         bootstrap.option(ChannelOption.SO_BACKLOG, backlog);
      }
      bootstrap.option(ChannelOption.SO_REUSEADDR, true);
      bootstrap.childOption(ChannelOption.SO_REUSEADDR, true);
      bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);
      bootstrap.childOption(ChannelOption.ALLOCATOR, PartialPooledByteBufAllocator.INSTANCE);
      channelGroup = new DefaultChannelGroup("hornetq-accepted-channels", GlobalEventExecutor.INSTANCE);

      serverChannelGroup = new DefaultChannelGroup("hornetq-acceptor-channels", GlobalEventExecutor.INSTANCE);

      startServerChannels();

      paused = false;

      if (notificationService != null)
      {
         TypedProperties props = new TypedProperties();
         props.putSimpleStringProperty(new SimpleString("factory"),
                                       new SimpleString(NettyAcceptorFactory.class.getName()));
         props.putSimpleStringProperty(new SimpleString("host"), new SimpleString(host));
         props.putIntProperty(new SimpleString("port"), port);
         Notification notification = new Notification(null, NotificationType.ACCEPTOR_STARTED, props);
         notificationService.sendNotification(notification);
      }

      if (batchDelay > 0)
      {
         flusher = new BatchFlusher();

         batchFlusherFuture = scheduledThreadPool.scheduleWithFixedDelay(flusher,
                                                                         batchDelay,
                                                                         batchDelay,
                                                                         TimeUnit.MILLISECONDS);
      }

      // TODO: Think about add Version back to netty
      HornetQServerLogger.LOGGER.startedNettyAcceptor("Netty-4.0.9.Final", host, port);
   }

   private void startServerChannels()
   {
      String[] hosts = TransportConfiguration.splitHosts(host);
      for (String h : hosts)
      {
         SocketAddress address;
         if (useInvm)
         {
            address = new LocalAddress(h);
         }
         else
         {
            address = new InetSocketAddress(h, port);
         }
         Channel serverChannel = bootstrap.bind(address).syncUninterruptibly().channel();
         serverChannelGroup.add(serverChannel);
      }
   }

   public Map<String, Object> getConfiguration()
   {
      return this.configuration;
   }

   public synchronized void stop()
   {
      if (channelClazz == null)
      {
         return;
      }

      if (protocolHandler != null)
      {
         protocolHandler.close();
      }

      if (batchFlusherFuture != null)
      {
         batchFlusherFuture.cancel(false);

         flusher.cancel();

         flusher = null;

         batchFlusherFuture = null;
      }


      // serverChannelGroup has been unbound in pause()
      serverChannelGroup.close().awaitUninterruptibly();
      ChannelGroupFuture future = channelGroup.close().awaitUninterruptibly();

      if (!future.isSuccess())
      {
         HornetQServerLogger.LOGGER.nettyChannelGroupError();
         Iterator<Channel> iterator = future.group().iterator();
         while (iterator.hasNext())
         {
            Channel channel = iterator.next();
            if (channel.isActive())
            {
               HornetQServerLogger.LOGGER.nettyChannelStillOpen(channel, channel.remoteAddress());
            }
         }
      }

      eventLoopGroup.shutdown();
      eventLoopGroup = null;

      channelClazz = null;

      for (Connection connection : connections.values())
      {
         listener.connectionDestroyed(connection.getID());
      }

      connections.clear();

      if (notificationService != null)
      {
         TypedProperties props = new TypedProperties();
         props.putSimpleStringProperty(new SimpleString("factory"),
                                       new SimpleString(NettyAcceptorFactory.class.getName()));
         props.putSimpleStringProperty(new SimpleString("host"), new SimpleString(host));
         props.putIntProperty(new SimpleString("port"), port);
         Notification notification = new Notification(null, NotificationType.ACCEPTOR_STOPPED, props);
         try
         {
            notificationService.sendNotification(notification);
         }
         catch (Exception e)
         {
            // TODO Auto-generated catch block
            e.printStackTrace();
         }
      }

      paused = false;
   }

   public boolean isStarted()
   {
      return channelClazz != null;
   }

   public synchronized void pause()
   {
      if (paused)
      {
         return;
      }

      if (channelClazz == null)
      {
         return;
      }

      // We *pause* the acceptor so no new connections are made
      ChannelGroupFuture future = serverChannelGroup.close().awaitUninterruptibly();
      if (!future.isSuccess())
      {
         HornetQServerLogger.LOGGER.nettyChannelGroupBindError();
         Iterator<Channel> iterator = future.group().iterator();
         while (iterator.hasNext())
         {
            Channel channel = iterator.next();
            if (channel.isActive())
            {
               HornetQServerLogger.LOGGER.nettyChannelStillBound(channel, channel.remoteAddress());
            }
         }
      }
      paused = true;
   }

   public void setNotificationService(final NotificationService notificationService)
   {
      this.notificationService = notificationService;
   }

   /**
    * not allowed
    * @param defaultHornetQPrincipal
    */
   public void setDefaultHornetQPrincipal(HornetQPrincipal defaultHornetQPrincipal)
   {
      throw new IllegalStateException("unsecure connections not allowed");
   }

   /**
    * only InVM acceptors should allow this
    * @return
    */
   public boolean isUnsecurable()
   {
      return false;
   }

   @Override
   public ClusterConnection getClusterConnection()
   {
      return clusterConnection;
   }

   public BufferDecoder getDecoder()
   {
      return decoder;
   }

   public ConnectionCreator createConnectionCreator()
   {
      return new HornetQServerChannelHandler(channelGroup, handler, new Listener());
   }

   // Inner classes -----------------------------------------------------------------------------

   private final class HornetQServerChannelHandler extends HornetQChannelHandler implements ConnectionCreator
   {

      HornetQServerChannelHandler(final ChannelGroup group,
                                  final BufferHandler handler,
                                  final ConnectionLifeCycleListener listener)
      {
         super(group, handler, listener);
      }

      public NettyServerConnection createConnection(final ChannelHandlerContext ctx, String protocol, boolean httpEnabled) throws Exception
      {
         super.channelActive(ctx);
         Listener connectionListener = new Listener();

         NettyServerConnection nc = new NettyServerConnection(configuration, ctx.channel(), connectionListener, !httpEnabled && batchDelay > 0, directDeliver);

         connectionListener.connectionCreated(NettyAcceptor.this, nc, protocol);

         SslHandler sslHandler = ctx.pipeline().get(SslHandler.class);
         if (sslHandler != null)
         {
            sslHandler.handshakeFuture().addListener(new GenericFutureListener<io.netty.util.concurrent.Future<Channel>>()
            {
               public void operationComplete(final io.netty.util.concurrent.Future<Channel> future) throws Exception
               {
                  if (future.isSuccess())
                  {
                     active = true;
                  }
                  else
                  {
                     future.getNow().close();
                  }
               }
            });
         }
         else
         {
            active = true;
         }
         return nc;
      }
   }

   private class Listener implements ConnectionLifeCycleListener
   {
      public void connectionCreated(final HornetQComponent component, final Connection connection, final String protocol)
      {
         if (connections.putIfAbsent(connection.getID(), (NettyServerConnection)connection) != null)
         {
            throw HornetQMessageBundle.BUNDLE.connectionExists(connection.getID());
         }

         listener.connectionCreated(component, connection, protocol);
      }

      public void connectionDestroyed(final Object connectionID)
      {
         if (connections.remove(connectionID) != null)
         {
            listener.connectionDestroyed(connectionID);
         }
      }

      public void connectionException(final Object connectionID, final HornetQException me)
      {
         // Execute on different thread to avoid deadlocks
         new Thread()
         {
            @Override
            public void run()
            {
               listener.connectionException(connectionID, me);
            }
         }.start();

      }

      public void connectionReadyForWrites(final Object connectionID, boolean ready)
      {
         NettyServerConnection conn = connections.get(connectionID);

         if (conn != null)
         {
            conn.fireReady(ready);
         }
      }
   }

   private class BatchFlusher implements Runnable
   {
      private boolean cancelled;

      public synchronized void run()
      {
         if (!cancelled)
         {
            for (Connection connection : connections.values())
            {
               connection.checkFlushBatchBuffer();
            }
         }
      }

      public synchronized void cancel()
      {
         cancelled = true;
      }
   }

   private static ClassLoader getThisClassLoader()
   {
      return AccessController.doPrivileged(new PrivilegedAction<ClassLoader>()
      {
         public ClassLoader run()
         {
            return ClientSessionFactoryImpl.class.getClassLoader();
         }
      });

   }
}
