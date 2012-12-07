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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQInterruptedException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.management.NotificationType;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.protocol.stomp.WebSocketServerHandler;
import org.hornetq.core.remoting.impl.ssl.SSLSupport;
import org.hornetq.core.server.cluster.ClusterConnection;
import org.hornetq.core.server.management.Notification;
import org.hornetq.core.server.management.NotificationService;
import org.hornetq.spi.core.protocol.ProtocolType;
import org.hornetq.spi.core.remoting.Acceptor;
import org.hornetq.spi.core.remoting.BufferDecoder;
import org.hornetq.spi.core.remoting.BufferHandler;
import org.hornetq.spi.core.remoting.Connection;
import org.hornetq.spi.core.remoting.ConnectionLifeCycleListener;
import org.hornetq.utils.ConfigurationHelper;
import org.hornetq.utils.TypedProperties;
import org.hornetq.utils.VersionLoader;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.DefaultChannelPipeline;
import org.jboss.netty.channel.StaticChannelPipeline;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.local.DefaultLocalServerChannelFactory;
import org.jboss.netty.channel.local.LocalAddress;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.channel.socket.oio.OioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.ssl.SslHandler;
import org.jboss.netty.util.Version;
import org.jboss.netty.util.VirtualExecutorService;

/**
 * A Netty TCP Acceptor that supports SSL
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="tlee@redhat.com">Trustin Lee</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * @version $Rev$, $Date$
 */
public class NettyAcceptor implements Acceptor
{
   static final Logger log = Logger.getLogger(NettyAcceptor.class);

   private ClusterConnection clusterConnection;

   private ChannelFactory channelFactory;

   private volatile ChannelGroup serverChannelGroup;

   private volatile ChannelGroup channelGroup;

   private ServerBootstrap bootstrap;

   private final BufferHandler handler;

   private final BufferDecoder decoder;

   private final ConnectionLifeCycleListener listener;

   private final boolean sslEnabled;

   private final boolean httpEnabled;

   private final long httpServerScanPeriod;

   private final long httpResponseTime;

   private final boolean useNio;

   private final boolean useInvm;

   private final ProtocolType protocol;

   private final String host;

   private final int port;

   private final String keyStorePath;

   private final String keyStorePassword;

   private final String trustStorePath;

   private final String trustStorePassword;

   private final boolean tcpNoDelay;

   private final int tcpSendBufferSize;

   private final int tcpReceiveBufferSize;

   private final int nioRemotingThreads;

   private final HttpKeepAliveRunnable httpKeepAliveRunnable;

   private HttpAcceptorHandler httpHandler = null;

   private final ConcurrentMap<Object, NettyConnection> connections = new ConcurrentHashMap<Object, NettyConnection>();

   private final Map<String, Object> configuration;

   private final Executor threadPool;

   private final ScheduledExecutorService scheduledThreadPool;

   private NotificationService notificationService;

   private VirtualExecutorService bossExecutor;

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
                        final ScheduledExecutorService scheduledThreadPool)
   {
      this(null, configuration, handler, decoder, listener, threadPool, scheduledThreadPool);
   }


   public NettyAcceptor(final ClusterConnection clusterConnection,
                        final Map<String, Object> configuration,
                        final BufferHandler handler,
                        final BufferDecoder decoder,
                        final ConnectionLifeCycleListener listener,
                        final Executor threadPool,
                        final ScheduledExecutorService scheduledThreadPool)
   {

      this.clusterConnection = clusterConnection;

      this.configuration = configuration;

      this.handler = handler;

      this.decoder = decoder;

      this.listener = listener;

      sslEnabled = ConfigurationHelper.getBooleanProperty(TransportConstants.SSL_ENABLED_PROP_NAME,
                                                          TransportConstants.DEFAULT_SSL_ENABLED,
                                                          configuration);

      httpEnabled = ConfigurationHelper.getBooleanProperty(TransportConstants.HTTP_ENABLED_PROP_NAME,
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
         httpKeepAliveRunnable = new HttpKeepAliveRunnable();
         Future<?> future = scheduledThreadPool.scheduleAtFixedRate(httpKeepAliveRunnable,
                                                                    httpServerScanPeriod,
                                                                    httpServerScanPeriod,
                                                                    TimeUnit.MILLISECONDS);
         httpKeepAliveRunnable.setFuture(future);
      }
      else
      {
         httpServerScanPeriod = 0;
         httpResponseTime = 0;
         httpKeepAliveRunnable = null;
      }
      useNio = ConfigurationHelper.getBooleanProperty(TransportConstants.USE_NIO_PROP_NAME,
                                                      TransportConstants.DEFAULT_USE_NIO_SERVER,
                                                      configuration);

      nioRemotingThreads = ConfigurationHelper.getIntProperty(TransportConstants.NIO_REMOTING_THREADS_PROPNAME,
                                                              -1,
                                                              configuration);

      useInvm = ConfigurationHelper.getBooleanProperty(TransportConstants.USE_INVM_PROP_NAME,
                                                       TransportConstants.DEFAULT_USE_INVM,
                                                       configuration);
      String protocolStr = ConfigurationHelper.getStringProperty(TransportConstants.PROTOCOL_PROP_NAME,
                                                                 TransportConstants.DEFAULT_PROTOCOL,
                                                                 configuration);
      protocol = ProtocolType.valueOf(protocolStr.toUpperCase());

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
                                                                  configuration);
         trustStorePath = ConfigurationHelper.getStringProperty(TransportConstants.TRUSTSTORE_PATH_PROP_NAME,
                                                                TransportConstants.DEFAULT_TRUSTSTORE_PATH,
                                                                configuration);
         trustStorePassword = ConfigurationHelper.getPasswordProperty(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME,
                                                                    TransportConstants.DEFAULT_TRUSTSTORE_PASSWORD,
                                                                    configuration);
      }
      else
      {
         keyStorePath = null;
         keyStorePassword = null;
         trustStorePath = null;
         trustStorePassword = null;
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

      this.threadPool = threadPool;

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
      if (channelFactory != null)
      {
         // Already started
         return;
      }

      bossExecutor = new VirtualExecutorService(threadPool);
      VirtualExecutorService workerExecutor = new VirtualExecutorService(threadPool);

      if (useInvm)
      {
         channelFactory = new DefaultLocalServerChannelFactory();
      }
      else if (useNio)
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

         channelFactory = new NioServerSocketChannelFactory(bossExecutor, workerExecutor, threadsToUse);
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
            Map<String, ChannelHandler> handlers = new LinkedHashMap<String, ChannelHandler>();

            if (sslEnabled)
            {
               SSLEngine engine = context.createSSLEngine();

               engine.setUseClientMode(false);

               SslHandler handler = new SslHandler(engine);

               handlers.put("ssl", handler);
            }

            if (httpEnabled)
            {
               handlers.put("http-decoder", new HttpRequestDecoder());

               handlers.put("http-aggregator", new HttpChunkAggregator(Integer.MAX_VALUE));

               handlers.put("http-encoder", new HttpResponseEncoder());

               httpHandler = new HttpAcceptorHandler(httpKeepAliveRunnable, httpResponseTime);
               handlers.put("http-handler", httpHandler);
            }

            if (protocol == ProtocolType.CORE)
            {
               // Core protocol uses its own optimised decoder

               handlers.put("hornetq-decoder", new HornetQFrameDecoder2());
            }
            else if (protocol == ProtocolType.STOMP_WS)
            {
               handlers.put("http-decoder", new HttpRequestDecoder());
               handlers.put("http-aggregator", new HttpChunkAggregator(65536));
               handlers.put("http-encoder", new HttpResponseEncoder());
               handlers.put("hornetq-decoder", new HornetQFrameDecoder(decoder));
               handlers.put("websocket-handler", new WebSocketServerHandler());
            }
            else if (protocol == ProtocolType.STOMP)
            {
               //With STOMP the decoding is handled in the StompFrame class
            }
            else
            {
               handlers.put("hornetq-decoder", new HornetQFrameDecoder(decoder));
            }

            handlers.put("handler", new HornetQServerChannelHandler(channelGroup, handler, new Listener()));

            /**
             * STOMP_WS protocol mandates use of named handlers to be able to replace http codecs
             * by websocket codecs after handshake.
             * Other protocols can use a faster static channel pipeline directly.
             */
            ChannelPipeline pipeline;
            if (protocol == ProtocolType.STOMP_WS)
            {
               pipeline = new DefaultChannelPipeline();
               for (Entry<String, ChannelHandler> handler : handlers.entrySet())
               {
                  pipeline.addLast(handler.getKey(), handler.getValue());
               }
            }
            else
            {
               pipeline = new StaticChannelPipeline(handlers.values().toArray(new ChannelHandler[handlers.size()]));
            }

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

      channelGroup = new DefaultChannelGroup("hornetq-accepted-channels");

      serverChannelGroup = new DefaultChannelGroup("hornetq-acceptor-channels");

      startServerChannels();

      paused = false;

      if (!Version.ID.equals(VersionLoader.getVersion().getNettyVersion()))
      {
         NettyAcceptor.log.warn("Unexpected Netty Version was expecting " + VersionLoader.getVersion()
                                                                                         .getNettyVersion() +
                                " using " +
                                Version.ID);
      }

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

      NettyAcceptor.log.info("Started Netty Acceptor version " + Version.ID +
                             " " +
                             host +
                             ":" +
                             port +
                             " for " +
                             protocol +
                             " protocol");
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
         Channel serverChannel = bootstrap.bind(address);
         serverChannelGroup.add(serverChannel);
      }
   }

   public Map<String, Object> getConfiguration()
   {
      return this.configuration;
   }

   public synchronized void stop()
   {
      if (channelFactory == null)
      {
         return;
      }

      if (batchFlusherFuture != null)
      {
         batchFlusherFuture.cancel(false);

         flusher.cancel();

         flusher = null;

         batchFlusherFuture = null;
      }

      serverChannelGroup.close().awaitUninterruptibly();

      if (httpKeepAliveRunnable != null)
      {
         httpKeepAliveRunnable.close();
      }

      // serverChannelGroup has been unbound in pause()
      serverChannelGroup.close().awaitUninterruptibly();
      ChannelGroupFuture future = channelGroup.close().awaitUninterruptibly();

      if (!future.isCompleteSuccess())
      {
         NettyAcceptor.log.warn("channel group did not completely close");
         Iterator<Channel> iterator = future.getGroup().iterator();
         while (iterator.hasNext())
         {
            Channel channel = iterator.next();
            if (channel.isBound())
            {
               NettyAcceptor.log.warn(channel + " is still connected to " + channel.getRemoteAddress());
            }
         }
      }

      channelFactory.releaseExternalResources();
      channelFactory = null;

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

      if (httpHandler != null)
      {
         httpHandler.shutdown();
      }

      paused = false;
   }

   public boolean isStarted()
   {
      return channelFactory != null;
   }

   public void pause()
   {
      if (paused)
      {
         return;
      }

      if (channelFactory == null)
      {
         return;
      }

      // We *pause* the acceptor so no new connections are made
      ChannelGroupFuture future = serverChannelGroup.unbind().awaitUninterruptibly();
      if (!future.isCompleteSuccess())
      {
         NettyAcceptor.log.warn("server channel group did not completely unbind");
         Iterator<Channel> iterator = future.getGroup().iterator();
         while (iterator.hasNext())
         {
            Channel channel = iterator.next();
            if (channel.isBound())
            {
               NettyAcceptor.log.warn(channel + " is still bound to " + channel.getRemoteAddress());
            }
         }
      }
      // TODO remove workaround when integrating Netty 3.2.x
      // https://jira.jboss.org/jira/browse/NETTY-256
      bossExecutor.shutdown();
      try
      {
         bossExecutor.awaitTermination(30, TimeUnit.SECONDS);
      }
      catch (InterruptedException e)
      {
         throw new HornetQInterruptedException(e);
      }

      paused = true;
   }

   public void setNotificationService(final NotificationService notificationService)
   {
      this.notificationService = notificationService;
   }

   /* (non-Javadoc)
    * @see org.hornetq.spi.core.remoting.Acceptor#getClusterConnection()
    */
   public ClusterConnection getClusterConnection()
   {
      return clusterConnection;
   }

   // Inner classes -----------------------------------------------------------------------------

   private final class HornetQServerChannelHandler extends HornetQChannelHandler
   {
      HornetQServerChannelHandler(final ChannelGroup group,
                                  final BufferHandler handler,
                                  final ConnectionLifeCycleListener listener)
      {
         super(group, handler, listener);
      }

      @Override
      public void channelConnected(final ChannelHandlerContext ctx, final ChannelStateEvent e) throws Exception
      {
         new NettyConnection(configuration, NettyAcceptor.this, e.getChannel(), new Listener(), !httpEnabled && batchDelay > 0, directDeliver);

         SslHandler sslHandler = ctx.getPipeline().get(SslHandler.class);
         if (sslHandler != null)
         {
            sslHandler.handshake().addListener(new ChannelFutureListener()
            {
               public void operationComplete(final ChannelFuture future) throws Exception
               {
                  if (future.isSuccess())
                  {
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
            active = true;
         }
      }
   }

   private class Listener implements ConnectionLifeCycleListener
   {
      public void connectionCreated(final Acceptor acceptor, final Connection connection, final ProtocolType protocol)
      {
         if (connections.putIfAbsent(connection.getID(), (NettyConnection)connection) != null)
         {
            throw new IllegalArgumentException("Connection already exists with id " + connection.getID());
         }

         listener.connectionCreated(acceptor, connection, NettyAcceptor.this.protocol);
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
         NettyConnection conn = connections.get(connectionID);

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
}
