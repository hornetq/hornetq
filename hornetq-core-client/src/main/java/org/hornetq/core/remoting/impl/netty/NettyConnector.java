
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

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import java.net.ConnectException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.api.config.HornetQDefaultConfiguration;
import org.hornetq.api.core.HornetQException;
import org.hornetq.core.client.HornetQClientLogger;
import org.hornetq.core.client.HornetQClientMessageBundle;
import org.hornetq.core.remoting.impl.ssl.SSLSupport;
import org.hornetq.core.server.HornetQComponent;
import org.hornetq.spi.core.protocol.ProtocolType;
import org.hornetq.spi.core.remoting.AbstractConnector;
import org.hornetq.spi.core.remoting.BufferHandler;
import org.hornetq.spi.core.remoting.Connection;
import org.hornetq.spi.core.remoting.ConnectionLifeCycleListener;
import org.hornetq.utils.ConfigurationHelper;
import org.hornetq.utils.FutureLatch;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.UpstreamMessageEvent;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.http.HttpTunnelingClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientBossPool;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioWorkerPool;
import org.jboss.netty.channel.socket.oio.OioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.http.Cookie;
import org.jboss.netty.handler.codec.http.CookieDecoder;
import org.jboss.netty.handler.codec.http.CookieEncoder;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpRequestEncoder;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseDecoder;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.ssl.SslHandler;
import org.jboss.netty.util.Version;
import org.jboss.netty.util.VirtualExecutorService;

/**
 * A NettyConnector
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:tlee@redhat.com">Trustin Lee</a>
 */
public class NettyConnector extends AbstractConnector
{
   // Constants -----------------------------------------------------
   public static final String JAVAX_KEYSTORE_PATH_PROP_NAME = "javax.net.ssl.keyStore";
   public static final String JAVAX_KEYSTORE_PASSWORD_PROP_NAME = "javax.net.ssl.keyStorePassword";
   public static final String JAVAX_TRUSTSTORE_PATH_PROP_NAME = "javax.net.ssl.trustStore";
   public static final String JAVAX_TRUSTSTORE_PASSWORD_PROP_NAME = "javax.net.ssl.trustStorePassword";

   public static final String HORNETQ_KEYSTORE_PROVIDER_PROP_NAME = "org.hornetq.ssl.keyStoreProvider";
   public static final String HORNETQ_TRUSTSTORE_PROVIDER_PROP_NAME = "org.hornetq.ssl.trustStoreProvider";
   public static final String HORNETQ_KEYSTORE_PATH_PROP_NAME = "org.hornetq.ssl.keyStore";
   public static final String HORNETQ_KEYSTORE_PASSWORD_PROP_NAME = "org.hornetq.ssl.keyStorePassword";
   public static final String HORNETQ_TRUSTSTORE_PATH_PROP_NAME = "org.hornetq.ssl.trustStore";
   public static final String HORNETQ_TRUSTSTORE_PASSWORD_PROP_NAME = "org.hornetq.ssl.trustStorePassword";

   private static String CLIENT_ENABLED_SSL_PROTOCOLS = System.getProperty("org.hornetq.ssl.client.enabled.protocols", "");

   // Attributes ----------------------------------------------------

   private ClientSocketChannelFactory channelFactory;

   private ClientBootstrap bootstrap;

   private ChannelGroup channelGroup;

   private final BufferHandler handler;

   private final ConnectionLifeCycleListener listener;

   private final boolean sslEnabled;

   private final boolean httpEnabled;

   private final long httpMaxClientIdleTime;

   private final long httpClientIdleScanPeriod;

   private final boolean httpRequiresSessionId;

   private final boolean useNio;

   private final boolean useServlet;

   private final String host;

   private final int port;

   private final String localAddress;

   private final int localPort;

   private final String keyStoreProvider;

   private final String keyStorePath;

   private final String keyStorePassword;

   private final String trustStoreProvider;

   private final String trustStorePath;

   private final String trustStorePassword;

   private final boolean tcpNoDelay;

   private final int tcpSendBufferSize;

   private final int tcpReceiveBufferSize;

   private final long batchDelay;

   private final ConcurrentMap<Object, Connection> connections = new ConcurrentHashMap<Object, Connection>();

   private final String servletPath;

   private final int nioRemotingThreads;

   private final boolean useNioGlobalWorkerPool;

   private final VirtualExecutorService virtualExecutor;

   private final ScheduledExecutorService scheduledThreadPool;

   private final Executor closeExecutor;

   private BatchFlusher flusher;

   private ScheduledFuture<?> batchFlusherFuture;

   private static NioWorkerPool nioWorkerPool;

   private static NioClientBossPool nioBossPool;

   private static final Object nioWorkerPoolGuard = new Object();

   private static final AtomicInteger nioChannelFactoryCount = new AtomicInteger(0);

   private final Executor threadPool;

   private int connectTimeoutMillis;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public NettyConnector(final Map<String, Object> configuration,
                         final BufferHandler handler,
                         final ConnectionLifeCycleListener listener,
                         final Executor closeExecutor,
                         final Executor threadPool,
                         final ScheduledExecutorService scheduledThreadPool)
   {
      super(configuration);
      if (listener == null)
      {
         throw HornetQClientMessageBundle.BUNDLE.nullListener();
      }

      if (handler == null)
      {
         throw HornetQClientMessageBundle.BUNDLE.nullHandler();
      }

      this.listener = listener;

      this.handler = handler;

      sslEnabled = ConfigurationHelper.getBooleanProperty(TransportConstants.SSL_ENABLED_PROP_NAME,
                                                          TransportConstants.DEFAULT_SSL_ENABLED,
                                                          configuration);
      httpEnabled = ConfigurationHelper.getBooleanProperty(TransportConstants.HTTP_ENABLED_PROP_NAME,
                                                           TransportConstants.DEFAULT_HTTP_ENABLED,
                                                           configuration);
      servletPath = ConfigurationHelper.getStringProperty(TransportConstants.SERVLET_PATH,
                                                          TransportConstants.DEFAULT_SERVLET_PATH,
                                                          configuration);
      if (httpEnabled)
      {
         httpMaxClientIdleTime = ConfigurationHelper.getLongProperty(TransportConstants.HTTP_CLIENT_IDLE_PROP_NAME,
                                                                     TransportConstants.DEFAULT_HTTP_CLIENT_IDLE_TIME,
                                                                     configuration);
         httpClientIdleScanPeriod = ConfigurationHelper.getLongProperty(TransportConstants.HTTP_CLIENT_IDLE_SCAN_PERIOD,
                                                                        TransportConstants.DEFAULT_HTTP_CLIENT_SCAN_PERIOD,
                                                                        configuration);
         httpRequiresSessionId = ConfigurationHelper.getBooleanProperty(TransportConstants.HTTP_REQUIRES_SESSION_ID,
                                                                        TransportConstants.DEFAULT_HTTP_REQUIRES_SESSION_ID,
                                                                        configuration);
      }
      else
      {
         httpMaxClientIdleTime = 0;
         httpClientIdleScanPeriod = -1;
         httpRequiresSessionId = false;
      }

      useNio = ConfigurationHelper.getBooleanProperty(TransportConstants.USE_NIO_PROP_NAME,
                                                      TransportConstants.DEFAULT_USE_NIO_CLIENT,
                                                      configuration);

      nioRemotingThreads = ConfigurationHelper.getIntProperty(TransportConstants.NIO_REMOTING_THREADS_PROPNAME,
                                                              -1,
                                                              configuration);

      useNioGlobalWorkerPool = ConfigurationHelper.getBooleanProperty(TransportConstants.USE_NIO_GLOBAL_WORKER_POOL_PROP_NAME,
                                                                      TransportConstants.DEFAULT_USE_NIO_GLOBAL_WORKER_POOL,
                                                                      configuration);

      useServlet = ConfigurationHelper.getBooleanProperty(TransportConstants.USE_SERVLET_PROP_NAME,
                                                          TransportConstants.DEFAULT_USE_SERVLET,
                                                          configuration);
      host = ConfigurationHelper.getStringProperty(TransportConstants.HOST_PROP_NAME,
                                                   TransportConstants.DEFAULT_HOST,
                                                   configuration);
      port = ConfigurationHelper.getIntProperty(TransportConstants.PORT_PROP_NAME,
                                                TransportConstants.DEFAULT_PORT,
                                                configuration);
      localAddress = ConfigurationHelper.getStringProperty(TransportConstants.LOCAL_ADDRESS_PROP_NAME,
                                                           TransportConstants.DEFAULT_LOCAL_ADDRESS,
                                                           configuration);

      localPort = ConfigurationHelper.getIntProperty(TransportConstants.LOCAL_PORT_PROP_NAME,
                                                     TransportConstants.DEFAULT_LOCAL_PORT,
                                                     configuration);
      if (sslEnabled)
      {
         keyStoreProvider = ConfigurationHelper.getStringProperty(TransportConstants.KEYSTORE_PROVIDER_PROP_NAME,
                                                                  TransportConstants.DEFAULT_KEYSTORE_PROVIDER,
                                                                  configuration);

         keyStorePath = ConfigurationHelper.getStringProperty(TransportConstants.KEYSTORE_PATH_PROP_NAME,
                                                              TransportConstants.DEFAULT_KEYSTORE_PATH,
                                                              configuration);

         keyStorePassword = ConfigurationHelper.getPasswordProperty(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME,
                                                                    TransportConstants.DEFAULT_KEYSTORE_PASSWORD,
                                                                    configuration,
                                                                    HornetQDefaultConfiguration.getPropMaskPassword(),
                                                                    HornetQDefaultConfiguration.getPropMaskPassword());

         trustStoreProvider = ConfigurationHelper.getStringProperty(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME,
                                                                    TransportConstants.DEFAULT_TRUSTSTORE_PROVIDER,
                                                                    configuration);

         trustStorePath = ConfigurationHelper.getStringProperty(TransportConstants.TRUSTSTORE_PATH_PROP_NAME,
                                                                TransportConstants.DEFAULT_TRUSTSTORE_PATH,
                                                                configuration);

         trustStorePassword = ConfigurationHelper.getPasswordProperty(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME,
                                                                      TransportConstants.DEFAULT_TRUSTSTORE_PASSWORD,
                                                                      configuration,
                                                                      HornetQDefaultConfiguration.getPropMaskPassword(),
                                                                      HornetQDefaultConfiguration.getPropMaskPassword());
      }
      else
      {
         keyStoreProvider = null;
         keyStorePath = null;
         keyStorePassword = null;
         trustStoreProvider = null;
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

      batchDelay = ConfigurationHelper.getLongProperty(TransportConstants.BATCH_DELAY,
                                                       TransportConstants.DEFAULT_BATCH_DELAY,
                                                       configuration);

      connectTimeoutMillis = ConfigurationHelper.getIntProperty(TransportConstants.NETTY_CONNECT_TIMEOUT,
                                                                TransportConstants.DEFAULT_NETTY_CONNECT_TIMEOUT,
                                                                configuration);
      this.closeExecutor = closeExecutor;

      this.threadPool = threadPool;

      virtualExecutor = new VirtualExecutorService(threadPool);

      this.scheduledThreadPool = scheduledThreadPool;
   }

   @Override
   public String toString()
   {
      return "NettyConnector [host=" + host +
         ", port=" +
         port +
         ", httpEnabled=" +
         httpEnabled +
         ", useServlet=" +
         useServlet +
         ", servletPath=" +
         servletPath +
         ", sslEnabled=" +
         sslEnabled +
         ", useNio=" +
         useNio +
         "]";
   }

   public synchronized void start()
   {
      if (channelFactory != null)
      {
         return;
      }

      if (useNio)
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

         if (useNioGlobalWorkerPool)
         {
            synchronized (nioWorkerPoolGuard)
            {
               if (nioWorkerPool == null)
               {
                  nioWorkerPool = new NioWorkerPool(threadPool, threadsToUse, null);
               }
               if (nioBossPool == null)
               {
                  //only used for connect so 1 will do
                  nioBossPool = new NioClientBossPool(virtualExecutor, 1);
               }
               channelFactory = new NioClientSocketChannelFactory(nioBossPool, nioWorkerPool);
               nioChannelFactoryCount.incrementAndGet();
            }
         }
         else
         {
            channelFactory = new NioClientSocketChannelFactory(virtualExecutor, virtualExecutor, threadsToUse);
         }

      }
      else
      {
         channelFactory = new OioClientSocketChannelFactory(virtualExecutor);
      }
      // if we are a servlet wrap the socketChannelFactory
      if (useServlet)
      {
         ClientSocketChannelFactory proxyChannelFactory = channelFactory;
         channelFactory = new HttpTunnelingClientSocketChannelFactory(proxyChannelFactory);
      }
      bootstrap = new ClientBootstrap(channelFactory);

      bootstrap.setOption("tcpNoDelay", tcpNoDelay);

      if (connectTimeoutMillis != -1)
      {
         bootstrap.setOption("connectTimeoutMillis", connectTimeoutMillis);
      }
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

      channelGroup = new DefaultChannelGroup("hornetq-connector");

      final SSLContext context;
      if (sslEnabled)
      {
         try
         {
            // HORNETQ-680 - override the server-side config if client-side system properties are set
            String realKeyStorePath = keyStorePath;
            String realKeyStoreProvider = keyStoreProvider;
            String realKeyStorePassword = keyStorePassword;
            if (System.getProperty(JAVAX_KEYSTORE_PATH_PROP_NAME) != null)
            {
               realKeyStorePath = System.getProperty(JAVAX_KEYSTORE_PATH_PROP_NAME);
            }
            if (System.getProperty(JAVAX_KEYSTORE_PASSWORD_PROP_NAME) != null)
            {
               realKeyStorePassword = System.getProperty(JAVAX_KEYSTORE_PASSWORD_PROP_NAME);
            }
            if (System.getProperty(HORNETQ_KEYSTORE_PROVIDER_PROP_NAME) != null)
            {
               realKeyStoreProvider = System.getProperty(HORNETQ_KEYSTORE_PROVIDER_PROP_NAME);
            }
            if (System.getProperty(HORNETQ_KEYSTORE_PASSWORD_PROP_NAME) != null)
            {
               realKeyStorePassword = System.getProperty(HORNETQ_KEYSTORE_PASSWORD_PROP_NAME);
            }

            if (System.getProperty(HORNETQ_KEYSTORE_PATH_PROP_NAME) != null)
            {
               realKeyStorePath = System.getProperty(HORNETQ_KEYSTORE_PATH_PROP_NAME);
            }

            String realTrustStorePath = trustStorePath;
            String realTrustStoreProvider = trustStoreProvider;
            String realTrustStorePassword = trustStorePassword;
            if (System.getProperty(JAVAX_TRUSTSTORE_PATH_PROP_NAME) != null)
            {
               realTrustStorePath = System.getProperty(JAVAX_TRUSTSTORE_PATH_PROP_NAME);
            }
            if (System.getProperty(JAVAX_TRUSTSTORE_PASSWORD_PROP_NAME) != null)
            {
               realTrustStorePassword = System.getProperty(JAVAX_TRUSTSTORE_PASSWORD_PROP_NAME);
            }

            if (System.getProperty(HORNETQ_TRUSTSTORE_PROVIDER_PROP_NAME) != null)
            {
               realTrustStoreProvider = System.getProperty(HORNETQ_TRUSTSTORE_PROVIDER_PROP_NAME);
            }

            if (System.getProperty(HORNETQ_TRUSTSTORE_PATH_PROP_NAME) != null)
            {
               realTrustStorePath = System.getProperty(HORNETQ_TRUSTSTORE_PATH_PROP_NAME);
            }
            if (System.getProperty(HORNETQ_TRUSTSTORE_PASSWORD_PROP_NAME) != null)
            {
               realTrustStorePassword = System.getProperty(HORNETQ_TRUSTSTORE_PASSWORD_PROP_NAME);
            }

            context = SSLSupport.createContext(realKeyStoreProvider, realKeyStorePath, realKeyStorePassword, realTrustStoreProvider, realTrustStorePath, realTrustStorePassword);
         }
         catch (Exception e)
         {
            close();
            IllegalStateException ise = new IllegalStateException("Unable to create NettyConnector for " + host + ":" + port);
            ise.initCause(e);
            throw ise;
         }
      }
      else
      {
         context = null; // Unused
      }

      if (context != null && useServlet)
      {
         bootstrap.setOption("sslContext", context);
      }

      bootstrap.setPipelineFactory(new ChannelPipelineFactory()
      {
         public ChannelPipeline getPipeline() throws Exception
         {
            List<ChannelHandler> handlers = new ArrayList<ChannelHandler>();

            if (sslEnabled && !useServlet)
            {
               SSLEngine engine = context.createSSLEngine();

               List<String> disallowedProtocols = SSLSupport.setEnabledProtocols(engine, CLIENT_ENABLED_SSL_PROTOCOLS);

               for (String disallowedProtocol : disallowedProtocols)
               {
                  HornetQClientLogger.LOGGER.disallowedProtocol(disallowedProtocol);
               }

               engine.setUseClientMode(true);

               engine.setWantClientAuth(true);

               SslHandler handler = new SslHandler(engine);

               handlers.add(handler);
            }

            if (httpEnabled)
            {
               handlers.add(new HttpRequestEncoder());

               handlers.add(new HttpResponseDecoder());

               handlers.add(new HttpChunkAggregator(Integer.MAX_VALUE));

               handlers.add(new HttpHandler());
            }

            handlers.add(new HornetQFrameDecoder2());

            handlers.add(new HornetQClientChannelHandler(channelGroup, handler, new Listener()));

            ChannelPipeline pipeline = Channels.pipeline(handlers.toArray(new ChannelHandler[handlers.size()]));

            return pipeline;
         }
      });

      if (batchDelay > 0)
      {
         flusher = new BatchFlusher();

         batchFlusherFuture = scheduledThreadPool.scheduleWithFixedDelay(flusher, batchDelay, batchDelay, TimeUnit.MILLISECONDS);
      }

      HornetQClientLogger.LOGGER.debug("Started Netty Connector version " + Version.ID);
   }

   public synchronized void close()
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

      bootstrap = null;
      channelGroup.close().awaitUninterruptibly();
      // if using shared pools only release them once there are no references
      if (useNioGlobalWorkerPool)
      {
         synchronized (nioWorkerPoolGuard)
         {
            if (nioChannelFactoryCount.decrementAndGet() == 0)
            {
               closePools();
            }
         }
      }
      else
      {
         channelFactory.releaseExternalResources();
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
      return channelFactory != null;
   }

   public Connection createConnection()
   {
      if (channelFactory == null)
      {
         return null;
      }

      SocketAddress remoteDestination;
      if (useServlet)
      {
         try
         {
            URI uri = new URI("http", null, host, port, servletPath, null, null);
            bootstrap.setOption("serverName", uri.getHost());
            bootstrap.setOption("serverPath", uri.getRawPath());
         }
         catch (URISyntaxException e)
         {
            throw new IllegalArgumentException(e.getMessage());
         }
      }

      // HORNETQ-907 - strip off IPv6 scope-id (if necessary)
      remoteDestination = new InetSocketAddress(host, port);
      InetAddress inetAddress = ((InetSocketAddress)remoteDestination).getAddress();
      if (inetAddress instanceof Inet6Address)
      {
         Inet6Address inet6Address = (Inet6Address)inetAddress;
         if (inet6Address.getScopeId() != 0)
         {
            try
            {
               remoteDestination = new InetSocketAddress(InetAddress.getByAddress(inet6Address.getAddress()), ((InetSocketAddress)remoteDestination).getPort());
            }
            catch (UnknownHostException e)
            {
               throw new IllegalArgumentException(e.getMessage());
            }
         }
      }
      HornetQClientLogger.LOGGER.debug("Remote destination: " + remoteDestination);

      ChannelFuture future;
      //port 0 does not work so only use local address if set
      if (localPort != 0)
      {
         SocketAddress localDestination;
         if (localAddress != null)
         {
            localDestination = new InetSocketAddress(localAddress, localPort);
         }
         else
         {
            localDestination = new InetSocketAddress(localPort);
         }
         future = bootstrap.connect(remoteDestination, localDestination);
      }
      else
      {
         future = bootstrap.connect(remoteDestination);
      }

      future.awaitUninterruptibly();

      if (future.isSuccess())
      {
         final Channel ch = future.getChannel();
         SslHandler sslHandler = ch.getPipeline().get(SslHandler.class);
         if (sslHandler != null)
         {
            ChannelFuture handshakeFuture = sslHandler.handshake();
            if (handshakeFuture.awaitUninterruptibly(30000))
            {
               if (handshakeFuture.isSuccess())
               {
                  ChannelPipeline channelPipeline = ch.getPipeline();
                  HornetQChannelHandler channelHandler = channelPipeline.get(HornetQChannelHandler.class);
                  channelHandler.active = true;
               }
               else
               {
                  ch.close().awaitUninterruptibly();
                  HornetQClientLogger.LOGGER.errorCreatingNettyConnection(handshakeFuture.getCause());
                  return null;
               }
            }
            else
            {
               handshakeFuture.setFailure(new SSLException("Handshake was not completed in 30 seconds"));
               ch.close().awaitUninterruptibly();
               return null;
            }

         }
         else
         {
            ChannelPipeline channelPipeline = ch.getPipeline();
            HornetQChannelHandler channelHandler = channelPipeline.get(HornetQChannelHandler.class);
            channelHandler.active = true;
         }

         // No acceptor on a client connection
         Listener connectionListener = new Listener();
         NettyConnection conn = new NettyConnection(configuration, ch, connectionListener, !httpEnabled && batchDelay > 0, false);
         connectionListener.connectionCreated(null, conn, ProtocolType.CORE);

         return conn;
      }
      else
      {
         Throwable t = future.getCause();

         if (t != null && !(t instanceof ConnectException))
         {
            HornetQClientLogger.LOGGER.errorCreatingNettyConnection(future.getCause());
         }

         return null;
      }
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private static final class HornetQClientChannelHandler extends HornetQChannelHandler
   {
      HornetQClientChannelHandler(final ChannelGroup group,
                                  final BufferHandler handler,
                                  final ConnectionLifeCycleListener listener)
      {
         super(group, handler, listener);
      }
   }

   class HttpHandler extends SimpleChannelHandler
   {
      private Channel channel;

      private long lastSendTime = 0;

      private boolean waitingGet = false;

      private HttpIdleTimer task;

      private final String url;

      private final FutureLatch handShakeFuture = new FutureLatch();

      private boolean active = false;

      private boolean handshaking = false;

      private final CookieDecoder cookieDecoder = new CookieDecoder();

      private String cookie;

      private final CookieEncoder cookieEncoder = new CookieEncoder(false);

      public HttpHandler() throws Exception
      {
         url = new URI("http", null, host, port, servletPath, null, null).toString();
      }

      @Override
      public void channelConnected(final ChannelHandlerContext ctx, final ChannelStateEvent e) throws Exception
      {
         super.channelConnected(ctx, e);
         channel = e.getChannel();
         if (httpClientIdleScanPeriod > 0)
         {
            task = new HttpIdleTimer();
            java.util.concurrent.Future<?> future = scheduledThreadPool.scheduleAtFixedRate(task,
                                                                                            httpClientIdleScanPeriod,
                                                                                            httpClientIdleScanPeriod,
                                                                                            TimeUnit.MILLISECONDS);
            task.setFuture(future);
         }
      }

      @Override
      public void channelClosed(final ChannelHandlerContext ctx, final ChannelStateEvent e) throws Exception
      {
         if (task != null)
         {
            task.close();
         }

         super.channelClosed(ctx, e);
      }

      @Override
      public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) throws Exception
      {
         HttpResponse response = (HttpResponse)e.getMessage();
         if (httpRequiresSessionId && !active)
         {
            Set<Cookie> cookieMap = cookieDecoder.decode(response.getHeader(HttpHeaders.Names.SET_COOKIE));
            for (Cookie cookie : cookieMap)
            {
               if (cookie.getName().equals("JSESSIONID"))
               {
                  cookieEncoder.addCookie(cookie);
                  this.cookie = cookieEncoder.encode();
               }
            }
            active = true;
            handShakeFuture.run();
         }
         MessageEvent event = new UpstreamMessageEvent(e.getChannel(), response.getContent(), e.getRemoteAddress());
         waitingGet = false;
         ctx.sendUpstream(event);
      }

      @Override
      public void writeRequested(final ChannelHandlerContext ctx, final MessageEvent e) throws Exception
      {
         if (e.getMessage() instanceof ChannelBuffer)
         {
            if (httpRequiresSessionId && !active)
            {
               if (handshaking)
               {
                  handshaking = true;
               }
               else
               {
                  if (!handShakeFuture.await(5000))
                  {
                     throw new RuntimeException("Handshake failed after timeout");
                  }
               }
            }

            HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, url);
            httpRequest.addHeader(HttpHeaders.Names.HOST, NettyConnector.this.host);
            if (cookie != null)
            {
               httpRequest.addHeader(HttpHeaders.Names.COOKIE, cookie);
            }
            ChannelBuffer buf = (ChannelBuffer)e.getMessage();
            httpRequest.setContent(buf);
            httpRequest.addHeader(HttpHeaders.Names.CONTENT_LENGTH, String.valueOf(buf.writerIndex()));
            Channels.write(ctx, e.getFuture(), httpRequest, e.getRemoteAddress());
            lastSendTime = System.currentTimeMillis();
         }
         else
         {
            Channels.write(ctx, e.getFuture(), e.getMessage(), e.getRemoteAddress());
            lastSendTime = System.currentTimeMillis();
         }
      }

      private class HttpIdleTimer implements Runnable
      {
         private boolean closed = false;

         private java.util.concurrent.Future<?> future;

         public synchronized void run()
         {
            if (closed)
            {
               return;
            }

            if (!waitingGet && System.currentTimeMillis() > lastSendTime + httpMaxClientIdleTime)
            {
               HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, url);
               httpRequest.addHeader(HttpHeaders.Names.HOST, NettyConnector.this.host);
               waitingGet = true;
               channel.write(httpRequest);
            }
         }

         public synchronized void setFuture(final java.util.concurrent.Future<?> future)
         {
            this.future = future;
         }

         public void close()
         {
            if (future != null)
            {
               future.cancel(false);
            }

            closed = true;
         }
      }
   }

   private class Listener implements ConnectionLifeCycleListener
   {
      public void connectionCreated(final HornetQComponent component, final Connection connection, final ProtocolType protocol)
      {
         if (connections.putIfAbsent(connection.getID(), connection) != null)
         {
            throw HornetQClientMessageBundle.BUNDLE.connectionExists(connection.getID());
         }
      }

      public void connectionDestroyed(final Object connectionID)
      {
         if (connections.remove(connectionID) != null)
         {
            // Execute on different thread to avoid deadlocks
            closeExecutor.execute(new Runnable()
            {
               public void run()
               {
                  listener.connectionDestroyed(connectionID);
               }
            });
         }
      }

      public void connectionException(final Object connectionID, final HornetQException me)
      {
         // Execute on different thread to avoid deadlocks
         closeExecutor.execute(new Runnable()
         {
            public void run()
            {
               listener.connectionException(connectionID, me);
            }
         });
      }

      public void connectionReadyForWrites(Object connectionID, boolean ready)
      {
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

   public boolean isEquivalent(Map<String, Object> configuration)
   {
      //here we only check host and port because these two parameters
      //is sufficient to determine the target host
      String host = ConfigurationHelper.getStringProperty(TransportConstants.HOST_PROP_NAME,
                                                          TransportConstants.DEFAULT_HOST,
                                                          configuration);
      Integer port = ConfigurationHelper.getIntProperty(TransportConstants.PORT_PROP_NAME,
                                                        TransportConstants.DEFAULT_PORT,
                                                        configuration);

      if (!port.equals(this.port)) return false;

      if (host.equals(this.host)) return true;

      //The host may be an alias. We need to compare raw IP address.
      boolean result = false;
      try
      {
         InetAddress inetAddr1 = InetAddress.getByName(host);
         InetAddress inetAddr2 = InetAddress.getByName(this.host);
         String ip1 = inetAddr1.getHostAddress();
         String ip2 = inetAddr2.getHostAddress();
         HornetQClientLogger.LOGGER.debug(this + " host 1: " + host + " ip address: " + ip1 + " host 2: " + this.host + " ip address: " + ip2);

         result = ip1.equals(ip2);
      }
      catch (UnknownHostException e)
      {
         HornetQClientLogger.LOGGER.error("Cannot resolve host", e);
      }

      return result;
   }

   private void closePools()
   {
      if (nioBossPool != null)
      {
         nioBossPool.shutdown();
         nioBossPool = null;
      }
      if (nioWorkerPool != null)
      {
         nioWorkerPool.shutdown();
         nioWorkerPool = null;
      }
   }

   public void finalize() throws Throwable
   {
      close();
      super.finalize();
   }

   //for test purpose only
   public ClientBootstrap getBootStrap()
   {
      return bootstrap;
   }

}

