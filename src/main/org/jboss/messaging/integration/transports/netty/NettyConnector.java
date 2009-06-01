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
import static org.jboss.netty.channel.Channels.write;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.impl.ssl.SSLSupport;
import org.jboss.messaging.core.remoting.spi.BufferHandler;
import org.jboss.messaging.core.remoting.spi.Connection;
import org.jboss.messaging.core.remoting.spi.ConnectionLifeCycleListener;
import org.jboss.messaging.core.remoting.spi.Connector;
import org.jboss.messaging.utils.ConfigurationHelper;
import org.jboss.messaging.utils.Future;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.UpstreamMessageEvent;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.http.HttpTunnelAddress;
import org.jboss.netty.channel.socket.http.HttpTunnelingClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.oio.OioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.http.Cookie;
import org.jboss.netty.handler.codec.http.CookieDecoder;
import org.jboss.netty.handler.codec.http.CookieEncoder;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpRequestEncoder;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseDecoder;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.ssl.SslHandler;

/**
 * A NettyConnector
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:tlee@redhat.com">Trustin Lee</a>
 */
public class NettyConnector implements Connector
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(NettyConnector.class);

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

   private final String keyStorePath;

   private final String keyStorePassword;

   private final boolean tcpNoDelay;

   private final int tcpSendBufferSize;

   private final int tcpReceiveBufferSize;

   private ConcurrentMap<Object, Connection> connections = new ConcurrentHashMap<Object, Connection>();

   private final String servletPath;
   
   private final Executor threadPool;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public NettyConnector(final Map<String, Object> configuration,
                         final BufferHandler handler,
                         final ConnectionLifeCycleListener listener,
                         final Executor threadPool)
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
      this.httpEnabled = ConfigurationHelper.getBooleanProperty(TransportConstants.HTTP_ENABLED_PROP_NAME,
                                                                TransportConstants.DEFAULT_HTTP_ENABLED,
                                                                configuration);
      servletPath = ConfigurationHelper.getStringProperty(TransportConstants.SERVLET_PATH,
                                                          TransportConstants.DEFAULT_SERVLET_PATH,
                                                          configuration);
      if (httpEnabled)
      {
         this.httpMaxClientIdleTime = ConfigurationHelper.getLongProperty(TransportConstants.HTTP_CLIENT_IDLE_PROP_NAME,
                                                                          TransportConstants.DEFAULT_HTTP_CLIENT_IDLE_TIME,
                                                                          configuration);
         this.httpClientIdleScanPeriod = ConfigurationHelper.getLongProperty(TransportConstants.HTTP_CLIENT_IDLE_SCAN_PERIOD,
                                                                             TransportConstants.DEFAULT_HTTP_CLIENT_SCAN_PERIOD,
                                                                             configuration);
         this.httpRequiresSessionId = ConfigurationHelper.getBooleanProperty(TransportConstants.HTTP_REQUIRES_SESSION_ID,
                                                                             TransportConstants.DEFAULT_HTTP_REQUIRES_SESSION_ID,
                                                                             configuration);
      }
      else
      {
         this.httpMaxClientIdleTime = 0;
         this.httpClientIdleScanPeriod = -1;
         this.httpRequiresSessionId = false;
      }

      this.useNio = ConfigurationHelper.getBooleanProperty(TransportConstants.USE_NIO_PROP_NAME,
                                                           TransportConstants.DEFAULT_USE_NIO,
                                                           configuration);
      this.useServlet = ConfigurationHelper.getBooleanProperty(TransportConstants.USE_SERVLET_PROP_NAME,
                                                               TransportConstants.DEFAULT_USE_SERVLET,
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
      
      this.threadPool = threadPool;
   }

   public synchronized void start()
   {
      if (channelFactory != null)
      {
         return;
      }   
      
      if (useNio)
      {    
         channelFactory = new NioClientSocketChannelFactory(threadPool, threadPool);
      }
      else
      {
         channelFactory = new OioClientSocketChannelFactory(threadPool);
      }
      // if we are a servlet wrap the socketChannelFactory
      if (useServlet)
      {
         ClientSocketChannelFactory proxyChannelFactory = channelFactory;
         channelFactory = new HttpTunnelingClientSocketChannelFactory(proxyChannelFactory, threadPool);
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

      channelGroup = new DefaultChannelGroup("jbm-connector");

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
            if (httpEnabled)
            {
               pipeline.addLast("httpRequestEncoder", new HttpRequestEncoder());
               pipeline.addLast("httpResponseDecoder", new HttpResponseDecoder());
               pipeline.addLast("httphandler", new HttpHandler());
            }
            ChannelPipelineSupport.addCodecFilter(pipeline, handler);
            pipeline.addLast("handler", new MessagingClientChannelHandler(channelGroup, handler, new Listener()));
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
      channelGroup.close().awaitUninterruptibly();
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

   public Connection createConnection()
   {
      if (channelFactory == null)
      {
         return null;
      }
      SocketAddress address;
      if (useServlet)
      {
         try
         {
            URI uri = new URI("http", null, host, port, servletPath, null, null);
            address = new HttpTunnelAddress(uri);
         }
         catch (URISyntaxException e)
         {
            throw new IllegalArgumentException(e.getMessage());
         }
      }
      else
      {
         address = new InetSocketAddress(host, port);
      }
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

         NettyConnection conn = new NettyConnection(ch, new Listener());

         return conn;
      }
      else
      {
         Throwable t = future.getCause();
         
         if (t != null && !(t instanceof ConnectException))
         {
            log.error("Failed to create netty connection", future.getCause());
         }
         
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
      MessagingClientChannelHandler(ChannelGroup group, BufferHandler handler, ConnectionLifeCycleListener listener)
      {
         super(group, handler, listener);
      }
   }

   @ChannelPipelineCoverage("one")
   class HttpHandler extends SimpleChannelHandler
   {
      private Channel channel;

      private long lastSendTime = 0;

      private boolean waitingGet = false;

      private Timer idleClientTimer;

      private HttpIdleTimerTask task;

      private String url = "http://" + host + ":" + port + servletPath;

      private Future handShakeFuture = new Future();

      private boolean active = false;

      private boolean handshaking = false;

      private CookieDecoder cookieDecoder = new CookieDecoder();

      private String cookie;

      private CookieEncoder cookieEncoder = new CookieEncoder(false);

      public void channelConnected(final ChannelHandlerContext ctx, final ChannelStateEvent e) throws Exception
      {
         super.channelConnected(ctx, e);
         channel = e.getChannel();
         if (httpClientIdleScanPeriod > 0)
         {
            idleClientTimer = new Timer("Http Idle Timer", true);
            task = new HttpIdleTimerTask();
            idleClientTimer.schedule(task, httpClientIdleScanPeriod, httpClientIdleScanPeriod);
         }
      }

      public void channelClosed(final ChannelHandlerContext ctx, final ChannelStateEvent e) throws Exception
      {
         if (idleClientTimer != null)
         {
            task.cancel();

            idleClientTimer.cancel();
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
            if (cookie != null)
            {
               httpRequest.addHeader(HttpHeaders.Names.COOKIE, cookie);
            }
            ChannelBuffer buf = (ChannelBuffer)e.getMessage();
            httpRequest.setContent(buf);
            httpRequest.addHeader(HttpHeaders.Names.CONTENT_LENGTH, String.valueOf(buf.writerIndex()));
            write(ctx, e.getFuture(), httpRequest, e.getRemoteAddress());
            lastSendTime = System.currentTimeMillis();
         }
         else
         {
            write(ctx, e.getFuture(), e.getMessage(), e.getRemoteAddress());
            lastSendTime = System.currentTimeMillis();
         }
      }

      private class HttpIdleTimerTask extends TimerTask
      {
         long currentTime = System.currentTimeMillis();

         public synchronized void run()
         {
            if (!waitingGet && System.currentTimeMillis() > lastSendTime + httpMaxClientIdleTime)
            {
               HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, url);
               waitingGet = true;
               channel.write(httpRequest);
            }
         }

         public synchronized boolean cancel()
         {
            return super.cancel();
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
            // Execute on different thread to avoid deadlocks
            new Thread()
            {
               public void run()
               {
                  listener.connectionDestroyed(connectionID);
               }
            }.start();
         }
      }

      public void connectionException(final Object connectionID, final MessagingException me)
      {
         // Execute on different thread to avoid deadlocks
         new Thread()
         {
            public void run()
            {
               listener.connectionException(connectionID, me);
            }
         }.start();
      }
   }

}
