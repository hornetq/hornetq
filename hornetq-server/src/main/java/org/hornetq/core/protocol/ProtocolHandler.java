package org.hornetq.core.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.core.buffers.impl.ChannelBufferWrapper;
import org.hornetq.core.protocol.stomp.WebSocketServerHandler;
import org.hornetq.core.remoting.impl.netty.ConnectionCreator;
import org.hornetq.core.remoting.impl.netty.HornetQFrameDecoder;
import org.hornetq.core.remoting.impl.netty.HttpAcceptorHandler;
import org.hornetq.core.remoting.impl.netty.HttpKeepAliveRunnable;
import org.hornetq.core.remoting.impl.netty.NettyAcceptor;
import org.hornetq.core.remoting.impl.netty.NettyServerConnection;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.spi.core.protocol.ProtocolManager;
import org.hornetq.utils.ConfigurationHelper;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ProtocolHandler
{
   private Map<String, ProtocolManager> protocolMap;

   private NettyAcceptor nettyAcceptor;

   private Map<String, Object> configuration;

   private ScheduledExecutorService scheduledThreadPool;

   private HttpKeepAliveRunnable httpKeepAliveRunnable;

   public ProtocolHandler(Map<String, ProtocolManager> protocolMap,
                          NettyAcceptor nettyAcceptor,
                          final Map<String, Object> configuration,
                          ScheduledExecutorService scheduledThreadPool)
   {
      this.protocolMap = protocolMap;
      this.nettyAcceptor = nettyAcceptor;
      this.configuration = configuration;
      this.scheduledThreadPool = scheduledThreadPool;
   }

   public ChannelHandler getProtocolDecoder()
   {
      return new ProtocolDecoder(true, false);
   }

   public void close()
   {
      if(httpKeepAliveRunnable != null)
      {
         httpKeepAliveRunnable.close();
      }
   }

   class ProtocolDecoder extends ByteToMessageDecoder
   {
      private final boolean http;

      private final boolean httpEnabled;

      public ProtocolDecoder(boolean http, boolean httpEnabled)
      {
         this.http = http;
         this.httpEnabled = httpEnabled;
      }

      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
      {
         if(msg instanceof DefaultFullHttpRequest)
         {
            DefaultFullHttpRequest request = (DefaultFullHttpRequest) msg;
            HttpHeaders headers = request.headers();
            String upgrade = headers.get("upgrade");
            if(upgrade != null && upgrade.equalsIgnoreCase("websocket"))
            {
               ctx.pipeline().addLast("hornetq-decoder", new HornetQFrameDecoder(nettyAcceptor.getDecoder()));
               ctx.pipeline().addLast("websocket-handler", new WebSocketServerHandler());
               ctx.pipeline().addLast(new ProtocolDecoder(false, false));
               ctx.pipeline().remove(this);
               ctx.pipeline().remove("http-handler");
               ctx.fireChannelRead(msg);
            }
         }
         else
         {
            super.channelRead(ctx, msg);
         }
      }

      @Override
      protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
         // Will use the first five bytes to detect a protocol.
         if (in.readableBytes() < 8) {
            return;
         }
         final int magic1 = in.getUnsignedByte(in.readerIndex());
         final int magic2 = in.getUnsignedByte(in.readerIndex() + 1);
         if (http && isHttp(magic1, magic2))
         {
            switchToHttp(ctx);
            return;
         }
         String protocolToUse = null;
         for (String protocol : protocolMap.keySet())
         {
            ProtocolManager protocolManager = protocolMap.get(protocol);
            if(protocolManager.isProtocol(in.copy(0, 8).array()))
            {
               protocolToUse = protocol;
               break;
            }
         }
         //if we get here we assume we use the core protocol as we match nothing else
         if(protocolToUse == null)
         {
            protocolToUse = HornetQClient.DEFAULT_CORE_PROTOCOL;
         }
         ProtocolManager protocolManagerToUse = protocolMap.get(protocolToUse);
         ConnectionCreator channelHandler = nettyAcceptor.createConnectionCreator();
         ChannelPipeline pipeline = ctx.pipeline();
         protocolManagerToUse.addChannelHandlers(pipeline);
         pipeline.addLast("handler", channelHandler);
         NettyServerConnection connection = channelHandler.createConnection(ctx, protocolToUse, httpEnabled);
         protocolManagerToUse.handshake(connection, new ChannelBufferWrapper(in));
         pipeline.remove(this);
      }

      private boolean isHttp(int magic1, int magic2)
      {
         return
               magic1 == 'G' && magic2 == 'E' || // GET
                     magic1 == 'P' && magic2 == 'O' || // POST
                     magic1 == 'P' && magic2 == 'U' || // PUT
                     magic1 == 'H' && magic2 == 'E' || // HEAD
                     magic1 == 'O' && magic2 == 'P' || // OPTIONS
                     magic1 == 'P' && magic2 == 'A' || // PATCH
                     magic1 == 'D' && magic2 == 'E' || // DELETE
                     magic1 == 'T' && magic2 == 'R'; // TRACE
                     //magic1 == 'C' && magic2 == 'O'; // CONNECT
      }

      private void switchToHttp(ChannelHandlerContext ctx)
      {
         ChannelPipeline p = ctx.pipeline();
         p.addLast("http-decoder", new HttpRequestDecoder());
         p.addLast("http-aggregator", new HttpObjectAggregator(Integer.MAX_VALUE));
         p.addLast("http-encoder", new HttpResponseEncoder());
         //create it lazily if and when we need it
         if (httpKeepAliveRunnable == null)
         {
            long httpServerScanPeriod = ConfigurationHelper.getLongProperty(TransportConstants.HTTP_SERVER_SCAN_PERIOD_PROP_NAME,
                  TransportConstants.DEFAULT_HTTP_SERVER_SCAN_PERIOD,
                  configuration);
            httpKeepAliveRunnable = new HttpKeepAliveRunnable();
            Future<?> future = scheduledThreadPool.scheduleAtFixedRate(httpKeepAliveRunnable,
                  httpServerScanPeriod,
                  httpServerScanPeriod,
                  TimeUnit.MILLISECONDS);
            httpKeepAliveRunnable.setFuture(future);
         }
         long httpResponseTime = ConfigurationHelper.getLongProperty(TransportConstants.HTTP_RESPONSE_TIME_PROP_NAME,
               TransportConstants.DEFAULT_HTTP_RESPONSE_TIME,
               configuration);
         HttpAcceptorHandler httpHandler = new HttpAcceptorHandler(httpKeepAliveRunnable, httpResponseTime, ctx.channel());
         ctx.pipeline().addLast("http-handler", httpHandler);
         p.addLast(new ProtocolDecoder(false, true));
         p.remove(this);
      }
   }
}
