package org.hornetq.tests.integration.stomp;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;

import java.nio.charset.Charset;


public class StompOverHttpTest extends StompTest
{
   @Override
   protected void addChannelHandlers(SocketChannel ch)
   {
      ch.pipeline().addLast(new HttpRequestEncoder());
      ch.pipeline().addLast(new HttpResponseDecoder());
      ch.pipeline().addLast(new HttpHandler());
      ch.pipeline().addLast("decoder", new StringDecoder(Charset.forName("UTF-8")));
      ch.pipeline().addLast("encoder", new StringEncoder(Charset.forName("UTF-8")));
      ch.pipeline().addLast(new StompClientHandler());
   }

   @Override
   public String receiveFrame(long timeOut) throws Exception
   {
      //we are request/response so may need to send an empty request so we get responses piggy backed
      sendFrame(new byte[]{});
      return super.receiveFrame(timeOut);
   }

   class HttpHandler extends ChannelDuplexHandler
   {
      @Override
      public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception
      {
         if(msg instanceof DefaultLastHttpContent)
         {
            DefaultLastHttpContent response = (DefaultLastHttpContent) msg;
            ctx.fireChannelRead(response.content());
         }
      }

      @Override
      public void write(final ChannelHandlerContext ctx, final Object msg, ChannelPromise promise) throws Exception
      {
         if (msg instanceof ByteBuf)
         {
            ByteBuf buf = (ByteBuf) msg;
            FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "", buf);
            httpRequest.headers().add(HttpHeaders.Names.CONTENT_LENGTH, String.valueOf(buf.readableBytes()));
            ctx.write(httpRequest, promise);
         }
         else
         {
            ctx.write(msg, promise);
         }
      }
   }
}
