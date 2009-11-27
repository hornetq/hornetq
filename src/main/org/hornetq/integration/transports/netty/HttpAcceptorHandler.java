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
package org.hornetq.integration.transports.netty;

import static org.jboss.netty.channel.Channels.write;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.UpstreamMessageEvent;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;

/**
 * takes care of making sure that every request has a response and also that any uninitiated responses always wait for a response.
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
@ChannelPipelineCoverage("one")
class HttpAcceptorHandler extends SimpleChannelHandler
{
   private final BlockingQueue<ResponseHolder> responses = new LinkedBlockingQueue<ResponseHolder>();

   private final BlockingQueue<Runnable> delayedResponses = new LinkedBlockingQueue<Runnable>();

   private final Executor executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, delayedResponses);

   private final HttpKeepAliveRunnable httpKeepAliveTask;

   private final long responseTime;

   private Channel channel;

   public HttpAcceptorHandler(final HttpKeepAliveRunnable httpKeepAliveTask, final long responseTime)
   {
      super();
      this.responseTime = responseTime;
      this.httpKeepAliveTask = httpKeepAliveTask;
   }

   @Override
   public void channelConnected(final ChannelHandlerContext ctx, final ChannelStateEvent e) throws Exception
   {
      super.channelConnected(ctx, e);
      channel = e.getChannel();
      httpKeepAliveTask.registerKeepAliveHandler(this);
   }

   @Override
   public void channelDisconnected(final ChannelHandlerContext ctx, final ChannelStateEvent e) throws Exception
   {
      super.channelDisconnected(ctx, e);
      httpKeepAliveTask.unregisterKeepAliveHandler(this);
      channel = null;
   }

   @Override
   public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) throws Exception
   {
      HttpRequest request = (HttpRequest)e.getMessage();
      HttpMethod method = request.getMethod();
      // if we are a post then we send upstream, otherwise we are just being prompted for a response.
      if (method.equals(HttpMethod.POST))
      {
         MessageEvent event = new UpstreamMessageEvent(e.getChannel(), request.getContent(), e.getRemoteAddress());
         ctx.sendUpstream(event);
      }
      // add a new response
      responses.put(new ResponseHolder(System.currentTimeMillis() + responseTime,
                                       new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)));
   }

   @Override
   public void writeRequested(final ChannelHandlerContext ctx, final MessageEvent e) throws Exception
   {
      // we are either a channel buffer, which gets delayed until a response is available, or we are the actual response
      if (e.getMessage() instanceof ChannelBuffer)
      {
         ChannelBuffer buf = (ChannelBuffer)e.getMessage();
         executor.execute(new ResponseRunner(buf));
      }
      else
      {
         write(ctx, e.getFuture(), e.getMessage(), e.getRemoteAddress());
      }
   }

   public void keepAlive(final long time)
   {
      // send some responses to catch up thus avoiding any timeout.
      int lateResponses = 0;
      for (ResponseHolder response : responses)
      {
         if (response.timeReceived < time)
         {
            lateResponses++;
         }
         else
         {
            break;
         }
      }
      for (int i = 0; i < lateResponses; i++)
      {
         executor.execute(new ResponseRunner());
      }
   }

   /**
    * this is prompted to delivery when a response is available in the response queue.
    */
   class ResponseRunner implements Runnable
   {
      private final ChannelBuffer buffer;

      private final boolean bogusResponse;

      public ResponseRunner(final ChannelBuffer buffer)
      {
         this.buffer = buffer;
         bogusResponse = false;
      }

      public ResponseRunner()
      {
         bogusResponse = true;
         buffer = ChannelBuffers.buffer(0);
      }

      public void run()
      {
         ResponseHolder responseHolder = null;
         do
         {
            try
            {
               responseHolder = responses.take();
            }
            catch (InterruptedException e)
            {
               // ignore, we'll just try again
            }
         }
         while (responseHolder == null);
         if (!bogusResponse)
         {
            ChannelBuffer piggyBackBuffer = piggyBackResponses();
            responseHolder.response.setContent(piggyBackBuffer);
            responseHolder.response.addHeader(HttpHeaders.Names.CONTENT_LENGTH,
                                              String.valueOf(piggyBackBuffer.writerIndex()));
            channel.write(responseHolder.response);
         }
         else
         {
            responseHolder.response.setContent(buffer);
            responseHolder.response.addHeader(HttpHeaders.Names.CONTENT_LENGTH, String.valueOf(buffer.writerIndex()));
            channel.write(responseHolder.response);
         }

      }

      private ChannelBuffer piggyBackResponses()
      {
         // if we are the last available response then we have to piggy back any remaining responses
         if (responses.isEmpty())
         {
            ChannelBuffer buf = org.jboss.netty.buffer.ChannelBuffers.dynamicBuffer();
            buf.writeBytes(buffer);
            do
            {
               try
               {
                  ResponseRunner responseRunner = (ResponseRunner)delayedResponses.poll(0, TimeUnit.MILLISECONDS);
                  if (responseRunner == null)
                  {
                     break;
                  }
                  buf.writeBytes(responseRunner.buffer);
               }
               catch (InterruptedException e)
               {
                  break;
               }
            }
            while (responses.isEmpty());
            return buf;
         }
         return buffer;
      }

   }

   /**
    * a holder class so we know what time  the request first arrived
    */
   private class ResponseHolder
   {
      final HttpResponse response;

      final long timeReceived;

      public ResponseHolder(final long timeReceived, final HttpResponse response)
      {
         this.timeReceived = timeReceived;
         this.response = response;
      }
   }

}
