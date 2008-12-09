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

import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.DefaultMessageEvent;
import static org.jboss.netty.channel.Channels.write;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpHeaders;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

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

   private final HttpKeepAliveTask httpKeepAliveTask;

   private final long responseTime;

   private Channel channel;

   public HttpAcceptorHandler(final HttpKeepAliveTask httpKeepAliveTask, long responseTime)
   {
      super();
      this.responseTime = responseTime;
      this.httpKeepAliveTask = httpKeepAliveTask;
   }

   public void channelConnected(final ChannelHandlerContext ctx, final ChannelStateEvent e) throws Exception
   {
      super.channelConnected(ctx, e);
      channel = e.getChannel();
      httpKeepAliveTask.registerKeepAliveHandler(this);
   }

   public void channelDisconnected(final ChannelHandlerContext ctx, final ChannelStateEvent e) throws Exception
   {
      super.channelDisconnected(ctx, e);
      httpKeepAliveTask.unregisterKeepAliveHandler(this);
      channel = null;
   }

   @Override
   public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) throws Exception
   {
      HttpRequest request = (HttpRequest) e.getMessage();
      HttpMethod method = request.getMethod();
      //if we are a post then we send upstream, otherwise we are just being prompted for a response.
      if (method.equals(HttpMethod.POST))
      {
         MessageEvent event = new DefaultMessageEvent(e.getChannel(), e.getFuture(), request.getContent(), e.getRemoteAddress());
         ctx.sendUpstream(event);
      }
      //add a new response
      responses.put(new ResponseHolder(System.currentTimeMillis() + responseTime, new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)));
   }

   @Override
   public void writeRequested(final ChannelHandlerContext ctx, final MessageEvent e) throws Exception
   {
      //we are either a channel buffer, which gets delayed until a response is available, or we are the actual response
      if (e.getMessage() instanceof ChannelBuffer)
      {
         ChannelBuffer buf = (ChannelBuffer) e.getMessage();
         executor.execute(new ResponseRunner(buf));
      }
      else
      {
         write(ctx, e.getChannel(), e.getFuture(), e.getMessage(), e.getRemoteAddress());
      }
   }

   public void keepAlive(final long time)
   {
      //send some responses to catch up thus avoiding any timeout.
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
               //ignore, we'll just try again
            }
         }
         while (responseHolder == null);
         if(!bogusResponse)
         {
            piggyBackResponses();
         }
         responseHolder.response.setContent(buffer);
         responseHolder.response.addHeader(HttpHeaders.CONTENT_LENGTH, String.valueOf(buffer.writerIndex()));
         channel.write(responseHolder.response);
      }

      private void piggyBackResponses()
      {
         //if we are the last available response then we have to piggy back any remaining responses
         if(responses.isEmpty())
         {
            do
            {
               try
               {
                  ResponseRunner responseRunner = (ResponseRunner) delayedResponses.poll(0, TimeUnit.MILLISECONDS);
                  if(responseRunner == null)
                  {
                     break;
                  }
                  buffer.writeBytes(responseRunner.buffer);
               }
               catch (InterruptedException e)
               {
                  break;
               }
            }
            while (responses.isEmpty());
         }
      }
   }

   /**
    * a holder class so we know what time  the request first arrived
    */
   private class ResponseHolder
   {
      final HttpResponse response;
      final long timeReceived;

      public ResponseHolder(long timeReceived, HttpResponse response)
      {
         this.timeReceived = timeReceived;
         this.response = response;
      }
   }
   
}
