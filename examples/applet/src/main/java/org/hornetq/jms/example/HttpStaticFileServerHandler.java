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

package org.hornetq.jms.example;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.stream.ChunkedFile;

/**
 * A HttpStaticFileServerHandler
 *
 * @author The Netty Project (netty-dev@lists.jboss.org)
 * @author Trustin Lee (tlee@redhat.com)
 * @author <a href="mailto:jmesnil@redhat.com>Jeff Mesnil</a>
 *
 *
 */
@ChannelPipelineCoverage("one")
public class HttpStaticFileServerHandler extends SimpleChannelUpstreamHandler
{

   @Override
   public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) throws Exception
   {
      HttpRequest request = (HttpRequest)e.getMessage();
      if (request.getMethod() != HttpMethod.GET)
      {
         sendError(ctx, HttpResponseStatus.METHOD_NOT_ALLOWED);
         return;
      }

      if (request.isChunked())
      {
         sendError(ctx, HttpResponseStatus.BAD_REQUEST);
         return;
      }

      String path = sanitizeUri(request.getUri());
      if (path == null)
      {
         sendError(ctx, HttpResponseStatus.FORBIDDEN);
         return;
      }

      File file = new File(path);
      if (file.isHidden() || !file.exists())
      {
         sendError(ctx, HttpResponseStatus.NOT_FOUND);
         return;
      }
      if (!file.isFile())
      {
         sendError(ctx, HttpResponseStatus.FORBIDDEN);
         return;
      }

      RandomAccessFile raf;
      try
      {
         raf = new RandomAccessFile(file, "r");
      }
      catch (FileNotFoundException fnfe)
      {
         sendError(ctx, HttpResponseStatus.NOT_FOUND);
         return;
      }
      long fileLength = raf.length();

      HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
      response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, String.valueOf(fileLength));

      Channel ch = e.getChannel();

      // Write the initial line and the header.
      ch.write(response);

      // Write the content.
      ChannelFuture writeFuture = ch.write(new ChunkedFile(raf, 0, fileLength, 8192));

      // Decide whether to close the connection or not.
      boolean close = HttpHeaders.Values.CLOSE.equalsIgnoreCase(request.getHeader(HttpHeaders.Names.CONNECTION)) || request.getProtocolVersion()
                                                                                                                           .equals(HttpVersion.HTTP_1_0) &&
                      !HttpHeaders.Values.KEEP_ALIVE.equalsIgnoreCase(request.getHeader(HttpHeaders.Names.CONNECTION));

      if (close)
      {
         // Close the connection when the whole content is written out.
         writeFuture.addListener(ChannelFutureListener.CLOSE);
      }
   }

   @Override
   public void exceptionCaught(final ChannelHandlerContext ctx, final ExceptionEvent e) throws Exception
   {
      Channel ch = e.getChannel();
      Throwable cause = e.getCause();
      if (cause instanceof TooLongFrameException)
      {
         sendError(ctx, HttpResponseStatus.BAD_REQUEST);
         return;
      }

      cause.printStackTrace();
      if (ch.isConnected())
      {
         sendError(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR);
      }
   }

   private String sanitizeUri(String uri)
   {
      // Decode the path.
      try
      {
         uri = URLDecoder.decode(uri, "UTF-8");
      }
      catch (UnsupportedEncodingException e)
      {
         try
         {
            uri = URLDecoder.decode(uri, "ISO-8859-1");
         }
         catch (UnsupportedEncodingException e1)
         {
            throw new Error();
         }
      }

      // Convert file separators.
      uri = uri.replace('/', File.separatorChar);

      // Simplistic dumb security check.
      // You will have to do something serious in the production environment.
      if (uri.contains(File.separator + ".") || uri.contains("." + File.separator) ||
          uri.startsWith(".") ||
          uri.endsWith("."))
      {
         return null;
      }

      // Convert to absolute path.
      return System.getProperty("user.dir") + File.separator + uri;
   }

   private void sendError(final ChannelHandlerContext ctx, final HttpResponseStatus status)
   {
      HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);
      response.setHeader(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8");
      response.setContent(ChannelBuffers.copiedBuffer("Failure: " + status.toString() + "\r\n", "UTF-8"));

      // Close the connection as soon as the error message is sent.
      ctx.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);
   }
}