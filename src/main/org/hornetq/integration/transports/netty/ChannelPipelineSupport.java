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

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.hornetq.core.protocol.stomp.StompFrameDelimiter;
import org.hornetq.spi.core.protocol.ProtocolType;
import org.hornetq.spi.core.remoting.BufferDecoder;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.handler.ssl.SslHandler;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @author <a href="mailto:tlee@redhat.com">Trustin Lee</a>
 * @version $Rev$, $Date$
 */
public class ChannelPipelineSupport
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   private ChannelPipelineSupport()
   {
      // Unused
   }

   // Public --------------------------------------------------------

   public static void addCodecFilter(final ProtocolType protocol, final ChannelPipeline pipeline, final BufferDecoder decoder)
   {
      assert pipeline != null;
      
      if (protocol == ProtocolType.CORE)
      {
         //Core protocol uses it's own optimised decoder
         pipeline.addLast("decoder", new HornetQFrameDecoder2());
      }
      else if (protocol == ProtocolType.STOMP)
      {
         pipeline.addLast("decoder", new StompFrameDelimiter());
      }
      else
      {
         pipeline.addLast("decoder", new HornetQFrameDecoder(decoder));
      }
   }

   public static void addSSLFilter(final ChannelPipeline pipeline, final SSLContext context, final boolean client) throws Exception
   {
      SSLEngine engine = context.createSSLEngine();
      engine.setUseClientMode(client);
      if (client)
      {
         engine.setWantClientAuth(true);
      }

      SslHandler handler = new SslHandler(engine);
      pipeline.addLast("ssl", handler);
   }

   // Package protected ---------------------------------------------

   // Inner classes -------------------------------------------------
}
