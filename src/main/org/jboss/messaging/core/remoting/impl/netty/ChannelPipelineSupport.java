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

package org.jboss.messaging.core.remoting.impl.netty;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.jboss.messaging.core.remoting.RemotingHandler;
import org.jboss.messaging.core.remoting.impl.ssl.SSLSupport;
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

   public static void addCodecFilter(final ChannelPipeline pipeline,
                                     final RemotingHandler handler)
   {
      assert pipeline != null;
      pipeline.addLast("decoder", new MessagingFrameDecoder(handler));
   }

   public static void addSSLFilter(
         final ChannelPipeline pipeline, final boolean client,
         final String keystorePath, final String keystorePassword, final String trustStorePath,
         final String trustStorePassword) throws Exception
   {
      SSLContext context = SSLSupport.getInstance(client, keystorePath, keystorePassword,
            trustStorePath, trustStorePassword);
      SSLEngine engine = context.createSSLEngine();
      if (client)
      {
         engine.setUseClientMode(true);
         engine.setWantClientAuth(true);
      }

      SslHandler handler = new SslHandler(engine);
      pipeline.addLast("ssl", handler);
   }

   // Package protected ---------------------------------------------

   // Inner classes -------------------------------------------------
}
