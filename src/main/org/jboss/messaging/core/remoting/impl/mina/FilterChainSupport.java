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

package org.jboss.messaging.core.remoting.impl.mina;

import javax.net.ssl.SSLContext;

import org.apache.mina.core.filterchain.DefaultIoFilterChainBuilder;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.ssl.SslFilter;
import org.jboss.messaging.core.remoting.impl.ssl.SSLSupport;
import org.jboss.messaging.core.remoting.spi.BufferHandler;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 */
public class FilterChainSupport
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   private FilterChainSupport()
   {
   }

   // Public --------------------------------------------------------

   public static void addCodecFilter(final DefaultIoFilterChainBuilder filterChain,
                                     final BufferHandler handler)
   {
      assert filterChain != null;

      filterChain.addLast("codec", new ProtocolCodecFilter(new MinaProtocolCodecFilter(handler)));
   }

   public static void addSSLFilter(
         final DefaultIoFilterChainBuilder filterChain, final boolean client,
         final String keystorePath, final String keystorePassword, final String trustStorePath,
         final String trustStorePassword) throws Exception
   {
      SSLContext context = SSLSupport.getInstance(client, keystorePath, keystorePassword,
            trustStorePath, trustStorePassword);
      SslFilter filter = new SslFilter(context);
      if (client)
      {
         filter.setUseClientMode(true);
         filter.setWantClientAuth(true);
      }
      filterChain.addLast("ssl", filter);
   }

   // Package protected ---------------------------------------------

   // Inner classes -------------------------------------------------
}
