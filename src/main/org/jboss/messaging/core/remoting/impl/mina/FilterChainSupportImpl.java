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

import org.apache.mina.common.DefaultIoFilterChainBuilder;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.ssl.SslFilter;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.impl.ssl.SSLSupport;

public class FilterChainSupportImpl implements FilterChainSupport
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(FilterChainSupport.class);

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void addCodecFilter(final DefaultIoFilterChainBuilder filterChain)
   {
      assert filterChain != null;

      filterChain.addLast("codec", new ProtocolCodecFilter(new MinaProtocolCodecFilter()));
   }

   public void addSSLFilter(
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
