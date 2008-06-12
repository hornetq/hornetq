/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina;

import javax.net.ssl.SSLContext;

import org.apache.mina.common.DefaultIoFilterChainBuilder;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.ssl.SslFilter;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.impl.ssl.SSLSupport;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class FilterChainSupport
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(FilterChainSupport.class);

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public static void addCodecFilter(final DefaultIoFilterChainBuilder filterChain)
   {
      assert filterChain != null;

      filterChain.addLast("codec", new ProtocolCodecFilter(new MessagingCodec()));
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
