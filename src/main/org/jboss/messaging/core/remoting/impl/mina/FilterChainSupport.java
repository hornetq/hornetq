/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina;

import static org.apache.mina.common.IdleStatus.BOTH_IDLE;
import static org.apache.mina.filter.keepalive.KeepAliveRequestTimeoutHandler.EXCEPTION;

import javax.net.ssl.SSLContext;

import org.apache.mina.common.DefaultIoFilterChainBuilder;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.keepalive.KeepAliveFilter;
import org.apache.mina.filter.ssl.SslFilter;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.KeepAliveFactory;
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
   
   public static void addKeepAliveFilter(final DefaultIoFilterChainBuilder filterChain,
         final KeepAliveFactory factory, final int keepAliveInterval,
         final int keepAliveTimeout, final CleanUpNotifier notifier)
   {
      assert filterChain != null;
      assert factory != null;
      assert notifier != null; 
     
      if (keepAliveTimeout > keepAliveInterval)
      {
         throw new IllegalArgumentException("timeout must be greater than the interval: "
               + "keepAliveTimeout= " + keepAliveTimeout
               + ", keepAliveInterval=" + keepAliveInterval);
      }

      KeepAliveFilter filter = new KeepAliveFilter(
            new MinaKeepAliveFactory(factory, notifier), BOTH_IDLE, EXCEPTION, keepAliveInterval,
            keepAliveTimeout);
      filter.setForwardEvent(true);
      filterChain.addLast("keep-alive", filter);
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

//   static void addMDCFilter(final DefaultIoFilterChainBuilder filterChain)
//   {
//      assert filterChain != null;
//
//      MdcInjectionFilter mdcInjectionFilter = new MdcInjectionFilter();
//      filterChain.addLast("mdc", mdcInjectionFilter);
//   }
//
//   static void addLoggingFilter(final DefaultIoFilterChainBuilder filterChain)
//   {
//      assert filterChain != null;
//
//      LoggingFilter filter = new LoggingFilter();
//
//      filter.setSessionCreatedLogLevel(TRACE);
//      filter.setSessionOpenedLogLevel(TRACE);
//      filter.setSessionIdleLogLevel(TRACE);
//      filter.setSessionClosedLogLevel(TRACE);
//
//      filter.setMessageReceivedLogLevel(TRACE);
//      filter.setMessageSentLogLevel(TRACE);
//
//      filter.setExceptionCaughtLogLevel(WARN);
//
//      filterChain.addLast("logger", filter);
//   }

   // Inner classes -------------------------------------------------
}
