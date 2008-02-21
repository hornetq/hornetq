/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina;

import static org.apache.mina.filter.keepalive.KeepAlivePolicy.EXCEPTION;
import static org.apache.mina.filter.logging.LogLevel.TRACE;
import static org.apache.mina.filter.logging.LogLevel.WARN;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import javax.net.ssl.SSLContext;

import org.apache.mina.common.DefaultIoFilterChainBuilder;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.executor.ExecutorFilter;
import org.apache.mina.filter.keepalive.KeepAliveFilter;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.filter.logging.MdcInjectionFilter;
import org.apache.mina.filter.reqres.RequestResponseFilter;
import org.apache.mina.filter.ssl.SslFilter;
import org.jboss.messaging.core.remoting.KeepAliveFactory;
import org.jboss.messaging.core.remoting.ssl.SSLSupport;
import org.jboss.messaging.util.Logger;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
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

   public static void addCodecFilter(DefaultIoFilterChainBuilder filterChain)
   {
      assert filterChain != null;

      filterChain.addLast("codec", new ProtocolCodecFilter(
            new PacketCodecFactory()));
   }
   
   public static void addKeepAliveFilter(DefaultIoFilterChainBuilder filterChain,
         KeepAliveFactory factory, int keepAliveInterval, int keepAliveTimeout, FailureNotifier notifier)
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

      filterChain.addLast("keep-alive", new KeepAliveFilter(
            new MinaKeepAliveFactory(factory, notifier), EXCEPTION, keepAliveInterval,
            keepAliveTimeout));
   }

   public static void addSSLFilter(
         DefaultIoFilterChainBuilder filterChain, boolean client,
         String keystorePath, String keystorePassword, String trustStorePath, String trustStorePassword) throws Exception
   {
      SSLContext context = SSLSupport.getInstance(!client, keystorePath, keystorePassword,
            trustStorePath, trustStorePassword);
      SslFilter filter = new SslFilter(context);
      if (client)
      {
         filter.setUseClientMode(true);
         filter.setWantClientAuth(true);
      }
      filterChain.addLast("ssl", filter);
      
      log.info("added ssl filter");  
   }
   
   // Package protected ---------------------------------------------

   static void addMDCFilter(DefaultIoFilterChainBuilder filterChain)
   {
      assert filterChain != null;

      MdcInjectionFilter mdcInjectionFilter = new MdcInjectionFilter();
      filterChain.addLast("mdc", mdcInjectionFilter);
   }

   static void addLoggingFilter(DefaultIoFilterChainBuilder filterChain)
   {
      assert filterChain != null;

      LoggingFilter filter = new LoggingFilter();

      filter.setSessionCreatedLogLevel(TRACE);
      filter.setSessionOpenedLogLevel(TRACE);
      filter.setSessionIdleLogLevel(TRACE);
      filter.setSessionClosedLogLevel(TRACE);

      filter.setMessageReceivedLogLevel(TRACE);
      filter.setMessageSentLogLevel(TRACE);

      filter.setExceptionCaughtLogLevel(WARN);

      filterChain.addLast("logger", filter);
   }

   static void addExecutorFilter(DefaultIoFilterChainBuilder filterChain)
   {
      ExecutorFilter executorFilter = new ExecutorFilter();
      filterChain.addLast("executor", executorFilter);
   }

   static ScheduledExecutorService addBlockingRequestResponseFilter(
         DefaultIoFilterChainBuilder filterChain)
   {
      assert filterChain != null;

      ScheduledExecutorService executorService = Executors
            .newScheduledThreadPool(1);
      RequestResponseFilter filter = new RequestResponseFilter(
            new MinaInspector(), executorService);
      filterChain.addLast("reqres", filter);

      return executorService;
   }

   // Inner classes -------------------------------------------------
}
