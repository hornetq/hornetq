/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina;

import static org.apache.mina.filter.logging.LogLevel.TRACE;
import static org.apache.mina.filter.logging.LogLevel.WARN;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.mina.common.DefaultIoFilterChainBuilder;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.filter.logging.MdcInjectionFilter;
import org.apache.mina.filter.reqres.RequestResponseFilter;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class FilterChainSupport
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   static void addCodecFilter(DefaultIoFilterChainBuilder filterChain)
   {
      assert filterChain != null;

      filterChain.addLast("codec", new ProtocolCodecFilter(
            new PacketCodecFactory()));
   }

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
   
   static ScheduledExecutorService addBlockingRequestResponseFilter(
         DefaultIoFilterChainBuilder filterChain)
   {
      assert filterChain != null;

      ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
      RequestResponseFilter filter = new RequestResponseFilter(
            new MinaInspector(), executorService);      
      filterChain.addLast("reqres", filter);
      
      return executorService;
   }

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
