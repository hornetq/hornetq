/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.integration;

import static org.apache.mina.filter.logging.LogLevel.TRACE;
import static org.apache.mina.filter.logging.LogLevel.WARN;

import org.apache.mina.common.DefaultIoFilterChainBuilder;
import org.apache.mina.filter.logging.LoggingFilter;

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

   static void addLoggingFilter(DefaultIoFilterChainBuilder filterChain)
   {
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

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
