/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl;

import static org.jboss.messaging.core.remoting.TransportType.INVM;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.logging.Logger;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class RemotingConfigurationValidator
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(RemotingConfigurationValidator.class);

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static void validate(Configuration configuration)
   {
      assert configuration != null;
      
      if (log.isDebugEnabled())
         log.debug("validating " + configuration.getURI());
      
      if (configuration.getTransport() == INVM
            && configuration.isInvmDisabled())
      {
         throw new IllegalStateException(
               "It is not allowed to disable invm communication when the transport is set to invm.");
      }
      if (configuration.getTransport() == INVM
            && configuration.isSSLEnabled())
      {
         throw new IllegalStateException(
               "It is not allowed to enable SSL when the transport is set to invm.");
      }
      if (configuration.getTransport() != INVM 
            && configuration.getPort() <= 0)
      {
         throw new IllegalStateException("Remoting port can not be negative when transport is not INVM");
      }
      
      if (configuration.getTcpReceiveBufferSize() != -1 && configuration.getTcpReceiveBufferSize() <= 0)
      {
         String message = "Invalid value for TCP receive buffer size: " + configuration.getTcpReceiveBufferSize();
         message += ". Value must be either -1 (not specified) or greater than 0";
         throw new IllegalStateException(message);
      }

      if (log.isDebugEnabled())
         log.debug("configuration is valid.");
   }
   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
