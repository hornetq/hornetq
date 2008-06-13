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
      if (configuration.getTransport() == INVM
            && configuration.isInVMDisabled())
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
      
      int receiveBufferSize = configuration.getConnectionParams().getTcpReceiveBufferSize();
      if (receiveBufferSize != -1 && receiveBufferSize <= 0)
      {
         String message = "Invalid value for TCP receive buffer size: " + receiveBufferSize;
         message += ". Value must be either -1 (not specified) or greater than 0";
         throw new IllegalStateException(message);
      }

      int sendBufferSize = configuration.getConnectionParams().getTcpSendBufferSize();
      if (sendBufferSize != -1 && sendBufferSize <= 0)
      {
         String message = "Invalid value for TCP send buffer size: " + sendBufferSize;
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
