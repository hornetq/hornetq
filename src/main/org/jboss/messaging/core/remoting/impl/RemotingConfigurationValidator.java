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

package org.jboss.messaging.core.remoting.impl;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.logging.Logger;
import static org.jboss.messaging.core.remoting.TransportType.INVM;

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
      if(configuration == null)
      {
         throw new IllegalStateException("Config must not be null");
      }
      if (configuration.getTransport() == INVM
            && !configuration.getConnectionParams().isInVMOptimisationEnabled())
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
