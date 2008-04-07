package org.jboss.messaging.core.remoting.impl;

import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.remoting.TransportType;

public class ConfigurationHelper
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public static ConfigurationImpl newConfiguration(TransportType transport, String localhost, int port)
   {
      ConfigurationImpl config = new ConfigurationImpl();
      if (transport == TransportType.INVM)
         config.setInvmDisabled(false);
      config.setTransport(transport);
      config.setHost(localhost);
      config.setPort(port);
      return config;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
