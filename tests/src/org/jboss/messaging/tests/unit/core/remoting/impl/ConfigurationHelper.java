package org.jboss.messaging.tests.unit.core.remoting.impl;

import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.remoting.TransportType;

public class ConfigurationHelper
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public static ConfigurationImpl newTCPConfiguration(String localhost, int port)
   {
      ConfigurationImpl config = new ConfigurationImpl();
      config.setTransport(TransportType.TCP);
      config.setHost(localhost);
      config.setPort(port);
      return config;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
