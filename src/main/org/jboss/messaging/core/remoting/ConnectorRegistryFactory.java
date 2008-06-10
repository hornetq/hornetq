/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting;

import org.jboss.messaging.core.remoting.impl.ConnectorRegistryImpl;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * @version <tt>$Revision$</tt>
 */
public class ConnectorRegistryFactory implements ConnectorRegistryLocator
{
   // Constants -----------------------------------------------------
   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   /**
    * This singleton is used to get NIOConnectors.
    * It also check if the RemotingConfiguration has been locally registered so that the client
    * can use an in-vm connection to the server directly and skip the network.
    */
   private ConnectorRegistry registry = new ConnectorRegistryImpl();

   private static ConnectorRegistryLocator REGISTRY_LOCATOR = null;

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------


   public static ConnectorRegistry getRegistry()
   {
      if (REGISTRY_LOCATOR == null)
      {
         //todo create the locator from another source using sys props, for now just use the default
         REGISTRY_LOCATOR = new ConnectorRegistryFactory();
      }
      return REGISTRY_LOCATOR.locate();
   }

   public static void setRegisteryLocator(ConnectorRegistryLocator RegisteryLocator)
   {
      REGISTRY_LOCATOR = RegisteryLocator;
   }

   // Z implementation ----------------------------------------------

   public ConnectorRegistry locate()
   {
      return registry;
   }
// Y overrides ---------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
