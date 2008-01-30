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
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class ConnectorRegistrySingleton
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   /**
    * This singleton is used to get NIOConnectors.
    * It also check if the RemotingConfiguration has been locally registered so that the client
    * can use an in-vm connection to the server directly and skip the network.
    */
   public static final ConnectorRegistry REGISTRY = new ConnectorRegistryImpl();
   
   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Z implementation ----------------------------------------------

   // Y overrides ---------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
