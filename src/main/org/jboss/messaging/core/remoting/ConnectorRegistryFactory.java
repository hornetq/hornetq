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
