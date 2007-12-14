/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting;

import static org.jboss.messaging.core.remoting.TransportType.INVM;
import static org.jboss.messaging.core.remoting.TransportType.TCP;

import java.util.HashSet;
import java.util.Set;

import org.jboss.messaging.core.remoting.impl.invm.INVMConnector;
import org.jboss.messaging.core.remoting.impl.mina.MinaConnector;
import org.jboss.messaging.core.remoting.impl.mina.MinaService;

/**
 * The ConnectorRegistry keeps track of ServerLocators and NIOConnectors.
 * 
 * When a {@link MinaService} is started, it register its {@link ServerLocator}.
 * 
 * When a {@link Client} is created, it gets its {@link NIOConnector} from the
 * ConnectorRegistry using the {@link ServerLocator} corresponding to the server
 * it wants to connect to. If the ConnectionRegistry contains this locator, it
 * implies that the Client is in the same VM than the server. In that case, we
 * optimize by returning a {@link INVMConnector} regardless of the transport
 * type defined by the locator
 * 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class ConnectorRegistry
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   private static Set<ServerLocator> locators = new HashSet<ServerLocator>();

   public static boolean register(ServerLocator locator)
   {
      return locators.add(locator);
   }

   public static boolean unregister(ServerLocator locator)
   {
      return locators.remove(locator);
   }

   public static NIOConnector get(ServerLocator locator)
   {
      assert locator != null;

      // check if the server is in the same vm than the client
      if (locators.contains(locator))
      {
         return new INVMConnector(locator.getHost(), locator.getPort());
      }

      TransportType transport = locator.getTransport();

      if (transport == TCP)
      {
         return new MinaConnector(locator.getTransport(), locator.getHost(),
               locator.getPort());
      } else if (transport == INVM)
      {
         return new INVMConnector(locator.getHost(), locator.getPort());
      } else
      {
         throw new IllegalArgumentException(
               "no connector defined for transport " + transport);
      }
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
