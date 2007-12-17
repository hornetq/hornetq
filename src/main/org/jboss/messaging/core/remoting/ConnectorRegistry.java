/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting;

import static org.jboss.messaging.core.remoting.TransportType.INVM;
import static org.jboss.messaging.core.remoting.TransportType.TCP;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.jboss.logging.Logger;
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

   private static final Logger log = Logger.getLogger(ConnectorRegistry.class);

   // Attributes ----------------------------------------------------

   private static Set<ServerLocator> locators = new HashSet<ServerLocator>();

   private static Map<ServerLocator, NIOConnectorHolder> connectors = new HashMap<ServerLocator, NIOConnectorHolder>();

   // Static --------------------------------------------------------

   /**
    * @return <code>true</code> if this locator has not already been registered,
    *         <code>false</code> else
    */
   public static boolean register(ServerLocator locator)
   {
      return locators.add(locator);
   }

   /**
    * @return <code>true</code> if this locator was registered,
    *         <code>false</code> else
    */   public static boolean unregister(ServerLocator locator)
   {
      return locators.remove(locator);
   }

   public static synchronized NIOConnector getConnector(ServerLocator locator)
   {
      assert locator != null;

      if (connectors.containsKey(locator))
      {
         NIOConnectorHolder holder = connectors.get(locator);
         holder.increment();
         NIOConnector connector = holder.getConnector();

         if (log.isDebugEnabled())
            log.debug("Reuse " + connector.getServerURI() + " to connect to "
                  + locator + " [count=" + holder.getCount() + "]");

         return connector;
      }

      // check if the server is in the same vm than the client
      if (locators.contains(locator))
      {
         NIOConnector connector = new INVMConnector(locator.getHost(), locator
               .getPort());

         if (log.isDebugEnabled())
            log.debug("Created " + connector.getServerURI() + " to connect to "
                  + locator);

         NIOConnectorHolder holder = new NIOConnectorHolder(connector);
         connectors.put(locator, holder);
         return connector;
      }

      NIOConnector connector = null;

      TransportType transport = locator.getTransport();

      if (transport == TCP)
      {
         connector = new MinaConnector(locator.getTransport(), locator
               .getHost(), locator.getPort());
      } else if (transport == INVM)
      {
         connector = new INVMConnector(locator.getHost(), locator.getPort());
      }

      if (connector == null)
      {
         throw new IllegalArgumentException(
               "no connector defined for transport " + transport);
      }

      if (log.isDebugEnabled())
         log.debug("Created " + connector.getServerURI() + " to connect to "
               + locator);
      NIOConnectorHolder holder = new NIOConnectorHolder(connector);
      connectors.put(locator, holder);
      return connector;
   }

   /**
    * Decrement the number of references on the NIOConnector corresponding to
    * the locator.
    * 
    * If there is only one reference, remove it from the connectors Map and
    * returns it. Otherwise return null.
    * 
    * @param locator
    *           a ServerLocator
    * @return the NIOConnector if there is no longer any references to it or
    *         <code>null</code>
    * @throws IllegalStateException
    *            if no NIOConnector were created for the given locator
    */
   public synchronized static NIOConnector removeConnector(ServerLocator locator)
   {
      assert locator != null;

      NIOConnectorHolder holder = connectors.get(locator);
      if (holder == null)
      {
         throw new IllegalStateException("No Connector were created for "
               + locator);
      }

      if (holder.getCount() == 1)
      {
         if (log.isDebugEnabled())
            log.debug("Removed connector for " + locator);
         connectors.remove(locator);
         return holder.getConnector();
      } else
      {
         holder.decrement();
         if (log.isDebugEnabled())
            log.debug(holder.getCount() + " remaining reference to "
                  + holder.getConnector().getServerURI() + " to " + locator);
         return null;
      }
   }

   public static ServerLocator[] getRegisteredLocators()
   {
      Set<ServerLocator> registeredLocators = connectors.keySet();
      return (ServerLocator[]) registeredLocators
            .toArray(new ServerLocator[registeredLocators.size()]);
   }

   public static Object getConnectorCount(ServerLocator locator)
   {
      NIOConnectorHolder holder = connectors.get(locator);
      if (holder == null)
      {
         return 0;
      }
      return holder.getCount();
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   static class NIOConnectorHolder
   {
      private final NIOConnector connector;
      private int count;

      public NIOConnectorHolder(NIOConnector connector)
      {
         assert connector != null;

         this.connector = connector;
         this.count = 1;
      }

      void increment()
      {
         assert count > 0;

         count++;
      }

      void decrement()
      {
         count--;

         assert count > 0;
      }

      int getCount()
      {
         return count;
      }

      public NIOConnector getConnector()
      {
         return connector;
      }
   }
}
