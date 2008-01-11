/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl;

import static org.jboss.messaging.core.remoting.TransportType.INVM;
import static org.jboss.messaging.core.remoting.TransportType.TCP;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.remoting.ConnectorRegistry;
import org.jboss.messaging.core.remoting.NIOConnector;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.ServerLocator;
import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.remoting.impl.invm.INVMConnector;
import org.jboss.messaging.core.remoting.impl.mina.MinaConnector;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class ConnectorRegistryImpl implements ConnectorRegistry
{
   // Constants -----------------------------------------------------

   public static final Logger log = Logger.getLogger(ConnectorRegistryImpl.class);

   // Attributes ----------------------------------------------------

   public Map<ServerLocator, PacketDispatcher> locators = new HashMap<ServerLocator, PacketDispatcher>();

   public Map<ServerLocator, NIOConnectorHolder> connectors = new HashMap<ServerLocator, NIOConnectorHolder>();

   // Static --------------------------------------------------------

   /**
    * @return <code>true</code> if this locator has not already been registered,
    *         <code>false</code> else
    */
   public boolean register(ServerLocator locator, PacketDispatcher serverDispatcher)
   {
      assert locator != null;
      assert serverDispatcher != null;
      
      PacketDispatcher previousDispatcher = locators.put(locator, serverDispatcher);
      return (previousDispatcher == null);
   }

   /**
    * @return <code>true</code> if this locator was registered,
    *         <code>false</code> else
    */  
   public boolean unregister(ServerLocator locator)
   {
       PacketDispatcher dispatcher = locators.remove(locator);
       return (dispatcher != null);
   }

   public synchronized NIOConnector getConnector(ServerLocator locator)
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
      if (locators.containsKey(locator))
      {
         NIOConnector connector = new INVMConnector(locator.getHost(), locator
               .getPort(), locators.get(locator));

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
         connector = new INVMConnector(locator.getHost(), locator.getPort(), locators.get(locator));
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
   public synchronized NIOConnector removeConnector(ServerLocator locator)
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

   public ServerLocator[] getRegisteredLocators()
   {
      Set<ServerLocator> registeredLocators = connectors.keySet();
      return (ServerLocator[]) registeredLocators
            .toArray(new ServerLocator[registeredLocators.size()]);
   }

   public int getConnectorCount(ServerLocator locator)
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

      public void increment()
      {
         assert count > 0;

         count++;
      }

      public void decrement()
      {
         count--;

         assert count > 0;
      }

      public int getCount()
      {
         return count;
      }

      public NIOConnector getConnector()
      {
         return connector;
      }
   }
}
