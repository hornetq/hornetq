/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl;

import static org.jboss.messaging.core.remoting.TransportType.TCP;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.ConnectorRegistry;
import org.jboss.messaging.core.remoting.NIOConnector;
import org.jboss.messaging.core.remoting.PacketDispatcher;
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

   // the String key corresponds to Configuration.getLocation()
   private Map<String, PacketDispatcher> localDispatchers = new HashMap<String, PacketDispatcher>();
   
   public Map<String, NIOConnectorHolder> connectors = new HashMap<String, NIOConnectorHolder>();

   // Static --------------------------------------------------------

   /**
    * @return <code>true</code> if this Configuration has not already been registered,
    *         <code>false</code> else
    */
   public boolean register(Configuration config, PacketDispatcher serverDispatcher)
   {
      assert config != null;
      assert serverDispatcher != null;
      String key = config.getLocation();

      PacketDispatcher previousDispatcher = localDispatchers.get(key);

      localDispatchers.put(key, serverDispatcher);
      if(log.isDebugEnabled())
      {
         log.debug("registered " + key + " for " + serverDispatcher);
      }
      
      return (previousDispatcher == null);
   }

   /**
    * @return <code>true</code> if this Configuration was registered,
    *         <code>false</code> else
    */  
   public boolean unregister(Configuration config)
   {      
      PacketDispatcher dispatcher = localDispatchers.remove(config.getLocation());

       if(log.isDebugEnabled())
       {
          log.debug("unregistered " + dispatcher);
       }

       return (dispatcher != null);
   }

   public synchronized NIOConnector getConnector(Configuration config, PacketDispatcher dispatcher)
   {
      assert config != null;
      String key = config.getLocation();
      
      if (connectors.containsKey(key))
      {
         NIOConnectorHolder holder = connectors.get(key);
         holder.increment();
         NIOConnector connector = holder.getConnector();

         if (log.isDebugEnabled())
            log.debug("Reuse " + connector + " to connect to "
                  + key + " [count=" + holder.getCount() + "]");

         return connector;
      }

      // check if the server is in the same vm than the client
      if (localDispatchers.containsKey(key))
      {
         PacketDispatcher localDispatcher = localDispatchers.get(key);
         NIOConnector connector = new INVMConnector(dispatcher, localDispatcher);

         if (log.isDebugEnabled())
            log.debug("Created " + connector + " to connect to "
                  + key);

         NIOConnectorHolder holder = new NIOConnectorHolder(connector);
         connectors.put(key, holder);
         return connector;
      }

      NIOConnector connector = null;

      TransportType transport = config.getTransport();

      if (transport == TCP)
      {
         connector = new MinaConnector(config, dispatcher);
      }

      if (connector == null)
      {
         throw new IllegalArgumentException(
               "no connector defined for transport " + transport);
      }

      if (log.isDebugEnabled())
         log.debug("Created " + connector + " to connect to "
               + config);
      
      NIOConnectorHolder holder = new NIOConnectorHolder(connector);
      connectors.put(key, holder);
      return connector;
   }

   /**
    * Decrement the number of references on the NIOConnector corresponding to
    * the Configuration.
    * 
    * If there is only one reference, remove it from the connectors Map and
    * returns it. Otherwise return null.
    * 
    * @param config
    *           a Configuration
    * @return the NIOConnector if there is no longer any references to it or
    *         <code>null</code>
    * @throws IllegalStateException
    *            if no NIOConnector were created for the given Configuration
    */
   public synchronized NIOConnector removeConnector(Configuration config)
   {
      assert config != null;
      String key = config.getLocation();

      NIOConnectorHolder holder = connectors.get(key);
      if (holder == null)
      {
         throw new IllegalStateException("No Connector were created for "
               + key);
      }

      if (holder.getCount() == 1)
      {
         if (log.isDebugEnabled())
            log.debug("Removed connector for " + key);
         connectors.remove(key);
         return holder.getConnector();
      } else
      {
         holder.decrement();
         if (log.isDebugEnabled())
            log.debug(holder.getCount() + " remaining references to "
                  + key);
         return null;
      }
   }

   public int getRegisteredConfigurationSize()
   {
      Collection<String> registeredConfigs = connectors.keySet();
      return registeredConfigs.size();
   }

   public int getConnectorCount(Configuration remotingConfig)
   {
      String key = remotingConfig.getLocation();
      NIOConnectorHolder holder = connectors.get(key);
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
