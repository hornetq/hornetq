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

import org.jboss.messaging.core.remoting.ConnectorRegistry;
import org.jboss.messaging.core.remoting.NIOConnector;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.RemotingConfiguration;
import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.remoting.impl.invm.INVMConnector;
import org.jboss.messaging.core.remoting.impl.mina.MinaConnector;
import org.jboss.messaging.util.Logger;

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

   private RemotingConfiguration localConfiguration = null;
   private PacketDispatcher localDispatcher = null;
   
   public Map<RemotingConfiguration, NIOConnectorHolder> connectors = new HashMap<RemotingConfiguration, NIOConnectorHolder>();

   // Static --------------------------------------------------------

   /**
    * @return <code>true</code> if this RemotingConfiguration has not already been registered,
    *         <code>false</code> else
    */
   public boolean register(RemotingConfiguration remotingConfig, PacketDispatcher serverDispatcher)
   {
      assert remotingConfig != null;
      assert serverDispatcher != null;
      
      PacketDispatcher previousDispatcher = localDispatcher;
      
      this.localConfiguration = remotingConfig;
      this.localDispatcher = serverDispatcher;
      
      if(log.isDebugEnabled())
      {
         log.debug("registered " + localDispatcher + " for " + localConfiguration);
      }
      return (previousDispatcher == null);
   }

   /**
    * @return <code>true</code> if this RemotingConfiguration was registered,
    *         <code>false</code> else
    */  
   public boolean unregister()
   {
       PacketDispatcher dispatcher = localDispatcher;

       localConfiguration = null;
       localDispatcher = null;
       
       if(log.isDebugEnabled())
       {
          log.debug("unregistered " + dispatcher);
       }

       return (dispatcher != null);
   }

   public synchronized NIOConnector getConnector(RemotingConfiguration remotingConfig)
   {
      assert remotingConfig != null;

      if (connectors.containsKey(remotingConfig))
      {
         NIOConnectorHolder holder = connectors.get(remotingConfig);
         holder.increment();
         NIOConnector connector = holder.getConnector();

         if (log.isDebugEnabled())
            log.debug("Reuse " + connector + " to connect to "
                  + remotingConfig + " [count=" + holder.getCount() + "]");

         return connector;
      }

      // check if the server is in the same vm than the client
      if (remotingConfig.equals(localConfiguration))
      {
         NIOConnector connector = new INVMConnector(localConfiguration.getHost(), localConfiguration
               .getPort(), localDispatcher);

         if (log.isDebugEnabled())
            log.debug("Created " + connector + " to connect to "
                  + remotingConfig);

         NIOConnectorHolder holder = new NIOConnectorHolder(connector);
         connectors.put(remotingConfig, holder);
         return connector;
      }

      NIOConnector connector = null;

      TransportType transport = remotingConfig.getTransport();

      if (transport == TCP)
      {
         connector = new MinaConnector(remotingConfig);
      } else if (transport == INVM)
      {
         assert localDispatcher != null;
         
         connector = new INVMConnector(remotingConfig.getHost(), remotingConfig.getPort(), localDispatcher);
      }

      if (connector == null)
      {
         throw new IllegalArgumentException(
               "no connector defined for transport " + transport);
      }

      if (log.isDebugEnabled())
         log.debug("Created " + connector + " to connect to "
               + remotingConfig);
      
      NIOConnectorHolder holder = new NIOConnectorHolder(connector);
      connectors.put(remotingConfig, holder);
      return connector;
   }

   /**
    * Decrement the number of references on the NIOConnector corresponding to
    * the RemotingConfiguration.
    * 
    * If there is only one reference, remove it from the connectors Map and
    * returns it. Otherwise return null.
    * 
    * @param remotingConfiguration
    *           a RemotingConfiguration
    * @return the NIOConnector if there is no longer any references to it or
    *         <code>null</code>
    * @throws IllegalStateException
    *            if no NIOConnector were created for the given RemotingConfiguration
    */
   public synchronized NIOConnector removeConnector(RemotingConfiguration remotingConfiguration)
   {
      assert remotingConfiguration != null;

      NIOConnectorHolder holder = connectors.get(remotingConfiguration);
      if (holder == null)
      {
         throw new IllegalStateException("No Connector were created for "
               + remotingConfiguration);
      }

      if (holder.getCount() == 1)
      {
         if (log.isDebugEnabled())
            log.debug("Removed connector for " + remotingConfiguration);
         connectors.remove(remotingConfiguration);
         return holder.getConnector();
      } else
      {
         holder.decrement();
         if (log.isDebugEnabled())
            log.debug(holder.getCount() + " remaining references to "
                  + holder.getConnector().getServerURI());
         return null;
      }
   }

   public RemotingConfiguration[] getRegisteredRemotingConfigurations()
   {
      Set<RemotingConfiguration> registeredRemotingConfigs = connectors.keySet();
      return (RemotingConfiguration[]) registeredRemotingConfigs
            .toArray(new RemotingConfiguration[registeredRemotingConfigs.size()]);
   }

   public int getConnectorCount(RemotingConfiguration remotingConfig)
   {
      NIOConnectorHolder holder = connectors.get(remotingConfig);
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
