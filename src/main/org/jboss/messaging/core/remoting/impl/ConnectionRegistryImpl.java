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

package org.jboss.messaging.core.remoting.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.jboss.messaging.core.client.ConnectionParams;
import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.ConnectionRegistry;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.RemotingHandler;
import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.remoting.spi.Connection;
import org.jboss.messaging.core.remoting.spi.ConnectionLifeCycleListener;
import org.jboss.messaging.core.remoting.spi.Connector;
import org.jboss.messaging.core.remoting.spi.ConnectorFactory;
import org.jboss.messaging.util.JBMThreadFactory;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public class ConnectionRegistryImpl implements ConnectionRegistry, ConnectionLifeCycleListener
{
   // Constants -----------------------------------------------------

   public static final Logger log = Logger.getLogger(ConnectionRegistryImpl.class);


   // Attributes ----------------------------------------------------

   private final Map<String, ConnectionHolder> connections = new HashMap<String, ConnectionHolder>();

   private final Map<TransportType, ConnectorFactory> connectorFactories = new HashMap<TransportType, ConnectorFactory>();

   private final Map<Object, RemotingConnection> remotingConnections = new HashMap<Object, RemotingConnection>();

   //TODO - core pool size should be configurable
   private final ScheduledThreadPoolExecutor pingExecutor = new ScheduledThreadPoolExecutor(20, new JBMThreadFactory("jbm-pinger-threads"));

   // Static --------------------------------------------------------

   // ConnectionRegistry implementation -----------------------------

   public synchronized RemotingConnection getConnection(final Location location,
                                                              final ConnectionParams connectionParams)
   {
      String key = location.getLocation();

      ConnectionHolder holder = connections.get(key);

      if (holder != null)
      {
         holder.increment();

         RemotingConnection connection = holder.getConnection();

         if (log.isDebugEnabled())
         {
            log.debug("Reusing " + connection + " to connect to " + key + " [count=" + holder.getCount() + "]");
         }

         return connection;
      }
      else
      {
         PacketDispatcher dispatcher = new PacketDispatcherImpl(null);

         RemotingHandler handler = new RemotingHandlerImpl(dispatcher, null);

         ConnectorFactory factory = connectorFactories.get(location.getTransport());
         
         if (factory == null)
         {
            throw new IllegalStateException("No connector factory registered for transport " + location.getTransport());
         }

         Connector connector = factory.createConnector(location, connectionParams, handler, this);
         
         connector.start();
            
         Connection tc = connector.createConnection();

         if (tc == null)
         {
            throw new IllegalStateException("Failed to connect to " + location);
         }

         long pingInterval = connectionParams.getPingInterval();
         RemotingConnection connection;

         if (pingInterval != -1)
         {
            connection = new RemotingConnectionImpl(tc, dispatcher, location,
                     connectionParams.getCallTimeout(), connectionParams.getPingInterval(), pingExecutor);
         }
         else
         {
            connection = new RemotingConnectionImpl(tc, dispatcher, location,
                     connectionParams.getCallTimeout());
         }

         remotingConnections.put(tc.getID(), connection);

         if (log.isDebugEnabled())
         {
            log.debug("Created " + connector + " to connect to "  + location);
         }

         holder = new ConnectionHolder(connection, connector);

         connections.put(key, holder);

         return connection;
      }
   }

   public synchronized void returnConnection(final Location location)
   {
      String key = location.getLocation();
      
      ConnectionHolder holder = connections.get(key);

      if (holder == null)
      {
         //This is ok and might happen if connection is returned after an error occurred on it in which
         //case it will have already automatically been closed and removed
         log.warn("Connection not found when returning - probably connection has failed and been automatically removed");
         return;
      }

      if (holder.getCount() == 1)
      {           
         RemotingConnection conn = remotingConnections.remove(holder.getConnection().getID());

         conn.destroy();

         holder.getConnector().close();

         connections.remove(key);
      }
      else
      {
         holder.decrement();
      }
   }

   public synchronized int size()
   {
      return connections.size();
   }

   public synchronized void registerConnectorFactory(final TransportType transport, final ConnectorFactory factory)
   {
      connectorFactories.put(transport, factory);
   }

   public synchronized void unregisterConnectorFactory(final TransportType transport)
   {
      connectorFactories.remove(transport);
   }

   public synchronized ConnectorFactory getConnectorFactory(final TransportType transport)
   {
      return connectorFactories.get(transport);
   }
   
   public synchronized int getCount(final Location location)
   {
      ConnectionHolder holder = connections.get(location.getLocation());
      
      if (holder != null)
      {
         return holder.getCount();
      }
      else
      {
         return 0;
      }
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // ConnectionLifeCycleListener implementation --------------------

   public void connectionCreated(final Connection connection)
   {
   }

   public void connectionDestroyed(final Object connectionID)
   {
      RemotingConnection conn = remotingConnections.remove(connectionID);

      if (conn != null)
      {
         ConnectionHolder holder = connections.remove(conn.getLocation().getLocation());

         //If conn still exists here this means that the underlying transport connection has been closed from the server side without
         //being returned from the client side so we need to fail the connection and call it's listeners
         MessagingException me = new MessagingException(MessagingException.OBJECT_CLOSED,
                                                        "The connection has been closed.");
         conn.fail(me);

         holder.getConnector().close();
      }
   }

   public void connectionException(final Object connectionID, final MessagingException me)
   {
      RemotingConnection conn = remotingConnections.remove(connectionID);

      if (conn == null)
      {
         throw new IllegalStateException("Cannot find connection with id " + connectionID);
      }

      ConnectionHolder holder = connections.remove(conn.getLocation().getLocation());

      conn.fail(me);

      holder.getConnector().close();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private static class ConnectionHolder
   {
      private final RemotingConnection connection;

      private final Connector connector;

      private int count;

      public ConnectionHolder(final RemotingConnection connection, final Connector connector)
      {
         assert connector != null;

         this.connection = connection;
         this.connector = connector;
         count = 1;
      }

      public void increment()
      {
         count++;
      }

      public void decrement()
      {
         count--;
      }

      public int getCount()
      {
         return count;
      }

      public RemotingConnection getConnection()
      {
         return connection;
      }

      public Connector getConnector()
      {
         return connector;
      }
   }
}
