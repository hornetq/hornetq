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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.ConnectionRegistry;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.spi.Connection;
import org.jboss.messaging.core.remoting.spi.ConnectionLifeCycleListener;
import org.jboss.messaging.core.remoting.spi.Connector;
import org.jboss.messaging.core.remoting.spi.ConnectorFactory;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
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

   private final Map<RegistryKey, ConnectionHolder> connections = new ConcurrentHashMap<RegistryKey, ConnectionHolder>();

   private final Map<Object, RegistryKey> reverseMap = new ConcurrentHashMap<Object, RegistryKey>();

   //TODO - core pool size should be configurable
   private final ScheduledThreadPoolExecutor pingExecutor =
      new ScheduledThreadPoolExecutor(10, new JBMThreadFactory("jbm-pinger-threads"));

   // Static --------------------------------------------------------
   
   public static ConnectionRegistry instance = new ConnectionRegistryImpl();

   // ConnectionRegistry implementation -----------------------------
      
   public synchronized RemotingConnection getConnection(final ConnectorFactory connectorFactory,
            final Map<String, Object> params,
            final long pingInterval, final long callTimeout)
   {
      RegistryKey key = new RegistryKey(connectorFactory, params);

      ConnectionHolder holder = connections.get(key);
      
      if (holder != null)
      {
         holder.increment();
         
         RemotingConnection connection = holder.getConnection();

         return connection;
      }
      else
      {
         DelegatingBufferHandler handler = new DelegatingBufferHandler();
         
         Connector connector = connectorFactory.createConnector(params, handler, this);
         
         connector.start();
            
         Connection tc = connector.createConnection();

         if (tc == null)
         {
            throw new IllegalStateException("Failed to connect");
         }
         
         RemotingConnection connection =
            new RemotingConnectionImpl(tc, callTimeout, pingInterval, null, pingExecutor, null, null, true);
         
         handler.conn = connection;
         
         connection.startPinger();
                 
         holder = new ConnectionHolder(connection, connector);
  
         connections.put(key, holder);

         reverseMap.put(tc.getID(), key);

         return connection;
      }
   }

   public synchronized void returnConnection(final Object connectionID)
   {
      RegistryKey key = reverseMap.get(connectionID);
      
      if (key == null)
      {
         //This is ok and might happen if connection is returned after an error occurred on it in which
         //case it will have already automatically been closed and removed
         log.warn("Connection not found when returning - probably connection has failed and been automatically removed");
         return;
      }
      
      ConnectionHolder holder = connections.get(key);
      
      if (holder.getCount() == 1)
      {           
         RemotingConnection conn = holder.getConnection();
           
         reverseMap.remove(connectionID);
         
         connections.remove(key);
         
         conn.destroy();

         holder.getConnector().close();
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

   public synchronized int getCount(final ConnectorFactory connectorFactory, final Map<String, Object> params)
   {
      RegistryKey key = new RegistryKey(connectorFactory, params);
      
      ConnectionHolder holder = connections.get(key);
       
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
      RegistryKey key = reverseMap.remove(connectionID);
               
      if (key != null)
      {
         ConnectionHolder holder = connections.remove(key);
         
        
         //If conn still exists here this means that the underlying transport connection has been closed from the server side without
         //being returned from the client side so we need to fail the connection and call it's listeners
         MessagingException me = new MessagingException(MessagingException.OBJECT_CLOSED,
                                                        "The connection has been closed.");
         holder.getConnection().fail(me);

         holder.getConnector().close();
      }
   }

   public void connectionException(final Object connectionID, final MessagingException me)
   { 
      RegistryKey key = reverseMap.remove(connectionID);

      if (key == null)
      {
         throw new IllegalStateException("Cannot find connection with id " + connectionID);
      }

      ConnectionHolder holder = connections.remove(key);

      holder.getConnection().fail(me);

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
   
   private class RegistryKey
   {
      private final String connectorFactoryClassName;
      
      private final Map<String, Object> params;
      
      RegistryKey(final ConnectorFactory connectorFactory, final Map<String, Object> params)
      {
         this.connectorFactoryClassName = connectorFactory.getClass().getName();
         
         this.params = params;
      }
      
      public boolean equals(Object other)
      {
         RegistryKey kother = (RegistryKey)other;

         if (this.connectorFactoryClassName.equals(kother.connectorFactoryClassName))
         {
            if (this.params == null)
            {
               return kother.params == null;
            }
            else
            {
               if (kother.params == null)
               {
                  return false;
               }               
               else if (this.params.size() == kother.params.size())
               {
                  for (Map.Entry<String, Object> entry: this.params.entrySet())
                  {
                     Object thisVal = entry.getValue();
                     
                     Object otherVal = kother.params.get(entry.getKey());
                     
                     if (otherVal == null || !otherVal.equals(thisVal))
                     {
                        return false;
                     }
                  }
                  return true;
               }
               else
               {
                  return false;
               }
            }
         }
         else
         {
            return false;
         }
      }
      
      public int hashCode()
      {
         return connectorFactoryClassName.hashCode();
      }
   }
   
   private class DelegatingBufferHandler extends AbstractBufferHandler
   {
      RemotingConnection conn;
      
      public void bufferReceived(final Object connectionID, final MessagingBuffer buffer)
      {         
         conn.bufferReceived(connectionID, buffer);
      }            
   }
}
