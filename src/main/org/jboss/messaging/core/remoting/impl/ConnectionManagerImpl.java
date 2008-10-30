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

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.ConnectionManager;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.spi.Connection;
import org.jboss.messaging.core.remoting.spi.ConnectionLifeCycleListener;
import org.jboss.messaging.core.remoting.spi.Connector;
import org.jboss.messaging.core.remoting.spi.ConnectorFactory;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.util.JBMThreadFactory;

/**
 * A ConnectionManagerImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 24 Oct 2008 09:08:12
 *
 *
 */
public class ConnectionManagerImpl implements ConnectionManager, ConnectionLifeCycleListener
{
   private static final Logger log = Logger.getLogger(ConnectionManagerImpl.class);
   
   private final ConnectorFactory connectorFactory;
   
   private final Map<String, Object> params;
   
   private final long pingInterval;
   
   private final long callTimeout;
   
   private final int maxConnections;
       
   //TODO - allow this to be configurable
   private static final ScheduledThreadPoolExecutor pingExecutor = new ScheduledThreadPoolExecutor(5,
                                                                             new JBMThreadFactory("jbm-pinger-threads"));
   
   private final Map<Object, ConnectionEntry> connections = new LinkedHashMap<Object, ConnectionEntry>();
      
   private int refCount;
   
   private Iterator<ConnectionEntry> mapIterator;
   
   private Object failConnectionLock = new Object();
      
   public ConnectionManagerImpl(final ConnectorFactory connectorFactory,
                                final Map<String, Object> params,
                                final long pingInterval,
                                final long callTimeout,
                                final int maxConnections,
                                final int pingPoolSize)  // FIXME - pingPoolSize is not used
   {
      this.connectorFactory = connectorFactory;
      
      this.params = params;
      
      this.pingInterval = pingInterval;
      
      this.callTimeout = callTimeout;
      
      this.maxConnections = maxConnections; 
   }
    
   public RemotingConnection createConnection()
   {
      DelegatingBufferHandler handler = new DelegatingBufferHandler();

      NoCacheConnectionLifeCycleListener listener = new NoCacheConnectionLifeCycleListener();

      Connector connector = connectorFactory.createConnector(params, handler, listener);

      connector.start();

      Connection tc = connector.createConnection();

      if (tc == null)
      {
         throw new IllegalStateException("Failed to connect");
      }

      RemotingConnection connection = new RemotingConnectionImpl(tc, callTimeout, pingInterval, pingExecutor, null);

      handler.conn = connection;

      listener.conn = connection;

      connection.startPinger();

      return connection;
   }
   
   public synchronized RemotingConnection getConnection()
   {
      RemotingConnection conn;

      if (connections.size() < maxConnections)
      {
         // Create a new one

         DelegatingBufferHandler handler = new DelegatingBufferHandler();

         Connector connector = connectorFactory.createConnector(params, handler, this);

         connector.start();

         Connection tc = connector.createConnection();

         if (tc == null)
         {
            throw new IllegalStateException("Failed to connect");
         }

         conn = new RemotingConnectionImpl(tc, callTimeout, pingInterval, pingExecutor, null);

         handler.conn = conn;

         conn.startPinger();

         connections.put(conn.getID(), new ConnectionEntry(conn, connector));
      }
      else
      {
         // Return one round-robin from the list
         
         if (mapIterator == null || !mapIterator.hasNext())
         {
            mapIterator = connections.values().iterator();
         }

         ConnectionEntry entry = mapIterator.next();
         
         conn = entry.connection;            
      }
      
      refCount++;
      
      return conn;
   }
   
   public synchronized void returnConnection(final Object connectionID)
   {              
      ConnectionEntry entry = connections.get(connectionID);
      
      if (refCount != 0)
      {
         refCount--;
      }
      
      if (entry != null)
      {                  
         checkCloseConnections();
      }
      else
      {
         //Can be legitimately null if session was closed before then went to remove session from csf
         //and locked since failover had started then after failover removes it but it's already been failed
      }
   }
    
   public void failConnection(final MessagingException me)
   {      
      synchronized (failConnectionLock)
      {
         //When a single connection fails, we fail *all* the connections
         
         Set<ConnectionEntry> copy = new HashSet<ConnectionEntry>(connections.values());         
         
         for (ConnectionEntry entry: copy)
         {
            entry.connection.fail(me);      
         }
         
         refCount = 0;
      }
   }
   
   public synchronized int getRefCount()
   {
      return refCount;
   }
   
   public synchronized int numConnections()
   {    
      return connections.size();
   }
   
   public synchronized Set<RemotingConnection> getConnections()
   {
      Set<RemotingConnection> conns = new HashSet<RemotingConnection>();
      
      for (ConnectionEntry entry: connections.values())
      {
         conns.add(entry.connection);
      }
      
      return conns;
   }
   
   // Private -------------------------------------------------------
   
   private void checkCloseConnections()
   {
      if (refCount == 0)
      {
         //Close connections
            
         Set<ConnectionEntry> copy = new HashSet<ConnectionEntry>(connections.values());
         
         connections.clear();    
         
         for (ConnectionEntry entry: copy)
         {
            try
            {
               entry.connection.destroy();
            
               entry.connector.close();
            }
            catch (Throwable ignore)
            {                  
            }                                    
         }                                    
      }
   }
   
   // ConnectionLifeCycleListener implementation --------------------

   public void connectionCreated(final Connection connection)
   {
   }

   public void connectionDestroyed(final Object connectionID)
   {      
      // If conn still exists here this means that the underlying transport
      // conn has been closed from the server side without
      // being returned from the client side so we need to fail the conn and
      // call it's listeners
      MessagingException me = new MessagingException(MessagingException.OBJECT_CLOSED,
                                                     "The conn has been closed.");
      failConnection(me);              
   }

   public void connectionException(final Object connectionID, final MessagingException me)
   {
      failConnection(me);      
   }
   
   // Inner classes ----------------------------------------------------------------
   
   private class ConnectionEntry
   {
      ConnectionEntry(final RemotingConnection connection, final Connector connector)
      {
         this.connection = connection;
         
         this.connector = connector;
      }
      
      final RemotingConnection connection;
      
      final Connector connector;
   }
   
   private class DelegatingBufferHandler extends AbstractBufferHandler
   {
      RemotingConnection conn;

      public void bufferReceived(final Object connectionID, final MessagingBuffer buffer)
      {
         conn.bufferReceived(connectionID, buffer);
      }
   }
   
   private static class NoCacheConnectionLifeCycleListener implements ConnectionLifeCycleListener
   {
      private RemotingConnection conn;

      public void connectionCreated(final Connection connection)
      {
      }

      public void connectionDestroyed(final Object connectionID)
      {
         if (conn != null)
         {
            conn.destroy();
         }
      }

      public void connectionException(final Object connectionID, final MessagingException me)
      {
         if (conn != null)
         {
            conn.fail(me);
         }
      }
   }

}
