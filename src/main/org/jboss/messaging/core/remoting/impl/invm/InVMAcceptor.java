/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.messaging.core.remoting.impl.invm;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.spi.Acceptor;
import org.jboss.messaging.core.remoting.spi.BufferHandler;
import org.jboss.messaging.core.remoting.spi.Connection;
import org.jboss.messaging.core.remoting.spi.ConnectionLifeCycleListener;
import org.jboss.messaging.util.ConfigurationHelper;

/**
 * A InVMAcceptor
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class InVMAcceptor implements Acceptor
{   
   private final int id;
   
   private final BufferHandler handler;
   
   private final ConnectionLifeCycleListener listener;
   
   private ConcurrentMap<String, Connection> connections = new ConcurrentHashMap<String, Connection>();
   
   private volatile boolean started;
   
   public InVMAcceptor(final Map<String, Object> configuration, final BufferHandler handler,
                       final ConnectionLifeCycleListener listener)
   {
      this.handler = handler;
      
      this.listener = listener;   
      
      this.id = ConfigurationHelper.getIntProperty(TransportConstants.SERVER_ID_PROP_NAME, 0, configuration);
   }
   
   public synchronized void start() throws Exception
   {      
      if (started)
      {
         return;
      }
      
      InVMRegistry registry = InVMRegistry.instance;
      
      registry.registerAcceptor(id, this);
      
      started = true;
   }

   public synchronized void stop()
   {   
      if (!started)
      {
         return;
      }
      
      for (Connection connection: connections.values())
      {
         listener.connectionDestroyed(connection.getID());
      }
      
      connections.clear();
      
      InVMRegistry.instance.unregisterAcceptor(id);
      
      started = true;
   }
   
   public boolean isStarted()
   {
      return started;
   }

   public BufferHandler getHandler()
   {
      if (!started)
      {
         throw new IllegalStateException("Acceptor is not started");
      }
      
      return handler;
   }
   
   public void connect(final String connectionID, final BufferHandler remoteHandler,
                       final InVMConnector connector)
   {
      if (!started)
      {
         throw new IllegalStateException("Acceptor is not started");
      }
      
      new InVMConnection(id, connectionID, remoteHandler, new Listener(connector));               
   }
   
   public void disconnect(final String connectionID)
   {
      if (!started)
      {
         throw new IllegalStateException("Acceptor is not started");
      }
      
      Connection conn = connections.get(connectionID);
      
      if (conn != null)
      {
         conn.close();
      }            
   }
   
   private class Listener implements ConnectionLifeCycleListener
   {
      private final InVMConnector connector;
      
      Listener(final InVMConnector connector)
      {
         this.connector = connector;
      }
      
      public void connectionCreated(final Connection connection)
      {
         if (connections.putIfAbsent((String)connection.getID(), connection) != null)
         {
            throw new IllegalArgumentException("Connection already exists with id " + connection.getID());
         }
         
         listener.connectionCreated(connection);
      }

      public void connectionDestroyed(final Object connectionID)
      {
         if (connections.remove(connectionID) != null)
         {                    
            //Remove on the other side too
            connector.disconnect((String)connectionID);
            
            listener.connectionDestroyed(connectionID);
         }
      }

      public void connectionException(final Object connectionID, final MessagingException me)
      {
         listener.connectionException(connectionID, me);
      }
      
   }
   
}
