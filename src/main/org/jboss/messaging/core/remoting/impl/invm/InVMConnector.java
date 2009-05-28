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
import java.util.concurrent.Executor;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.spi.Acceptor;
import org.jboss.messaging.core.remoting.spi.BufferHandler;
import org.jboss.messaging.core.remoting.spi.Connection;
import org.jboss.messaging.core.remoting.spi.ConnectionLifeCycleListener;
import org.jboss.messaging.core.remoting.spi.Connector;
import org.jboss.messaging.utils.ConfigurationHelper;
import org.jboss.messaging.utils.OrderedExecutorFactory;

/**
 * A InVMConnector
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class InVMConnector implements Connector
{
   public static final Logger log = Logger.getLogger(InVMConnector.class);

   // Used for testing failure only
   public static volatile boolean failOnCreateConnection;

   public static volatile int numberOfFailures = -1;

   private static volatile int failures;

   public static synchronized void resetFailures()
   {
      failures = 0;
      failOnCreateConnection = false;
      numberOfFailures = -1;
   }

   private static synchronized void incFailures()
   {
      failures++;
      if (failures == numberOfFailures)
      {
         resetFailures();
      }
   }

   protected final int id;

   private final BufferHandler handler;

   private final ConnectionLifeCycleListener listener;

   private final InVMAcceptor acceptor;

   private ConcurrentMap<String, Connection> connections = new ConcurrentHashMap<String, Connection>();

   private volatile boolean started;

   protected final OrderedExecutorFactory executorFactory;

   public InVMConnector(final Map<String, Object> configuration,
                        final BufferHandler handler,
                        final ConnectionLifeCycleListener listener,
                        final Executor threadPool)
   {
      this.listener = listener;

      this.id = ConfigurationHelper.getIntProperty(TransportConstants.SERVER_ID_PROP_NAME, 0, configuration);

      this.handler = handler;

      this.executorFactory = new OrderedExecutorFactory(threadPool);

      InVMRegistry registry = InVMRegistry.instance;

      acceptor = registry.getAcceptor(id);
   }

   public Acceptor getAcceptor()
   {
      return acceptor;
   }

   public synchronized void close()
   {
      if (!started)
      {
         return;
      }

      for (Connection connection : connections.values())
      {
         listener.connectionDestroyed(connection.getID());
      }

      started = false;
   }

   public boolean isStarted()
   {
      return started;
   }

   public Connection createConnection()
   {
      if (failOnCreateConnection)
      {
         incFailures();
         // For testing only
         return null;
      }

      Connection conn = internalCreateConnection(acceptor.getHandler(), new Listener(), acceptor.getExecutorFactory().getExecutor());

      acceptor.connect((String)conn.getID(), handler, this, executorFactory.getExecutor());

      return conn;
   }

   public synchronized void start()
   {
      started = true;
   }

   public BufferHandler getHandler()
   {
      return handler;
   }

   public void disconnect(final String connectionID)
   {
      if (!started)
      {
         return;
      }

      Connection conn = connections.get(connectionID);

      if (conn != null)
      {
         conn.close();
      }
   }

   // This may be an injection point for mocks on tests
   protected Connection internalCreateConnection(final BufferHandler handler,
                                                 final ConnectionLifeCycleListener listener,
                                                 final Executor serverExecutor)
   {
      return new InVMConnection(id, handler, listener, serverExecutor);
   }

   private class Listener implements ConnectionLifeCycleListener
   {
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
            // Close the corresponding connection on the other side
            acceptor.disconnect((String)connectionID);
            
            // Execute on different thread to avoid deadlocks
            new Thread()
            {
               public void run()
               {
                  listener.connectionDestroyed(connectionID);                  
               }
            }.start();
         }
      }

      public void connectionException(final Object connectionID, final MessagingException me)
      {                  
         // Execute on different thread to avoid deadlocks
         new Thread()
         {
            public void run()
            {
               listener.connectionException(connectionID, me);       
            }
         }.start();
      }

   }

}
