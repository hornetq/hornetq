/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.core.remoting.impl.invm;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.spi.Acceptor;
import org.hornetq.core.remoting.spi.BufferHandler;
import org.hornetq.core.remoting.spi.Connection;
import org.hornetq.core.remoting.spi.ConnectionLifeCycleListener;
import org.hornetq.utils.ConfigurationHelper;
import org.hornetq.utils.ExecutorFactory;
import org.hornetq.utils.OrderedExecutorFactory;

/**
 * A InVMAcceptor
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class InVMAcceptor implements Acceptor
{
   private static final Logger log = Logger.getLogger(InVMAcceptor.class);

   private final int id;

   private final BufferHandler handler;

   private final ConnectionLifeCycleListener listener;

   private ConcurrentMap<String, Connection> connections = new ConcurrentHashMap<String, Connection>();

   private volatile boolean started;

   private final ExecutorFactory executorFactory;
   
   private boolean paused;

   public InVMAcceptor(final Map<String, Object> configuration,
                       final BufferHandler handler,
                       final ConnectionLifeCycleListener listener,
                       final Executor threadPool)
   {
      this.handler = handler;

      this.listener = listener;

      this.id = ConfigurationHelper.getIntProperty(TransportConstants.SERVER_ID_PROP_NAME, 0, configuration);

      this.executorFactory = new OrderedExecutorFactory(threadPool);
   }

   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }

      InVMRegistry.instance.registerAcceptor(id, this);

      started = true;
      
      paused = false;
   }

   public synchronized void stop()
   {
      if (!started)
      {
         return;
      }

      if (!paused)
      {
         InVMRegistry.instance.unregisterAcceptor(id);
      }

      for (Connection connection : connections.values())
      {
         listener.connectionDestroyed(connection.getID());
      }

      connections.clear();

      started = false;
      
      paused = false;
   }

   public synchronized boolean isStarted()
   {
      return started;
   }
   
   /*
    * Stop accepting new connections
    */
   public synchronized void pause()
   {      
      if (!started || paused)
      {
         return;
      }
      
      InVMRegistry.instance.unregisterAcceptor(id);   
      
      paused = true;
   }
   
   public synchronized void resume()
   {
      if (!paused || !started)
      {
         return;
      }
      
      InVMRegistry.instance.registerAcceptor(id, this);
      
      paused = false;
   }

   public BufferHandler getHandler()
   {
      if (!started)
      {
         throw new IllegalStateException("Acceptor is not started");
      }

      return handler;
   }

   public ExecutorFactory getExecutorFactory()
   {
      return this.executorFactory;
   }

   public void connect(final String connectionID,
                       final BufferHandler remoteHandler,
                       final InVMConnector connector,
                       final Executor clientExecutor)
   {
      if (!started)
      {
         throw new IllegalStateException("Acceptor is not started");
      }

      new InVMConnection(id, connectionID, remoteHandler, new Listener(connector), clientExecutor);
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
            listener.connectionDestroyed(connectionID);

            // Execute on different thread to avoid deadlocks
            new Thread()
            {
               public void run()
               {
                  // Remove on the other side too
                  connector.disconnect((String)connectionID);
               }
            }.start();
         }
      }

      public void connectionException(final Object connectionID, final HornetQException me)
      {
         listener.connectionException(connectionID, me);
      }

   }

}
