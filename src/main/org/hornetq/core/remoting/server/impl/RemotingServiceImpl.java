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

package org.hornetq.core.remoting.server.impl;

import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.DISCONNECT;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.management.ManagementService;
import org.hornetq.core.remoting.Channel;
import org.hornetq.core.remoting.ChannelHandler;
import org.hornetq.core.remoting.Interceptor;
import org.hornetq.core.remoting.Packet;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.AbstractBufferHandler;
import org.hornetq.core.remoting.impl.RemotingConnectionImpl;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.TransportConstants;
import org.hornetq.core.remoting.impl.wireformat.PacketImpl;
import org.hornetq.core.remoting.impl.wireformat.Ping;
import org.hornetq.core.remoting.server.RemotingService;
import org.hornetq.core.remoting.spi.Acceptor;
import org.hornetq.core.remoting.spi.AcceptorFactory;
import org.hornetq.core.remoting.spi.BufferHandler;
import org.hornetq.core.remoting.spi.Connection;
import org.hornetq.core.remoting.spi.ConnectionLifeCycleListener;
import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.impl.HornetQPacketHandler;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public class RemotingServiceImpl implements RemotingService, ConnectionLifeCycleListener
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(RemotingServiceImpl.class);

   private static final long CONNECTION_TTL_CHECK_INTERVAL = 2000;

   // Attributes ----------------------------------------------------

   private volatile boolean started = false;

   private final Set<TransportConfiguration> transportConfigs;

   private final List<Interceptor> interceptors = new CopyOnWriteArrayList<Interceptor>();

   private final Set<Acceptor> acceptors = new HashSet<Acceptor>();

   private final Map<Object, ConnectionEntry> connections = new ConcurrentHashMap<Object, ConnectionEntry>();

   private final BufferHandler bufferHandler = new DelegatingBufferHandler();

   private final Configuration config;

   private volatile HornetQServer server;

   private ManagementService managementService;

   private volatile RemotingConnection serverSideReplicatingConnection;

   private final Executor threadPool;

   private final ScheduledExecutorService scheduledThreadPool;

   private final int managementConnectorID;

   private FailureCheckThread failureCheckThread;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public RemotingServiceImpl(final Configuration config,
                              final HornetQServer server,
                              final ManagementService managementService,
                              final Executor threadPool,
                              final ScheduledExecutorService scheduledThreadPool,
                              final int managementConnectorID)
   {
      transportConfigs = config.getAcceptorConfigurations();

      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      for (String interceptorClass : config.getInterceptorClassNames())
      {
         try
         {
            Class<?> clazz = loader.loadClass(interceptorClass);
            interceptors.add((Interceptor)clazz.newInstance());
         }
         catch (Exception e)
         {
            log.warn("Error instantiating interceptor \"" + interceptorClass + "\"", e);
         }
      }

      this.config = config;
      this.server = server;
      this.managementService = managementService;
      this.threadPool = threadPool;
      this.scheduledThreadPool = scheduledThreadPool;
      this.managementConnectorID = managementConnectorID;
   }

   // RemotingService implementation -------------------------------

   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }

      ClassLoader loader = Thread.currentThread().getContextClassLoader();

      for (TransportConfiguration info : transportConfigs)
      {
         try
         {
            Class<?> clazz = loader.loadClass(info.getFactoryClassName());

            AcceptorFactory factory = (AcceptorFactory)clazz.newInstance();

            Acceptor acceptor = factory.createAcceptor(info.getParams(),
                                                       bufferHandler,
                                                       this,
                                                       threadPool,
                                                       scheduledThreadPool);

            acceptors.add(acceptor);

            if (managementService != null)
            {
               managementService.registerAcceptor(acceptor, info);
            }
         }
         catch (Exception e)
         {
            log.warn("Error instantiating acceptor \"" + info.getFactoryClassName() + "\"", e);
         }
      }

      // We now create a "special" acceptor used by management to send/receive management messages - this is an invm
      // acceptor with a -ve server id
      // TODO this is not the best solution, management should send/receive management messages direct.
      // Remove this code when this is implemented without having to require a special acceptor
      // https://jira.jboss.org/jira/browse/JBMESSAGING-1649

      if (config.isJMXManagementEnabled())
      {
         Map<String, Object> params = new HashMap<String, Object>();

         params.put(TransportConstants.SERVER_ID_PROP_NAME, managementConnectorID);

         AcceptorFactory factory = new InVMAcceptorFactory();

         Acceptor acceptor = factory.createAcceptor(params, bufferHandler, this, threadPool, scheduledThreadPool);

         acceptors.add(acceptor);

         if (managementService != null)
         {
            TransportConfiguration info = new TransportConfiguration(InVMAcceptorFactory.class.getName(), params);

            managementService.registerAcceptor(acceptor, info);
         }
      }

      for (Acceptor a : acceptors)
      {
         a.start();
      }

      failureCheckThread = new FailureCheckThread(CONNECTION_TTL_CHECK_INTERVAL);

      failureCheckThread.start();

      started = true;
   }

   public synchronized void freeze()
   {
      // Used in testing - prevents service taking any more connections

      for (Acceptor acceptor : acceptors)
      {
         try
         {
            acceptor.stop();
         }
         catch (Exception e)
         {
            log.error("Failed to stop acceptor", e);
         }
      }
   }

   public synchronized void stop() throws Exception
   {
      if (!started)
      {
         return;
      }

      failureCheckThread.close();
      
      // We need to stop them accepting first so no new connections are accepted after we send the disconnect message
      for (Acceptor acceptor : acceptors)
      {
         acceptor.pause();
      }
     
      for (ConnectionEntry entry : connections.values())
      {       
         entry.connection.getChannel(0, -1, false).sendAndFlush(new PacketImpl(DISCONNECT));
      }
           
      for (Acceptor acceptor : acceptors)
      {
         acceptor.stop();
      }
     
      acceptors.clear();

      connections.clear();

      managementService.unregisterAcceptors();

      started = false;
   }

   public boolean isStarted()
   {
      return started;
   }

   public RemotingConnection removeConnection(final Object remotingConnectionID)
   {
      ConnectionEntry entry = this.connections.remove(remotingConnectionID);

      if (entry != null)
      {
         return entry.connection;
      }
      else
      {
         return null;
      }
   }

   public synchronized Set<RemotingConnection> getConnections()
   {
      Set<RemotingConnection> conns = new HashSet<RemotingConnection>();

      for (ConnectionEntry entry : this.connections.values())
      {
         conns.add(entry.connection);
      }

      return conns;
   }

   public RemotingConnection getServerSideReplicatingConnection()
   {
      return serverSideReplicatingConnection;
   }

   // ConnectionLifeCycleListener implementation -----------------------------------

   public void connectionCreated(final Connection connection)
   {
      if (server == null)
      {
         throw new IllegalStateException("Unable to create connection, server hasn't finished starting up");
      }
      
      RemotingConnection rc = new RemotingConnectionImpl(connection,
                                                         interceptors,                                                        
                                                         server.getConfiguration().isAsyncConnectionExecutionEnabled() ? server.getExecutorFactory()
                                                                                                                               .getExecutor()
                                                                                                                      : null);

      Channel channel1 = rc.getChannel(1, -1, false);

      ChannelHandler handler = new HornetQPacketHandler(server, channel1, rc);

      channel1.setHandler(handler);

      long ttl = ClientSessionFactoryImpl.DEFAULT_CONNECTION_TTL;
      if (config.getConnectionTTLOverride() != -1)
      {
         ttl = config.getConnectionTTLOverride();
      }
      final ConnectionEntry entry = new ConnectionEntry(rc,
                                                        System.currentTimeMillis(),
                                                        ttl);

      connections.put(connection.getID(), entry);

      final Channel channel0 = rc.getChannel(0, -1, false);

      channel0.setHandler(new ChannelHandler()
      {
         public void handlePacket(final Packet packet)
         {
            if (packet.getType() == PacketImpl.PING)
            {
               Ping ping = (Ping)packet;

               if (config.getConnectionTTLOverride() == -1)
               {
                  // Allow clients to specify connection ttl
                  entry.ttl = ping.getConnectionTTL();
               }

               // Just send a ping back
               channel0.send(packet);
            }
         }
      });

      if (config.isBackup())
      {
         serverSideReplicatingConnection = rc;
      }
   }

   public void connectionDestroyed(final Object connectionID)
   {
      ConnectionEntry conn = connections.get(connectionID);

      if (conn != null)
      {
         // if the connection has no failure listeners it means the sesssions etc were already closed so this is a clean
         // shutdown, therefore we can destroy the connection
         // otherwise client might have crashed/exited without closing connections so we leave them for connection TTL

         if (conn.connection.getFailureListeners().isEmpty())
         {
            connections.remove(connectionID);

            conn.connection.destroy();
         }
      }
   }

   public void connectionException(final Object connectionID, final HornetQException me)
   {
      // We DO NOT call fail on connection exception, otherwise in event of real connection failure, the
      // connection will be failed, the session will be closed and won't be able to reconnect

      // E.g. if live server fails, then this handler wil be called on backup server for the server
      // side replicating connection.
      // If the connection fail() is called then the sessions on the backup will get closed.

      // Connections should only fail when TTL is exceeded
   }

   public void addInterceptor(final Interceptor interceptor)
   {
      interceptors.add(interceptor);
   }

   public boolean removeInterceptor(final Interceptor interceptor)
   {
      return interceptors.remove(interceptor);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private final class DelegatingBufferHandler extends AbstractBufferHandler
   {
      public void bufferReceived(final Object connectionID, final HornetQBuffer buffer)
      {
         ConnectionEntry conn = connections.get(connectionID);

         if (conn != null)
         {
            conn.connection.bufferReceived(connectionID, buffer);
         }
      }
   }

   private static final class ConnectionEntry
   {
      final RemotingConnection connection;

      volatile long lastCheck;

      volatile long ttl;

      ConnectionEntry(final RemotingConnection connection, final long lastCheck, final long ttl)
      {
         this.connection = connection;

         this.lastCheck = lastCheck;

         this.ttl = ttl;
      }
   }

   private final class FailureCheckThread extends Thread
   {
      private long pauseInterval;

      private volatile boolean closed;

      FailureCheckThread(final long pauseInterval)
      {
         this.pauseInterval = pauseInterval;
      }

      public synchronized void close()
      {
         closed = true;

         synchronized (this)
         {
            notify();
         }

         try
         {
            join();
         }
         catch (InterruptedException ignore)
         {
         }
      }

      public void run()
      {         
         while (!closed)
         {
            long now = System.currentTimeMillis();

            Set<Object> idsToRemove = new HashSet<Object>();

            for (ConnectionEntry entry : connections.values())
            {
               if (entry.ttl != -1)
               {
                  if (now >= entry.lastCheck + entry.ttl)
                  {
                     RemotingConnection conn = entry.connection;

                     if (!conn.checkDataReceived())
                     {
                        idsToRemove.add(conn.getID());
                     }
                     else
                     {
                        entry.lastCheck = now;
                     }
                  }
               }
            }

            for (Object id : idsToRemove)
            {
               RemotingConnection conn = removeConnection(id);

               HornetQException me = new HornetQException(HornetQException.CONNECTION_TIMEDOUT,
                                                              "Did not receive ping from " + conn.getRemoteAddress() +
                                                                       ". It is likely the client has exited or crashed without " +
                                                                       "closing its connection, or the network between the server and client has failed. The connection will now be closed.");
               conn.fail(me);
            }

            synchronized (this)
            {
               long toWait = pauseInterval;

               long start = System.currentTimeMillis();

               while (!closed && toWait > 0)
               {
                  try
                  {
                     wait(toWait);
                  }
                  catch (InterruptedException e)
                  {
                  }

                  now = System.currentTimeMillis();

                  toWait -= now - start;

                  start = now;
               }
            }
         }
      }
   }
}