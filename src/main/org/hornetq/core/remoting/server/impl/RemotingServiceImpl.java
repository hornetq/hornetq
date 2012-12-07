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

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQInterruptedException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.protocol.core.impl.CoreProtocolManagerFactory;
import org.hornetq.core.protocol.stomp.StompProtocolManagerFactory;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.core.remoting.server.RemotingService;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.cluster.ClusterConnection;
import org.hornetq.core.server.cluster.ClusterManager;
import org.hornetq.core.server.impl.ServerSessionImpl;
import org.hornetq.core.server.management.ManagementService;
import org.hornetq.spi.core.protocol.ConnectionEntry;
import org.hornetq.spi.core.protocol.ProtocolManager;
import org.hornetq.spi.core.protocol.ProtocolType;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.remoting.Acceptor;
import org.hornetq.spi.core.remoting.AcceptorFactory;
import org.hornetq.spi.core.remoting.BufferHandler;
import org.hornetq.spi.core.remoting.Connection;
import org.hornetq.spi.core.remoting.ConnectionLifeCycleListener;
import org.hornetq.utils.ConfigurationHelper;
import org.hornetq.utils.HornetQThreadFactory;

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

   private static final boolean isTrace = log.isTraceEnabled();

   public static final long CONNECTION_TTL_CHECK_INTERVAL = 2000;

   // Attributes ----------------------------------------------------

   private volatile boolean started = false;

   private final Set<TransportConfiguration> acceptorsConfig;

   private final List<Interceptor> interceptors = new CopyOnWriteArrayList<Interceptor>();

   private final Set<Acceptor> acceptors = new HashSet<Acceptor>();

   private final Map<Object, ConnectionEntry> connections = new ConcurrentHashMap<Object, ConnectionEntry>();

   private final Configuration config;

   private final HornetQServer server;

   private final ManagementService managementService;

   private volatile RemotingConnection serverSideReplicatingConnection;

   private ExecutorService threadPool;

   private final ScheduledExecutorService scheduledThreadPool;

   private FailureCheckAndFlushThread failureCheckAndFlushThread;

   private final ClusterManager clusterManager;

   private Map<ProtocolType, ProtocolManager> protocolMap = new ConcurrentHashMap<ProtocolType, ProtocolManager>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public RemotingServiceImpl(final ClusterManager clusterManager,
                              final Configuration config,
                              final HornetQServer server,
                              final ManagementService managementService,
                              final ScheduledExecutorService scheduledThreadPool)
   {
      acceptorsConfig = config.getAcceptorConfigurations();

      this.server = server;

      this.clusterManager = clusterManager;

      for (String interceptorClass : config.getInterceptorClassNames())
      {
         try
         {
             interceptors.add((Interceptor) safeInitNewInstance(interceptorClass));
         }
         catch (Exception e)
         {
            RemotingServiceImpl.log.warn("Error instantiating interceptor \"" + interceptorClass + "\"", e);
         }
      }

      this.config = config;

      this.managementService = managementService;

      this.scheduledThreadPool = scheduledThreadPool;

      this.protocolMap.put(ProtocolType.CORE,
                           new CoreProtocolManagerFactory().createProtocolManager(server, interceptors));
      // difference between Stomp and Stomp over Web Sockets is handled in NettyAcceptor.getPipeline()
      this.protocolMap.put(ProtocolType.STOMP,
                           new StompProtocolManagerFactory().createProtocolManager(server, interceptors));
      this.protocolMap.put(ProtocolType.STOMP_WS,
                           new StompProtocolManagerFactory().createProtocolManager(server, interceptors));
   }

   // RemotingService implementation -------------------------------

   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }

      ClassLoader tccl = AccessController.doPrivileged(new PrivilegedAction<ClassLoader>()
      {
         public ClassLoader run()
         {
            return Thread.currentThread().getContextClassLoader();
         }
      });

      // The remoting service maintains it's own thread pool for handling remoting traffic
      // If OIO each connection will have it's own thread
      // If NIO these are capped at nio-remoting-threads which defaults to num cores * 3
      // This needs to be a different thread pool to the main thread pool especially for OIO where we may need
      // to support many hundreds of connections, but the main thread pool must be kept small for better performance

      ThreadFactory tFactory = new HornetQThreadFactory("HornetQ-remoting-threads-" + server.toString() +
                                                        "-" +
                                                        System.identityHashCode(this), false, tccl);

      threadPool = Executors.newCachedThreadPool(tFactory);

      ClassLoader loader = Thread.currentThread().getContextClassLoader();

      for (TransportConfiguration info : acceptorsConfig)
      {
         try
         {
            Class<?> clazz = loader.loadClass(info.getFactoryClassName());

            AcceptorFactory factory = (AcceptorFactory)clazz.newInstance();

            // Check valid properties

            if (info.getParams() != null)
            {
               Set<String> invalid = ConfigurationHelper.checkKeys(factory.getAllowableProperties(), info.getParams()
                                                                                                         .keySet());

               if (!invalid.isEmpty())
               {
                  RemotingServiceImpl.log.warn(ConfigurationHelper.stringSetToCommaListString("The following keys are invalid for configuring the acceptor: ",
                                                                                              invalid) + " the acceptor will not be started.");

                  continue;
               }
            }

            String protocolString = ConfigurationHelper.getStringProperty(TransportConstants.PROTOCOL_PROP_NAME,
                                                                          TransportConstants.DEFAULT_PROTOCOL,
                                                                          info.getParams());

            ProtocolType protocol = ProtocolType.valueOf(protocolString.toUpperCase());

            ProtocolManager manager = protocolMap.get(protocol);

            ClusterConnection clusterConnection = lookupClusterConnection(info);

            Acceptor acceptor = factory.createAcceptor(clusterConnection,
                                                       info.getParams(),
                                                       new DelegatingBufferHandler(),
                                                       manager,
                                                       this,
                                                       threadPool,
                                                       scheduledThreadPool);

            acceptors.add(acceptor);

            if (managementService != null)
            {
               acceptor.setNotificationService(managementService);

               managementService.registerAcceptor(acceptor, info);
            }
         }
         catch (Exception e)
         {
            RemotingServiceImpl.log.warn("Error instantiating acceptor \"" + info.getFactoryClassName() + "\"", e);
         }
      }

      for (Acceptor a : acceptors)
      {
         a.start();
      }

      // This thread checks connections that need to be closed, and also flushes confirmations
      failureCheckAndFlushThread = new FailureCheckAndFlushThread(RemotingServiceImpl.CONNECTION_TTL_CHECK_INTERVAL);

      failureCheckAndFlushThread.start();

      started = true;
   }

   public synchronized void freeze()
   {
      // Used in testing - prevents service taking any more connections

      for (Acceptor acceptor : acceptors)
      {
         try
         {
            acceptor.pause();
         }
         catch (Exception e)
         {
            RemotingServiceImpl.log.error("Failed to stop acceptor", e);
         }
      }
   }

   public void stop(final boolean criticalError) throws Exception
   {
      if (!started)
      {
         return;
      }

      if (!started)
      {
         return;
      }

      failureCheckAndFlushThread.close(criticalError);

      // We need to stop them accepting first so no new connections are accepted after we send the disconnect message
      for (Acceptor acceptor : acceptors)
      {
         if (log.isDebugEnabled())
         {
            log.debug("Pausing acceptor " + acceptor);
         }
         acceptor.pause();
      }

      if (log.isDebugEnabled())
      {
         log.debug("Sending disconnect on live connections");
      }

      HashSet<ConnectionEntry> connectionEntries = new HashSet<ConnectionEntry>();

      connectionEntries.addAll(connections.values());

      // Now we ensure that no connections will process any more packets after this method is complete
      // then send a disconnect packet
      for (ConnectionEntry entry : connectionEntries)
      {
         RemotingConnection conn = entry.connection;

         if (log.isTraceEnabled())
         {
            log.trace("Sending connection.disconnection packet to " + conn);
         }

         conn.disconnect(criticalError);
      }

      for (Acceptor acceptor : acceptors)
      {
         acceptor.stop();
      }

      acceptors.clear();

      connections.clear();

      if (managementService != null)
      {
         managementService.unregisterAcceptors();
      }

      threadPool.shutdown();

      if (!criticalError)
      {
         boolean ok = threadPool.awaitTermination(10000, TimeUnit.MILLISECONDS);

         if (!ok)
         {
            log.warn("Timed out waiting for remoting thread pool to terminate");
         }
      }

      started = false;

   }

   public boolean isStarted()
   {
      return started;
   }

   public RemotingConnection removeConnection(final Object remotingConnectionID)
   {
      ConnectionEntry entry = connections.remove(remotingConnectionID);

      if (entry != null)
      {
         return entry.connection;
      }
      else
      {
         log.info("failed to remove connection");

         return null;
      }
   }

   public synchronized Set<RemotingConnection> getConnections()
   {
      Set<RemotingConnection> conns = new HashSet<RemotingConnection>();

      for (ConnectionEntry entry : connections.values())
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

   private ProtocolManager getProtocolManager(ProtocolType protocol)
   {
      return protocolMap.get(protocol);
   }

   public void connectionCreated(final Acceptor acceptor, final Connection connection, final ProtocolType protocol)
   {
      if (server == null)
      {
         throw new IllegalStateException("Unable to create connection, server hasn't finished starting up");
      }

      ProtocolManager pmgr = this.getProtocolManager(protocol);

      if (pmgr == null)
      {
         throw new IllegalArgumentException("Unknown protocol " + protocol);
      }

      ConnectionEntry entry = pmgr.createConnectionEntry(acceptor, connection);

      if (isTrace)
      {
         log.trace("Connection created " + connection);
      }

      connections.put(connection.getID(), entry);

      if (config.isBackup())
      {
         serverSideReplicatingConnection = entry.connection;
      }
   }

   public void connectionDestroyed(final Object connectionID)
   {

      if (isTrace)
      {
         log.trace("Connection removed " + connectionID + " from server " + this.server, new Exception("trace"));
      }

      ConnectionEntry conn = connections.get(connectionID);

      if (conn != null)
      {
         // Bit of a hack - find a better way to do this

         List<FailureListener> failureListeners = conn.connection.getFailureListeners();

         boolean empty = true;

         for (FailureListener listener : failureListeners)
         {
            if (listener instanceof ServerSessionImpl)
            {
               empty = false;

               break;
            }
         }

         // We only destroy the connection if the connection has no sessions attached to it
         // Otherwise it means the connection has died without the sessions being closed first
         // so we need to keep them for ttl, in case re-attachment occurs
         if (empty)
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

   public void connectionReadyForWrites(final Object connectionID, final boolean ready)
   {
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

   private ClusterConnection lookupClusterConnection(TransportConfiguration acceptorConfig)
   {
      String clusterConnectionName = (String)acceptorConfig.getParams().get(org.hornetq.core.remoting.impl.netty.TransportConstants.CLUSTER_CONNECTION);

      ClusterConnection clusterConnection = null;
      if (clusterConnectionName != null)
      {
         clusterConnection = clusterManager.getClusterConnection(clusterConnectionName);
      }

      // if not found we will still use the default name, even if a name was provided
      if (clusterConnection == null)
      {
         clusterConnection = clusterManager.getDefaultConnection(acceptorConfig);
      }

      return clusterConnection;
   }

   // Inner classes -------------------------------------------------

   private final class DelegatingBufferHandler implements BufferHandler
   {
      public void bufferReceived(final Object connectionID, final HornetQBuffer buffer)
      {
         ConnectionEntry conn = connections.get(connectionID);

         if (conn != null)
         {
            conn.connection.bufferReceived(connectionID, buffer);
         }
         else
         {
            if (log.isTraceEnabled())
            {
               log.trace("ConnectionID = " + connectionID + " was already closed, so ignoring packet");
            }
         }
      }
   }

   private final class FailureCheckAndFlushThread extends Thread
   {
      private final long pauseInterval;

      private volatile boolean closed;

      FailureCheckAndFlushThread(final long pauseInterval)
      {
         super("hornetq-failure-check-thread");

         this.pauseInterval = pauseInterval;
      }

      public void close(final boolean criticalError)
      {
         closed = true;

         synchronized (this)
         {
            notify();
         }

         if (!criticalError)
         {
            try
            {
               join();
            }
            catch (InterruptedException e)
            {
               throw new HornetQInterruptedException(e);
            }
         }
      }

      @Override
      public void run()
      {
         while (!closed)
         {
            try
            {
               long now = System.currentTimeMillis();

               Set<Object> idsToRemove = new HashSet<Object>();

               for (ConnectionEntry entry : connections.values())
               {
                  RemotingConnection conn = entry.connection;

                  boolean flush = true;

                  if (entry.ttl != -1)
                  {
                     if (now >= entry.lastCheck + entry.ttl)
                     {
                        if (!conn.checkDataReceived())
                        {
                           idsToRemove.add(conn.getID());

                           flush = false;
                        }
                        else
                        {
                           entry.lastCheck = now;
                        }
                     }
                  }

                  if (flush)
                  {
                     conn.flush();
                  }
               }

               for (Object id : idsToRemove)
               {
                  RemotingConnection conn = removeConnection(id);
                  if (conn != null)
                  {
                     HornetQException me = new HornetQException(HornetQException.CONNECTION_TIMEDOUT,
                                                                "Did not receive data from " + conn.getRemoteAddress() +
                                                                         ". It is likely the client has exited or crashed without " +
                                                                         "closing its connection, or the network between the server and client has failed. " +
                                                                         "You also might have configured connection-ttl and client-failure-check-period incorrectly. " +
                                                                         "Please check user manual for more information." +
                                                                         " The connection will now be closed.");
                     conn.fail(me);
                  }
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
            catch (Throwable e)
            {
               log.warn(e.getMessage(), e);
            }
         }
      }
   }


   /** This seems duplicate code all over the place, but for security reasons we can't let something like this to be open in a
    *  utility class, as it would be a door to load anything you like in a safe VM.
    *  For that reason any class trying to do a privileged block should do with the AccessController directly.
    */
   private static Object safeInitNewInstance(final String className)
   {
      return AccessController.doPrivileged(new PrivilegedAction<Object>()
      {
         public Object run()
         {
            ClassLoader loader = getClass().getClassLoader();
            try
            {
               Class<?> clazz = loader.loadClass(className);
               return clazz.newInstance();
            }
            catch (Throwable t)
            {
                try
                {
                    loader = Thread.currentThread().getContextClassLoader();
                    if (loader != null)
                        return loader.loadClass(className).newInstance();
                }
                catch (RuntimeException e)
                {
                    throw e;
                }
                catch (Exception e)
                {
                }

                throw new IllegalArgumentException("Could not find class " + className);
            }
         }
      });
   }


}