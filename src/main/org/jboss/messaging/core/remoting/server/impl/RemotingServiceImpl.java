/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors by
 * the @authors tag. See the copyright.txt in the distribution for a full listing of individual contributors. This is
 * free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of the License, or (at your option) any later version.
 * This software is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public License along with this software; if not,
 * write to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.core.remoting.server.impl;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.DISCONNECT;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.PING;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.ManagementService;
import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.remoting.ChannelHandler;
import org.jboss.messaging.core.remoting.Interceptor;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.AbstractBufferHandler;
import org.jboss.messaging.core.remoting.impl.Pinger;
import org.jboss.messaging.core.remoting.impl.RemotingConnectionImpl;
import org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.Ping;
import org.jboss.messaging.core.remoting.server.RemotingService;
import org.jboss.messaging.core.remoting.spi.Acceptor;
import org.jboss.messaging.core.remoting.spi.AcceptorFactory;
import org.jboss.messaging.core.remoting.spi.BufferHandler;
import org.jboss.messaging.core.remoting.spi.Connection;
import org.jboss.messaging.core.remoting.spi.ConnectionLifeCycleListener;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.impl.MessagingServerPacketHandler;

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

   private static final long INITIAL_PING_TIMEOUT = 10000;

   // Attributes ----------------------------------------------------

   private volatile boolean started = false;

   private final Set<TransportConfiguration> transportConfigs;

   private final List<Interceptor> interceptors = new ArrayList<Interceptor>();

   private final Set<Acceptor> acceptors = new HashSet<Acceptor>();

   private final Map<Object, RemotingConnection> connections = new ConcurrentHashMap<Object, RemotingConnection>();

   private final BufferHandler bufferHandler = new DelegatingBufferHandler();

   private final Configuration config;

   private volatile MessagingServer server;

   private ManagementService managementService;

   private volatile RemotingConnection serverSideReplicatingConnection;

   private final Executor threadPool;

   private final ScheduledExecutorService scheduledThreadPool;

   private Map<Object, FailedConnectionRunnable> connectionTTLRunnables = new ConcurrentHashMap<Object, FailedConnectionRunnable>();

   private Map<Object, Pinger> pingRunnables = new ConcurrentHashMap<Object, Pinger>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public RemotingServiceImpl(final Configuration config,
                              final MessagingServer server,
                              final ManagementService managementService,
                              final Executor threadPool,
                              final ScheduledExecutorService scheduledThreadPool)
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
   }

   // RemotingService implementation -------------------------------

   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }

      // when JMX is enabled, it requires a INVM acceptor to send the core messages
      // corresponding to the JMX management operations (@see ReplicationAwareStandardMBeanWrapper)
      if (config.isJMXManagementEnabled())
      {
         boolean invmAcceptorConfigured = false;

         for (TransportConfiguration config : transportConfigs)
         {
            if (InVMAcceptorFactory.class.getName().equals(config.getFactoryClassName()))
            {
               invmAcceptorConfigured = true;
            }
         }

         if (!invmAcceptorConfigured)
         {
            transportConfigs.add(new TransportConfiguration(InVMAcceptorFactory.class.getName(),
                                                            new HashMap<String, Object>(),
                                                            "in-vm"));
         }
      }

      ClassLoader loader = Thread.currentThread().getContextClassLoader();

      for (TransportConfiguration info : transportConfigs)
      {
         try
         {
            Class<?> clazz = loader.loadClass(info.getFactoryClassName());

            AcceptorFactory factory = (AcceptorFactory)clazz.newInstance();

            Acceptor acceptor = factory.createAcceptor(info.getParams(), bufferHandler, this, threadPool);

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

      for (Acceptor a : acceptors)
      {
         a.start();
      }

      started = true;
   }

   public synchronized void freeze()
   {
      // Used in testing - prevents service taking any more connections

      for (Acceptor acceptor : acceptors)
      {
         acceptor.pause();
      }
   }

   public synchronized void stop() throws Exception
   {
      if (!started)
      {
         return;
      }

      // We need to stop them accepting first so no new connections are accepted after we send the disconnect message
      for (Acceptor acceptor : acceptors)
      {
         acceptor.pause();
      }

      for (RemotingConnection connection : connections.values())
      {
         connection.getChannel(0, -1, false).sendAndFlush(new PacketImpl(DISCONNECT));
      }

      for (Acceptor acceptor : acceptors)
      {
         acceptor.stop();
      }

      acceptors.clear();

      for (FailedConnectionRunnable runnable : connectionTTLRunnables.values())
      {
         runnable.close();
      }

      for (Pinger runnable : pingRunnables.values())
      {
         runnable.close();
      }

      connections.clear();

      started = false;
   }

   public boolean isStarted()
   {
      return started;
   }

   public RemotingConnection getConnection(final Object remotingConnectionID)
   {
      return connections.get(remotingConnectionID);
   }

   public RemotingConnection removeConnection(final Object remotingConnectionID)
   {
      return closeConnection(remotingConnectionID);
   }

   public synchronized Set<RemotingConnection> getConnections()
   {
      return new HashSet<RemotingConnection>(connections.values());
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

      RemotingConnection rc = new RemotingConnectionImpl(connection, interceptors, !config.isBackup());

      Channel channel1 = rc.getChannel(1, -1, false);

      ChannelHandler handler = new MessagingServerPacketHandler(server, channel1, rc);

      channel1.setHandler(handler);

      Channel channel0 = rc.getChannel(0, -1, false);

      Channel0Handler channel0Handler = new Channel0Handler(rc);

      channel0.setHandler(channel0Handler);

      Object id = connection.getID();

      connections.put(id, rc);

      InitialPingTimeout runnable = new InitialPingTimeout(rc, channel0Handler);

      // We schedule an initial ping timeout. An inital ping is always sent from the client as the first thing it
      // does after creating a connection, this contains the ping period and connection TTL, if it doesn't
      // arrive the connection will get closed
      scheduledThreadPool.schedule(runnable, INITIAL_PING_TIMEOUT, TimeUnit.MILLISECONDS);

      if (config.isBackup())
      {
         serverSideReplicatingConnection = rc;
      }
   }

   public void connectionDestroyed(final Object connectionID)
   {
      RemotingConnection conn = connections.get(connectionID);

      if (conn != null)
      {
         // if the connection has no failure listeners it means the sesssions etc were already closed so this is a clean
         // shutdown, therefore we can destroy the connection
         // otherwise client might have crashed/exited without closing connections so we leave them for connection TTL

         if (conn.getFailureListeners().isEmpty())
         {
            closeConnection(connectionID);

            conn.destroy();
         }
      }
   }

   public void connectionException(final Object connectionID, final MessagingException me)
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

   public void cancelPingerForConnectionID(final Object connectionID)
   {
      Pinger pinger = pingRunnables.get(connectionID);
      
      pinger.close();
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void setupScheduledRunnables(final RemotingConnection conn,
                                        final long clientFailureCheckPeriod,
                                        final long connectionTTL)
   {
      if ((connectionTTL <= 0 || clientFailureCheckPeriod <= 0) && connectionTTL != -1 && clientFailureCheckPeriod != -1)
      {
         log.warn("Invalid values of connectionTTL/clientFailureCheckPeriod");

         closeConnection(conn.getID());

         return;
      }

      long connectionTTLToUse = config.getConnectionTTLOverride() != -1 ? config.getConnectionTTLOverride()
                                                                       : connectionTTL;

      if (connectionTTLToUse != -1)
      {
         FailedConnectionRunnable runnable = new FailedConnectionRunnable(conn);
       
         Future<?> connectionTTLFuture = scheduledThreadPool.scheduleAtFixedRate(runnable,
                                                                                 connectionTTLToUse,
                                                                                 connectionTTLToUse,
                                                                                 TimeUnit.MILLISECONDS);

         runnable.setFuture(connectionTTLFuture);

         connectionTTLRunnables.put(conn.getID(), runnable);
      }

      long pingPeriod = clientFailureCheckPeriod == -1 ? -1 : clientFailureCheckPeriod / 2;

      if (pingPeriod != -1)
      {
         Pinger pingRunnable = new Pinger(conn);

         Future<?> pingFuture = scheduledThreadPool.scheduleAtFixedRate(pingRunnable, 0, pingPeriod, TimeUnit.MILLISECONDS);         

         pingRunnable.setFuture(pingFuture);

         pingRunnables.put(conn.getID(), pingRunnable);
      }
   }

   private RemotingConnection closeConnection(final Object connectionID)
   {
      RemotingConnection connection = connections.remove(connectionID);

      FailedConnectionRunnable runnable = connectionTTLRunnables.remove(connectionID);

      if (runnable != null)
      {
         runnable.close();
      }

      Pinger pingRunnable = pingRunnables.remove(connectionID);

      if (pingRunnable != null)
      {
         pingRunnable.close();
      }

      return connection;
   }

   // Inner classes -------------------------------------------------

   private class Channel0Handler implements ChannelHandler
   {
      private final RemotingConnection conn;

      private volatile boolean gotInitialPing;

      private Channel0Handler(final RemotingConnection conn)
      {
         this.conn = conn;
      }

      public void handlePacket(final Packet packet)
      {
         final byte type = packet.getType();

         if (type == PING)
         {
            if (!gotInitialPing)
            {
               Ping ping = (Ping)packet;

               setupScheduledRunnables(conn, ping.getClientFailureCheckPeriod(), ping.getConnectionTTL());

               gotInitialPing = true;
            }
         }
         else
         {
            throw new IllegalArgumentException("Invalid packet: " + packet);
         }
      }

      private boolean isGotInitialPing()
      {
         return gotInitialPing;
      }
   }

   private class InitialPingTimeout implements Runnable
   {
      private final RemotingConnection conn;

      private final Channel0Handler handler;

      private InitialPingTimeout(final RemotingConnection conn, final Channel0Handler handler)
      {
         this.conn = conn;

         this.handler = handler;
      }

      public void run()
      {
         if (!handler.isGotInitialPing())
         {
            // Never received initial ping
            log.warn("Did not receive initial ping for connection, it will be closed");

            closeConnection(conn);

            conn.destroy();
         }
      }
   }

   private class FailedConnectionRunnable implements Runnable
   {
      private boolean closed;

      private RemotingConnection conn;

      private Future<?> future;

      FailedConnectionRunnable(final RemotingConnection conn)
      {
         this.conn = conn;
      }

      public synchronized void setFuture(final Future<?> future)
      {
         this.future = future;
      }

      public synchronized void run()
      {
         if (closed)
         {
            return;
         }
         
         if (!conn.isDataReceived())
         {
            removeConnection(conn.getID());

            MessagingException me = new MessagingException(MessagingException.CONNECTION_TIMEDOUT,
                                                           "Did not receive ping on connection. It is likely a client has exited or crashed without " + "closing its connection, or the network between the server and client has failed. The connection will now be closed.");

            conn.fail(me);
         }
         else
         {           
            conn.clearDataReceived();
         }
      }

      public synchronized void close()
      {
         future.cancel(false);

         closed = true;
      }
   }

   private class DelegatingBufferHandler extends AbstractBufferHandler
   {
      public void bufferReceived(final Object connectionID, final MessagingBuffer buffer)
      {
         RemotingConnection conn = connections.get(connectionID);

         if (conn != null)
         {
            conn.bufferReceived(connectionID, buffer);
         }
      }
   }

}