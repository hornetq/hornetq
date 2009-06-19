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
import org.jboss.messaging.core.remoting.impl.invm.TransportConstants;
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

   private Map<Object, Pinger> pingers = new ConcurrentHashMap<Object, Pinger>();

   private final int managementConnectorID;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public RemotingServiceImpl(final Configuration config,
                              final MessagingServer server,
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

         Acceptor acceptor = factory.createAcceptor(params, bufferHandler, this, threadPool);

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

      for (Pinger runnable : pingers.values())
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

      connections.put(connection.getID(), rc);

      InitialPingTimeout runnable = new InitialPingTimeout(rc);

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

   public void stopPingingForConnectionID(final Object connectionID)
   {
      Pinger pinger = pingers.get(connectionID);

      pinger.stopPinging();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void setupPinger(final RemotingConnection conn,
                                        final long clientFailureCheckPeriod,
                                        final long connectionTTL)
   {
      if ((connectionTTL <= 0 || clientFailureCheckPeriod <= 0) && connectionTTL != -1 &&
          clientFailureCheckPeriod != -1)
      {
         log.warn("Invalid values of connectionTTL/clientFailureCheckPeriod");

         closeConnection(conn.getID());

         return;
      }

      long connectionTTLToUse = config.getConnectionTTLOverride() != -1 ? config.getConnectionTTLOverride()
                                                                       : connectionTTL;

      long pingPeriod = clientFailureCheckPeriod == -1 ? -1 : clientFailureCheckPeriod / 2;

      Pinger pingRunnable = new Pinger(conn, connectionTTLToUse, null, new FailedConnectionAction(conn), System.currentTimeMillis());

      Future<?> pingFuture = scheduledThreadPool.scheduleAtFixedRate(pingRunnable, 0, pingPeriod, TimeUnit.MILLISECONDS);

      pingRunnable.setFuture(pingFuture);

      pingers.put(conn.getID(), pingRunnable);
   }

   private RemotingConnection closeConnection(final Object connectionID)
   {
      RemotingConnection connection = connections.remove(connectionID);
      
      Pinger pinger = pingers.remove(connectionID);

      if (pinger != null)
      {
         pinger.close();
      }

      return connection;
   }

   // Inner classes -------------------------------------------------

   private class InitialPingTimeout implements Runnable, ChannelHandler
   {
      private final RemotingConnection conn;

      private boolean gotInitialPing;

      private InitialPingTimeout(final RemotingConnection conn)
      {
         this.conn = conn;
         
         conn.getChannel(0, -1, false).setHandler(this);
      }
      
      public synchronized void handlePacket(final Packet packet)
      {
         final byte type = packet.getType();

         if (type == PING)
         {
            if (!gotInitialPing)
            {
               Ping ping = (Ping)packet;

               setupPinger(conn, ping.getClientFailureCheckPeriod(), ping.getConnectionTTL());

               gotInitialPing = true;
            }
         }
         else
         {
            throw new IllegalArgumentException("Invalid packet: " + packet);
         }
      }

      public synchronized void run()
      {
         if (!gotInitialPing)
         {
            // Never received initial ping
            log.warn("Did not receive initial ping for connection, it will be closed");

            closeConnection(conn);

            conn.destroy();
         }
      }
   }

   private class FailedConnectionAction implements Runnable
   {
      private RemotingConnection conn;

      FailedConnectionAction(final RemotingConnection conn)
      {
         this.conn = conn;
      }

      public synchronized void run()
      {
         removeConnection(conn.getID());

         MessagingException me = new MessagingException(MessagingException.CONNECTION_TIMEDOUT,
                                                        "Did not receive ping from client. It is likely a client has exited or crashed without " + "closing its connection, or the network between the server and client has failed. The connection will now be closed.");

         conn.fail(me);
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