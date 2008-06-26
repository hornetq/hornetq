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

package org.jboss.messaging.core.remoting.impl.mina;

import org.apache.mina.common.*;
import org.apache.mina.filter.ssl.SslFilter;
import org.apache.mina.transport.socket.SocketConnector;
import org.apache.mina.transport.socket.SocketSessionConfig;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.jboss.messaging.core.client.ConnectionParams;
import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.client.RemotingSessionListener;
import org.jboss.messaging.core.client.impl.ConnectionParamsImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.ping.Pinger;
import org.jboss.messaging.core.ping.impl.PingerImpl;
import org.jboss.messaging.core.remoting.*;
import org.jboss.messaging.core.remoting.impl.ResponseHandlerImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.Ping;
import org.jboss.messaging.core.remoting.impl.wireformat.Pong;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * @version <tt>$Revision$</tt>
 */
public class MinaConnector implements RemotingConnector, CleanUpNotifier
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(MinaConnector.class);

   private static boolean trace = log.isTraceEnabled();

   // Attributes ----------------------------------------------------

   private final Location location;

   private final ConnectionParams connectionParams;
   
   private final FilterChainSupport chainSupport;

   private transient SocketConnector connector;
   
   private transient SocketSessionConfig connectorConfig;

   private final PacketDispatcher dispatcher;

   private ExecutorService threadPool;

   private IoSession session;

   private final List<RemotingSessionListener> listeners = new ArrayList<RemotingSessionListener>();

   private IoServiceListenerAdapter ioListener;

   private MinaHandler handler;

   private final ScheduledExecutorService scheduledExecutor;

   private boolean alive = true;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public MinaConnector(final Location location, final PacketDispatcher dispatcher)
   {
      this(location, new ConnectionParamsImpl(), dispatcher);
   }

   public MinaConnector(final Location location, final ConnectionParams connectionParams, final PacketDispatcher dispatcher)
   {
      this(location, connectionParams, new NioSocketConnector(), new FilterChainSupportImpl(),dispatcher);
   }
   
   public MinaConnector(final Location location, final ConnectionParams connectionParams, final SocketConnector connector, 
                        FilterChainSupport chainSupport, final PacketDispatcher dispatcher)
   {
      if (location == null)
      {
         throw new IllegalArgumentException("Invalid argument null location");
      }
      
      if (dispatcher == null)
      {
         throw new IllegalArgumentException("Invalid argument null dispatcher");
      }
      
      if (connectionParams == null)
      {
         throw new IllegalArgumentException("Invalid argument null connectionParams");
      }
      
      if (connector == null)
      {
         throw new IllegalArgumentException("Invalid argument null connector");
      }

      if (chainSupport == null)
      {
         throw new IllegalArgumentException("Invalid argument null chainSupport");
      }

      this.location = location;
      this.connectionParams = connectionParams;
      this.dispatcher = dispatcher;
      this.connector = connector;
      this.connectorConfig = connector.getSessionConfig();
      this.chainSupport = chainSupport;
      
      
      DefaultIoFilterChainBuilder filterChain = connector.getFilterChain();

      connector.setSessionDataStructureFactory(new MessagingIOSessionDataStructureFactory());

      // addMDCFilter(filterChain);
      if (connectionParams.isSSLEnabled())
      {
         try
         {
            this.chainSupport.addSSLFilter(filterChain, true, connectionParams.getKeyStorePath(), connectionParams.getKeyStorePassword(), null, null);
         }
         catch (Exception e)
         {
            IllegalStateException ise = new IllegalStateException("Unable to create MinaConnector for " + location);
            ise.initCause(e);
            throw ise;
         }
      }
      this.chainSupport.addCodecFilter(filterChain);
      connectorConfig.setTcpNoDelay(connectionParams.isTcpNoDelay());
      int receiveBufferSize = connectionParams.getTcpReceiveBufferSize();
      if (receiveBufferSize != -1)
      {
         connectorConfig.setReceiveBufferSize(receiveBufferSize);
      }
      int sendBufferSize = connectionParams.getTcpSendBufferSize();
      if (sendBufferSize != -1)
      {
         connectorConfig.setSendBufferSize(sendBufferSize);
      }
      connectorConfig.setKeepAlive(true);
      connectorConfig.setReuseAddress(true);

      scheduledExecutor = new ScheduledThreadPoolExecutor(1);
   }

   // NIOConnector implementation -----------------------------------

   public RemotingSession connect() throws IOException
   {
      if (session != null && session.isConnected())
      {
         return new MinaSession(session);
      }

      threadPool = Executors.newCachedThreadPool();
      //We don't order executions in the handler for messages received - this is done in the ClientConsumeImpl
      //since they are put on the queue in order
      handler = new MinaHandler(dispatcher, threadPool, this, false, false);
      connector.setHandler(handler);
      InetSocketAddress address = new InetSocketAddress(location.getHost(), location.getPort());
      ConnectFuture future = connector.connect(address);
      connector.setDefaultRemoteAddress(address);
      ioListener = new IoServiceListenerAdapter();
      connector.addListener(ioListener);

      future.awaitUninterruptibly();
      if (!future.isConnected())
      {
         throw new IOException("Cannot connect to " + address.toString());
      }
      session = future.getSession();
      
      log.info("Connected on session " + session);

      MinaSession minaSession = new MinaSession(session);
      //register a handler for dealing with server pings
      dispatcher.register(new PacketHandler()
      {
         public long getID()
         {
            return 0;
         }

         public void handle(Packet packet, PacketReturner sender)
         {
            Ping decodedPing = (Ping) packet;
            Pong pong = new Pong(decodedPing.getSessionID(), !alive);
            pong.setTargetID(decodedPing.getResponseTargetID());
            try
            {
               sender.send(pong);
            }
            catch (Exception e)
            {
               log.warn("unable to pong server");
            }
         }
      });
      /**
       * if we are a TCP transport start pinging the server
       */
      if (connectionParams.getPingInterval() > 0 && location.getTransport() == TransportType.TCP)
      {
         ResponseHandler pongHandler = new ResponseHandlerImpl(dispatcher.generateID());
         Pinger pinger = new PingerImpl(dispatcher, minaSession, connectionParams.getPingTimeout(), pongHandler, this);
         scheduledExecutor.scheduleAtFixedRate(pinger, connectionParams.getPingInterval(), connectionParams.getPingInterval(), TimeUnit.MILLISECONDS);
      }
      return minaSession;
   }

   public synchronized boolean disconnect()
   {
      if (session == null)
      {
         // TODO: shouldn't this throw an exception such as not initialized, not connected instead of return false? (written by Clebert)
         return false;
      }
      alive = false;
      scheduledExecutor.shutdownNow();
      connector.removeListener(ioListener);
      CloseFuture closeFuture = session.close().awaitUninterruptibly();
      boolean closed = closeFuture.isClosed();

      connector.dispose();
      threadPool.shutdown();

      SslFilter sslFilter = (SslFilter) session.getFilterChain().get("ssl");
      // FIXME without this hack, exceptions are thrown:
      // "Unexpected exception from SSLEngine.closeInbound()." -> because the ssl session is not stopped
      // "java.io.IOException: Connection reset by peer" -> on the server side
      if (sslFilter != null)
      {
         try
         {
            sslFilter.stopSsl(session).awaitUninterruptibly();
            Thread.sleep(500);
         }
         catch (Exception e)
         {
            // ignore
         }
      }

      connector = null;
      session = null;

      return closed;
   }

   public synchronized void addSessionListener(final RemotingSessionListener listener)
   {
      assert listener != null;
      assert connector != null;

      listeners.add(listener);

      if (trace)
      {
         log.trace("added listener " + listener + " to " + this);
      }
   }

   public synchronized void removeSessionListener(RemotingSessionListener listener)
   {
      assert listener != null;
      assert connector != null;

      listeners.remove(listener);

      if (trace)
      {
         log.trace("removed listener " + listener + " from " + this);
      }
   }

   public Location getLocation()
   {
      return location;
   }
   
   public ConnectionParams getConnectionParams()
   {
      return connectionParams;
   }

   public PacketDispatcher getDispatcher()
   {
      return dispatcher;
   }

   // FailureNotifier implementation -------------------------------

   public synchronized void fireCleanup(long sessionID, MessagingException me)
   {
      scheduledExecutor.shutdownNow();
      alive = false;
      for (RemotingSessionListener listener : listeners)
      {
         listener.sessionDestroyed(sessionID, me);
      }

      session = null;
      connector = null;
   }

   public void fireCleanup(long sessionID)
   {
      fireCleanup(sessionID, null);
   }

   // Public --------------------------------------------------------

   @Override
   public String toString()
   {
      return "MinaConnector@" + System.identityHashCode(this) + "[configuration=" + location + "]";
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private final class IoServiceListenerAdapter implements IoServiceListener
   {
      private final Logger log = Logger
              .getLogger(IoServiceListenerAdapter.class);

      private IoServiceListenerAdapter()
      {
      }

      public void serviceActivated(IoService service)
      {
         if (trace)
         {
            log.trace("activated " + service);
         }
      }

      public void serviceDeactivated(IoService service)
      {
         if (trace)
         {
            log.trace("deactivated " + service);
         }
      }

      public void serviceIdle(IoService service, IdleStatus idleStatus)
      {
         if (trace)
         {
            log.trace("idle " + service + ", status=" + idleStatus);
         }
      }

      public void sessionCreated(IoSession session)
      {
         if (trace)
         {
            log.trace("created session " + session);
         }
      }

      public void sessionDestroyed(IoSession session)
      {
         fireCleanup(session.getId(),
                 new MessagingException(MessagingException.INTERNAL_ERROR, "MINA session has been destroyed"));
      }
   }
}
