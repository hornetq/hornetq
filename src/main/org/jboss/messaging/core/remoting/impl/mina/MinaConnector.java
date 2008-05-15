/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina;

import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addCodecFilter;
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addKeepAliveFilter;
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addSSLFilter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.mina.common.CloseFuture;
import org.apache.mina.common.ConnectFuture;
import org.apache.mina.common.DefaultIoFilterChainBuilder;
import org.apache.mina.common.IdleStatus;
import org.apache.mina.common.IoService;
import org.apache.mina.common.IoServiceListener;
import org.apache.mina.common.IoSession;
import org.apache.mina.filter.ssl.SslFilter;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.jboss.messaging.core.client.ConnectionParams;
import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.client.RemotingSessionListener;
import org.jboss.messaging.core.client.impl.ConnectionParamsImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.KeepAliveFactory;
import org.jboss.messaging.core.remoting.NIOConnector;
import org.jboss.messaging.core.remoting.NIOSession;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.impl.wireformat.Ping;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class MinaConnector implements NIOConnector, CleanUpNotifier
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(MinaConnector.class);
   
   private static boolean trace = log.isTraceEnabled();
   
   // Attributes ----------------------------------------------------

   private Location location;

   private ConnectionParams connectionParams;

   private transient NioSocketConnector connector;

   private PacketDispatcher dispatcher;

   private ExecutorService threadPool;
   
   private IoSession session;

   private List<RemotingSessionListener> listeners = new ArrayList<RemotingSessionListener>();
   private IoServiceListenerAdapter ioListener;
   
   private MinaHandler handler;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
   public MinaConnector(Location location, PacketDispatcher dispatcher)
   {
      this(location, new ConnectionParamsImpl(),  dispatcher, new ClientKeepAliveFactory());
   }

   public MinaConnector(Location location, ConnectionParams connectionParams, PacketDispatcher dispatcher)
   {
      this(location, connectionParams,  dispatcher, new ClientKeepAliveFactory());
   }

   public MinaConnector(Location location, PacketDispatcher dispatcher,
         KeepAliveFactory keepAliveFactory)
   {
      this(location, new ConnectionParamsImpl(), dispatcher, keepAliveFactory);
   }

   public MinaConnector(Location location, ConnectionParams connectionParams, PacketDispatcher dispatcher,
         KeepAliveFactory keepAliveFactory)
   {
      assert location != null;
      assert dispatcher != null;
      assert keepAliveFactory != null;
      assert connectionParams != null;

      this.location = location;
      this.connectionParams = connectionParams;
      this.dispatcher = dispatcher;

      this.connector = new NioSocketConnector();
      DefaultIoFilterChainBuilder filterChain = connector.getFilterChain();
      
      connector.setSessionDataStructureFactory(new MessagingIOSessionDataStructureFactory());

      // addMDCFilter(filterChain);
      if (connectionParams.isSSLEnabled())
      {
         try
         {
            addSSLFilter(filterChain, true, connectionParams.getKeyStorePath(), connectionParams.getKeyStorePassword(), null, null);
         } catch (Exception e)
         {
            IllegalStateException ise = new IllegalStateException("Unable to create MinaConnector for " + location);
            ise.initCause(e);
            throw ise;
         }
      }
      addCodecFilter(filterChain);
//      addKeepAliveFilter(filterChain, keepAliveFactory, connectionParams.getKeepAliveInterval(),
//            connectionParams.getKeepAliveTimeout(), this);
      connector.getSessionConfig().setTcpNoDelay(connectionParams.isTcpNoDelay());
      int receiveBufferSize = connectionParams.getTcpReceiveBufferSize();
      if (receiveBufferSize != -1)
      {
         connector.getSessionConfig().setReceiveBufferSize(receiveBufferSize);
      }
      int sendBufferSize = connectionParams.getTcpSendBufferSize();
      if (sendBufferSize != -1)
      {
         connector.getSessionConfig().setSendBufferSize(sendBufferSize);
      }
      connector.getSessionConfig().setKeepAlive(true);
      connector.getSessionConfig().setReuseAddress(true);
   }

   // NIOConnector implementation -----------------------------------

   public NIOSession connect() throws IOException
   {
      if (session != null && session.isConnected())
      {
         return new MinaSession(session, handler);
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
      this.session = future.getSession();
//      Packet packet = new Ping(session.getId());
//      session.write(packet);
      
      return new MinaSession(session, handler);
   }

   public boolean disconnect()
   {
      if (session == null)
      {
         return false;
      }

      CloseFuture closeFuture = session.close().awaitUninterruptibly();
      boolean closed = closeFuture.isClosed();
      
      connector.removeListener(ioListener);
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
         } catch (Exception e)
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
         log.trace("added listener " + listener + " to " + this);
   }

   public synchronized void removeSessionListener(RemotingSessionListener listener)
   {
      assert listener != null;
      assert connector != null;

      listeners.remove(listener);

      if (trace)
         log.trace("removed listener " + listener + " from " + this);
   }

   public String getServerURI()
   { 
      return location.getLocation() + connectionParams.getURI();
   }
   
   // FailureNotifier implementation -------------------------------
   
   public synchronized void fireCleanup(long sessionID, MessagingException me)
   {
      for (RemotingSessionListener listener: listeners)
      {
         listener.sessionDestroyed(sessionID, me);
      }
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
            log.trace("activated " + service);
      }

      public void serviceDeactivated(IoService service)
      {
         if (trace)
            log.trace("deactivated " + service);
      }

      public void serviceIdle(IoService service, IdleStatus idleStatus)
      {
         if (trace)
            log.trace("idle " + service + ", status=" + idleStatus);
      }

      public void sessionCreated(IoSession session)
      {
         if (trace)
            log.trace("created session " + session);
      }

      public void sessionDestroyed(IoSession session)
      {
         fireCleanup(session.getId(), 
               new MessagingException(MessagingException.INTERNAL_ERROR, "MINA session has been destroyed"));
      }
   }
}
