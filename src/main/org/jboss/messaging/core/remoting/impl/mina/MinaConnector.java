/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina;

import static org.jboss.messaging.core.remoting.TransportType.TCP;
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addBlockingRequestResponseFilter;
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addCodecFilter;
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addExecutorFilter;
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addKeepAliveFilter;
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addLoggingFilter;
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addMDCFilter;
import static org.jboss.messaging.core.remoting.impl.mina.MinaService.KEEP_ALIVE_INTERVAL_KEY;
import static org.jboss.messaging.core.remoting.impl.mina.MinaService.KEEP_ALIVE_TIMEOUT_KEY;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

import org.apache.mina.common.CloseFuture;
import org.apache.mina.common.ConnectFuture;
import org.apache.mina.common.DefaultIoFilterChainBuilder;
import org.apache.mina.common.IdleStatus;
import org.apache.mina.common.IoService;
import org.apache.mina.common.IoServiceListener;
import org.apache.mina.common.IoSession;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.jboss.jms.client.remoting.ConsolidatedRemotingConnectionListener;
import org.jboss.messaging.core.remoting.KeepAliveFactory;
import org.jboss.messaging.core.remoting.ConnectionExceptionListener;
import org.jboss.messaging.core.remoting.NIOConnector;
import org.jboss.messaging.core.remoting.NIOSession;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.ServerLocator;
import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;
import org.jboss.messaging.core.remoting.wireformat.SetSessionIDMessage;
import org.jboss.messaging.util.Logger;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class MinaConnector implements NIOConnector, KeepAliveNotifier
{
   // Constants -----------------------------------------------------

   private final Logger log = Logger.getLogger(MinaConnector.class);

   // Attributes ----------------------------------------------------

   private String host;

   private int port;

   private NioSocketConnector connector;

   private ScheduledExecutorService blockingScheduler;

   private IoSession session;

   // FIXME clean up this listener mess
   private Map<ConsolidatedRemotingConnectionListener, IoServiceListener> listeners = new HashMap<ConsolidatedRemotingConnectionListener, IoServiceListener>();
   private ConnectionExceptionListener listener;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public MinaConnector(ServerLocator locator)
   {
      this(locator.getTransport(), locator.getHost(), locator.getPort(),
            locator.getParameters(), new ClientKeepAliveFactory());
   }

   public MinaConnector(ServerLocator locator, KeepAliveFactory keepAliveFactory)
   {
      this(locator.getTransport(), locator.getHost(), locator.getPort(),
            locator.getParameters(), keepAliveFactory);
   }

   public MinaConnector(TransportType transport, String host, int port)
   {
      this(transport, host, port, new HashMap<String, String>(),
            new ClientKeepAliveFactory());
   }

   private MinaConnector(TransportType transport, String host, int port,
         Map<String, String> parameters, KeepAliveFactory keepAliveFactory)
   {
      assert transport == TCP;
      assert host != null;
      assert port > 0;
      assert parameters != null;
      assert keepAliveFactory != null;

      this.host = host;
      this.port = port;

      this.connector = new NioSocketConnector();
      DefaultIoFilterChainBuilder filterChain = connector.getFilterChain();

      // FIXME no hard-coded values
      int keepAliveInterval = parameters.containsKey(KEEP_ALIVE_INTERVAL_KEY) ? Integer
            .parseInt(parameters.get(KEEP_ALIVE_INTERVAL_KEY))
            : 10;
      int keepAliveTimeout = parameters.containsKey(KEEP_ALIVE_TIMEOUT_KEY) ? Integer
            .parseInt(parameters.get(KEEP_ALIVE_TIMEOUT_KEY))
            : 5;

      addMDCFilter(filterChain);
      addCodecFilter(filterChain);
      addLoggingFilter(filterChain);
      blockingScheduler = addBlockingRequestResponseFilter(filterChain);
      addKeepAliveFilter(filterChain, keepAliveFactory, keepAliveInterval,
            keepAliveTimeout);
      addExecutorFilter(filterChain);

      connector.setHandler(new MinaHandler(PacketDispatcher.client, this));
      connector.getSessionConfig().setKeepAlive(true);
      connector.getSessionConfig().setReuseAddress(true);
   }

   // NIOConnector implementation -----------------------------------

   public NIOSession connect() throws IOException
   {
      if (session != null)
      {
         return new MinaSession(session);
      }
      InetSocketAddress address = new InetSocketAddress(host, port);
      ConnectFuture future = connector.connect(address);
      connector.setDefaultRemoteAddress(address);

      future.awaitUninterruptibly();
      if (!future.isConnected())
      {
         throw new IOException("Cannot connect to " + address.toString());
      }
      this.session = future.getSession();
      AbstractPacket packet = new SetSessionIDMessage(Long.toString(session
            .getId()));
      session.write(packet);

      return new MinaSession(session);
   }

   public boolean disconnect()
   {
      if (session == null)
      {
         return false;
      }

      CloseFuture closeFuture = session.close().awaitUninterruptibly();
      boolean closed = closeFuture.isClosed();

      connector.dispose();
      blockingScheduler.shutdown();

      connector = null;
      blockingScheduler = null;
      session = null;

      return closed;
   }

   public void addConnectionListener(
         final ConsolidatedRemotingConnectionListener listener)
   {
      assert listener != null;
      assert connector != null;

      IoServiceListener ioListener = new IoServiceListenerAdapter(listener);
      connector.addListener(ioListener);
      listeners.put(listener, ioListener);

      if (log.isTraceEnabled())
         log.trace("added listener " + listener + " to " + this);
   }

   public void removeConnectionListener(
         ConsolidatedRemotingConnectionListener listener)
   {
      assert listener != null;
      assert connector != null;

      connector.removeListener(listeners.get(listener));
      listeners.remove(listener);

      if (log.isTraceEnabled())
         log.trace("removed listener " + listener + " from " + this);
   }

   public String getServerURI()
   {
      if (connector == null)
      {
         return TCP + "://" + host + ":" + port;
      }
      InetSocketAddress address = connector.getDefaultRemoteAddress();
      if (address != null)
      {
         return TCP + "://" + address.toString();
      } else
      {
         return TCP + "://" + host + ":" + port;
      }
   }

   public void setConnectionExceptionListener(ConnectionExceptionListener listener)
   {
      assert listener != null;
      
      this.listener = listener;
   }
   
   // KeepAliveManager implementation -------------------------------
   
   public void notifyKeepAliveTimeout(TimeoutException cause, String remoteSessionID)
   {
      if (listener != null)
         listener.handleConnectionException(cause, remoteSessionID);
      
      Iterator<ConsolidatedRemotingConnectionListener> set = listeners.keySet().iterator();
      while (set.hasNext())
      {
         ConsolidatedRemotingConnectionListener listener = set.next();
         listener.handleConnectionException(cause);
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private final class IoServiceListenerAdapter implements IoServiceListener
   {
      private final Logger log = Logger
            .getLogger(IoServiceListenerAdapter.class);

      private final ConsolidatedRemotingConnectionListener listener;

      private IoServiceListenerAdapter(
            ConsolidatedRemotingConnectionListener listener)
      {
         this.listener = listener;
      }

      public void serviceActivated(IoService service)
      {
         if (log.isTraceEnabled())
            log.trace("activated " + service);
      }

      public void serviceDeactivated(IoService service)
      {
         if (log.isTraceEnabled())
            log.trace("deactivated " + service);
      }

      public void serviceIdle(IoService service, IdleStatus idleStatus)
      {
         if (log.isTraceEnabled())
            log.trace("idle " + service + ", status=" + idleStatus);
      }

      public void sessionCreated(IoSession session)
      {
         if (log.isInfoEnabled())
            log.info("created session " + session);
      }

      public void sessionDestroyed(IoSession session)
      {
         log.warn("destroyed session " + session);

         Throwable t = new Throwable("MINA session has been destroyed");
         listener.handleConnectionException(t);
      }
   }
}
