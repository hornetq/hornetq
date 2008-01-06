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
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addLoggingFilter;
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addMDCFilter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.mina.common.CloseFuture;
import org.apache.mina.common.ConnectFuture;
import org.apache.mina.common.DefaultIoFilterChainBuilder;
import org.apache.mina.common.IdleStatus;
import org.apache.mina.common.IoService;
import org.apache.mina.common.IoServiceListener;
import org.apache.mina.common.IoSession;
import org.apache.mina.filter.executor.ExecutorFilter;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.jboss.jms.client.remoting.ConsolidatedRemotingConnectionListener;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.remoting.NIOConnector;
import org.jboss.messaging.core.remoting.NIOSession;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.TransportType;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class MinaConnector implements NIOConnector
{
   // Constants -----------------------------------------------------

   private final Logger log = Logger.getLogger(MinaConnector.class);

   // Attributes ----------------------------------------------------

   private String host;

   private int port;
   
   private NioSocketConnector connector;

   private ScheduledExecutorService blockingScheduler;

   private IoSession session;

   private Map<ConsolidatedRemotingConnectionListener, IoServiceListener> listeners = new HashMap<ConsolidatedRemotingConnectionListener, IoServiceListener>();

   private ExecutorFilter executorFilter;


   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public MinaConnector(TransportType transport, String host, int port)
   {
      assert transport == TCP;
      assert host != null;
      assert port > 0;
      
      this.host = host;
      this.port = port;
      
      this.connector = new NioSocketConnector();
      DefaultIoFilterChainBuilder filterChain = connector.getFilterChain();
      
      addMDCFilter(filterChain);

      addCodecFilter(filterChain);

      blockingScheduler = addBlockingRequestResponseFilter(filterChain);

      addLoggingFilter(filterChain);
      
      executorFilter = FilterChainSupport.addExecutorFilter(filterChain);

      connector.setHandler(new MinaHandler(PacketDispatcher.client));
      connector.getSessionConfig().setKeepAlive(true);
      connector.getSessionConfig().setReuseAddress(true);
   }

   // NIOConnector implementation -----------------------------------
   
   public NIOSession connect() throws IOException 
   {
      InetSocketAddress address = new InetSocketAddress(host, port);
      ConnectFuture future = connector.connect(address);
      connector.setDefaultRemoteAddress(address);

      future.awaitUninterruptibly();
      if (!future.isConnected())
      {
         throw new IOException("Cannot connect to " + address.toString());
      }
      this.session = future.getSession();
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
      
      this.executorFilter.destroy();

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
   
   public void removeConnectionListener(ConsolidatedRemotingConnectionListener listener)
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
      } else {
         return TCP + "://" + host + ":" + port;
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
