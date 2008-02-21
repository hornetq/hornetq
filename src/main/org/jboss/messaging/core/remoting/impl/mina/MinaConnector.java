/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina;

import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addBlockingRequestResponseFilter;
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addCodecFilter;
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addExecutorFilter;
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addKeepAliveFilter;
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addLoggingFilter;
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addMDCFilter;
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addSSLFilter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.mina.common.CloseFuture;
import org.apache.mina.common.ConnectFuture;
import org.apache.mina.common.DefaultIoFilterChainBuilder;
import org.apache.mina.common.IdleStatus;
import org.apache.mina.common.IoService;
import org.apache.mina.common.IoServiceListener;
import org.apache.mina.common.IoSession;
import org.apache.mina.filter.ssl.SslFilter;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.jboss.jms.client.api.FailureListener;
import org.jboss.messaging.core.remoting.KeepAliveFactory;
import org.jboss.messaging.core.remoting.NIOConnector;
import org.jboss.messaging.core.remoting.NIOSession;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.RemotingConfiguration;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;
import org.jboss.messaging.core.remoting.wireformat.Ping;
import org.jboss.messaging.util.Logger;
import org.jboss.messaging.util.MessagingException;
import org.jboss.messaging.util.RemotingException;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class MinaConnector implements NIOConnector, FailureNotifier
{
   // Constants -----------------------------------------------------

   private final Logger log = Logger.getLogger(MinaConnector.class);
   
   // Attributes ----------------------------------------------------

   private RemotingConfiguration configuration;

   private NioSocketConnector connector;

   private ScheduledExecutorService blockingScheduler;

   private IoSession session;

   private List<FailureListener> listeners = new ArrayList<FailureListener>();
   private IoServiceListenerAdapter ioListener;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public MinaConnector(RemotingConfiguration configuration)
   {
      this(configuration, new ClientKeepAliveFactory());
   }

   public MinaConnector(RemotingConfiguration configuration, KeepAliveFactory keepAliveFactory)
   {
      assert configuration != null;
      assert keepAliveFactory != null;

      this.configuration = configuration;

      this.connector = new NioSocketConnector();
      DefaultIoFilterChainBuilder filterChain = connector.getFilterChain();

      addMDCFilter(filterChain);
      if (configuration.isSSLEnabled())
      {
         try
         {
            addSSLFilter(filterChain, true, configuration.getKeyStorePath(), configuration.getKeyStorePassword(), null, null);
         } catch (Exception e)
         {
            IllegalStateException ise = new IllegalStateException("Unable to create MinaConnector for " + configuration);
            ise.initCause(e);
            throw ise;
         }
      }
      addCodecFilter(filterChain);
      addLoggingFilter(filterChain);
      blockingScheduler = addBlockingRequestResponseFilter(filterChain);
      addKeepAliveFilter(filterChain, keepAliveFactory, configuration.getKeepAliveInterval(),
            configuration.getKeepAliveTimeout(), this);
      addExecutorFilter(filterChain);

      connector.setHandler(new MinaHandler(PacketDispatcher.client, this, false));
      connector.getSessionConfig().setKeepAlive(true);
      connector.getSessionConfig().setReuseAddress(true);
   }

   // NIOConnector implementation -----------------------------------

   public NIOSession connect() throws IOException
   {
      if (session != null && session.isConnected())
      {
         return new MinaSession(session);
      }
      InetSocketAddress address = new InetSocketAddress(configuration.getHost(), configuration.getPort());
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
      AbstractPacket packet = new Ping(Long.toString(session.getId()));
      session.write(packet);

      return new MinaSession(session);
   }

   public boolean disconnect()
   {
      if (session == null)
      {
         return false;
      }

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
      CloseFuture closeFuture = session.close().awaitUninterruptibly();
      boolean closed = closeFuture.isClosed();
      try
      {
         Thread.sleep(1000);
      } catch (InterruptedException e)
      {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
      
      connector.removeListener(ioListener);
      connector.dispose();
      blockingScheduler.shutdown();

      connector = null;
      blockingScheduler = null;
      session = null;

      return closed;
   }

   public synchronized void addFailureListener(final FailureListener listener)
   {
      assert listener != null;
      assert connector != null;

      listeners.add(listener);

      if (log.isTraceEnabled())
         log.trace("added listener " + listener + " to " + this);
   }

   public synchronized void removeFailureListener(FailureListener listener)
   {
      assert listener != null;
      assert connector != null;

      listeners.remove(listener);

      if (log.isTraceEnabled())
         log.trace("removed listener " + listener + " from " + this);
   }

   public String getServerURI()
   { 
      return configuration.getURI();
   }
   
   // FailureNotifier implementation -------------------------------
   
   public synchronized void fireFailure(MessagingException me)
   {
      for (FailureListener listener: listeners)
      {
         listener.onFailure(me);
      }
   }

   // Public --------------------------------------------------------
   
   @Override
   public String toString()
   {
      return "MinaConnector@" + System.identityHashCode(this) + "[configuration=" + configuration + "]"; 
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
         RemotingException re =
            new RemotingException(MessagingException.INTERNAL_ERROR, "MINA session has been destroyed", Long.toString(session.getId()));
         fireFailure(re);
      }
   }
}
