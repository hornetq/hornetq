/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina;

import org.apache.mina.common.*;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.jboss.beans.metadata.api.annotations.Install;
import org.jboss.beans.metadata.api.annotations.Uninstall;
import org.jboss.messaging.core.client.RemotingSessionListener;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.ping.Pinger;
import org.jboss.messaging.core.ping.impl.PingerImpl;
import org.jboss.messaging.core.remoting.ConnectorRegistryFactory;
import org.jboss.messaging.core.remoting.Interceptor;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.RemotingService;
import org.jboss.messaging.core.remoting.ResponseHandler;

import static org.jboss.messaging.core.remoting.TransportType.INVM;
import org.jboss.messaging.core.remoting.impl.PacketDispatcherImpl;
import org.jboss.messaging.core.remoting.impl.ResponseHandlerImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.Pong;

import static org.jboss.messaging.core.remoting.impl.RemotingConfigurationValidator.validate;
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addCodecFilter;
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addSSLFilter;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @version <tt>$Revision$</tt>
 */
public class RemotingServiceImpl implements RemotingService, CleanUpNotifier
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(RemotingServiceImpl.class);

   // Attributes ----------------------------------------------------

   private boolean started = false;

   private Configuration config;

   private NioSocketAcceptor acceptor;

   private IoServiceListener acceptorListener;

   private final PacketDispatcher dispatcher;

   private ExecutorService threadPool;

   private List<RemotingSessionListener> listeners = new ArrayList<RemotingSessionListener>();

   private List<Interceptor> filters = new CopyOnWriteArrayList<Interceptor>();

   private ServerKeepAliveFactory factory;

   private ScheduledExecutorService scheduledExecutor;
   private Map<IoSession, ScheduledFuture<?>> currentScheduledPingers;
   private Map<IoSession, Pinger> currentPingers;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public RemotingServiceImpl(Configuration config)
   {
      this(config, new ServerKeepAliveFactory());
   }

   public RemotingServiceImpl(Configuration config, ServerKeepAliveFactory factory)
   {
      assert config != null;
      assert factory != null;

      validate(config);

      this.config = config;
      this.factory = factory;
      dispatcher = new PacketDispatcherImpl(filters);

      scheduledExecutor = new ScheduledThreadPoolExecutor(config.getScheduledThreadPoolMaxSize());
      currentScheduledPingers = new ConcurrentHashMap<IoSession, ScheduledFuture<?>>();
      currentPingers = new ConcurrentHashMap<IoSession, Pinger>();
   }

   @Install
   public void addInterceptor(Interceptor filter)
   {
      filters.add(filter);
   }

   @Uninstall
   public void removeInterceptor(Interceptor filter)
   {
      filters.remove(filter);
   }

   public void addRemotingSessionListener(RemotingSessionListener listener)
   {
      assert listener != null;

      listeners.add(listener);
   }

   public void removeRemotingSessionListener(RemotingSessionListener listener)
   {
      assert listener != null;

      listeners.remove(listener);
   }

   // TransportService implementation -------------------------------

   public void start() throws Exception
   {
      if (log.isDebugEnabled())
      {
         log.debug("Start RemotingServiceImpl with configuration:" + config);
      }

      // if INVM transport is set, we bypass MINA setup
      if (config.getTransport() != INVM
              && acceptor == null)
      {
         acceptor = new NioSocketAcceptor();

         acceptor.setSessionDataStructureFactory(new MessagingIOSessionDataStructureFactory());

         DefaultIoFilterChainBuilder filterChain = acceptor.getFilterChain();

         // addMDCFilter(filterChain);
         if (config.isSSLEnabled())
         {
            addSSLFilter(filterChain, false, config.getKeyStorePath(),
                    config.getKeyStorePassword(), config
                    .getTrustStorePath(), config
                    .getTrustStorePassword());
         }
         addCodecFilter(filterChain);

         // Bind
         acceptor.setDefaultLocalAddress(new InetSocketAddress(config.getHost(), config.getPort()));
         acceptor.getSessionConfig().setTcpNoDelay(config.isTcpNoDelay());
         int receiveBufferSize = config.getTcpReceiveBufferSize();
         if (receiveBufferSize != -1)
         {
            acceptor.getSessionConfig().setReceiveBufferSize(receiveBufferSize);
         }
         int sendBufferSize = config.getTcpSendBufferSize();
         if (sendBufferSize != -1)
         {
            acceptor.getSessionConfig().setSendBufferSize(sendBufferSize);
         }
         acceptor.setReuseAddress(true);
         acceptor.getSessionConfig().setReuseAddress(true);
         acceptor.getSessionConfig().setKeepAlive(true);
         acceptor.setCloseOnDeactivation(false);

         threadPool = Executors.newCachedThreadPool();
         acceptor.setHandler(new MinaHandler(dispatcher, threadPool, this, true, true));
         acceptor.bind();
         acceptorListener = new MinaSessionListener();
         acceptor.addListener(acceptorListener);
      }

      // TODO reenable invm transport
//      boolean disableInvm = config.isInvmDisabled();
//      if (log.isDebugEnabled())
//         log.debug("invm optimization for remoting is " + (disableInvm ? "disabled" : "enabled"));
      // if (!disableInvm)

      log.info("Registering:" + config.getLocation());
      ConnectorRegistryFactory.getRegistry().register(config.getLocation(), dispatcher);

      started = true;
   }

   public void stop()
   {
      if (acceptor != null)
      {
         // remove the listener before disposing the acceptor
         // so that we're not notified when the sessions are destroyed
         acceptor.removeListener(acceptorListener);
         acceptor.unbind();
         acceptor.dispose();
         acceptor = null;
         threadPool.shutdown();
      }

      ConnectorRegistryFactory.getRegistry().unregister(config.getLocation());

      started = false;
   }

   public PacketDispatcher getDispatcher()
   {
      return dispatcher;
   }

   public Configuration getConfiguration()
   {
      return config;
   }

   public ServerKeepAliveFactory getKeepAliveFactory()
   {
      return factory;
   }

   /**
    * This method must only be called by tests which requires
    * to insert Filters (e.g. to simulate network failures)
    */
   public DefaultIoFilterChainBuilder getFilterChain()
   {
      assert started == true;
      assert acceptor != null;

      return acceptor.getFilterChain();
   }

   // FailureNotifier implementation -------------------------------

   public void fireCleanup(long sessionID, MessagingException me)
   {
      if (factory.getSessions().contains(sessionID))
      {
         for (RemotingSessionListener listener : listeners)
         {
            listener.sessionDestroyed(sessionID, me);
         }
         factory.getSessions().remove(sessionID);
      }
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private final class MinaSessionListener implements IoServiceListener
   {

      public void serviceActivated(IoService service)
      {
      }

      public void serviceDeactivated(IoService service)
      {
      }

      public void serviceIdle(IoService service, IdleStatus idleStatus)
      {
      }

      /**
       * register a pinger for the new client
       *
       * @param session
       */
      public void sessionCreated(IoSession session)
      {
         //register pinger
         if (config.getKeepAliveInterval() > 0)
         {
            ResponseHandler pongHandler = new ResponseHandlerImpl(dispatcher.generateID());
            Pinger pinger = new PingerImpl(getDispatcher(), new MinaSession(session, null), config.getKeepAliveTimeout(), pongHandler, RemotingServiceImpl.this);
            ScheduledFuture<?> future = scheduledExecutor.scheduleAtFixedRate(pinger, config.getKeepAliveInterval(), config.getKeepAliveInterval(), TimeUnit.MILLISECONDS);
            currentScheduledPingers.put(session, future);
            currentPingers.put(session, pinger);
            factory.getSessions().add(session.getId());
         }
      }

      /**
       * destry th epinger and stop
       *
       * @param session
       */
      public void sessionDestroyed(IoSession session)
      {
         ScheduledFuture<?> future = currentScheduledPingers.remove(session);
         if (future != null)
         {
            future.cancel(true);
         }
         Pinger pinger = currentPingers.remove(session);
         if (pinger != null)
         {
            pinger.close();
         }
         fireCleanup(session.getId(), null);
      }
   }
}