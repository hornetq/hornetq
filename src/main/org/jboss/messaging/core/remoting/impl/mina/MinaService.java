/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina;

import static org.jboss.messaging.core.remoting.ConnectorRegistrySingleton.*;
import static org.jboss.messaging.core.remoting.TransportType.*;
import static org.jboss.messaging.core.remoting.impl.RemotingConfigurationValidator.*;
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.*;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.mina.common.DefaultIoFilterChainBuilder;
import org.apache.mina.common.IdleStatus;
import org.apache.mina.common.IoService;
import org.apache.mina.common.IoServiceListener;
import org.apache.mina.common.IoSession;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.jboss.beans.metadata.api.annotations.Install;
import org.jboss.beans.metadata.api.annotations.Uninstall;
import org.jboss.messaging.core.client.RemotingSessionListener;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Interceptor;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.RemotingService;
import org.jboss.messaging.core.remoting.impl.PacketDispatcherImpl;
import org.jboss.messaging.core.server.ClientPinger;
import org.jboss.messaging.core.server.impl.ClientPingerImpl;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class MinaService implements RemotingService, CleanUpNotifier
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(MinaService.class);

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

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public MinaService(Configuration config)
   {
      this(config, new ServerKeepAliveFactory());
   }

   public MinaService(Configuration config, ServerKeepAliveFactory factory)
   {
      assert config != null;
      assert factory != null;

      validate(config);

      this.config = config;
      this.factory = factory;
      dispatcher = new PacketDispatcherImpl(filters);
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
         log.debug("Start MinaService with configuration:" + config);
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
         acceptor.setHandler(new MinaHandler(dispatcher, threadPool,
                                             this, true, true,
                                             config.getWriteQueueBlockTimeout(),
                                             config.getWriteQueueMinBytes(),
                                             config.getWriteQueueMaxBytes()));
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
         REGISTRY.register(config.getLocation(), dispatcher);

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

      REGISTRY.unregister(config.getLocation());

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

   public void setKeepAliveFactory(ServerKeepAliveFactory factory)
   {
      assert factory != null;

      this.factory = factory;
   }

   // Public --------------------------------------------------------


   public void setRemotingConfiguration(Configuration remotingConfig)
   {
      assert started == false;

      config = remotingConfig;
   }

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

      public void sessionCreated(IoSession session)
      {
      }

      public void sessionDestroyed(IoSession session)
      {
         fireCleanup(session.getId(), null);
      }
   }
}