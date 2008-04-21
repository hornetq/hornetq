/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina;

import static org.jboss.messaging.core.remoting.ConnectorRegistrySingleton.REGISTRY;
import static org.jboss.messaging.core.remoting.TransportType.INVM;
import static org.jboss.messaging.core.remoting.impl.RemotingConfigurationValidator.validate;
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addCodecFilter;
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addKeepAliveFilter;
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addLoggingFilter;
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addMDCFilter;
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addSSLFilter;

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
import org.jboss.messaging.core.client.FailureListener;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Interceptor;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.RemotingException;
import org.jboss.messaging.core.remoting.RemotingService;
import org.jboss.messaging.core.remoting.impl.PacketDispatcherImpl;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class MinaService implements RemotingService, FailureNotifier
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(MinaService.class);

   // Attributes ----------------------------------------------------

   private boolean started = false;
   
   private Configuration config;
   
   private NioSocketAcceptor acceptor;

   private IoServiceListener acceptorListener;

   private PacketDispatcher dispatcher;

   private ExecutorService threadPool; 
   
   private List<FailureListener> listeners = new ArrayList<FailureListener>();

   private ServerKeepAliveFactory factory;
   
   private List<Interceptor> filters = new CopyOnWriteArrayList<Interceptor>();

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
      this.dispatcher = new PacketDispatcherImpl(this.filters);
   }
   
   @Install
   public void addInterceptor(Interceptor filter)
   {
      this.filters.add(filter);
   }

   @Uninstall
   public void removeInterceptor(Interceptor filter)
   {
      this.filters.remove(filter);
   }

   public void addFailureListener(FailureListener listener)
   {
      assert listener != null;

      listeners.add(listener);
   }

   public void removeFailureListener(FailureListener listener)
   {
      assert listener != null;

      listeners.remove(listener);
   }

   // TransportService implementation -------------------------------

   public void start() throws Exception
   {
      if (log.isDebugEnabled())
         log.debug("Start MinaService with configuration:" + config);
      
      // if INVM transport is set, we bypass MINA setup
      if (config.getTransport() != INVM 
            && acceptor == null)
      {
         acceptor = new NioSocketAcceptor();
         DefaultIoFilterChainBuilder filterChain = acceptor.getFilterChain();

         addMDCFilter(filterChain);
         if (config.isSSLEnabled())
         {
            addSSLFilter(filterChain, false, config.getKeyStorePath(),
                  config.getKeyStorePassword(), config
                        .getTrustStorePath(), config
                        .getTrustStorePassword());
         }
         addCodecFilter(filterChain);
         addLoggingFilter(filterChain);
         addKeepAliveFilter(filterChain, factory,
               config.getKeepAliveInterval(), config.getKeepAliveTimeout(), this);

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
         acceptor.setHandler(new MinaHandler(dispatcher, threadPool, this, true));
         acceptor.bind();
         acceptorListener = new MinaSessionListener();
         acceptor.addListener(acceptorListener);
      }
      
      boolean disableInvm = config.isInvmDisabled();
      if (log.isDebugEnabled())
         log.debug("invm optimization for remoting is " + (disableInvm ? "disabled" : "enabled"));
      if (!disableInvm)
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

   public void fireFailure(MessagingException me)
   {
      if (me instanceof RemotingException)
      {
         RemotingException re = (RemotingException) me;
         long sessionID = re.getSessionID();
         long clientSessionID = factory.getSessions().get(sessionID);
         for (FailureListener listener : listeners)
         {
            listener.onFailure(new RemotingException(re.getCode(), re.getMessage(), clientSessionID));
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

   public void setRemotingConfiguration(Configuration remotingConfig)
   {
      assert started == false;
      
      this.config = remotingConfig;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private final class MinaSessionListener implements IoServiceListener {

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
         if (session.isClosing())
            return;

         long sessionID = session.getId();
         if (factory.getSessions().containsKey(sessionID))
         {
            fireFailure(new RemotingException(MessagingException.INTERNAL_ERROR, "MINA session destroyed", sessionID));
         }
      }
   }
}