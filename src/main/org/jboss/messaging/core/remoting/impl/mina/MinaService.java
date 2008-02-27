/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina;

import static org.jboss.messaging.core.remoting.ConnectorRegistrySingleton.REGISTRY;
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addCodecFilter;
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addExecutorFilter;
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addKeepAliveFilter;
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addLoggingFilter;
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addMDCFilter;
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addSSLFilter;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.mina.common.DefaultIoFilterChainBuilder;
import org.apache.mina.common.IdleStatus;
import org.apache.mina.common.IoService;
import org.apache.mina.common.IoServiceListener;
import org.apache.mina.common.IoSession;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.jboss.beans.metadata.api.annotations.Install;
import org.jboss.beans.metadata.api.annotations.Uninstall;
import org.jboss.messaging.core.client.FailureListener;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.Interceptor;
import org.jboss.messaging.core.remoting.RemotingException;
import org.jboss.messaging.core.remoting.RemotingService;
import org.jboss.messaging.core.remoting.impl.PacketDispatcherImpl;
import org.jboss.messaging.core.remoting.impl.RemotingConfiguration;
import org.jboss.messaging.core.server.MessagingException;

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
   
   private RemotingConfiguration remotingConfig;
   
   private NioSocketAcceptor acceptor;

   private IoServiceListener acceptorListener;

   private PacketDispatcher dispatcher;

   private List<FailureListener> listeners = new ArrayList<FailureListener>();

   private ServerKeepAliveFactory factory;
   
   private List<Interceptor> filters = new CopyOnWriteArrayList<Interceptor>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------
   
   public MinaService(RemotingConfiguration remotingConfig)
   {
      this(remotingConfig, new ServerKeepAliveFactory());
   }

   public MinaService(RemotingConfiguration remotingConfig, ServerKeepAliveFactory factory)
   {
      assert remotingConfig != null;
      assert factory != null;

      this.remotingConfig = remotingConfig;
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
         log.debug("Start MinaService with configuration:" + remotingConfig);
      
      if (acceptor == null)
      {
         acceptor = new NioSocketAcceptor();
         DefaultIoFilterChainBuilder filterChain = acceptor.getFilterChain();

         addMDCFilter(filterChain);
         if (remotingConfig.isSSLEnabled())
         {
            addSSLFilter(filterChain, false, remotingConfig.getKeyStorePath(),
                  remotingConfig.getKeyStorePassword(), remotingConfig
                        .getTrustStorePath(), remotingConfig
                        .getTrustStorePassword());
         }
         addCodecFilter(filterChain);
         addLoggingFilter(filterChain);
         addKeepAliveFilter(filterChain, factory,
               remotingConfig.getKeepAliveInterval(), remotingConfig.getKeepAliveTimeout(), this);
         addExecutorFilter(filterChain);

         // Bind
         acceptor.setDefaultLocalAddress(new InetSocketAddress(remotingConfig.getHost(), remotingConfig.getPort()));
         acceptor.setReuseAddress(true);
         acceptor.getSessionConfig().setReuseAddress(true);
         acceptor.getSessionConfig().setKeepAlive(true);
         acceptor.setCloseOnDeactivation(false);

         acceptor.setHandler(new MinaHandler(dispatcher, this, true));
         acceptor.bind();
         acceptorListener = new MinaSessionListener();
         acceptor.addListener(acceptorListener);
         
         boolean disableInvm = remotingConfig.isInvmDisabled();
         if (log.isDebugEnabled())
            log.debug("invm optimization for remoting is " + (disableInvm ? "disabled" : "enabled"));
         if (!disableInvm)
            REGISTRY.register(remotingConfig, dispatcher);
      }
      
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
      }
      
      REGISTRY.unregister(remotingConfig);
      
      started = false;
   }

   public PacketDispatcher getDispatcher()
   {
      return dispatcher;
   }
   
   public RemotingConfiguration getRemotingConfiguration()
   {
      return remotingConfig;
   }
   
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
         String sessionID = re.getSessionID();
         String clientSessionID = factory.getSessions().get(sessionID);
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

   public void setRemotingConfiguration(RemotingConfiguration remotingConfig)
   {
      assert started == false;
      
      this.remotingConfig = remotingConfig;
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
         String sessionID = Long.toString(session.getId());
         if (factory.getSessions().containsKey(sessionID))
         {
            fireFailure(new RemotingException(MessagingException.INTERNAL_ERROR, "MINA session destroyed", sessionID));
         }
      }
   }
}