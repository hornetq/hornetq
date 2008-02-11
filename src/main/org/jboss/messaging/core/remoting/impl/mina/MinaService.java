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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.mina.common.DefaultIoFilterChainBuilder;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.jboss.beans.metadata.api.annotations.Install;
import org.jboss.beans.metadata.api.annotations.Uninstall;
import org.jboss.messaging.core.remoting.ConnectionExceptionListener;
import org.jboss.messaging.core.remoting.Interceptor;
import org.jboss.messaging.core.remoting.KeepAliveFactory;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.RemotingConfiguration;
import org.jboss.messaging.core.remoting.RemotingService;
import org.jboss.messaging.util.Logger;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class MinaService implements RemotingService, ConnectionExceptionNotifier
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(MinaService.class);

   // Attributes ----------------------------------------------------

   private boolean started = false;
   
   private RemotingConfiguration remotingConfig;
   
   private NioSocketAcceptor acceptor;

   private PacketDispatcher dispatcher;

   private List<ConnectionExceptionListener> listeners = new ArrayList<ConnectionExceptionListener>();

   private KeepAliveFactory factory;
   
   private List<Interceptor> filters = new CopyOnWriteArrayList<Interceptor>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------
   
   public MinaService(RemotingConfiguration remotingConfig)
   {
      this(remotingConfig, new ServerKeepAliveFactory());
   }

   public MinaService(RemotingConfiguration remotingConfig, KeepAliveFactory factory)
   {
      assert remotingConfig != null;
      assert factory != null;

      this.remotingConfig = remotingConfig;
      this.factory = factory;
      this.dispatcher = new PacketDispatcher(this.filters);
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

   public void addConnectionExceptionListener(ConnectionExceptionListener listener)
   {
      assert listener != null;

      listeners.add(listener);
   }

   public void removeConnectionExceptionListener(ConnectionExceptionListener listener)
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
         addCodecFilter(filterChain);
         addLoggingFilter(filterChain);
         addKeepAliveFilter(filterChain, factory,
               remotingConfig.getKeepAliveInterval(), remotingConfig.getKeepAliveTimeout());
         addExecutorFilter(filterChain);

         // Bind
         acceptor.setLocalAddress(new InetSocketAddress(remotingConfig.getHost(), remotingConfig.getPort()));
         acceptor.setReuseAddress(true);
         acceptor.getSessionConfig().setReuseAddress(true);
         acceptor.getSessionConfig().setKeepAlive(true);
         acceptor.setDisconnectOnUnbind(false);

         acceptor.setHandler(new MinaHandler(dispatcher, this));
         acceptor.bind();

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

   // ConnectionExceptionNotifier implementation -------------------------------

   public void fireConnectionException(Throwable t, String remoteSessionID)
   {
      for (ConnectionExceptionListener listener : listeners)
      {
         String clientSessionID = PacketDispatcher.sessions.get(remoteSessionID);
         listener.handleConnectionException(t, clientSessionID);
      }
   }

   // Public --------------------------------------------------------

   public void setKeepAliveFactory(KeepAliveFactory factory)
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
}
