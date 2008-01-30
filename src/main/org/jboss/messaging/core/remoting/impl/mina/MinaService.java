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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;

import org.apache.mina.common.DefaultIoFilterChainBuilder;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.jboss.beans.metadata.api.annotations.Install;
import org.jboss.beans.metadata.api.annotations.Uninstall;
import org.jboss.messaging.core.remoting.ConnectionExceptionListener;
import org.jboss.messaging.core.remoting.KeepAliveFactory;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.PacketFilter;
import org.jboss.messaging.core.remoting.RemotingConfiguration;
import org.jboss.messaging.core.remoting.RemotingService;
import org.jboss.messaging.util.Logger;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class MinaService implements RemotingService, KeepAliveNotifier
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(MinaService.class);

   public static final String DISABLE_INVM_KEY = "disable-invm";

   // Attributes ----------------------------------------------------

   private RemotingConfiguration remotingConfig;
   
   private NioSocketAcceptor acceptor;

   private PacketDispatcher dispatcher;

   private ConnectionExceptionListener listener;

   private KeepAliveFactory factory;
   
   private List<PacketFilter> filters = new CopyOnWriteArrayList<PacketFilter>();

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
   public void addFilter(PacketFilter filter)
   {
      this.filters.add(filter);
   }

   @Uninstall
   public void removeFilter(PacketFilter filter)
   {
      this.filters.remove(filter);
   }

   public void setConnectionExceptionListener(ConnectionExceptionListener listener)
   {
      assert listener != null;

      this.listener = listener;
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
   }

   public void stop()
   {
      if (acceptor != null)
      {
         acceptor.unbind();
         acceptor.dispose();
         acceptor = null;
      }
      
      REGISTRY.unregister();
   }

   public PacketDispatcher getDispatcher()
   {
      return dispatcher;
   }
   
   public RemotingConfiguration getRemotingConfiguration()
   {
      return remotingConfig;
   }

   // KeepAliveManager implementation -------------------------------

   public void notifyKeepAliveTimeout(TimeoutException e, String remoteSessionID)
   {
      if (listener != null)
      {
         String clientSessionID = PacketDispatcher.sessions.get(remoteSessionID);
         listener.handleConnectionException(e, clientSessionID);
      }
   }

   // Public --------------------------------------------------------

   public void setKeepAliveFactory(KeepAliveFactory factory)
   {
      assert factory != null;
      
      this.factory = factory;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
