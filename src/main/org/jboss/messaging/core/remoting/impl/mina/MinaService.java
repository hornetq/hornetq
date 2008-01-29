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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.jboss.messaging.core.remoting.ServerLocator;
import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.util.Logger;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class MinaService implements KeepAliveNotifier
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(MinaService.class);

   public static final String KEEP_ALIVE_INTERVAL_KEY = "keepAliveInterval";
   public static final String KEEP_ALIVE_TIMEOUT_KEY = "keepAliveTimeout";
   public static final String DISABLE_INVM_KEY = "disable-invm";

   // Attributes ----------------------------------------------------

   private TransportType transport;

   private final String host;

   private final int port;

   private Map<String, String> parameters;

   private NioSocketAcceptor acceptor;

   private PacketDispatcher dispatcher;

   private ConnectionExceptionListener listener;

   private KeepAliveFactory factory;
   
   private List<PacketFilter> filters = new CopyOnWriteArrayList<PacketFilter>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public MinaService(String transport, String host, int port)
   {
      this(TransportType.valueOf(transport.toUpperCase()), host, port, new ServerKeepAliveFactory());
   }

   public MinaService(TransportType transport, String host, int port, KeepAliveFactory factory)
   {
      assert transport != null;
      assert host != null;
      assert port > 0;
      assert factory != null;

      this.transport = transport;
      this.host = host;
      this.port = port;
      this.parameters = new HashMap<String, String>();
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

   public void setParameters(Map<String, String> parameters)
   {
      assert parameters != null;

      this.parameters = parameters;
   }

   public void setKeepAliveFactory(KeepAliveFactory factory)
   {
      assert factory != null;
      
      this.factory = factory;
   }

   public ServerLocator getLocator()
   {
      return new ServerLocator(transport, host, port, parameters);
   }

   public PacketDispatcher getDispatcher()
   {
      return dispatcher;
   }

   public void start() throws Exception
   {
      if (acceptor == null)
      {
         acceptor = new NioSocketAcceptor();
         DefaultIoFilterChainBuilder filterChain = acceptor.getFilterChain();

         addMDCFilter(filterChain);
         addCodecFilter(filterChain);
         addLoggingFilter(filterChain);
         if (parameters.containsKey(KEEP_ALIVE_INTERVAL_KEY)
               && parameters.containsKey(KEEP_ALIVE_TIMEOUT_KEY))
         {
            int keepAliveInterval = Integer.parseInt(parameters
                  .get(KEEP_ALIVE_INTERVAL_KEY));
            int keepAliveTimeout = Integer.parseInt(parameters
                  .get(KEEP_ALIVE_TIMEOUT_KEY));
            addKeepAliveFilter(filterChain, factory,
                  keepAliveInterval, keepAliveTimeout);
         }
         addExecutorFilter(filterChain);

         // Bind
         acceptor.setLocalAddress(new InetSocketAddress(host, port));
         acceptor.setReuseAddress(true);
         acceptor.getSessionConfig().setReuseAddress(true);
         acceptor.getSessionConfig().setKeepAlive(true);
         acceptor.setDisconnectOnUnbind(false);

         acceptor.setHandler(new MinaHandler(dispatcher, this));
         acceptor.bind();

         boolean disableInvm = false;
         if (parameters.containsKey(DISABLE_INVM_KEY))
               disableInvm = Boolean.valueOf(parameters.get(DISABLE_INVM_KEY)).booleanValue();
         if (log.isDebugEnabled())
            log.debug("invm optimization for remoting is " + (disableInvm ? "disabled" : "enabled"));
         if (!disableInvm)
            REGISTRY.register(getLocator(), dispatcher);
      }
   }

   public void stop()
   {
      if (acceptor != null)
      {
         acceptor.unbind();
         acceptor.dispose();
         acceptor = null;

         REGISTRY.unregister(getLocator());
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
