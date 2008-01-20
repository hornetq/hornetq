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
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addLoggingFilter;
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addMDCFilter;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.mina.common.DefaultIoFilterChainBuilder;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.jboss.messaging.util.Logger;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.ServerLocator;
import org.jboss.messaging.core.remoting.TransportType;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class MinaService
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(MinaService.class);

   // Attributes ----------------------------------------------------

   private TransportType transport;

   private final String host;

   private final int port;
   
   private Map<String, String> parameters;
   
   private NioSocketAcceptor acceptor;

   private int blockingRequestTimeout = 5;

   private PacketDispatcher dispatcher;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public MinaService(String transport, String host, int port)
   {
      this(TransportType.valueOf(transport.toUpperCase()), host, port);
   }
   
   public MinaService(TransportType transport, String host, int port)
   {
      assert transport != null;
      assert host !=  null;
      assert port > 0;
      
      this.transport = transport;
      this.host = host;
      this.port = port;
      this.parameters = new HashMap<String, String>();
      this.dispatcher = new PacketDispatcher();
   }

   // Public --------------------------------------------------------
   
   public void setParameters(Map<String, String> parameters)
   {
      assert parameters != null;
      
      this.parameters = parameters;
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
         addExecutorFilter(filterChain);
         
         // Bind
         acceptor.setLocalAddress(new InetSocketAddress(host, port));
         acceptor.setReuseAddress(true);
         acceptor.getSessionConfig().setReuseAddress(true);
         acceptor.getSessionConfig().setKeepAlive(true);
         acceptor.setDisconnectOnUnbind(false);

         acceptor.setHandler(new MinaHandler(dispatcher));
         acceptor.bind();
         
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
